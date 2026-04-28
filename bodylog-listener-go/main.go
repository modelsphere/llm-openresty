// bodylog-listener: 接收 openresty 通过 lua-resty-logger-socket 发来的
// 长度前缀二进制帧，解析后落盘成抽取过 SSE content 的 JSONL。
//
// 二进制帧（与 session_route.conf 里的 bodylog_finalize 保持同步，big-endian）：
//   [u32 total_len][u16 meta_len][meta_json][u32 req_len][req_bytes][u32 resp_len][resp_bytes]
// total_len 不含开头 4 字节自身。openresty 端只 cjson.encode 小 meta，
// req_body / resp_body 作为裸字节传输，跳过大字符串 escape 扫描（实测能省 5-8ms / entry）。
//
// 接口：
//   - 监听 TCP 9999（BODYLOG_HOST / BODYLOG_PORT 可覆盖）
//   - 落盘 /usr/local/openresty/nginx/logs/bodies/YYYY-MM-DD.jsonl（BODYLOG_DIR 可覆盖）
//   - 每天 0:00 切日，旧文件 gzip + 14 天滚动删除
//   - SIGTERM/SIGINT 优雅退出
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = "9999"
	defaultDir  = "/usr/local/openresty/nginx/logs/bodies"
	maxFrame    = 64 * 1024 * 1024 // 单帧最大 64 MB（防御 OOM；正常 256K input + 2M resp cap 远小于此）
	keepDays    = 14
	readBufSize = 1 << 20
)

type dayWriter struct {
	mu  sync.Mutex
	day string
	f   *os.File
	dir string
}

func (w *dayWriter) write(p []byte) error {
	today := time.Now().Format("2006-01-02")
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.day != today {
		if w.f != nil {
			_ = w.f.Close()
		}
		if err := os.MkdirAll(w.dir, 0o755); err != nil {
			return err
		}
		path := filepath.Join(w.dir, today+".jsonl")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		w.f = f
		w.day = today
		log.Printf("opened %s", path)
		go housekeep(w.dir)
	}
	_, err := w.f.Write(p)
	return err
}

func (w *dayWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
}

// extractText: OpenAI/vllm/Anthropic 通用文本片段抽取
func extractText(obj map[string]any, parts *[]string) {
	if choices, ok := obj["choices"].([]any); ok {
		for _, c := range choices {
			cm, _ := c.(map[string]any)
			if cm == nil {
				continue
			}
			if d, ok := cm["delta"].(map[string]any); ok {
				if s, ok := d["content"].(string); ok {
					*parts = append(*parts, s)
				}
			}
			if m, ok := cm["message"].(map[string]any); ok {
				if s, ok := m["content"].(string); ok {
					*parts = append(*parts, s)
				}
			}
		}
	}
	if d, ok := obj["delta"].(map[string]any); ok {
		if s, ok := d["text"].(string); ok {
			*parts = append(*parts, s)
		}
	}
	if content, ok := obj["content"].([]any); ok {
		for _, b := range content {
			bm, _ := b.(map[string]any)
			if bm == nil {
				continue
			}
			if s, ok := bm["text"].(string); ok {
				*parts = append(*parts, s)
			}
		}
	}
}

// extractResp: SSE wire / 完整 JSON → 纯文本结果。失败返回原文。
func extractResp(s string) string {
	if s == "" {
		return s
	}
	parts := make([]string, 0, 32)
	var whole map[string]any
	if err := json.Unmarshal([]byte(s), &whole); err == nil {
		extractText(whole, &parts)
		if len(parts) > 0 {
			return strings.Join(parts, "")
		}
	}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimLeft(line[5:], " \t")
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		extractText(obj, &parts)
	}
	if len(parts) > 0 {
		return strings.Join(parts, "")
	}
	return s
}

// readFrame 读一个完整二进制帧，返回 meta_json / req_body / resp_body 三段。
// 帧布局：[u32 total_len][u16 meta_len][meta][u32 req_len][req][u32 resp_len][resp]
func readFrame(r io.Reader) (meta, req, resp []byte, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total == 0 || total > maxFrame {
		err = fmt.Errorf("invalid frame length %d", total)
		return
	}
	buf := make([]byte, total)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	if len(buf) < 2 {
		err = fmt.Errorf("frame too short for meta_len")
		return
	}
	metaLen := int(binary.BigEndian.Uint16(buf[:2]))
	pos := 2
	if pos+metaLen+4 > len(buf) {
		err = fmt.Errorf("frame truncated at meta")
		return
	}
	meta = buf[pos : pos+metaLen]
	pos += metaLen
	reqLen := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	if pos+reqLen+4 > len(buf) {
		err = fmt.Errorf("frame truncated at req_body")
		return
	}
	req = buf[pos : pos+reqLen]
	pos += reqLen
	respLen := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	if pos+respLen != len(buf) {
		err = fmt.Errorf("frame size mismatch: pos=%d+resp=%d != total=%d", pos, respLen, len(buf))
		return
	}
	resp = buf[pos : pos+respLen]
	return
}

func handleConn(c net.Conn, w *dayWriter) {
	defer c.Close()
	r := bufio.NewReaderSize(c, readBufSize)
	for {
		meta, req, resp, err := readFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("read err from %s: %v", c.RemoteAddr(), err)
			}
			return
		}
		out, err := assembleEntry(meta, req, resp)
		if err != nil {
			log.Printf("assemble err: %v", err)
			continue
		}
		if err := w.write(out); err != nil {
			log.Printf("write err: %v", err)
			return
		}
	}
}

// assembleEntry: 合并 meta_json + req(utf-8 检查/base64) + resp(SSE 抽取) → 单行 JSONL
func assembleEntry(metaJSON, req, resp []byte) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(metaJSON, &m); err != nil {
		return nil, fmt.Errorf("meta unmarshal: %w", err)
	}
	// req_body：utf8 → 直接放，否则 base64
	if len(req) > 0 {
		if utf8.Valid(req) {
			m["req_body"] = string(req)
		} else {
			m["req_body"] = base64.StdEncoding.EncodeToString(req)
			m["req_body_b64"] = true
		}
	} else {
		m["req_body"] = ""
	}
	// resp_body：SSE 抽取后多半是干净 utf-8；不是就 base64 原始
	if len(resp) > 0 {
		extracted := extractResp(string(resp))
		if utf8.ValidString(extracted) {
			m["resp_body"] = extracted
		} else {
			m["resp_body"] = base64.StdEncoding.EncodeToString([]byte(extracted))
			m["resp_body_b64"] = true
		}
	} else {
		m["resp_body"] = ""
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("entry marshal: %w", err)
	}
	return append(out, '\n'), nil
}

func housekeep(dir string) {
	today := time.Now().Format("2006-01-02")
	cutoff := time.Now().AddDate(0, 0, -keepDays).Format("2006-01-02")
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("housekeep readdir %s: %v", dir, err)
		return
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".jsonl"):
			base := strings.TrimSuffix(name, ".jsonl")
			if base == today || len(base) < 10 {
				continue
			}
			src := filepath.Join(dir, name)
			dst := src + ".gz"
			if _, err := os.Stat(dst); err == nil {
				continue
			}
			if err := gzipFile(src, dst); err != nil {
				log.Printf("gzip %s failed: %v", src, err)
				continue
			}
			_ = os.Remove(src)
			log.Printf("compressed %s → %s", src, dst)
		case strings.HasSuffix(name, ".jsonl.gz"):
			base := strings.TrimSuffix(name, ".jsonl.gz")
			if len(base) >= 10 && base < cutoff {
				_ = os.Remove(filepath.Join(dir, name))
				log.Printf("removed old %s", name)
			}
		}
	}
}

func gzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	gw, _ := gzip.NewWriterLevel(out, 6)
	if _, err := io.Copy(gw, in); err != nil {
		_ = gw.Close()
		return err
	}
	return gw.Close()
}

func envOr(k, dflt string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return dflt
}

// 编译期防止 unused import
var _ = bytes.Buffer{}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	host := envOr("BODYLOG_HOST", defaultHost)
	port := envOr("BODYLOG_PORT", defaultPort)
	dir := envOr("BODYLOG_DIR", defaultDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", dir, err)
	}
	addr := host + ":" + port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	log.Printf("listening on %s, writing to %s/", addr, dir)

	w := &dayWriter{dir: dir}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go func() {
		<-ctx.Done()
		log.Println("shutdown requested")
		_ = ln.Close()
	}()

	go housekeep(dir)
	go func() {
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				housekeep(dir)
			}
		}
	}()

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept err: %v", err)
			continue
		}
		go handleConn(c, w)
	}
	w.close()
	log.Println("exited")
}
