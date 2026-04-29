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

// respFields: 从 SSE wire / JSON 抽取出的关键字段（用于 status<400 的精简 entry）
type respFields struct {
	parts        []string
	id           string // OpenAI chatcmpl-xxx / Anthropic msg_xxx，用于跨日志关联
	finishReason string
	model        string
	usage        any
	toolCalls    []any
	stopReason   string // Anthropic
	errorObj     any
}

// extractFromObj: OpenAI / vllm / Anthropic 通用字段抽取
func extractFromObj(obj map[string]any, rf *respFields) {
	if choices, ok := obj["choices"].([]any); ok {
		for _, c := range choices {
			cm, _ := c.(map[string]any)
			if cm == nil {
				continue
			}
			if d, ok := cm["delta"].(map[string]any); ok {
				if s, ok := d["content"].(string); ok {
					rf.parts = append(rf.parts, s)
				}
				if tc, ok := d["tool_calls"].([]any); ok {
					rf.toolCalls = append(rf.toolCalls, tc...)
				}
			}
			if m, ok := cm["message"].(map[string]any); ok {
				if s, ok := m["content"].(string); ok {
					rf.parts = append(rf.parts, s)
				}
				if tc, ok := m["tool_calls"].([]any); ok {
					rf.toolCalls = append(rf.toolCalls, tc...)
				}
			}
			if fr, ok := cm["finish_reason"].(string); ok && fr != "" {
				rf.finishReason = fr
			}
		}
	}
	if d, ok := obj["delta"].(map[string]any); ok {
		if s, ok := d["text"].(string); ok {
			rf.parts = append(rf.parts, s)
		}
		if sr, ok := d["stop_reason"].(string); ok && sr != "" {
			rf.stopReason = sr
		}
	}
	if content, ok := obj["content"].([]any); ok {
		for _, b := range content {
			bm, _ := b.(map[string]any)
			if bm == nil {
				continue
			}
			if s, ok := bm["text"].(string); ok {
				rf.parts = append(rf.parts, s)
			}
		}
	}
	if u, ok := obj["usage"].(map[string]any); ok && u != nil {
		rf.usage = u
	}
	if m, ok := obj["model"].(string); ok && m != "" {
		rf.model = m
	}
	if id, ok := obj["id"].(string); ok && id != "" {
		rf.id = id
	}
	if sr, ok := obj["stop_reason"].(string); ok && sr != "" {
		rf.stopReason = sr
	}
	if e, ok := obj["error"]; ok && e != nil {
		rf.errorObj = e
	}
}

// extractResp: 从 SSE wire / 完整 JSON 抽出关键字段。失败时 parts 为空，调用方处理。
func extractResp(s string) respFields {
	rf := respFields{}
	if s == "" {
		return rf
	}
	var whole map[string]any
	if err := json.Unmarshal([]byte(s), &whole); err == nil {
		extractFromObj(whole, &rf)
		if len(rf.parts) > 0 || rf.errorObj != nil {
			return rf
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
		extractFromObj(obj, &rf)
	}
	return rf
}

// buildRespMeta: 把抽取出的非 content 字段组装成 resp_meta 子对象（仅在有内容时返回非 nil）
func buildRespMeta(rf respFields) map[string]any {
	m := map[string]any{}
	if rf.id != "" {
		m["id"] = rf.id
	}
	if rf.finishReason != "" {
		m["finish_reason"] = rf.finishReason
	}
	if rf.model != "" {
		m["model"] = rf.model
	}
	if rf.usage != nil {
		m["usage"] = rf.usage
	}
	if len(rf.toolCalls) > 0 {
		m["tool_calls"] = rf.toolCalls
	}
	if rf.stopReason != "" {
		m["stop_reason"] = rf.stopReason
	}
	if rf.errorObj != nil {
		m["error"] = rf.errorObj
	}
	if len(m) == 0 {
		return nil
	}
	return m
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
	// resp_body 处理（option D）：
	//   - status >= 400：完整 SSE wire（lossless 复盘）
	//   - status < 400：抽取 content 文本 + resp_meta（finish_reason/usage/model/tool_calls/...）
	statusCode := 0
	if v, ok := m["status"].(float64); ok {
		statusCode = int(v)
	}
	if len(resp) > 0 {
		if statusCode >= 400 {
			if utf8.Valid(resp) {
				m["resp_body"] = string(resp)
			} else {
				m["resp_body"] = base64.StdEncoding.EncodeToString(resp)
				m["resp_body_b64"] = true
			}
		} else {
			rf := extractResp(string(resp))
			content := strings.Join(rf.parts, "")
			if content == "" && rf.errorObj == nil && rf.finishReason == "" {
				// 抽不出任何东西，原文兜底（不丢数据）
				if utf8.Valid(resp) {
					m["resp_body"] = string(resp)
				} else {
					m["resp_body"] = base64.StdEncoding.EncodeToString(resp)
					m["resp_body_b64"] = true
				}
			} else {
				if utf8.ValidString(content) {
					m["resp_body"] = content
				} else {
					m["resp_body"] = base64.StdEncoding.EncodeToString([]byte(content))
					m["resp_body_b64"] = true
				}
				if meta := buildRespMeta(rf); meta != nil {
					m["resp_meta"] = meta
				}
			}
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
