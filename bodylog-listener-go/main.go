// bodylog-listener: 接收 lua-resty-logger-socket 通过 TCP 发来的 JSONL 日志，
// 在内存里抽出 SSE / OpenAI / Anthropic 响应里的纯文本结果，落盘成精简 entry。
//
// 替代 Python 版本的原因：单进程 Python 在高 qps（~23 req/s × 1500 chunk/req）
// 下被 GIL 限到单核 ~28%，TCP recv 反压会让 openresty 端 cosocket send
// 多 yield 几次（实测 W3 pct=100 TTFT +16%）。Go 用 goroutine 跑满所有核，
// json.Unmarshal 比 Python 快几倍，反压基本消失。
//
// 接口与 Python 版完全一致：
//   - 监听 TCP 9999（BODYLOG_HOST / BODYLOG_PORT 可覆盖）
//   - 落盘到 /usr/local/openresty/nginx/logs/bodies/YYYY-MM-DD.jsonl（BODYLOG_DIR 可覆盖）
//   - 每天 0:00 切日，旧文件 gzip + 14 天滚动删除
//   - SIGTERM/SIGINT 优雅退出
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = "9999"
	defaultDir  = "/usr/local/openresty/nginx/logs/bodies"
	maxLine     = 32 * 1024 * 1024 // 单条 entry 最大 32 MB（防御 OOM）
	keepDays    = 14
	readBufSize = 1 << 20 // 1 MB bufio buffer
)

// dayWriter: 单 fd + Mutex 串行 write；切日时关旧开新。
// O_APPEND 在 ext4/xfs 下对 ≤PIPE_BUF (4KB) 是原子的，更大写入仍可能交错，
// 所以用 Mutex 兜底（Go runtime 多 goroutine 并发写时可见）。
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
		// 切日异步触发 housekeep（gzip 大文件可能耗时几秒）
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

// extractText: 通用文本片段抽取，覆盖 OpenAI/vllm/Anthropic 三种 schema
//
//	OpenAI 流式: choices[].delta.content
//	OpenAI 非流: choices[].message.content
//	Anthropic 流式: delta.text
//	Anthropic 非流: content[].text
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

// extractResp: 从一段 SSE wire 或完整 JSON 抽出纯文本。失败返回原文。
func extractResp(s string) string {
	if s == "" {
		return s
	}
	parts := make([]string, 0, 32)
	// 整体当 JSON 试一次（非流式响应）
	var whole map[string]any
	if err := json.Unmarshal([]byte(s), &whole); err == nil {
		extractText(whole, &parts)
		if len(parts) > 0 {
			return strings.Join(parts, "")
		}
	}
	// SSE: 按行扫 data: 前缀
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

// processLine: 解析 entry → 抽取 resp_body → 重新序列化。失败原样透传。
func processLine(line []byte) []byte {
	// 去掉尾部 \n 再 unmarshal
	trimmed := bytes.TrimRight(line, "\n")
	var e map[string]any
	if err := json.Unmarshal(trimmed, &e); err != nil {
		return line // 不是合法 JSON，原样写
	}
	rb, _ := e["resp_body"].(string)
	if rb != "" {
		if b64, _ := e["resp_body_b64"].(bool); b64 {
			if raw, err := base64.StdEncoding.DecodeString(rb); err == nil {
				e["resp_body"] = extractResp(string(raw))
				// 抽取出来的多半是干净 utf-8，去掉 b64 标记
				e["resp_body_b64"] = false
			}
		} else {
			e["resp_body"] = extractResp(rb)
		}
	}
	out, err := json.Marshal(e)
	if err != nil {
		return line
	}
	return append(out, '\n')
}

// readCappedLine: 读到 \n，超过 cap 字节丢弃整行（消化到下一个 \n）。
// 返回 (line, dropped, err)：dropped=true 表示这一行超长被丢弃，应继续下一行。
func readCappedLine(r *bufio.Reader, cap int) ([]byte, bool, error) {
	line, err := r.ReadSlice('\n')
	if err == nil {
		// ReadSlice 返回的 slice 在下次 read 时会被复用，必须 copy
		out := make([]byte, len(line))
		copy(out, line)
		return out, false, nil
	}
	if errors.Is(err, bufio.ErrBufferFull) {
		// 行长 > buffer，逐段读到 \n 为止，统计大小
		total := len(line)
		for {
			seg, err2 := r.ReadSlice('\n')
			total += len(seg)
			if err2 == nil {
				break // 找到 \n 了
			}
			if errors.Is(err2, bufio.ErrBufferFull) {
				if total > cap {
					// 超 cap 还没结束，继续 drain 但不再分配
					continue
				}
				continue
			}
			return nil, true, err2
		}
		log.Printf("oversized line dropped (~%d B > %d cap)", total, cap)
		return nil, true, nil
	}
	if errors.Is(err, io.EOF) {
		if len(line) > 0 {
			out := make([]byte, len(line)+1)
			copy(out, line)
			out[len(line)] = '\n'
			return out, false, nil
		}
		return nil, false, io.EOF
	}
	return nil, false, err
}

func handleConn(c net.Conn, w *dayWriter) {
	defer c.Close()
	r := bufio.NewReaderSize(c, readBufSize)
	for {
		line, dropped, err := readCappedLine(r, maxLine)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("read err from %s: %v", c.RemoteAddr(), err)
			}
			return
		}
		if dropped {
			continue
		}
		out := processLine(line)
		if !bytes.HasSuffix(out, []byte{'\n'}) {
			out = append(out, '\n')
		}
		if err := w.write(out); err != nil {
			log.Printf("write err: %v", err)
			return
		}
	}
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
				continue // 已存在
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

	// 启动时 housekeep 一次（冷启动覆盖错过的 day-rollover）
	go housekeep(dir)
	// 周期 housekeep（每 6h，覆盖零流量天）
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
