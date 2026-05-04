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
//   - 落盘 BODYLOG_DIR/YYYY-MM-DD/HH.jsonl（按天分目录、按小时切文件）
//   - 跨日时：housekeep 把昨天目录整个 tar.gz → BODYLOG_DIR/YYYY-MM-DD.tar.gz，删原目录
//   - 14 天后删历史 .tar.gz
//   - 兼容迁移：BODYLOG_DIR 根上残留的旧版 .jsonl / .jsonl.gz 沿用旧 housekeep 逻辑
//   - SIGTERM/SIGINT 优雅退出
package main

import (
	"archive/tar"
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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = "9999"
	defaultDir  = "/mnt/nvme0n1/nginx/bodylog"
	maxFrame    = 64 * 1024 * 1024 // 单帧最大 64 MB（防御 OOM；正常 256K input + 2M resp cap 远小于此）
	keepDays    = 14
	readBufSize = 1 << 20
)

// hourWriter: 按小时切的 jsonl 文件写入器。
//
// 路径规则：BODYLOG_DIR/YYYY-MM-DD/HH.jsonl
//   - 跨小时：close 旧 fd，open 同日目录下新 HH.jsonl
//   - 跨日：mkdir 新日目录，async 触发 housekeep 把上一日 tar.gz 归档
//
// 当天内不做压缩，所有小时文件保持 .jsonl 明文，便于实时 tail / grep。
type hourWriter struct {
	mu   sync.Mutex
	day  string // "2026-05-04"
	hour string // "13"
	f    *os.File
	dir  string
}

func (w *hourWriter) write(p []byte) error {
	now := time.Now()
	today := now.Format("2006-01-02")
	thisHour := now.Format("15")
	w.mu.Lock()
	defer w.mu.Unlock()

	dayChanged := w.day != today
	hourChanged := dayChanged || w.hour != thisHour

	if hourChanged {
		if w.f != nil {
			_ = w.f.Close()
		}
		dayDir := filepath.Join(w.dir, today)
		if err := os.MkdirAll(dayDir, 0o755); err != nil {
			return err
		}
		path := filepath.Join(dayDir, thisHour+".jsonl")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		w.f = f
		w.day = today
		w.hour = thisHour
		log.Printf("opened %s", path)
		if dayChanged {
			// 跨日才需要触发 housekeep（昨天目录待打包 + 14 天 cutoff）
			go housekeep(w.dir)
		}
	}
	_, err := w.f.Write(p)
	return err
}

func (w *hourWriter) close() {
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
	reasoning    []string // Kimi-K2.5 / GLM-5 / OpenAI o1 的思考链（message.reasoning / delta.reasoning）
	id           string   // OpenAI chatcmpl-xxx / Anthropic msg_xxx，用于跨日志关联
	finishReason string
	model        string
	usage        any
	toolCalls    []any
	stopReason   string // Anthropic
	errorObj     any
}

// extractFromObj: OpenAI chat / OpenAI completions(legacy) / Anthropic messages 通用抽取
func extractFromObj(obj map[string]any, rf *respFields) {
	if choices, ok := obj["choices"].([]any); ok {
		for _, c := range choices {
			cm, _ := c.(map[string]any)
			if cm == nil {
				continue
			}
			// OpenAI legacy /v1/completions: choices[].text
			if s, ok := cm["text"].(string); ok {
				rf.parts = append(rf.parts, s)
			}
			if d, ok := cm["delta"].(map[string]any); ok {
				if s, ok := d["content"].(string); ok {
					rf.parts = append(rf.parts, s)
				}
				if s, ok := d["reasoning"].(string); ok {
					rf.reasoning = append(rf.reasoning, s)
				}
				if s, ok := d["reasoning_content"].(string); ok {
					rf.reasoning = append(rf.reasoning, s)
				}
				if tc, ok := d["tool_calls"].([]any); ok {
					rf.toolCalls = append(rf.toolCalls, tc...)
				}
			}
			if m, ok := cm["message"].(map[string]any); ok {
				if s, ok := m["content"].(string); ok {
					rf.parts = append(rf.parts, s)
				}
				if s, ok := m["reasoning"].(string); ok {
					rf.reasoning = append(rf.reasoning, s)
				}
				if s, ok := m["reasoning_content"].(string); ok {
					rf.reasoning = append(rf.reasoning, s)
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
	// Anthropic /v1/messages 流式：delta.{text|thinking}
	if d, ok := obj["delta"].(map[string]any); ok {
		if s, ok := d["text"].(string); ok {
			rf.parts = append(rf.parts, s)
		}
		if s, ok := d["thinking"].(string); ok {
			rf.reasoning = append(rf.reasoning, s)
		}
		if sr, ok := d["stop_reason"].(string); ok && sr != "" {
			rf.stopReason = sr
		}
	}
	// Anthropic /v1/messages 流式 content_block_start.content_block 初始内容
	if cb, ok := obj["content_block"].(map[string]any); ok {
		if s, ok := cb["text"].(string); ok && s != "" {
			rf.parts = append(rf.parts, s)
		}
		if s, ok := cb["thinking"].(string); ok && s != "" {
			rf.reasoning = append(rf.reasoning, s)
		}
	}
	// Anthropic /v1/messages 非流：content[] 数组（混合 text/thinking/tool_use 块）
	if content, ok := obj["content"].([]any); ok {
		for _, b := range content {
			bm, _ := b.(map[string]any)
			if bm == nil {
				continue
			}
			if s, ok := bm["text"].(string); ok {
				rf.parts = append(rf.parts, s)
			}
			if s, ok := bm["thinking"].(string); ok {
				rf.reasoning = append(rf.reasoning, s)
			}
			// tool_use 块映射成 OpenAI tool_calls 形态便于统一消费
			if t, _ := bm["type"].(string); t == "tool_use" {
				rf.toolCalls = append(rf.toolCalls, bm)
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
		if len(rf.parts) > 0 || len(rf.reasoning) > 0 || rf.errorObj != nil {
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

// reassembleToolCalls: SSE 流式把同一个 tool_call 切成多个 delta（按 index 共享）。
// 每片只有 name 或 arguments 部分。这里按 index 合并 + arguments 拼接。
// 非流式响应每个 fragment 已经是完整 call，按 index=0 单条直接返回。
func reassembleToolCalls(fragments []any) []any {
	byIdx := map[int]map[string]any{}
	order := []int{}
	nextSynthIdx := -1
	for _, f := range fragments {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		idx := 0
		if v, ok := fm["index"].(float64); ok {
			idx = int(v)
		} else {
			// 非流式响应没有 index 字段，给每条单独 idx 避免误合并
			idx = nextSynthIdx
			nextSynthIdx--
		}
		cur, exists := byIdx[idx]
		if !exists {
			cur = map[string]any{}
			byIdx[idx] = cur
			order = append(order, idx)
		}
		for k, v := range fm {
			if k == "function" {
				continue
			}
			// 后到 fragment 不覆盖已有非空字段（id/type 通常首片就完整）
			if _, has := cur[k]; !has {
				cur[k] = v
			}
		}
		if fn, ok := fm["function"].(map[string]any); ok {
			existing, _ := cur["function"].(map[string]any)
			if existing == nil {
				existing = map[string]any{}
				cur["function"] = existing
			}
			for fk, fv := range fn {
				if fk == "arguments" {
					if s, _ := fv.(string); s != "" {
						prev, _ := existing["arguments"].(string)
						existing["arguments"] = prev + s
					} else if _, has := existing["arguments"]; !has {
						existing["arguments"] = fv
					}
				} else {
					if _, has := existing[fk]; !has {
						existing[fk] = fv
					}
				}
			}
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	out := make([]any, 0, len(order))
	for _, idx := range order {
		out = append(out, byIdx[idx])
	}
	return out
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
		m["tool_calls"] = reassembleToolCalls(rf.toolCalls)
	}
	if len(rf.reasoning) > 0 {
		m["reasoning"] = strings.Join(rf.reasoning, "")
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

func handleConn(c net.Conn, w *hourWriter) {
	defer c.Close()
	// 从 TCP RemoteAddr 取源 IP（去掉 :port）。loopback 写 "127.0.0.1"，
	// LAN/跨机时是发送方 OpenResty 主机 IP；落盘到 entry.source_addr 字段，
	// 用于多 OpenResty 共用同一 listener 时区分上游来源。
	srcAddr := c.RemoteAddr().String()
	if h, _, err := net.SplitHostPort(srcAddr); err == nil {
		srcAddr = h
	}
	r := bufio.NewReaderSize(c, readBufSize)
	for {
		meta, req, resp, err := readFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("read err from %s: %v", c.RemoteAddr(), err)
			}
			return
		}
		out, err := assembleEntry(meta, req, resp, srcAddr)
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
func assembleEntry(metaJSON, req, resp []byte, sourceAddr string) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(metaJSON, &m); err != nil {
		return nil, fmt.Errorf("meta unmarshal: %w", err)
	}
	// 源地址（OpenResty 主机 IP，由 TCP 连接 RemoteAddr 注入；listener 端权威，
	// 不依赖发送方 meta，防止伪造）
	if sourceAddr != "" {
		m["source_addr"] = sourceAddr
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
			if content == "" && len(rf.reasoning) == 0 && rf.errorObj == nil && rf.finishReason == "" {
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

// housekeep 扫描 BODYLOG_DIR，处理：
//   1. YYYY-MM-DD/ 目录（非今日）→ tar.gz 整个目录 → 删原目录
//   2. YYYY-MM-DD.tar.gz（≥ 14 天）→ 删
//   3. .tar.gz.tmp 残留（上次崩溃没完成）→ 删
//   4. （兼容）旧版 X.jsonl 在根目录 → gzip → 删原文件
//   5. （兼容）旧版 X.jsonl.gz 在根目录（≥ 14 天）→ 删
//
// 多次并发触发是安全的：每个目标都先 Stat 检查 dst 是否已存在再处理。
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
		full := filepath.Join(dir, name)
		switch {
		case e.IsDir() && isDateName(name):
			if name == today {
				continue // 今日目录还在写
			}
			dst := filepath.Join(dir, name+".tar.gz")
			if _, err := os.Stat(dst); err == nil {
				// .tar.gz 已存在但目录还没删（上次 housekeep 半路失败），删目录就行
				if err := os.RemoveAll(full); err != nil {
					log.Printf("housekeep remove stale dir %s: %v", full, err)
				} else {
					log.Printf("removed stale dir (tar.gz exists): %s", full)
				}
				continue
			}
			if err := tarGzDir(full, dst); err != nil {
				log.Printf("tar.gz %s failed: %v", full, err)
				continue
			}
			if err := os.RemoveAll(full); err != nil {
				log.Printf("housekeep remove %s: %v", full, err)
				continue
			}
			log.Printf("archived %s → %s", full, dst)

		case !e.IsDir() && strings.HasSuffix(name, ".tar.gz"):
			base := strings.TrimSuffix(name, ".tar.gz")
			if isDateName(base) && base < cutoff {
				_ = os.Remove(full)
				log.Printf("removed old %s", name)
			}

		case !e.IsDir() && strings.HasSuffix(name, ".tar.gz.tmp"):
			// 上次崩溃残留：直接删，下次 housekeep 会重新打包对应目录
			_ = os.Remove(full)
			log.Printf("removed crashed temp %s", name)

		// ---- 向后兼容旧版（YYYY-MM-DD.jsonl 单日单文件模式）----
		case !e.IsDir() && strings.HasSuffix(name, ".jsonl"):
			base := strings.TrimSuffix(name, ".jsonl")
			if base == today || !isDateName(base) {
				continue
			}
			dst := full + ".gz"
			if _, err := os.Stat(dst); err == nil {
				continue
			}
			if err := gzipFile(full, dst); err != nil {
				log.Printf("gzip %s failed: %v", full, err)
				continue
			}
			_ = os.Remove(full)
			log.Printf("compressed legacy %s → %s", full, dst)

		case !e.IsDir() && strings.HasSuffix(name, ".jsonl.gz"):
			base := strings.TrimSuffix(name, ".jsonl.gz")
			if isDateName(base) && base < cutoff {
				_ = os.Remove(full)
				log.Printf("removed legacy old %s", name)
			}
		}
	}
}

// isDateName 判断字符串是不是 YYYY-MM-DD 形式（最低粒度，不严格校验日期合法性）
func isDateName(s string) bool {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' {
		return false
	}
	for i, c := range s {
		if i == 4 || i == 7 {
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// tarGzDir 打包 srcDir → dst（先写 dst.tmp 再原子 rename，crash 时不会留半截 .tar.gz）
func tarGzDir(srcDir, dst string) error {
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
		// 失败时清掉 .tmp（成功路径上已经 rename 走了，不影响）
		_ = os.Remove(tmp)
	}()
	gw, _ := gzip.NewWriterLevel(out, 6)
	tw := tar.NewWriter(gw)

	parent := filepath.Dir(srcDir)
	walkErr := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(parent, path)
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if walkErr != nil {
		return walkErr
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
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

	w := &hourWriter{dir: dir}

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
