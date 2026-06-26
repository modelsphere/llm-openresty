// bodylog-listener: 接收 openresty 通过 lua-resty-logger-socket 发来的
// 长度前缀二进制帧，解析后落盘成抽取过 SSE content 的 JSONL。
//
// 二进制帧（与 session_route.conf 里的 bodylog_finalize 保持同步，big-endian）：
//
//	[u32 total_len][u16 meta_len][meta_json][u32 req_len][req_bytes][u32 resp_len][resp_bytes]
//
// total_len 不含开头 4 字节自身。openresty 端只 cjson.encode 小 meta，
// req_body / resp_body 作为裸字节传输，跳过大字符串 escape 扫描（实测能省 5-8ms / entry）。
//
// 接口：
//   - 监听 TCP 9999（BODYLOG_HOST / BODYLOG_PORT 可覆盖）
//   - 落盘 BODYLOG_DIR/YYYY-MM-DD/HH.jsonl（按天分目录、按小时切文件）
//   - 跨日时：housekeep 把昨天目录整个 tar.gz → BODYLOG_DIR/YYYY-MM-DD.tar.gz，删原目录
//   - keepDays 天后删历史 .tar.gz（默认 90，BODYLOG_KEEP_DAYS 可覆盖）
//   - 兼容迁移：BODYLOG_DIR 根上残留的旧版 .jsonl / .jsonl.gz 沿用旧 housekeep 逻辑
//   - SIGTERM/SIGINT 优雅退出
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	_ "github.com/marcboeker/go-duckdb"
)

const (
	defaultHost            = "127.0.0.1"
	defaultPort            = "9999"
	defaultDir             = "/mnt/nvme0n1/nginx/bodylog"
	maxFrame               = 64 * 1024 * 1024 // 单帧最大 64 MB（防御 OOM；正常 256K input + 2M resp cap 远小于此）
	defaultKeepDays        = 90               // 历史 .tar.gz 默认保留天数，可被 BODYLOG_KEEP_DAYS 覆盖
	defaultDetailsKeepDays = 365              // metrics 明细 parquet 保留天数，可被 BODYLOG_DETAILS_KEEP_DAYS 覆盖
	metricsMaxRows         = 5000             // /metrics 单次返回硬上限（≈2.5MB JSON）；截断时回 returned_to 供按 ts 续取
	readBufSize            = 1 << 20
)

// keepDays: 历史归档保留天数。默认 defaultKeepDays，main() 启动时从 BODYLOG_KEEP_DAYS 覆盖。
var keepDays = defaultKeepDays

// detailsKeepDays: metrics 明细 parquet 保留天数。main() 从 BODYLOG_DETAILS_KEEP_DAYS 覆盖。
var detailsKeepDays = defaultDetailsKeepDays

// duckDB: 进程内 in-memory DuckDB（go-duckdb / CGO）。用于 metrics/details 的
// 每日 jsonl→parquet 转换 与 /metrics 接口的混读查询。main() 启动时 sql.Open；
// 若打开失败则保持 nil，相关功能优雅降级（/metrics 返回 503，rotate 跳过）。
var duckDB *sql.DB

// httpToken: /summary 与 /metrics 的鉴权 token（BODYLOG_HTTP_TOKEN）。
// 空="" → 不鉴权（opt-in，向后兼容：不设就跟以前一样开放）。非空 → 两接口要求
// Authorization: Bearer <token> 或 ?token=<token>。/healthz 永远开放。
// ⚠️ 启用后，调用方(monitor 的 /summary 拉取)必须带上同一个 token，否则 401。
var httpToken string

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
			// 跨日才需要触发 housekeep（昨天目录待打包 + keepDays cutoff）
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

// detailRecord: 每请求 metrics 明细（剥掉所有正文：req_body / resp_body / req_headers /
// resp_meta.reasoning / resp_meta.tool_calls）。~400B/行，按天写 metrics/details/<date>.jsonl，
// 跨天封口后由 housekeep 转 parquet。/metrics 接口查的就是这份。
type detailRecord struct {
	Ts               string  `json:"ts"`                // 请求开始时刻
	TsEnd            string  `json:"ts_end,omitempty"`  // 请求结束时刻 = ts + rt（聚合分桶用此）
	RequestID        string  `json:"request_id,omitempty"`
	SourceAddr       string  `json:"source_addr,omitempty"`
	URI              string  `json:"uri,omitempty"`
	Method           string  `json:"method,omitempty"`
	Stream           *bool   `json:"stream,omitempty"` // 请求参数 stream，从 req_body 提取（null=无法判定）
	Status           int64   `json:"status"`
	Peer             string  `json:"peer,omitempty"`         // openresty 选中的上游（可能是 router 中间层）
	ForwardedTo      string  `json:"forwarded_to,omitempty"` // 原始 X-Routed-Peer URL
	Backend          string  `json:"backend"`                // 归一化后的真实后端 host:port（= 聚合用的 peer key）
	Mode             string  `json:"mode,omitempty"`
	SessionSrc       string  `json:"session_src,omitempty"`
	Model            string  `json:"model,omitempty"`
	FinishReason     string  `json:"finish_reason,omitempty"`
	Frt              float64 `json:"frt"` // first_chunk_t（≈ TTFT，流式首 token）
	Lct              float64 `json:"lct"` // last_chunk_t
	Rt               float64 `json:"rt"`
	ChunkCount       int64   `json:"chunk_count"`
	ReqBytes         int64   `json:"req_bytes"`
	RespBytes        int64   `json:"resp_bytes"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	CachedTokens     int64   `json:"cached_tokens"`
	TotalTokens      int64   `json:"total_tokens"`
	ReasoningTokens  int64   `json:"reasoning_tokens"`
}

// extractDetail: 从 assembleEntry 解析好的 map 里抽出 metrics 明细，不带任何正文。
// 复用 normalizePeer / extractUsage，与 aggregator.ingest 口径一致。
func extractDetail(m map[string]any) detailRecord {
	u := extractUsage(m)
	d := detailRecord{
		Ts:               getString(m["ts"]),
		TsEnd:            getString(m["ts_end"]),
		RequestID:        getString(m["request_id"]),
		SourceAddr:       getString(m["source_addr"]),
		URI:              getString(m["uri"]),
		Method:           getString(m["method"]),
		Stream:           streamOf(m),
		Status:           getInt64(m["status"]),
		Peer:             getString(m["peer"]),
		ForwardedTo:      getString(m["forwarded_to"]),
		Backend:          normalizePeer(m),
		Mode:             getString(m["mode"]),
		SessionSrc:       getString(m["session_src"]),
		Frt:              getFloat(m["first_chunk_t"]),
		Lct:              getFloat(m["last_chunk_t"]),
		Rt:               getFloat(m["rt"]),
		ChunkCount:       getInt64(m["chunk_count"]),
		ReqBytes:         int64(len(getString(m["req_body"]))),
		RespBytes:        getInt64(m["resp_bytes"]),
		PromptTokens:     u.prompt,
		CompletionTokens: u.compl,
		CachedTokens:     u.cached,
		TotalTokens:      u.total,
		ReasoningTokens:  u.reasoning,
	}
	if rm, ok := m["resp_meta"].(map[string]any); ok {
		d.Model = getString(rm["model"])
		d.FinishReason = getString(rm["finish_reason"])
	}
	if d.RespBytes == 0 {
		// openresty 未带 resp_bytes 时兜底用抽取后的 resp_body 长度
		d.RespBytes = int64(len(getString(m["resp_body"])))
	}
	return d
}

// detailsWriter: 按天切的 metrics 明细 jsonl 写入器。
// 路径：BODYLOG_DIR/metrics/details/<date>.jsonl（与分钟聚合 metrics/<date>.jsonl 分子目录，
// 避免被同一 glob 误读）。跨天 close 旧 fd、open 新日文件；当天保持明文便于 tail。
type detailsWriter struct {
	mu  sync.Mutex
	day string
	f   *os.File
	dir string // BODYLOG_DIR
}

func (w *detailsWriter) write(d detailRecord) error {
	today := time.Now().Format("2006-01-02")
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.day != today {
		if w.f != nil {
			_ = w.f.Close()
		}
		ddir := filepath.Join(w.dir, "metrics", "details")
		if err := os.MkdirAll(ddir, 0o755); err != nil {
			return err
		}
		path := filepath.Join(ddir, today+".jsonl")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		w.f = f
		w.day = today
		log.Printf("opened %s", path)
	}
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	// 单次 Write（<4KB append 在本地 fs 上原子，不会写出半行 → DuckDB 读 live jsonl 不会读到撕裂行）
	_, err = w.f.Write(b)
	return err
}

func (w *detailsWriter) close() {
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

func handleConn(c net.Conn, w *hourWriter, dw *detailsWriter, agg *aggregator) {
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
		m, out, err := assembleEntry(meta, req, resp, srcAddr)
		if err != nil {
			log.Printf("assemble err: %v", err)
			continue
		}
		if err := w.write(out); err != nil {
			log.Printf("write err: %v", err)
			return
		}
		// 预算 stream(扫一次 req_body),缓存进 m 供 ingest(分桶)与 extractDetail(明细)复用。
		// 在 out 落盘之后做,不影响全量 bodylog（__stream 不入 out）。
		m["__stream"] = extractStream(m)
		// 同步 agg.ingest：O(1) 加锁更新一个 bucket，纳秒级，不影响主路径吞吐
		if agg != nil {
			agg.ingest(m)
		}
		// per-request metrics 明细（剥正文，~400B）：旁挂写，失败只告警不影响主落盘
		if dw != nil {
			if err := dw.write(extractDetail(m)); err != nil {
				log.Printf("details write err: %v", err)
			}
		}
	}
}

// assembleEntry: 合并 meta_json + req(utf-8 检查/base64) + resp(SSE 抽取) → 单行 JSONL
//
// 返回 (parsed map, jsonl bytes, err)。map 用于 aggregator.ingest 避免重复
// JSON 解析；bytes 用于 hourWriter.write 落盘。
func assembleEntry(metaJSON, req, resp []byte, sourceAddr string) (map[string]any, []byte, error) {
	var m map[string]any
	if err := json.Unmarshal(metaJSON, &m); err != nil {
		return nil, nil, fmt.Errorf("meta unmarshal: %w", err)
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
	// ts_end = ts + rt（请求结束时刻）。落盘冗余一份，便于按完成时间复盘/对账，
	// 也与 ingest 的结束时刻分桶口径（parseEntryMinute）保持一致。
	if end, ok := entryEndTime(m); ok {
		m["ts_end"] = end.Format(tsLayout)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil, nil, fmt.Errorf("entry marshal: %w", err)
	}
	return m, append(out, '\n'), nil
}

// housekeep 扫描 BODYLOG_DIR，处理：
//  1. YYYY-MM-DD/ 目录（非今日）→ tar.gz 整个目录 → 删原目录
//  2. YYYY-MM-DD.tar.gz（≥ keepDays 天）→ 删
//  3. .tar.gz.tmp 残留（上次崩溃没完成）→ 删
//  4. （兼容）旧版 X.jsonl 在根目录 → gzip → 删原文件
//  5. （兼容）旧版 X.jsonl.gz 在根目录（≥ keepDays 天）→ 删
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

	// metrics 明细：把封口的 <date>.jsonl 转 parquet + 清过期 parquet（与 housekeep 同节奏）
	rotateDetails(dir)
}

// rotateDetails 处理 BODYLOG_DIR/metrics/details/：
//  1. 非今日且无同名 .parquet 的 <date>.jsonl → DuckDB COPY 转 zstd parquet，成功后删 jsonl
//  2. <date>.parquet 已存在但 jsonl 还在（上次崩溃）→ 删多余 jsonl
//  3. 超过 detailsKeepDays 的 .parquet → 删
//
// duckDB 为 nil（go-duckdb 打开失败）时整体跳过：当天 jsonl 仍在写、不丢数据，
// 只是不转 parquet，/metrics 也会因 duckDB nil 而 503。
func rotateDetails(dir string) {
	if duckDB == nil {
		return
	}
	ddir := filepath.Join(dir, "metrics", "details")
	entries, err := os.ReadDir(ddir)
	if err != nil {
		return // 目录还没建（无明细产生），正常
	}
	today := time.Now().Format("2006-01-02")
	cutoff := time.Now().AddDate(0, 0, -detailsKeepDays).Format("2006-01-02")
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		switch {
		case strings.HasSuffix(name, ".jsonl"):
			date := strings.TrimSuffix(name, ".jsonl")
			if !isDateName(date) || date == today {
				continue
			}
			jsonlPath := filepath.Join(ddir, name)
			parquetPath := filepath.Join(ddir, date+".parquet")
			if _, err := os.Stat(parquetPath); err == nil {
				_ = os.Remove(jsonlPath) // parquet 已在，清残留 jsonl
				continue
			}
			q := fmt.Sprintf(
				"COPY (SELECT * FROM %s) TO %s (FORMAT parquet, COMPRESSION zstd)",
				readDetailsJSON(sqlStr(jsonlPath)), sqlStr(parquetPath))
			if _, err := duckDB.Exec(q); err != nil {
				log.Printf("details parquet convert %s failed: %v", name, err)
				_ = os.Remove(parquetPath) // 删可能的半截输出，下轮重试
				continue
			}
			_ = os.Remove(jsonlPath)
			log.Printf("details rotated %s → %s.parquet", name, date)

		case strings.HasSuffix(name, ".parquet"):
			date := strings.TrimSuffix(name, ".parquet")
			if isDateName(date) && date < cutoff {
				_ = os.Remove(filepath.Join(ddir, name))
				log.Printf("details removed old %s", name)
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

// ─── per-minute 聚合器（按 peer 分组）────────────────────────────────────
//
// 每帧 entry 经 handleConn → assembleEntry 后调用 agg.ingest，按【结束时刻
// ts+rt】(parseEntryMinute) 对齐到 minute、按 peer 分桶累计 token / 字节 /
// 状态码 / 延迟。按结束时刻分桶 → 桶分钟 == ingest 分钟 → 无 rt 长尾迟到漏算。
//
// 周期 flush（60s ticker）把 minute < now-90s 的 bucket 移到 archive 并
// append 到 metrics/YYYY-MM-DD.jsonl（明文不压缩，永久保留）。
//
// HTTP server（默认 :9998）暴露 /summary?minutes=N，返回 N 分钟内 per-peer
// aggregate（已合并所有分钟），可选 ?breakdown=true 返回每分钟 buckets。

type bucket struct {
	Minute int64  `json:"minute"`
	Peer   string `json:"peer"`
	// Stream: "true"/"false"/"" —— 按请求参数 stream 分桶。空="未知"(req_body 缺/base64/未传)。
	// metrics/<date>.jsonl 因此每 (minute,peer) 可有多行(不同 stream);reload 按
	// (minute,peer,stream) 合并。/summary 默认跨 stream 合回 (minute,peer)(monitor 兼容),
	// ?stream=true/false 时按此过滤。
	Stream    string `json:"stream,omitempty"`
	Requests  int64  `json:"requests"`
	Status2xx int64  `json:"status_2xx"`
	Status4xx int64  `json:"status_4xx"`
	Status5xx int64  `json:"status_5xx"`
	PromptTok int64  `json:"prompt_tok"`
	CachedTok int64  `json:"cached_tok"` // prompt_tokens_details.cached_tokens（vllm prefix cache 命中）
	ComplTok  int64  `json:"compl_tok"`
	TotalTok  int64  `json:"total_tok"`
	ReqBytes  int64  `json:"req_bytes"`
	RespBytes int64  `json:"resp_bytes"`
	RTSumMs   int64  `json:"rt_sum_ms"`
	RTMaxMs   int64  `json:"rt_max_ms"`
	// first_chunk_t = ngx.now() - ngx.req.start_time()（openresty body_filter
	// 收到第一个响应 chunk 时记录），流式响应里约等于 TTFT（首 token 到达）；
	// 非流式响应里等于完整 rt。每分钟 Sum/Max 聚合（N 用 Status2xx 近似不另存：
	// 只有 2xx 成功才有 first_chunk_t，OpenResty Lua 那侧已经过滤了 nil 值）。
	FrtSumMs int64 `json:"frt_sum_ms"`
	FrtMaxMs int64 `json:"frt_max_ms"`
	FrtN     int64 `json:"frt_n"` // 实际带 first_chunk_t 的请求数（兼容老数据用，可能 < Status2xx）
}

type aggregator struct {
	mu                 sync.RWMutex
	active             map[int64]map[string]*bucket // minute → peer → bucket（当前 active）
	archive            []*bucket                    // 已 flush 的 closed buckets，按 Minute 升序（指针，迟到请求可原地累加）
	archiveIdx         map[int64]map[string]*bucket // archive 的 (minute, peer) 索引，O(1) 查找；与 archive slice 共享 bucket 指针
	archiveDirty       map[*bucket]struct{}         // 上次 flush 之后被 ingest 修改过的 archive buckets
	archiveLastWritten map[*bucket]bucket           // archive bucket 上次写盘时的快照，用于算 delta
	dir                string                       // BODYLOG_DIR
	archiveKeepMin     int                          // 内存最多保留的分钟数（按 archive 中最老 minute 算）
}

func newAggregator(dir string, keepMin int) *aggregator {
	return &aggregator{
		active:             map[int64]map[string]*bucket{},
		archiveIdx:         map[int64]map[string]*bucket{},
		archiveDirty:       map[*bucket]struct{}{},
		archiveLastWritten: map[*bucket]bucket{},
		dir:                dir,
		archiveKeepMin:     keepMin,
	}
}

// ts 是 openresty bodylog_finalize 写的 ISO8601 + 北京时区，毫秒精度，例：
// "2026-05-04T10:30:00.123+08:00"。ts 是请求开始时刻；结束时刻 = ts + rt(秒)。
const tsLayout = "2006-01-02T15:04:05.000-07:00"

// entryEndTime 返回请求结束时刻 = ts(开始) + rt(秒)。ts 缺失/解析失败返回 (零值,false)。
// listener 在请求结束时才收到帧并 ingest，所以按结束时刻分桶能保证「桶的分钟 ==
// ingest 的分钟」：一个桶在它自己那一分钟过去后就再也收不到新数据，下游(monitor)
// 落盘冻结时桶已收齐——根除「rt 超过 persist 延迟的长请求在开始分钟桶被冻结后才
// 完成、token 永远补不进去」的漏算（按开始时间 ts 分桶时 ingest 分钟比桶分钟晚 rt）。
func entryEndTime(m map[string]any) (time.Time, bool) {
	ts, ok := m["ts"].(string)
	if !ok || ts == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(tsLayout, ts)
	if err != nil {
		return time.Time{}, false
	}
	if rt, ok := m["rt"].(float64); ok && rt > 0 {
		t = t.Add(time.Duration(rt * float64(time.Second)))
	}
	return t, true
}

// parseEntryMinute 把 entry 对齐到分钟桶，按【结束时刻】。直接读 assembleEntry 已
// 算好并落盘的 m["ts_end"](= ts+rt，见 entryEndTime)，不重复计算。
func parseEntryMinute(m map[string]any) int64 {
	if tsEnd, ok := m["ts_end"].(string); ok && tsEnd != "" {
		if t, err := time.Parse(tsLayout, tsEnd); err == nil {
			return t.Unix() - t.Unix()%60
		}
	}
	now := time.Now().Unix()
	return now - now%60
}

func getInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}

func getString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func getFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return 0
}

// normalizePeer: 优先用 forwarded_to（router 回的真实后端）聚合，否则用 openresty 选中的 peer。
// 去掉 http(s):// 前缀、剥 path，留 host:port；空则 "(none)"。aggregator.ingest 与
// extractDetail 共用，保证明细的 backend 字段与分钟聚合的 peer key 口径一致。
func normalizePeer(m map[string]any) string {
	peer := getString(m["peer"])
	if fwd := getString(m["forwarded_to"]); fwd != "" {
		fwd = strings.TrimPrefix(fwd, "https://")
		fwd = strings.TrimPrefix(fwd, "http://")
		if i := strings.IndexByte(fwd, '/'); i >= 0 {
			fwd = fwd[:i]
		}
		if fwd != "" {
			peer = fwd
		}
	}
	if peer == "" {
		peer = "(none)"
	}
	return peer
}

type usageTokens struct {
	prompt, cached, compl, total, reasoning int64
}

// extractUsage: 从 resp_meta.usage 抽 token 计数，兼容 OpenAI(prompt/completion_tokens)
// 与 Anthropic(input/output_tokens)；cached 取 prompt_tokens_details.cached_tokens，
// reasoning 取 completion_tokens_details.reasoning_tokens，取不到再回退 usage 顶层
// reasoning_tokens（sglang Kimi 放顶层）。
func extractUsage(m map[string]any) usageTokens {
	var u usageTokens
	rm, ok := m["resp_meta"].(map[string]any)
	if !ok {
		return u
	}
	us, ok := rm["usage"].(map[string]any)
	if !ok {
		return u
	}
	u.prompt = getInt64(us["prompt_tokens"])
	u.compl = getInt64(us["completion_tokens"])
	u.total = getInt64(us["total_tokens"])
	if u.prompt == 0 {
		u.prompt = getInt64(us["input_tokens"])
	}
	if u.compl == 0 {
		u.compl = getInt64(us["output_tokens"])
	}
	if u.total == 0 {
		u.total = u.prompt + u.compl
	}
	if d, ok := us["prompt_tokens_details"].(map[string]any); ok {
		u.cached = getInt64(d["cached_tokens"])
	}
	if d, ok := us["completion_tokens_details"].(map[string]any); ok {
		u.reasoning = getInt64(d["reasoning_tokens"])
	}
	// 兜底：部分后端（sglang Kimi-K2.x）把 reasoning_tokens 放在 usage 顶层，而非
	// completion_tokens_details 子对象里（实测 resp_meta.usage 形如
	// {"completion_tokens":N,"reasoning_tokens":M,"total_tokens":...}，无 *_details）。
	// 上面取不到时回退读顶层，避免明细 reasoning_tokens 恒为 0。
	if u.reasoning == 0 {
		u.reasoning = getInt64(us["reasoning_tokens"])
	}
	return u
}

// streamRe 匹配 req_body 里的 "stream":true/false。要求 key 前是 { 或 ,（JSON 对象
// key 位置）→ 避开 "stream_options" 以及正文里转义的 \"stream\"（前缀是 \ 不是 {/,）。
var streamRe = regexp.MustCompile(`[{,]\s*"stream"\s*:\s*(true|false)`)

// extractStream 从 req_body 提取请求参数 stream。req_body 为空 / base64（非 utf8）/
// 找不到时返回 nil（明细里该字段省略=null）。用定向正则而非全量 json.Unmarshal，
// 避免为一个布尔解析整个可能上 MB 的请求体。
func extractStream(m map[string]any) *bool {
	if b, _ := m["req_body_b64"].(bool); b {
		return nil
	}
	body := getString(m["req_body"])
	if body == "" {
		return nil
	}
	mt := streamRe.FindStringSubmatch(body)
	if mt == nil {
		return nil
	}
	v := mt[1] == "true"
	return &v
}

// streamOf 取请求的 stream(*bool)。handleConn 会预先把 extractStream 结果缓存进
// m["__stream"](避免 ingest 与 extractDetail 各扫一遍 req_body);没缓存时现算兜底。
func streamOf(m map[string]any) *bool {
	if v, ok := m["__stream"]; ok {
		if b, ok := v.(*bool); ok {
			return b
		}
		return nil
	}
	return extractStream(m)
}

// streamKeyOf: *bool → 分桶 key 字符串。nil→""(未知),true→"true",false→"false"。
func streamKeyOf(sp *bool) string {
	if sp == nil {
		return ""
	}
	if *sp {
		return "true"
	}
	return "false"
}

// bkey: aggregator 内层 map 的 (peer, stream) 复合键。用 \x00 分隔(peer/stream 都不含)。
func bkey(peer, stream string) string { return peer + "\x00" + stream }

// sqlStr: DuckDB SQL 字符串字面量转义（单引号翻倍）。用于把文件路径安全拼进
// read_json/read_parquet/COPY 的 SQL（路径来自日期/配置，仍统一转义防御）。
func sqlStr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// detailsColumns: 明细各列的显式 DuckDB schema。固定类型（避免依赖 read_json_auto
// 的数据相关推断）→ 保证 rotateDetails 写出的 parquet 与 metricsHandler 读的 live jsonl
// 列类型完全一致，UNION ALL BY NAME 不会因两边类型分歧报错。
//
// 踩坑（e2e 抓到）：32 位十六进制 request_id 被某文件的 read_json_auto 推断成 HUGEINT，
// 跨「parquet + jsonl」合并时把另一文件的字符串 id 往 INT128 转 → "Could not convert
// string ... to INT128"。显式 VARCHAR 根治。
const detailsColumns = "{" +
	"'ts':'VARCHAR','ts_end':'VARCHAR','request_id':'VARCHAR','source_addr':'VARCHAR','uri':'VARCHAR'," +
	"'method':'VARCHAR','stream':'BOOLEAN','status':'BIGINT','peer':'VARCHAR','forwarded_to':'VARCHAR'," +
	"'backend':'VARCHAR','mode':'VARCHAR','session_src':'VARCHAR','model':'VARCHAR'," +
	"'finish_reason':'VARCHAR','frt':'DOUBLE','lct':'DOUBLE','rt':'DOUBLE'," +
	"'chunk_count':'BIGINT','req_bytes':'BIGINT','resp_bytes':'BIGINT'," +
	"'prompt_tokens':'BIGINT','completion_tokens':'BIGINT','cached_tokens':'BIGINT'," +
	"'total_tokens':'BIGINT','reasoning_tokens':'BIGINT'}"

// readDetailsJSON: 用显式 schema 读明细 jsonl 的 DuckDB 表达式。arg 是单个 sqlStr 路径
// 或文件名列表（read_json 两者都接受）。rotateDetails 与 metricsHandler 共用，保证写/读
// 口径一致。NDJSON（一行一条）→ format='newline_delimited'。
func readDetailsJSON(arg string) string {
	return "read_json(" + arg + ", columns=" + detailsColumns + ", format='newline_delimited')"
}

func (a *aggregator) ingest(m map[string]any) {
	if a == nil {
		return
	}
	minute := parseEntryMinute(m)
	// router 场景：openresty 选中的 peer 是 router_ip:port（中间层），router 在响应里回
	// X-Routed-Peer 标识真实后端 vllm（写到 forwarded_to）。normalizePeer 优先按真实后端聚合，
	// 让 monitor 的 per-peer TPM 不被 router 塌缩成一行。
	peer := normalizePeer(m)
	streamKey := streamKeyOf(streamOf(m))
	pkey := bkey(peer, streamKey) // 内层 map 按 (peer, stream) 分桶
	status := int(getInt64(m["status"]))

	u := extractUsage(m)
	prompt, cached, compl, total := u.prompt, u.cached, u.compl, u.total
	reqBytes := int64(len(getString(m["req_body"])))
	respBytes := int64(len(getString(m["resp_body"])))
	var rtMs int64
	if v, ok := m["rt"].(float64); ok {
		rtMs = int64(v * 1000)
	}
	// first_chunk_t（≈ TTFT for streaming responses，全 rt for 非流式）
	var frtMs int64
	hasFrt := false
	if v, ok := m["first_chunk_t"].(float64); ok && v > 0 {
		frtMs = int64(v * 1000)
		hasFrt = true
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// 选 bucket：优先 active；其次 archive（迟到请求 rt > 90s）；最后兜底新建 active
	// 这样同 (minute, peer) 永远只有一个 bucket，不会因为 minute 已被 flush 就重建副本
	var b *bucket
	if bk, ok := a.active[minute]; ok {
		b = bk[pkey]
		if b == nil {
			b = &bucket{Minute: minute, Peer: peer, Stream: streamKey}
			bk[pkey] = b
		}
	} else if bkA, ok := a.archiveIdx[minute]; ok {
		b = bkA[pkey]
		if b == nil {
			// archive 有这个 minute（其它 peer/stream），但没这个 (peer,stream)：新增到 archive
			b = &bucket{Minute: minute, Peer: peer, Stream: streamKey}
			bkA[pkey] = b
			// 二分插入保持 archive 升序（避免 O(N log N) 的全量 sort）
			idx := sort.Search(len(a.archive), func(i int) bool { return a.archive[i].Minute >= minute })
			a.archive = append(a.archive, nil)
			copy(a.archive[idx+1:], a.archive[idx:])
			a.archive[idx] = b
		}
		// 标记 dirty：下次 flush 时把累加后的快照再写一行到 metrics 文件，
		// reload 会按 (minute, peer, stream) 合并，保证重启后数据完整。
		a.archiveDirty[b] = struct{}{}
	} else {
		// 既不在 active 也不在 archive（新 minute，或 minute 已超出 archiveKeepMin）
		bk := map[string]*bucket{}
		b = &bucket{Minute: minute, Peer: peer, Stream: streamKey}
		bk[pkey] = b
		a.active[minute] = bk
	}
	b.Requests++
	switch {
	case status >= 200 && status < 300:
		b.Status2xx++
	case status >= 400 && status < 500:
		b.Status4xx++
	case status >= 500:
		b.Status5xx++
	}
	b.PromptTok += prompt
	b.CachedTok += cached
	b.ComplTok += compl
	b.TotalTok += total
	b.ReqBytes += reqBytes
	b.RespBytes += respBytes
	b.RTSumMs += rtMs
	if rtMs > b.RTMaxMs {
		b.RTMaxMs = rtMs
	}
	if hasFrt {
		b.FrtSumMs += frtMs
		b.FrtN++
		if frtMs > b.FrtMaxMs {
			b.FrtMaxMs = frtMs
		}
	}
}

// flushClosedMinutes 把 minute < now-90s 的 bucket 从 active 移到 archive，
// 并 append 写到 metrics/YYYY-MM-DD.jsonl（明文，每行一个 bucket）。
// 90s 窗口容忍：跨日 / 异常 / 迟到 entry。
//
// 注意：archive 存指针，archiveIdx 是 (minute, peer) → 同一指针 的索引。
// 迟到请求（rt > 90s）的 ingest 会通过 archiveIdx 找到 archive 里的 bucket
// 原地累加，避免重复 (minute, peer) bucket。
func (a *aggregator) flushClosedMinutes() {
	cutoff := time.Now().Unix() - 90

	a.mu.Lock()
	var closed []bucket // 写盘用 value copy 快照
	var newPtrs []*bucket
	newPtrSet := map[*bucket]struct{}{}
	for m, bk := range a.active {
		if m < cutoff {
			if a.archiveIdx[m] == nil {
				a.archiveIdx[m] = map[string]*bucket{}
			}
			for pkey, b := range bk {
				closed = append(closed, *b) // 写盘快照
				a.archiveIdx[m][pkey] = b   // 索引（同一指针，(peer,stream) 复合键）
				a.archiveLastWritten[b] = *b
				newPtrs = append(newPtrs, b)
				newPtrSet[b] = struct{}{}
			}
			delete(a.active, m)
		}
	}
	if len(newPtrs) > 0 {
		a.archive = append(a.archive, newPtrs...)
		sort.Slice(a.archive, func(i, j int) bool { return a.archive[i].Minute < a.archive[j].Minute })
	}
	// 收集自上次 flush 起被 ingest 修改过的 archive buckets（迟到请求累加）。
	// 写**delta**（cur - lastWritten）而非当前累加快照，避免 reload 时与首次 flush 那行
	// double-count。reload 把同 (minute, peer) 多行累加，初始 + 各次 delta = 当前累加值。
	// 跳过本轮刚 close 的（已在 closed 里），避免立刻又写一行 0 delta。
	var dirty []bucket
	for b := range a.archiveDirty {
		if _, isNew := newPtrSet[b]; isNew {
			continue
		}
		last := a.archiveLastWritten[b]
		cur := *b
		delta := bucket{
			Minute:    cur.Minute,
			Peer:      cur.Peer,
			Stream:    cur.Stream,
			Requests:  cur.Requests - last.Requests,
			Status2xx: cur.Status2xx - last.Status2xx,
			Status4xx: cur.Status4xx - last.Status4xx,
			Status5xx: cur.Status5xx - last.Status5xx,
			PromptTok: cur.PromptTok - last.PromptTok,
			CachedTok: cur.CachedTok - last.CachedTok,
			ComplTok:  cur.ComplTok - last.ComplTok,
			TotalTok:  cur.TotalTok - last.TotalTok,
			ReqBytes:  cur.ReqBytes - last.ReqBytes,
			RespBytes: cur.RespBytes - last.RespBytes,
			RTSumMs:   cur.RTSumMs - last.RTSumMs,
			RTMaxMs:   cur.RTMaxMs, // max 不能算 delta，写当前值（reload 取 max）
		}
		if delta.Requests == 0 {
			continue // 没新增请求，可能是别的字段被改了或者重复 ingest，跳过
		}
		dirty = append(dirty, delta)
		a.archiveLastWritten[b] = cur
	}
	a.archiveDirty = map[*bucket]struct{}{} // 清空，下次 flush 起重新累积
	// 淘汰超 keepMin 的 archive 条目（同步清 archiveIdx + archiveLastWritten）
	if a.archiveKeepMin > 0 {
		cutMin := time.Now().Unix() - int64(a.archiveKeepMin)*60
		drop := 0
		for drop < len(a.archive) && a.archive[drop].Minute < cutMin {
			drop++
		}
		if drop > 0 {
			for i := 0; i < drop; i++ {
				delete(a.archiveIdx, a.archive[i].Minute)
				delete(a.archiveLastWritten, a.archive[i])
			}
			a.archive = a.archive[drop:]
		}
	}
	a.mu.Unlock()

	if len(closed) > 0 {
		a.appendToFile(closed)
	}
	if len(dirty) > 0 {
		// 迟到累加的 delta 落盘，reload 会合并（求和）首次 flush + 各 delta 行
		a.appendToFile(dirty)
	}
}

func (a *aggregator) appendToFile(buckets []bucket) {
	metricsDir := filepath.Join(a.dir, "metrics")
	if err := os.MkdirAll(metricsDir, 0o755); err != nil {
		log.Printf("metrics mkdir %s: %v", metricsDir, err)
		return
	}
	// 用 bucket 自身的 minute 选择落盘日期（跨日 flush 时把跨过去的 bucket
	// 写到正确的日期文件里）
	byDay := map[string][]bucket{}
	for _, b := range buckets {
		day := time.Unix(b.Minute, 0).Format("2006-01-02")
		byDay[day] = append(byDay[day], b)
	}
	for day, bs := range byDay {
		path := filepath.Join(metricsDir, day+".jsonl")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			log.Printf("metrics open %s: %v", path, err)
			continue
		}
		enc := json.NewEncoder(f)
		for _, b := range bs {
			if err := enc.Encode(b); err != nil {
				log.Printf("metrics encode err: %v", err)
			}
		}
		_ = f.Close()
	}
}

// reload 从今日 metrics 文件恢复 archive；listener 重启不丢历史
func (a *aggregator) reload() {
	today := time.Now().Format("2006-01-02")
	path := filepath.Join(a.dir, "metrics", today+".jsonl")
	f, err := os.Open(path)
	if err != nil {
		return // 没文件就跳过（首次启动 / 日期切了 / 还没 flush）
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	// metrics 文件历史上可能存在同 (minute, peer) 多行（修复前的迟到请求 bug）
	// reload 时按 (minute, peer) 累加合并，恢复出干净的单条 bucket
	idx := map[int64]map[string]*bucket{}
	for scanner.Scan() {
		var b bucket
		if json.Unmarshal(scanner.Bytes(), &b) != nil {
			continue
		}
		mp := idx[b.Minute]
		if mp == nil {
			mp = map[string]*bucket{}
			idx[b.Minute] = mp
		}
		k := bkey(b.Peer, b.Stream)
		if existing, ok := mp[k]; ok {
			existing.Requests += b.Requests
			existing.Status2xx += b.Status2xx
			existing.Status4xx += b.Status4xx
			existing.Status5xx += b.Status5xx
			existing.PromptTok += b.PromptTok
			existing.CachedTok += b.CachedTok
			existing.ComplTok += b.ComplTok
			existing.TotalTok += b.TotalTok
			existing.ReqBytes += b.ReqBytes
			existing.RespBytes += b.RespBytes
			existing.RTSumMs += b.RTSumMs
			if b.RTMaxMs > existing.RTMaxMs {
				existing.RTMaxMs = b.RTMaxMs
			}
			existing.FrtSumMs += b.FrtSumMs
			existing.FrtN += b.FrtN
			if b.FrtMaxMs > existing.FrtMaxMs {
				existing.FrtMaxMs = b.FrtMaxMs
			}
		} else {
			cp := b
			mp[k] = &cp
		}
	}
	var loaded []*bucket
	for _, mp := range idx {
		for _, p := range mp {
			loaded = append(loaded, p)
		}
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Minute < loaded[j].Minute })
	a.mu.Lock()
	a.archive = loaded
	a.archiveIdx = idx
	a.mu.Unlock()
	log.Printf("reloaded %d metrics buckets from %s (merged from raw lines)", len(loaded), path)
}

func envOr(k, dflt string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return dflt
}

// envOrInt 读 int 型环境变量；非法/非正数则回退默认值并告警。
func envOrInt(k string, dflt int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
		log.Printf("invalid %s=%q (need positive int), using default %d", k, v, dflt)
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
	keepDays = envOrInt("BODYLOG_KEEP_DAYS", defaultKeepDays)
	detailsKeepDays = envOrInt("BODYLOG_DETAILS_KEEP_DAYS", defaultDetailsKeepDays)
	httpToken = envOr("BODYLOG_HTTP_TOKEN", "")
	if httpToken != "" {
		log.Printf("HTTP auth: ON — /summary + /metrics require Bearer token (BODYLOG_HTTP_TOKEN)")
	} else {
		log.Printf("HTTP auth: OFF — /summary + /metrics open (set BODYLOG_HTTP_TOKEN to enable)")
	}
	log.Printf("history archives retention: %d days (BODYLOG_KEEP_DAYS); details parquet retention: %d days (BODYLOG_DETAILS_KEEP_DAYS)", keepDays, detailsKeepDays)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", dir, err)
	}

	// 进程内 DuckDB（in-memory）：metrics/details 的 jsonl→parquet 转换 + /metrics 混读查询。
	// 打开失败不致命——主落盘/聚合不依赖它，只是明细不转 parquet、/metrics 返回 503。
	if ddb, err := sql.Open("duckdb", ""); err != nil {
		log.Printf("duckdb open failed (/metrics + parquet rotate disabled): %v", err)
	} else if err := ddb.Ping(); err != nil {
		log.Printf("duckdb ping failed (/metrics + parquet rotate disabled): %v", err)
		_ = ddb.Close()
	} else {
		duckDB = ddb
		defer duckDB.Close()
		log.Printf("duckdb ready (in-process, metrics/details parquet + /metrics enabled)")
	}

	addr := host + ":" + port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	log.Printf("listening on %s, writing to %s/", addr, dir)

	w := &hourWriter{dir: dir}

	// per-request metrics 明细（剥正文），按天写 metrics/details/。BODYLOG_DETAILS=off 时
	// 不创建 → handleConn 里 dw==nil 跳过 extractDetail+写盘，每请求零额外开销（kill-switch +
	// 性能 A/B 对照基线）。
	var dw *detailsWriter
	if envOr("BODYLOG_DETAILS", "on") != "off" {
		dw = &detailsWriter{dir: dir}
		log.Printf("per-request details sink: ON (metrics/details/)")
	} else {
		log.Printf("per-request details sink: OFF (BODYLOG_DETAILS=off)")
	}

	// per-minute aggregator：archive 内存保留 7 天 = 10080 分钟
	agg := newAggregator(dir, 7*24*60)
	agg.reload()

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

	// 每 60s flush 一次：把 minute < now-90s 的 bucket 移到 archive 并写盘
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				agg.flushClosedMinutes() // shutdown 前最后冲一次（active 里旧的）
				return
			case <-t.C:
				agg.flushClosedMinutes()
			}
		}
	}()

	// HTTP server（独立端口，独立 goroutine；listener crash 不影响 frame 接收）
	httpAddr := envOr("BODYLOG_HTTP_HOST", "0.0.0.0") + ":" + envOr("BODYLOG_HTTP_PORT", "9998")
	go agg.serveHTTP(httpAddr)

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept err: %v", err)
			continue
		}
		go handleConn(c, w, dw, agg)
	}
	w.close()
	if dw != nil {
		dw.close()
	}
	log.Println("exited")
}
