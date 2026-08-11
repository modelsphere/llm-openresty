package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// detailRecord:与 bodylog-listener 的 detailRecord 对应的子集(只取做指标要的字段)。
// 每行 details/<date>.jsonl 一条。字段可靠性(见 plan):backend 空时为 "(none)";
// stream 未知=null;frt 非流式=0;model/finish_reason 可能空;token 无 usage=0。
type detailRecord struct {
	Ts               string  `json:"ts"`
	TsEnd            string  `json:"ts_end"`
	RequestID        string  `json:"request_id"`
	Stream           *bool   `json:"stream"`
	Status           int64   `json:"status"`
	Backend          string  `json:"backend"`
	Model            string  `json:"model"`
	FinishReason     string  `json:"finish_reason"`
	Frt              float64 `json:"frt"` // ≈TTFT,秒
	Rt               float64 `json:"rt"`  // 总响应时间,秒
	ReqBytes         int64   `json:"req_bytes"`
	RespBytes        int64   `json:"resp_bytes"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	CachedTokens     int64   `json:"cached_tokens"`
	TotalTokens      int64   `json:"total_tokens"`
	ReasoningTokens  int64   `json:"reasoning_tokens"`
}

type metrics struct {
	requests      *prometheus.CounterVec // {backend,model,status_class,stream}
	promptTok     *prometheus.CounterVec // {backend,model}
	completionTok *prometheus.CounterVec
	cachedTok     *prometheus.CounterVec
	reasoningTok  *prometheus.CounterVec
	totalTok      *prometheus.CounterVec
	reqBytes      *prometheus.CounterVec
	respBytes     *prometheus.CounterVec
	finishReason  *prometheus.CounterVec // {backend,model,finish_reason}
	rt            *prometheus.HistogramVec
	ttft          *prometheus.HistogramVec
	outTokPerSec  *prometheus.HistogramVec
	// exporter 自监控
	lines    prometheus.Counter
	offset   prometheus.Gauge
	lastTs   prometheus.Gauge
	recovers prometheus.Counter
}

func newMetrics(reg *prometheus.Registry) *metrics {
	bm := []string{"backend", "model"}
	ctr := func(name, help string, labels []string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	hist := func(name, help string, buckets []float64, labels []string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	}
	m := &metrics{
		requests:      ctr("bodylog_requests_total", "请求数", []string{"backend", "model", "status_class", "stream"}),
		promptTok:     ctr("bodylog_prompt_tokens_total", "prompt token 累计", bm),
		completionTok: ctr("bodylog_completion_tokens_total", "completion token 累计", bm),
		cachedTok:     ctr("bodylog_cached_tokens_total", "cached token 累计", bm),
		reasoningTok:  ctr("bodylog_reasoning_tokens_total", "reasoning token 累计", bm),
		totalTok:      ctr("bodylog_total_tokens_total", "total token 累计", bm),
		reqBytes:      ctr("bodylog_req_bytes_total", "请求体字节累计", bm),
		respBytes:     ctr("bodylog_resp_bytes_total", "响应体字节累计", bm),
		finishReason:  ctr("bodylog_finish_reason_total", "按 finish_reason 计数", []string{"backend", "model", "finish_reason"}),
		rt:            hist("bodylog_rt_seconds", "总响应时间(秒)", []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300}, []string{"backend", "model", "stream"}),
		ttft:          hist("bodylog_ttft_seconds", "首 token 时间/TTFT(秒,仅流式)", []float64{0.05, 0.1, 0.2, 0.5, 1, 2, 3, 5, 10}, bm),
		outTokPerSec:  hist("bodylog_output_tok_per_second", "单请求生成速率 completion_tokens/rt(tok/s)", []float64{5, 10, 20, 30, 50, 80, 120, 200, 400}, bm),
		lines:         prometheus.NewCounter(prometheus.CounterOpts{Name: "bodylog_exporter_lines_total", Help: "已 observe 的明细行数"}),
		offset:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_exporter_offset_bytes", Help: "当前 tail 文件的字节 offset"}),
		lastTs:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_exporter_last_ts_seconds", Help: "最新 observe 行的结束时刻(unix 秒),判滞后"}),
		recovers:      prometheus.NewCounter(prometheus.CounterOpts{Name: "bodylog_exporter_recovery_total", Help: "跨天缺口 HTTP 补读次数"}),
	}
	reg.MustRegister(
		m.requests, m.promptTok, m.completionTok, m.cachedTok, m.reasoningTok, m.totalTok,
		m.reqBytes, m.respBytes, m.finishReason, m.rt, m.ttft, m.outTokPerSec,
		m.lines, m.offset, m.lastTs, m.recovers,
	)
	return m
}

func statusClass(s int64) string {
	switch {
	case s >= 200 && s < 300:
		return "2xx"
	case s >= 400 && s < 500:
		return "4xx"
	case s >= 500 && s < 600:
		return "5xx"
	default:
		return "other"
	}
}

func streamLabel(b *bool) string {
	if b == nil {
		return "unknown"
	}
	if *b {
		return "true"
	}
	return "false"
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// observe:把一条明细行累加进 Prometheus。0/空字段按"缺测"跳过,不污染分位/不造无谓 series。
func (m *metrics) observe(d detailRecord) {
	backend := orDefault(d.Backend, "(none)")
	model := orDefault(d.Model, "unknown")
	sc := statusClass(d.Status)
	sl := streamLabel(d.Stream)

	m.requests.WithLabelValues(backend, model, sc, sl).Inc()
	if d.PromptTokens > 0 {
		m.promptTok.WithLabelValues(backend, model).Add(float64(d.PromptTokens))
	}
	if d.CompletionTokens > 0 {
		m.completionTok.WithLabelValues(backend, model).Add(float64(d.CompletionTokens))
	}
	if d.CachedTokens > 0 {
		m.cachedTok.WithLabelValues(backend, model).Add(float64(d.CachedTokens))
	}
	if d.ReasoningTokens > 0 {
		m.reasoningTok.WithLabelValues(backend, model).Add(float64(d.ReasoningTokens))
	}
	if d.TotalTokens > 0 {
		m.totalTok.WithLabelValues(backend, model).Add(float64(d.TotalTokens))
	}
	if d.ReqBytes > 0 {
		m.reqBytes.WithLabelValues(backend, model).Add(float64(d.ReqBytes))
	}
	if d.RespBytes > 0 {
		m.respBytes.WithLabelValues(backend, model).Add(float64(d.RespBytes))
	}
	if d.FinishReason != "" {
		m.finishReason.WithLabelValues(backend, model, d.FinishReason).Inc()
	}
	if d.Rt > 0 {
		m.rt.WithLabelValues(backend, model, sl).Observe(d.Rt)
	}
	if d.Frt > 0 { // 非流式 frt=0,跳过,不污染 TTFT 分位
		m.ttft.WithLabelValues(backend, model).Observe(d.Frt)
	}
	if d.Rt > 0 && d.CompletionTokens > 0 {
		m.outTokPerSec.WithLabelValues(backend, model).Observe(float64(d.CompletionTokens) / d.Rt)
	}

	m.lines.Inc()
	if ts := parseTsSeconds(d.TsEnd, d.Ts); ts > 0 {
		m.lastTs.Set(ts)
	}
}

// parseTsSeconds:best-effort 把 ts_end/ts 串解析成 unix 秒(仅供自监控 gauge)。
// 兼容 RFC3339Nano 与 "2006-01-02 15:04:05.999999999 -0700 MST" 等常见落盘格式。
func parseTsSeconds(candidates ...string) float64 {
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999-07:00"}
	for _, s := range candidates {
		if s == "" {
			continue
		}
		for _, l := range layouts {
			if t, err := time.Parse(l, s); err == nil {
				return float64(t.UnixNano()) / 1e9
			}
		}
	}
	return 0
}
