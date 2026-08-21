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

	// 富化:后端 pod IP → route/model(给每条指标打稳定的 route label)。nil=不富化(route=unknown)。
	resolver *podRouteResolver
}

func newMetrics(reg *prometheus.Registry) *metrics {
	// service = ModelRoute discovery.service(ns/name,用户主聚合维度);route = nginx.route。
	// 二者都随后端 pod 稳定(pod IP 漂移也不变),补上 bodylog backend/model 缺的 service 归属。
	srbm := []string{"service", "route", "backend", "model"}
	ctr := func(name, help string, labels []string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	hist := func(name, help string, buckets []float64, labels []string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: name, Help: help,
			Buckets:                         buckets, // 经典桶:双发,保留兼容/回退(Prometheus 关 native 时仍能用)
			NativeHistogramBucketFactor:     1.1,     // 指数 native:每桶 ~10% 宽,高分辨率 → p95/p99 更准
			NativeHistogramMaxBucketNumber:  160,     // 桶数上限,防高基数/异常值撑爆
			NativeHistogramMinResetDuration: time.Hour,
		}, labels)
	}
	m := &metrics{
		requests:      ctr("bodylog_requests_total", "请求数", []string{"service", "route", "backend", "model", "status_class", "stream"}),
		promptTok:     ctr("bodylog_prompt_tokens_total", "prompt token 累计", srbm),
		completionTok: ctr("bodylog_completion_tokens_total", "completion token 累计", srbm),
		cachedTok:     ctr("bodylog_cached_tokens_total", "cached token 累计", srbm),
		reasoningTok:  ctr("bodylog_reasoning_tokens_total", "reasoning token 累计", srbm),
		totalTok:      ctr("bodylog_total_tokens_total", "total token 累计", srbm),
		reqBytes:      ctr("bodylog_req_bytes_total", "请求体字节累计", srbm),
		respBytes:     ctr("bodylog_resp_bytes_total", "响应体字节累计", srbm),
		finishReason:  ctr("bodylog_finish_reason_total", "按 finish_reason 计数", []string{"service", "route", "backend", "model", "finish_reason"}),
		rt:            hist("bodylog_rt_seconds", "总响应时间(秒)", []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300}, []string{"service", "route", "backend", "model", "stream"}),
		ttft:          hist("bodylog_ttft_seconds", "首 token 时间/TTFT(秒,仅流式)", []float64{0.05, 0.1, 0.2, 0.5, 1, 2, 3, 5, 10}, srbm),
		outTokPerSec:  hist("bodylog_output_tok_per_second", "单请求生成速率 completion_tokens/rt(tok/s)", []float64{5, 10, 20, 30, 50, 80, 120, 200, 400}, srbm),
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
	model := orDefault(d.Model, "unknown") // model 纯来自明细,不做兜底
	// 富化:后端 pod IP → service(= ns/name,主聚合维度)+ route(nginx.route)。未命中回退 unknown。
	route, service := "unknown", "unknown"
	if m.resolver != nil {
		if rt, svc, ok := m.resolver.Lookup(backendIP(d.Backend)); ok {
			if rt != "" {
				route = rt
			}
			if svc != "" {
				service = svc
			}
		}
	}
	sc := statusClass(d.Status)
	sl := streamLabel(d.Stream)

	m.requests.WithLabelValues(service, route, backend, model, sc, sl).Inc()
	if d.PromptTokens > 0 {
		m.promptTok.WithLabelValues(service, route, backend, model).Add(float64(d.PromptTokens))
	}
	if d.CompletionTokens > 0 {
		m.completionTok.WithLabelValues(service, route, backend, model).Add(float64(d.CompletionTokens))
	}
	if d.CachedTokens > 0 {
		m.cachedTok.WithLabelValues(service, route, backend, model).Add(float64(d.CachedTokens))
	}
	if d.ReasoningTokens > 0 {
		m.reasoningTok.WithLabelValues(service, route, backend, model).Add(float64(d.ReasoningTokens))
	}
	if d.TotalTokens > 0 {
		m.totalTok.WithLabelValues(service, route, backend, model).Add(float64(d.TotalTokens))
	}
	if d.ReqBytes > 0 {
		m.reqBytes.WithLabelValues(service, route, backend, model).Add(float64(d.ReqBytes))
	}
	if d.RespBytes > 0 {
		m.respBytes.WithLabelValues(service, route, backend, model).Add(float64(d.RespBytes))
	}
	if d.FinishReason != "" {
		m.finishReason.WithLabelValues(service, route, backend, model, d.FinishReason).Inc()
	}
	if d.Rt > 0 {
		m.rt.WithLabelValues(service, route, backend, model, sl).Observe(d.Rt)
	}
	// TTFT 只记流式请求。
	// ⚠️ 不能靠 frt>0 过滤(此前的做法):非流式的 first_chunk_t ≈ rt —— 非流式只有一个 body
	//    chunk,它到达时响应也就结束了,所以 frt 恒 >0,一条都滤不掉。后果是 TTFT 直方图混进
	//    大量 frt≈rt 的样本,实测 p99(82s)甚至超过 RT p99(77s),物理上不可能。
	// stream 由 listener 从请求体正则抠出(main.go extractStream);抠不到=nil,含
	//    「客户端没传 stream 参数」(OpenAI 语义即非流式)与「req_body 没采到/base64」两种。
	//    实测 800 条明细里 stream=nil 的无一呈流式形态(frt<0.8*rt),故按非流式处理。
	// 代价:req_body 未采到的【真流式】请求会漏记 TTFT(样本变少,不会算错);当前为 0 条。
	if d.Frt > 0 && d.Stream != nil && *d.Stream {
		m.ttft.WithLabelValues(service, route, backend, model).Observe(d.Frt)
	}
	if d.Rt > 0 && d.CompletionTokens > 0 {
		m.outTokPerSec.WithLabelValues(service, route, backend, model).Observe(float64(d.CompletionTokens) / d.Rt)
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
