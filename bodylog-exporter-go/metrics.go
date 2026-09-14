package main

import (
	"fmt"
	"slices"
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
	// service = ModelRoute discovery.service(ns/name,用户主聚合维度);route = nginx.route(省略则 metadata.name)。
	// 二者都随后端 pod 稳定(pod IP 漂移也不变),补上 bodylog backend/model 缺的 service 归属。
	srbm := []string{"service", "route", "backend", "model"}
	// srbmp = srbm + prompt_bucket。用 slices.Concat 而非 append(srbm, ...):append 在 cap>len
	// 时原地写入并返回共享底层数组的 slice,多处 append 会互相覆盖(当前 cap==len 只是侥幸安全)。
	srbmp := slices.Concat(srbm, []string{"prompt_bucket"})
	ctr := func(name, help string, labels []string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	// native-only:不传 Buckets 即无经典桶(client_golang 仅在 factor<=1 时才补 DefBuckets)。
	// 起因:Prometheus 开了 scrapeClassicHistograms,双发会真进 TSDB(实测 12x series 膨胀);
	// 全库告警/面板都是裸名 native 写法,无一处引用 _bucket/_sum/_count。
	// ⚠️ 若 Prometheus 关掉 native histogram,这三个直方图将没有任何桶。
	hist := func(name, help string, labels []string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: name, Help: help,
			NativeHistogramBucketFactor:     1.1, // 指数 native:每桶 ~10% 宽,高分辨率 → p95/p99 更准
			NativeHistogramMaxBucketNumber:  160, // 桶数上限,防高基数/异常值撑爆
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
		rt:            hist("bodylog_rt_seconds", "总响应时间(秒)", []string{"service", "route", "backend", "model", "stream"}),
		// prompt_bucket:输入长度分档,用于“某 context 区间的 TTFT 分位数”。见 promptBucket()。
		ttft: hist("bodylog_ttft_seconds", "首 token 时间/TTFT(秒,仅流式)", srbmp),
		// 2026-08-26:分母从 rt 改为 rt-frt(扣除 prefill),仅流式 + 下限过滤,与 openresty 引擎
		// 的 tps_limit_tps 口径对齐。详见 observe() 里的说明。历史数据与新数据不可比。
		// prompt_bucket:同 ttft —— 解码速率同样随 context 变长而下降(KV 越长 attention 越贵),
		// 分档后才能看出「长输入到底拖慢多少」。见 promptBucket()。
		outTokPerSec: hist("bodylog_output_tok_per_second", "单请求解码速率 completion_tokens/(rt-frt)(tok/s,已扣 prefill;仅流式)", srbmp),
		lines:        prometheus.NewCounter(prometheus.CounterOpts{Name: "bodylog_exporter_lines_total", Help: "已 observe 的明细行数"}),
		offset:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_exporter_offset_bytes", Help: "当前 tail 文件的字节 offset"}),
		lastTs:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_exporter_last_ts_seconds", Help: "最新 observe 行的结束时刻(unix 秒),判滞后"}),
		recovers:     prometheus.NewCounter(prometheus.CounterOpts{Name: "bodylog_exporter_recovery_total", Help: "跨天缺口 HTTP 补读次数"}),
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
	// 富化:后端 pod IP → service(= ns/name,主聚合维度)+ route(nginx.route,省略则 metadata.name)。未命中回退 unknown。
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
		m.ttft.WithLabelValues(service, route, backend, model, promptBucket(d.PromptTokens)).Observe(d.Frt)
	}
	// 输出 token 速率 = completion_tokens / 请求**总时长 rt**(含 prefill),流式与非流式一视同仁。
	// ⚠️ 2026-09-07 口径变更(与 openresty 引擎 lua/access.lua 同步改,两边必须一致):
	//   ① 分母由「解码耗时 rt-frt」改回**总时长 rt**;
	//   ② 取消 stream 过滤 —— 非流式请求同样占并发、同样消耗后端算力,排除它们会让
	//      「非流式为主」的路由样本长期为空,该指标失去监控意义。
	//   ①是②的前提:非流式只有一个 body chunk、frt≈rt,旧口径下 rt-frt≈0,相除得天文数字,
	//   会把整个直方图顶进 overflow 桶(这正是当初加 stream 过滤的原因)。
	// ⚠️ 与旧口径相比系统性偏低,prefill 占比越高差得越多(实测慢尾请求 frt 中位占 rt 的 48%,
	//    该段样本约低一半)。**历史数据不可比,告警阈值与引擎 tps_limit_tps 需按新口径同步重定。**
	// ⚠️ 下限过滤对齐引擎(tps_min_tokens=16 / tps_min_decode_s=0.5,后者现在卡的是总时长):
	//    短响应固定开销占比过高(20 token / 2s = 10 tok/s),不代表稳态吞吐,不滤会污染低尾。
	if d.CompletionTokens >= 16 && d.Rt >= 0.5 {
		m.outTokPerSec.WithLabelValues(service, route, backend, model, promptBucket(d.PromptTokens)).
			Observe(float64(d.CompletionTokens) / d.Rt)
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

// prompt_bucket:把 prompt_tokens 落到固定档位,作为 TTFT / OTPS 的 label,用于回答
// 「某 context 区间的 TTFT p80」这类问题。
//
// 为什么必须预先定档:Prometheus 存不了「每条请求」,切分维度必须在写入时就是 label,
// 查询时只能合并相邻档(变粗)、不能拆分(变细)。
// ⚠️ 边界上线后【不要改】:改档位 = 旧 label 停更、新值从零、跨改动点的 histogram_quantile
//
//	不可信。需要任意区间/分位请查明细 parquet(保留 365 天):
//	  SELECT quantile_cont(first_chunk_t, 0.8) FROM read_parquet('.../<date>.parquet')
//	  WHERE prompt_tokens BETWEEN 6144 AND 12288 AND stream;
//
// 边界含 6/16/32/64/128/256k 等业务关注点,查询时用正则合并,如 6~16K:
//
//	prompt_bucket=~"0006k_0008k|0008k_0010k|0010k_0012k|0012k_0016k"
//
// 末档 1024k_inf 是溢出哨兵(超模型 context 上限)。空档不产生 series,零成本。
var promptBucketBounds = []int64{
	1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 32, 40, 48, 64,
	80, 96, 128, 160, 192, 256, 384, 512, 768, 1024,
}

// promptBucketLabels:与 promptBucketBounds 对应的 label 值,左闭右开 [lo, hi)。
// 4 位零填充保证 Grafana/PromQL 里【字典序 == 数值序】(否则 "96k" 会排到 "128k" 之后)。
var promptBucketLabels = func() []string {
	ls := make([]string, 0, len(promptBucketBounds)+1)
	lo := int64(0)
	for _, hi := range promptBucketBounds {
		ls = append(ls, fmt.Sprintf("%04dk_%04dk", lo, hi))
		lo = hi
	}
	return append(ls, fmt.Sprintf("%04dk_inf", lo))
}()

// promptBucket 返回 tok 所属档位的 label,按 1024 token = 1k 换算,区间左闭右开。
// tok<=0 单列 "unknown",【不能】混进首档:这批是 usage 缺失的失败请求(实测 status 多为 400/429、
// finish_reason 全 null),frt 反映的是错误返回耗时;混入会让首档 42% 是噪声、TTFT p80 从
// 0.11s 虚高到 1.37s。单列而非丢弃 —— 样本不丢,且 400/429 突增本身是信号。
func promptBucket(tok int64) string {
	if tok <= 0 {
		return "unknown"
	}
	k := tok / 1024
	for i, hi := range promptBucketBounds {
		if k < hi {
			return promptBucketLabels[i]
		}
	}
	return promptBucketLabels[len(promptBucketLabels)-1]
}
