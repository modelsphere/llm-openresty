// openresty-poll:把【k8s 集群内那台 openresty】的实时路由/限流状态(shared dict)
// 翻译成 Prometheus 指标。不改 openresty —— 只 GET 它已经暴露的 JSON 调试端点:
//
//	/<route>/_route_state      每 peer 当前并发 / 是否被 ban / max / priority / active_level
//	/<route>/_tps_status       自适应并发 adaptive_cc(当前动态上限/min/max/当前并发/本区间被压需求)+ 解码速率 EWMA
//	/<route>/_ttft_status      TTFT EWMA + 限流是否 active
//	/<route>/_429_status      429 rejections of this route so far, by reason (concurrency/ttft/tps)
//
// 与 tail 文件那套的分工:tail = 事后逐请求(counter/histogram,零丢);poll = 即时控制面态(gauge,快照)。
// 这些是 bodylog 拿不到的 live 信号(每 peer 并发/被 ban 的 peer/限流档位),对自动扩缩容反应更快。
//
// 只 poll 一台(k8s openresty Service;HA 时 Service 只选 active leader → 天然单逻辑目标),不带 instance label。
// An empty URL turns the module off (the default: an exporter on bare metal cannot reach k8s).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ---- 配置 ----

type orConfig struct {
	baseURL      string        // 如 http://openresty:8080(集群内 Service,路径路由);空=不启用
	staticRoutes []string      // 静态 route 覆盖(OPENRESTY_POLL_ROUTES);非空则不走 k8s 动态发现
	interval     time.Duration // poll 周期
	timeout      time.Duration // 单请求超时
}

func loadORConfig() orConfig {
	var routes []string
	for _, r := range strings.Split(envOr("OPENRESTY_POLL_ROUTES", ""), ",") {
		if r = strings.TrimSpace(r); r != "" {
			routes = append(routes, r)
		}
	}
	return orConfig{
		baseURL:      strings.TrimRight(strings.TrimSpace(envOr("OPENRESTY_POLL_URL", "")), "/"),
		staticRoutes: routes,
		interval:     time.Duration(envOrInt("OPENRESTY_POLL_INTERVAL_MS", 15000)) * time.Millisecond,
		timeout:      time.Duration(envOrInt("OPENRESTY_POLL_TIMEOUT_MS", 3000)) * time.Millisecond,
	}
}

// ---- 指标 ----

type orMetrics struct {
	// /_route_state(每 cycle 清空重填 → 掉线 peer 的旧 series 消失)
	activeLevel  *prometheus.GaugeVec // {route}       当前生效优先级层
	activeLimit  *prometheus.GaugeVec // {route}       生效层总并发上限(healthy peer max 之和)
	healthyPeers *prometheus.GaugeVec // {route}       生效层健康 peer 数
	peerActive   *prometheus.GaugeVec // {route,peer,name,priority} 该 peer 当前并发
	peerBanned   *prometheus.GaugeVec // {route,peer,name,priority} 是否被健康检查 ban(1/0)
	peerMax      *prometheus.GaugeVec // {route,peer,name,priority} 该 peer 静态并发上限
	// /_tps_status
	tpsActive      *prometheus.GaugeVec // {route}        TPS/自适应并发限流是否生效(1/0)
	tpsEwma        *prometheus.GaugeVec // {route,model}  解码速率 EWMA(tok/s)
	adaptiveCC     *prometheus.GaugeVec // {route,model}  当前动态并发上限(AIMD)
	adaptiveCCMin  *prometheus.GaugeVec // {route,model}  生效下限
	adaptiveCCMax  *prometheus.GaugeVec // {route,model}  静态池容量(AIMD clamp)
	adaptiveCCInit *prometheus.GaugeVec // {route,model}  start point and low-traffic shrink floor
	adaptiveCCConc *prometheus.GaugeVec // {route,model}  当前并发(timer 判压力用的实时在途)
	adaptiveCCRej  *prometheus.GaugeVec // {route,model}  本区间被压抑需求(并发 429 数)
	// /_ttft_status
	ttftActive *prometheus.GaugeVec // {route}        TTFT 限流是否生效(1/0)
	ttftEwma   *prometheus.GaugeVec // {route,model}  TTFT EWMA(ms)
	// /_429_status(counter:delta 累加,跨 poll 单调)
	rejected *prometheus.CounterVec // {route,reason}
	// poll 自监控
	// 上轮 poll 是否成功,**按 route 分**(1/0)。
	// 早先是全局单个 Gauge(「是否全成功」),任何一条 route 失败就归零。
	// 后果不是数据断流(各 route 的 gauge 是分开填的,其余 route 数据照常),
	// 而是**这个告警本身失去信号价值**:一条注定失败的 video 路由让
	// OpenRestyPollDown 连响 3 天(2026-09-03~09-06),期间真出轮询故障也分辨不出来。
	// 按 route 拆开后,坏一条只黑一条,告警还能指名道姓。
	pollUp     *prometheus.GaugeVec
	pollErrors prometheus.Counter   // poll 出错累计
	pollLastOK *prometheus.GaugeVec // 上次成功 poll 的 unix 秒

	resettable []*prometheus.GaugeVec // 每 cycle 需清空的 gauge(不含 counter/self)
}

func newORMetrics(reg *prometheus.Registry) *orMetrics {
	g := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	}
	// service = 后端 discovery.service(ns/name,route→service 由 podRouteResolver 提供);与 bodylog_* 对齐,便于按 service 聚合(如 429)。
	sroute := []string{"service", "route"}
	peerLbls := []string{"service", "route", "peer", "name", "priority"}
	srm := []string{"service", "route", "model"}
	m := &orMetrics{
		activeLevel:  g("openresty_route_active_level", "当前生效优先级层(3=cart/2=backend/1=svc 兜底)", sroute...),
		activeLimit:  g("openresty_route_active_limit", "生效层总并发上限(healthy peer max 之和)", sroute...),
		healthyPeers: g("openresty_route_healthy_peers", "生效层健康 peer 数", sroute...),
		peerActive:   g("openresty_peer_active_conns", "该 peer 当前并发(least_conn 计数)", peerLbls...),
		peerBanned:   g("openresty_peer_banned", "该 peer 是否被健康检查 ban(1/0)", peerLbls...),
		peerMax:      g("openresty_peer_max_concurrency", "该 peer 静态并发上限", peerLbls...),

		tpsActive:      g("openresty_tps_limiter_active", "TPS/自适应并发限流是否生效(1/0)", sroute...),
		tpsEwma:        g("openresty_tps_ewma", "解码速率 EWMA(tok/s)", srm...),
		adaptiveCC:     g("openresty_adaptive_cc", "当前动态并发上限(AIMD)", srm...),
		adaptiveCCMin:  g("openresty_adaptive_cc_min", "自适应并发生效下限", srm...),
		adaptiveCCMax:  g("openresty_adaptive_cc_max", "自适应并发静态池容量(AIMD clamp)", srm...),
		adaptiveCCInit: g("openresty_adaptive_cc_init", "Adaptive concurrency start point and low-traffic shrink floor (= min unless configured)", srm...),
		adaptiveCCConc: g("openresty_adaptive_cc_conc", "当前并发(timer 判压力用的实时在途)", srm...),
		adaptiveCCRej:  g("openresty_adaptive_cc_rej", "本区间被压抑需求(并发 429 数)", srm...),

		ttftActive: g("openresty_ttft_limiter_active", "TTFT 限流是否生效(1/0)", sroute...),
		ttftEwma:   g("openresty_ttft_ewma_ms", "TTFT EWMA(ms)", srm...),

		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "openresty_rejected_total", Help: "429 限流累计(按 service × route × reason=concurrency/ttft/tps)",
		}, []string{"service", "route", "reason"}),

		pollUp:     g("openresty_poll_up", "Whether the last poll of this route on its openresty succeeded (1/0)", "service", "route"),
		pollErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "openresty_poll_errors_total", Help: "openresty poll 出错累计"}),
		pollLastOK: g("openresty_poll_last_success_seconds", "Unix seconds of the last successful poll of this route", "service", "route"),
	}
	m.resettable = []*prometheus.GaugeVec{
		// pollUp 要进来:route 下线后不该留一条恒 0 的平线触发告警。
		// pollLastOK **不进来**:它的语义就是"上次成功的时刻",靠变旧来表达失败;
		// 每轮 Reset 会把这条信息抹掉,失败时反而什么都看不到。
		m.pollUp,
		m.activeLevel, m.activeLimit, m.healthyPeers, m.peerActive, m.peerBanned, m.peerMax,
		m.tpsActive, m.tpsEwma, m.adaptiveCC, m.adaptiveCCMin, m.adaptiveCCMax, m.adaptiveCCInit, m.adaptiveCCConc, m.adaptiveCCRej,
		m.ttftActive, m.ttftEwma,
	}
	for _, gv := range m.resettable {
		reg.MustRegister(gv)
	}
	// pollUp 已随 resettable 注册过,这里不能再来一次(会 duplicate registration panic)。
	reg.MustRegister(m.rejected, m.pollErrors, m.pollLastOK)
	return m
}

// ---- openresty JSON 端点的响应结构(只取要的字段) ----

type routeStateResp struct {
	Route               string  `json:"route"`
	ActiveLevel         float64 `json:"active_level"`
	Limit               float64 `json:"limit"`
	HealthyPeersInLevel float64 `json:"healthy_peers_in_level"`
	ByPriority          map[string]struct {
		Peers []struct {
			Name   string  `json:"name"`
			Peer   string  `json:"peer"`
			Banned bool    `json:"banned"`
			Active float64 `json:"active"`
			Max    float64 `json:"max"`
		} `json:"peers"`
	} `json:"by_priority"`
}

// null(未初始化)与 0 要区分 → *float64,nil 时跳过不 set。
type tpsStatusResp struct {
	Active         bool                `json:"active"`
	EwmaTps        map[string]*float64 `json:"ewma_tps"`
	AdaptiveCC     map[string]*float64 `json:"adaptive_cc"`
	AdaptiveCCMin  map[string]*float64 `json:"adaptive_cc_min"`
	AdaptiveCCMax  map[string]*float64 `json:"adaptive_cc_max"`
	AdaptiveCCInit map[string]*float64 `json:"adaptive_cc_init"` // absent on older engines → no series
	AdaptiveCCConc map[string]*float64 `json:"adaptive_cc_conc"`
	AdaptiveCCRej  map[string]*float64 `json:"adaptive_cc_rej"`
}

type ttftStatusResp struct {
	Active bool                `json:"active"`
	EwmaMs map[string]*float64 `json:"ewma_ms"`
}

// openresty 限流的 reason 全集,与 _429_status 的 by_reason 字段一致。
// 用于每轮预初始化 counter,让「零 429」表现为恒 0 的曲线而不是空 vector(见 pollOnce)。
// 新增限流维度时要同步加进来,否则那一维在无事发生时又会退化成空 vector。
var rejectReasons = []string{"concurrency", "ttft", "tps"}

type rejectStatusResp struct {
	ByRoute map[string]map[string]float64 `json:"by_route"`
}

// ---- poller ----

type orPoller struct {
	cfg       orConfig
	m         *orMetrics
	client    *http.Client
	targetsFn func() []pollTarget // what to poll: (openresty, route, service); base "" = cfg.baseURL
	prev      map[string]float64  // 429 counter delta tracking: key "base|route|reason" -> last absolute value
}

func newORPollerTargets(cfg orConfig, m *orMetrics, targetsFn func() []pollTarget) *orPoller {
	return &orPoller{cfg: cfg, m: m, client: &http.Client{Timeout: cfg.timeout}, targetsFn: targetsFn, prev: map[string]float64{}}
}

// newORPoller: routes on the single OPENRESTY_POLL_URL, labelled by svcFn (nil or "" -> unknown). Used for
// static OPENRESTY_POLL_ROUTES and kept for callers that only know route names.
func newORPoller(cfg orConfig, m *orMetrics, routesFn func() []string, svcFn func(string) string) *orPoller {
	return newORPollerTargets(cfg, m, func() []pollTarget {
		rs := routesFn()
		out := make([]pollTarget, 0, len(rs))
		for _, r := range rs {
			svc := ""
			if svcFn != nil {
				svc = svcFn(r)
			}
			out = append(out, pollTarget{route: r, service: svc})
		}
		return out
	})
}

func (p *orPoller) baseOf(t pollTarget) string {
	if t.base != "" {
		return t.base
	}
	return p.cfg.baseURL
}

func (p *orPoller) run(ctx context.Context) {
	t := time.NewTicker(p.cfg.interval)
	defer t.Stop()
	p.pollOnce(ctx) // 启动即拉一次,不等第一个 tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *orPoller) pollOnce(ctx context.Context) {
	// 每 cycle 先清空 gauge:掉线的 peer/route/model 旧 series 随之消失(counter 不清)。
	for _, gv := range p.m.resettable {
		gv.Reset()
	}
	targets := p.targetsFn() // dynamic: routes come and go with the ModelRoutes (gone routes vanish via the Reset above)
	for i := range targets {
		if targets[i].service == "" {
			targets[i].service = "unknown" // same as the bodylog_* metrics
		}
	}
	type tkey struct{ base, route string }
	targetOK := map[tkey]bool{}
	for _, t := range targets {
		targetOK[tkey{p.baseOf(t), t.route}] = true
	}
	fail := func(t pollTarget, what string, err error) {
		p.pollErr(what, t.route, err)
		targetOK[tkey{p.baseOf(t), t.route}] = false
	}

	for _, t := range targets {
		base, service, route := p.baseOf(t), t.service, t.route
		var rs routeStateResp
		if err := p.getJSON(ctx, base, "/"+route+"/_route_state", &rs); err != nil {
			fail(t, "route_state", err)
		} else {
			p.m.activeLevel.WithLabelValues(service, route).Set(rs.ActiveLevel)
			p.m.activeLimit.WithLabelValues(service, route).Set(rs.Limit)
			p.m.healthyPeers.WithLabelValues(service, route).Set(rs.HealthyPeersInLevel)
			for prio, bucket := range rs.ByPriority {
				for _, pr := range bucket.Peers {
					l := prometheus.Labels{"service": service, "route": route, "peer": pr.Peer, "name": pr.Name, "priority": prio}
					p.m.peerActive.With(l).Set(pr.Active)
					p.m.peerBanned.With(l).Set(b2f(pr.Banned))
					p.m.peerMax.With(l).Set(pr.Max)
				}
			}
		}

		var ts tpsStatusResp
		if err := p.getJSON(ctx, base, "/"+route+"/_tps_status", &ts); err != nil {
			fail(t, "tps_status", err)
		} else {
			p.m.tpsActive.WithLabelValues(service, route).Set(b2f(ts.Active))
			setModelMap(p.m.tpsEwma, service, route, ts.EwmaTps)
			setModelMap(p.m.adaptiveCC, service, route, ts.AdaptiveCC)
			setModelMap(p.m.adaptiveCCMin, service, route, ts.AdaptiveCCMin)
			setModelMap(p.m.adaptiveCCMax, service, route, ts.AdaptiveCCMax)
			setModelMap(p.m.adaptiveCCInit, service, route, ts.AdaptiveCCInit)
			setModelMap(p.m.adaptiveCCConc, service, route, ts.AdaptiveCCConc)
			setModelMap(p.m.adaptiveCCRej, service, route, ts.AdaptiveCCRej)
		}

		var tt ttftStatusResp
		if err := p.getJSON(ctx, base, "/"+route+"/_ttft_status", &tt); err != nil {
			fail(t, "ttft_status", err)
		} else {
			p.m.ttftActive.WithLabelValues(service, route).Set(b2f(tt.Active))
			setModelMap(p.m.ttftEwma, service, route, tt.EwmaMs)
		}

		// 429 counters: pull this route's own view (/<route>/_429_status, no ?all=1), like the other
		// endpoints, so a failure only marks this route down. The counters live in one dict per
		// openresty (reject_stat, key "<route>:<reason>") and ?all=1 would return every route at once,
		// but pulling that through one route coupled them all: a ModelRoute added before its engine is
		// ready has no server block yet and answers 502 -- measured on a production cluster, that one
		// cold-starting route fired OpenRestyPollDown for every route on its openresty.
		var rj rejectStatusResp
		if err := p.getJSON(ctx, base, "/"+route+"/_429_status", &rj); err != nil {
			fail(t, "429_status", err)
		} else {
			// ⚠️ First pre-initialise the series for this route x every reason to 0.
			//
			// Without it, a counter does not EXIST before its first Add, and openresty only puts an entry
			// in by_route once a 429 has actually happened -- so "no 429 at all" shows up as an EMPTY
			// vector, indistinguishable from "collection broke" (measured 2026-08-31: after openresty was
			// recreated, by_route={} and the metric vanished entirely). An empty vector is much worse than
			// a value of 0:
			//   - sum()/rate() over it is still empty, it does NOT become 0;
			//   - an alert expression built on it returns nothing -> never fires (not "judged 0, no alert":
			//     there is no value to judge at all);
			//   - the panel is blank and looks like monitoring is down.
			// Contrast: adaptive_cc_rej from the same poll is a gauge, Set(0) every round, so it always has
			// a series.
			//
			// Done every round rather than once at startup: routes are discovered dynamically, and a new
			// route needs its zeros too. Add(0) has no effect on an existing series (a counter never goes
			// back) and creates one that does not exist.
			//
			// -- A deliberate trade-off: route is a DYNAMIC label, yet this uses pre-initialisation, a
			//    technique meant for FINITE labels known at compile time. Not an oversight -- a choice
			//    made after weighing it, written down here so the next person does not take it for one.
			//
			// The strictly standard way is two layers:
			//    reason (3 values, known at compile time) -> pre-initialise;
			//    route (discovered dynamically)          -> export only the current ones and Delete the
			//                                                gone ones, so Prometheus staleness applies.
			// This code base does exactly that for gauges (resettable + Reset every round). Counters cannot
			// be Reset (it would break monotonicity); the counterpart is DeleteLabelValues.
			//
			// That step is NOT taken here, because it costs more than it gains:
			//  · what is left behind is only a flat line at 0, which nothing consumes today (zero
			//    references in the alert rules);
			//  · it is bounded and heals itself -- client_golang keeps child metrics in memory and keeps
			//    exporting them (measured: still collected after Add stops, only Delete removes them), but
			//    an exporter restart on a chart upgrade clears them;
			//  · routes are rarely added or removed (a ModelRoute is a stable object, not a pod);
			//  · and Delete would drop the accumulated value when a route is briefly recreated (e.g. a
			//    ModelRoute deleted and created again), so increase() would undercount across that point --
			//    trading real data continuity for removing a flat line nobody looks at is not worth it.
			//  · Besides, refresh() KEEPS the last mapping on an apiserver error instead of clearing it, so
			//    jitter by itself does not make routes disappear -- which narrows what Delete could save.
			//
			// [When to come back and add Delete]: once routes are added and removed often (e.g. ModelRoutes
			// created per tenant), or once an alert or autoscaling starts consuming this metric -- then a
			// flat line at 0 becomes misleading.
			for _, reason := range rejectReasons {
				p.m.rejected.WithLabelValues(service, route, reason).Add(0)
			}
			for reason, cur := range rj.ByRoute[route] { // ByRoute holds only this route without ?all=1
				key := base + "|" + route + "|" + reason
				last := p.prev[key]
				if cur >= last {
					p.m.rejected.WithLabelValues(service, route, reason).Add(cur - last)
				} else { // openresty restarted -> reject_stat reset to zero: count the whole value as the increase
					p.m.rejected.WithLabelValues(service, route, reason).Add(cur)
				}
				p.prev[key] = cur
			}
		}
	}

	now := float64(time.Now().Unix())
	for _, t := range targets {
		if targetOK[tkey{p.baseOf(t), t.route}] {
			p.m.pollUp.WithLabelValues(t.service, t.route).Set(1)
			p.m.pollLastOK.WithLabelValues(t.service, t.route).Set(now)
		} else {
			p.m.pollUp.WithLabelValues(t.service, t.route).Set(0)
		}
	}
}

func (p *orPoller) pollErr(what, route string, err error) {
	p.m.pollErrors.Inc()
	log.Printf("openresty-poll: %s route=%s: %v", what, route, err)
}

func (p *orPoller) getJSON(ctx context.Context, base, path string, v interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK { // 503 no-peers 等 → 跳过该端点(gauge 已清空)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, v)
}

// setModelMap:把 {model: *val} 写进 {service,route,model} gauge;nil(未初始化)跳过,不造 0 误导。
func setModelMap(g *prometheus.GaugeVec, service, route string, mm map[string]*float64) {
	for model, v := range mm {
		if v != nil {
			g.WithLabelValues(service, route, model).Set(*v)
		}
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
