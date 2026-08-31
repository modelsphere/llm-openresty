// openresty-poll:把【k8s 集群内那台 openresty】的实时路由/限流状态(shared dict)
// 翻译成 Prometheus 指标。不改 openresty —— 只 GET 它已经暴露的 JSON 调试端点:
//
//	/<route>/_route_state      每 peer 当前并发 / 是否被 ban / max / priority / active_level
//	/<route>/_tps_status       自适应并发 adaptive_cc(当前动态上限/min/max/当前并发/本区间被压需求)+ 解码速率 EWMA
//	/<route>/_ttft_status      TTFT EWMA + 限流是否 active
//	/<route>/_429_status?all=1 429 限流累计(route × reason=concurrency/ttft/tps)—— 全局,poll 一次即可
//
// 与 tail 文件那套的分工:tail = 事后逐请求(counter/histogram,零丢);poll = 即时控制面态(gauge,快照)。
// 这些是 bodylog 拿不到的 live 信号(每 peer 并发/被 ban 的 peer/限流档位),对自动扩缩容反应更快。
//
// 只 poll 一台(k8s openresty Service;HA 时 Service 只选 active leader → 天然单逻辑目标),不带 instance label。
// URL 空 = 整个模块不启用(裸机 ts31 exporter 够不到 k8s,默认关)。
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
	adaptiveCCConc *prometheus.GaugeVec // {route,model}  当前并发(timer 判压力用的实时在途)
	adaptiveCCRej  *prometheus.GaugeVec // {route,model}  本区间被压抑需求(并发 429 数)
	// /_ttft_status
	ttftActive *prometheus.GaugeVec // {route}        TTFT 限流是否生效(1/0)
	ttftEwma   *prometheus.GaugeVec // {route,model}  TTFT EWMA(ms)
	// /_429_status(counter:delta 累加,跨 poll 单调)
	rejected *prometheus.CounterVec // {route,reason}
	// poll 自监控
	pollUp     prometheus.Gauge   // 上轮 poll 是否全成功(1/0)
	pollErrors prometheus.Counter // poll 出错累计
	pollLastOK prometheus.Gauge   // 上次成功 poll 的 unix 秒

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
		adaptiveCCConc: g("openresty_adaptive_cc_conc", "当前并发(timer 判压力用的实时在途)", srm...),
		adaptiveCCRej:  g("openresty_adaptive_cc_rej", "本区间被压抑需求(并发 429 数)", srm...),

		ttftActive: g("openresty_ttft_limiter_active", "TTFT 限流是否生效(1/0)", sroute...),
		ttftEwma:   g("openresty_ttft_ewma_ms", "TTFT EWMA(ms)", srm...),

		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "openresty_rejected_total", Help: "429 限流累计(按 service × route × reason=concurrency/ttft/tps)",
		}, []string{"service", "route", "reason"}),

		pollUp:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "openresty_poll_up", Help: "上轮 openresty poll 是否全成功(1/0)"}),
		pollErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "openresty_poll_errors_total", Help: "openresty poll 出错累计"}),
		pollLastOK: prometheus.NewGauge(prometheus.GaugeOpts{Name: "openresty_poll_last_success_seconds", Help: "上次成功 poll 的 unix 秒"}),
	}
	m.resettable = []*prometheus.GaugeVec{
		m.activeLevel, m.activeLimit, m.healthyPeers, m.peerActive, m.peerBanned, m.peerMax,
		m.tpsActive, m.tpsEwma, m.adaptiveCC, m.adaptiveCCMin, m.adaptiveCCMax, m.adaptiveCCConc, m.adaptiveCCRej,
		m.ttftActive, m.ttftEwma,
	}
	for _, gv := range m.resettable {
		reg.MustRegister(gv)
	}
	reg.MustRegister(m.rejected, m.pollUp, m.pollErrors, m.pollLastOK)
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
	cfg      orConfig
	m        *orMetrics
	client   *http.Client
	routesFn func() []string     // 当前要 poll 的 route 集合(静态或 k8s 动态发现)
	svcFn    func(string) string // route → discovery.service(ns/name);nil 或返回 "" → service=unknown
	prev     map[string]float64  // 429 counter delta 追踪:key="route|reason" → 上次绝对值
}

func newORPoller(cfg orConfig, m *orMetrics, routesFn func() []string, svcFn func(string) string) *orPoller {
	return &orPoller{
		cfg:      cfg,
		m:        m,
		client:   &http.Client{Timeout: cfg.timeout},
		routesFn: routesFn,
		svcFn:    svcFn,
		prev:     map[string]float64{},
	}
}

// serviceOf:route → service label 值。svcFn 缺省或未知 → "unknown"(与 bodylog_* 对齐)。
func (p *orPoller) serviceOf(route string) string {
	if p.svcFn == nil {
		return "unknown"
	}
	if s := p.svcFn(route); s != "" {
		return s
	}
	return "unknown"
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
	routes := p.routesFn() // 动态:route 增删自动跟随(下线的 route gauge 因上面 Reset 消失)
	ok := true

	for _, route := range routes {
		service := p.serviceOf(route)
		var rs routeStateResp
		if err := p.getJSON(ctx, "/"+route+"/_route_state", &rs); err != nil {
			p.pollErr("route_state", route, err)
			ok = false
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
		if err := p.getJSON(ctx, "/"+route+"/_tps_status", &ts); err != nil {
			p.pollErr("tps_status", route, err)
			ok = false
		} else {
			p.m.tpsActive.WithLabelValues(service, route).Set(b2f(ts.Active))
			setModelMap(p.m.tpsEwma, service, route, ts.EwmaTps)
			setModelMap(p.m.adaptiveCC, service, route, ts.AdaptiveCC)
			setModelMap(p.m.adaptiveCCMin, service, route, ts.AdaptiveCCMin)
			setModelMap(p.m.adaptiveCCMax, service, route, ts.AdaptiveCCMax)
			setModelMap(p.m.adaptiveCCConc, service, route, ts.AdaptiveCCConc)
			setModelMap(p.m.adaptiveCCRej, service, route, ts.AdaptiveCCRej)
		}

		var tt ttftStatusResp
		if err := p.getJSON(ctx, "/"+route+"/_ttft_status", &tt); err != nil {
			p.pollErr("ttft_status", route, err)
			ok = false
		} else {
			p.m.ttftActive.WithLabelValues(service, route).Set(b2f(tt.Active))
			setModelMap(p.m.ttftEwma, service, route, tt.EwmaMs)
		}
	}

	// 429 是全局聚合(reject_stat 全局 dict),用任一 route 路径 + ?all=1 拉一次即可。
	if len(routes) > 0 {
		var rj rejectStatusResp
		if err := p.getJSON(ctx, "/"+routes[0]+"/_429_status?all=1", &rj); err != nil {
			p.pollErr("429_status", "*", err)
			ok = false
		} else {
			// ⚠️ 先把「当前所有 route x 所有 reason」的 series 预初始化成 0。
			//
			// 不这么做的话:counter 在第一次 Add 之前【不存在】,而 openresty 只在真发生 429 时
			// 才往 by_route 里放条目 —— 于是「一次 429 都没有」表现为【空 vector】,与「采集断了」
			// 完全无法区分(实测 2026-08-31:openresty 重建后 by_route={},该指标整个消失)。
			// 空 vector 的后果比"值为 0"严重得多:
			//   - sum()/rate() 套上去仍是空,**不会**变成 0;
			//   - 基于它的告警表达式返回空 → 永远不触发(不是判为 0 不告警,是根本没值可判);
			//   - 面板空白,看起来像监控挂了。
			// 对照:同一次 poll 的 adaptive_cc_rej 是 gauge、每轮 Set(0),所以恒有 series。
			//
			// 放在每轮而不是启动时一次:route 是动态发现的,新 route 上线也要补零。
			// Add(0) 对已存在的 series 无副作用(counter 不倒退),对不存在的则创建出来。
			//
			// ── 一个刻意的取舍:route 是【动态】标签,却仍用了「预初始化」这个本该给
			//    【编译期已知的有限标签】用的手法。不是疏忽,是权衡后的选择,记在这里免得
			//    下一个人以为是漏了。
			//
			// 严格的标准做法是两段叠加:
			//    reason(3 个值,编译期已知) → 预初始化;
			//    route (动态发现)          → 只导出当前存在的,消失的用 Delete 摘掉,
			//                                让 Prometheus 的 staleness 机制生效。
			// 本代码库对 gauge 正是这么做的(resettable + 每轮 Reset)。counter 不能 Reset
			// (会破坏单调性),对应手段是 DeleteLabelValues。
			//
			// 这里【不做】那一步,因为代价大于收益:
			//  · 残留的表现只是「一条恒 0 的平线」,当前无任何消费方(告警规则里零引用);
			//  · 残留有界且自愈 —— client_golang 把子指标常驻内存、会一直导出(实测:停止
			//    Add 后仍被 Collect,只有 Delete 能摘),但 exporter 随 chart 升级重启即清空;
			//  · route 极少增删(ModelRoute 是稳定对象,不是 pod);
			//  · 而 Delete 会在 route 短暂重建(如 MR 先删后建)时丢掉累计值,increase() 跨
			//    该点少算 —— 为清一条无人看的平线,换来真实数据的不连续,不划算。
			//  · 另外 refresh() 在 apiserver 出错时【保留上次映射】而不是清空,所以抖动本身
			//    不会让 route 消失 —— 这也让 Delete 能挽回的场景进一步变窄。
			//
			// 【什么时候该回来补上 Delete】:route 变得频繁增删(如按租户动态建 ModelRoute),
			// 或有告警/扩缩容开始消费本指标 —— 那时恒 0 的平线会变成误导。
			for _, route := range routes {
				service := p.serviceOf(route)
				for _, reason := range rejectReasons {
					p.m.rejected.WithLabelValues(service, route, reason).Add(0)
				}
			}
			for route, byReason := range rj.ByRoute {
				service := p.serviceOf(route)
				for reason, cur := range byReason {
					key := route + "|" + reason
					last := p.prev[key]
					if cur >= last {
						p.m.rejected.WithLabelValues(service, route, reason).Add(cur - last)
					} else { // openresty worker 重启 → reject_stat 清零:当整段增量补上
						p.m.rejected.WithLabelValues(service, route, reason).Add(cur)
					}
					p.prev[key] = cur
				}
			}
		}
	}

	if ok {
		p.m.pollUp.Set(1)
		p.m.pollLastOK.Set(float64(time.Now().Unix()))
	} else {
		p.m.pollUp.Set(0)
	}
}

func (p *orPoller) pollErr(what, route string, err error) {
	p.m.pollErrors.Inc()
	log.Printf("openresty-poll: %s route=%s: %v", what, route, err)
}

func (p *orPoller) getJSON(ctx context.Context, path string, v interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.baseURL+path, nil)
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
