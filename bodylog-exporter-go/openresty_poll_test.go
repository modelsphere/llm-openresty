package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// mock openresty:route "r1" 的四个调试端点。验证 poller 把 service label(来自 svcFn)打到各指标上。
func newMockOpenresty(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/r1/_route_state":
			_, _ = w.Write([]byte(`{"route":"r1","active_level":2,"limit":100,"healthy_peers_in_level":2,` +
				`"by_priority":{"2":{"peers":[{"name":"n1","peer":"10.0.0.5:8050","banned":false,"active":5,"max":50}]}}}`))
		case "/r1/_tps_status":
			_, _ = w.Write([]byte(`{"active":true,"ewma_tps":{"_":123.0},"adaptive_cc":{"_":45.0}}`))
		case "/r1/_ttft_status":
			_, _ = w.Write([]byte(`{"active":false,"ewma_ms":{"_":800.0}}`))
		case "/r1/_429_status":
			_, _ = w.Write([]byte(`{"by_route":{"r1":{"concurrency":10,"tps":0,"ttft":2}}}`))
		default:
			t.Errorf("未预期路径: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
}

func newTestPoller(srv *httptest.Server, m *orMetrics, svcFn func(string) string) *orPoller {
	cfg := orConfig{baseURL: srv.URL, interval: time.Second, timeout: 2 * time.Second}
	return newORPoller(cfg, m, func() []string { return []string{"r1"} }, svcFn)
}

// svcFn 提供 route→service 时:所有 openresty_* 打上该 service label。
func TestPollerServiceLabel(t *testing.T) {
	srv := newMockOpenresty(t)
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newORMetrics(reg)
	p := newTestPoller(srv, m, func(route string) string {
		if route == "r1" {
			return "ns/r1-svc"
		}
		return ""
	})
	p.pollOnce(context.Background())

	const svc, route = "ns/r1-svc", "r1"
	checks := []struct {
		name string
		val  float64
	}{
		{"active_level", testutil.ToFloat64(m.activeLevel.WithLabelValues(svc, route))},
		{"peer_active", testutil.ToFloat64(m.peerActive.WithLabelValues(svc, route, "10.0.0.5:8050", "n1", "2"))},
		{"tps_ewma", testutil.ToFloat64(m.tpsEwma.WithLabelValues(svc, route, "_"))},
		{"ttft_ewma", testutil.ToFloat64(m.ttftEwma.WithLabelValues(svc, route, "_"))},
		{"rejected_concurrency", testutil.ToFloat64(m.rejected.WithLabelValues(svc, route, "concurrency"))},
	}
	want := map[string]float64{"active_level": 2, "peer_active": 5, "tps_ewma": 123, "ttft_ewma": 800, "rejected_concurrency": 10}
	for _, c := range checks {
		if c.val != want[c.name] {
			t.Errorf("%s{service=%q,route=%q} = %v, 想要 %v", c.name, svc, route, c.val, want[c.name])
		}
	}
}

// svcFn=nil(静态 route 模式):service 回退 unknown。
func TestPollerServiceUnknown(t *testing.T) {
	srv := newMockOpenresty(t)
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newORMetrics(reg)
	newTestPoller(srv, m, nil).pollOnce(context.Background())

	if v := testutil.ToFloat64(m.activeLevel.WithLabelValues("unknown", "r1")); v != 2 {
		t.Errorf("svcFn=nil 时 active_level{service=unknown} = %v, 想要 2", v)
	}
	if v := testutil.ToFloat64(m.rejected.WithLabelValues("unknown", "r1", "ttft")); v != 2 {
		t.Errorf("svcFn=nil 时 rejected{service=unknown,reason=ttft} = %v, 想要 2", v)
	}
}

// 零 429 时,openresty_rejected_total 必须仍有 series(值 0),不能是空 vector。
//
// 回归的是一个真实故障(2026-08-31):openresty pod 重建后 _429_status 的 by_route 变成 {},
// exporter 只遍历 by_route → 一次 Add 都不调 → counter 在第一次 Add 前【不存在】→ 整个指标
// 从 Prometheus 消失。而空 vector 与「限流为 0」无法区分,后果比"值为 0"严重得多:
// sum()/rate() 套上去仍是空(不会变成 0),基于它的告警表达式返回空 → 永远不触发。
func TestPollerRejectedZeroInitialized(t *testing.T) {
	// by_route 为空 = 从未发生过 429,正是故障当时的现场
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/r1/_route_state":
			_, _ = w.Write([]byte(`{"route":"r1","active_level":2,"limit":100,"healthy_peers_in_level":1,"by_priority":{}}`))
		case "/r1/_tps_status":
			_, _ = w.Write([]byte(`{"active":true,"ewma_tps":{"_":1.0}}`))
		case "/r1/_ttft_status":
			_, _ = w.Write([]byte(`{"active":false,"ewma_ms":{"_":1.0}}`))
		case "/r1/_429_status":
			_, _ = w.Write([]byte(`{"by_route":{}}`)) // ← 空
		default:
			t.Errorf("未预期路径: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newORMetrics(reg)
	p := newTestPoller(srv, m, func(string) string { return "ns/svc" })
	p.pollOnce(context.Background())

	if n := testutil.CollectAndCount(m.rejected); n != len(rejectReasons) {
		t.Fatalf("rejected series 数 = %d, 想要 %d(每个 reason 一条,值 0)", n, len(rejectReasons))
	}
	for _, reason := range rejectReasons {
		if v := testutil.ToFloat64(m.rejected.WithLabelValues("ns/svc", "r1", reason)); v != 0 {
			t.Errorf("reason=%s 的值 = %v, 想要 0", reason, v)
		}
	}
}

// 预初始化不能把真实计数抹掉:有 429 时仍应累加出正确的值。
func TestPollerRejectedStillCounts(t *testing.T) {
	srv := newMockOpenresty(t)
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newORMetrics(reg)
	p := newTestPoller(srv, m, func(string) string { return "ns/svc" })
	p.pollOnce(context.Background())

	// mock 返回 concurrency=10 / tps=0 / ttft=2
	for reason, want := range map[string]float64{"concurrency": 10, "tps": 0, "ttft": 2} {
		if v := testutil.ToFloat64(m.rejected.WithLabelValues("ns/svc", "r1", reason)); v != want {
			t.Errorf("reason=%s 的值 = %v, 想要 %v", reason, v, want)
		}
	}
}

// 一条 route 挂了,不能把别的 route 也标成 down。
//
// 早先 openresty_poll_up 是全局单个 gauge(「上轮是否全成功」),任何一条 route 失败
// 就归零 —— 生产上一条纯反向代理的 video 路由(没有 lua 引擎端点、必然 poll 失败)
// 让 OpenRestyPollDown 连响 3 天,告警彻底失去信号价值(期间真出故障也分辨不出来)。
func TestPollUpIsPerRoute(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/bad/") { // bad 这条全端点 401,模拟无 lua 引擎/被鉴权拦下
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"error":"missing or invalid api key"}`))
			return
		}
		switch r.URL.Path {
		case "/good/_route_state":
			_, _ = w.Write([]byte(`{"route":"good","active_level":1,"limit":10,"healthy_peers_in_level":1}`))
		case "/good/_tps_status":
			_, _ = w.Write([]byte(`{"active":false,"ewma_tps":{"_":1.0},"adaptive_cc":{"_":2.0}}`))
		case "/good/_ttft_status":
			_, _ = w.Write([]byte(`{"active":false,"ewma_ms":{"_":100.0}}`))
		case "/good/_429_status":
			_, _ = w.Write([]byte(`{"by_route":{}}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newORMetrics(reg)
	cfg := orConfig{baseURL: srv.URL, interval: time.Second, timeout: 2 * time.Second}
	p := newORPoller(cfg, m, func() []string { return []string{"good", "bad"} }, nil)
	p.pollOnce(context.Background())

	if got := testutil.ToFloat64(m.pollUp.WithLabelValues("good")); got != 1 {
		t.Errorf("openresty_poll_up{route=good} = %v, 想要 1(它自己是好的,不该被 bad 连累)", got)
	}
	if got := testutil.ToFloat64(m.pollUp.WithLabelValues("bad")); got != 0 {
		t.Errorf("openresty_poll_up{route=bad} = %v, 想要 0", got)
	}
}
