package main

import (
	"context"
	"net/http"
	"net/http/httptest"
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
