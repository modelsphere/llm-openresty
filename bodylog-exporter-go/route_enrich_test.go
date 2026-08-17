package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestBackendIP(t *testing.T) {
	cases := map[string]string{
		"10.0.0.5:8050":            "10.0.0.5",
		"10.0.0.5:8050 (retry#1)":  "10.0.0.5",
		"10.102.29.46:8071":             "10.102.29.46",
		"  10.0.0.5:8050  ":          "10.0.0.5",
		"(none)":                        "(none)",
		"":                              "",
	}
	for in, want := range cases {
		if got := backendIP(in); got != want {
			t.Errorf("backendIP(%q) = %q, 想要 %q", in, got, want)
		}
	}
}

func TestSplitNsName(t *testing.T) {
	cases := []struct{ in, ns, name string }{
		{"model-service/fallback-model-service-01", "model-service", "fallback-model-service-01"},
		{"llm-route/openresty", "llm-route", "openresty"},
		{"bare-name", "default", "bare-name"},
	}
	for _, c := range cases {
		ns, name := splitNsName(c.in)
		if ns != c.ns || name != c.name {
			t.Errorf("splitNsName(%q) = (%q,%q), 想要 (%q,%q)", c.in, ns, name, c.ns, c.name)
		}
	}
}

// mrFull:两个 route,各指一个 discovery.service;第三个没 nginx.route(monitor-only,应跳过)。
const mrFullJSON = `{"items":[
  {"spec":{"nginx":{"route":"fallback-model-service-0.1"},"monitor":{"model":"fallback-model-service-0.1"},"discovery":{"service":"model-service/fallback-model-service-01"}}},
  {"spec":{"nginx":{"route":"kimi-k2.5"},"monitor":{"model":"kimi-k2.5"},"discovery":{"service":"kimi/k25-svc"}}},
  {"spec":{"monitor":{"model":"m-only"}}}
]}`

// 两个 service 各自的 EndpointSlice(fallback 两个 pod、kimi 一个 pod)。
func esJSON(ips ...string) string {
	var q []string
	for _, ip := range ips {
		q = append(q, `"`+ip+`"`)
	}
	return `{"items":[{"endpoints":[{"addresses":[` + strings.Join(q, ",") + `]}]}]}`
}

func newTestResolver(t *testing.T, srv *httptest.Server) *podRouteResolver {
	t.Helper()
	r := newPodRouteResolver(prometheus.NewRegistry())
	r.apiBase = srv.URL
	r.client = srv.Client()
	r.tokenPath = "/nonexistent-token" // mock 不校验
	return r
}

func TestResolverBuild(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/apis/routing.gpucluster.io/v1alpha1/modelroutes":
			_, _ = w.Write([]byte(mrFullJSON))
		case r.URL.Path == "/apis/discovery.k8s.io/v1/namespaces/model-service/endpointslices":
			if got := r.URL.Query().Get("labelSelector"); got != "kubernetes.io/service-name=fallback-model-service-01" {
				t.Errorf("labelSelector = %q", got)
			}
			_, _ = w.Write([]byte(esJSON("10.0.0.5", "10.0.0.5")))
		case r.URL.Path == "/apis/discovery.k8s.io/v1/namespaces/kimi/endpointslices":
			_, _ = w.Write([]byte(esJSON("10.0.0.5")))
		default:
			t.Errorf("未预期路径: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	want := map[string]routeInfo{
		"10.0.0.5": {route: "fallback-model-service-0.1", service: "model-service/fallback-model-service-01"},
		"10.0.0.5":  {route: "fallback-model-service-0.1", service: "model-service/fallback-model-service-01"},
		"10.0.0.5": {route: "kimi-k2.5", service: "kimi/k25-svc"},
	}
	for ip, wi := range want {
		gotR, gotSvc, ok := r.Lookup(ip)
		if !ok || gotR != wi.route || gotSvc != wi.service {
			t.Errorf("Lookup(%q) = (%q,%q,%v), 想要 (%q,%q,true)", ip, gotR, gotSvc, ok, wi.route, wi.service)
		}
	}
	if _, _, ok := r.Lookup("10.0.0.1"); ok {
		t.Errorf("未知 IP 应 ok=false")
	}
	if _, _, ok := r.Lookup("m-only"); ok {
		t.Errorf("monitor-only(无 discovery.service)不应进映射")
	}
}

// labelVals:从已 gather 的指标族里找第一个匹配 backend 的 series,返回其全部 label(name→value)。
func labelVals(t *testing.T, reg *prometheus.Registry, metric, backend string) (map[string]string, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metric {
			continue
		}
		for _, m := range mf.GetMetric() {
			lbl := map[string]string{}
			for _, lp := range m.GetLabel() {
				lbl[lp.GetName()] = lp.GetValue()
			}
			if lbl["backend"] == backend {
				return lbl, true
			}
		}
	}
	return nil, false
}

func TestObserveRouteLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	// 直接塞 byIP(不走 http),验证 observe 会据后端 IP 打 service + route。
	m.resolver = &podRouteResolver{byIP: map[string]routeInfo{
		"10.0.0.5": {route: "fallback-model-service-0.1", service: "model-service/fallback-model-service-01"},
	}}

	// backend 带 :port + retry 后缀,model 抓空 → service/route 命中富化,model 保持 unknown(不兜底)。
	m.observe(detailRecord{Backend: "10.0.0.5:8050 (retry#1)", Model: "", Status: 200, PromptTokens: 10})
	lbl, ok := labelVals(t, reg, "bodylog_requests_total", "10.0.0.5:8050 (retry#1)")
	if !ok {
		t.Fatal("找不到该 backend 的 series")
	}
	if lbl["service"] != "model-service/fallback-model-service-01" {
		t.Errorf("service = %q,想要 model-service/fallback-model-service-01", lbl["service"])
	}
	if lbl["route"] != "fallback-model-service-0.1" {
		t.Errorf("route = %q,想要 fallback-model-service-0.1", lbl["route"])
	}
	if lbl["model"] != "unknown" {
		t.Errorf("model = %q,想要 unknown(不做兜底)", lbl["model"])
	}

	// 未命中的后端 → service/route=unknown,model 保留客户端发的值。
	m.observe(detailRecord{Backend: "10.0.0.1:8050", Model: "qwen", Status: 200})
	lbl2, ok := labelVals(t, reg, "bodylog_requests_total", "10.0.0.1:8050")
	if !ok || lbl2["service"] != "unknown" || lbl2["route"] != "unknown" || lbl2["model"] != "qwen" {
		t.Errorf("未命中 series = %v,想要 service=unknown route=unknown model=qwen", lbl2)
	}
}
