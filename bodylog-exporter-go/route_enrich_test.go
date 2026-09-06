package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestBackendIP(t *testing.T) {
	cases := map[string]string{
		"10.0.0.5:8050":           "10.0.0.5",
		"10.0.0.5:8050 (retry#1)": "10.0.0.5",
		"10.102.29.46:8071":            "10.102.29.46",
		"  10.0.0.5:8050  ":         "10.0.0.5",
		"(none)":                       "(none)",
		"":                             "",
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

func TestNormalizeServiceLabel(t *testing.T) {
	cases := []struct{ in, want string }{
		{"kimi/kimi-k25-leader", "kimi/kimi-k25"},                                  // LWS leader → 逻辑服务名
		{"model-service/fallback-model-service-01", "model-service/fallback-model-service-01"}, // 非 LWS,原样
		{"kimi-k25-leader", "kimi-k25"},                                            // 无 ns 也剥
		{"kimi/kimi-k25", "kimi/kimi-k25"},                                         // 已无后缀,不动
		{"foo-leader/bar", "foo-leader/bar"},                                       // "-leader" 在 ns 段(结尾是 bar)→ 不误剥
	}
	for _, c := range cases {
		if got := normalizeServiceLabel(c.in); got != c.want {
			t.Errorf("normalizeServiceLabel(%q) = %q, 想要 %q", c.in, got, c.want)
		}
	}
}

// mrFull:两个 route,各指一个 discovery.service;第三个没 nginx.route(monitor-only,应跳过)。
const mrFullJSON = `{"items":[
  {"spec":{"nginx":{"route":"fallback-model-service-0.1"},"monitor":{"model":"fallback-model-service-0.1"},"discovery":{"service":"model-service/fallback-model-service-01"}}},
  {"spec":{"nginx":{"route":"kimi-k2.5"},"monitor":{"model":"kimi-k2.5"},"discovery":{"service":"kimi/k25-svc"}}},
  {"spec":{"monitor":{"model":"m-only"}}}
]}`

// 某 service 的 EndpointSlice;每个 spec 是一个 endpoint 的 IP,带 "!" 后缀=未就绪(conditions.ready=false)。
func esJSON(specs ...string) string {
	var eps []string
	for _, s := range specs {
		ready, ip := "true", s
		if strings.HasSuffix(s, "!") {
			ready, ip = "false", strings.TrimSuffix(s, "!")
		}
		eps = append(eps, `{"addresses":["`+ip+`"],"conditions":{"ready":`+ready+`}}`)
	}
	return `{"items":[{"endpoints":[` + strings.Join(eps, ",") + `]}]}`
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
			// 两个后端,第二个未就绪 → total=2、ready=1;映射仍应含两者。
			_, _ = w.Write([]byte(esJSON("10.0.0.5", "10.0.0.5!")))
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

	// replicas(总数,含未就绪) vs replicas_ready(就绪):fallback 2/1,kimi 1/1。
	if n := testutil.ToFloat64(r.replicas.WithLabelValues("model-service/fallback-model-service-01", "fallback-model-service-0.1")); n != 2 {
		t.Errorf("fallback replicas(total) = %v, 想要 2", n)
	}
	if n := testutil.ToFloat64(r.replicasReady.WithLabelValues("model-service/fallback-model-service-01", "fallback-model-service-0.1")); n != 1 {
		t.Errorf("fallback replicas_ready = %v, 想要 1", n)
	}
	if n := testutil.ToFloat64(r.replicas.WithLabelValues("kimi/k25-svc", "kimi-k2.5")); n != 1 {
		t.Errorf("kimi replicas(total) = %v, 想要 1", n)
	}
	if n := testutil.ToFloat64(r.replicasReady.WithLabelValues("kimi/k25-svc", "kimi-k2.5")); n != 1 {
		t.Errorf("kimi replicas_ready = %v, 想要 1", n)
	}

	// poll 视角(收敛后同一发现器产出):routes(过滤+去重+排序)+ route→service。
	if got, want := r.getRoutes(), []string{"fallback-model-service-0.1", "kimi-k2.5"}; !reflect.DeepEqual(got, want) {
		t.Errorf("getRoutes() = %v, 想要 %v", got, want)
	}
	if got := r.serviceFor("fallback-model-service-0.1"); got != "model-service/fallback-model-service-01" {
		t.Errorf("serviceFor(fallback) = %q", got)
	}
	if got := r.serviceFor("nonexist"); got != "" {
		t.Errorf("serviceFor(未知) 应空, 得 %q", got)
	}
}

// nginxService 过滤:只有 nginx.service 匹配的 route 进 poll 列表;但 svc map 含所有 route。
func TestResolverRouteFilter(t *testing.T) {
	const mr = `{"items":[
	  {"spec":{"nginx":{"route":"a","service":"llm-route/openresty"},"discovery":{"service":"ns/a-svc"}}},
	  {"spec":{"nginx":{"route":"b","service":"llm-route/other"},"discovery":{"service":"ns/b-svc"}}}
	]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "endpointslices") {
			_, _ = w.Write([]byte(esJSON())) // 空端点(不影响本测试)
			return
		}
		_, _ = w.Write([]byte(mr))
	}))
	defer srv.Close()

	r := newTestResolver(t, srv)
	r.nginxService = "llm-route/openresty"
	r.refresh(context.Background())

	if got, want := r.getRoutes(), []string{"a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("过滤后 getRoutes() = %v, 想要 [a]", got)
	}
	if got := r.serviceFor("b"); got != "ns/b-svc" { // 被 poll 过滤掉,但 svc map 仍含它
		t.Errorf("serviceFor(b) = %q, 想要 ns/b-svc(svc map 不受 poll 过滤影响)", got)
	}
}

// apiserver 报错时:保留上次成功的 routes/映射,不清空(避免抖动全失明)。
func TestResolverKeepsLastOnError(t *testing.T) {
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.Contains(r.URL.Path, "endpointslices") {
			_, _ = w.Write([]byte(esJSON("10.0.0.5")))
			return
		}
		_, _ = w.Write([]byte(mrFullJSON))
	}))
	defer srv.Close()

	r := newTestResolver(t, srv)
	r.refresh(context.Background())
	before := r.getRoutes()
	fail = true
	r.refresh(context.Background()) // 这轮失败
	if got := r.getRoutes(); !reflect.DeepEqual(got, before) {
		t.Errorf("出错后 routes 应保留 %v, 实际 %v", before, got)
	}
	if _, _, ok := r.Lookup("10.0.0.5"); !ok {
		t.Errorf("出错后 byIP 映射应保留")
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

// esWithPods:带 targetRef 的 EndpointSlice —— 期望副本数要靠它拿到一个 pod 名做上溯入口。
// 每个 spec 形如 "ip|podName",带 "!" 后缀=未就绪。
func esWithPods(specs ...string) string {
	var eps []string
	for _, s := range specs {
		ready := "true"
		if strings.HasSuffix(s, "!") {
			ready, s = "false", strings.TrimSuffix(s, "!")
		}
		ip, pod, _ := strings.Cut(s, "|")
		eps = append(eps, `{"addresses":["`+ip+`"],"conditions":{"ready":`+ready+
			`},"targetRef":{"kind":"Pod","name":"`+pod+`"}}`)
	}
	return `{"items":[{"endpoints":[` + strings.Join(eps, ",") + `]}]}`
}

func ownerJSON(apiVersion, kind, name string) string {
	return `{"metadata":{"ownerReferences":[{"apiVersion":"` + apiVersion +
		`","kind":"` + kind + `","name":"` + name + `","controller":true}]}}`
}

// 期望副本数必须来自【顶层】工作负载。
//
// 回归的是一个真实误报:LWS 滚动更新时,LWS 控制器按 maxSurge 把 leader StatefulSet 的
// spec.replicas 抬高(实测 surge 中 sts=3 而 lws=2)。上溯若停在 StatefulSet 就会拿到 3,
// 于是「就绪 2 < 总数 3」被判成降级 —— 而服务其实是满的。
func TestDesiredReplicasWalksToTop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/routing.gpucluster.io/v1alpha1/modelroutes":
			_, _ = w.Write([]byte(mrFullJSON))

		// ── LWS 侧:pod → StatefulSet(surge 到 3) → LeaderWorkerSet(2) ──
		case "/apis/discovery.k8s.io/v1/namespaces/kimi/endpointslices":
			_, _ = w.Write([]byte(esWithPods("10.0.0.5|kimi-k25-0", "10.0.0.5|kimi-k25-1!")))
		case "/api/v1/namespaces/kimi/pods/kimi-k25-0":
			_, _ = w.Write([]byte(ownerJSON("apps/v1", "StatefulSet", "kimi-k25")))
		case "/apis/apps/v1/namespaces/kimi/statefulsets/kimi-k25":
			// spec.replicas=3 是被 surge 抬高的值,取到它就是 bug。
			_, _ = w.Write([]byte(`{"metadata":{"ownerReferences":[{"apiVersion":"leaderworkerset.x-k8s.io/v1","kind":"LeaderWorkerSet","name":"kimi-k25","controller":true}]},"spec":{"replicas":3}}`))
		case "/apis/leaderworkerset.x-k8s.io/v1/namespaces/kimi/leaderworkersets/kimi-k25":
			_, _ = w.Write([]byte(`{"metadata":{},"spec":{"replicas":2}}`))

		// ── Deployment 侧:pod → ReplicaSet(旧 RS 残值 7) → Deployment(20) ──
		case "/apis/discovery.k8s.io/v1/namespaces/model-service/endpointslices":
			_, _ = w.Write([]byte(esWithPods("10.0.0.5|fb-abc-1")))
		case "/api/v1/namespaces/model-service/pods/fb-abc-1":
			_, _ = w.Write([]byte(ownerJSON("apps/v1", "ReplicaSet", "fb-abc")))
		case "/apis/apps/v1/namespaces/model-service/replicasets/fb-abc":
			_, _ = w.Write([]byte(`{"metadata":{"ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"fb","controller":true}]},"spec":{"replicas":7}}`))
		case "/apis/apps/v1/namespaces/model-service/deployments/fb":
			_, _ = w.Write([]byte(`{"metadata":{},"spec":{"replicas":20}}`))

		default:
			t.Errorf("未预期路径: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	if n := testutil.ToFloat64(r.replicasDesired.WithLabelValues("kimi/k25-svc", "kimi-k2.5")); n != 2 {
		t.Errorf("kimi desired = %v, 想要 2(LWS 的值,不是被 surge 抬高的 StatefulSet 的 3)", n)
	}
	// 实际/就绪仍按 EndpointSlice 口径:2 个 endpoint、1 个就绪。降级判定看 ready(1) < desired(2) → 确实降级。
	if n := testutil.ToFloat64(r.replicas.WithLabelValues("kimi/k25-svc", "kimi-k2.5")); n != 2 {
		t.Errorf("kimi replicas(实际) = %v, 想要 2", n)
	}
	if n := testutil.ToFloat64(r.replicasDesired.WithLabelValues("model-service/fallback-model-service-01", "fallback-model-service-0.1")); n != 20 {
		t.Errorf("model-service desired = %v, 想要 20(Deployment 的值,不是旧 ReplicaSet 的 7)", n)
	}
}

// 拿不到期望副本数时,不发这条 series —— 发 0 会被读成「缩容到零」,比缺点危险得多。
func TestDesiredReplicasAbsentWhenUnknown(t *testing.T) {
	const mr = `{"items":[{"spec":{"nginx":{"route":"bare"},"discovery":{"service":"ns/bare-svc"}}}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/routing.gpucluster.io/v1alpha1/modelroutes":
			_, _ = w.Write([]byte(mr))
		case "/apis/discovery.k8s.io/v1/namespaces/ns/endpointslices":
			_, _ = w.Write([]byte(esWithPods("10.0.0.5|bare-pod")))
		case "/api/v1/namespaces/ns/pods/bare-pod":
			_, _ = w.Write([]byte(`{"metadata":{}}`)) // 无 ownerReferences:裸 pod,没有期望副本数可言
		default:
			t.Errorf("未预期路径: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	if n := testutil.CollectAndCount(r.replicasDesired); n != 0 {
		t.Errorf("desired series 数 = %d, 想要 0(裸 pod 查不到期望副本数就不该发点)", n)
	}
	if n := testutil.ToFloat64(r.replicas.WithLabelValues("ns/bare-svc", "bare")); n != 1 {
		t.Errorf("实际副本数仍应正常上报, 得 %v", n)
	}
}

// video 等纯反向代理的 route 不进 poll 列表(它们没有 lua 引擎的 _route_state/_tps_status/
// _ttft_status,poll 必然失败;而 openresty_poll_up 是全局的,一条失败就把整个 openresty
// 监控打成盲区 —— 生产 2026-09-03 实拍过)。但它们仍要留在富化/副本统计里。
func TestResolverSkipsNonLLMRoutesForPoll(t *testing.T) {
	const mr = `{"items":[
	  {"spec":{"nginx":{"route":"kimi"},"discovery":{"service":"ns/kimi-svc"}}},
	  {"spec":{"modelType":"llm","nginx":{"route":"glm"},"discovery":{"service":"ns/glm-svc"}}},
	  {"spec":{"modelType":"video","nginx":{"route":"minimax-h3"},"discovery":{"service":"ns/h3-router"}}}
	]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "endpointslices") {
			_, _ = w.Write([]byte(esJSON("10.0.0.1")))
			return
		}
		_, _ = w.Write([]byte(mr))
	}))
	defer srv.Close()

	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	// modelType 空 = llm(CRD 默认),显式 llm 也要;video 不要。
	if got, want := r.getRoutes(), []string{"glm", "kimi"}; !reflect.DeepEqual(got, want) {
		t.Errorf("getRoutes() = %v, 想要 %v(video 路由不该进 poll 列表)", got, want)
	}
	// 但富化仍要覆盖 video —— bodylog 的 service label 和副本指标都靠它。
	if got := r.serviceFor("minimax-h3"); got != "ns/h3-router" {
		t.Errorf("serviceFor(minimax-h3) = %q, 想要 ns/h3-router(video 只是不 poll,不是不认)", got)
	}
}
