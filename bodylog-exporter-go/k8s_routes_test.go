package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// 模拟 apiserver 返回的 ModelRouteList:qwen/opt 归本 openresty,glm 归另一台,
// monitoronly 没配 nginx.route,再来个重复 qwen(验证去重)。
const mrListJSON = `{"apiVersion":"routing.gpucluster.io/v1alpha1","kind":"ModelRouteList","items":[
  {"spec":{"nginx":{"route":"qwen","service":"llm-route/openresty"}}},
  {"spec":{"nginx":{"route":"opt","service":"llm-route/openresty"}}},
  {"spec":{"nginx":{"route":"glm","service":"llm-route/other-openresty"}}},
  {"spec":{"monitor":{"model":"m-only"}}},
  {"spec":{"nginx":{"route":"qwen","service":"llm-route/openresty"}}}
]}`

func newTestDiscoverer(t *testing.T, srv *httptest.Server, filter string) *routeDiscoverer {
	t.Helper()
	rd := newRouteDiscoverer(prometheus.NewRegistry(), discoverConfig{
		group: "routing.gpucluster.io", version: "v1alpha1", plural: "modelroutes",
		nginxService: filter,
	})
	rd.apiBase = srv.URL
	rd.client = srv.Client()
	rd.tokenPath = "/nonexistent-token" // 无 token 也能列(mock 不校验)
	return rd
}

func TestDiscoverRoutes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "/apis/routing.gpucluster.io/v1alpha1/modelroutes"; r.URL.Path != want {
			t.Errorf("路径 = %q, 想要 %q", r.URL.Path, want)
		}
		_, _ = w.Write([]byte(mrListJSON))
	}))
	defer srv.Close()

	// 无过滤:全要(含另一台 openresty 的 glm),去重 + 排序,跳过无 route 的。
	rd := newTestDiscoverer(t, srv, "")
	rd.refresh(context.Background())
	if got, want := rd.get(), []string{"glm", "opt", "qwen"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("无过滤 routes = %v, 想要 %v", got, want)
	}

	// 按 nginx.service 过滤:只要归本 openresty 的。
	rd2 := newTestDiscoverer(t, srv, "llm-route/openresty")
	rd2.refresh(context.Background())
	if got, want := rd2.get(), []string{"opt", "qwen"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("过滤 routes = %v, 想要 %v", got, want)
	}
}

// apiserver 报错时:保留上次成功的 routes,不清空(避免抖动全 route 失明)。
func TestDiscoverKeepsLastOnError(t *testing.T) {
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(mrListJSON))
	}))
	defer srv.Close()

	rd := newTestDiscoverer(t, srv, "llm-route/openresty")
	rd.refresh(context.Background())
	before := rd.get()
	fail = true
	rd.refresh(context.Background()) // 这轮失败
	if got := rd.get(); !reflect.DeepEqual(got, before) {
		t.Fatalf("出错后 routes 应保留 %v, 实际 %v", before, got)
	}
}
