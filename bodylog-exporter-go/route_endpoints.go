// 富化:把 bodylog 明细里的后端 pod IP 映射到它所属的 {route, model}。
//
// 为什么需要:bodylog 的 backend 是 openresty 转发到的真实后端 = k8s pod IP:port,pod 重建会漂移;
// 且明细的 model 字段常抓空(→unknown)。唯一稳定可靠的归属键是「这个 pod IP 属于哪个 ModelRoute」。
// 数据源:list ModelRoute CR 取 spec.discovery.service → 查该 Service 的 EndpointSlice 得 pod IP 集,
// 反建 map[podIP]{route,model}。周期刷新,pod 增删自动跟随。observe() 据此给 bodylog_* 打稳定的 route label。
//
// 与 k8s_routes.go 的 routeDiscoverer 是两码事:后者列 route 名给 openresty-poll 用;本解析器建 IP→route
// 反查表给 tail 明细指标用。二者都走 in-cluster SA REST(裸 net/http,不引 client-go)。
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// routeInfo:一个后端 pod 所属的 route 及其 service(来自它所在的 ModelRoute)。
// route   = spec.nginx.route(如 "fallback-model-service-0.1");
// service = spec.discovery.service(ns/name 形式,如 "model-service/fallback-model-service-01",用户一般按它聚合)。
type routeInfo struct{ route, service string }

type podRouteResolver struct {
	group, version, plural string
	interval, timeout      time.Duration
	apiBase                string
	tokenPath              string
	client                 *http.Client

	mu   sync.RWMutex
	byIP map[string]routeInfo

	up            prometheus.Gauge
	pods          prometheus.Gauge
	errs          prometheus.Counter
	replicas      *prometheus.GaugeVec // {service, route} → 后端 pod 总数(含未就绪)
	replicasReady *prometheus.GaugeVec // {service, route} → 就绪后端 pod 数
}

func newPodRouteResolver(reg *prometheus.Registry) *podRouteResolver {
	// 复用 route 发现的 env(group/version/plural/interval/timeout),与 openresty-poll 动态发现一致。
	interval := time.Duration(envOrInt("ROUTE_DISCOVERY_INTERVAL_SECONDS", 30)) * time.Second
	timeout := time.Duration(envOrInt("ROUTE_DISCOVERY_TIMEOUT_MS", 4000)) * time.Millisecond

	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	apiBase := "https://kubernetes.default.svc"
	if host != "" {
		if port == "" {
			port = "443"
		}
		apiBase = "https://" + host + ":" + port
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca, err := os.ReadFile(k8sCAPath); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(ca) {
			tlsCfg.RootCAs = pool
		}
	} else {
		log.Printf("route-enrich: 读 CA %s 失败(%v),TLS 用系统根", k8sCAPath, err)
	}
	r := &podRouteResolver{
		group:     envOr("MODELROUTE_GROUP", "routing.gpucluster.io"),
		version:   envOr("MODELROUTE_VERSION", "v1alpha1"),
		plural:    envOr("MODELROUTE_PLURAL", "modelroutes"),
		interval:  interval,
		timeout:   timeout,
		apiBase:   apiBase,
		tokenPath: k8sTokenPath,
		client:    &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: tlsCfg}},
		byIP:      map[string]routeInfo{},
		up:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_route_resolver_up", Help: "上轮 podIP→route 映射刷新是否成功(1/0)"}),
		pods:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_route_resolver_pods", Help: "当前映射覆盖的后端 pod IP 数"}),
		errs:      prometheus.NewCounter(prometheus.CounterOpts{Name: "bodylog_route_resolver_errors_total", Help: "podIP→route 映射刷新出错累计"}),
		replicas:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bodylog_service_replicas", Help: "该 service(discovery.service)当前后端 pod 总数(EndpointSlice endpoint 数,含未就绪)"}, []string{"service", "route"}),
		replicasReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bodylog_service_replicas_ready", Help: "该 service 当前就绪后端 pod 数(conditions.ready)"}, []string{"service", "route"}),
	}
	reg.MustRegister(r.up, r.pods, r.errs, r.replicas, r.replicasReady)
	return r
}

// svcRep:一个 service 的副本数(build 顺带产出,refresh 灌进 gauge)。n=总数(含未就绪),ready=就绪数。
type svcRep struct {
	service, route string
	n, ready       int
}

// Lookup:后端 pod IP → route/service。未命中返回 ok=false(observe 侧回退 route/service=unknown)。
func (r *podRouteResolver) Lookup(ip string) (route, service string, ok bool) {
	if ip == "" {
		return "", "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	ri, ok := r.byIP[ip]
	return ri.route, ri.service, ok
}

func (r *podRouteResolver) run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	r.refresh(ctx) // 启动即建一次
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refresh(ctx)
		}
	}
}

func (r *podRouteResolver) refresh(ctx context.Context) {
	next, reps, err := r.build(ctx)
	if err != nil {
		r.errs.Inc()
		r.up.Set(0)
		log.Printf("route-enrich: %v", err)
		return // 保留上次映射(不清空,避免 apiserver 抖动时全 route 归属失明)
	}
	r.mu.Lock()
	r.byIP = next
	r.mu.Unlock()
	// Reset 后重灌:缩容/下线的 service series 自动消失,不残留旧值。
	r.replicas.Reset()
	r.replicasReady.Reset()
	for _, rp := range reps {
		r.replicas.WithLabelValues(rp.service, rp.route).Set(float64(rp.n))
		r.replicasReady.WithLabelValues(rp.service, rp.route).Set(float64(rp.ready))
	}
	r.up.Set(1)
	r.pods.Set(float64(len(next)))
}

// modelRouteFull:富化要的投影 —— nginx.route + discovery.service。
type modelRouteFull struct {
	Items []struct {
		Spec struct {
			Nginx     struct{ Route string `json:"route"` }     `json:"nginx"`
			Discovery struct{ Service string `json:"service"` } `json:"discovery"`
		} `json:"spec"`
	} `json:"items"`
}

// endpointSliceList:discovery.k8s.io/v1 EndpointSlice —— 取 endpoints[].addresses[] + conditions.ready。
type endpointSliceList struct {
	Items []struct {
		Endpoints []struct {
			Addresses  []string `json:"addresses"`
			Conditions struct {
				Ready *bool `json:"ready"` // nil=unknown(EndpointSlice 约定按 ready 处理)
			} `json:"conditions"`
		} `json:"endpoints"`
	} `json:"items"`
}

// build:list ModelRoute → 每个 route 查 discovery.service 的 EndpointSlice → 汇总
// map[podIP]{route,service} 及每 service 的后端 pod 数(svcRep)。
func (r *podRouteResolver) build(ctx context.Context) (map[string]routeInfo, []svcRep, error) {
	body, err := r.get(ctx, fmt.Sprintf("%s/apis/%s/%s/%s", r.apiBase, r.group, r.version, r.plural))
	if err != nil {
		return nil, nil, err
	}
	var lst modelRouteFull
	if err := json.Unmarshal(body, &lst); err != nil {
		return nil, nil, fmt.Errorf("decode ModelRouteList: %w", err)
	}
	out := map[string]routeInfo{}
	var reps []svcRep
	for _, it := range lst.Items {
		route := strings.TrimSpace(it.Spec.Nginx.Route)
		svc := strings.TrimSpace(it.Spec.Discovery.Service)
		if route == "" || svc == "" { // 无 nginx 路由或无后端 Service → 无法归属,跳过
			continue
		}
		ns, name := splitNsName(svc)
		if name == "" {
			continue
		}
		ips, ready, err := r.podIPsForService(ctx, ns, name)
		if err != nil {
			log.Printf("route-enrich: route=%s svc=%s EndpointSlice 失败: %v", route, svc, err)
			continue // 单个 service 失败不拖累其它 route
		}
		ri := routeInfo{route: route, service: svc} // svc 已是 "ns/name" 形式
		for _, ip := range ips {
			out[ip] = ri // 映射含未就绪 pod(它可能刚服务过一个请求,仍要能归属)
		}
		reps = append(reps, svcRep{service: svc, route: route, n: len(ips), ready: ready})
	}
	return out, reps, nil
}

// podIPsForService:查某 Service 的全部 EndpointSlice(按 kubernetes.io/service-name label 归属)。
// 返回全部 pod IP(含未就绪,给映射用)+ 就绪 IP 数(ready gauge 用)。
// ready 语义:conditions.ready==true 计就绪;nil(unknown)按 EndpointSlice 约定视为就绪。
func (r *podRouteResolver) podIPsForService(ctx context.Context, ns, name string) (ips []string, ready int, err error) {
	sel := url.QueryEscape("kubernetes.io/service-name=" + name)
	u := fmt.Sprintf("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=%s", r.apiBase, ns, sel)
	body, err := r.get(ctx, u)
	if err != nil {
		return nil, 0, err
	}
	var esl endpointSliceList
	if err := json.Unmarshal(body, &esl); err != nil {
		return nil, 0, fmt.Errorf("decode EndpointSliceList: %w", err)
	}
	for _, es := range esl.Items {
		for _, ep := range es.Endpoints {
			isReady := ep.Conditions.Ready == nil || *ep.Conditions.Ready
			for _, ip := range ep.Addresses {
				ips = append(ips, ip)
				if isReady {
					ready++
				}
			}
		}
	}
	return ips, ready, nil
}

// get:带 SA token 的 k8s REST GET(token 每次现读,支持轮转)。
func (r *podRouteResolver) get(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if tok, err := os.ReadFile(r.tokenPath); err == nil {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s → HTTP %d: %s", u, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// splitNsName:"ns/name" → (ns, name);无 "/" 时 ns 回退 "default"。
func splitNsName(s string) (ns, name string) {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return "default", s
}

// backendIP:从 bodylog backend 串抽出纯 IP。
// 形如 "10.0.0.5:8050",也可能带 openresty 重试后缀 " (retry#1)",或 "(none)"。
func backendIP(backend string) string {
	s := strings.TrimSpace(backend)
	if i := strings.IndexByte(s, ' '); i >= 0 { // 去 " (retry#N)"
		s = s[:i]
	}
	if i := strings.LastIndexByte(s, ':'); i >= 0 { // 去 ":port"
		s = s[:i]
	}
	return s
}
