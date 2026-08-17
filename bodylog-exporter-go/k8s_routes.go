// k8s route 动态发现:列 ModelRoute CR(routing.gpucluster.io/v1alpha1,plural=modelroutes)
// 得到"当前 k8s 里所有 route"—— openresty 没有全局列举端点,route 的权威源就是 ModelRoute CR。
// 用 pod 的 in-cluster ServiceAccount 直接走 k8s REST(裸 net/http + token/CA,不引 client-go)。
// 周期性 list、热更新;route 增删自动跟随,无需重启/改 values。
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
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	k8sTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	k8sCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type discoverConfig struct {
	group        string        // routing.gpucluster.io
	version      string        // v1alpha1
	plural       string        // modelroutes
	nginxService string        // 过滤:只要 spec.nginx.service==此(ns/name);空=全要(单 openresty 留空)
	interval     time.Duration // list 周期
	timeout      time.Duration
}

func loadDiscoverConfig() discoverConfig {
	return discoverConfig{
		group:        envOr("MODELROUTE_GROUP", "routing.gpucluster.io"),
		version:      envOr("MODELROUTE_VERSION", "v1alpha1"),
		plural:       envOr("MODELROUTE_PLURAL", "modelroutes"),
		nginxService: strings.TrimSpace(envOr("OPENRESTY_SERVICE", "")),
		interval:     time.Duration(envOrInt("ROUTE_DISCOVERY_INTERVAL_SECONDS", 30)) * time.Second,
		timeout:      time.Duration(envOrInt("ROUTE_DISCOVERY_TIMEOUT_MS", 4000)) * time.Millisecond,
	}
}

type routeDiscoverer struct {
	cfg       discoverConfig
	apiBase   string // https://<KUBERNETES_SERVICE_HOST>:<PORT>
	tokenPath string
	client    *http.Client

	mu     sync.RWMutex
	routes []string
	svc    map[string]string // route → discovery.service(ns/name);给 openresty_* 打 service label

	up    prometheus.Gauge
	count prometheus.Gauge
	errs  prometheus.Counter
}

func newRouteDiscoverer(reg *prometheus.Registry, cfg discoverConfig) *routeDiscoverer {
	// in-cluster apiserver 地址(env 由 kubelet 注入);回退 kubernetes.default.svc。
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	apiBase := "https://kubernetes.default.svc"
	if host != "" {
		if port == "" {
			port = "443"
		}
		apiBase = "https://" + host + ":" + port
	}
	// TLS:用集群 CA 校验 apiserver(CA 读不到就退化到系统根,通常会 TLS 失败→ up=0 可见)。
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca, err := os.ReadFile(k8sCAPath); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(ca) {
			tlsCfg.RootCAs = pool
		}
	} else {
		log.Printf("route-discovery: 读 CA %s 失败(%v),TLS 用系统根", k8sCAPath, err)
	}
	rd := &routeDiscoverer{
		cfg:       cfg,
		apiBase:   apiBase,
		tokenPath: k8sTokenPath,
		client:    &http.Client{Timeout: cfg.timeout, Transport: &http.Transport{TLSClientConfig: tlsCfg}},
		up:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "openresty_route_discovery_up", Help: "上轮 ModelRoute 发现是否成功(1/0)"}),
		count:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "openresty_discovered_routes", Help: "当前发现的 route 数"}),
		errs:      prometheus.NewCounter(prometheus.CounterOpts{Name: "openresty_route_discovery_errors_total", Help: "ModelRoute 发现出错累计"}),
	}
	reg.MustRegister(rd.up, rd.count, rd.errs)
	return rd
}

func (rd *routeDiscoverer) get() []string {
	rd.mu.RLock()
	defer rd.mu.RUnlock()
	out := make([]string, len(rd.routes))
	copy(out, rd.routes)
	return out
}

// serviceFor:route → discovery.service(ns/name)。未知返回 ""(poller 侧回退 unknown)。
func (rd *routeDiscoverer) serviceFor(route string) string {
	rd.mu.RLock()
	defer rd.mu.RUnlock()
	return rd.svc[route]
}

func (rd *routeDiscoverer) run(ctx context.Context) {
	t := time.NewTicker(rd.cfg.interval)
	defer t.Stop()
	rd.refresh(ctx) // 启动即拉一次
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rd.refresh(ctx)
		}
	}
}

// modelRouteList:取 spec.nginx.{route,service}(路由名 + 归属 openresty)+ discovery.service(后端 Service)。
type modelRouteList struct {
	Items []struct {
		Spec struct {
			Nginx struct {
				Route   string `json:"route"`
				Service string `json:"service"`
			} `json:"nginx"`
			Discovery struct {
				Service string `json:"service"`
			} `json:"discovery"`
		} `json:"spec"`
	} `json:"items"`
}

func (rd *routeDiscoverer) refresh(ctx context.Context) {
	routes, svc, err := rd.list(ctx)
	if err != nil {
		rd.errs.Inc()
		rd.up.Set(0)
		log.Printf("route-discovery: %v", err)
		return // 保留上次成功的 routes(不清空,避免 apiserver 抖动时全 route 失明)
	}
	rd.mu.Lock()
	changed := !equalStrs(rd.routes, routes)
	rd.routes = routes
	rd.svc = svc
	rd.mu.Unlock()
	rd.up.Set(1)
	rd.count.Set(float64(len(routes)))
	if changed {
		log.Printf("route-discovery: routes=%v", routes)
	}
}

func (rd *routeDiscoverer) list(ctx context.Context) ([]string, map[string]string, error) {
	// Namespaced CR 的全 ns 列举:/apis/<group>/<version>/<plural>
	url := fmt.Sprintf("%s/apis/%s/%s/%s", rd.apiBase, rd.cfg.group, rd.cfg.version, rd.cfg.plural)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	if tok, err := os.ReadFile(rd.tokenPath); err == nil { // token 会轮转,每次现读
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	req.Header.Set("Accept", "application/json")
	resp, err := rd.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("list %s → HTTP %d: %s", rd.cfg.plural, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var lst modelRouteList
	if err := json.Unmarshal(body, &lst); err != nil {
		return nil, nil, fmt.Errorf("decode ModelRouteList: %w", err)
	}
	seen := map[string]bool{}
	svc := map[string]string{}
	var routes []string
	for _, it := range lst.Items {
		r := strings.TrimSpace(it.Spec.Nginx.Route)
		if r == "" { // 没配 nginx 路由(如 monitor-only)→ 无路可 poll,跳过
			continue
		}
		if rd.cfg.nginxService != "" && it.Spec.Nginx.Service != rd.cfg.nginxService {
			continue // 不是这台 openresty 服务的 route
		}
		if !seen[r] {
			seen[r] = true
			routes = append(routes, r)
		}
		svc[r] = strings.TrimSpace(it.Spec.Discovery.Service) // route → 后端 Service(ns/name)
	}
	sort.Strings(routes)
	return routes, svc, nil
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
