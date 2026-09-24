// 富化:把 bodylog 明细里的后端 pod IP 映射到它所属的 {route, model}。
//
// 为什么需要:bodylog 的 backend 是 openresty 转发到的真实后端 = k8s pod IP:port,pod 重建会漂移;
// 且明细的 model 字段常抓空(→unknown)。唯一稳定可靠的归属键是「这个 pod IP 属于哪个 ModelRoute」。
// 数据源:list ModelRoute CR 取 spec.discovery.service → 查该 Service 的 EndpointSlice 得 pod IP 集,
// 反建 map[podIP]{route,service}。周期刷新,pod 增删自动跟随。observe() 据此给 bodylog_* 打稳定的 service/route label。
//
// 单一发现器:同一份 ModelRoute list 既产出富化映射(byIP + 副本数,给 tail 明细指标),
// 也产出 route 列表 + route→service(给 openresty-poll)。走 in-cluster SA REST(裸 net/http,不引 client-go)。
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

// routeInfo:一个后端 pod 所属的 route 及其 service(来自它所在的 ModelRoute)。
// route   = spec.nginx.route(如 "fallback-model-service-0.1"),省略则 metadata.name(同 autoconfig,见 routeName);
// service = spec.discovery.service(ns/name 形式,如 "model-service/fallback-model-service-01",用户一般按它聚合),
//
//	经 normalizeServiceLabel 剥掉 LWS 的 "-leader" 后缀(kimi/kimi-k25-leader → kimi/kimi-k25)。
type routeInfo struct{ route, service string }

type podRouteResolver struct {
	group, version, plural string
	nginxService           string // 只把 nginx.service==此的 route 计入 poll 列表(多 openresty 用;空=全要)
	interval, timeout      time.Duration
	apiBase                string
	tokenPath              string
	client                 *http.Client

	mu     sync.RWMutex
	byIP   map[string]routeInfo // 后端 pod IP → {route, service}(富化用,含所有 route)
	routes []string             // poll 用的 route 列表(按 nginxService 过滤 + 去重排序)
	svc    map[string]string    // route → discovery.service(poll 打 service label 用,含所有 route)

	up              prometheus.Gauge
	pods            prometheus.Gauge
	routesG         prometheus.Gauge
	errs            prometheus.Counter
	replicas        *prometheus.GaugeVec // {service, route} → 后端 pod 总数(含未就绪)
	replicasReady   *prometheus.GaugeVec // {service, route} → 就绪后端 pod 数
	replicasDesired *prometheus.GaugeVec // {service, route} → 期望副本数(工作负载 spec.replicas)
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
		group:         envOr("MODELROUTE_GROUP", "routing.modelsphere.dev"),
		version:       envOr("MODELROUTE_VERSION", "v1alpha1"),
		plural:        envOr("MODELROUTE_PLURAL", "modelroutes"),
		nginxService:  strings.TrimSpace(envOr("OPENRESTY_SERVICE", "")),
		interval:      interval,
		timeout:       timeout,
		apiBase:       apiBase,
		tokenPath:     k8sTokenPath,
		client:        &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: tlsCfg}},
		byIP:          map[string]routeInfo{},
		svc:           map[string]string{},
		up:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_route_resolver_up", Help: "上轮 ModelRoute 发现/映射刷新是否成功(1/0)"}),
		pods:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_route_resolver_pods", Help: "当前映射覆盖的后端 pod IP 数"}),
		routesG:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "bodylog_route_resolver_routes", Help: "当前发现的 route 数(poll 列表)"}),
		errs:          prometheus.NewCounter(prometheus.CounterOpts{Name: "bodylog_route_resolver_errors_total", Help: "ModelRoute 发现/映射刷新出错累计"}),
		replicas:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bodylog_service_replicas", Help: "该 service(discovery.service)当前后端 pod 总数(EndpointSlice endpoint 数,含未就绪)"}, []string{"service", "route"}),
		replicasReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bodylog_service_replicas_ready", Help: "该 service 当前就绪后端 pod 数(conditions.ready)"}, []string{"service", "route"}),
		// 期望副本数:判「降级」要拿【就绪】比【期望】,不能比【实际】—— 滚动更新的 maxSurge 会把实际抬高,
		// 拿实际当分母会在每次 rollout 期间把满员的服务误判成降级。查不到时不发这条 series(见 desiredForPod)。
		replicasDesired: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bodylog_service_replicas_desired", Help: "该 service 期望副本数(顶层工作负载 spec.replicas;LWS 取 LWS 本身而非被 surge 抬高的 StatefulSet)"}, []string{"service", "route"}),
	}
	reg.MustRegister(r.up, r.pods, r.routesG, r.errs, r.replicas, r.replicasReady, r.replicasDesired)
	return r
}

// getRoutes:poll 要 poll 的 route 列表(按 nginxService 过滤后的快照)。
func (r *podRouteResolver) getRoutes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.routes))
	copy(out, r.routes)
	return out
}

// serviceFor:route → discovery.service(ns/name)。未知返回 ""(poller 侧回退 unknown)。
func (r *podRouteResolver) serviceFor(route string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.svc[route]
}

// svcRep:一个 service 的副本数(build 顺带产出,refresh 灌进 gauge)。n=总数(含未就绪),ready=就绪数。
// desired=期望副本数,desiredOK=false 表示这轮没查出来(不发 gauge,而不是发 0 —— 0 会被读成"缩到零")。
type svcRep struct {
	service, route string
	n, ready       int
	desired        int
	desiredOK      bool
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
	res, err := r.build(ctx)
	if err != nil {
		r.errs.Inc()
		r.up.Set(0)
		log.Printf("route-enrich: %v", err)
		return // 保留上次映射(不清空,避免 apiserver 抖动时全 route 归属失明)
	}
	r.mu.Lock()
	changed := !equalStrs(r.routes, res.routes)
	r.byIP, r.routes, r.svc = res.byIP, res.routes, res.svc
	r.mu.Unlock()
	if changed { // route 增删时留个线索(排查某 route 何时上下线)
		log.Printf("route-enrich: routes=%v", res.routes)
	}
	// Reset 后重灌:缩容/下线的 service series 自动消失,不残留旧值。
	r.replicas.Reset()
	r.replicasReady.Reset()
	r.replicasDesired.Reset()
	for _, rp := range res.reps {
		r.replicas.WithLabelValues(rp.service, rp.route).Set(float64(rp.n))
		r.replicasReady.WithLabelValues(rp.service, rp.route).Set(float64(rp.ready))
		if rp.desiredOK {
			r.replicasDesired.WithLabelValues(rp.service, rp.route).Set(float64(rp.desired))
		}
	}
	r.up.Set(1)
	r.pods.Set(float64(len(res.byIP)))
	r.routesG.Set(float64(len(res.routes)))
}

// resolveResult:一轮 build 的产出。byIP/reps/svc 含所有 route;routes 是 poll 用(按 nginxService 过滤)。
type resolveResult struct {
	byIP   map[string]routeInfo
	reps   []svcRep
	routes []string
	svc    map[string]string
}

// modelRouteFull:投影 —— nginx.{route,service} + discovery.service + modelType。
//
// modelType 用来决定「要不要 poll 这条 route 的引擎状态端点」:
// video 路由是 autoconfig 渲染的**纯反向代理**,压根没有 _route_state/_tps_status/_ttft_status
// 这些 lua 引擎端点,poll 它必然失败。而 openresty_poll_up 是**全局**的
// (「上轮 poll 是否全成功」),一条 route 失败就把它打成 0。
//
// 危害不是"指标断了"——pollOnce 是逐条 route 填 gauge 的,失败的只让自己那份空着,
// 其余 route 的数据照常(2026-09-03~09-06 实测:LLM 路由的 healthy_peers/active_level
// 全程连续)。真正的危害是 **OpenRestyPollDown 连响 3 天、这个告警彻底失去信号价值**:
// 期间若真发生轮询故障(openresty 不可达等),没人分辨得出来——它早就在响了。
// 而看板上完全看不出异常(poll_up 在三个 dashboard 里零引用),所以拖了 3 天。
type modelRouteFull struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			ModelType string `json:"modelType"` // 空 = llm(CRD 默认值)
			Nginx     struct {
				Route           string            `json:"route"`
				Service         string            `json:"service"`
				OutputConfigMap string            `json:"outputConfigMap"`
				Peers           []json.RawMessage `json:"peers"`
			} `json:"nginx"`
			Discovery struct {
				Service string `json:"service"`
			} `json:"discovery"`
		} `json:"spec"`
	} `json:"items"`
}

// routeName:与 autoconfig 的 nginxRoute() 同一规则 —— spec.nginx.route 优先,省略则用 metadata.name。
// nginx.route 在 CRD 里是 omitempty,chart 也只在 values 显式写了才渲染;只认 route 字段会把
// 「没写 route、靠默认名」的正常路由当成 monitor-only 整条跳过(2026-09-14 线上:model-service-03-kimi /
// mf-dummpy 路由在跑、openresty 有 conf 和 sock,但 bodylog_service_replicas 里没有它们,
// 服务健康总览看板上整个服务消失)。
// 真正的 monitor-only = 连 nginx 段都没有(无 outputConfigMap、无 peers),这种返回 ""。
func routeName(name, route, outputConfigMap string, peers int) string {
	if r := strings.TrimSpace(route); r != "" {
		return r
	}
	if strings.TrimSpace(outputConfigMap) == "" && peers == 0 {
		return ""
	}
	return strings.TrimSpace(name)
}

// endpointSliceList:discovery.k8s.io/v1 EndpointSlice —— 取 endpoints[].addresses[] + conditions.ready
// + targetRef(哪个 pod,作为 ownerRef 上溯求期望副本数的入口)。
type endpointSliceList struct {
	Items []struct {
		Endpoints []struct {
			Addresses  []string `json:"addresses"`
			Conditions struct {
				Ready *bool `json:"ready"` // nil=unknown(EndpointSlice 约定按 ready 处理)
			} `json:"conditions"`
			TargetRef *struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"targetRef"`
		} `json:"endpoints"`
	} `json:"items"`
}

// ownerObj:ownerRef 上溯只需要每层对象的两样东西 —— 它的控制器 owner,和它自己的 spec.replicas。
type ownerObj struct {
	Metadata struct {
		OwnerReferences []struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
			Controller *bool  `json:"controller"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int `json:"replicas"`
	} `json:"spec"`
}

// ownerWalkMaxDepth:Pod → ReplicaSet → Deployment 与 Pod → StatefulSet → LeaderWorkerSet 都是 2 跳,
// 留一点余量后设死上限 —— ownerReferences 理论上可以成环(手工构造/控制器 bug),没有上限就是死循环。
const ownerWalkMaxDepth = 5

// build:list ModelRoute → ① 每个有 discovery.service 的 route 查 EndpointSlice → map[podIP]{route,service} + 副本数;
// ② route→service 映射(含所有 route);③ poll 用 route 列表(按 nginxService 过滤 + 去重排序)。
func (r *podRouteResolver) build(ctx context.Context) (resolveResult, error) {
	body, err := r.get(ctx, fmt.Sprintf("%s/apis/%s/%s/%s", r.apiBase, r.group, r.version, r.plural))
	if err != nil {
		return resolveResult{}, err
	}
	var lst modelRouteFull
	if err := json.Unmarshal(body, &lst); err != nil {
		return resolveResult{}, fmt.Errorf("decode ModelRouteList: %w", err)
	}
	res := resolveResult{byIP: map[string]routeInfo{}, svc: map[string]string{}}
	seen := map[string]bool{}
	// 同一轮内共享:多个 route 常指向同一组工作负载(如 cart 与推理服务同属一个 release),
	// 上溯路径高度重合,缓存把 apiserver 的 GET 次数压到每个对象一次。
	ownerCache := map[string]*ownerObj{}
	for _, it := range lst.Items {
		route := routeName(it.Metadata.Name, it.Spec.Nginx.Route, it.Spec.Nginx.OutputConfigMap, len(it.Spec.Nginx.Peers))
		if route == "" { // monitor-only(无 nginx 路由)→ 既不 poll 也无法归属
			continue
		}
		svc := strings.TrimSpace(it.Spec.Discovery.Service) // 原始 discovery.service:查 EndpointSlice 必须用真实 Service 名
		svcLabel := normalizeServiceLabel(svc)              // 展示用 label:剥掉 LWS 的 "-leader" 后缀
		res.svc[route] = svcLabel                           // 含所有 route(serviceFor 稳健)

		// poll route 列表:按 nginxService 过滤(多 openresty 用;空=全要),
		// 并且**只 poll 走 lua 引擎的 route**(modelType 空或 llm)。
		// video 等纯反向代理没有 _route_state/_tps_status/_ttft_status,poll 必然失败,
		// 而 poll_up 是全局的,会把整个 openresty 监控拖成盲区(见 modelRouteFull 注释)。
		mt := strings.TrimSpace(it.Spec.ModelType)
		pollable := mt == "" || mt == "llm"
		if pollable && (r.nginxService == "" || strings.TrimSpace(it.Spec.Nginx.Service) == r.nginxService) {
			if !seen[route] {
				seen[route] = true
				res.routes = append(res.routes, route)
			}
		}

		// 富化映射:要 discovery.service 才能查后端 pod IP。
		if svc == "" {
			continue
		}
		ns, name := splitNsName(svc)
		if name == "" {
			continue
		}
		ips, ready, samplePod, err := r.podIPsForService(ctx, ns, name)
		if err != nil {
			log.Printf("route-enrich: route=%s svc=%s EndpointSlice 失败: %v", route, svc, err)
			continue // 单个 service 失败不拖累其它 route
		}
		ri := routeInfo{route: route, service: svcLabel} // "ns/name" 形式(已剥 -leader)
		for _, ip := range ips {
			res.byIP[ip] = ri // 映射含未就绪 pod(它可能刚服务过一个请求,仍要能归属)
		}
		rep := svcRep{service: svcLabel, route: route, n: len(ips), ready: ready}
		if samplePod != "" {
			// 期望副本数走 ownerRef 上溯,失败只是这条 gauge 缺一轮,不影响富化映射。
			if d, ok := r.desiredForPod(ctx, ns, samplePod, ownerCache); ok {
				rep.desired, rep.desiredOK = d, true
			}
		}
		res.reps = append(res.reps, rep)
	}
	sort.Strings(res.routes)
	return res, nil
}

// podIPsForService:查某 Service 的全部 EndpointSlice(按 kubernetes.io/service-name label 归属)。
// 返回全部 pod IP(含未就绪,给映射用)+ 就绪 IP 数(ready gauge 用)+ 任一 pod 名(求期望副本数的入口)。
// ready 语义:conditions.ready==true 计就绪;nil(unknown)按 EndpointSlice 约定视为就绪。
func (r *podRouteResolver) podIPsForService(ctx context.Context, ns, name string) (ips []string, ready int, samplePod string, err error) {
	sel := url.QueryEscape("kubernetes.io/service-name=" + name)
	u := fmt.Sprintf("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=%s", r.apiBase, ns, sel)
	body, err := r.get(ctx, u)
	if err != nil {
		return nil, 0, "", err
	}
	var esl endpointSliceList
	if err := json.Unmarshal(body, &esl); err != nil {
		return nil, 0, "", fmt.Errorf("decode EndpointSliceList: %w", err)
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
			// 任意一个 endpoint 的 pod 都行:同一个 Service 背后的 pod 归同一个工作负载,
			// 上溯到顶拿到的是同一个 spec.replicas。滚动更新期间新旧组并存也不影响
			// —— 新旧 pod 的 ownerRef 链最终都收敛到同一个顶层对象。
			if samplePod == "" && ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" {
				samplePod = ep.TargetRef.Name
			}
		}
	}
	return ips, ready, samplePod, nil
}

// desiredForPod:从一个后端 pod 沿 ownerReferences 上溯到顶层工作负载,取它的 spec.replicas。
//
// 为什么要走到【顶】而不是停在第一层:LeaderWorkerSet 的 leader pod 属于一个 StatefulSet,
// 而滚动更新时 LWS 控制器会把那个 StatefulSet 的 spec.replicas 按 maxSurge 抬高
// (实测 surge 中 sts=3 / lws=2)。停在 StatefulSet 拿到的就是被 surge 污染的数字,
// 正是这个 gauge 要避免的东西。Deployment 侧同理:停在 ReplicaSet 会拿到旧 RS 的残值。
//
// 找不到(裸 pod / 顶层对象没有 spec.replicas / 权限不足)返回 ok=false,调用方据此跳过发点。
func (r *podRouteResolver) desiredForPod(ctx context.Context, ns, pod string, cache map[string]*ownerObj) (int, bool) {
	apiVersion, kind, name := "v1", "Pod", pod
	for depth := 0; depth < ownerWalkMaxDepth; depth++ {
		obj, err := r.getOwnerObj(ctx, ns, apiVersion, kind, name, cache)
		if err != nil {
			log.Printf("route-enrich: 求期望副本数 %s/%s %s/%s 失败: %v", ns, pod, kind, name, err)
			return 0, false
		}
		var owner *struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
			Controller *bool  `json:"controller"`
		}
		for i := range obj.Metadata.OwnerReferences {
			if o := &obj.Metadata.OwnerReferences[i]; o.Controller != nil && *o.Controller {
				owner = o
				break
			}
		}
		if owner == nil { // 到顶了:这一层就是工作负载本身
			if obj.Spec.Replicas == nil {
				return 0, false
			}
			return *obj.Spec.Replicas, true
		}
		apiVersion, kind, name = owner.APIVersion, owner.Kind, owner.Name
	}
	log.Printf("route-enrich: 求期望副本数 %s/%s 上溯超过 %d 层(ownerReferences 可能成环),放弃", ns, pod, ownerWalkMaxDepth)
	return 0, false
}

// getOwnerObj:按 apiVersion/kind/name 取一个命名空间对象(只解出 ownerRefs + spec.replicas)。
// resource 名由 Kind 小写加 s 推导 —— 覆盖这条链上会出现的全部类型
// (Pod/ReplicaSet/Deployment/StatefulSet/LeaderWorkerSet/DaemonSet/Job),不引 discovery 客户端。
func (r *podRouteResolver) getOwnerObj(ctx context.Context, ns, apiVersion, kind, name string, cache map[string]*ownerObj) (*ownerObj, error) {
	key := apiVersion + "/" + kind + "/" + ns + "/" + name
	if o, ok := cache[key]; ok {
		return o, nil
	}
	plural := strings.ToLower(kind) + "s"
	var u string
	if strings.Contains(apiVersion, "/") { // 有组:/apis/<group>/<version>/...
		u = fmt.Sprintf("%s/apis/%s/namespaces/%s/%s/%s", r.apiBase, apiVersion, ns, plural, name)
	} else { // 核心组(apiVersion="v1"):/api/v1/...
		u = fmt.Sprintf("%s/api/%s/namespaces/%s/%s/%s", r.apiBase, apiVersion, ns, plural, name)
	}
	body, err := r.get(ctx, u)
	if err != nil {
		return nil, err
	}
	var obj ownerObj
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, fmt.Errorf("decode %s/%s: %w", kind, name, err)
	}
	cache[key] = &obj
	return &obj, nil
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

// equalStrs:两个 route 切片是否逐项相等(均已排序)。
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

// normalizeServiceLabel:把展示用的 service label 剥掉 LWS leader 服务的 "-leader" 后缀。
// 背景:LWS(LeaderWorkerSet)暴露推理端口(8050)的 Service 恒名为 "<lws-name>-leader"
// —— 同名的无头 governing 服务 "<lws-name>" 没有端口,discovery 只能指向 "-leader" 那个。
// 于是 discovery.service = "kimi/kimi-k25-leader",而用户眼里的逻辑服务是 "kimi/kimi-k25"(= LWS 名)。
// 本函数只归一化【label 展示】,查 EndpointSlice 仍用原始 discovery.service(真实 Service 名),互不影响。
// 只在结尾剥后缀:namespace 段在 "/" 之前不受影响;非 LWS 服务(如 fallback-model-service-01)不含该后缀,原样返回。
func normalizeServiceLabel(svc string) string {
	return strings.TrimSuffix(svc, "-leader")
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
