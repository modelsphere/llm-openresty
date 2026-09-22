# bodylog-exporter

把 `bodylog-listener` 落的**每请求明细 jsonl** 翻译成 **Prometheus 指标**的独立小工具。用于把 LLM 网关的**流量侧**指标(QPS、token 速率、延迟/TTFT 分位、错误率)接进 Prometheus,供 Grafana 看板与**自动扩缩容(LLMScaler / KEDA / HPA)**消费。

- **不改 `bodylog-listener`**:按字节 offset `tail` append-only 的 `details/<date>.jsonl`,逐行增量 `observe`。
- **零丢**:文件当持久缓冲;offset checkpoint 续读;跨天 `jsonl→parquet` 轮转的缺口走 bodylog `/metrics` HTTP 补读(request_id 去重)。
- **Prometheus 原生**:counter 单调累计、histogram 桶累计 → `rate()`/`histogram_quantile()` 正确(不是 scrape 时现算的窗口 gauge)。

---

## 暴露的指标

端点:`GET <listen>/metrics`(默认 `:9110`,Prometheus 文本格式,无鉴权 —— 只有聚合值、无请求正文)。

### 业务指标(维度:`service` / `route` / `backend` / `model`)

**每条业务指标都带 4 个维度**:`service`(= ModelRoute 的 `discovery.service`,`ns/name` 形式,**用户主聚合维度**)、`route`(= `nginx.route`,省略则 ModelRoute 的 `metadata.name`,与 autoconfig 一致)、`backend`(真后端 pod IP:port,会漂移)、`model`(明细里的 served-model-name,可能空)。`service`/`route` 由**富化**据 `backend` pod IP 反查 ModelRoute 得到(见下方「富化:podIP→service/route」);in-cluster 才有,裸机退化为 `unknown`。

| 指标 | 类型 | 额外 label | 含义 |
|---|---|---|---|
| `bodylog_requests_total` | counter | `status_class`,`stream` | 请求数 |
| `bodylog_prompt_tokens_total` | counter | — | prompt token 累计 |
| `bodylog_completion_tokens_total` | counter | — | completion token 累计 |
| `bodylog_cached_tokens_total` | counter | — | cached(命中前缀)token 累计 |
| `bodylog_reasoning_tokens_total` | counter | — | reasoning token 累计(仅推理模型) |
| `bodylog_total_tokens_total` | counter | — | total token 累计 |
| `bodylog_req_bytes_total` | counter | — | 请求体字节累计 |
| `bodylog_resp_bytes_total` | counter | — | 响应体字节累计 |
| `bodylog_finish_reason_total` | counter | `finish_reason` | 按停止原因计数 |
| `bodylog_rt_seconds` | **histogram**(native-only) | `stream` | 总响应时间(秒) |
| `bodylog_ttft_seconds` | **histogram**(native-only) | **`prompt_bucket`** | 首 token 时间/TTFT(秒,**仅流式**) |
| `bodylog_output_tok_per_second` | **histogram**(native-only) | **`prompt_bucket`** | **单请求输出 token 速率** `completion_tokens/rt`(tok/s,**分母为总时长、含 prefill**;流式与非流式均计,过滤 `ctok>=16` + `rt>=0.5s`) |
| `bodylog_overall_output_tok_per_second` | **histogram**(native-only) | — | **总体输出 token 速率** `completion_tokens/rt`(tok/s);仅 `status` 为 2xx、`completion_tokens>0`、`rt>0` 的请求,不带 `prompt_bucket`/`stream` 过滤 |
| `bodylog_decode_output_tok_per_second` | **histogram**(native-only) | — | **解码输出 token 速率** `completion_tokens/(lct-frt)`(tok/s);仅 2xx、`completion_tokens>0` 且 `lct-frt>0.3s` 的请求,不带 `prompt_bucket`/`stream` 过滤 |

> **⚠️ 2026-08-31(exporter 0.2.0)起,上述五个 histogram 均为 native-only** —— 不再双发经典桶,
> `bodylog_*_bucket` / `_sum` / `_count` 这些 series **不再产生**。求分位数必须用**裸名、不带 `le`**:
> `histogram_quantile(0.95, sum by(service)(rate(bodylog_ttft_seconds[5m])))`。
> 起因:Prometheus 侧开了 `scrapeClassicHistograms`,双发会真被抓进 TSDB(实测 12x series 膨胀)。

`bodylog_output_tok_per_second` 是历史 legacy 指标,语义和 HELP 保持不变(含 `prompt_bucket`、`completion_tokens>=16`、`rt>=0.5s`),不得作为新实验的 TPS 口径；Watchmen 使用的 overall/decode 指标是新增指标,不能用旧指标替代。`decode` 明确使用明细 JSON 的 `lct-frt`，只要求 `lct-frt>0.3s`，不额外限定 `stream`；三个 TPS 指标的 observation 都是单请求直方图样本,Prometheus 查询时按 `service` 聚合即可。

#### `prompt_bucket`:按输入长度分档(ttft / output_tok_per_second)

回答「某 context 区间的 TTFT p80」这类问题。**26 档 + `unknown`**,左闭右开,4 位零填充保证字典序 == 数值序:

```
0000k_0001k 0001k_0002k 0002k_0003k 0003k_0004k 0004k_0006k 0006k_0008k
0008k_0010k 0010k_0012k 0012k_0016k 0016k_0020k 0020k_0024k 0024k_0032k
0032k_0040k 0040k_0048k 0048k_0064k 0064k_0080k 0080k_0096k 0096k_0128k
0128k_0160k 0160k_0192k 0192k_0256k 0256k_0384k 0384k_0512k 0512k_0768k
0768k_1024k 1024k_inf   unknown
```

- 边界含 6/16/32/64/128/256k 等业务关注点,查询时用正则**合并相邻档**(只能变粗、不能变细):
  ```promql
  # 6~16K 输入的 TTFT p80
  histogram_quantile(0.8, sum(rate(bodylog_ttft_seconds{
    model="kimi-k2.5", prompt_bucket=~"0006k_0008k|0008k_0010k|0010k_0012k|0012k_0016k"}[5m])))
  ```
- **`unknown`** = `prompt_tokens<=0`(usage 缺失)。实测这批多是 `status` 400/429 的**失败请求**,
  `frt` 反映的是错误返回耗时,与输入长度无关 —— 单列出来避免污染首档(混入会让首档 TTFT p80 从 0.11s 虚高到 1.37s)。
- `1024k_inf` 是溢出哨兵(超模型 context 上限),正常应为空。空档不产生 series,零成本。
- ⚠️ **档位边界上线后不可再改**:改档位 = 旧 label 停更、新值从零、跨改动点的 `histogram_quantile` 不可信。
  需要任意区间/任意分位 → 查明细 parquet(保留 365 天),见 `metrics.go` 注释里的 DuckDB 示例。

### 服务副本数(富化,维度 `service` / `route`)

据 ModelRoute 的 `discovery.service` 查 EndpointSlice 得到的后端 pod 数(in-cluster 才有)。可与上面的吞吐 join 出「每副本吞吐」。

| 指标 | 类型 | 含义 |
|---|---|---|
| `bodylog_service_replicas` | gauge | 该 service 后端 pod **总数**(EndpointSlice endpoint 数,含未就绪) |
| `bodylog_service_replicas_ready` | gauge | 该 service **就绪**后端 pod 数(`conditions.ready`;nil 按约定视为就绪) |

### exporter 自监控

| 指标 | 类型 | 含义 |
|---|---|---|
| `bodylog_exporter_lines_total` | counter | 已 observe 的明细行数 |
| `bodylog_exporter_offset_bytes` | gauge | 当前 tail 文件的字节 offset |
| `bodylog_exporter_last_ts_seconds` | gauge | 最新 observe 行的结束时刻(unix 秒),**判滞后** |
| `bodylog_exporter_recovery_total` | counter | 跨天缺口 HTTP 补读次数 |
| `bodylog_route_resolver_up` | gauge | 上轮 ModelRoute 发现/映射刷新成功(1/0);失败**保留上次不清空** |
| `bodylog_route_resolver_pods` | gauge | 当前 podIP→route 映射覆盖的 pod 数 |
| `bodylog_route_resolver_routes` | gauge | 当前发现的 route 数(poll 列表) |
| `bodylog_route_resolver_errors_total` | counter | ModelRoute 发现/映射出错累计 |

### openresty 控制面指标(openresty-poll,可选)

**流量侧(上面)是事后逐请求;这一组是 openresty 的即时控制面态**(每 peer 当前并发、被 ban 的 peer、限流档位)——bodylog 拿不到,**对自动扩缩容反应更快**。开启方式见下方「openresty-poll 配置」:exporter 定时 GET openresty 已暴露的 JSON 端点(`/<route>/_route_state|_tps_status|_ttft_status|_429_status`),**不改 openresty**。全是 **gauge**(快照,每 poll 周期清空重填 → 掉线 peer 自动消失),仅 `openresty_rejected_total` 是 counter。

维度:`service`(= `discovery.service`,与 `bodylog_*` 对齐,便于按 service 聚合)/ `route`(=ModelRoute 名)/ `peer`(真后端 host:port)/ `name`(peer 名)/ `priority`(路由层级)/ `model`(子池,无分模型时为 `_`)。`service` 同样来自富化(route→`discovery.service`);未知回退 `unknown`。

| 指标 | 类型 | label | 含义 |
|---|---|---|---|
| `openresty_peer_active_conns` | gauge | service,route,peer,name,priority | 该 peer **当前并发**(least_conn 计数) |
| `openresty_peer_banned` | gauge | 同上 | 是否被健康检查 **ban**(1/0) |
| `openresty_peer_max_concurrency` | gauge | 同上 | 该 peer 静态并发上限 |
| `openresty_route_active_level` | gauge | service,route | 当前生效优先级层(3=cart/2=backend/1=svc 兜底) |
| `openresty_route_active_limit` | gauge | service,route | 生效层总并发上限 |
| `openresty_route_healthy_peers` | gauge | service,route | 生效层健康 peer 数 |
| `openresty_adaptive_cc` | gauge | service,route,model | **当前动态并发上限**(AIMD) |
| `openresty_adaptive_cc_min` / `_max` | gauge | service,route,model | 生效下限 / 静态池容量 |
| `openresty_adaptive_cc_conc` | gauge | service,route,model | **当前并发**(timer 判压力用的实时在途) |
| `openresty_adaptive_cc_rej` | gauge | service,route,model | 本区间被压抑需求(并发 429 数) |
| `openresty_tps_ewma` | gauge | service,route,model | 输出 token 速率 EWMA(tok/s,口径同上:分母含 prefill、含非流式) |
| `openresty_ttft_ewma_ms` | gauge | service,route,model | TTFT EWMA(ms) |
| `openresty_tps_limiter_active` / `openresty_ttft_limiter_active` | gauge | service,route | 限流是否生效(1/0) |
| `openresty_rejected_total` | **counter** | service,route,reason | 429 限流累计(reason=concurrency/ttft/tps) |
| `openresty_poll_up` | gauge | — | 上轮 poll 是否全成功(1/0) |
| `openresty_poll_errors_total` | counter | — | poll 出错累计 |
| `openresty_poll_last_success_seconds` | gauge | — | 上次成功 poll 的 unix 秒 |

route 动态发现自监控与富化**同一个发现器**,见上方 `bodylog_route_resolver_*`(不再有独立的 `openresty_discovered_routes` / `openresty_route_discovery_*`)。

**只 poll 一台**:k8s openresty Service(HA 时只选 active leader → 天然单逻辑目标),不带 instance label。

### label 说明

- **`service`**:后端所属 ModelRoute 的 `spec.discovery.service`(`ns/name`,如 `model-service/fallback-model-service-01`)。**推荐主聚合维度** —— pod IP 会漂移、`model` 会抓空,但 service 稳定。富化未命中(裸机/无 SA/pod 刚建未进 EndpointSlice)= `unknown`。
- **`route`**:后端所属 ModelRoute 的 `spec.nginx.route`(如 `fallback-model-service-0.1`),省略则取 `metadata.name`(与 autoconfig 生成 `session_route_<route>.conf` 的规则一致;只有连 nginx 段都没有的 monitor-only 才不计),与 service 1:1。未命中 = `unknown`。
- **`backend`**:归一化后的**真实后端** `host:port`(= bodylog 聚合的 peer key)。缺失时为 `(none)`(如未路由的 4xx);重试会出现 `<peer> (retry#1)` 变体。
- **`model`**:响应里的 served-model-name(取自 `resp_meta.model`,**有界**);缺失为 `unknown`。**纯取明细,不用 ModelRoute 兜底**(要稳定的服务维度用 `service`)。
- **`status_class`**:`2xx` / `4xx` / `5xx` / `other`(429 归 `4xx`;精确 429 分 reason 见 `openresty_rejected_total`)。
- **`stream`**:`true` / `false` / `unknown`(请求未声明 stream 时)。
- **`finish_reason`**:停止原因串(如 `stop`/`length`/`tool_calls`);为空不计。
- **`prompt_bucket`**:输入长度档位(仅 `bodylog_ttft_seconds` / `bodylog_output_tok_per_second`)。取值见上方档位表;`prompt_tokens<=0` → `unknown`。

### 富化:podIP→service/route(单一发现器)

`bodylog_*`(明细)与 `openresty_*`(poll)都需要把「后端」归到一个稳定的服务身份上。**同一个发现器**(in-cluster SA REST,不引 client-go)一份 ModelRoute list 产出三样:

1. **`map[podIP]→{route,service}`**:每个 ModelRoute 的 `discovery.service` → 查该 Service 的 EndpointSlice 得 pod IP 集 → 反建。`observe()` 用 `backend` 的 IP(剥 `:port`/`(retry#N)`)查表打 `service`/`route`。**含未就绪 pod**(它可能刚服务过一个请求,仍要能归属)。
2. **每 service 副本数**(`bodylog_service_replicas` / `_ready`)。
3. **route 列表 + route→service**:给 openresty-poll 用(route 列表按 `OPENRESTY_SERVICE` 过滤;service map 含全部 route)。

周期刷新(`ROUTE_DISCOVERY_INTERVAL_SECONDS`,默认 30s),pod/route 增删自动跟随;apiserver 抖动某轮失败 → **保留上次不清空**。**仅 in-cluster 启用**(需 `KUBERNETES_SERVICE_HOST` + SA);裸机 exporter 无此能力 → `service`/`route` 恒 `unknown`(其余指标照常)。**RBAC 需 `list` `modelroutes` + `endpointslices`**(helm `rbac.yaml` 已建)。

> **⚠️ `service` label 与 target label 撞名**:kube-prometheus-stack 抓取时会注入一个 target label `service`(= 抓的 k8s Service 名)。必须 **ServiceMonitor `honorLabels: true`**(bodylog chart 与本 README 的 manifest 都已设),让 exporter 自带的 `service`(模型 service)胜出,`by(service)` 才聚合到模型而非抓取目标。

### ⚠️ 字段可靠性(0/空按"缺测"跳过,不污染分位/不造无谓 series)

- `ttft`(frt):**只在 `stream=true` 时记**(明细里的 `stream` 由 listener 从请求体正则抠出)。
  ⚠️ 早期版本按 `frt > 0` 过滤,以为「非流式 frt=0」——**实际非流式的 `first_chunk_t ≈ rt`**
  (只有一个 body chunk,它到达时响应就结束),恒 >0,一条都没滤掉,导致 TTFT 直方图混进大量
  非流式样本,实测 p99(82s)甚至超过 RT p99(77s)。0.1.10 起改为按 `stream` 过滤。
  代价:`req_body` 未采到/为 base64 时 `stream` 抠不出来(=null),该请求即使是流式也会漏记 TTFT
  ——**样本变少,不会算错**;实测 800 条明细中 `stream=null` 的无一呈流式形态。
- **token 类**:响应无 usage 时为 0 → **流式请求必须带 `stream_options.include_usage=true`**,否则 `completion_tokens`=0、`output_tok_per_second` 无数据。
- `model`/`finish_reason`:响应非 JSON(如 error)时可能为空。
- `reasoning_tokens`:非推理模型恒 0。

---

## 配置(环境变量)

| 变量 | 默认 | 说明 |
|---|---|---|
| `BODYLOG_DETAILS_DIR` | `/data/bodylog/metrics/details` | bodylog 的 `metrics/details` 目录(要能读到) |
| `EXPORTER_LISTEN` | `:9110` | 监听地址(k8s 抓取需 `0.0.0.0:9110`) |
| `CHECKPOINT_PATH` | `/var/lib/bodylog-exporter/checkpoint.json` | offset 持久化文件 |
| `BODYLOG_URL` | 空 | bodylog HTTP base(如 `http://127.0.0.1:9998`),**仅跨天缺口补读用**;空=不补读 |
| `BODYLOG_HTTP_TOKEN` | 空 | bodylog `/metrics` 鉴权(补读用) |
| `RECOVERY_LAG_SECONDS` | `10` | 补读上界 = `now - lag`(等 flush 落定) |
| `FOLLOW_INTERVAL_MS` | `300` | tail 到 EOF 后的轮询间隔 |
| `DATE_TZ` | `Asia/Shanghai` | 文件按天命名的时区,**须与 bodylog 一致**;无 tzdata 回退固定 `+08:00`(中国无 DST 正好正确) |

### openresty-poll 配置(可选;空 URL = 只 tail、不 poll)

| 变量 | 默认 | 说明 |
|---|---|---|
| `OPENRESTY_POLL_URL` | 空 | openresty base(如集群内 `http://openresty:8080`);**空=不启用**。裸机 exporter 够不到 k8s 故默认关 |
| `OPENRESTY_POLL_ROUTES` | 空 | **静态逃生口**:逗号分隔 route 列表(如 `qwen,opt`)。**非空则不走动态发现**;留空=动态发现 |
| `OPENRESTY_POLL_INTERVAL_MS` | `15000` | poll 周期 |
| `OPENRESTY_POLL_TIMEOUT_MS` | `3000` | 单请求超时 |

**route 集合默认【动态发现】**(不填 `OPENRESTY_POLL_ROUTES` 时):走**与富化同一个发现器**(in-cluster SA 列 **ModelRoute CR** `routing.gpucluster.io/v1alpha1`),取 `spec.nginx.route`(省略则 `metadata.name`,同 autoconfig)得 route 列表、`spec.discovery.service` 得 route→service,周期刷新、增删自动跟随 —— **无需静态配、route 变了不重启**。下面这几个 env **同时**控制富化和 poll 发现(一份 list 两用):

| 变量 | 默认 | 说明 |
|---|---|---|
| `MODELROUTE_GROUP` / `_VERSION` / `_PLURAL` | `routing.gpucluster.io` / `v1alpha1` / `modelroutes` | CR 坐标 |
| `ROUTE_DISCOVERY_INTERVAL_SECONDS` | `30` | list ModelRoute 周期(富化 + poll 发现共用) |
| `ROUTE_DISCOVERY_TIMEOUT_MS` | `4000` | 单次 list/EndpointSlice 请求超时 |
| `OPENRESTY_SERVICE` | 空 | 只把 `nginx.service` 指向本 openresty 的 route 计入 **poll 列表**(多 openresty 时用);空=全要。**富化映射/service map 不受此过滤**(全 route) |

发现自监控:`bodylog_route_resolver_up`(1/0)、`bodylog_route_resolver_routes`(route 数)、`bodylog_route_resolver_pods`(映射覆盖 pod 数)、`bodylog_route_resolver_errors_total`。apiserver 抖动某轮失败 → **保留上次成功的映射/routes 不清空**。RBAC 需 `list` `modelroutes`(发现)+ `endpointslices`(富化查后端 pod)。

> **只读**:poll 的都是 openresty 的 GET 状态端点(`_route_state`/`_tps_status`/`_ttft_status`/`_429_status`),不改任何配置、不 reload。k8s 部署时作 bodylog **sidecar**(同 pod 可短名 `http://openresty:8080` 直连,SA 带 modelroute list 权限)。

---

## 部署

### 裸机 systemd(主场景,如生产 bodylog 所在的 ts31/ts34)

```bash
# 二进制放主机;编辑 bodylog-exporter.service 的 Environment
cp bodylog-exporter /usr/local/bin/
cp bodylog-exporter.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now bodylog-exporter
curl -s localhost:9110/metrics | head
```

### k8s Prometheus 抓取(裸机 target 走 Endpoints)

bodylog 在裸机、Prometheus 在 k8s(kube-prometheus-stack):建**无 selector 的 Service + 手写 Endpoints(裸机 IP:9110)+ ServiceMonitor**(label 带 `release: kube-prometheus-stack`)。现成 manifest:[`deploy/prometheus-scrape.yaml`](deploy/prometheus-scrape.yaml)(`kubectl apply -f`,换主机改 name/IP 即可):

```yaml
apiVersion: v1
kind: Service
metadata: { name: bodylog-ts31, namespace: monitoring, labels: { app: bodylog-exporter } }
spec: { clusterIP: None, ports: [ { name: metrics, port: 9110, targetPort: 9110 } ] }
---
apiVersion: v1
kind: Endpoints
metadata: { name: bodylog-ts31, namespace: monitoring, labels: { app: bodylog-exporter } }
subsets: [ { addresses: [ { ip: 10.0.0.1 } ], ports: [ { name: metrics, port: 9110 } ] } ]
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata: { name: bodylog-ts31, namespace: monitoring, labels: { release: kube-prometheus-stack } }
spec:
  namespaceSelector: { matchNames: [ monitoring ] }
  selector: { matchLabels: { app: bodylog-exporter } }
  endpoints: [ { port: metrics, path: /metrics, interval: 30s, honorLabels: true } ]
```

### k8s:独立 chart(与 listener 同节点,一个实例同时出两类指标)

exporter 是**独立 chart**(不再是 bodylog chart 的 sidecar):只读 tail listener 落在**节点本地盘**的明细(`bodylog_*`)+ poll 集群内 openresty(`openresty_*`)。**一个实例 = tail + poll 两半全出**。独立部署的好处是升级 exporter 不重启 listener,收帧/落盘零中断;代价是**必须与 listener 钉在同一节点**(明细在该节点本地盘)。

chart 发布在 [modelsphere/helm-charts](https://github.com/modelsphere/helm-charts):

```bash
helm repo add modelsphere https://modelsphere.github.io/helm-charts
helm repo update

# 先装 listener
helm -n <ns> upgrade --install bodylog modelsphere/bodylog \
  --set-string secret.token=<token> \
  --set persistence.hostPath=/data/bodylog \
  --set nodeSelector."kubernetes\.io/hostname"=<node>

# 再装 exporter:同节点 + hostPath 与 listener 一致 + 复用它的 Secret
helm -n <ns> upgrade --install bodylog-exporter modelsphere/bodylog-exporter \
  --set data.hostPath=/data/bodylog \
  --set nodeSelector."kubernetes\.io/hostname"=<node>
```

关键 values(详见 chart 自带的 `values.yaml`):

```yaml
data:
  dir: /data/bodylog             # 容器内挂载点,须 = listener 的 dataDir
  hostPath: /data/bodylog        # 节点真实目录,须 = listener persistence.hostPath
openrestyPoll:
  url: "http://openresty:8080"   # 集群内 openresty Service;空=只 tail 不 poll
  routes: []                     # 留空=动态发现;填了=静态覆盖
  intervalMs: 15000
routeDiscovery:
  enabled: true                  # list ModelRoute CR(chart 自动建 SA+ClusterRole+Binding)
  openrestyService: ""           # 只统计 nginx.service 指向本 openresty 的 route;空=全要
serviceMonitor: { enabled: true, releaseLabel: kube-prometheus-stack }
```

⚠️ `data.hostPath` 非空但 `nodeSelector` 为空 → chart 直接 `fail`(否则 pod 调度到别的 node 会读到空目录,看着像"一条数据都没有")。

Service 自动加 `metrics:9110` 口 + ServiceMonitor(集群内直接抓,已置 `honorLabels: true` 解决 `service` 撞名)。

**data 卷三选一**(`values.persistence`):
- `persistence.enabled: true`(默认)→ 建 **PVC**(`storageClassName`/`size`);
- **`persistence.hostPath: <路径>`** → 用**宿主机目录**(复用某台机器已有的 bodylog 数据盘,不建 PVC)。**必须配 `nodeSelector`** 把 pod 钉到那台 node,否则 `DirectoryOrCreate` 会在别的 node 新建空目录(看似丢历史数据)—— chart 已加 **helm 硬校验**:hostPath 非空但 nodeSelector 为空直接 `fail`;
- `persistence.enabled: false` → emptyDir(仅测试,重启丢数据)。

```bash
# 复用宿主机裸盘(如 bodylog 从裸机迁进 k8s,续用原数据目录)
helm -n <ns> upgrade --install bodylog modelsphere/bodylog --version <tag> \
  --set-string secret.token=<token> \
  --set persistence.hostPath=/data/bodylog \
  --set nodeSelector."kubernetes\.io/hostname"=<node>
```

### k8s:独立 poll-only 部署(集群没 bodylog 时)

只想把 openresty 控制面态导进 Prometheus、集群里又没 bodylog:用独立 Deployment(tail 空跑,只 poll)。现成 manifest [`deploy/exporter-standalone.yaml`](deploy/exporter-standalone.yaml)(含 SA+ClusterRole+Binding+Deployment+Service+ServiceMonitor,`OPENRESTY_POLL_URL=http://openresty:8080` + 动态发现):

```bash
kubectl apply -f deploy/exporter-standalone.yaml   # 换 ns/openresty Service 名即可复用
```

### 裸机启用 openresty-poll(可选)

裸机 systemd 场景默认只 tail;要顺带 poll **同机**的 openresty,给 `bodylog-exporter.service` 的 Environment 加 `OPENRESTY_POLL_URL`(如 `http://127.0.0.1:18080`)+ `OPENRESTY_POLL_ROUTES`(裸机非 k8s、无 ModelRoute API → 用静态 route 列表)。

CI:`.github/workflows/release.yml` 的 `image` job(matrix 里的 `exporter`)。**只发 exporter 用带前缀的 tag**:

```bash
git tag exporter/v0.2.0 && git push origin exporter/v0.2.0
# → 镜像 4pdosc/bodylog-exporter:0.2.0
#   (前缀被剥掉;openresty / bodylog 不进 matrix,它们的 :latest 也不会被挪)
```

同理 `openresty/v*` / `bodylog/v*`;**无前缀的 tag(如 `0.1.13`)仍然三个组件一起发**。
认不出的前缀(打错成 `exporters/v1`)直接让 workflow 失败 —— 宁可什么都不发生。
预发布 tag(带 `-`,如 `0.2.0-rc1`)照常出镜像,但**不动 `:latest`**。

chart 不在本仓发布:三个 chart 在
[modelsphere/helm-charts](https://github.com/modelsphere/helm-charts),
由那个仓的 chart-releaser 发到 GitHub Pages。

---

## 常用 PromQL

**按 `service` 聚合是推荐用法**(pod IP 漂移、model 抓空都不影响):

```promql
# 每 service QPS / 输出吞吐(tok/s)
sum by(service)(rate(bodylog_requests_total{backend!="(none)"}[1m]))
sum by(service)(rate(bodylog_completion_tokens_total[1m]))

# 每 service TTFT p95 / RT p95(秒)—— native histogram:裸名、不带 _bucket/le
histogram_quantile(0.95, sum by(service)(rate(bodylog_ttft_seconds[5m])))
histogram_quantile(0.95, sum by(service)(rate(bodylog_rt_seconds[5m])))

# 每 service 错误率
sum by(service)(rate(bodylog_requests_total{status_class=~"4xx|5xx",backend!="(none)"}[5m]))
  / clamp_min(sum by(service)(rate(bodylog_requests_total{backend!="(none)"}[5m])),0.001)

# 每 service 副本数 / 每就绪副本吞吐
bodylog_service_replicas_ready
sum by(service)(rate(bodylog_completion_tokens_total[1m])) / bodylog_service_replicas_ready

# 每 service 429 速率(按 reason 细分)—— openresty 控制面 counter
sum by(service)(rate(openresty_rejected_total[1m]))
sum by(service,reason)(rate(openresty_rejected_total[1m]))

# 单请求生成速率 p50(每条请求 completion/rt 的分布)
histogram_quantile(0.5, sum by(service)(rate(bodylog_output_tok_per_second[5m])))

# Watchmen overall/decode TPS 均值(按 service 聚合;新指标不带 prompt_bucket)
histogram_avg(sum by(service)(rate(bodylog_overall_output_tok_per_second[1m])))
histogram_avg(sum by(service)(rate(bodylog_decode_output_tok_per_second[1m])))

# 更细:某 service 下每个 backend(pod)QPS —— pod 扩缩时按 service 聚合更稳
sum by(backend)(rate(bodylog_requests_total{service="model-service/fallback-model-service-01"}[1m]))

# exporter 是否滞后(last_ts 与 now 差)
time() - bodylog_exporter_last_ts_seconds
```

> histogram 为 **native-only**(2026-08-31 起,exporter 0.2.0):不再双发经典桶,`_bucket`/`_sum`/`_count` 不再产生。
> 分位数用裸名:`histogram_quantile(0.95, sum by(service)(rate(bodylog_ttft_seconds[5m])))`;
> 均值用 `histogram_sum(...)/histogram_count(...)`。⚠️ 前提是 Prometheus 侧支持 native histogram —— 若关闭,这三个直方图将没有任何桶。

### 自动扩缩容(LLMScaler `metrics` 片段)

```yaml
metrics:
  - name: qps-per-pod        # 负载可按副本分摊,适合 ratio 扩缩
    query: 'avg(sum by(backend)(rate(bodylog_requests_total{model="qwen",backend!="(none)"}[1m])))'
    target: "<单 pod QPS 预算>"
  - name: ttft-p95           # 饱和护栏,NaN 兜底 0
    query: 'histogram_quantile(0.95, sum(rate(bodylog_ttft_seconds{model="qwen"}[2m]))) or vector(0)'
    target: "2"
```

**用 openresty 控制面态扩缩容(比 bodylog 更即时)**:`active_conns` / `adaptive_cc` 是实时压力,反应比事后 counter 快:

```promql
# 每 pod 平均并发占用率(当前并发 / 静态上限)—— 越接近 1 越该扩
avg(openresty_peer_active_conns{route="qwen"}) / avg(openresty_peer_max_concurrency{route="qwen"})

# 自适应并发已顶到池容量(AIMD 打满 → 需要更多 pod)
avg(openresty_adaptive_cc{route="qwen"}) / avg(openresty_adaptive_cc_max{route="qwen"})

# 有 peer 被 ban(健康恶化,别盲目缩容)
sum(openresty_peer_banned{route="qwen"})

# 并发 429 正在发生(容量不足的直接信号)
sum(rate(openresty_rejected_total{route="qwen",reason="concurrency"}[1m]))
```

---

## 覆盖边界(重要)

本 exporter **只覆盖流量侧**指标。以下 bodylog 拿不到,需**别的 Prometheus 源**(通常已在集群):

| 需求 | 来源 |
|---|---|
| KV cache 占用 / 运行中 / 等待中 / cache 命中率 | 引擎 `/metrics`(`sglang:token_usage`/`num_running_reqs`/`num_queue_reqs`/`cache_hit_rate`、`vllm:*`,需 `--enable-metrics`) |
| GPU 温度 / 显存 / 利用率 | `nvidia-dcgm-exporter`(`DCGM_FI_DEV_*`) |
| 服务 up/down | Prometheus `up{}`(bodylog 只看发生过的请求,**空闲≠宕机**) |

整套 monitor 看板 = **bodylog exporter + 引擎 /metrics + dcgm + up{}** 四类源合起来。

---

## 崩溃/丢数据语义

| 谁挂 | 结果 |
|---|---|
| exporter 短挂(当天 jsonl 还在) | **不丢**:重启从 offset 续读补齐;counter 归零由 `rate()` 处理 |
| exporter 长挂(跨当天 `jsonl→parquet` 轮转) | 缺口走 `/metrics` HTTP 补读(读 parquet+jsonl)→ **不丢**(需配 `BODYLOG_URL`) |
| bodylog 挂 | 已落盘明细安全;恢复后继续 append,exporter 继续 tail |
