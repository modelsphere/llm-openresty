# bodylog-exporter

把 `bodylog-listener` 落的**每请求明细 jsonl** 翻译成 **Prometheus 指标**的独立小工具。用于把 LLM 网关的**流量侧**指标(QPS、token 速率、延迟/TTFT 分位、错误率)接进 Prometheus,供 Grafana 看板与**自动扩缩容(LLMScaler / KEDA / HPA)**消费。

- **不改 `bodylog-listener`**:按字节 offset `tail` append-only 的 `details/<date>.jsonl`,逐行增量 `observe`。
- **零丢**:文件当持久缓冲;offset checkpoint 续读;跨天 `jsonl→parquet` 轮转的缺口走 bodylog `/metrics` HTTP 补读(request_id 去重)。
- **Prometheus 原生**:counter 单调累计、histogram 桶累计 → `rate()`/`histogram_quantile()` 正确(不是 scrape 时现算的窗口 gauge)。

---

## 暴露的指标

端点:`GET <listen>/metrics`(默认 `:9110`,Prometheus 文本格式,无鉴权 —— 只有聚合值、无请求正文)。

### 业务指标(维度:`backend` / `model`)

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
| `bodylog_rt_seconds` | **histogram** | `stream` | 总响应时间(秒);桶 `0.1 .25 .5 1 2 5 10 20 30 60 120 300` |
| `bodylog_ttft_seconds` | **histogram** | — | 首 token 时间/TTFT(秒,**仅流式**);桶 `.05 .1 .2 .5 1 2 3 5 10` |
| `bodylog_output_tok_per_second` | **histogram** | — | **单请求生成速率** `completion_tokens/rt`(tok/s);桶 `5 10 20 30 50 80 120 200 400` |

### exporter 自监控

| 指标 | 类型 | 含义 |
|---|---|---|
| `bodylog_exporter_lines_total` | counter | 已 observe 的明细行数 |
| `bodylog_exporter_offset_bytes` | gauge | 当前 tail 文件的字节 offset |
| `bodylog_exporter_last_ts_seconds` | gauge | 最新 observe 行的结束时刻(unix 秒),**判滞后** |
| `bodylog_exporter_recovery_total` | counter | 跨天缺口 HTTP 补读次数 |

### openresty 控制面指标(openresty-poll,可选)

**流量侧(上面)是事后逐请求;这一组是 openresty 的即时控制面态**(每 peer 当前并发、被 ban 的 peer、限流档位)——bodylog 拿不到,**对自动扩缩容反应更快**。开启方式见下方「openresty-poll 配置」:exporter 定时 GET openresty 已暴露的 JSON 端点(`/<route>/_route_state|_tps_status|_ttft_status|_429_status`),**不改 openresty**。全是 **gauge**(快照,每 poll 周期清空重填 → 掉线 peer 自动消失),仅 `openresty_rejected_total` 是 counter。

维度:`route`(=ModelRoute 名)/ `peer`(真后端 host:port)/ `name`(peer 名)/ `priority`(路由层级)/ `model`(子池,无分模型时为 `_`)。

| 指标 | 类型 | label | 含义 |
|---|---|---|---|
| `openresty_peer_active_conns` | gauge | route,peer,name,priority | 该 peer **当前并发**(least_conn 计数) |
| `openresty_peer_banned` | gauge | 同上 | 是否被健康检查 **ban**(1/0) |
| `openresty_peer_max_concurrency` | gauge | 同上 | 该 peer 静态并发上限 |
| `openresty_route_active_level` | gauge | route | 当前生效优先级层(3=cart/2=backend/1=svc 兜底) |
| `openresty_route_active_limit` | gauge | route | 生效层总并发上限 |
| `openresty_route_healthy_peers` | gauge | route | 生效层健康 peer 数 |
| `openresty_adaptive_cc` | gauge | route,model | **当前动态并发上限**(AIMD) |
| `openresty_adaptive_cc_min` / `_max` | gauge | route,model | 生效下限 / 静态池容量 |
| `openresty_adaptive_cc_conc` | gauge | route,model | **当前并发**(timer 判压力用的实时在途) |
| `openresty_adaptive_cc_rej` | gauge | route,model | 本区间被压抑需求(并发 429 数) |
| `openresty_tps_ewma` | gauge | route,model | 解码速率 EWMA(tok/s) |
| `openresty_ttft_ewma_ms` | gauge | route,model | TTFT EWMA(ms) |
| `openresty_tps_limiter_active` / `openresty_ttft_limiter_active` | gauge | route | 限流是否生效(1/0) |
| `openresty_rejected_total` | **counter** | route,reason | 429 限流累计(reason=concurrency/ttft/tps) |
| `openresty_poll_up` | gauge | — | 上轮 poll 是否全成功(1/0) |
| `openresty_poll_errors_total` | counter | — | poll 出错累计 |
| `openresty_poll_last_success_seconds` | gauge | — | 上次成功 poll 的 unix 秒 |

**route 动态发现自监控**(走 k8s ModelRoute 发现时,见下方「openresty-poll 配置」):

| 指标 | 类型 | 含义 |
|---|---|---|
| `openresty_discovered_routes` | gauge | 当前发现到的 route 数(= list ModelRoute CR 得到) |
| `openresty_route_discovery_up` | gauge | 上轮 list ModelRoute 成功(1/0);失败时**保留上次 routes 不清空** |
| `openresty_route_discovery_errors_total` | counter | ModelRoute 发现出错累计 |

**只 poll 一台**:k8s openresty Service(HA 时只选 active leader → 天然单逻辑目标),不带 instance label。

### label 说明

- **`backend`**:归一化后的**真实后端** `host:port`(= bodylog 聚合的 peer key)。缺失时为 `(none)`(如未路由的 4xx);重试会出现 `<peer> (retry#1)` 变体。
- **`model`**:响应里的 served-model-name(取自 `resp_meta.model`,**有界**);缺失为 `unknown`。
- **`status_class`**:`2xx` / `4xx` / `5xx` / `other`。
- **`stream`**:`true` / `false` / `unknown`(请求未声明 stream 时)。
- **`finish_reason`**:停止原因串(如 `stop`/`length`/`tool_calls`);为空不计。

### ⚠️ 字段可靠性(0/空按"缺测"跳过,不污染分位/不造无谓 series)

- `ttft`(frt):**非流式恒 0**,跳过 → TTFT 只统计流式请求。
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

**route 集合默认【动态发现】**(不填 `OPENRESTY_POLL_ROUTES` 时):exporter 用 pod 的 in-cluster ServiceAccount 列 **ModelRoute CR**(`routing.gpucluster.io/v1alpha1`,`spec.nginx.route`)得到"当前 k8s 里所有 route",周期刷新、增删自动跟随 —— **无需静态配、route 变了不重启**。需 RBAC(SA 有 `list modelroutes`,helm `rbac.yaml` 已建)。

| 变量 | 默认 | 说明 |
|---|---|---|
| `MODELROUTE_GROUP` / `_VERSION` / `_PLURAL` | `routing.gpucluster.io` / `v1alpha1` / `modelroutes` | CR 坐标 |
| `ROUTE_DISCOVERY_INTERVAL_SECONDS` | `30` | list ModelRoute 周期 |
| `OPENRESTY_SERVICE` | 空 | 只要 `nginx.service` 指向这台 openresty 的 route(多 openresty 时用);空=全要 |

发现自监控:`openresty_route_discovery_up`(1/0)、`openresty_discovered_routes`(route 数)、`openresty_route_discovery_errors_total`。apiserver 抖动某轮失败 → **保留上次成功的 routes 不清空**。

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

### k8s sidecar(集群自带 bodylog 时)

exporter 作 sidecar 加进 bodylog pod,**共享挂载 bodylog 卷**(只读),Service + ServiceMonitor 同上。

CI:`openresty/.gitlab-ci.yml` 的 `build:exporter` 打 git tag 出镜像 `registry.example.com/llm/bodylog-exporter:<tag>`。

---

## 常用 PromQL

```promql
# 每 model QPS(排除 4xx 无后端噪声)
sum by(model) (rate(bodylog_requests_total{backend!="(none)"}[1m]))

# 每 model 错误率
sum by(model)(rate(bodylog_requests_total{status_class=~"4xx|5xx",backend!="(none)"}[5m]))
  / clamp_min(sum by(model)(rate(bodylog_requests_total{backend!="(none)"}[5m])),0.001)

# 每 model TTFT p95 / 总响应时间 p95(秒)
histogram_quantile(0.95, sum by(model,le)(rate(bodylog_ttft_seconds_bucket[5m])))
histogram_quantile(0.95, sum by(model,le)(rate(bodylog_rt_seconds_bucket[5m])))

# 每 model 输出吞吐(tok/s) 与 单请求生成速率 p50
sum by(model)(rate(bodylog_completion_tokens_total[1m]))
histogram_quantile(0.5, sum by(model,le)(rate(bodylog_output_tok_per_second_bucket[5m])))

# 每 peer(pod)QPS —— backend 就是真后端;pod 扩缩时用 model 聚合更稳
sum by(backend)(rate(bodylog_requests_total{model="qwen"}[1m]))

# exporter 是否滞后(last_ts 与 now 差)
time() - bodylog_exporter_last_ts_seconds
```

### 自动扩缩容(LLMScaler `metrics` 片段)

```yaml
metrics:
  - name: qps-per-pod        # 负载可按副本分摊,适合 ratio 扩缩
    query: 'avg(sum by(backend)(rate(bodylog_requests_total{model="qwen",backend!="(none)"}[1m])))'
    target: "<单 pod QPS 预算>"
  - name: ttft-p95           # 饱和护栏,NaN 兜底 0
    query: 'histogram_quantile(0.95, sum by(le)(rate(bodylog_ttft_seconds_bucket{model="qwen"}[2m]))) or vector(0)'
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
