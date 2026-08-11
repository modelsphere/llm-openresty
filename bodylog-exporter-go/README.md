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
