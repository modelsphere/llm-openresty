# bodylog-exporter

A small standalone tool that turns the **per-request detail jsonl** written by
`bodylog-listener` into **Prometheus metrics**. It brings the LLM gateway's
**traffic-side** numbers (QPS, token rates, latency/TTFT quantiles, error rates)
into Prometheus, for Grafana boards and for **autoscaling** (LLMScaler / KEDA / HPA).

- **Does not touch `bodylog-listener`**: it `tail`s the append-only
  `details/<date>.jsonl` by byte offset and `observe`s each new line.
- **Loses nothing**: the file is the durable buffer; an offset checkpoint resumes
  reading; the gap left by the day's `jsonl -> parquet` rotation is filled over
  bodylog's `/metrics` HTTP endpoint (de-duplicated by request_id).
- **Native Prometheus semantics**: counters accumulate monotonically and
  histograms accumulate buckets, so `rate()` and `histogram_quantile()` are
  correct -- these are not windowed gauges computed at scrape time.

---

## Exposed metrics

Endpoint: `GET <listen>/metrics` (default `:9110`, Prometheus text format, no
authentication -- it carries aggregates only, never request bodies).

### Traffic metrics (dimensions: `service` / `route` / `backend` / `model`)

**Every traffic metric carries four dimensions**: `service` (the ModelRoute's
`discovery.service`, as `ns/name`, **the dimension to aggregate by**), `route`
(the `nginx.route`, or the ModelRoute's `metadata.name` when omitted, matching
autoconfig), `backend` (the real backend pod `IP:port`, which drifts), and
`model` (the served-model-name from the detail line, possibly empty).
`service` and `route` come from **enrichment**, which looks a `backend` pod IP
back up to its ModelRoute (see "Enrichment: podIP -> service/route" below); they
exist only in-cluster and degrade to `unknown` on bare metal.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `bodylog_requests_total` | counter | `status_class`, `stream` | Requests |
| `bodylog_prompt_tokens_total` | counter | — | Prompt tokens |
| `bodylog_completion_tokens_total` | counter | — | Completion tokens |
| `bodylog_cached_tokens_total` | counter | — | Cached (prefix-hit) tokens |
| `bodylog_reasoning_tokens_total` | counter | — | Reasoning tokens (reasoning models only) |
| `bodylog_total_tokens_total` | counter | — | Total tokens |
| `bodylog_req_bytes_total` | counter | — | Request body bytes |
| `bodylog_resp_bytes_total` | counter | — | Response body bytes |
| `bodylog_finish_reason_total` | counter | `finish_reason` | Requests by stop reason |
| `bodylog_rt_seconds` | **histogram** (native-only) | `stream` | Total response time (seconds) |
| `bodylog_ttft_seconds` | **histogram** (native-only) | **`prompt_bucket`** | Time to first token (seconds, **streaming only**) |
| `bodylog_output_tok_per_second` | **histogram** (native-only) | **`prompt_bucket`** | **Per-request output token rate**, `completion_tokens/rt` (tok/s; **the denominator is the whole duration and includes prefill**; streaming and non-streaming alike, filtered to `ctok>=16` and `rt>=0.5s`) |
| `bodylog_overall_output_tok_per_second` | **histogram** (native-only) | — | **Overall output token rate**, `completion_tokens/rt` (tok/s); only requests with a 2xx `status`, `completion_tokens>0` and `rt>0`, with no `prompt_bucket`/`stream` filter |
| `bodylog_decode_output_tok_per_second` | **histogram** (native-only) | — | **Decode output token rate**, `completion_tokens/(lct-frt)` (tok/s); only 2xx requests with `completion_tokens>0` and `lct-frt>0.3s`, with no `prompt_bucket`/`stream` filter |

> **⚠️ Since 2026-08-31 (exporter 0.2.0) all five histograms are native-only** --
> classic buckets are no longer emitted in parallel, so the
> `bodylog_*_bucket` / `_sum` / `_count` series **no longer exist**. Take quantiles
> from the **bare metric name, with no `le`**:
> `histogram_quantile(0.95, sum by(service)(rate(bodylog_ttft_seconds[5m])))`.
> Why: Prometheus has `scrapeClassicHistograms` enabled, so emitting both really
> did land both in the TSDB (measured: 12x the series).

`bodylog_output_tok_per_second` is the legacy metric. Its meaning and HELP stay
as they are (`prompt_bucket`, `completion_tokens>=16`, `rt>=0.5s`) and it must
not be used as the TPS definition for new experiments; the overall/decode
metrics that Watchmen consumes are new metrics and the old one is not a
substitute. `decode` deliberately uses `lct-frt` from the detail JSON, requires
only `lct-frt>0.3s`, and does not additionally constrain `stream`. All three TPS
metrics observe one histogram sample per request, so a query only has to
aggregate by `service`.

#### `prompt_bucket`: input-length bands (ttft / output_tok_per_second)

For questions like "the TTFT p80 for this context range". **26 bands plus
`unknown`**, left-closed and right-open, zero-padded to four digits so
lexical order equals numeric order:

```
0000k_0001k 0001k_0002k 0002k_0003k 0003k_0004k 0004k_0006k 0006k_0008k
0008k_0010k 0010k_0012k 0012k_0016k 0016k_0020k 0020k_0024k 0024k_0032k
0032k_0040k 0040k_0048k 0048k_0064k 0064k_0080k 0080k_0096k 0096k_0128k
0128k_0160k 0160k_0192k 0192k_0256k 0256k_0384k 0384k_0512k 0512k_0768k
0768k_1024k 1024k_inf   unknown
```

- The boundaries fall on the sizes that get asked about (6/16/32/64/128/256k and
  so on). A query **merges adjacent bands** with a regex -- it can only go
  coarser, never finer:
  ```promql
  # TTFT p80 for 6~16K of input
  histogram_quantile(0.8, sum(rate(bodylog_ttft_seconds{
    model="kimi-k2.5", prompt_bucket=~"0006k_0008k|0008k_0010k|0010k_0012k|0012k_0016k"}[5m])))
  ```
- **`unknown`** means `prompt_tokens<=0` (usage missing). Measured, these are
  mostly **failed requests** with `status` 400/429, where `frt` is how long the
  error took to come back and has nothing to do with input length -- keeping
  them in their own band avoids polluting the first one (mixing them in inflated
  the first band's TTFT p80 from 0.11s to 1.37s).
- `1024k_inf` is an overflow sentinel (past the model's context limit) and should
  normally be empty. An empty band produces no series, so it costs nothing.
- ⚠️ **Band boundaries cannot be changed once in use**: changing them stops the
  old labels, starts the new values from zero, and makes any
  `histogram_quantile` spanning the change untrustworthy. For an arbitrary range
  or quantile, query the detail parquet instead (kept 365 days); there is a
  DuckDB example in the comments in `metrics.go`.

### Service replica counts (enrichment; dimensions `service` / `route`)

The number of backend pods behind a ModelRoute's `discovery.service`, from its
EndpointSlices (in-cluster only). Joins with the throughput metrics above to
give per-replica throughput.

| Metric | Type | Meaning |
|---|---|---|
| `bodylog_service_replicas` | gauge | Backend pods **in total** for this service (EndpointSlice endpoints, ready or not) |
| `bodylog_service_replicas_ready` | gauge | **Ready** backend pods for this service (`conditions.ready`; nil counts as ready, by convention) |
| `bodylog_service_replicas_desired` | gauge | **Desired** replicas for this service (the top-level workload's `spec.replicas`); **not emitted** when it cannot be read, because a 0 would read as "scaled to zero" |

All three carry a **`role`** label:

- An ordinary service has `role=""`.
- A **prefill/decode-disaggregated (DisaggregatedSet) service** is split in
  three. `role="router"` is what counting the `discovery.service` itself yields:
  under disaggregation that Service selects each slice's **router** -- the pods
  holding the public port -- not the engines. `role="prefill"` and
  `role="decode"` are the engines, taken from the per-(role, slice)
  LeaderWorkerSets that the DisaggregatedSet creates.

**One LWS group is one engine** (a leader and its workers), so the role
dimension counts engines rather than pods -- with
`leaderWorkerTemplate.size > 1`, a pod count could never match the desired
number. The engine counts are deliberately **not** taken from the per-role
headless Services (`<rel>-<role>-hl-s<N>`): those carry
`publishNotReadyAddresses`, so that a router can resolve its upstreams while the
engines are still loading, and their EndpointSlices therefore mark every address
ready. A ready count built on them could never fall -- including through the
drain it is supposed to show.

`role` is one more label on the same metrics rather than a metric of its own
because the `LLMServiceDegraded` rule is `_ready < _replicas`: a binary operator
matches on the **full label set**, so labelling only one side would silently
reduce it to nothing.

> When aggregating, de-duplicate before summing across roles
> (`sum by(service)(max by(service, role)(...))`): the same
> `(service, route, role)` briefly has two series while the exporter restarts.
> An `avg` or `max` taken `by(service)` alone collapses the role dimension along
> with everything else.

### Exporter self-monitoring

| Metric | Type | Meaning |
|---|---|---|
| `bodylog_exporter_lines_total` | counter | Detail lines observed |
| `bodylog_exporter_offset_bytes` | gauge | Current byte offset in the tailed file |
| `bodylog_exporter_last_ts_seconds` | gauge | End time of the newest observed line (unix seconds) -- **use this to detect lag** |
| `bodylog_exporter_recovery_total` | counter | HTTP back-fills of a day-rotation gap |
| `bodylog_route_resolver_up` | gauge | Whether the last ModelRoute discovery/mapping refresh succeeded (1/0); on failure the **previous mapping is kept, not cleared** |
| `bodylog_route_resolver_pods` | gauge | Pods covered by the current podIP -> route mapping |
| `bodylog_route_resolver_routes` | gauge | Routes currently discovered (the poll list) |
| `bodylog_route_resolver_errors_total` | counter | ModelRoute discovery/mapping errors |

### openresty control-plane metrics (openresty-poll, optional)

**The traffic side above is per-request and after the fact; this group is
openresty's instantaneous control-plane state** -- each peer's current
concurrency, which peers are banned, the rate-limiting level. bodylog cannot see
any of it, and **it reacts faster for autoscaling**. See "openresty-poll
configuration" below: the exporter periodically GETs the JSON endpoints
openresty already exposes (`/<route>/_route_state|_tps_status|_ttft_status|_429_status`),
and **openresty itself is not modified**. These are all **gauges** (snapshots,
cleared and refilled every poll cycle, so a peer that disappears stops being
reported); only `openresty_rejected_total` is a counter.

Dimensions: `service` (the `discovery.service`, aligned with `bodylog_*` so both
aggregate by the same key), `route` (the ModelRoute name), `peer` (the real
backend `host:port`), `name` (the peer's name), `priority` (the routing tier),
and `model` (the sub-pool, `_` when there is no per-model split). `service`
again comes from enrichment (route -> `discovery.service`) and falls back to
`unknown`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `openresty_peer_active_conns` | gauge | service,route,peer,name,priority | This peer's **current concurrency** (the least_conn counter) |
| `openresty_peer_banned` | gauge | as above | Whether the health check has **banned** it (1/0) |
| `openresty_peer_max_concurrency` | gauge | as above | This peer's static concurrency ceiling |
| `openresty_route_active_level` | gauge | service,route | The priority tier in effect (3=cart / 2=backend / 1=svc fallback) |
| `openresty_route_active_limit` | gauge | service,route | Total concurrency ceiling of that tier |
| `openresty_route_healthy_peers` | gauge | service,route | Healthy peers in that tier |
| `openresty_adaptive_cc` | gauge | service,route,model | **The current dynamic concurrency ceiling** (AIMD) |
| `openresty_adaptive_cc_min` / `_max` | gauge | service,route,model | The floor in effect / the static pool capacity |
| `openresty_adaptive_cc_init` | gauge | service,route,model | Start point and low-traffic shrink floor (= min unless `adaptive_cc_init` is set; absent on engines that predate it) |
| `openresty_adaptive_cc_conc` | gauge | service,route,model | **Current concurrency** (what the timer reads to judge pressure) |
| `openresty_adaptive_cc_rej` | gauge | service,route,model | Demand suppressed this interval (concurrency 429s) |
| `openresty_tps_ewma` | gauge | service,route,model | Output token rate EWMA (tok/s, same definition as above: the denominator includes prefill, and non-streaming requests count) |
| `openresty_ttft_ewma_ms` | gauge | service,route,model | TTFT EWMA (ms) |
| `openresty_tps_limiter_active` / `openresty_ttft_limiter_active` | gauge | service,route | Whether that limiter is in effect (1/0) |
| `openresty_rejected_total` | **counter** | service,route,reason | 429s from rate limiting (reason=concurrency/ttft/tps) |
| `openresty_poll_up` | gauge | service,route | Whether all four endpoints of this route answered in the last poll (1/0); one route failing does not affect the others |
| `openresty_poll_errors_total` | counter | — | Poll errors |
| `openresty_poll_last_success_seconds` | gauge | service,route | Unix time of this route's last fully successful poll |

Dynamic route discovery shares **one discoverer** with enrichment and reports
through `bodylog_route_resolver_*` above (there is no separate
`openresty_discovered_routes` / `openresty_route_discovery_*` any more).

**Only one target is polled**: the k8s openresty Service (under HA it resolves to
the active leader, so there is naturally one logical target), with no instance
label.

### The labels

- **`service`**: the `spec.discovery.service` of the backend's ModelRoute
  (`ns/name`, e.g. `model-service/fallback-model-service-01`). **The dimension
  to aggregate by** -- pod IPs drift and `model` can come back empty, but the
  service is stable. Enrichment that does not hit (bare metal, no service
  account, a pod too new to be in an EndpointSlice) gives `unknown`.
- **`route`**: the `spec.nginx.route` of the backend's ModelRoute (e.g.
  `fallback-model-service-0.1`), falling back to `metadata.name` when omitted
  (the same rule autoconfig uses to name `session_route_<route>.conf`; only a
  monitor-only ModelRoute with no nginx section at all is skipped). One route to
  one service. No hit gives `unknown`.
- **`backend`**: the normalised **real backend** `host:port` (the peer key
  bodylog aggregates on). `(none)` when absent, as for a 4xx that was never
  routed; a retry shows up as the variant `<peer> (retry#1)`.
- **`model`**: the served-model-name from the response (`resp_meta.model`,
  **bounded**); `unknown` when absent. **Taken only from the detail line, never
  filled in from the ModelRoute** -- for a stable service dimension, use
  `service`.
- **`status_class`**: `2xx` / `4xx` / `5xx` / `other` (429 counts as `4xx`; for
  429 broken down by reason see `openresty_rejected_total`).
- **`stream`**: `true` / `false` / `unknown` (when the request did not say).
- **`finish_reason`**: the stop reason string (`stop`, `length`, `tool_calls`
  and so on); empty is not counted.
- **`prompt_bucket`**: the input-length band, on `bodylog_ttft_seconds` and
  `bodylog_output_tok_per_second` only. Values are in the table above;
  `prompt_tokens<=0` gives `unknown`.

### Enrichment: podIP -> service/route (one discoverer)

Both `bodylog_*` (details) and `openresty_*` (poll) need to attribute "a
backend" to a stable service identity. **One discoverer** (in-cluster REST with
the service account, no client-go) produces three things from a single
ModelRoute list:

1. **`map[podIP] -> {route, service}`**: for each ModelRoute, its
   `discovery.service` -> that Service's EndpointSlices -> the set of pod IPs ->
   inverted. `observe()` takes the IP out of `backend` (stripping `:port` and
   `(retry#N)`), looks it up, and attaches `service`/`route`. **Not-ready pods
   are included**: one may just have served a request and still has to be
   attributable.
2. **Replica counts per service** (`bodylog_service_replicas` and `_ready`).
3. **The route list and route -> service**, for openresty-poll (the route list
   is filtered by `OPENRESTY_SERVICE`; the service map holds every route).

It refreshes periodically (`ROUTE_DISCOVERY_INTERVAL_SECONDS`, default 30s), so
pods and routes coming and going are followed automatically; when the apiserver
flaps and a round fails, **the previous result is kept, not cleared**. It is
**enabled in-cluster only** (it needs `KUBERNETES_SERVICE_HOST` and a service
account); a bare-metal exporter has no such ability, so `service` and `route`
stay `unknown` while every other metric carries on. **RBAC needs `list` on
`modelroutes` and `endpointslices`** (the helm `rbac.yaml` already creates it).
The per-role series additionally need `list` on `leaderworkersets`; without that
permission, or on a cluster without the CRD, **only the role dimension goes
missing** and everything else carries on (one log line, then no further requests
for 30 minutes). To turn it off outright, set `PD_ROLE_GROUP=""` and no request
is made at all. Also note that when `_desired` is split by role, it is
**emitted only once every slice of that role has reported its `spec.replicas`**:
summing a subset would give a smaller number that still looks like an answer.

> **⚠️ The `service` label collides with a target label**: kube-prometheus-stack
> injects a target label `service` (the name of the k8s Service being scraped)
> when it scrapes. **`honorLabels: true` on the ServiceMonitor is required** (the
> bodylog chart and the manifest in this README both set it) so that the
> exporter's own `service` -- the model service -- wins, and `by(service)`
> aggregates by model rather than by scrape target.

### ⚠️ Field reliability (0 or empty is treated as "not measured", skipped rather than polluting quantiles or creating pointless series)

- `ttft` (frt): **recorded only when `stream=true`** (the `stream` field in the
  detail line is pulled out of the request body by the listener with a regex).
  ⚠️ An early version filtered on `frt > 0`, assuming "non-streaming means
  frt=0". **In reality a non-streaming response has `first_chunk_t ≈ rt`** --
  there is one body chunk, and when it arrives the response is over -- so frt is
  always > 0 and nothing was filtered at all. The TTFT histogram filled up with
  non-streaming samples, and its p99 (82s) was measured higher than the RT p99
  (77s). Since 0.1.10 the filter is on `stream`.
  The cost: when `req_body` was not captured or is base64, `stream` cannot be
  extracted (null) and that request's TTFT is missed even if it was streaming --
  **fewer samples, not wrong ones**. Measured over 800 detail lines, none of the
  `stream=null` ones looked like a stream.
- **Token fields**: 0 when the response has no usage, so **a streaming request
  must send `stream_options.include_usage=true`**, or `completion_tokens` is 0
  and `output_tok_per_second` has no data.
- `model` and `finish_reason`: may be empty when the response is not JSON, as
  with an error.
- `reasoning_tokens`: always 0 for a non-reasoning model.

---

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `BODYLOG_DETAILS_DIR` | `/data/bodylog/metrics/details` | bodylog's `metrics/details` directory (must be readable) |
| `EXPORTER_LISTEN` | `:9110` | Listen address (`0.0.0.0:9110` to be scraped in k8s) |
| `CHECKPOINT_PATH` | `/var/lib/bodylog-exporter/checkpoint.json` | Where the offset is persisted |
| `BODYLOG_URL` | empty | bodylog's HTTP base (e.g. `http://127.0.0.1:9998`), **used only to back-fill a day-rotation gap**; empty means no back-fill |
| `BODYLOG_HTTP_TOKEN` | empty | Auth for bodylog's `/metrics` (for the back-fill) |
| `RECOVERY_LAG_SECONDS` | `10` | Back-fill up to `now - lag`, to let the flush settle |
| `FOLLOW_INTERVAL_MS` | `300` | Poll interval once the tail reaches EOF |
| `DATE_TZ` | `Asia/Shanghai` | Timezone the daily file names use; **must match bodylog's**. Without tzdata it falls back to a fixed `+08:00`, which is exactly right where there is no DST |

### openresty-poll configuration (optional; an empty URL means tail only, no polling)

| Variable | Default | Meaning |
|---|---|---|
| `OPENRESTY_POLL_URL` | empty | openresty's base (in-cluster, e.g. `http://openresty:8080`); **empty disables polling**. Off by default because a bare-metal exporter cannot reach k8s |
| `OPENRESTY_POLL_ROUTES` | empty | **The static escape hatch**: a comma-separated route list (e.g. `qwen,opt`). **Non-empty bypasses dynamic discovery**; leave it empty for discovery |
| `OPENRESTY_POLL_INTERVAL_MS` | `15000` | Poll period |
| `OPENRESTY_POLL_TIMEOUT_MS` | `3000` | Per-request timeout |

**The route set is discovered dynamically by default** (when
`OPENRESTY_POLL_ROUTES` is unset): through **the same discoverer** enrichment
uses (in-cluster, with the service account, listing **ModelRoute CRs** in
`routing.modelsphere.dev/v1alpha1`), taking `spec.nginx.route` (or
`metadata.name` when omitted, as autoconfig does) for the route list and
`spec.discovery.service` for route -> service, refreshed periodically so
additions and removals are followed -- **nothing to configure statically, and no
restart when a route changes**. The variables below control **both** enrichment
and poll discovery, since one list serves both:

| Variable | Default | Meaning |
|---|---|---|
| `MODELROUTE_GROUP` / `_VERSION` / `_PLURAL` | `routing.modelsphere.dev` / `v1alpha1` / `modelroutes` | CR coordinates. `MODELROUTE_GROUP` may list several groups, comma-separated (e.g. while ModelRoutes move from one group to another): every group is listed, an object in more than one (same namespace/name) is taken from the earliest, a group the cluster does not serve (404) is skipped, and any other error keeps the last snapshot. RBAC must grant every group listed |
| `ROUTE_DISCOVERY_INTERVAL_SECONDS` | `30` | How often ModelRoutes are listed (shared by enrichment and poll discovery) |
| `ROUTE_DISCOVERY_TIMEOUT_MS` | `4000` | Timeout for one list/EndpointSlice request |
| `PD_ROLE_GROUP` / `PD_ROLE_VERSION` | `leaderworkerset.x-k8s.io` / `v1` | Coordinates of the LeaderWorkerSet API the per-role engine counts come from. **`PD_ROLE_GROUP=""` turns the whole thing off**: not a single request is made (which is what the chart's `pdRoles.enabled=false` passes) |
| `OPENRESTY_SERVICE` | empty | Count only the routes whose `nginx.service` points at this openresty towards the **poll list** (for several openrestys); empty takes all. **The enrichment mapping and the service map are not filtered by this** and hold every route |
| `OPENRESTY_POLL_PER_SERVICE` | `true` | Poll each ModelRoute on the openresty its `spec.nginx.service` names (`ns/name`, or `name` in the ModelRoute's namespace) at `http://<name>.<ns>.svc:<OPENRESTY_POLL_PORT>`, and label the result with that ModelRoute's service. ModelRoutes that name no openresty use `OPENRESTY_POLL_URL`. `false` = the old single-openresty behaviour, where ModelRoutes sharing a route name on different openrestys (a canary next to production) all read the one openresty and got one route's label. |
| `OPENRESTY_POLL_PORT` | `8080` | Dispatch port of the openresty Services polled per ModelRoute |

Discovery reports on itself through `bodylog_route_resolver_up` (1/0),
`bodylog_route_resolver_routes` (route count), `bodylog_route_resolver_pods`
(pods covered) and `bodylog_route_resolver_errors_total`. When the apiserver
flaps and a round fails, **the last successful mapping and route list are kept,
not cleared**. RBAC needs `list` on `modelroutes` (discovery) and
`endpointslices` (enrichment, to find the backend pods).

> **Read-only**: everything polled is a GET status endpoint of openresty
> (`_route_state`, `_tps_status`, `_ttft_status`, `_429_status`); nothing is
> changed and nothing is reloaded. In k8s it runs as a **sidecar** to bodylog
> (same pod, so the short name `http://openresty:8080` resolves, with a service
> account that may list ModelRoutes).

---

## Deployment

### Bare-metal systemd (the main case: the host that runs bodylog)

```bash
# Put the binary on the host; edit the Environment lines in bodylog-exporter.service
cp bodylog-exporter /usr/local/bin/
cp bodylog-exporter.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now bodylog-exporter
curl -s localhost:9110/metrics | head
```

### Scraping a bare-metal exporter from k8s Prometheus (the target goes through Endpoints)

bodylog on bare metal with Prometheus in k8s (kube-prometheus-stack): create a
**selector-less Service, hand-written Endpoints (the bare-metal `IP:9110`), and
a ServiceMonitor** labelled `release: kube-prometheus-stack`. There is a ready
manifest in [`deploy/prometheus-scrape.yaml`](deploy/prometheus-scrape.yaml)
(`kubectl apply -f`; for another host, change the name and the IP):

```yaml
apiVersion: v1
kind: Service
metadata: { name: bodylog-baremetal, namespace: monitoring, labels: { app: bodylog-exporter } }
spec: { clusterIP: None, ports: [ { name: metrics, port: 9110, targetPort: 9110 } ] }
---
apiVersion: v1
kind: Endpoints
metadata: { name: bodylog-baremetal, namespace: monitoring, labels: { app: bodylog-exporter } }
subsets: [ { addresses: [ { ip: 10.0.0.1 } ], ports: [ { name: metrics, port: 9110 } ] } ]
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata: { name: bodylog-baremetal, namespace: monitoring, labels: { release: kube-prometheus-stack } }
spec:
  namespaceSelector: { matchNames: [ monitoring ] }
  selector: { matchLabels: { app: bodylog-exporter } }
  endpoints: [ { port: metrics, path: /metrics, interval: 30s, honorLabels: true } ]
```

### k8s: its own chart (co-located with the listener, one instance emitting both metric groups)

The exporter is **its own chart**, no longer a sidecar in the bodylog chart: it
tails the details the listener writes to **node-local disk** (`bodylog_*`) and
polls the in-cluster openresty (`openresty_*`). **One instance emits both
halves.** Deploying it separately means upgrading the exporter does not restart
the listener, so frame intake and writing are never interrupted; the cost is
that it **must be pinned to the listener's node**, since the details are on that
node's local disk.

The charts are published at [modelsphere/helm-charts](https://github.com/modelsphere/helm-charts):

```bash
helm repo add modelsphere https://modelsphere.github.io/helm-charts
helm repo update

# the listener first
helm -n <ns> upgrade --install bodylog modelsphere/bodylog \
  --set-string secret.token=<token> \
  --set persistence.hostPath=/data/bodylog \
  --set nodeSelector."kubernetes\.io/hostname"=<node>

# then the exporter: same node, the same hostPath as the listener, reusing its Secret
helm -n <ns> upgrade --install bodylog-exporter modelsphere/bodylog-exporter \
  --set data.hostPath=/data/bodylog \
  --set nodeSelector."kubernetes\.io/hostname"=<node>
```

The values that matter (the chart's own `values.yaml` has the rest):

```yaml
data:
  dir: /data/bodylog             # mount point in the container; must equal the listener's dataDir
  hostPath: /data/bodylog        # the real directory on the node; must equal the listener's persistence.hostPath
openrestyPoll:
  url: "http://openresty:8080"   # the in-cluster openresty Service; empty = tail only, no polling
  routes: []                     # empty = dynamic discovery; set = static override
  intervalMs: 15000
routeDiscovery:
  enabled: true                  # list ModelRoute CRs (the chart creates the SA, ClusterRole and Binding)
  openrestyService: ""           # count only routes whose nginx.service points here; empty = all
serviceMonitor: { enabled: true, releaseLabel: kube-prometheus-stack }
```

⚠️ A non-empty `data.hostPath` with an empty `nodeSelector` makes the chart
`fail` outright -- otherwise the pod could be scheduled onto another node, read
an empty directory, and look exactly like "there is no data at all".

The Service gets a `metrics:9110` port automatically, along with a ServiceMonitor
(scraped directly in-cluster, with `honorLabels: true` already set to resolve the
`service` name collision).

**Three ways to give it a data volume** (`values.persistence`):
- `persistence.enabled: true` (the default) creates a **PVC**
  (`storageClassName`, `size`);
- **`persistence.hostPath: <path>`** uses a **directory on the node** (to reuse a
  machine's existing bodylog data disk, with no PVC). **A `nodeSelector` is
  mandatory** to pin the pod to that node, or `DirectoryOrCreate` will create an
  empty directory on a different one, which looks like losing the history. The
  chart enforces this: a non-empty hostPath with an empty nodeSelector is a
  hard `fail`;
- `persistence.enabled: false` uses an emptyDir (testing only; a restart loses
  the data).

```bash
# reuse a disk on the node, e.g. when bodylog moves from bare metal into k8s and keeps its data directory
helm -n <ns> upgrade --install bodylog modelsphere/bodylog --version <tag> \
  --set-string secret.token=<token> \
  --set persistence.hostPath=/data/bodylog \
  --set nodeSelector."kubernetes\.io/hostname"=<node>
```

### k8s: standalone poll-only (a cluster without bodylog)

To bring only openresty's control-plane state into Prometheus on a cluster that
has no bodylog: run a standalone Deployment, where the tail finds nothing and
only the poll does any work. There is a ready manifest in
[`deploy/exporter-standalone.yaml`](deploy/exporter-standalone.yaml) (SA,
ClusterRole, Binding, Deployment, Service and ServiceMonitor, with
`OPENRESTY_POLL_URL=http://openresty:8080` and dynamic discovery):

```bash
kubectl apply -f deploy/exporter-standalone.yaml   # change the namespace and the openresty Service name to reuse it
```

### Enabling openresty-poll on bare metal (optional)

A bare-metal systemd deployment only tails by default. To have it also poll the
openresty on the **same host**, add `OPENRESTY_POLL_URL` (e.g.
`http://127.0.0.1:18080`) and `OPENRESTY_POLL_ROUTES` to the Environment of
`bodylog-exporter.service` -- bare metal is not k8s and has no ModelRoute API, so
the route list has to be static.

CI: the `image` job in `.github/workflows/release.yml` (`exporter` in its
matrix). **To release only the exporter, use a prefixed tag**:

```bash
git tag exporter/v<X.Y.Z> && git push origin exporter/v<X.Y.Z>
# -> image 4pdosc/bodylog-exporter:<X.Y.Z>
#    (the prefix is stripped; openresty and bodylog stay out of the matrix, and their :latest is left alone)
```

Likewise `openresty/v*` and `bodylog/v*`. **An unprefixed tag (such as `0.1.13`)
still releases all three components.** A prefix that is not recognised (a typo
like `exporters/v1`) fails the workflow outright -- better that nothing happens
at all. A pre-release tag (one containing `-`, such as `0.2.0-rc1`) produces an
image as usual but **does not move `:latest`**.

The charts are not published from this repository: all three live in
[modelsphere/helm-charts](https://github.com/modelsphere/helm-charts) and are
released to GitHub Pages by that repository's chart-releaser.

---

## Everyday PromQL

**Aggregating by `service` is the recommended usage** -- pod IPs drifting and an
empty `model` do not affect it:

```promql
# QPS and output throughput (tok/s) per service
sum by(service)(rate(bodylog_requests_total{backend!="(none)"}[1m]))
sum by(service)(rate(bodylog_completion_tokens_total[1m]))

# TTFT p95 and RT p95 per service (seconds) -- native histograms: the bare name, no _bucket or le
histogram_quantile(0.95, sum by(service)(rate(bodylog_ttft_seconds[5m])))
histogram_quantile(0.95, sum by(service)(rate(bodylog_rt_seconds[5m])))

# Error rate per service
sum by(service)(rate(bodylog_requests_total{status_class=~"4xx|5xx",backend!="(none)"}[5m]))
  / clamp_min(sum by(service)(rate(bodylog_requests_total{backend!="(none)"}[5m])),0.001)

# Replicas per service, and throughput per ready replica
bodylog_service_replicas_ready
sum by(service)(rate(bodylog_completion_tokens_total[1m])) / bodylog_service_replicas_ready

# 429 rate per service, broken down by reason -- the openresty control-plane counter
sum by(service)(rate(openresty_rejected_total[1m]))
sum by(service,reason)(rate(openresty_rejected_total[1m]))

# p50 of the per-request generation rate (the distribution of completion/rt over requests)
histogram_quantile(0.5, sum by(service)(rate(bodylog_output_tok_per_second[5m])))

# Mean of the Watchmen overall/decode TPS (aggregated by service; the new metrics have no prompt_bucket)
histogram_avg(sum by(service)(rate(bodylog_overall_output_tok_per_second[1m])))
histogram_avg(sum by(service)(rate(bodylog_decode_output_tok_per_second[1m])))

# Finer: QPS per backend (pod) within one service -- aggregating by service is steadier while pods scale
sum by(backend)(rate(bodylog_requests_total{service="model-service/fallback-model-service-01"}[1m]))

# Is the exporter lagging (the gap between last_ts and now)
time() - bodylog_exporter_last_ts_seconds
```

> The histograms are **native-only** (since 2026-08-31, exporter 0.2.0): classic
> buckets are no longer emitted in parallel, and `_bucket`/`_sum`/`_count` no
> longer exist. Take quantiles from the bare name:
> `histogram_quantile(0.95, sum by(service)(rate(bodylog_ttft_seconds[5m])))`,
> and a mean with `histogram_sum(...)/histogram_count(...)`. ⚠️ This assumes
> Prometheus supports native histograms -- with that disabled, these three
> histograms have no buckets at all.

### Autoscaling (an LLMScaler `metrics` fragment)

```yaml
metrics:
  - name: qps-per-pod        # load divides across replicas, so it suits ratio scaling
    query: 'avg(sum by(backend)(rate(bodylog_requests_total{model="qwen",backend!="(none)"}[1m])))'
    target: "<the per-pod QPS budget>"
  - name: ttft-p95           # a saturation guard rail, falling back to 0 on NaN
    query: 'histogram_quantile(0.95, sum(rate(bodylog_ttft_seconds{model="qwen"}[2m]))) or vector(0)'
    target: "2"
```

**Scaling on openresty's control-plane state (more immediate than bodylog)**:
`active_conns` and `adaptive_cc` are live pressure and react faster than an
after-the-fact counter:

```promql
# Mean per-pod concurrency utilisation (current concurrency / the static ceiling) -- the closer to 1, the more it needs to scale out
avg(openresty_peer_active_conns{route="qwen"}) / avg(openresty_peer_max_concurrency{route="qwen"})

# Adaptive concurrency has reached the pool's capacity (AIMD maxed out -> more pods needed)
avg(openresty_adaptive_cc{route="qwen"}) / avg(openresty_adaptive_cc_max{route="qwen"})

# A peer is banned (health is degrading; do not scale in blindly)
sum(openresty_peer_banned{route="qwen"})

# Concurrency 429s are happening (the direct signal of running out of capacity)
sum(rate(openresty_rejected_total{route="qwen",reason="concurrency"}[1m]))
```

---

## What this does not cover (important)

This exporter covers **the traffic side only**. bodylog cannot see any of the
following, which need **other Prometheus sources** -- usually already in the
cluster:

| What is needed | Where it comes from |
|---|---|
| KV cache usage / running / queued / cache hit rate | The engine's `/metrics` (`sglang:token_usage`, `num_running_reqs`, `num_queue_reqs`, `cache_hit_rate`, `vllm:*`; needs `--enable-metrics`) |
| GPU temperature / memory / utilisation | `nvidia-dcgm-exporter` (`DCGM_FI_DEV_*`) |
| Whether a service is up or down | Prometheus `up{}` (bodylog only sees requests that happened, and **idle is not down**) |

A complete monitoring board is the four sources together: **this exporter, the
engine's /metrics, dcgm, and `up{}`**.

---

## Crash and data-loss semantics

| What fails | Result |
|---|---|
| The exporter is down briefly (the day's jsonl is still there) | **Nothing is lost**: it resumes from the offset on restart and catches up; the counters restarting from zero is handled by `rate()` |
| The exporter is down across the day's `jsonl -> parquet` rotation | The gap is back-filled over `/metrics` (which reads both parquet and jsonl), so **nothing is lost** (needs `BODYLOG_URL`) |
| bodylog itself is down | The details already written are safe; when it comes back it appends again and the exporter keeps tailing |
