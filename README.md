# llm-openresty

A session-affinity router for LLM inference backends, built on OpenResty.

Requests carrying a session id are pinned to the same backend so the engine's prefix
cache stays warm; requests without one are spread by least-connections. On top of
that it carries the things a shared inference gateway ends up needing: active health
checking, adaptive concurrency, TTFT/TPS-based shedding, content-based rejection, and
full request/response body logging.

## Why session affinity

vLLM and SGLang keep a prefix cache per instance. A multi-turn conversation whose
turns land on different backends recomputes the shared prefix every time. Pinning a
conversation to one backend turns that into a cache hit, which is the difference
between reprocessing tens of thousands of prompt tokens and not.

The router therefore treats "same session -> same backend" as the default, and falls
back to load balancing only when there is no session id, or when the pinned backend
is unhealthy.

## Request flow

```
                     ┌─────────────────────────────────────────┐
client ── :8080 ───► │ dispatch: /<route>/... -> unix socket   │
                     └──────────────────┬──────────────────────┘
                                        │
                     ┌──────────────────▼──────────────────────┐
                     │ per-route server (one per model/route)  │
                     │                                         │
                     │ access_by_lua:                          │
                     │   api key check                         │
                     │   extract session id (6 sources)        │
                     │   reject rules (opt-in)                 │
                     │   assess pool -> 503 / 429 / admit      │
                     │   pick peer (hash | least_conn)         │
                     │ balancer_by_lua: set_current_peer       │
                     │ body_filter: TTFT/TPS sampling, bodylog │
                     │ log_by_lua: release conn counter        │
                     └──────────────────┬──────────────────────┘
                                        ▼
                                 inference backends
```

One external port. Adding a model means dropping a new `session_route_<name>.conf`
into `conf.d/routes/` — no port allocation, no change to the dispatcher.

`:8090` serves `/healthz` for probes.

## Layout

```
nginx.conf              main config; declares `env` passed through to Lua
session_base.conf       shared dicts, upstream, init block, dispatcher, admin server
router_locations.inc    /v1/ plus the debug and hot-toggle endpoints, shared by
                        every per-route server block
lua/                    the engine (12 modules)
  router.lua              module loader
  route.lua               route registration, pool assessment, peer selection
  access.lua              the access-phase entry point
  api_keys.lua            key loading and the Bearer guard
  reqtransform.lua        session id extraction, request rewriting
  timers.lua              health checks, cluster averages, adaptive concurrency
  ttft.lua / tps.lua      latency and decode-rate measurement and shedding
  reject_rules.lua        content-based rejection
  bodylog.lua             async request/response capture
  debug_endpoints.lua     everything under /_*
  util.lua                shared helpers
k8s/                    a standalone Deployment manifest (the Helm charts live in
                        modelsphere/helm-charts)
bodylog-listener-go/    receives body-log frames, writes JSONL
bodylog-exporter-go/    turns body-log details into Prometheus metrics
test/                   self-contained test harnesses (see Testing)
vendor/                 lua-resty-logger-socket (BSD, see vendor/resty/logger/LICENSE)
```

Per-route configs are deliberately **not** in this repo: they hold your backend
addresses. They are mounted at runtime.

## Defining a route

A route is a Lua factory registered by name. Minimal form:

```lua
set_by_lua_block $__init {
    _G.register_route("my-model", function() return {
        peers = {
            -- {host, port, name, priority, max_concurrency}
            {"10.0.0.11", 8000, "node-a", 0, 64},
            {"10.0.0.12", 8000, "node-b", 0, 64},
        },
    } end)
    return ""
}
```

Peers carry a priority: the router only sends traffic to the highest-priority tier
that still has healthy members, so a cache-aware router or a faster pool can sit in
front of a fallback tier and take over automatically when it recovers.

Useful optional fields:

| Field | Effect |
|---|---|
| `peers_by_model` | Sub-pools keyed by `body.model`; unknown model gets 400 with the supported list |
| `health_check_interval`, `health_ban_ttl` | Active probe cadence and ban duration |
| `health_probe_path`, per-peer `probe` | Probe path; per-peer override matters when a peer is itself a router whose `/v1/models` answers 200 while its workers are down |
| `adaptive_cc` | AIMD concurrency control driven by observed TTFT/TPS |
| `ttft_limit_ms`, `tps_limit_tps`, `*_metrics` | Shed load when latency or decode rate crosses a threshold |
| `reject_rules` | Reject by request content (see below) |
| `api_keys` | Per-route key table; defaults to the shared one |

### Request body limit

LLM routes default to a 10m request body (`client_max_body_size`, set at http level in
`session_base.conf`, with `client_body_buffer_size` 10m so such bodies never spill to disk). A route
raises or lowers it with its own `client_max_body_size <size>;` in its server block -- autoconfig
renders that from `max_body_size` in the ModelRoute values. The dispatch server's 200m is the ceiling
for any route. Behind CART, raise CART's `max_body_size` too: it defaults to the same 10m.

## Session id extraction

Checked in order; the first non-blank value wins:

1. `x-litellm-session-id`, `x-claude-code-session-id`, `x-session-id` headers
2. `metadata.session_id` / `litellm_metadata.session_id` in the body
3. `metadata.user_id.session_id` (object or JSON string)
4. `metadata.user_id` (plain string)
5. `user` (the OpenAI standard field)

Whitespace-only values are rejected rather than accepted, otherwise every such
request hashes to the same backend.

With a session id the peer is `crc32(sid) % #peers`; if that peer is banned the
request falls back to least-connections among the healthy set.

## Routing and protection

**Health checking.** One worker per route (elected through a shared-dict lock) probes
each peer on an interval, bans failures for a TTL, and unbans on recovery. A peer that
answers 429 or times out is treated as alive but busy — banning it would remove
capacity exactly when it is under load.

**Concurrency.** Each peer has a `max_concurrency`. In-flight counts live in a shared
dict, so the pool limit is enforced across workers. Exceeding it yields 429; no
healthy peer at all yields 503.

**Adaptive concurrency (opt-in).** With `adaptive_cc`, the effective limit moves by
AIMD against measured TTFT and decode rate instead of staying at a static number.
It moves within `[adaptive_cc_min, static max]`. Setting `adaptive_cc_init` (slots) or
`adaptive_cc_init_frac` (fraction of the static max) adds a point inside that band: a
fresh or expired limit starts there, and low traffic shrinks the limit only down to it.
Only overload pushes the limit below `init`, toward `min`; once the backend is healthy again
the limit climbs back to `init` by the normal growth factor per interval, even under light
traffic. Without it, `init` equals `min`.
`adaptive_cc_hold_when_idle` (on by default, `_G.ADAPTIVE_CC_HOLD_WHEN_IDLE`) stops low or
absent traffic from lowering the limit at all: no low-traffic shrink and the value never
expires, while growth under pressure and overload shrink work as usual. It holds whatever
value the limit has, including one an overload pushed below `init`: after such an overload
followed by an idle period, the limit stays low and climbs back to `init` only once traffic
returns. A route sets it to `false` to get the slack shrink and the limit TTL back.

Boolean options (`cross_tier_fallback`, `adaptive_cc_hold_when_idle`, `adaptive_cc_use_ttft`)
accept `true`/`false`, and also `yes`/`no`, `on`/`off`, `1`/`0` in any case with a warning;
anything else is logged and the option's default applies. (A plain truthiness test used to read
the string `"False"` as on.)

**Content-based rejection (opt-in, off by default).** Rules match the request itself —
`input_bytes`, `input_chars`, `messages_count`, `max_tokens`, `stream`, or any dotted
path in the body — with `gt/ge/lt/le/eq/ne`, `in/nin`, `match/contains/prefix`,
`exists/absent`, and `all`/`any` nesting. Invalid rules are logged and dropped at
startup rather than taking the route down.

**Session turn priority (opt-in, off by default).** The client gateway pins each
session to the provider that served it, so a 429 on turn N loses the whole session
and its KV cache, and the prefix-cache hit rate drops. The gateway marks requests
with `x-phanrouter-session: {"session_id":"...","turn":N}`. With `session_prio = true`
on a route, the three rejection gates grade by turn bucket (1 / 2-3 / 4-7 / ... up to
`session_prio_full_turn`, default 32):

- **Concurrency**: the ceiling scales by the bucket's `share` (`session_prio_r_new`,
  default 0.85, rising to 1.0 at full warmth), so under load turn 1 is turned away
  first while a warm session still fits.
- **TTFT**: an EWMA at or above `threshold x session_prio_severe` (default 1.5) is a
  hard trip and rejects every bucket; between `threshold x tol(bucket)` and that, a
  soft zone rejects lower buckets while warmer ones keep a probe slot. Turn 1 never
  takes from the warm probe budget; instead it gets its own tiny budget of
  `session_prio_t1_probes` per-probe-window slots (default 1, 0 restores the
  earlier never-probe behaviour), and only while no warm request has probed that
  window yet — a pool of almost-only-new sessions keeps trickling samples
  through, so recovery does not wait for the whole EWMA to expire. That budget
  applies only while the gate is actually tripped. A hysteresis flag holds turn 1
  out, with no probe slot, until the EWMA falls below
  `threshold x session_prio_release` (default 0.9) or expires.
- **TPS** (only with adaptive concurrency off): the same grading, inverted — a hard
  trip below `threshold / severe`, a soft zone below `threshold / tol(bucket)`.

Requests without a usable header (missing, malformed, `turn` not an integer from 1 to 10^9, or
`session_id` blank or over 256 chars) are treated as fully warm — top-bucket policy —
but counted separately (`bnohdr:*`), with a rate-limited WARN when their share of a
minute exceeds `session_prio_warn_frac` (default 0.2). A route can also replace the
whole bucket table with `session_prio_table = { {min_turn, share, tol}, ... }` (first
`min_turn` 1, strictly ascending).

The adaptive-cc controller and the TTFT/TPS EWMA measurement are unchanged: the
SLO guard itself is not touched. One interaction to know about: on an
adaptive-cc route, a turn-1 request rejected by the reserve (in-flight between
`limit x share` and `limit`) still counts as unmet demand in the controller's
per-pool `rej` counter, so on a healthy route the controller keeps growing the
limit to make room inside the reserve — exactly what the reserve is for. With
the feature off every path is byte-identical — same 429 bodies, same sid
sources, no counter traffic. `POST /_session_prio_toggle?on=0|1` flips it at
runtime without a reload, and `GET /_session_status` shows the effective table,
per-bucket admits/rejections, and the cache hit rate (`ctok/ptok`).

Two known limitations. The prompt/cached token counters (`ptok`, `ctok`) are parsed
out of the TPS response tail, which only exists while TPS measurement is on — routes
without it get no token accounting (rejections and admits still count). The second
is that the concurrency gate admits while in-flight is under
`limit x rt_limit_factor x share`, so a bucket can fill the pool up to
`ceil(limit x rt_limit_factor x share)` requests; the table below assumes the
default `rt_limit_factor = 1`. With the default shares
0.85/0.88/0.91/0.94/0.97/1.0 that means:

| `limit` | ceilings per bucket (`ceil(limit x share)`, `rt_limit_factor = 1`) |
|---|---|
| 6 | 6, 6, 6, 6, 6, 6 |
| 7 | 6, 7, 7, 7, 7, 7 |
| 10 | 9, 9, 10, 10, 10, 10 |
| 20 | 17, 18, 19, 19, 20, 20 |

At `limit = 6` the grading disappears entirely — every bucket admits the same six
requests — and turn 1 is only first separated from the rest at `limit >= 7` (the
criterion is `limit x (1 - r_new) >= 1`). Even at modest sizes the buckets tie in
pairs. The fix for a small pool is to *lower* `session_prio_r_new`, or supply a
`session_prio_table` with wider share gaps, so the gaps survive the `ceil`. Note
also that under adaptive concurrency the limit itself is a float that moves over
time, so the effective number of distinct ceilings moves with it.

## Response post-processing

A route can rewrite upstream responses before they reach the client, to hide backend
quirks. It is off unless the route names a handler (`lua/postprocess.lua`):

```yaml
# ModelRoute
spec:
  nginx:
    values:
      postprocess: strip_actual_usage
```

(in a route conf: `postprocess = "strip_actual_usage"`; `_G.POSTPROCESS_DEFAULT` in
`session_base.conf` sets one for every route that names none.)

| handler | what it does |
|---|---|
| `strip_actual_usage` | removes the `actual_usage` some sglang builds return next to `usage` (a second token count, e.g. with different `cached_tokens`), at both placements those builds use: the top level, and inside each entry of `choices`. Measured on a recent sglang build (2026-11), a stream carries it inside `choices[0]` on the frame with `finish_reason` and at the top level on the trailing `"choices":[]` frame, so covering one placement alone leaves the field reaching clients. Nothing else is touched -- note `vllm_format`'s own `actual_usage` entry is a top-level drop, so a route that needs every frame shape covered names this handler. |
| `vllm_format` | normalizes vLLM's response shape: drops vLLM's placeholder fields when they are null (`prompt_token_ids`, `prompt_logprobs`, `logprobs`, `token_ids`, `refusal`, `annotations`, ...), always drops `stop_reason`, `service_tier`, `system_fingerprint`, `kv_transfer_params`, `ec_transfer_params` and the **top-level** `actual_usage` (only that placement -- on a backend that also puts it inside `choices` this handler leaves that copy in place, so pick `strip_actual_usage` if the field has to be gone from every frame shape), and renames `reasoning` to `reasoning_content` and `created_cache_tokens` to `cache_write_tokens`. |

How it behaves:

- One handler per route.
- Only 2xx responses, and only `text/event-stream` (each `data:` frame) and
  `application/json` bodies; anything else passes through.
- A frame or body with nothing to change is sent byte-identical. A changed one is
  re-encoded, so its key order and `/` escaping can differ from the backend's.
- `Content-Length` is dropped on the route's 2xx responses (they are sent chunked),
  and a non-stream JSON body is held until it is complete (up to 8 MiB; a larger
  one passes through unchanged). A stream is not held: each complete line goes out
  with the chunk it arrived in, and only a line a chunk ends in the middle of waits
  for the next chunk.
- Fail-open: a frame that does not parse, or a handler error, leaves the original bytes.
- The bodylog sees the raw upstream response (it runs first). The listener keeps
  `usage` and `actual_usage` in the record's `resp_meta`, so a stripped
  `actual_usage` is still in the bodylog.

## Authentication

Keys are read from a file, default `/etc/openresty/api-keys/keys`, overridable with
`OPENRESTY_API_KEYS_FILE`. Format: `key1:owner1,key2:owner2`.

Supporting several keys at once is what makes rotation possible — issue the new key
alongside the old one, move callers over, then retire the old one. With a single key
every rotation is an outage, so nobody rotates.

A file rather than an environment variable, because **nginx reads `env` declarations
only when the master starts**. `openresty -s reload` does not re-read them, but it
does re-execute `init_by_lua`, which re-reads this file. So a key change costs one
graceful reload instead of restarting the master and dropping every in-flight stream —
which for LLM traffic can mean responses that have been streaming for minutes.

**With no readable key file, requests are allowed through.** This is a deliberate
fail-open: a misconfigured secret that 401s an entire gateway is worse than a short
window without authentication. It is not silent — an error is logged at startup, and
`/_health_status` reports it:

```json
{"_meta": {"api_keys_configured": false,
           "route_auth_enabled": false,
           "key_file_status": "missing"}}
```

`key_file_status` separates `missing` (no file — presumably deliberate) from
`unreadable` (file present but unreadable — a broken mount or wrong permissions).
Both fail open, but they mean opposite things and an operator needs to tell them
apart. Alert on `api_keys_configured: false`.

## Observability

Every `/v1/*` response carries `X-Routed-Session`, `X-Routed-Source`, `X-Routed-Mode`
(`hash` / `least_conn` / `hash_fallback`), `X-Routed-Peer`, and `X-Routed-Retries`.

`/_route_opts` (read-only, open like `/_tps_status`) lists the route's tunable options -- key, label, group, kind,
default, description, in the same shape as the console's deploy form -- with the value the
engine actually runs with and whether the route set it, plus the derived adaptive-concurrency
bounds (static max, floor, start point) per sub-pool. The catalog lives in `lua/directives.lua`:
`directives` holds the 18 options an operator is expected to tune; engine internals (AIMD
coefficients, measurement windows, probe allowances ...) are marked hidden and served under
`advanced` instead -- still settable, just not offered by the form. `test/test_route_opts.sh` fails
if a literal default in either list drifts from the engine.

Read-only endpoints (no key required): `/_health_status`, `/_active_conns`,
`/_cluster_avg`, `/_route_state`, `/_route_debug?sid=…`, `/_route_inspect`,
`/_ttft_status`, `/_tps_status`, `/_429_status`, `/_bodylog_status`,
`/_reject_rules_status`, `/_route_opts`, `/_rate_limit_status`.

Hot toggles (bound to 127.0.0.1) change behaviour without a reload:
`/_ttft_toggle`, `/_ttft_limit`, `/_tps_toggle`, `/_tps_limit`, `/_bodylog_toggle`,
`/_reject_rules_toggle`, `/_cch_strip_toggle`, `/_kimi_normalize_toggle`,
`/_include_usage_toggle`, `/_rate_limit_toggle`.

**Rate-limit master switch.** One switch turns off all three load-based 429s: concurrency (the
static pool limit or the adaptive one, including `session_prio`'s per-turn shares), TTFT
(including `session_prio`'s graded gate), and decode rate. With it off those requests are
let through, while everything behind them keeps running: the EWMAs, the adaptive limit, health
bans and the 503 for an all-banned pool. Each gate a request trips while the switch is off is
counted, per gate, in `/_rate_limit_status` and in the `rate_limit` field of `/_429_status`.
These are trips rather than exact refusals: a probe slot or a warm turn in `session_prio`'s soft
band might have been admitted anyway, and a request tripping two gates counts on both. They are
never added to the real 429 counters. Content rules (`reject_rules`) have their own
switch and are not covered. Three layers, highest first:
- the runtime override `POST /<route>/_rate_limit_toggle?on=0|1` (127.0.0.1 only) takes effect without a reload
  and survives one; `?reset=1` removes it;
- the route option `load_rate_limit` (route conf, or `nginx.values` on a ModelRoute). Named so,
  not `rate_limit`: on a ModelRoute that key is a video route's download speed limit, and the CRD
  rejects it on llm routes;
- the global `_G.LOAD_RATE_LIMIT_ENABLED` in `session_base.conf`.

Things to watch when switching it off at runtime. The override lives in this openresty
instance's shared memory: with several replicas (e.g. two openresty pods) send it to every one of
them, and a restarted instance comes back without it. It survives reloads and lasts until
`?reset=1` or a full restart, so remove it once the incident is over (`/_rate_limit_status` shows
`source: runtime` while it is in place). To keep limiting off for longer than an incident, set the
route option (`load_rate_limit: false` in the ModelRoute's `nginx.values`) instead: it is part of the
deployed configuration and applies to every instance. And the adaptive limit keeps moving on traffic it no
longer caps -- down under overload, up under pressure -- so switching back on can bring a burst of
429s until it settles. Likewise `session_prio`'s turn-1 hold flag is neither set nor released while
the switch is off: a flag left set from before is re-evaluated by the first request after it comes
back on (and it expires with the TTFT TTL in any case).

**Body logging** captures full request and response bodies asynchronously over a
cosocket (a length-prefixed binary frame; the request path never blocks on it) and
ships them to `bodylog-listener-go`, which writes hourly JSONL. `bodylog-exporter-go`
turns those into Prometheus metrics. Set `BODYLOG_LISTENER_HOST` to enable it; leave
it unset and nothing is captured or sent.

## Deploying

### Docker

```bash
docker build -t llm-openresty:dev .
docker run --rm -p 8080:8080 -p 8090:8090 \
  -v "$PWD/routes:/usr/local/openresty/nginx/conf/conf.d/routes:ro" \
  -v "$PWD/keys:/etc/openresty/api-keys:ro" \
  llm-openresty:dev
```

### Kubernetes

The charts are published from
[modelsphere/helm-charts](https://github.com/modelsphere/helm-charts):

```bash
helm repo add modelsphere https://modelsphere.github.io/helm-charts
helm repo update

helm -n llm upgrade --install openresty modelsphere/openresty --create-namespace \
  --set existingSecret=openresty-api-keys
```

There are three: `openresty` (this router), `bodylog` (the listener that records
what it served) and `bodylog-exporter` (Prometheus metrics from those records).

The chart runs two replicas in master-standby: routing state (connection counts,
ban lists) is per-pod in shared memory, so only the leader takes traffic. A sidecar
watches `conf.d/routes/` and the API-key mount and reloads on change, which is what
makes both route updates and key rotation free of restarts.

Mount the key secret as a **whole volume, never with `subPath`** — `subPath` copies
the file once and stops following the `..data` symlink swap, so rotation silently
stops working.

`k8s/deployment.yaml` is a single-pod standalone manifest for trying things out;
the chart is the supported path.

Graceful shutdown needs three things together: `STOPSIGNAL SIGQUIT` in the image (in
place), a `terminationGracePeriodSeconds` matching your longest stream, and a preStop
sleep long enough for endpoint removal to propagate before the listener closes.

## Testing

The harnesses under `test/` are self-contained: each builds its own OpenResty prefix
on a high port with its own mock backends, asserts, and cleans up. They do not touch
anything already running.

```bash
cd test
./test_permodel.sh          # per-model sub-pools
./test_ttft.sh              # TTFT measurement and shedding
./test_tps.sh               # decode-rate measurement and shedding
./test_adaptive_cc.sh       # AIMD concurrency control
./test_api_keys_reload.sh   # key rotation through a reload
./test_empty_peers_degrade.sh   # graceful degradation with no peers
./test_lazy_init_timing.sh      # lazy route registration across workers
```

Pure-Lua unit tests (`test/utest_*.lua`) need only `resty`. Run them all from the
repository root, as CI does on every pull request that touches `lua/` or a unit test:

```bash
bash test/run_lua_unit_tests.sh [path/to/resty]
```

It exits non-zero if any test fails, or if it finds none. A test that needs a special
invocation (a shared dict, another working directory) is listed in the script.

A note on how these are written: several assertions exist specifically to fail when
the thing under test is removed. A check that only confirms "no error appeared" also
passes when the feature never ran, so the harnesses pair each such check with a
control that must produce the opposite result. `test_empty_peers_degrade.sh`, for
instance, asserts that a *control* route with a dead peer does log health failures
before concluding that the empty route's silence means anything.

## License

Apache 2.0. `vendor/resty/logger/socket.lua` is third-party (BSD) — see
`vendor/resty/logger/LICENSE`.
