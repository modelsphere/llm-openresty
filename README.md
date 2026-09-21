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
k8s/                    Helm charts (openresty, bodylog, bodylog-exporter) and a
                        standalone Deployment manifest
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

**Content-based rejection (opt-in, off by default).** Rules match the request itself —
`input_bytes`, `input_chars`, `messages_count`, `max_tokens`, `stream`, or any dotted
path in the body — with `gt/ge/lt/le/eq/ne`, `in/nin`, `match/contains/prefix`,
`exists/absent`, and `all`/`any` nesting. Invalid rules are logged and dropped at
startup rather than taking the route down.

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

Read-only endpoints (no key required): `/_health_status`, `/_active_conns`,
`/_cluster_avg`, `/_route_state`, `/_route_debug?sid=…`, `/_route_inspect`,
`/_ttft_status`, `/_tps_status`, `/_429_status`, `/_bodylog_status`,
`/_reject_rules_status`.

Hot toggles (bound to 127.0.0.1) change behaviour without a reload:
`/_ttft_toggle`, `/_ttft_limit`, `/_tps_toggle`, `/_tps_limit`, `/_bodylog_toggle`,
`/_reject_rules_toggle`, `/_cch_strip_toggle`, `/_kimi_normalize_toggle`,
`/_include_usage_toggle`.

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

The build installs OpenResty from openresty.org. If your build host cannot reach it,
point `BASE` at an image that already carries OpenResty — the Dockerfile detects that
and skips the install:

```bash
docker build --build-arg BASE=<registry>/openresty-base:1.29.2.3 -t llm-openresty:dev .
```

### Kubernetes

```bash
helm -n llm upgrade --install openresty k8s/helm/openresty --create-namespace \
  --set image.repository=<your-registry>/llm-openresty \
  --set image.tag=<tag> \
  --set existingSecret=openresty-api-keys
```

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

Pure-Lua unit tests need only `resty`:

```bash
resty utest_surgical_normalize.lua ../lua/reqtransform.lua
```

A note on how these are written: several assertions exist specifically to fail when
the thing under test is removed. A check that only confirms "no error appeared" also
passes when the feature never ran, so the harnesses pair each such check with a
control that must produce the opposite result. `test_empty_peers_degrade.sh`, for
instance, asserts that a *control* route with a dead peer does log health failures
before concluding that the empty route's silence means anything.

## License

Apache 2.0. `vendor/resty/logger/socket.lua` is third-party (BSD) — see
`vendor/resty/logger/LICENSE`.
