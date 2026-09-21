# Running the router on Kubernetes

One external port. The dispatcher on **8080** routes on the first path segment
(`/<route>/`) to a per-route server behind a unix socket. The framework base
(shared dicts, the 8080 dispatcher, the 8090 admin server, the Lua engine) is
baked into the image; **per-route configs are not** — they are mounted from a
ConfigMap into `conf.d/routes/`, so adding a model never rebuilds anything.

## Two ways to deploy

| | When | Where routes come from | HA / reload |
|---|---|---|---|
| **Helm chart** (recommended) | Anything ongoing | A ConfigMap, edited by hand or maintained by the autoconfig operator | master/standby leader election, reload sidecar, graceful shutdown |
| **`deployment.yaml`** (standalone) | Trying it out | You fill in the `openresty-routes` ConfigMap yourself | single replica, no sidecars — a config change means a manual reload |

The charts are published from
[project-modelpilot/helm-charts](https://github.com/project-modelpilot/helm-charts),
not from this repository:

```bash
helm repo add modelpilot https://project-modelpilot.github.io/helm-charts
helm repo update
helm -n openresty upgrade --install openresty modelpilot/openresty --create-namespace
```

For the standalone manifest instead:

```bash
kubectl apply -f k8s/deployment.yaml
kubectl -n openresty rollout status deploy/openresty-router
```

Building the image yourself is a plain single-stage build — see the `Dockerfile`
at the repository root.

## Why a Deployment and not a StatefulSet

The router holds no durable state, so none of what a StatefulSet offers applies:

| StatefulSet gives you | Needed here |
|---|---|
| Stable pod identity and DNS | No — clients address the Service, never a pod |
| A persistent volume per pod | No — routes come from a ConfigMap, and the shared dicts (connection counts, ban lists, shedding counters) are per-pod memory rebuilt on start |
| Ordered startup and shutdown | No — replicas are interchangeable |

Routing is a crc32 consistent hash over the session id, so as long as every
replica sees the same peer list, the same session lands on the same backend and
replacing a pod does not break affinity.

One caveat that does shape the topology: because that routing state is per-pod
memory, the chart runs **master/standby** rather than active/active — only the
leader is in the Service. Two active replicas would each see half the traffic
and neither would have a complete picture of in-flight load.

## Graceful shutdown: three things together

nginx's signals are inverted from what Kubernetes assumes: **SIGTERM is the fast
shutdown that cuts in-flight requests**, SIGQUIT is the graceful one. Kubernetes
sends SIGTERM by default, so all three of these are needed:

1. **`STOPSIGNAL SIGQUIT` in the image** — stopping a pod then makes nginx stop
   accepting and let in-flight streams finish.
2. **`terminationGracePeriodSeconds`** (chart default 3600) — the one hard
   cutoff, after which the kernel kills it. Size it to your longest response; a
   64K-token completion can stream for many minutes. The only cost of a large
   value is that a terminating pod lingers.
3. **`preStop: sleep 5`** — removal from the Service endpoints propagates
   asynchronously, so sleep first and let it land before nginx closes its
   listener; otherwise new connections arrive at a pod that has stopped
   accepting.

Rolling updates use `maxSurge: 1 / maxUnavailable: 0`, so a new pod is ready
before an old one drains. Prefer a SIGHUP reload over replacing pods for config
changes — a reload does not interrupt streams.

## Where per-route configs come from

The image carries the framework only, with zero routes. At runtime
`conf.d/routes/` is a ConfigMap:

- **With the autoconfig operator**: it discovers backends from `ModelRoute`
  resources, writes `session_route_<route>.conf` into the ConfigMap, and the
  reload sidecar picks it up. Peers follow the backend pods, so scaling and
  restarts need no manual edits.
- **Standalone**: put `session_route_<route>.conf` into the `openresty-routes`
  ConfigMap yourself and reload.

## Body logging (optional)

The router ships request and response bodies asynchronously to
`BODYLOG_LISTENER_HOST:PORT`. Left unset, nothing is captured and nothing is
sent — there is deliberately no default address. To collect them, deploy the
`bodylog` chart and point the router at it with a fully qualified Service name;
a bare name will not resolve, and the frames are dropped silently.

## Ports

- **8080** — the dispatcher, and the only external entrypoint. Clients call
  `8080/<route>/v1/...`; adding a model does not add a port.
- **8090** — admin and health (`/healthz`). Fixed, so probes and leader election
  watch something that does not move when routes change.
- Per-route servers listen on unix sockets, not TCP ports.
