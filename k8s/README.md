# openresty 路由器上 k8s

把 `openresty/` 这套全模型 session-affinity 路由(K2.5/K2.6/GLM/b300/... 一套引擎)容器化 + k8s 部署。
**路径路由**:外部只打一个口 **8080**,dispatch 按请求路径首段 `/<route>/` 派生到 per-model server 的
unix socket。**框架基座**(dicts + 8080 dispatch + 8090 admin + lua 引擎)烤进镜像;**per-model 路由
conf(`session_route_<route>.conf`)不烤镜像** —— 挂 ConfigMap 到 `conf.d/routes/`(手填,或由 autoconfig 动态写入)。

## 两种部署方式

| 方式 | 何时用 | 路由来源 | HA / 热更 |
|---|---|---|---|
| **Helm chart(`helm/openresty`,推荐)** | 生产 | autoconfig 从 ModelRoute 动态写 `conf.d/routes` | 主备 HA(hagate)+ reload sidecar 热更 + 优雅停机 |
| **`k8s/deployment.yaml`(standalone)** | 快速起个参考 pod | 自己填 `openresty-routes` ConfigMap | 单副本、无 sidecar(改配置 = 手动 reload / 重部) |

```bash
# 构建镜像(在 openresty/ 目录下;Dockerfile FROM harbor openresty-base,纯 COPY 配置)
docker build -t registry.example.com/llm/llm-openresty:<tag> .
docker push registry.example.com/llm/llm-openresty:<tag>

# A) Helm(生产:主备 + autoconfig 动态路由 + 热更)
helm -n openresty upgrade --install openresty helm/openresty --create-namespace \
  --set image.tag=<tag>

# B) standalone 参考(单副本、路由自己填 openresty-routes ConfigMap)
kubectl apply -f k8s/deployment.yaml
kubectl -n openresty rollout status deploy/openresty-router
```

## 为什么用 Deployment 而不是 StatefulSet

路由器是**无状态**的,StatefulSet 三个卖点都用不上:

| StatefulSet 提供 | 这里需要吗 |
|---|---|
| 稳定 pod 身份 / 稳定 DNS | ❌ 藏在 Service 后,客户端不寻址单 pod |
| 每 pod 独立持久卷(PVC) | ❌ 路由 conf 走 ConfigMap;`lua_shared_dict`(active_conns/ban 列表/限流计数)是每 pod 内存态、重启重建,不持久 |
| 有序启停 | ❌ 副本完全等价 |

路由是 **crc32 一致性哈希(按 sid)**:各副本 PEERS 一致 → 同 sid 必落同后端,换 pod 不破坏会话亲和。
→ **Deployment**。注意:openresty 的路由态是**每 pod 内存**,所以 Helm chart 用 **主备(master-standby)**
只让 leader 收流量(不能 active-active),而非简单水平扩多活。

## 优雅停机(三件套,`k8s/deployment.yaml` 与 Helm chart 一致)

nginx 信号是反的:**SIGTERM=快速停机(砍在途)**、**SIGQUIT=优雅排空**。k8s 停 pod 默认发 SIGTERM,三件套解决:

1. **镜像 `STOPSIGNAL SIGQUIT`** → k8s 停 pod 时实际发 SIGQUIT,nginx 优雅排空(停 accept、等在途长流跑完再退)。
2. **`terminationGracePeriodSeconds`**(默认 **600**)→ 唯一硬切刀,到点 SIGKILL。按最长流式响应设(proxy timeout
   3600s、64K 输出可能上千秒;调大代价是滚更时老 pod Terminating 更久)。Helm:`terminationGracePeriodSeconds` value。
3. **`preStop: sleep 5`** → pod 进 Terminating 后要几秒才从 Service 端点摘除完(传播到各节点 kube-proxy),先睡住
   让摘除传播完再关 listener → 排空期不再进新连接被拒。Helm:`preStop.sleepSeconds` value(设 0 = 不注入)。

滚动策略 `maxSurge:1 / maxUnavailable:0`(先起新 pod ready 再排空老 pod,零不可用);Helm:`updateStrategy` value。
配置变更**优先 SIGHUP reload(reload sidecar 干这个)、别重建 pod** —— reload 不断长流。

## per-model 路由从哪来(不烤镜像)

镜像只烤**框架基座**(`session_base.conf` 的 8080 dispatch + 8090 admin + dicts + `lua/`),**零具体路由**。
运行时 `conf.d/routes/` 由 ConfigMap 提供:

- **Helm + autoconfig(推荐)**:autoconfig 从 `ModelRoute` CRD 发现后端 → 写 `session_route_<route>.conf`
  进 `openresty-conf` ConfigMap → reload sidecar SIGHUP 生效。**peers = 后端 pod IP,由 autoconfig 事件驱动跟随**
  (扩缩/重启自动更新),无需手工维护 PEERS。
- **standalone**:自己往 `openresty-routes` ConfigMap 填 `session_route_<route>.conf`,改完 `openresty -s reload`。

## bodylog listener(可选)

openresty 把 body-log 异步发到 `BODYLOG_LISTENER_HOST:PORT`(镜像已把 nginx.conf 改成 env 透传)。
默认留空 → logger-socket 连不上会 buffer/drop、**非阻塞、不影响转发**。要收集就单独部一个 listener
(Go 二进制,源码 `openresty/bodylog-listener-go/`)+ Service,把 env 指过去。

## 端口

- **8080** = dispatch(唯一外部入口;客户端/llm-gateway 打 `8080/<route>/v1/…`,加模型不用改端口)。
- **8090** = admin/health(baked,`/healthz`;k8s 探针 + hagate 都盯它,不随动态路由变)。
- per-model 不再占 TCP 端口(只监听 `conf.d/routes` 里的 unix socket)。443 域名
  `llm-gateway-1.example.com.conf` 是 gateway-host 专属、需 SSL,不进通用镜像。
