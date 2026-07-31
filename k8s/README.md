# openresty 路由器上 k8s

把 `openresty/` 这套全模型 session-affinity 路由(K2.5/K2.6/GLM/b300/... 一套 conf)
容器化 + k8s 部署。配置 = 本仓库权威副本,烤进镜像。

## 构建 & 部署

底座 = `ubuntu:22.04` + 从 openresty.org apt 装 openresty(**对齐现有 harbor 镜像 `llm/llm-openresty`**;不是 docker.io/openresty,内网拉不到)。已在 chat 实测 build 通过、`openresty -t` 全模型配置校验成功(openresty 1.29.2.3(对齐生产),镜像 193MB)。

```bash
# 在 openresty/ 目录下(Dockerfile 用相对 COPY)
docker build -t registry.example.com/llm/llm-openresty:0.1.0 .
docker push registry.example.com/llm/llm-openresty:0.1.0

kubectl apply -f k8s/deployment.yaml                 # Namespace + Deployment(replicas:1) + Service
kubectl -n openresty rollout status deploy/openresty-router
```

## 为什么用 Deployment 而不是 StatefulSet

路由器是**无状态**的,StatefulSet 三个卖点都用不上:

| StatefulSet 提供 | 这里需要吗 |
|---|---|
| 稳定 pod 身份 / 稳定 DNS | ❌ 藏在 Service 后,客户端不寻址单 pod |
| 每 pod 独立持久卷(PVC) | ❌ 配置在镜像;`lua_shared_dict`(active_conns/ban 列表/限流计数)是每 pod 内存态、重启重建,不持久 |
| 有序启停 | ❌ 副本完全等价 |

路由是 **crc32 一致性哈希(按 sid)**:各副本 PEERS 一致 → 同 sid 必落同后端,换 pod 不破坏会话亲和。
→ **Deployment**,`replicas:1` 起步,抗压直接水平扩。

## 优雅停机(和主 README「优雅停机」一致)

nginx 信号是反的:**SIGTERM=快速停机(砍在途)**,**SIGQUIT=优雅排空**。而 k8s 停 pod 默认发 SIGTERM。
三件套解决:

1. **镜像 `STOPSIGNAL SIGQUIT`** → k8s 停 pod 时实际发 SIGQUIT,nginx 走优雅排空(停止 accept、等在途长流跑完再退)。
2. **`terminationGracePeriodSeconds`**(deployment.yaml 默认 600)→ 唯一硬切刀,到点 SIGKILL。按最长流式响应设(64K 输出可能上千秒;调大代价是滚更时老 pod Terminating 更久)。
3. **`preStop: sleep 5`** → pod 进 Terminating 后要几秒才从上游 Service 端点摘除,睡一下保证排空期间不再进新连接。

配置变更**优先 SIGHUP reload、别重建 pod**(见下),reload 不断长流。

## ⚠️ 上集群前必调两项

1. **PEERS 要重指向集群内地址**:`session_route*.conf` 里的 `_G.PEERS` 现在是生产裸机 IP(`10.0.0.1` 等)。
   集群内若路由不到这些 IP,得把 PEERS 改成 in-cluster **Service 地址**(每个模型/每组一个 ClusterIP Service,
   见主 README「多副本 LWS 路由」一节)。这是让路由真正生效的关键,镜像只是把当前 conf 烤进去。
2. **bodylog listener**(可选):openresty 把 body-log 异步发到 `BODYLOG_LISTENER_HOST:PORT`。
   deployment.yaml 里默认留空 → logger-socket 连不上会 buffer/drop、非阻塞,**不影响转发**。
   要收集就单独部一个 listener(Go 二进制,源码 `openresty/bodylog-listener-go/`)+ Service,把 env 指过去。

## 配置热更(不重建镜像)——可选

当前是「配置烤进镜像」:改配置 = 重建镜像 + 滚动更新(有 STOPSIGNAL+grace 兜底,滚更也优雅)。
若要不重建就热更:把 `conf.d/`(含 lua)改成 **ConfigMap 挂载** + 一个 **reloader sidecar**
(watch ConfigMap 变化 → `openresty -s reload`,SIGHUP 优雅不断连接)。代价是 ConfigMap 有 1MiB 上限、
10+ 个 lua 模块要拆多个 ConfigMap,取舍看是否需要频繁热更。

## 端口

18080 主(K2.5)| 18082 k2.6 | 18083 glm | 18084 glm-b300 | 18085 k2.6-b300 |
18086 model-service | 18087 fallback | 18089 canary | 18090 finch | 18091 vendor-gpu
(443 域名 `llm-gateway-1.example.com.conf` 是 gateway-host 专属、需 SSL,不进通用镜像)
