# openresty 全模型路由 app 镜像(K2.5/K2.6/GLM/b300/... 一套 conf)
#
# FROM harbor 基础镜像(已含 openresty 1.29.2.3 + logger-socket + STOPSIGNAL,见 Dockerfile.base),
# 本文件【只 COPY 配置、不装任何东西】→ CI 的 public-buildx runner(够不到 docker.io/外网)也能 build。
# 升级 openresty:重建并改 openresty-base 的 tag(见 Dockerfile.base),再改下面 FROM。
#
# 本地/CI 构建:docker build -t registry.example.com/llm/llm-openresty:<tag> .

FROM registry.example.com/llm/openresty-base:1.29.2.3

ARG OR=/usr/local/openresty

# 1) 主配置
COPY nginx.conf ${OR}/nginx/conf/nginx.conf
# 2) 框架基座 session_base.conf(dicts + upstream + init 框架 + 全局默认)+ 共享 location 片段(.inc);
#    per-model 路由(session_route_<model>.conf)不 bake —— 由 autoconfig 生成、挂 ConfigMap 到 conf.d/routes/。
COPY session_base.conf router_locations.inc ${OR}/nginx/conf/conf.d/
RUN mkdir -p ${OR}/nginx/conf/conf.d/routes
# 路径路由 dispatch 的 unix socket 目录(session_base.conf 里 listen/proxy_pass 用绝对路径 <prefix>/sock);
# 非 tmpfs、随镜像层持久,容器起来就在,per-model server bind socket 前目录已存在。
RUN mkdir -p ${OR}/nginx/sock
# 3) 引擎 lua 模块
COPY lua/ ${OR}/nginx/conf/conf.d/lua/

RUN set -eux; \
    # bodylog listener 地址改成从容器 env 透传(仓库里硬编码 10.0.0.1),不设则 lua 回落默认
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_HOST)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf; \
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_PORT)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf
# 注:不在 build 期跑 openresty -t —— 本镜像只 bake 基座、conf.d/routes/ 为空,
# router_locations.inc 引用的 $routed_session_id 由 per-model server(运行时 autoconfig 填入 routes/)声明,
# 空 routes/ 下 -t 会假报 "unknown routed_session_id"(良性)。-t 在部署后 routes/ 有 conf 时才有意义。

# 端口:8080 路径路由 dispatch(单外部口,新)+ 8090 health/probe + 18080/各 per-model(过渡期共存);
# STOPSIGNAL/CMD 继承自基础镜像
EXPOSE 8080 8090 18080 18082 18083 18084 18085 18086 18087 18088 18089 18090 18091
