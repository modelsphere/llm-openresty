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
# 2) 路由 conf(*.conf 自动 include)+ 共享 location 片段(.inc 被各 server 显式 include)
COPY session_route.conf session_route_*.conf _glm_vendor-gpu.conf router_locations.inc ${OR}/nginx/conf/conf.d/
# 3) 引擎 lua 模块
COPY lua/ ${OR}/nginx/conf/conf.d/lua/

RUN set -eux; \
    # bodylog listener 地址改成从容器 env 透传(仓库里硬编码 10.0.0.1),不设则 lua 回落默认
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_HOST)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf; \
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_PORT)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf; \
    # 构建期语法自检:校验 nginx.conf + 全部 conf.d + lua require 能加载
    ${OR}/bin/openresty -t

# 路由端口:18080 主(K2.5)+ 各 per-model;STOPSIGNAL/CMD 继承自基础镜像
EXPOSE 18080 18082 18083 18084 18085 18086 18087 18089 18090 18091
