# openresty session-affinity 路由器镜像(全模型路由:K2.5/K2.6/GLM/b300/... 一套 conf)
#
# 底座对齐现有 harbor 镜像 llm/llm-openresty:0.1.1-canary —— 即
#   FROM ubuntu:22.04 + 从 openresty.org apt 装 openresty(不是 docker.io/openresty,内网拉不到)。
# 相比 canary 的改进:① 烤入【全模型】配置(canary 只有 K2.5+canary 两个 conf);
#   ② STOPSIGNAL SIGQUIT —— canary 没设,停 pod 会走 nginx 的 SIGTERM=快速停机砍在途长流;这里钉成优雅排空。
#
# 构建(在 openresty/ 目录下):
#   docker build -t registry.example.com/llm/llm-openresty:<tag> .
# 配置 = 本仓库权威副本,直接烤进镜像(要热更不重建见 k8s/README.md 的 ConfigMap+reloader 方案)。

FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# ★ 版本 pin 到生产在用的 openresty(gateway-host/gateway-host/chat 实测均 1.29.2.3),
#   避免 apt 装成 latest(1.31.x)与生产不一致。air-gap 环境改用 install_openresty.sh 的
#   DEB_DIR 离线 deb(chat 的 /tmp/openresty-debs/ 就是这版)。
ARG OPENRESTY_VER=1.29.2.3-1~jammy1

# openresty 运行/构建依赖 + 从 openresty.org 官方 apt 装【指定版本】openresty(jammy)
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates lsb-release curl iproute2 procps; \
    wget -qO - https://openresty.org/package/pubkey.gpg | gpg --dearmor -o /usr/share/keyrings/openresty.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/openresty.gpg] http://openresty.org/package/ubuntu jammy main" \
        > /etc/apt/sources.list.d/openresty.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends "openresty=${OPENRESTY_VER}"; \
    rm -rf /var/lib/apt/lists/*

# ★ 把停机信号变成 nginx 优雅退出(SIGQUIT);默认 SIGTERM = nginx 快速停机会砍在途长流
STOPSIGNAL SIGQUIT

ARG OR=/usr/local/openresty

# 1) 主配置
COPY nginx.conf ${OR}/nginx/conf/nginx.conf
# 2) 路由 conf(*.conf 自动 include)+ 共享 location 片段(.inc 被各 server 显式 include);不含 gateway-host 的 443 域名 conf
COPY session_route.conf session_route_*.conf _glm_vendor-gpu.conf router_locations.inc ${OR}/nginx/conf/conf.d/
# 3) 引擎 lua 模块(lua_package_path 指向 conf/conf.d/lua/)
COPY lua/ ${OR}/nginx/conf/conf.d/lua/
# 4) vendored lua-resty-logger-socket(bodylog 异步落盘用)
COPY vendor/resty/logger/socket.lua ${OR}/lualib/resty/logger/socket.lua

RUN set -eux; \
    # nginx.conf 里 include /etc/nginx/sites-enabled/*; 是裸 glob,建空目录让它无害
    mkdir -p /etc/nginx/sites-enabled; \
    # bodylog listener 地址:仓库硬编码 10.0.0.1,改成从容器 env 透传;不设则 lua 回落默认
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_HOST)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf; \
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_PORT)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf; \
    # 日志导到容器 stdout/stderr
    mkdir -p ${OR}/nginx/logs; \
    ln -sf /dev/stdout ${OR}/nginx/logs/access.log; \
    ln -sf /dev/stderr ${OR}/nginx/logs/error.log; \
    # 构建期语法自检(fail fast):校验 nginx.conf + 全部 conf.d + lua require 能加载
    ${OR}/bin/openresty -t

# 路由端口:18080 主(K2.5)+ 各 per-model
EXPOSE 18080 18082 18083 18084 18085 18086 18087 18089 18090 18091

# 前台运行,PID1 = openresty master → 收 STOPSIGNAL(SIGQUIT)优雅排空
CMD ["/usr/local/openresty/bin/openresty", "-g", "daemon off;"]
