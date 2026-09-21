# OpenResty session router image.
#
# BASE defaults to a public image so a plain checkout builds anywhere:
#
#   docker build -t llm-openresty:dev .
#
# Where the build host cannot reach openresty.org, pass a base that already carries
# OpenResty and turn the install off:
#
#   docker build --build-arg BASE=<registry>/openresty-base:1.29.2.3 \
#                --build-arg INSTALL_OPENRESTY=0 -t llm-openresty:x .
#
# INSTALL_OPENRESTY is an explicit switch rather than a test for an existing
# installation, deliberately. Detecting it with `[ -x /usr/local/openresty/bin/
# openresty ]` looks obviously right and fails on our build runner: in that base
# the path is a symlink to ../nginx/sbin/nginx, and while the target is present
# and executable when the image runs on a cluster (same digest, verified), the
# test comes out false inside `docker run` on the runner. Build behaviour should
# not hinge on a filesystem probe that answers differently per environment.
#
# The image ships the engine and the framework config only. Per-route configs
# (session_route_<route>.conf) are NOT baked in -- they are mounted into
# conf.d/routes/ at runtime, so adding a model never requires rebuilding.

ARG BASE=ubuntu:22.04
FROM ${BASE}

# ARGs are scoped to the build stage: these must be re-declared after FROM to be
# visible in the instructions below.
ARG OPENRESTY_VER=1.29.2.3-1~jammy1
ARG OR=/usr/local/openresty
# 1 = install from openresty.org (matches the ubuntu default above)
# 0 = the base already carries it
ARG INSTALL_OPENRESTY=1
ENV DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
    if [ "${INSTALL_OPENRESTY}" != "1" ]; then \
        echo "INSTALL_OPENRESTY=${INSTALL_OPENRESTY}: using OpenResty from the base image"; \
    else \
        apt-get update; \
        apt-get install -y --no-install-recommends \
            wget gnupg ca-certificates lsb-release curl iproute2 procps; \
        wget -qO - https://openresty.org/package/pubkey.gpg \
            | gpg --dearmor -o /usr/share/keyrings/openresty.gpg; \
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/openresty.gpg] http://openresty.org/package/ubuntu jammy main" \
            > /etc/apt/sources.list.d/openresty.list; \
        apt-get update; \
        apt-get install -y --no-install-recommends "openresty=${OPENRESTY_VER}"; \
        rm -rf /var/lib/apt/lists/*; \
    fi

# Vendored lua-resty-logger-socket, used by the body-log path (async, cosocket).
# Copied unconditionally: harmless when the base already has it, required otherwise.
COPY vendor/resty/logger/socket.lua ${OR}/lualib/resty/logger/socket.lua

RUN set -eux; \
    mkdir -p /etc/nginx/sites-enabled ${OR}/nginx/logs; \
    ln -sf /dev/stdout ${OR}/nginx/logs/access.log; \
    ln -sf /dev/stderr ${OR}/nginx/logs/error.log

# 1) main config
COPY nginx.conf ${OR}/nginx/conf/nginx.conf
# 2) framework base (shared dicts, upstream, init block, global defaults) and the
#    shared location fragment included by every per-route server block
COPY session_base.conf router_locations.inc ${OR}/nginx/conf/conf.d/
# 3) the Lua engine
COPY lua/ ${OR}/nginx/conf/conf.d/lua/

# routes/ is where per-route configs are mounted; sock/ holds the unix sockets the
# 8080 dispatcher forwards to. Both must exist before the first per-route server
# binds, so create them in the image rather than at startup.
RUN mkdir -p ${OR}/nginx/conf/conf.d/routes ${OR}/nginx/sock

# The body-log listener address is a per-deployment value: blank it here so the
# container takes it from the environment. Unset means body logging is off, with no
# fallback address -- shipping request bodies to a host this deployment never named
# would be a data-exfiltration shape, not a convenience.
RUN set -eux; \
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_HOST)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf; \
    sed -ri 's|^(\s*env\s+BODYLOG_LISTENER_PORT)\s*=[^;]*;|\1;|' ${OR}/nginx/conf/nginx.conf

# No `openresty -t` at build time: routes/ is empty in the image, and
# router_locations.inc references variables that per-route server blocks declare.
# With no routes mounted, -t reports a spurious "unknown routed_session_id".
# It is meaningful only after routes are mounted.

# 8080 = path dispatch (the single external port), 8090 = admin/health.
EXPOSE 8080 8090

# nginx treats SIGTERM as fast shutdown, which cuts in-flight requests. SIGQUIT
# drains them. LLM responses can stream for minutes, so this matters.
STOPSIGNAL SIGQUIT
CMD ["/usr/local/openresty/bin/openresty", "-g", "daemon off;"]
