#!/usr/bin/env bash
# Phase 0 · 探针①:纯 nginx(无 lua)验证「路径捕获 route + rewrite 剥前缀 + $route 跨内部重定向保留」。
# 只证核心机制:dispatch regex location 捕获 <route>、rewrite last 剥掉 /<route>、内部重定向到 ^~ 内容
# location(v1/_route_inspect)后 $route 仍可读、且不会再被 dispatch regex 二次匹配成环。
# 在有 openresty 的机器上跑(chat):bash test_path_dispatch_probe.sh
set -uo pipefail
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PORT="${PORT:-19790}"
PREFIX="${PREFIX:-/tmp/path_probe}"
FAIL=0; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c nginx.conf -s stop 2>/dev/null; pkill -9 -f "$PREFIX/logs/nginx.pid" 2>/dev/null; }
trap cleanup EXIT
rm -rf "$PREFIX"; mkdir -p "$PREFIX/logs" "$PREFIX/temp"

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 64; }
http {
  access_log off;
  client_body_temp_path temp; proxy_temp_path temp; fastcgi_temp_path temp; uwsgi_temp_path temp; scgi_temp_path temp;
  server {
    listen $PORT; server_name _;
    # dispatch:捕获首段为 route,剥掉 /<route> 前缀,内部重定向重新匹配 location
    location ~ ^/(?<route>[a-z0-9._-]+)/ {
      rewrite ^/[a-z0-9._-]+(/.*)\$ \$1 last;
    }
    # 内容 location 用 ^~ / = ,剥前缀后它们压过 dispatch regex → 不会二次匹配成环
    location ^~ /v1/            { default_type text/plain; return 200 "V1 route=\$route uri=\$uri\n"; }
    location ^~ /_route_inspect { default_type text/plain; return 200 "INSPECT route=\$route uri=\$uri\n"; }
    location ^~ /_active_conns  { default_type text/plain; return 200 "CONNS route=\$route uri=\$uri\n"; }
    location = /healthz         { default_type text/plain; return 200 "ok\n"; }
  }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | sed 's/^/  [nginx -t] /'
"$OPENRESTY" -p "$PREFIX" -c nginx.conf || { bad "openresty 起不来"; exit 1; }
sleep 1
B="http://127.0.0.1:$PORT"
check(){ local url="$1" want="$2" got; got=$(curl -s "$B$url"); echo "    $url -> $got"; echo "$got" | grep -qF "$want" && ok "$url => $want" || bad "$url 期望含 '$want'"; }

check /glm/v1/chat/completions        "V1 route=glm uri=/v1/chat/completions"
check /kimi-k2.6/v1/models            "V1 route=kimi-k2.6 uri=/v1/models"
check /glm-b300/_route_inspect        "INSPECT route=glm-b300 uri=/_route_inspect"
check /glm/_active_conns              "CONNS route=glm uri=/_active_conns"
check /healthz                        "ok"

echo "=== 结果 ==="; [ "$FAIL" = 0 ] && echo "PROBE PASS ✅ (路径机制成立)" || echo "PROBE FAIL ❌"; exit $FAIL
