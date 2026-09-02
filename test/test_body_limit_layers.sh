#!/usr/bin/env bash
# 验证请求体上限落在预期的层:dispatch(:8080) 是天花板(100m),业务上限在 per-model。
#
# 背景:dispatch 在 per-model【之前】处理请求,一条路径上取最严的那层。原来 dispatch 压 10m,
# 于是任何 per-model 想放宽(视频生成要 64MB 的 base64 图)都够不着。改成 dispatch 100m +
# per-model 10m 后,LLM 的实际上限不变,而 per-model 自己配大额度的路由才真的能生效。
#
# 做法:用本仓库的 conf 起一份**独立的** openresty(高位端口、自己的 prefix),不碰在跑的服务。
# 后端用 mock,响应固定 200 —— 我们只关心 nginx 在哪一层返回 413。
#
#   bash test/test_body_limit_layers.sh
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
PREFIX=$(mktemp -d)
DISPATCH_PORT=${DISPATCH_PORT:-19180}
BIG_PORT=${BIG_PORT:-19181}      # 一个"大额度 per-model",模拟视频生成路由
MOCK_PORT=${MOCK_PORT:-19182}
trap 'pkill -F "$PREFIX/nginx.pid" 2>/dev/null; kill %1 2>/dev/null; rm -rf "$PREFIX"' EXIT

command -v openresty >/dev/null || { echo "SKIP: 本机没有 openresty"; exit 0; }

mkdir -p "$PREFIX"/{conf,logs,sock}

# 从仓库里的实际配置取值(不写死,改了 conf 这个测试自动跟着变)
limit_of() { grep -oE 'client_max_body_size[[:space:]]+[0-9]+[kKmMgG]?' "$1" | head -1 | awk '{print $2}'; }
DISPATCH_LIMIT=$(limit_of session_base.conf)
PERMODEL_LIMIT=$(limit_of router_locations.inc)
echo "取自仓库:dispatch=$DISPATCH_LIMIT  per-model=$PERMODEL_LIMIT"
# 后端 mock:什么都回 200(用 nginx 自己起,免依赖)
cat > "$PREFIX/conf/nginx.conf" <<EOF
worker_processes 1;
error_log $PREFIX/logs/error.log warn;
pid $PREFIX/nginx.pid;
events { worker_connections 256; }
http {
    access_log off;
    client_body_temp_path $PREFIX/body_temp;
    proxy_temp_path $PREFIX/proxy_temp;
    fastcgi_temp_path $PREFIX/fcgi_temp;
    uwsgi_temp_path $PREFIX/uwsgi_temp;
    scgi_temp_path $PREFIX/scgi_temp;

    # ── 后端 mock:什么都回 200。client_max_body_size 0 = 不限
    #    (漏了这行会走 nginx 默认 1m,于是所有大 body 都被 mock 拒掉、
    #     看起来像被测层拒的,白排查一轮)
    server {
        listen $MOCK_PORT;
        client_max_body_size 0;
        location / { return 200 "ok"; }
    }

    # ── per-model:业务上限(取自 router_locations.inc 的实际值)
    server {
        listen unix:$PREFIX/sock/llm.sock;
        client_max_body_size $PERMODEL_LIMIT;
        location / { proxy_pass http://127.0.0.1:$MOCK_PORT; }
    }
    # ── per-model:大额度路由(视频生成那类)
    server {
        listen unix:$PREFIX/sock/video.sock;
        client_max_body_size 64m;
        location / { proxy_pass http://127.0.0.1:$MOCK_PORT; }
    }
    # 直连大额度 per-model(绕过 dispatch),用来确认"业务层自己说了算"
    server {
        listen $BIG_PORT;
        client_max_body_size 64m;
        location / { proxy_pass http://127.0.0.1:$MOCK_PORT; }
    }

    # ── dispatch:天花板(取自 session_base.conf 的实际值)
    server {
        listen $DISPATCH_PORT;
        client_max_body_size $DISPATCH_LIMIT;
        location ~ ^/(?<rkey>[a-z0-9._-]+)(?<rest>/.*)\$ {
            proxy_pass http://unix:$PREFIX/sock/\$rkey.sock:\$rest;
            proxy_request_buffering on;
        }
    }
}
EOF

openresty -p "$PREFIX" -c conf/nginx.conf -t 2>&1 | tail -2
openresty -p "$PREFIX" -c conf/nginx.conf || { echo "FAIL: 起不来"; exit 1; }
sleep 1

body() { head -c "$1" /dev/zero | tr '\0' 'a'; }
probe() {  # probe <url> <bytes>
  body "$2" | curl -s -o /dev/null -w '%{http_code}' -m 30 --data-binary @- \
      -H 'Content-Type: application/json' "$1"
}

fail=0
check() {  # check <说明> <期望码> <实际码>
  if [ "$2" = "$3" ]; then echo "  PASS  $1 -> $3"; else echo "  FAIL  $1 -> 期望 $2,实际 $3"; fail=1; fi
}

echo "== 经 dispatch 打 LLM 路由(业务上限 10m)"
check "5MB 应放行"      200 "$(probe "http://127.0.0.1:$DISPATCH_PORT/llm/v1/chat/completions" $((5*1024*1024)))"
check "20MB 应被 413"   413 "$(probe "http://127.0.0.1:$DISPATCH_PORT/llm/v1/chat/completions" $((20*1024*1024)))"

echo "== 经 dispatch 打大额度路由(业务上限 64m):dispatch 不再是瓶颈"
check "20MB 应放行"     200 "$(probe "http://127.0.0.1:$DISPATCH_PORT/video/v2/video_generation" $((20*1024*1024)))"
check "60MB 应放行"     200 "$(probe "http://127.0.0.1:$DISPATCH_PORT/video/v2/video_generation" $((60*1024*1024)))"

echo "== 超过 dispatch 天花板(100m)时,连大额度路由也进不来"
check "110MB 应被 413"  413 "$(probe "http://127.0.0.1:$DISPATCH_PORT/video/v2/video_generation" $((110*1024*1024)))"

echo "== 直连 per-model(绕过 dispatch):业务层自己说了算"
check "60MB 应放行"     200 "$(probe "http://127.0.0.1:$BIG_PORT/v2/video_generation" $((60*1024*1024)))"

exit $fail
