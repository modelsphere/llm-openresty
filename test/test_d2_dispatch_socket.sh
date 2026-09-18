#!/usr/bin/env bash
# dispatch+socket 探针:验证「per-model server 只把 listen 改成 unix socket(其余不动:真 lua factory +
# router_locations),前面一个 dispatch server 从路径捕获 route、proxy 到 /<route>.sock」全链路。
# 证:① /<route>/v1/ 经 dispatch → socket → 真 do_route → mock 后端拿到真回答 ② SSE 流式过双跳
# ③ /<route>/_tps_status 等 debug 也通 ④ 未知 route 优雅报错(不崩)⑤ Host 头原样透传(不被魔改)。
# 有 openresty 的机器上跑(chat):bash test_d2_dispatch_socket.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/d2test}"; KEY="${API_KEY:-}"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c nginx.conf -s stop 2>/dev/null; sleep 1; pkill -9 -f "$PREFIX/nginx" 2>/dev/null
  for p in 28941 28942; do pkill -9 -f "mock_vllm_sse.py --port $p" 2>/dev/null; done; }
trap cleanup EXIT
rm -rf "$PREFIX"; mkdir -p "$PREFIX/logs" "$PREFIX/temp" "$PREFIX/routes"

# 抽 session_base 的 init_by_lua_block(require router + 全局配置)+ 前置 lua_package_path
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
  lua_shared_dict active_conns_glm 4m; lua_shared_dict cluster_avg_glm 16k; lua_shared_dict lc_locks_glm 1m; lua_shared_dict bad_peers_glm 1m; lua_shared_dict bodylog_ctl_glm 1m; lua_shared_dict cch_ctl_glm 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}; _G.TTFT_BUCKETS_MS = {50,100,200,300,500,800,1200}
    -- 路径路由 里注册照旧靠 per-model server 的 set_by_lua;这里图省事直接 init_worker 注册 glm(注册机制无关本次验证)
    _G.register_route("glm", function() return {
      peers = {{"127.0.0.1",28941,"g1"},{"127.0.0.1",28942,"g2"}},
      default_max=50, bodylog_default_enabled=false, health_check_interval=9999, adaptive_cc=false } end)
  }

  # ── per-model server:唯一改动 = listen 成 unix socket;server_name/set \$route/router_locations 全不动 ──
  server {
    listen unix:$PREFIX/routes/glm.sock;
    server_name _;
    set \$route "glm";
    set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    location /v1/ {
      access_by_lua_block { _G.do_route(_G.__route_opts.glm) }
      body_filter_by_lua_block { _G.bodylog_filter_chunk() }
      proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s;
      proxy_set_header Host \$host;
      log_by_lua_block { _G.do_log_release(_G.__route_opts.glm) }
    }
    location = /_tps_status  { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.glm) } }
    location = /_echo_host   { default_type text/plain; return 200 "host=\$host route=\$route\n"; }
  }

  # ── dispatch:捕获路径首段 route,剥前缀,proxy 到 /<route>.sock(socket 路径从 \$route 派生,零映射)──
  server {
    listen 19593; server_name _;
    location ~ ^/(?<droute>[a-z0-9._-]+)(?<rest>/.*)\$ {
      proxy_pass http://unix:$PREFIX/routes/\$droute.sock:\$rest;
      proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s;
      proxy_set_header Host \$host;   # 原样透传客户端 Host(路径路由 用不着占 Host 做路由 → 能保留它)
    }
    location = /_up { return 200 "dispatch up\n"; }
  }
}
EOF

echo "=== nginx -t ==="
"$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | tail -2 | sed 's/^/  /'
"$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | tail -1 | grep -q successful || exit 1
for p in 28941 28942; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 10 --prefill-delay-ms 30 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c nginx.conf; sleep 2

P=0; F=0; ok(){ echo "  PASS: $1"; P=$((P+1)); }; no(){ echo "  FAIL: $1"; F=$((F+1)); }
A="Authorization: Bearer $KEY"; H="Content-Type: application/json"; D=http://127.0.0.1:19593
body(){ echo "{\"model\":\"glm-5.1-fp8\",\"stream\":${1:-false},\"stream_options\":{\"include_usage\":true},\"chunk_delay_ms\":10,\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"; }

echo "=== ① 经 dispatch 路径 → socket → do_route → mock(非流)==="
R=$(curl -s -w "\n%{http_code}" -H "$A" -H "$H" -d "$(body false)" "$D/glm/v1/chat/completions")
CODE=$(echo "$R"|tail -1); BODY=$(echo "$R"|sed '$d')
echo "  code=$CODE body=$(echo "$BODY"|head -c 120)"
[ "$CODE" = 200 ] && echo "$BODY"|grep -q '"choices"' && ok "/glm/v1/ 经 dispatch→socket→do_route 拿到真 completion" || no "路由未通(code=$CODE)"

echo "=== ② SSE 流式过双跳(dispatch→socket)==="
CHUNKS=$(curl -s -N -H "$A" -H "$H" -d "$(body true)" "$D/glm/v1/chat/completions" | grep -c '^data:')
echo "  SSE data 行数=$CHUNKS"
[ "${CHUNKS:-0}" -ge 2 ] && ok "SSE 分块到达(≥2 data 行,未被缓冲成一坨)" || no "SSE 未流式($CHUNKS)"

echo "=== ③ debug endpoint 经 dispatch(/glm/_tps_status)==="
TC=$(curl -s -o /dev/null -w "%{http_code}" "$D/glm/_tps_status")
[ "$TC" = 200 ] && ok "/glm/_tps_status 经 dispatch → 200" || no "debug endpoint 未通($TC)"

echo "=== ④ 未知 route 优雅报错(socket 不存在)==="
UC=$(curl -s -o /dev/null -w "%{http_code}" "$D/nosuchroute/v1/chat/completions" -H "$A" -H "$H" -d "$(body false)")
echo "  未知 route code=$UC"
[ "$UC" = 502 ] || [ "$UC" = 404 ] || [ "$UC" = 503 ] && ok "未知 route → $UC(优雅错误,进程没崩)" || no "未知 route 行为异常($UC)"
curl -s -o /dev/null -w "%{http_code}" "$D/_up" | grep -q 200 && ok "dispatch 进程仍健康(未知 route 后)" || no "dispatch 崩了"

echo "=== ⑤ Host 头原样透传(路径路由 不魔改 Host)==="
HH=$(curl -s -H "Host: myhost.example" "$D/glm/_echo_host")
echo "  $HH"
echo "$HH"|grep -q 'host=myhost.example' && ok "Host 原样透传(route 从路径来,不占 Host)" || no "Host 被改了: $HH"

echo ""; echo "================ 路径路由 dispatch+socket: PASS=$P FAIL=$F ================"; [ "$F" = 0 ] && echo "PROBE PASS ✅" || echo "PROBE FAIL ❌"
