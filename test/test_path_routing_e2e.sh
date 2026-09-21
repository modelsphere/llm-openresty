#!/usr/bin/env bash
# Phase 1 回归:真 session_base.conf(含新 dispatch)+ 真 router_locations.inc + 2 个 per-model conf
# (真 form:dicts + 双 listen[老端口+socket] + set $route + register + include router_locations)+ mock。
# 证:① 经 8080 dispatch 按 /<key>/ 路由到对应模型(glm/kimi-k2.6 各自 mock)② SSE ③ 老端口仍直连(共存)
#     ④ 两路由隔离(打 glm 不影响 kimi 的池)。有 openresty 的机器上跑(chat)。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"; BASE="$HERE/.."
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"; PREFIX="${PREFIX:-/tmp/prE2E}"; KEY="${API_KEY:-}"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c nginx.conf -s stop 2>/dev/null; sleep 1; pkill -9 -f "$PREFIX/nginx" 2>/dev/null
  for p in 28951 28952; do pkill -9 -f "mock_vllm_sse.py --port $p" 2>/dev/null; done; }
trap cleanup EXIT
rm -rf "$PREFIX"; mkdir -p "$PREFIX/logs" "$PREFIX/temp" "$PREFIX/sock" "$PREFIX/conf.d/routes" "$PREFIX/conf.d/lua"

# 真基座 + 真 router_locations + 真 lua(镜像布局:conf.d/*.conf + conf.d/routes/*.conf + conf.d/lua/)
# listen unix: 要绝对路径 → 把 session_base 里的固定 sock 目录 sed 成本 harness 的 prefix/sock
cp "$BASE/session_base.conf" "$PREFIX/conf.d/session_base.conf"
sed -i "s#/usr/local/openresty/nginx/sock#$PREFIX/sock#g" "$PREFIX/conf.d/session_base.conf"
# chat 上 live openresty 已占 8080/8090/1808x → 隔离测试全改高位端口(dispatch 8080→19080、health 8090→19090)
sed -i "s#listen 8080;#listen 19080;#; s#listen 8090;#listen 19090;#" "$PREFIX/conf.d/session_base.conf"
cp "$BASE/router_locations.inc" "$PREFIX/conf.d/router_locations.inc"
cp "$BASE/lua/"*.lua "$PREFIX/conf.d/lua/"
cp -r "$BASE/vendor" "$PREFIX/vendor" 2>/dev/null

# per-model conf(真 form):唯一相对现状的新增 = `listen unix:sock/<key>.sock;`(老端口保留 = 共存)
mkroute(){ local key="$1" port="$2" mport="$3"
cat > "$PREFIX/conf.d/routes/session_route_$key.conf" <<EOF
lua_shared_dict active_conns_$key 4m; lua_shared_dict cluster_avg_$key 16k; lua_shared_dict lc_locks_$key 1m;
lua_shared_dict bad_peers_$key 1m; lua_shared_dict bodylog_ctl_$key 1m; lua_shared_dict cch_ctl_$key 1m;
server {
    listen $port;                              # 老端口(共存,过渡期)
    listen unix:$PREFIX/sock/$key.sock;        # ★ 新增:dispatch 按 /<key>/ 找到这个 socket(绝对路径)
    server_name _;
    set \$route "$key";
    set_by_lua_block \$__${key}_init { _G.register_route("$key", function() return {
        peers={{"127.0.0.1",$mport,"m-$key"}}, default_max=50, bodylog_default_enabled=false,
        health_check_interval=9999, adaptive_cc=false } end) return "" }
    include conf.d/router_locations.inc;
}
EOF
}
mkroute glm         19583 28951
mkroute kimi-k2.6   19582 28952

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
env BODYLOG_LISTENER_HOST=127.0.0.1; env BODYLOG_LISTENER_PORT=59999;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  log_format session_log escape=json '{"status":\$status,"sid":"\$routed_session_id","peer":"\$routed_peer"}';   # router_locations.inc 引用(内容不测)
  lua_package_path '$PREFIX/conf.d/lua/?.lua;$PREFIX/vendor/?.lua;;';
  include conf.d/*.conf;          # 真 session_base.conf(dicts+upstream+init+dispatch 8080+health 8090)
  include conf.d/routes/*.conf;   # per-model(socket+老端口)
}
EOF

echo "=== nginx -t ==="; "$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | tail -2 | sed 's/^/  /'
"$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | tail -1 | grep -q successful || { echo "  --- error ---"; "$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | tail -8; exit 1; }
for p in 28951 28952; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 10 --prefill-delay-ms 20 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c nginx.conf; sleep 2

P=0; F=0; ok(){ echo "  PASS: $1"; P=$((P+1)); }; no(){ echo "  FAIL: $1"; F=$((F+1)); }
A="Authorization: Bearer $KEY"; H="Content-Type: application/json"
body(){ echo "{\"model\":\"$1\",\"stream\":${2:-false},\"stream_options\":{\"include_usage\":true},\"chunk_delay_ms\":10,\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"; }
# 经 8080 dispatch,路径带 key
dget(){ curl -s -w "\n%{http_code}" -H "$A" -H "$H" -d "$(body "$2" "${3:-false}")" "http://127.0.0.1:19080/$1/v1/chat/completions"; }

echo "=== ① 经 dispatch 按路径分发到对应模型 ==="
for key in glm kimi-k2.6; do
  R=$(dget "$key" "$key"); C=$(echo "$R"|tail -1); B=$(echo "$R"|sed '$d')
  peer=$(echo "$B"|grep -oE 'm-[a-z0-9.-]+' | head -1)
  echo "  /$key/ -> code=$C peer_in_mock_name=$peer"
  { [ "$C" = 200 ] && echo "$B"|grep -q '"choices"'; } && ok "/$key/v1 经 8080 → 真 completion" || no "/$key/ 路由失败(code=$C)"
done

echo "=== ② SSE 流式过 dispatch ==="
CH=$(curl -s -N -H "$A" -H "$H" -d "$(body glm true)" "http://127.0.0.1:19080/glm/v1/chat/completions" | grep -c '^data:')
[ "${CH:-0}" -ge 2 ] && ok "SSE 分块(=$CH)" || no "SSE 未流式($CH)"

echo "=== ③ 老端口仍直连(共存)==="
OC=$(curl -s -o /dev/null -w "%{http_code}" -H "$A" -H "$H" -d "$(body glm false)" "http://127.0.0.1:19583/v1/chat/completions")
[ "$OC" = 200 ] && ok "老端口 18083 直连仍 200(过渡期共存)" || no "老端口挂了($OC)"

echo "=== ④ 未知 key → 502,dispatch 不崩 ==="
UC=$(curl -s -o /dev/null -w "%{http_code}" -H "$A" -H "$H" -d "$(body x false)" "http://127.0.0.1:19080/nosuch/v1/chat/completions")
{ [ "$UC" = 502 ] || [ "$UC" = 404 ]; } && ok "未知 key → $UC" || no "未知 key 异常($UC)"

echo "=== ⑤ 健康口 8090 ==="
curl -s http://127.0.0.1:19090/healthz | grep -q ok && ok "8090 /healthz ok(与 dispatch 分离)" || no "8090 挂了"

echo ""; echo "================ Phase1 path-routing e2e: PASS=$P FAIL=$F ================"; [ "$F" = 0 ] && echo "E2E PASS ✅" || { echo "E2E FAIL ❌ (error.log 尾)"; tail -8 "$PREFIX/logs/error.log"; }
