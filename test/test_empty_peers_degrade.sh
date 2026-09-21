#!/bin/bash
# cases.md K1-K4: a route registered with peers={} must degrade gracefully, not crash.
#   K1 GET  /_route_state   -> 503 {"error":"no peers configured","route":...}
#   K2 POST /v1/            -> 503 {"error":"all peers banned, no healthy upstream",...}
#   K3 POST /_route_inspect -> no crash (no 500, no "pick" field)
#   K4 health timer starts but skips every round -> no "[<route>] health" line in error.log
# K4 is only meaningful if the timer really started, so we first assert the
# "timers started on worker" line — otherwise "no health log" would pass vacuously.
# Isolated scratch openresty (listen 19880), self-cleaning, never touches LIVE/production.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/emptypeers}"
ROUTE=k26e
PORT=19880
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log info; pid logs/nginx.pid;
events { worker_connections 256; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict active_conns 4m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict cluster_avg 16k; lua_shared_dict lc_locks 1m; lua_shared_dict bad_peers 1m;
  lua_shared_dict api_keys 1m; lua_shared_dict bodylog_ctl 1m; lua_shared_dict cch_ctl 1m; lua_shared_dict reject_stat 128k;
  # Per-route dicts: route.lua defaults opts.*_dict to "<dict>_<route>" and hard-validates
  # them at registration, so a real route conf declares its own set. Without these the
  # request dies in reqtransform (nil cch_ctl) long before reaching the empty-peers path.
  lua_shared_dict active_conns_$ROUTE 4m; lua_shared_dict bad_peers_$ROUTE 1m;
  lua_shared_dict lc_locks_$ROUTE 1m;     lua_shared_dict cluster_avg_$ROUTE 16k;
  lua_shared_dict cch_ctl_$ROUTE 1m;      lua_shared_dict bodylog_ctl_$ROUTE 1m;
  lua_shared_dict api_keys_$ROUTE 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  server { listen $PORT; server_name _;
    set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    # peers={} on purpose: this is the degraded shape K1-K4 describe.
    # health_check_interval=3 so several health rounds elapse inside the K4 window.
    set_by_lua_block \$__init { _G.register_route("$ROUTE", function() return {
        peers={}, default_max=50, bodylog_default_enabled=false,
        health_check_interval=3, adaptive_cc=false } end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$ROUTE) } proxy_pass http://vllm_backends; }
    location = /_route_state   { content_by_lua_block { _G.dbg_route_state(_G.__route_opts.$ROUTE) } }
    location = /_route_inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.$ROUTE) } }
  }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
U="http://127.0.0.1:$PORT"; H="Content-Type: application/json"
ELOG="$PREFIX/logs/error.log"

echo "########## K1. /_route_state 空 peers 优雅退化 ##########"
body=$(curl -s -o "$PREFIX/k1.out" -w "%{http_code}" "$U/_route_state")
out=$(cat "$PREFIX/k1.out")
[ "$body" = "503" ] && ok "K1 /_route_state → 503(不是 500 崩溃)" || no "K1 期望 503,得 $body(body=$out)"
echo "$out" | grep -q '"error":"no peers configured"' && echo "$out" | grep -q "\"route\":\"$ROUTE\"" \
  && ok "K1 body = {\"error\":\"no peers configured\",\"route\":\"$ROUTE\"}" || no "K1 body 不符: $out"
echo "$out" | grep -q 'active_level' && no "K1 不应回落旧 schema(出现了 active_level): $out" \
  || ok "K1 未回落旧 schema(无 active_level 字段)"

echo "########## K2. POST /v1/ → 503 all peers banned ##########"
c2=$(curl -s -o "$PREFIX/k2.out" -w "%{http_code}" -X POST -H "$H" \
     -d '{"model":"m","messages":[{"role":"user","content":"hi"}]}' "$U/v1/chat/completions")
o2=$(cat "$PREFIX/k2.out")
[ "$c2" = "503" ] && ok "K2 POST /v1/ → 503" || no "K2 期望 503,得 $c2(body=$o2)"
echo "$o2" | grep -q '"error":"all peers banned, no healthy upstream"' && echo "$o2" | grep -q "\"route\":\"$ROUTE\"" \
  && ok "K2 body = {\"error\":\"all peers banned, no healthy upstream\",\"route\":\"$ROUTE\"}" || no "K2 body 不符: $o2"

echo "########## K3. /_route_inspect 不崩 ##########"
c3=$(curl -s -o "$PREFIX/k3.out" -w "%{http_code}" -X POST -H "$H" \
     -d '{"model":"m","messages":[{"role":"user","content":"hi"}]}' "$U/_route_inspect")
o3=$(cat "$PREFIX/k3.out")
[ "$c3" != "500" ] && ok "K3 /_route_inspect → $c3(非 500,未崩)" || no "K3 崩成 500: $o3"
echo "$o3" | grep -q '"pick"' && no "K3 空 peers 不该回 pick 字段: $o3" || ok "K3 无 pick 字段(符合 pick=nil 预期)"

echo "########## K4. health timer 启动但每轮跳过 ##########"
# 先证 timer 真起来了(route.lua 抢 lc_locks 的 __timer_alive_<name>),否则 K4 的"无 health 日志"是空过。
started=""
for i in $(seq 1 20); do
  grep -q "\[$ROUTE\] timers started on worker" "$ELOG" && { started=1; break; }
  sleep 1
done
[ -n "$started" ] && ok "K4 前置:health/cluster_avg timer 已启动(timers started on worker)" \
  || no "K4 前置失败:20s 内没有 'timers started on worker',后面的断言会空过"
# health loop 首轮在 5s 后,interval=3 → 再等 14s 足够跑过 ~4 轮
sleep 14
hn=$(grep -c "\[$ROUTE\] health" "$ELOG")
[ "$hn" -eq 0 ] && ok "K4 error.log 无 '[$ROUTE] health' 日志(每轮因 #peers==0 跳过)" \
  || no "K4 出现 $hn 条 '[$ROUTE] health' 日志(期望 0):$(grep "\[$ROUTE\] health" "$ELOG" | head -3)"
hc=$(grep -c "\[$ROUTE\] health_loop crashed" "$ELOG")
[ "$hc" -eq 0 ] && ok "K4 health_loop 未崩溃" || no "K4 health_loop crashed ×$hc"

echo "================ 空 peers 退化(K1-K4): PASS=$P FAIL=$F ================"
[ "$F" -eq 0 ]
