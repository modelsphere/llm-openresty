#!/bin/bash
# cases.md K1-K4: a route registered with peers={} must degrade gracefully, not crash.
#   K1 GET  /_route_state   -> 503 {"error":"no peers configured","route":...}
#   K2 POST /v1/            -> 503 {"error":"all peers banned, no healthy upstream",...}
#   K3 POST /_route_inspect -> 503 {"error":"no peers configured",...}
#   K4 health loop stays quiet on the empty route while a control route logs
#
# Two routes are registered on purpose:
#   <route>   peers={}                 -- the degradation under test
#   <route>c  one peer on a dead port  -- control group
# The control exists because "no health log" is trivially true when the timer never
# started, when the log format changed, or when the harness never came up. Asserting
# that the control route DOES log "health: " first makes the empty route's silence
# mean something.
#
# What K4 does NOT prove: that the `#opts.peers == 0` early-skip in timers.lua is
# present. check_all_peers() iterates opts.peer_keys, so an empty table yields zero
# log lines whether the skip is there or not -- the branch is not observable from
# outside. K4 covers "the empty route neither crashes the health loop nor floods the
# log", which is the part that is actually observable. Do not read more into it.
#
# The routes carry an explicit empty api_keys table: do_route authenticates BEFORE
# evaluating the pool (access.lua), and the default table comes from the key file on
# the host. On a machine that has /etc/openresty/api-keys/keys -- which chat_deploy.sh
# writes, and which bare metal is supposed to have -- K2 would get 401 and never reach
# the 503 under test. Pinning the table here keeps the case machine-independent.
#
# Isolated scratch openresty, self-cleaning, never touches LIVE/production.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
# Both are overridable so two runs on a shared box do not delete each other's scratch
# dir or fight over the port.
PREFIX="${PREFIX:-/tmp/emptypeers.$$}"
PORT="${PORT:-19880}"
CPORT="${CPORT:-19882}"
ROUTE=k26e
CROUTE=k26c

cleanup(){
  "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null
  # Wait for the master to actually go: `-s stop` is asynchronous, and the pid file
  # disappearing is what tells us the workers went with it.
  for _ in $(seq 1 20); do [ -f "$PREFIX/logs/nginx.pid" ] || break; sleep 0.5; done
  # Fallback. `ps | grep $PREFIX` only ever matches the MASTER -- a worker's cmdline is
  # "nginx: worker process" and carries no path -- so killing by pattern alone can leave
  # orphaned workers holding the port, which turns the NEXT run into a silent false pass.
  # Kill the master's children too, then verify the port is free.
  for pid in $(ps -eo pid,cmd | grep "$PREFIX/nginx" | grep -v grep | awk '{print $1}'); do
    pkill -9 -P "$pid" 2>/dev/null
    kill -9 "$pid" 2>/dev/null
  done
  rm -rf "$PREFIX"
}
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
  # them at registration. Without these the request dies in reqtransform (nil cch_ctl)
  # long before reaching the empty-peers path under test.
  lua_shared_dict active_conns_$ROUTE 4m; lua_shared_dict bad_peers_$ROUTE 1m;
  lua_shared_dict lc_locks_$ROUTE 1m;     lua_shared_dict cluster_avg_$ROUTE 16k;
  lua_shared_dict cch_ctl_$ROUTE 1m;      lua_shared_dict bodylog_ctl_$ROUTE 1m;
  lua_shared_dict api_keys_$ROUTE 1m;
  lua_shared_dict active_conns_$CROUTE 4m; lua_shared_dict bad_peers_$CROUTE 1m;
  lua_shared_dict lc_locks_$CROUTE 1m;     lua_shared_dict cluster_avg_$CROUTE 16k;
  lua_shared_dict cch_ctl_$CROUTE 1m;      lua_shared_dict bodylog_ctl_$CROUTE 1m;
  lua_shared_dict api_keys_$CROUTE 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  server { listen $PORT; server_name _;
    set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    # peers={} on purpose: this is the degraded shape K1-K4 describe.
    # health_check_interval=3 so several health rounds elapse inside the K4 window.
    set_by_lua_block \$__init { _G.register_route("$ROUTE", function() return {
        peers={}, api_keys={}, default_max=50, bodylog_default_enabled=false,
        health_check_interval=3, adaptive_cc=false } end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$ROUTE) } proxy_pass http://vllm_backends; }
    location = /_route_state    { content_by_lua_block { _G.dbg_route_state(_G.__route_opts.$ROUTE) } }
    location = /_route_inspect  { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.$ROUTE) } }
    location = /_health_status  { content_by_lua_block { _G.dbg_health_status(_G.__route_opts.$ROUTE) } }
  }
  server { listen $CPORT; server_name _;
    set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    # Control: one peer on a closed port, so its health loop must log "health: ... DOWN".
    set_by_lua_block \$__cinit { _G.register_route("$CROUTE", function() return {
        peers={{"127.0.0.1",1,"dead"}}, api_keys={}, default_max=50, bodylog_default_enabled=false,
        health_check_interval=3, adaptive_cc=false } end); return "" }
    location = /_route_state { content_by_lua_block { _G.dbg_route_state(_G.__route_opts.$CROUTE) } }
  }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"

# Startup gate. `-t` passing does not mean it came up: the port may be held by an
# orphaned worker from an earlier run. Without this gate every later judgement that
# tolerates a missing answer (a "not 500" check, an upper-bound count) would pass on a
# dead server -- the whole suite would go green having tested nothing.
up=""
for _ in $(seq 1 20); do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 "http://127.0.0.1:$PORT/_route_state" 2>/dev/null)
  [ -n "$code" ] && [ "$code" != "000" ] && { up=1; break; }
  sleep 0.5
done
[ -n "$up" ] || { echo "FAIL: openresty 未在 10s 内起来(端口 $PORT 可能被上次残留的 worker 占用)"
  echo "--- error.log 末尾:"; tail -5 "$PREFIX/logs/error.log" 2>/dev/null; exit 1; }

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
U="http://127.0.0.1:$PORT"; CU="http://127.0.0.1:$CPORT"; H="Content-Type: application/json"
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
[ "$c2" = "503" ] && ok "K2 POST /v1/ → 503" \
  || no "K2 期望 503,得 $c2$([ "$c2" = "401" ] && echo '(401 = 鉴权先于池评估;factory 的 api_keys={} 没生效?)')(body=$o2)"
echo "$o2" | grep -q '"error":"all peers banned, no healthy upstream"' && echo "$o2" | grep -q "\"route\":\"$ROUTE\"" \
  && ok "K2 body = {\"error\":\"all peers banned, no healthy upstream\",\"route\":\"$ROUTE\"}" || no "K2 body 不符: $o2"

echo "########## K3. /_route_inspect 空 peers 退化 ##########"
# 判据是确定值 503,不是"非 500" —— 后者在服务没起来(curl 得 000)、401、404 时全都会通过。
c3=$(curl -s -o "$PREFIX/k3.out" -w "%{http_code}" -X POST -H "$H" \
     -d '{"model":"m","messages":[{"role":"user","content":"hi"}]}' "$U/_route_inspect")
o3=$(cat "$PREFIX/k3.out")
[ "$c3" = "503" ] && ok "K3 /_route_inspect → 503(未崩成 500)" || no "K3 期望 503,得 $c3(body=$o3)"
echo "$o3" | grep -q '"error":"no peers configured"' \
  && ok "K3 body = no peers configured" || no "K3 body 不符: $o3"
echo "$o3" | grep -q '"pick"' && no "K3 空 peers 不该回 pick 字段: $o3" || ok "K3 无 pick 字段(符合 pick=nil 预期)"

echo "########## K4. 空 peers 路由的 health loop 安静,对照路由有日志 ##########"
# 先摸到对照路由,触发它的注册 + timer
curl -s -o /dev/null --max-time 2 "$CU/_route_state"
# 两条路由的 timer 都在 lc_locks_<route> 各自抢锁,互不影响
started=""
for _ in $(seq 1 20); do
  grep -q "\[$ROUTE\] timers started on worker" "$ELOG" && { started=1; break; }
  sleep 1
done
[ -n "$started" ] && ok "K4 前置:空 peers 路由的 timer 已启动" \
  || no "K4 前置失败:20s 内没有 '[$ROUTE] timers started on worker'"
# health loop 首轮在 5s 后,interval=3 → 再等 14s 跑过约 4 轮
sleep 14
# 对照组:死 peer 必须产出 health 日志。它失败 = harness 观测不到 health 日志
# (timer 没跑/日志格式变了/等待不够),此时空 peers 的"零日志"没有任何意义。
chn=$(grep -c "\[$CROUTE\] health: " "$ELOG")
[ "$chn" -ge 1 ] && ok "K4 对照组:死 peer 路由产出 $chn 条 '[$CROUTE] health: '(证明本 harness 观测得到)" \
  || no "K4 对照组失败:死 peer 路由也没有 health 日志 —— 空 peers 的'零日志'无意义,本轮作废"
# 精确匹配 "health: ",不匹配 health_loop crashed / health check skipped ——
# 那两条是别的故障,混进来会把"loop 崩了"误述成"skip 没生效"。
hn=$(grep -c "\[$ROUTE\] health: " "$ELOG")
[ "$hn" -eq 0 ] && ok "K4 空 peers 路由无 '[$ROUTE] health: ' 日志" \
  || no "K4 空 peers 路由出现 $hn 条 health 日志(期望 0):$(grep "\[$ROUTE\] health: " "$ELOG" | head -2)"
# 这两条单独判,故障语义各不相同
[ "$(grep -c "\[$ROUTE\] health_loop crashed" "$ELOG")" -eq 0 ] \
  && ok "K4 health_loop 未崩溃" || no "K4 health_loop crashed"
[ "$(grep -c "\[$ROUTE\] health check skipped" "$ELOG")" -eq 0 ] \
  && ok "K4 无 'health check skipped'(dict 已声明,loop 真的在跑)" \
  || no "K4 出现 'health check skipped' —— loop 因 dict 缺失压根没调度,K4 空过"

echo "########## K5. 空 peers 下监控仍拿得到鉴权状态 ##########"
# 回归守卫。曾经 _meta 只在 peers 循环之后构造,于是空 peers 路由的 /_health_status
# 在 503 早返处就结束了 —— 监控读不到 api_keys_configured / route_auth_enabled /
# key_file_status。而这恰恰是最需要它的时刻:peers 没了、同时 key Secret 也可能没挂上。
# api_keys.lua / values.yaml / secret.yaml 三处都承诺「fail-open 可经 /_health_status 发现」,
# 那个承诺在空 peers 上曾经不成立。
c5=$(curl -s -o "$PREFIX/k5.out" -w "%{http_code}" "$U/_health_status")
o5=$(cat "$PREFIX/k5.out")
[ "$c5" = "503" ] && ok "K5 空 peers → /_health_status 仍返 503(状态码不变)" || no "K5 期望 503,得 $c5(body=$o5)"
echo "$o5" | grep -q '"error":"no peers configured"' \
  && ok "K5 body 仍含 no peers configured" || no "K5 body 不含 no peers configured: $o5"
for fld in api_keys_configured route_auth_enabled key_file_status; do
  echo "$o5" | grep -q "\"$fld\"" \
    && ok "K5 503 响应里仍带 _meta.$fld(监控可见)" \
    || no "K5 _meta.$fld 缺失 —— 空 peers 时监控读不到鉴权状态: $o5"
done
# 本路由 factory 写死 api_keys={},所以它必须报“这条路由没在鉴权”。
# 这一条同时防止 _meta 退化成恒定输出:若它恒为 true,这里就会红。
echo "$o5" | grep -q '"route_auth_enabled":false' \
  && ok "K5 route_auth_enabled=false(与 factory 的 api_keys={} 一致,非恒定输出)" \
  || no "K5 route_auth_enabled 不是 false: $o5"

echo "================ 空 peers 退化(K1-K5): PASS=$P FAIL=$F ================"
[ "$F" -eq 0 ]
