#!/bin/bash
# cases.md M1-M5: self-contained route + lazy init timing, with worker_processes 4.
#   M1 first request registers the route          -> "[<route>] route registered (N peers)"
#   M2 later requests hit the cache               -> no extra registration on a worker that already has it
#   M3 health/cluster_avg timers scheduled once   -> "[<route>] timers started on worker N" exactly once
#   M4 concurrent registration across workers     -> one line per worker pid, never more
#   M5 reload re-runs registration                -> new "route registered" lines after the reload
#
# NOTE — cases.md is stale on two points; the assertions below follow the code:
#   * There is no `ngx.worker.id() == 0` guard. Timer ownership is taken with the
#     lc_locks shdict key `__timer_alive_<route>` (route.lua:330-367), so ANY single
#     worker may own it — what is guaranteed is that exactly one does.
#   * The log line is "[<route>] route registered (N peers)" (route.lua:371); the
#     ", timers scheduled" suffix written in cases.md does not exist. Timer startup
#     is a separate line, "[<route>] timers started on worker N".
#
# Peers point at a dead port on purpose: registration/caching/timer ownership is what
# is under test here, and requests are expected to fail routing.
# Isolated scratch openresty (listen 19881), self-cleaning, never touches LIVE/production.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/lazyinit}"
ROUTE=k26m
PORT=19881
WORKERS=4
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
worker_processes $WORKERS; error_log logs/error.log info; pid logs/nginx.pid;
events { worker_connections 256; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict active_conns 4m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict cluster_avg 16k; lua_shared_dict lc_locks 1m; lua_shared_dict bad_peers 1m;
  lua_shared_dict api_keys 1m; lua_shared_dict bodylog_ctl 1m; lua_shared_dict cch_ctl 1m; lua_shared_dict reject_stat 128k;
  # Per-route dicts: route.lua defaults opts.*_dict to "<dict>_<route>" and hard-validates
  # them at registration. lc_locks_<route> in particular is what the timer-ownership lock
  # lives in, so without it no worker ever starts the health/cluster_avg timers.
  lua_shared_dict active_conns_$ROUTE 4m; lua_shared_dict bad_peers_$ROUTE 1m;
  lua_shared_dict lc_locks_$ROUTE 1m;     lua_shared_dict cluster_avg_$ROUTE 16k;
  lua_shared_dict cch_ctl_$ROUTE 1m;      lua_shared_dict bodylog_ctl_$ROUTE 1m;
  lua_shared_dict api_keys_$ROUTE 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  server { listen $PORT; server_name _;
    set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    # Lazy registration: set_by_lua_block runs per request, register_route is idempotent.
    set_by_lua_block \$__init { _G.register_route("$ROUTE", function() return {
        peers={{"127.0.0.1",1,"dead1"},{"127.0.0.1",2,"dead2"}},
        default_max=50, bodylog_default_enabled=false,
        health_check_interval=9999, adaptive_cc=false } end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$ROUTE) } proxy_pass http://vllm_backends; }
    location = /_route_state { content_by_lua_block { _G.dbg_route_state(_G.__route_opts.$ROUTE) } }
  }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
U="http://127.0.0.1:$PORT"; H="Content-Type: application/json"
ELOG="$PREFIX/logs/error.log"
REG="\[$ROUTE\] route registered"
hit(){ curl -s -o /dev/null --max-time 5 "$U/_route_state"; }
regn(){ grep -c "$REG" "$ELOG"; }
# distinct worker pids that logged a registration ("... [notice] <pid>#<tid>: ...")
regpids(){ grep "$REG" "$ELOG" | sed -E 's/.*\[notice\] ([0-9]+)#.*/\1/' | sort -u; }

echo "########## M1. 首请求触发注册 ##########"
n0=$(regn)
[ "$n0" -eq 0 ] && ok "M1 前置:启动后尚未有请求 → 未注册(lazy,不在 init 期注册)" \
  || no "M1 前置失败:没发请求就已有 $n0 条注册日志(不是 lazy?)"
hit
sleep 1
n1=$(regn)
[ "$n1" -ge 1 ] && ok "M1 首请求后出现注册日志($n1 条)" || no "M1 首请求后仍无 '$REG'"
grep -q "\[$ROUTE\] route registered (2 peers)" "$ELOG" \
  && ok "M1 日志形态 = '[$ROUTE] route registered (2 peers)'" \
  || no "M1 日志形态不符:$(grep "$REG" "$ELOG" | head -1)"

echo "########## M2. 再次请求走 cache,不重复注册 ##########"
# 同一 worker 第二次请求必须直接返 cached opts。多 worker 下请求可能落到新 worker
# (那是 M4 覆盖的合法新增),所以这里的判据是:注册数永不超过 worker 数。
before=$(regn)
for i in $(seq 1 30); do hit; done
sleep 1
after=$(regn)
[ "$after" -le "$WORKERS" ] \
  && ok "M2 30 次请求后注册数 $before→$after,未超过 worker 数 $WORKERS(cache 生效)" \
  || no "M2 注册数 $before→$after 超过 worker 数 $WORKERS = 每请求重复注册"

echo "########## M3. timer 只被一个 worker 接管 ##########"
tstart=""
for i in $(seq 1 20); do
  [ "$(grep -c "\[$ROUTE\] timers started on worker" "$ELOG")" -ge 1 ] && { tstart=1; break; }
  sleep 1
done
tn=$(grep -c "\[$ROUTE\] timers started on worker" "$ELOG")
[ -n "$tstart" ] && ok "M3 timer 已被接管:$(grep "\[$ROUTE\] timers started on worker" "$ELOG" | head -1 | sed -E 's/.*(timers started on worker [0-9]+).*/\1/')" \
  || no "M3 20s 内无 'timers started on worker'"
[ "$tn" -eq 1 ] && ok "M3 恰好 1 个 worker 抢到 lc_locks/__timer_alive_$ROUTE(不是每 worker 各起一套)" \
  || no "M3 有 $tn 个 worker 都启动了 timer(期望 1;lc_locks 抢锁失效)"
[ "$(grep -c "\[$ROUTE\] cannot start timers" "$ELOG")" -eq 0 ] \
  && ok "M3 无 'cannot start timers'(lc_locks_dict 已声明)" || no "M3 出现 cannot start timers"

echo "########## M4. 多 worker 并发注册安全 ##########"
for i in $(seq 1 40); do hit & done
wait
sleep 1
n4=$(regn); pids4=$(regpids | wc -l | tr -d ' ')
[ "$n4" -le "$WORKERS" ] && ok "M4 40 并发后注册数=$n4 ≤ worker 数 $WORKERS" \
  || no "M4 并发下注册数 $n4 超过 worker 数 $WORKERS(register_route 并发不安全)"
[ "$n4" -eq "$pids4" ] && ok "M4 每 worker 恰注册 1 次(注册日志 $n4 条来自 $pids4 个不同 worker pid)" \
  || no "M4 $n4 条注册日志只来自 $pids4 个 worker(有 worker 重复注册)"
[ "$(grep -c "\[$ROUTE\] register_route factory" "$ELOG")" -eq 0 ] \
  && ok "M4 无 factory 报错" || no "M4 出现 register_route factory 报错"

echo "########## M5. reload 后重新注册 ##########"
pre5=$(regn)
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s reload
sleep 3
mid5=$(regn)
[ "$mid5" -eq "$pre5" ] && ok "M5 reload 后未发请求时不注册(新 worker 同样 lazy,$pre5 条不变)" \
  || no "M5 reload 后未发请求就注册了($pre5→$mid5)"
for i in $(seq 1 20); do hit; done
sleep 1
post5=$(regn)
[ "$post5" -gt "$mid5" ] && ok "M5 reload 后新 worker 重新注册($mid5→$post5,_G 已重置)" \
  || no "M5 reload 后请求未触发重新注册($mid5→$post5)"
[ "$((post5 - mid5))" -le "$WORKERS" ] \
  && ok "M5 reload 后新增注册 $((post5 - mid5)) 条 ≤ worker 数 $WORKERS" \
  || no "M5 reload 后新增 $((post5 - mid5)) 条,超过 worker 数 $WORKERS"

echo "================ lazy init 时序(M1-M5): PASS=$P FAIL=$F ================"
[ "$F" -eq 0 ]
