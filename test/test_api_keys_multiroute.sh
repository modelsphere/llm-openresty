#!/bin/bash
# 两个路由带**不同**的 key 表时,共用的 api_keys dict 不能互相冲刷。
#
# 背景:lua_shared_dict api_keys 是所有路由共用一份(session_base.conf),
# 而 opts.api_keys 允许 per-route 覆盖。若播种用"整份 flush 再灌",两张表会
# 互相冲掉,并在"A 灌完 → B 冲掉 → A 查表"之间产生**假 401** —— 打到 LLM 路由上
# 就是线上间歇性鉴权失败。现在按指纹给条目加前缀,两张表并存。
#
# 交叉验证:A 的 key 在 B 上必须 401,反之亦然;交替打不能出现任何一次假 401。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/akmulti}"
KA="sk-route-a-1111"; KB="sk-route-b-2222"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  cp "$PREFIX/logs/error.log" /tmp/akmulti-error.log 2>/dev/null; rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp" "$PREFIX/lua"
LUA_SRC="${LUA_SRC:-$HERE/../lua}"
cp "$LUA_SRC/"*.lua "$PREFIX/lua/"
echo "  lua 来源: $LUA_SRC"

cat > "$PREFIX/nginx.conf" <<NG
worker_processes 4; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 256; }
http {
  lua_package_path '$PREFIX/lua/?.lua;;';
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict active_conns 4m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict cluster_avg 16k; lua_shared_dict lc_locks 1m; lua_shared_dict bad_peers 1m;
  lua_shared_dict api_keys 1m; lua_shared_dict bodylog_ctl 1m; lua_shared_dict cch_ctl 1m; lua_shared_dict reject_stat 128k;
  init_by_lua_block { require "router"; _G.RT_LIMIT_FACTOR = 2; _G.MAX_CONCURRENCY_PER_PEER = 100 }
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  server { listen 19582; location / { return 200 "backend-ok"; } }
  server { listen 19580; server_name _;
    set \$route "ra"; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-";
    set_by_lua_block \$__ra { _G.register_route("ra", function()
        return { peers = { {"127.0.0.1", 19582, "m", 0, 100} }, health_check = false,
                 api_keys = { ["$KA"] = "a" },
                 active_conns_dict="active_conns", ttft_dict="ttft_stat", tps_dict="tps_stat",
                 cluster_avg_dict="cluster_avg", lc_locks_dict="lc_locks", bad_peers_dict="bad_peers",
                 cch_ctl_dict="cch_ctl", bodylog_ctl_dict="bodylog_ctl", reject_stat_dict="reject_stat" }
    end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts[ngx.var.route]) } proxy_pass http://vllm_backends; }
  }
  server { listen 19581; server_name _;
    set \$route "rb"; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-";
    set_by_lua_block \$__rb { _G.register_route("rb", function()
        return { peers = { {"127.0.0.1", 19582, "m", 0, 100} }, health_check = false,
                 api_keys = { ["$KB"] = "b" },
                 active_conns_dict="active_conns", ttft_dict="ttft_stat", tps_dict="tps_stat",
                 cluster_avg_dict="cluster_avg", lc_locks_dict="lc_locks", bad_peers_dict="bad_peers",
                 cch_ctl_dict="cch_ctl", bodylog_ctl_dict="bodylog_ctl", reject_stat_dict="reject_stat" }
    end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts[ngx.var.route]) } proxy_pass http://vllm_backends; }
  }
}
NG
command -v "$OPENRESTY" >/dev/null || { echo "SKIP: 没有 $OPENRESTY"; exit 0; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" || { echo "FAIL: 起不来"; exit 1; }
sleep 1
code() { curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $2" "http://127.0.0.1:$1/v1/models"; }

fail=0
chk(){ if [ "$2" = "$3" ]; then echo "  PASS $1 (期望$2 实际$3)"; else echo "  FAIL $1 (期望$2 实际$3)"; fail=1; fi; }
chk "A路由 用A的key"  200 "$(code 19580 "$KA")"
chk "B路由 用B的key"  200 "$(code 19581 "$KB")"
chk "A路由 用B的key"  401 "$(code 19580 "$KB")"
chk "B路由 用A的key"  401 "$(code 19581 "$KA")"

# ⚠️ 这一段【没有牙】,如实记下来:flush 式播种在 600 次并发交叉下也全绿。
# 那个窗口(某 worker 查完 __sig、还没查 key 时,另一个 worker flush_all)确实存在,
# 但太窄,这个规模的压不出来。留着它是为了守住**跨路由隔离**(上面四条),
# 至于窗口本身,是靠"不再 flush"从结构上消掉的,不是靠这条测试证伪的。
#
# 并发交叉打:
# 必须 worker_processes > 1 且两个路由**同时**在打。单 worker 串行请求时,
# 每个请求都会在查表前把自己那份重灌一遍,flush 式播种也能全绿(踩过:
# 第一版就是单 worker 串行,flush 版照样 ALL PASS,等于没测到)。
N=${N:-300}
: > "$PREFIX/a.out"; : > "$PREFIX/b.out"
( for i in $(seq 1 $N); do code 19580 "$KA" >> "$PREFIX/a.out"; echo >> "$PREFIX/a.out"; done ) &
pa=$!
( for i in $(seq 1 $N); do code 19581 "$KB" >> "$PREFIX/b.out"; echo >> "$PREFIX/b.out"; done ) &
pb=$!
wait $pa $pb
bad=$(cat "$PREFIX/a.out" "$PREFIX/b.out" | grep -c -v '^200$')
got=$(cat "$PREFIX/a.out" "$PREFIX/b.out" | grep -c '^[0-9]')
[ "$got" -eq $((N*2)) ] || { echo "  只收到 $got/$((N*2)) 个结果,本次无效"; exit 2; }
chk "并发交叉 $((N*2)) 次请求,假 401 次数" 0 "$bad"
echo; [ $fail -eq 0 ] && echo "test_api_keys_multiroute: ALL PASS" || { echo "test_api_keys_multiroute: FAIL"; exit 1; }
