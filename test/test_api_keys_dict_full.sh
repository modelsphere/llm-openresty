#!/bin/bash
# dict 写满/条目被驱逐时,合法 key 不能被拒。
#
# api_keys dict 只是缓存(没有任何地方在运行时增删 key,权威是 opts.api_keys 这张
# Lua 表)。它满了以后 ngx.shared 会按 LRU 驱逐,若鉴权只认 dict,就会把**合法 key**
# 判成无效 —— 全站 401 风暴。这里把 dict 缩到最小并塞进大量 key 强制驱逐,
# 断言鉴权仍然正确。
#
# NO_FALLBACK=1 可去掉兜底重跑,用来确认这条测试抓得住(应 FAIL)。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/akfull}"
KEY="sk-real-key-0001"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  cp "$PREFIX/logs/error.log" /tmp/akfull-error.log 2>/dev/null; rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp" "$PREFIX/lua"
cp "${LUA_SRC:-$HERE/../lua}/"*.lua "$PREFIX/lua/"

if [ "${NO_FALLBACK:-}" = 1 ]; then
  python3 - "$PREFIX/lua/access.lua" <<'PYX'
import sys
p=sys.argv[1]; s=open(p).read()
s=s.replace('not (ak:get(sig .. ":" .. akey) or opts.api_keys[akey])', 'not ak:get(sig .. ":" .. akey)')
open(p,"w").write(s)
PYX
  echo "  (NO_FALLBACK=1:去掉回落到 Lua 表的兜底,期望 FAIL)"
fi

# 32k(8k 起不来:slab 分配失败)+ 2000 条 key,必然驱逐
cat > "$PREFIX/nginx.conf" <<NG
worker_processes 1; error_log logs/error.log crit; pid logs/nginx.pid;
events { worker_connections 256; }
http {
  lua_package_path '$PREFIX/lua/?.lua;;';
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict active_conns 4m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict cluster_avg 16k; lua_shared_dict lc_locks 1m; lua_shared_dict bad_peers 1m;
  lua_shared_dict api_keys 32k; lua_shared_dict bodylog_ctl 1m; lua_shared_dict cch_ctl 1m; lua_shared_dict reject_stat 128k;
  init_by_lua_block { require "router"; _G.RT_LIMIT_FACTOR = 2; _G.MAX_CONCURRENCY_PER_PEER = 100 }
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  server { listen 19592; location / { return 200 "backend-ok"; } }
  server { listen 19590; server_name _;
    set \$route "t"; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-";
    set_by_lua_block \$__init { _G.register_route("t", function()
        -- 一张塞得下真 key、但远超 8k dict 容量的表
        local keys = { ["$KEY"] = "real" }
        for i = 1, 2000 do keys["sk-filler-" .. i] = "filler-" .. i end
        return { peers = { {"127.0.0.1", 19592, "m", 0, 100} }, health_check = false, api_keys = keys,
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
code(){ curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" http://127.0.0.1:19590/v1/models; }
fail=0
chk(){ if [ "$2" = "$3" ]; then echo "  PASS $1 (期望$2 实际$3)"; else echo "  FAIL $1 (期望$2 实际$3)"; fail=1; fi; }
chk "dict 被塞爆后,合法 key 仍通过" 200 "$(code "$KEY")"
chk "非法 key 仍然拒"               401 "$(code sk-nope)"
# 连打确认不是碰巧命中缓存
bad=0; for i in $(seq 1 20); do [ "$(code "$KEY")" = 200 ] || bad=$((bad+1)); done
chk "连打 20 次,被误拒次数"          0 "$bad"
echo; [ $fail -eq 0 ] && echo "test_api_keys_dict_full: ALL PASS" || { echo "test_api_keys_dict_full: FAIL"; exit 1; }
