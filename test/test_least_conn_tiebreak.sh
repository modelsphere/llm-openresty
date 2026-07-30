#!/bin/bash
# least_conn 平局随机化专项测试(route.lua pick_from 蓄水池抽样 size=1)。
# 抽取被测 ENGINE 的 init_by_lua_block,自建高位端口 openresty:1 个路由、4 个【同优先级等容量】
# peer → 零负载时全部并列。仅用 /_route_inspect 预览 pick(不代理后端、无需 mock;health 关掉,
# 未监听的 peer 也算健康)。覆盖:
#   T0 自检:active_level 上有 4 个健康 peer(否则无平局可测)
#   T1 唯一最小(3 个 peer active=5、留 1 个=0)→ 20/20 确定选最空的
#      —— 验证平局随机【不破坏】正常 least_conn 语义(有唯一最小时仍确定)
#   T2 全并列(flush 全 0)→ 80 次预览散到 ≥3/4 个 peer
#      —— 旧代码严格 `<` 会恒选 healthy_peers[1] → distinct 恒=1;修复后应均匀散开
#   T3 (info)打印分布计数(均匀性受随机波动,不做硬断言)
# 用法:ENGINE=<session_route.conf> bash test_least_conn_tiebreak.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/lctest}"; KEY=REDACTED-API-KEY; U="http://127.0.0.1:19692"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  pkill -9 -f "$PREFIX/nginx" 2>/dev/null; rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
# init_by_lua_block 现只 `require "router"`,需 lua_package_path 指向 openresty/lua/
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"
cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m;
  lua_shared_dict active_conns_lc 4m; lua_shared_dict cluster_avg_lc 16k; lua_shared_dict lc_locks_lc 1m; lua_shared_dict bad_peers_lc 1m; lua_shared_dict bodylog_ctl_lc 1m; lua_shared_dict cch_ctl_lc 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.TTFT_BUCKETS_MS = {50,100,200,300,500,800,1200}
    -- 4 个同优先级(默认 prio 0)等容量 peer → 同处 active_level,零负载全并列。
    local PEERS = {{"127.0.0.1",28951,"p1"},{"127.0.0.1",28952,"p2"},{"127.0.0.1",28953,"p3"},{"127.0.0.1",28954,"p4"}}
    _G.register_route("lc", function() return { peers=PEERS, session_affinity_enabled=false, bodylog_default_enabled=false, health_check_interval=9999 } end)
  }
  server { listen 19692; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /lc/inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.lc) } }
    location = /lc/set     { content_by_lua_block { _G.dbg_active_conns_set(_G.__route_opts.lc) } }
    location = /lc/state   { content_by_lua_block { _G.dbg_route_state(_G.__route_opts.lc) } }
  }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAIL"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 1
A="Authorization: Bearer $KEY"; H="Content-Type: application/json"
PASS=0; FAIL=0
ok(){ echo "  ✓ $*"; PASS=$((PASS+1)); }
no(){ echo "  ✗ FAIL: $*"; FAIL=$((FAIL+1)); }
pick(){ curl -s -X POST "$U/lc/inspect" -H "$A" -H "$H" -d '{}' | "$PY" -c "import sys,json;print(json.load(sys.stdin).get('pick',''))" 2>/dev/null; }
setc(){ curl -s "$U/lc/set?peer=$1&value=$2" -H "$A" >/dev/null; }
flush(){ curl -s "$U/lc/set?flush=1" -H "$A" >/dev/null; }

echo "###### T0. 自检:active_level 上有 4 个健康 peer(否则无平局可测)######"
hp=$(curl -s "$U/lc/state" -H "$A" | "$PY" -c "import sys,json;print(json.load(sys.stdin).get('healthy_peers_in_level',0))" 2>/dev/null)
[ "$hp" = "4" ] && ok "T0 healthy_peers_in_level=4" || no "T0 healthy_peers_in_level=$hp (期望 4)"

echo "###### T1. 唯一最小 → 20/20 确定选最空(平局随机不破坏正常 least_conn)######"
flush; setc 127.0.0.1:28952 5; setc 127.0.0.1:28953 5; setc 127.0.0.1:28954 5   # 仅 p1(28951)=0 → 唯一最小
t1out=$(for i in $(seq 1 20); do pick; done)
t1d=$(echo "$t1out" | sort -u | grep -c .); t1v=$(echo "$t1out" | sort -u | head -1)
{ [ "$t1d" = "1" ] && echo "$t1v" | grep -qE "28951|^p1$"; } \
  && ok "T1 20/20 确定选唯一最小 ($t1v)" || no "T1 distinct=$t1d val=$t1v (期望 distinct=1 且=最空 p1/28951)"

echo "###### T2. 全并列(flush) → 80 次预览散到 ≥3/4 个 peer(旧代码 distinct 恒=1)######"
flush
picks=$(for i in $(seq 1 80); do pick; done)
distinct=$(echo "$picks" | sort -u | grep -c .)
echo "  分布: $(echo "$picks" | sort | uniq -c | tr '\n' ' ')"
[ "$distinct" -ge 3 ] && ok "T2 80 次散到 $distinct/4 个 peer (≥3)" || no "T2 只散到 $distinct 个 peer (旧 bug=1;期望 ≥3)"

echo "###### T3. (info)分布见 T2 上方计数;均匀性受随机波动,不做硬断言 ######"

echo "================ least_conn 平局随机化: PASS=$PASS FAIL=$FAIL ================"
[ "$FAIL" -eq 0 ]
