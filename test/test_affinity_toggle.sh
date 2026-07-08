#!/bin/bash
# session 亲和性开关(_G.SESSION_AFFINITY_ENABLED + opts.session_affinity_enabled)隔离验证。
# 抽取被测 ENGINE 的 init_by_lua_block,自建高位端口 openresty + 本地 mock,查 /_route_inspect 的
# source/mode,不碰生产。覆盖:
#   ① 显式 true(=默认全局开)+ sid            → source=header.x-session-id, mode=hash
#   ② 显式 false                     + sid    → source=affinity_off,        mode=least_conn
#   ③ 全局关(init_worker 置 false)+ 不设 flag + sid → 继承关 → affinity_off/least_conn(验全局开关+回落)
#   ④ 显式 true(覆盖全局关)         + sid     → source=header.x-session-id, mode=hash(验每路由覆盖全局)
#   ⑤ 无 sid(任意路由)                        → source=none,                mode=least_conn(不变)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/afftest}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  pkill -9 -f "$PREFIX/nginx" 2>/dev/null
  for p in 28941 28942; do pkill -9 -f "mock_vllm_sse.py --port $p" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m;
  lua_shared_dict active_conns_rdef 4m; lua_shared_dict cluster_avg_rdef 16k; lua_shared_dict lc_locks_rdef 1m; lua_shared_dict bad_peers_rdef 1m; lua_shared_dict bodylog_ctl_rdef 1m; lua_shared_dict cch_ctl_rdef 1m;
  lua_shared_dict active_conns_ron 4m;  lua_shared_dict cluster_avg_ron 16k;  lua_shared_dict lc_locks_ron 1m;  lua_shared_dict bad_peers_ron 1m;  lua_shared_dict bodylog_ctl_ron 1m;  lua_shared_dict cch_ctl_ron 1m;
  lua_shared_dict active_conns_roff 4m; lua_shared_dict cluster_avg_roff 16k; lua_shared_dict lc_locks_roff 1m; lua_shared_dict bad_peers_roff 1m; lua_shared_dict bodylog_ctl_roff 1m; lua_shared_dict cch_ctl_roff 1m;
  lua_shared_dict active_conns_rinh 4m; lua_shared_dict cluster_avg_rinh 16k; lua_shared_dict lc_locks_rinh 1m; lua_shared_dict bad_peers_rinh 1m; lua_shared_dict bodylog_ctl_rinh 1m; lua_shared_dict cch_ctl_rinh 1m;
  lua_shared_dict active_conns_rfon 4m; lua_shared_dict cluster_avg_rfon 16k; lua_shared_dict lc_locks_rfon 1m; lua_shared_dict bad_peers_rfon 1m; lua_shared_dict bodylog_ctl_rfon 1m; lua_shared_dict cch_ctl_rfon 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.TTFT_BUCKETS_MS = {50,100,200,300,500,800,1200}
    local PEERS = {{"127.0.0.1",28941,"m1"},{"127.0.0.1",28942,"m2"}}
    -- ⓪ rdef:不设 flag,注册在任何覆盖之前 → 继承 conf init_by_lua 的出厂默认(现为 false)
    _G.register_route("rdef", function() return { peers=PEERS, bodylog_default_enabled=false, health_check_interval=9999 } end)
    -- ① ron:显式开
    _G.register_route("ron",  function() return { peers=PEERS, session_affinity_enabled=true,  bodylog_default_enabled=false, health_check_interval=9999 } end)
    -- ② roff:显式关
    _G.register_route("roff", function() return { peers=PEERS, session_affinity_enabled=false, bodylog_default_enabled=false, health_check_interval=9999 } end)
    -- 现在把全局关掉,再注册 ③④ 验「全局开关 + 每路由回落/覆盖」
    _G.SESSION_AFFINITY_ENABLED = false
    -- ③ rinh:不设 flag → 回落全局(现为 false)→ 关
    _G.register_route("rinh", function() return { peers=PEERS, bodylog_default_enabled=false, health_check_interval=9999 } end)
    -- ④ rfon:显式开,覆盖全局关 → 仍开
    _G.register_route("rfon", function() return { peers=PEERS, session_affinity_enabled=true,  bodylog_default_enabled=false, health_check_interval=9999 } end)
  }
  server { listen 19690; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /rdef/inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.rdef) } }
    location = /ron/inspect  { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.ron)  } }
    location = /roff/inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.roff) } }
    location = /rinh/inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.rinh) } }
    location = /rfon/inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.rfon) } }
    location = /ron/debug    { content_by_lua_block { _G.dbg_route_debug(_G.__route_opts.ron)  } }
    location = /rdef/debug   { content_by_lua_block { _G.dbg_route_debug(_G.__route_opts.rdef) } } }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "=== openresty -t FAILED ==="; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28941 28942; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 10 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
A="Authorization: Bearer $KEY"; H="Content-Type: application/json"; U=http://127.0.0.1:19690
BODY='{"model":"m","messages":[{"role":"user","content":"hi"}]}'
# insp <route> <sid|-> → 打印 "source|mode"(避开老 bash set -u 空数组展开)
insp(){ local r=$1 sid=$2 out
  if [ "$sid" != "-" ]; then out=$(curl -s -H "$A" -H "$H" -H "x-session-id: $sid" -d "$BODY" "$U/$r/inspect")
  else                        out=$(curl -s -H "$A" -H "$H"                          -d "$BODY" "$U/$r/inspect"); fi
  echo "$out" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('source'),'|',d.get('mode'))"; }
chk(){ local desc=$1 route=$2 sid=$3 want=$4; local got; got=$(insp "$route" "$sid")
  if [ "$got" = "$want" ]; then ok "$desc  ($route sid=$sid → $got)"; else no "$desc  ($route sid=$sid → got[$got] want[$want])"; fi; }
# dbg <route> <sid> → 打印 "natural有值|actual_mode|affinity字段"(验 _route_debug 的 natural-vs-actual 分叉)
dbg(){ local r=$1 sid=$2; curl -s "$U/$r/debug?sid=$sid" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(('nat' if d.get('natural_target') else 'nonat'),'|',d.get('actual_mode'),'|',d.get('session_affinity_enabled'))"; }
chkd(){ local desc=$1 route=$2 sid=$3 want=$4; local got; got=$(dbg "$route" "$sid")
  if [ "$got" = "$want" ]; then ok "$desc  ($route → $got)"; else no "$desc  ($route → got[$got] want[$want])"; fi; }

echo "########## session 亲和性开关 ##########"
chk "⓪ 不设 flag(继承出厂默认=关)+ sid → least_conn" rdef s0 "affinity_off | least_conn"
chk "① 显式 true + sid → hash"                       ron  s1 "header.x-session-id | hash"
chk "② 显式 false + sid → affinity_off/least_conn"   roff s2 "affinity_off | least_conn"
chk "③ 全局关+不设 flag + sid → 继承关"               rinh s3 "affinity_off | least_conn"
chk "④ 显式 true 覆盖全局关 + sid → hash"             rfon s4 "header.x-session-id | hash"
chk "⑤ 无 sid(开的路由)→ none/least_conn 不变"        ron  -  "none | least_conn"
chk "⑤ 无 sid(关的路由)→ none/least_conn 不变"        roff -  "none | least_conn"
echo "########## _route_debug natural-vs-actual 分叉 ##########"
chkd "⑥ 开:natural 有值 + actual=hash + affinity=True"          ron  z1 "nat | hash | True"
chkd "⑦ 关:natural 仍有值(哈希数学)+ actual=least_conn + affinity=False" rdef z2 "nat | least_conn | False"

echo "==== 结果: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
