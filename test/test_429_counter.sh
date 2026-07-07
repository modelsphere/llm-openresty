#!/bin/bash
# /_429_status 计数器功能测试(隔离 scratch + mock,自清理,不碰生产)。
# 验证:① 触发并发 429 后 by_route/by_reason/total 累加正确;② route 标签正确;
#       ③ 未触发的 reason 保持 0;④ ?reset=1 清零(且非 127.0.0.1 拒绝——本测试都在本机,只验清零);
#       ⑤ reject_stat 未声明时 do_route 不崩(nil 守卫)——由 test_adaptive_cc.sh 覆盖,这里不重复。
# 触发并发 429:route 用 rt_limit_factor=1 + 极小 default_max,mock 慢响应(prefill-delay 长)占住并发,
#   并发发一批 → 超 limit 的直接 429(concurrency)。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/rj429test}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28941 28942; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
  lua_shared_dict reject_stat 128k;
  lua_shared_dict active_conns_rt 4m; lua_shared_dict cluster_avg_rt 16k; lua_shared_dict lc_locks_rt 1m; lua_shared_dict bad_peers_rt 1m; lua_shared_dict bodylog_ctl_rt 1m; lua_shared_dict cch_ctl_rt 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28941; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    -- rt route:紧并发闸(rt_limit_factor=1, per-peer max=1 → 静态 max=2),极易触发 concurrency 429
    local p2 = {{"127.0.0.1",28941,"m1",0,1},{"127.0.0.1",28942,"m2",0,1}}
    local base = {default_max=2,bodylog_default_enabled=false,health_check_interval=5}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("rt", function() return R({peers=p2, rt_limit_factor=1}) end)
  }
  server { listen 19590; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.rt) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.rt) } }
    location = /_429_status { content_by_lua_block { _G.dbg_429_status(_G.__route_opts.rt) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
# mock:prefill-delay 长(占住并发),output 小
for p in 28941 28942; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 10 --chunk-delay-ms 5 --prefill-delay-ms 2000 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
U=http://127.0.0.1:19590
jqget(){ "$PY" -c "import sys,json;d=json.load(sys.stdin);print(eval(sys.argv[1]))" "$1"; }

echo "############ /_429_status 计数器测试 ############"
# T0: 初始 reset,基线为空
curl -s "$U/_429_status?reset=1" >/dev/null
BASE=$(curl -s "$U/_429_status")
echo "  baseline: $BASE"
[ "$(echo "$BASE" | jqget "d['total']")" = "0" ] && ok "T0 reset 后 total=0" || no "T0 total 非 0"

# T1: 并发打 20 个(远超 max=2),mock 每个占 2s prefill → 超限的直接 concurrency 429
for i in $(seq 1 20); do
  curl -s -o /dev/null -X POST "$U/v1/chat/completions" -H "$A" -H "$H" \
    -d '{"model":"x","messages":[{"role":"user","content":"hi"}],"max_tokens":5}' &
done
sleep 4; wait
S1=$(curl -s "$U/_429_status")
echo "  after burst: $S1"
TOT=$(echo "$S1" | jqget "d['total']")
CONC=$(echo "$S1" | jqget "d['by_reason']['concurrency']")
RT_CONC=$(echo "$S1" | jqget "d['by_route'].get('rt',{}).get('concurrency',0)")
TTFT=$(echo "$S1" | jqget "d['by_reason']['ttft']")
TPS=$(echo "$S1" | jqget "d['by_reason']['tps']")
[ "$TOT" -gt 0 ] 2>/dev/null && ok "T1 total>0 (=$TOT)" || no "T1 total 未累加 (=$TOT)"
[ "$CONC" -gt 0 ] 2>/dev/null && ok "T2 by_reason.concurrency>0 (=$CONC)" || no "T2 concurrency 未计 (=$CONC)"
[ "$RT_CONC" = "$CONC" ] && ok "T3 by_route.rt.concurrency == by_reason.concurrency (route 标签正确)" || no "T3 route 标签错 (rt=$RT_CONC reason=$CONC)"
[ "$TTFT" = "0" ] && [ "$TPS" = "0" ] && ok "T4 未触发的 ttft/tps 保持 0" || no "T4 ttft=$TTFT tps=$TPS 应为 0"
[ "$TOT" = "$CONC" ] && ok "T5 total == concurrency(本测试只触发这一类)" || no "T5 total=$TOT != conc=$CONC"

# T6: reset 清零
curl -s "$U/_429_status?reset=1" >/dev/null
S2=$(curl -s "$U/_429_status")
[ "$(echo "$S2" | jqget "d['total']")" = "0" ] && ok "T6 reset 后归零" || no "T6 reset 未清零: $S2"

echo
echo "================ /_429_status: PASS=$P FAIL=$F ================"
[ "$F" = "0" ] && exit 0 || exit 1
