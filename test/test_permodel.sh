#!/bin/bash
# per-model 在线 override 补测:验证 /_tps_limit?tps=N&model=X 与 /_ttft_limit?ms=N&model=X
# 在 peers_by_model 路由上:① 只改指定模型不动别的 ② 盖住 by_model(override>by_model)③ 清除回落。
# 隔离 openresty + mock,自清理,不碰生产。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/pmtest}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  pkill -9 -f "$PREFIX/nginx" 2>/dev/null
  for p in 28931 28932 28933; do pkill -9 -f "mock_vllm_sse.py --port $p" 2>/dev/null; done
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
  lua_shared_dict active_conns_pm 4m; lua_shared_dict cluster_avg_pm 16k; lua_shared_dict lc_locks_pm 1m; lua_shared_dict bad_peers_pm 1m; lua_shared_dict bodylog_ctl_pm 1m; lua_shared_dict cch_ctl_pm 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28931; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.TTFT_BUCKETS_MS = {50,100,200,300,500,800,1200}
    _G.register_route("pm", function() return {
      peers_by_model={["kimi-k2.6"]={{"127.0.0.1",28931,"k1"},{"127.0.0.1",28932,"k2"}},["glm-5.1-fp8"]={{"127.0.0.1",28933,"g1"}}},
      default_max=50, bodylog_default_enabled=false, health_check_interval=9999,
      tps_limit_tps=30, tps_limit_by_model={["kimi-k2.6"]=50,["glm-5.1-fp8"]=10},
      tps_window=3, tps_ttl=8, tps_probe_window=3, tps_probe_per_window=5, tps_min_decode_s=0.3,
      ttft_limit_ms=1000, ttft_limit_by_model={["kimi-k2.6"]=400,["glm-5.1-fp8"]=1500},
      ttft_window=3, ttft_ttl=8, ttft_probe_window=3, ttft_probe_per_window=5 } end)
  }
  server { listen 19590; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.pm) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.pm) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.pm) } }
    location = /_tps_limit  { content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts.pm) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.pm) } }
    location = /_ttft_limit  { content_by_lua_block { _G.dbg_ttft_limit(_G.__route_opts.pm) } } }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28931 28932 28933; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 30 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
A="Authorization: Bearer $KEY"; H="Content-Type: application/json"; U=http://127.0.0.1:19590
# fire $1=model $2=chunk_delay $3=max $4=prefill  (TPS 用 chunk_delay/max;TTFT 用 prefill)
fire(){ curl -s -o /dev/null -H "$A" -H "$H" -d "{\"model\":\"$1\",\"stream\":true,\"stream_options\":{\"include_usage\":true},\"chunk_delay_ms\":${2:-50},\"max_tokens\":${3:-20},\"prefill_delay_ms\":${4:-30},\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" "$U/v1/chat/completions"; }
firec(){ curl -s -o /dev/null -w "%{http_code}" -H "$A" -H "$H" -d "{\"model\":\"$1\",\"stream\":true,\"stream_options\":{\"include_usage\":true},\"chunk_delay_ms\":${2:-50},\"max_tokens\":${3:-20},\"prefill_delay_ms\":${4:-30},\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" "$U/v1/chat/completions"; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
estab(){ align; for i in 1 2 3 4 5 6; do ( fire "$@" ) & done; wait; sleep 4; fire "$@"; sleep 1; }
burst(){ local m=$1; shift; local d="$PREFIX/b"; rm -f ${d}_*; for i in $(seq 1 8); do ( firec "$m" "$@"; echo ) >${d}_$i & done; wait; cat ${d}_*|sort|uniq -c|tr '\n' ' '; }
expire(){ sleep 10; }
tpv(){ curl -s "$U/_tps_status"|python3 -c "import sys,json;print(json.load(sys.stdin)['ewma_tps'].get('$1',''))"; }
ttv(){ curl -s "$U/_ttft_status"|python3 -c "import sys,json;print(json.load(sys.stdin)['ewma_ms'].get('$1',''))"; }

echo "########## TPS per-model 在线 override(override > by_model)##########"
# kimi/glm 同窗建 ewma(桶40);kimi by_model=50 → 限;glm by_model=10 → 不限
expire; align
for i in 1 2 3 4; do ( fire kimi-k2.6 50 20 >/dev/null ) & ( fire glm-5.1-fp8 50 20 >/dev/null ) & done; wait
sleep 4; fire kimi-k2.6 50 20 >/dev/null; fire glm-5.1-fp8 50 20 >/dev/null; sleep 1
ek=$(tpv kimi-k2.6); eg=$(tpv glm-5.1-fp8)
rk=$(burst kimi-k2.6 50 20); rg=$(burst glm-5.1-fp8 50 20)
{ echo "$rk"|grep -q 429 && ! echo "$rg"|grep -q 429; } && ok "PM1 by_model:kimi(ewma=$ek,floor50)限 / glm(ewma=$eg,floor10)不限" || no "PM1 kimi=[$rk] glm=[$eg:$rg]"
# 设 kimi override=30(< ewma40)→ kimi 不再限(override 盖住 by_model 50);glm 不受影响
curl -s "$U/_tps_limit?tps=30&model=kimi-k2.6" >/dev/null
ov=$(curl -s "$U/_tps_limit?model=kimi-k2.6"|python3 -c "import sys,json;print(json.load(sys.stdin).get('limit_override_tps'))")
rk2=$(burst kimi-k2.6 50 20); rg2=$(burst glm-5.1-fp8 50 20)
{ [ "$ov" = "30" ] && ! echo "$rk2"|grep -q 429 && ! echo "$rg2"|grep -q 429; } && ok "PM2 override kimi=30(盖住by_model50)→ kimi 不限[$rk2] / glm 仍不限[$rg2]" || no "PM2 ov=$ov kimi=[$rk2] glm=[$rg2]"
# 清 kimi override → 回落 by_model 50 → kimi 又限
curl -s "$U/_tps_limit?tps=0&model=kimi-k2.6" >/dev/null
ov3=$(curl -s "$U/_tps_limit?model=kimi-k2.6"|python3 -c "import sys,json;print(json.load(sys.stdin).get('limit_override_tps'))")
rk3=$(burst kimi-k2.6 50 20)
{ [ "$ov3" = "None" ] && echo "$rk3"|grep -q 429; } && ok "PM3 清 override → 回落 by_model 50 → kimi 又限[$rk3]" || no "PM3 ov=$ov3 kimi=[$rk3]"

echo "########## TTFT per-model 在线 override ##########"
# kimi ttft ewma 建高(prefill=800→桶1200);kimi by_model=400 → 限
expire; align
for i in 1 2 3 4; do ( fire kimi-k2.6 1 5 800 >/dev/null ) & done; wait; sleep 4; fire kimi-k2.6 1 5 800 >/dev/null; sleep 1
tk=$(ttv kimi-k2.6)
rk4=$(burst kimi-k2.6 1 5 800)
echo "$rk4"|grep -q 429 && ok "TM1 by_model:kimi ttft(ewma=$tk,limit400)→ 限[$rk4]" || no "TM1 tk=$tk [$rk4]"
# 设 kimi override=2000(> ewma1200)→ 不限
curl -s "$U/_ttft_limit?ms=2000&model=kimi-k2.6" >/dev/null
tov=$(curl -s "$U/_ttft_limit?model=kimi-k2.6"|python3 -c "import sys,json;print(json.load(sys.stdin).get('limit_override_ms'))")
rk5=$(burst kimi-k2.6 1 5 800)
{ [ "$tov" = "2000" ] && ! echo "$rk5"|grep -q 429; } && ok "TM2 override kimi=2000(>ewma1200,盖住by_model400)→ 不限[$rk5]" || no "TM2 ov=$tov [$rk5]"
# 清除 → 回落 400 → 又限
curl -s "$U/_ttft_limit?ms=0&model=kimi-k2.6" >/dev/null
rk6=$(burst kimi-k2.6 1 5 800)
echo "$rk6"|grep -q 429 && ok "TM3 清 override → 回落 by_model 400 → 又限[$rk6]" || no "TM3 [$rk6]"

echo ""
echo "================ per-model override 套件: PASS=$P FAIL=$F ================"
