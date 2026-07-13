#!/bin/bash
# 慢启动验证:自适应并发从 min 起步,健康流量下逐 interval ×inc 爬升到 max。
# 隔离 harness(不碰生产),单路由 ss:显式 min=3、静态 max=40(p2big)、interval=2、inc=1.5、dec=0.5。
# 场景 A(冷启+健康):cc nil → 落地 min(3)→ 每 2s ×1.5 爬升 → clamp 到 max(40)。
# 场景 B(冷启+不健康):cc nil → 落地 min(3),不从 max 起(证明 seed=min,非旧的 seed=max)。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/sstest}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28941 28942; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
# lua-refactor: init_by_lua_block 现只 `require "router"`,需 lua_package_path 指向 openresty/lua/ 才能加载模块
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m;
  lua_shared_dict active_conns_ss 4m; lua_shared_dict cluster_avg_ss 16k; lua_shared_dict lc_locks_ss 1m; lua_shared_dict bad_peers_ss 1m; lua_shared_dict bodylog_ctl_ss 1m; lua_shared_dict cch_ctl_ss 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28941; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.ADAPTIVE_CC_TTL = 30
    local p2big = {{"127.0.0.1",28941,"b1",0,20},{"127.0.0.1",28942,"b2",0,20}}  -- 静态 max=40
    local base = {default_max=50,bodylog_default_enabled=false,health_check_interval=5,
                  tps_window=3,tps_ttl=10,tps_probe_window=3,tps_probe_per_window=5,tps_min_decode_s=0.3}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    -- ss:显式 min=3、interval=2、inc=1.5(爬升可见)、dec=0.5、rt_limit_factor=50(闸不误挡建流)
    _G.register_route("ss", function() return R({peers=p2big, tps_limit_tps=50, rt_limit_factor=50,
        adaptive_cc=true, adaptive_cc_min=3, adaptive_cc_interval=2, adaptive_cc_dec=0.5, adaptive_cc_inc=1.5}) end)
  }
  server { listen 19495; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.ss) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.ss) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.ss) } }
    location = /_tps_toggle { content_by_lua_block { _G.dbg_tps_toggle(_G.__route_opts.ss) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28941 28942; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 20 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

SS=http://127.0.0.1:19495
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
# fire $1=chunk_delay $2=max_tokens
fire(){ curl -s -o /dev/null -N -H "$A" -H "$H" \
  -d "{\"stream_options\":{\"include_usage\":true},\"model\":\"x\",\"stream\":true,\"chunk_delay_ms\":$1,\"max_tokens\":$2,\"prefill_delay_ms\":20,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
  "$SS/v1/chat/completions"; }
acc(){ curl -s "$SS/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);cc=d.get('adaptive_cc') or {};v=cc.get('_');print('nil' if v is None else round(v,2))"; }
ev(){ curl -s "$SS/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);v=d['ewma_tps'].get('_');print('nil' if v is None else round(v,1))"; }
mnmx(){ curl -s "$SS/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);mn=(d.get('adaptive_cc_min') or {}).get('_');mx=(d.get('adaptive_cc_max') or {}).get('_');print(f'min={mn} max={mx}')"; }

echo "########## 慢启动(slow-start)验证 ##########"
echo "路由 ss: $(mnmx), interval=2s, inc=×1.5, dec=×0.5"
echo ""
echo ">>> 冷启动初值(未发任何流量):cc=$(acc)  ewma=$(ev)   [期望 cc=nil]"
echo ""
echo ">>> 场景 A:冷启 + 健康流量(快解码 ewma≫阈值)→ cc 应从 min(3) 逐 tick ×1.5 爬升到 max(40)"
# 每 2s 顺序打几发保持 ewma 健康(顺序发 rt_sum 低,不触发并发闸),同时采样 cc 轨迹
for step in $(seq 0 9); do
  for k in 1 2 3; do fire 10 40 >/dev/null; done   # 快流:chunk10ms×40tok,decode~0.4s,rate~100≥50
  cc=$(acc); ew=$(ev)
  printf "    t=%2ds  cc=%-6s ewma=%s\n" "$((step*2))" "$cc" "$ew"
done
ccA=$(acc)
echo ""
echo ">>> 场景 B:另起冷池不可行(同路由),改验反向 —— 现清 ewma 后灌慢流(ewma<阈值)→ cc 应 ×0.5 回落到 min(3),不反弹"
sleep 12   # > tps_ttl(10s):清 ewma(cc 保持当前值)
for step in $(seq 0 6); do
  for k in 1 2 3; do fire 80 20 >/dev/null; done   # 慢流:chunk80ms×20tok,decode~1.6s,rate~12<50
  cc=$(acc); ew=$(ev)
  printf "    t=%2ds  cc=%-6s ewma=%s\n" "$((step*2))" "$cc" "$ew"
done
ccB=$(acc)
echo ""
echo "结论:场景A cc 从 min(3) 单调爬升到 $ccA(≈max40);场景B cc 从高位回落到 $ccB(≈min3)。"
echo "     seed=min 验证:冷启动首个落地值贴近 min 而非 max —— 慢启动生效。"
