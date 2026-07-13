#!/bin/bash
# adaptive_cc 端到端场景验证(隔离 openresty+mock,自清理)。复现今天 canary 发现的问题 + 大流量回归。
# S1 小流量突发(canary场景):min 低、突发超容量 → 验 cc 从 min 解锁爬起、429 率随 cc 上升而下降。
# S2 大流量持续(回归):高稳态并发+healthy → cc 跟随爬到能服务、稳定、稳态 429 低。
# S3 过载保护(回归):慢解码(tps<阈值)→ cc 收缩(ewma<thr ×dec),不往过载后端里灌。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/scntest}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28961 28962; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
events { worker_connections 4096; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
  lua_shared_dict active_conns_r 8m; lua_shared_dict cluster_avg_r 16k; lua_shared_dict lc_locks_r 1m; lua_shared_dict bad_peers_r 1m; lua_shared_dict bodylog_ctl_r 1m; lua_shared_dict cch_ctl_r 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.ADAPTIVE_CC_TTL = 40
    -- canary-like:min=5,阈值 tps_limit=30,ABS=5;per-peer max=200 → 静态max=400(不封顶);
    -- 加速收敛:interval=2s,inc=1.5,dec=0.7(仍是"减快增慢"方向,只是比生产 1.01/0.97 快,便于测)
    local peers = {{"127.0.0.1",28961,"m1",0,200},{"127.0.0.1",28962,"m2",0,200}}
    _G.register_route("r", function() return {peers=peers, tps_limit_tps=30, rt_limit_factor=1,
      adaptive_cc=true, adaptive_cc_min=5, adaptive_cc_abs=5, adaptive_cc_interval=2,
      adaptive_cc_inc=1.5, adaptive_cc_dec=0.7,
      default_max=400, bodylog_default_enabled=false, health_check_interval=9999,
      tps_window=3, tps_ttl=10, tps_probe_window=3, tps_probe_per_window=5, tps_min_decode_s=0.3} end)
  }
  server { listen 19690; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.r) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.r) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.r) } } }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
# mock:快解码(chunk-delay 由请求体传);output-len 80
for p in 28961 28962; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 80 --chunk-delay-ms 5 --prefill-delay-ms 10 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"; R=http://127.0.0.1:19690
# fire url chunk_delay max_tokens
fire(){ curl -s -o /dev/null -w "%{http_code}\n" -N -H "$A" -H "$H" -d "{\"stream_options\":{\"include_usage\":true},\"model\":\"x\",\"stream\":true,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"prefill_delay_ms\":10,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" "$1/v1/chat/completions"; }
cc(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);v=d.get('adaptive_cc',{});print(round(v.get('_'),1) if isinstance(v,dict) and v.get('_') is not None else '')"; }
ev(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);v=d.get('ewma_tps',{});print(round(v.get('_'),1) if isinstance(v,dict) and v.get('_') is not None else '')"; }
# hammer url n cd mx → 同时发 n 个,统计 "200=X 429=Y pct429=Z"
hammer(){ local u=$1 n=$2 cd=$3 mx=$4 d="$PREFIX/h"; rm -f ${d}_*; for i in $(seq 1 $n); do ( fire "$u" "$cd" "$mx" ) >${d}_$i 2>/dev/null & done; wait
  local c200=$(cat ${d}_* | grep -c 200); local c429=$(cat ${d}_* | grep -c 429); local tot=$((c200+c429))
  echo "200=$c200 429=$c429 pct429=$([ $tot -gt 0 ] && echo $((c429*100/tot)) || echo 0)"; }
# 后台持续压 n 并发 dur 秒(cd 控制单请求时长/解码速率)
load_bg(){ local u=$1 n=$2 cd=$3 mx=$4 dur=$5; local endt=$((SECONDS+dur)); while [ "$SECONDS" -lt "$endt" ]; do for i in $(seq 1 $n); do fire "$u" "$cd" "$mx" >/dev/null 2>&1 & done; sleep 0.5; done; wait; }
expire(){ sleep "${1:-11}"; }   # > tps_ttl(10s):清 ewma(cc 由 CC_TTL=40 保持)

echo "########## S1 小流量突发(canary 场景:min=5,突发超容量)##########"
echo "  初始 cc=$(cc $R)"
# 第1波:突发 30 并发(远超初始 cc≈5),快解码 → 大量 429(cc 还没爬起来)
w1=$(hammer $R 30 6 60); echo "  第1波(cc≈$(cc $R)前): $w1"
p1=$(echo $w1 | sed 's/.*pct429=//')
# 持续突发压 12s 让 cc 爬(abs 抬底 + rej 快涨信号)
load_bg $R 20 6 60 12
cc2=$(cc $R); ev2=$(ev $R); echo "  持续压后 cc=$cc2 ewma=$ev2"
# 第2波:同样 30 并发 → cc 已爬高 → 429 大幅减少
w2=$(hammer $R 30 6 60); echo "  第2波(cc≈$(cc $R)后): $w2"
p2=$(echo $w2 | sed 's/.*pct429=//')
awk "BEGIN{exit !($cc2>=10)}" && ok "S1a cc 从 min=5 解锁爬起 (cc=$cc2 ≥10)" || no "S1a cc=$cc2 没爬起来(卡 min)"
[ "${p2:-100}" -lt "${p1:-0}" ] && ok "S1b 429率随 cc 上升而下降 (第1波${p1}% → 第2波${p2}%)" || no "S1b 429率没降(${p1}%→${p2}%)"

echo "########## S2 大流量持续(回归:高稳态并发)##########"
load_bg $R 60 6 60 14
cc3=$(cc $R); ev3=$(ev $R); w3=$(hammer $R 40 6 60)
p3=$(echo $w3 | sed 's/.*pct429=//')
echo "  高并发稳态 cc=$cc3 ewma=$ev3  稳态波: $w3"
awk "BEGIN{exit !($cc3>=30)}" && ok "S2a cc 跟随高并发爬到能服务 (cc=$cc3 ≥30)" || no "S2a cc=$cc3 没跟上大流量"
awk "BEGIN{exit !($cc3<=400)}" && ok "S2b cc 不 runaway 超 maxcc (cc=$cc3 ≤400)" || no "S2b cc=$cc3 越界"
[ "${p3:-100}" -lt 35 ] && ok "S2c 稳态 429率低 (${p3}%)" || no "S2c 稳态 429率偏高 (${p3}%)"

echo "########## S3 过载保护(回归:慢解码 tps<阈值 → 缩)##########"
expire 11   # 清 S2 建的高 ewma(168),让慢流从低重建 → 直接 <30 触发收缩(免等 EWMA 慢衰减)
cc_before=$(cc $R); echo "  清 ewma 后 cc=$cc_before(保持)"
# 慢解码:chunk_delay=80,max_tokens=30 → 解码≈2.4s,tps≈12 (<30 阈值) → ewma 建低 → cc ×dec 缩
load_bg $R 20 80 30 14
cc4=$(cc $R); ev4=$(ev $R); echo "  慢解码后 cc=$cc4 ewma=$ev4 (ewma 应<30,cc 应缩)"
awk "BEGIN{exit !(${ev4:-99}<30)}" && ok "S3a 慢流 ewma 跌破阈值 (ewma=$ev4 <30)" || no "S3a ewma=$ev4 没跌破(慢流不够慢/没建起来)"
awk "BEGIN{exit !($cc4<$cc_before)}" && ok "S3b 过载 cc 收缩保护 (cc $cc_before→$cc4)" || no "S3b cc 没缩(过载保护失效 $cc_before→$cc4)"

echo "==== 结果: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
