#!/bin/bash
# adaptive_cc 绝对头寸(ABS)+ 429-as-pressure(修复1)隔离验证。自建 openresty + mock,自清理。
# A/B 对照(同低并发):ab5(ABS=5)cc 应抬到 ~conc+5;ab0(ABS=0)退回旧的 ~conc/mid(1.25×)。
# 证明:① 小并发下绝对头寸把 cc 抬离 1.25×conc;② 缩不破 conc+ABS 绝对底;③ 429 计入 rej 信号。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/abstest}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28951 28952; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
  lua_shared_dict active_conns_ab5 4m; lua_shared_dict cluster_avg_ab5 16k; lua_shared_dict lc_locks_ab5 1m; lua_shared_dict bad_peers_ab5 1m; lua_shared_dict bodylog_ctl_ab5 1m; lua_shared_dict cch_ctl_ab5 1m;
  lua_shared_dict active_conns_ab0 4m; lua_shared_dict cluster_avg_ab0 16k; lua_shared_dict lc_locks_ab0 1m; lua_shared_dict bad_peers_ab0 1m; lua_shared_dict bodylog_ctl_ab0 1m; lua_shared_dict cch_ctl_ab0 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.ADAPTIVE_CC_TTL = 30
    -- per-peer max=100 → 静态 max=200(不封顶,让 ABS/相对逻辑说话);min=2;interval=2s,inc=2,dec=0.5
    local peers = {{"127.0.0.1",28951,"m1",0,100},{"127.0.0.1",28952,"m2",0,100}}
    local base = {peers=peers, tps_limit_tps=50, rt_limit_factor=1, adaptive_cc=true, adaptive_cc_min=2,
                  adaptive_cc_interval=2, adaptive_cc_dec=0.5, adaptive_cc_inc=2.0,
                  default_max=200, bodylog_default_enabled=false, health_check_interval=9999,
                  tps_window=3, tps_ttl=8, tps_probe_window=3, tps_probe_per_window=5, tps_min_decode_s=0.3}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("ab5", function() return R({adaptive_cc_abs=5}) end)   -- 新:绝对头寸 5
    _G.register_route("ab0", function() return R({adaptive_cc_abs=0}) end)   -- 对照:关绝对头寸=旧相对逻辑
  }
  server { listen 19590; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.ab5) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.ab5) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.ab5) } } }
  server { listen 19591; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.ab0) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.ab0) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.ab0) } } }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28951 28952; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 100 --chunk-delay-ms 12 --prefill-delay-ms 15 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
AB5=http://127.0.0.1:19590; AB0=http://127.0.0.1:19591
fire(){ curl -s -o /dev/null -N -H "$A" -H "$H" -d "{\"stream_options\":{\"include_usage\":true},\"model\":\"x\",\"stream\":true,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"prefill_delay_ms\":15,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" "$1/v1/chat/completions"; }
# 维持 ~n 个重叠并发 dur 秒:每 0.8s 发 n 个 ~1.2s(chunk12×100)长请求
load_n(){ local u=$1 n=$2 dur=$3; local endt=$((SECONDS+dur)); while [ "$SECONDS" -lt "$endt" ]; do for i in $(seq 1 $n); do fire "$u" 12 100 >/dev/null 2>&1 & done; sleep 0.8; done; wait; }
fv(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('$2',{}).get('_') if isinstance(d.get('$2'),dict) else d.get('$2'))"; }

echo "########## adaptive_cc 绝对头寸(ABS) ##########"
# 同样 ~3 持续并发打两条路由(healthy 快解码 → ewma>50)。跑 ~14s(7 个 interval)让 cc 收敛。
( load_n $AB5 3 14 ) & ( load_n $AB0 3 14 ) & wait
cc5=$(fv $AB5 adaptive_cc); cc0=$(fv $AB0 adaptive_cc)
ev5=$(fv $AB5 ewma_tps);    conc5=$(fv $AB5 adaptive_cc_conc); conc0=$(fv $AB0 adaptive_cc_conc)
echo "  [ab5] cc=$cc5 conc=$conc5 ewma=$ev5   [ab0] cc=$cc0 conc=$conc0"
# ① ABS=5:cc 收敛到 ~conc+5(绝对头寸),即 cc ≥ conc+4(留 1 容差)
awk "BEGIN{exit !($cc5>=$conc5+4)}" && ok "① ABS=5 cc 抬到 conc+ABS (cc=$cc5 ≥ conc($conc5)+4)" || no "① cc5=$cc5 未达 conc($conc5)+ABS"
# ② ABS=0 对照:退回旧相对逻辑 cc≈conc×1.25,无绝对头寸(cc0 < conc0+ABS,且 ≤ conc0×1.5)
awk "BEGIN{exit !($cc0<=$conc0*1.5 && $cc0<$conc0+4)}" && ok "② ABS=0 退回旧逻辑 cc≈1.25×conc (cc=$cc0, conc=$conc0)" || no "② cc0=$cc0 未回落旧相对逻辑(conc=$conc0)"
awk "BEGIN{exit !($cc5>$cc0+2)}" && ok "③ ABS=5 显著高于 ABS=0 (cc5=$cc5 vs cc0=$cc0)" || no "③ cc5($cc5) 未显著高于 cc0($cc0)"

# ④ 缩保留 ABS 绝对底:ab5 停到很低并发(1),cc 应缩但不破 conc+ABS(≈6),不塌到 min=2
load_n $AB5 1 12
cc5b=$(fv $AB5 adaptive_cc)
echo "  [ab5] 降并发后 cc=$cc5b (期望缩但 ≥ ~6,不塌到 min=2)"
awk "BEGIN{exit !($cc5b>=5)}" && ok "④ 缩保留 ABS 绝对底 (cc=$cc5b ≥5,未塌 min=2)" || no "④ cc=$cc5b 塌破绝对底"

# ⑤ 修复1:突发 20 并发 >> cc → 并发429 → do_route 记 per-pool rej;loop 每 tick 读+清零当快涨信号
cc_before=$(fv $AB5 adaptive_cc)
for i in $(seq 1 20); do ( fire $AB5 12 50 ) & done
sleep 0.6   # 让 429 累计,<interval(2s) → loop 还没清零 rej
rej=$(fv $AB5 adaptive_cc_rej)
echo "  [ab5] 突发20并发后 rej=$rej (cc_before=$cc_before)"
awk "BEGIN{exit !(${rej:-0}>=1)}" && ok "⑤ 并发429 计入 rej 快涨信号 (rej=$rej ≥1)" || no "⑤ rej=$rej 未记录429"
sleep 3      # 过 1~2 个 tick,rej 驱动 cc 朝 desired 快涨
cc_after=$(fv $AB5 adaptive_cc)
echo "  [ab5] rej 快涨后 cc=$cc_after (before=$cc_before)"
awk "BEGIN{exit !($cc_after>=$cc_before)}" && ok "⑥ rej 信号驱动 cc 未降(快涨/保持,cc $cc_before→$cc_after)" || no "⑥ cc 反降 $cc_before→$cc_after"
wait 2>/dev/null

echo "==== 结果: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
