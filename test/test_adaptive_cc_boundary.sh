#!/bin/bash
# 自适应并发(AIMD)**边界**测试(隔离 scratch + mock,自清理,不碰生产)。
# 补 AC1-8 之外的边界:clamp 上/下界严格不越界、min≥max 配置钳、factor×动态cc、
#   peers_by_model 每模型独立 cc、peer ban 后 limit 跟随实际健康容量、在线改阈值翻转 AIMD 方向。
# 加速尺度:interval=2、dec=0.5、inc=2.0、tps_window=3、tps_ttl=8、per-peer max=3。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/accbnd}"; KEY=REDACTED-API-KEY
MPORTS="28941 28942 28943"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in $MPORTS; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
  lua_shared_dict active_conns_cb 4m; lua_shared_dict cluster_avg_cb 16k; lua_shared_dict lc_locks_cb 1m; lua_shared_dict bad_peers_cb 1m; lua_shared_dict bodylog_ctl_cb 1m; lua_shared_dict cch_ctl_cb 1m;
  lua_shared_dict active_conns_cf 4m; lua_shared_dict cluster_avg_cf 16k; lua_shared_dict lc_locks_cf 1m; lua_shared_dict bad_peers_cf 1m; lua_shared_dict bodylog_ctl_cf 1m; lua_shared_dict cch_ctl_cf 1m;
  lua_shared_dict active_conns_mm 4m; lua_shared_dict cluster_avg_mm 16k; lua_shared_dict lc_locks_mm 1m; lua_shared_dict bad_peers_mm 1m; lua_shared_dict bodylog_ctl_mm 1m; lua_shared_dict cch_ctl_mm 1m;
  lua_shared_dict active_conns_pm 4m; lua_shared_dict cluster_avg_pm 16k; lua_shared_dict lc_locks_pm 1m; lua_shared_dict bad_peers_pm 1m; lua_shared_dict bodylog_ctl_pm 1m; lua_shared_dict cch_ctl_pm 1m;
  lua_shared_dict active_conns_bn 4m; lua_shared_dict cluster_avg_bn 16k; lua_shared_dict lc_locks_bn 1m; lua_shared_dict bad_peers_bn 1m; lua_shared_dict bodylog_ctl_bn 1m; lua_shared_dict cch_ctl_bn 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28941; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    local p2 = {{"127.0.0.1",28941,"m1",0,3},{"127.0.0.1",28942,"m2",0,3}}   -- 静态 max=6
    local base = {default_max=50,bodylog_default_enabled=false,health_check_interval=3,
                  tps_window=3,tps_ttl=8,tps_probe_window=3,tps_probe_per_window=5,tps_min_decode_s=0.3,
                  adaptive_cc=true, adaptive_cc_interval=2, adaptive_cc_dec=0.5, adaptive_cc_inc=2.0, tps_limit_tps=50}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    -- cb:clamp 上/下界(min=2,max=6,factor=1)
    _G.register_route("cb", function() return R({peers=p2, adaptive_cc_min=2, rt_limit_factor=1}) end)
    -- cf:factor=2(gate = cc×2)
    _G.register_route("cf", function() return R({peers=p2, adaptive_cc_min=2, rt_limit_factor=2}) end)
    -- mm:配 min=10 > 静态max 6 → 生效 min 应钳到 6
    _G.register_route("mm", function() return R({peers=p2, adaptive_cc_min=10, rt_limit_factor=1}) end)
    -- pm:peers_by_model,每模型独立 cc(kimi 2peer/max6,glm 1peer/max3),min=2
    _G.register_route("pm", function() return R({peers_by_model={["kimi-k2.6"]={{"127.0.0.1",28941,"k1",0,3},{"127.0.0.1",28942,"k2",0,3}},["glm-5.1-fp8"]={{"127.0.0.1",28943,"g1",0,3}}}, adaptive_cc_min=2, rt_limit_factor=1}) end)
    -- bn:peer ban 测试(2peer max3=6);跑中杀一个 mock → 健康探活 ban → pool_limit 6→3
    _G.register_route("bn", function() return R({peers=p2, adaptive_cc_min=2, rt_limit_factor=1}) end)
  }
  server { listen 19510; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.cb) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.cb) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.cb) } }
    location = /_tps_limit  { content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts.cb) } } }
  server { listen 19511; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.cf) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.cf) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.cf) } } }
  server { listen 19512; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.mm) } }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.mm) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.mm) } } }
  server { listen 19513; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.pm) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.pm) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.pm) } } }
  server { listen 19514; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.bn) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.bn) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.bn) } }
    location = /_health_status { content_by_lua_block { _G.dbg_health_status(_G.__route_opts.bn) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
startmock(){ setsid nohup "$PY" "$MOCK" --port $1 --name m-$1 --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 20 >"$PREFIX/mock_$1.log" 2>&1 & disown; }
for p in $MPORTS; do startmock $p; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
CB=http://127.0.0.1:19510; CF=http://127.0.0.1:19511; MM=http://127.0.0.1:19512; PM=http://127.0.0.1:19513; BN=http://127.0.0.1:19514
fire(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" \
  -d "{\"stream_options\":{\"include_usage\":true},\"model\":\"${4:-x}\",\"stream\":true,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"prefill_delay_ms\":20,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
  "$1/v1/chat/completions"; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
# 顺序建 ewma(rt_sum=1,不触发缩小的并发闸)。$4=model
estab(){ align; for i in 1 2 3 4 5 6; do fire "$1" "$2" "$3" "${4:-x}" >/dev/null; done; sleep 4; fire "$1" "$2" "$3" "${4:-x}" >/dev/null; sleep 1; }
expire(){ sleep 10; }
burst(){ local u=$1 n=$2 cd=$3 mx=$4 md=${5:-x} d="$PREFIX/b"; rm -f ${d}_*; for i in $(seq 1 $n); do ( fire "$u" "$cd" "$mx" "$md"; echo ) >${d}_$i & done; wait; cat ${d}_* | sort | uniq -c | tr '\n' ' '; }
# 持续并发压力($4=model,$5=dur秒):制造"并发顶到 cc"的压力 + 快流喂 ewma → cc 才 ×inc 爬(新逻辑)
load(){ local u=$1 cd=$2 mx=$3 md=${4:-x} dur=$5; local endt=$((SECONDS+dur)); while [ "$SECONDS" -lt "$endt" ]; do for i in 1 2 3 4 5 6 7 8; do fire "$u" "$cd" "$mx" "$md" >/dev/null 2>&1 & done; sleep 0.3; done; wait; }
acc(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);cc=d.get('adaptive_cc') or {};print(cc.get('${2:-_}') if cc.get('${2:-_}') is not None else '')"; }
accmin(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);m=d.get('adaptive_cc_min') or {};print(m.get('${2:-_}'))"; }

echo "########## 自适应并发 边界 cases ##########"

# B1 clamp 上界:快流+并发压力多个 tick(inc×2)→ cc 封顶在 max=6,绝不超(不会 12)
expire; estab $CB 10 40; load $CB 10 40 x 8; c=$(acc $CB)
{ [ "$c" = "6" ]; } && ok "B1 clamp 上界:快流+压力多tick → cc=$c(=max6,不越界)" || no "B1 cc=$c(期望 6)"

# B2 clamp 下界:慢流多个 tick(dec×0.5)→ cc 落底在 min=2,绝不低于(不会 1/0)
expire; estab $CB 50 20; sleep 10; c=$(acc $CB)
{ [ "$c" = "2" ]; } && ok "B2 clamp 下界:慢流多tick → cc=$c(=min2,不越界)" || no "B2 cc=$c(期望 2)"

# B3 min≥max 配置:mm 配 adaptive_cc_min=10 > 静态max6 → 生效 min 钳到 6;慢流也压不下 6
mn=$(accmin $MM)
expire; estab $MM 50 20; sleep 10; c=$(acc $MM)
{ [ "$mn" = "6" ] && [ "$c" = "6" ]; } && ok "B3 min≥max 钳:配min10→生效min=$mn(=max6),慢流 cc=$c 压不下 6" || no "B3 min=$mn cc=$c(期望 6/6)"

# B4 factor×动态cc:cf factor=2,慢流压 cc→2 → gate=cc×2=4;burst6 → 应在 4 处触发(甩 2)
expire; estab $CF 50 20; sleep 10; ccf=$(acc $CF)
r=$(burst $CF 6 50 20); b429=$(echo "$r"|grep -oE "[0-9]+ 429"|grep -oE "^[0-9]+"); b429=${b429:-0}
{ awk "BEGIN{exit !($ccf<=3)}" && [ "$b429" -ge 1 ] && [ "$b429" -le 3 ]; } \
  && ok "B4 factor×cc:cc=$ccf,gate=cc×2≈4 → burst6=[$r](甩 $b429,≈2)" || no "B4 cc=$ccf 429=$b429 r=[$r]"

# B5 peers_by_model 每模型独立 cc:kimi 慢+无压力(cc缩到min) / glm 快+压力(cc涨到max3),互不影响
# 每模型独立 rt_sum(tps_key_prefix 带 model)→ 压力也各判各的
expire; align
_e5=$((SECONDS+9))
while [ "$SECONDS" -lt "$_e5" ]; do
  fire $PM 50 20 kimi-k2.6 >/dev/null 2>&1 &                          # kimi 慢、单发(低并发无压力 + ewma<阈值 → 缩)
  for i in 1 2 3 4 5 6; do fire $PM 10 40 glm-5.1-fp8 >/dev/null 2>&1 & done  # glm 快×6(压力+高ewma → 涨到 max3)
  sleep 0.4
done; wait
ck=$(acc $PM kimi-k2.6); cg=$(acc $PM glm-5.1-fp8)
{ [ -n "$ck" ] && [ -n "$cg" ] && awk "BEGIN{exit !($ck < $cg)}"; } \
  && ok "B5 每模型独立:kimi慢 cc=$ck < glm快 cc=$cg(各自 AIMD,互不干扰)" || no "B5 kimi=$ck glm=$cg"

# B6 peer ban → limit 跟随实际容量:cc 升到 max6;杀 m2 → 探活 ban → pool_limit 6→3
#   → do_route limit=min(cc6,pool3)=3 → burst5 应在 3 处甩(证明尊重实际健康容量,不按过时 cc6)
expire; estab $BN 10 40; sleep 8; cbn=$(acc $BN)     # cc 升到 6
for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port 28942"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 8                                               # > health_check_interval(3)*2,等 ban
bantxt=$(curl -s "$BN/_health_status" | python3 -c "import sys,json;d=json.load(sys.stdin);print([k for k,v in d.items() if v.get('banned')])" 2>/dev/null)
r=$(burst $BN 5 10 40); b429=$(echo "$r"|grep -oE "[0-9]+ 429"|grep -oE "^[0-9]+"); b429=${b429:-0}
{ echo "$bantxt"|grep -q 28942 && [ "$b429" -ge 1 ]; } \
  && ok "B6 peer ban→limit 跟随健康容量:cc=$cbn,ban=$bantxt,pool 6→3 → burst5=[$r] 在 3 处甩($b429)" || no "B6 cc=$cbn ban=$bantxt r=[$r] 429=$b429"
startmock 28942   # 复原

echo ""
echo "================ 自适应并发 边界套件: PASS=$P FAIL=$F ================"
