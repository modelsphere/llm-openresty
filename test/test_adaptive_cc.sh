#!/bin/bash
# 自适应并发(AIMD)功能测试(隔离 scratch + mock,自清理,不碰生产)。
# 复用 TPS EWMA 当反馈信号:每 adaptive_cc_interval 调池并发上限——ewma<阈值 ×dec 减、>= ×inc 增,
#   clamp 在 [min, 静态max];ewma 过期(nil)保持不动;与 TPS 硬熔断互斥(adaptive 路由不出 tps-429)。
# 为加速把尺度调小(逻辑与生产一致):interval=2s、dec=0.5、inc=2.0、tps_window=3、tps_ttl=8、
#   per-peer max=3(静态max=6)、min=2 → 收敛 2~3 步。tps≈1000/chunk_delay:delay50→20(<阈值50 减)、
#   delay10→100(>=50 增)。为不被"缩小后的并发闸"挡住 ewma 建立,estab 用顺序发(rt_sum=1<min)。
# 覆盖:减(降到min+高并发429带adaptive_cc trigger)/ 增(回升到max+429停)/ 不甩轻流(低并发不429)/
#   互斥(adaptive 不出 tps-429)/ 无信号保持(ewma过期冻结)/ clamp(不越min) / 派生min(acd) /
#   零回归对照(hard 路由仍 tps-429)。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/acctest2}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28931 28932 28933; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
  lua_shared_dict active_conns_ac 4m; lua_shared_dict cluster_avg_ac 16k; lua_shared_dict lc_locks_ac 1m; lua_shared_dict bad_peers_ac 1m; lua_shared_dict bodylog_ctl_ac 1m; lua_shared_dict cch_ctl_ac 1m;
  lua_shared_dict active_conns_acd 4m; lua_shared_dict cluster_avg_acd 16k; lua_shared_dict lc_locks_acd 1m; lua_shared_dict bad_peers_acd 1m; lua_shared_dict bodylog_ctl_acd 1m; lua_shared_dict cch_ctl_acd 1m;
  lua_shared_dict active_conns_hd 4m; lua_shared_dict cluster_avg_hd 16k; lua_shared_dict lc_locks_hd 1m; lua_shared_dict bad_peers_hd 1m; lua_shared_dict bodylog_ctl_hd 1m; lua_shared_dict cch_ctl_hd 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28931; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.ADAPTIVE_CC_TTL = 16   -- 测试用小 TTL(生产默认 300);AC5 自愈快速触发。expire(10s)<16 → cc 保持
    -- per-peer max=3 → 静态 max=6(两 peer 同优先级 sum)
    local p2 = {{"127.0.0.1",28931,"m1",0,3},{"127.0.0.1",28932,"m2",0,3}}
    -- per-peer max=20 → 静态 max=40(派生 min = floor(40*0.1)=4)
    local p2big = {{"127.0.0.1",28931,"b1",0,20},{"127.0.0.1",28932,"b2",0,20}}
    -- adaptive_cc_abs=0:本套专测相对 dec/inc/pressure/slack 逻辑,关掉绝对头寸(小 scale max=6 与 ABS=5 默认冲突;
    -- 绝对头寸+429信号单独由 test_adaptive_cc_abs.sh 覆盖)
    local base = {default_max=50,bodylog_default_enabled=false,health_check_interval=5,adaptive_cc_abs=0,
                  tps_window=3,tps_ttl=8,tps_probe_window=3,tps_probe_per_window=5,tps_min_decode_s=0.3}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    -- ac:自适应,显式 min=2,加速参数;rt_limit_factor=1(闸更紧,小并发即可触发)
    _G.register_route("ac",  function() return R({peers=p2, tps_limit_tps=50, rt_limit_factor=1,
        adaptive_cc=true, adaptive_cc_min=2, adaptive_cc_interval=2, adaptive_cc_dec=0.5, adaptive_cc_inc=2.0}) end)
    -- acd:配 tps_limit_tps 但**不写 adaptive_cc** → 应按全局默认翻成自适应(验证 default-on);不配 min → 派生 10
    _G.register_route("acd", function() return R({peers=p2big, tps_limit_tps=50,
        adaptive_cc_interval=2}) end)
    -- hard:硬熔断对照(显式 adaptive_cc=false;全局默认已翻自适应)→ 仍 tps-429
    _G.register_route("hd",  function() return R({peers=p2, tps_limit_tps=50, adaptive_cc=false}) end)
  }
  server { listen 19490; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.ac) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.ac) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.ac) } }
    location = /_tps_limit { content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts.ac) } }
    location = /_tps_toggle { content_by_lua_block { _G.dbg_tps_toggle(_G.__route_opts.ac) } } }
  server { listen 19491; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.acd) } } }
  server { listen 19492; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.hd) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.hd) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.hd) } }
    location = /_active_conns_set { content_by_lua_block { _G.dbg_active_conns_set(_G.__route_opts.hd) } }
    location = /_ban_set { content_by_lua_block { local o=_G.__route_opts.hd; ngx.shared[o.bad_peers_dict]:set(ngx.var.arg_peer, true, tonumber(ngx.var.arg_ttl) or 30); ngx.say("ok") } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28931 28932 28933; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 20 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
AC=http://127.0.0.1:19490; ACD=http://127.0.0.1:19491; HD=http://127.0.0.1:19492
# fire $1=url $2=chunk_delay $3=max_tokens → 打印 http_code
fire(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" \
  -d "{\"stream_options\":{\"include_usage\":true},\"model\":\"x\",\"stream\":true,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"prefill_delay_ms\":20,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
  "$1/v1/chat/completions"; }
# fireb:返回 body(查 429 trigger 用)
fireb(){ curl -s -N -H "$A" -H "$H" \
  -d "{\"stream_options\":{\"include_usage\":true},\"model\":\"x\",\"stream\":true,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"prefill_delay_ms\":20,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
  "$1/v1/chat/completions"; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
# 顺序建 ewma(rt_sum=1,不触发缩小后的并发闸):6 个跨窗 + fold
estab_seq(){ align; for i in 1 2 3 4 5 6; do fire "$1" "$2" "$3" >/dev/null; done; sleep 4; fire "$1" "$2" "$3" >/dev/null; sleep 1; }
expire(){ sleep 10; }   # > ewma ttl(8s):清 ewma;10s < CC_TTL(16s)故 adaptive_cc 仍保持(不误过期)
# 并发 burst,返回 "N 200 M 429"
burst(){ local u=$1 n=$2 cd=$3 mx=$4 d="$PREFIX/b"; rm -f ${d}_*; for i in $(seq 1 $n); do ( fire "$u" "$cd" "$mx"; echo ) >${d}_$i & done; wait; cat ${d}_* | sort | uniq -c | tr '\n' ' '; }
# 持续并发压力:dur 秒内每 0.3s 发一批 8 并发。制造"并发顶到 cc"的压力(do_route 存高 rt_sum)+
# 被 admit 的快流喂 ewma(≥阈值)→ cc 才会 ×inc 爬(新逻辑:只有有压力才涨)。cd 小=快解码=高 ewma。
load(){ local u=$1 cd=$2 mx=$3 dur=$4; local endt=$((SECONDS+dur)); while [ "$SECONDS" -lt "$endt" ]; do for i in 1 2 3 4 5 6 7 8; do fire "$u" "$cd" "$mx" >/dev/null 2>&1 & done; sleep 0.3; done; wait; }
# 可调并发的持续负载($1=并发数 $2=秒):每 0.4s 发 n 个较长(chunk10×60=600ms)快解码请求 → 维持 ~n 重叠并发(rt_sum≥1)
load_n(){ local n=$1 dur=$2; local endt=$((SECONDS+dur)); while [ "$SECONDS" -lt "$endt" ]; do for i in $(seq 1 $n); do fire $AC 10 60 >/dev/null 2>&1 & done; sleep 0.4; done; wait; }
acc(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);cc=d.get('adaptive_cc') or {};print(cc.get('${2:-_}') if cc.get('${2:-_}') is not None else '')"; }
ev(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['ewma_tps'].get('${2:-_}',''))"; }
stf(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('$2'))"; }

echo "########## 自适应并发(AIMD)cases ##########"

# AC1 减:快流+并发压力 prime 到 max(6)→ 切慢流 ewma<50 → cc 每 interval ×0.5 降到 min(2)
expire; estab_seq $AC 10 40; load $AC 10 40 8   # 快流(ewma~100)+持续并发压力 → cc 顶着爬到 6
ccmax=$(acc $AC)
expire                                           # 清 ewma(cc 保持)
estab_seq $AC 50 20; sleep 7                     # 慢流 ewma~20 → cc 降(缩不看压力)
cclow=$(acc $AC); elo=$(ev $AC)
{ [ -n "$ccmax" ] && [ -n "$cclow" ] && awk "BEGIN{exit !($cclow<$ccmax && $cclow<=3 && $cclow>=2)}"; } \
  && ok "AC1 减:cc $ccmax→$cclow(降到 min2 区间,ewma=$elo<50)" || no "AC1 ccmax=$ccmax cclow=$cclow ewma=$elo"

# AC2 增:快流+并发压力 → cc ×2 回升到 max(6);且低于 cc 的并发(5<6)不被 429
expire; estab_seq $AC 10 40; load $AC 10 40 8   # 快流+压力 → cc 升到 6
cchi=$(acc $AC); ehi=$(ev $AC)
r2=$(burst $AC 5 10 40)                          # 轻并发 5 < cc6 → 应全 200,不 429(守"低于 cc 放行")
{ awk "BEGIN{exit !($cchi>$cclow && $cchi>=5)}" && ! echo "$r2"|grep -q 429; } \
  && ok "AC2 增:cc $cclow→$cchi(有压力回升 max6,ewma=$ehi)+低于cc并发5不429[$r2]" || no "AC2 cchi=$cchi ewma=$ehi burst=[$r2]"

# AC2b 健康但无压力(快流顺序发 ewma≥阈值 但并发~1)→ cc **不涨**(已在 min2,slack 缩也被 min 兜住 → 停 2)
# 断言 ewma 确实 ≥阈值(否则 cc 平是 ewma<thr 造成、而非压力门,会假过)
expire; estab_seq $AC 50 20; sleep 6            # cc→min 2
cc_before=$(acc $AC)
expire; for r in 1 2 3 4 5 6; do fire $AC 10 40 >/dev/null; sleep 2; done   # 快流顺序(ewma~100,并发~1),跨 3 tick
cc_after=$(acc $AC); e2b=$(ev $AC)
{ [ -n "$cc_before" ] && [ -n "$cc_after" ] && [ -n "$e2b" ] && awk "BEGIN{exit !($cc_after<=$cc_before+0.01 && $e2b>=50)}"; } \
  && ok "AC2b 健康无压力不涨:cc $cc_before→$cc_after(ewma=$e2b≥阈值50 但并发低 → 不爬,停在 min)" || no "AC2b cc $cc_before→$cc_after ewma=$e2b(需 ewma≥50 且 cc 不涨)"

# AC2c 忙→真实低并发缩:压力 prime 到 max6 → 切**轻量重叠并发(~2,conc≥1)**(ewma健康)→ cc 往下跟到中位
# (顺序单发 rt_sum=0 会命中"conc=0 保持"分支——那是防空闲/长请求误缩,不是这里要测的;故用重叠并发)
expire; estab_seq $AC 10 40; load $AC 10 40 8   # cc→6
cc_hi=$(acc $AC)
load_n 2 12                                      # ~2 重叠并发(conc≥1)+ 快解码(ewma~100)→ conc<cc×slack → 缩到中位
cc_lo=$(acc $AC); e2c=$(ev $AC)
{ [ -n "$cc_hi" ] && [ -n "$cc_lo" ] && [ -n "$e2c" ] && awk "BEGIN{exit !($cc_lo<$cc_hi && $cc_lo>=2 && $e2c>=50)}"; } \
  && ok "AC2c 忙→低并发缩:cc $cc_hi→$cc_lo(ewma=$e2c≥阈值,真实低并发 → cc 跟着降到中位≥min,不卡峰值)" || no "AC2c cc $cc_hi→$cc_lo ewma=$e2c(应缩到中位)"

# AC2d 收敛到中位平衡点 + 不振荡(#2/#3/#4):把并发精确钉在 3(/_active_conns_set)→ cc 应收敛到 ~3/mid=3.75
# (中位:>min2 且 <max6),且跨 tick 稳定(mid-clamp 不 ping-pong;若无 clamp 大步长会 6↔3 弹)。
setc(){ curl -s "$AC/_active_conns_set?peer=127.0.0.1:28931&value=$1" >/dev/null 2>&1; }
expire; estab_seq $AC 10 40                       # ewma 健康
for k in 1 2 3 4 5 6; do setc 3; fire $AC 10 40 >/dev/null 2>&1; sleep 2; done  # 钉并发3:触发 do_route 存 rt_sum=3 + 喂 ewma(cc>3 后 fire 不 429)
cc_eq1=$(acc $AC); sleep 2; cc_eq2=$(acc $AC)     # 两次读验稳定
setc 0
{ [ -n "$cc_eq1" ] && [ -n "$cc_eq2" ] && awk "BEGIN{exit !($cc_eq1>2.5 && $cc_eq1<5 && $cc_eq2>2.5 && $cc_eq2<5)}"; } \
  && ok "AC2d 收敛中位不振荡:cc≈$cc_eq1/$cc_eq2(并发钉3 → cc 停 ~3.75,不到 min/max、跨 tick 稳)" || no "AC2d cc=$cc_eq1/$cc_eq2(应≈3.75 稳定)"

# AC3 不甩轻流 + 互斥:慢流把 cc 压到 2,单发(rt_sum=1<2)→ 200,且 body 不含 tps-429
expire; estab_seq $AC 50 20; sleep 7            # cc→2,ewma<50
cc3=$(acc $AC); c1=$(fire $AC 50 20); c2=$(fire $AC 50 20)
b3=$(fireb $AC 50 20)
{ [ "$c1" = 200 ] && [ "$c2" = 200 ] && ! echo "$b3"|grep -q "tps limit exceeded"; } \
  && ok "AC3 不甩轻流+互斥:cc=$cc3,单发顺序 200/200,无 tps-429" || no "AC3 cc=$cc3 c1=$c1 c2=$c2 body=$(echo "$b3"|head -c120)"

# AC4 contention → 429 带 adaptive_cc trigger(cc=2,8 并发慢 → 超 cap 429)
res=$(burst $AC 8 50 20)
# 背景压 5 慢保持 rt_sum 高,前台单发抓 429 body
for i in 1 2 3 4 5; do ( fire $AC 50 20 >/dev/null ) & done; sleep 0.4; b4=$(fireb $AC 50 20); wait
{ echo "$res"|grep -q 429 && echo "$b4"|grep -q "concurrency limit exceeded" && echo "$b4"|grep -q '"adaptive_cc"'; } \
  && ok "AC4 contention 429:burst[$res],trigger=concurrency+adaptive_cc" || no "AC4 burst=[$res] body=$(echo "$b4"|head -c160)"

# AC5 无信号:短缺口保持 + 持续无信号自愈(CC_TTL=tps_ttl*2=16s)
# cc 压到 2 后停流量:① ewma 过期(>8s)但 <CC_TTL → cc 仍在(保持);② 静默 >CC_TTL → cc 过期复位
expire; estab_seq $AC 50 20; sleep 5; cc5a=$(acc $AC)   # cc 刚落地
sleep 6;  cc5hold=$(acc $AC); ev5=$(ev $AC)            # ewma 已空,cc 仍在(< CC_TTL)
sleep 26; cc5heal=$(acc $AC)                            # 静默 >> CC_TTL → cc 过期 → 空(回退满容量,防冻死)
{ [ -n "$cc5a" ] && [ -n "$cc5hold" ] && [ -z "$ev5" ] && [ -z "$cc5heal" ]; } \
  && ok "AC5 无信号:短缺口保持(cc=$cc5hold,ewma空)→ 持续静默自愈过期(cc空)" \
  || no "AC5 cc5a=$cc5a hold=$cc5hold ewma='$ev5' heal='$cc5heal'"

# AC6 默认开 + 派生 min:acd 只配 tps_limit_tps(未写 adaptive_cc)→ 全局默认翻自适应(on=true);
#   不配 min → 派生=静态max(40)*0.25=10,max=40
read on mn_ mx_ < <(curl -s "$ACD/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);mn=d.get('adaptive_cc_min') or {};mx=d.get('adaptive_cc_max') or {};print(d.get('adaptive_cc_on'), mn.get('_'), mx.get('_'))")
{ [ "$on" = "True" ] && [ "$mn_" = "4" ] && [ "$mx_" = "40" ]; } \
  && ok "AC6 默认开(只配tps_limit_tps→on=$on)+派生min=$mn_(=40*0.1),max=$mx_" || no "AC6 on=$on min=$mn_ max=$mx_"

# AC7 零回归对照:hard 路由(不开 adaptive)慢流 → 仍 tps-429(硬熔断存活 + 互斥真实)
expire; estab_seq $HD 50 20
r7=$(burst $HD 8 50 20)
for i in 1 2 3 4 5 6; do ( fire $HD 50 20 >/dev/null ) & done; sleep 0.3; b7=$(fireb $HD 50 20); wait
{ echo "$r7"|grep -q 429 && echo "$b7"|grep -q "tps limit exceeded"; } \
  && ok "AC7 零回归对照:hard 仍 tps-429[$r7]" || no "AC7 burst=[$r7] body=$(echo "$b7"|head -c160)"

# AC8 __off 是所有 tps 限流的统一开关:关掉后自适应立即失效(limit 回退 pool_limit,不再用收缩的 cc)
#   自适应生效时 cc≈2 → burst8 甩 6;__off 后回退 pool_limit=6(gate=6×factor1)→ 只甩 ~2
expire; estab_seq $AC 50 20; sleep 7; cc_on=$(acc $AC)   # 自适应生效,cc≈2
curl -s -X POST "$AC/_tps_toggle?on=0" >/dev/null         # 关所有 tps 限流(硬熔断 + 自适应)
res_off=$(burst $AC 8 50 20)                              # 立即回退 pool_limit → 429 明显减少
curl -s -X POST "$AC/_tps_toggle?on=1" >/dev/null          # 复原
off429=$(echo "$res_off"|grep -oE "[0-9]+ 429"|grep -oE "^[0-9]+"); off429=${off429:-0}
{ awk "BEGIN{exit !($cc_on<=3)}" && [ "$off429" -le 3 ]; } \
  && ok "AC8 __off 统一关:cc生效=$cc_on(甩6);__off 后回退 pool_limit(只甩 $off429≤3)" || no "AC8 cc_on=$cc_on off429=$off429 res=[$res_off]"

# AC9 rt_sum 含 banned peer 的在途连接(2026-07-03):ban 只是"不发新流量"的路由决策,被 ban peer 上
#   残留的在途流仍是真实负载,必须计入 rt_sum(尤其转发型 peer 共享下游池,如 CART router)。用 hd 路由
#   (非自适应,免 adaptive_cc 阈值干扰):28931/28932 各 max3。ban 28932(pin 3 条在途)→ healthy 仅
#   28931 → limit=3;set 28931=2。旧逻辑漏算 banned:rt_sum=2<3 → 放行(真实负载 5 被忽略、过量 admit)。
#   新逻辑:rt_sum=2+3=5 ≥ 3 → 429,body realtime=5(含 banned 的 3)。
curl -s "$HD/_active_conns_set?peer=127.0.0.1:28931&value=2" >/dev/null
curl -s "$HD/_active_conns_set?peer=127.0.0.1:28932&value=3" >/dev/null
curl -s "$HD/_ban_set?peer=127.0.0.1:28932&ttl=30" >/dev/null
b9=$(fireb $HD 10 20)     # 立即打一发(趁 health timer 未解 ban)
rt9=$(echo "$b9" | grep -oE '"realtime":[0-9]+' | grep -oE '[0-9]+')
{ echo "$b9" | grep -q '"trigger":"realtime' && [ "${rt9:-0}" -eq 5 ]; } \
  && ok "AC9 rt_sum 含 banned:ban 28932(3在途)+28931(2)→ rt_sum=$rt9=5 ≥ limit3 → 429 甩(旧逻辑漏算成2会放行)" \
  || no "AC9 期望 429+realtime=5,实得 realtime=$rt9 body=[$b9]"

echo ""
echo "================ 自适应并发套件: PASS=$P FAIL=$F ================"
