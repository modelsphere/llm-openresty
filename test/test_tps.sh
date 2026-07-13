#!/bin/bash
# TPS 限流(解码速率)功能 + 边界 case 自动化测试(隔离 scratch + mock,自清理,不碰生产)。
# 镜像 test_ttft.sh:为加速把时间尺度/桶调小(逻辑与生产一致):
#   tps_window=3s、tps_ttl=8s、tps_probe_window=3s、tps_min_decode_s=0.3、tps_min_tokens=16、
#   小桶 {5,10,20,40,80,150,300}(溢出>300→P20代表=600)、floor=30 tok/s。
# tps ≈ 1000/chunk_delay_ms:delay=50→20(慢,<30 限流)、delay=10→100(快,>30 放行)、
#   delay=2→500(溢出)。max_tokens=completion_tokens(mock 跑满)。
# 覆盖:alpha钳位 / P20(低尾非均值) / 少样本P20 / 桶溢出 / 限流+半开探测 / 窗口折叠 /
#   每模型阈值(peers_by_model) / 非流式不喂 / 错误不喂 / toggle / 空窗保持+TTL过期 /
#   正常放行 / 在线阈值 / no-usage fail-open+nousage / min_tokens边界 / min_decode边界 /
#   gmatch末匹配(fix#3 decoy) / opt-in未配不限流+warning。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/tpstest_suite}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28911 28912 28913 28914; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
  lua_shared_dict active_conns_s 4m; lua_shared_dict cluster_avg_s 16k; lua_shared_dict lc_locks_s 1m; lua_shared_dict bad_peers_s 1m; lua_shared_dict bodylog_ctl_s 1m; lua_shared_dict cch_ctl_s 1m;
  lua_shared_dict active_conns_pm 4m; lua_shared_dict cluster_avg_pm 16k; lua_shared_dict lc_locks_pm 1m; lua_shared_dict bad_peers_pm 1m; lua_shared_dict bodylog_ctl_pm 1m; lua_shared_dict cch_ctl_pm 1m;
  lua_shared_dict active_conns_a 4m; lua_shared_dict cluster_avg_a 16k; lua_shared_dict lc_locks_a 1m; lua_shared_dict bad_peers_a 1m; lua_shared_dict bodylog_ctl_a 1m; lua_shared_dict cch_ctl_a 1m;
  lua_shared_dict active_conns_e 4m; lua_shared_dict cluster_avg_e 16k; lua_shared_dict lc_locks_e 1m; lua_shared_dict bad_peers_e 1m; lua_shared_dict bodylog_ctl_e 1m; lua_shared_dict cch_ctl_e 1m;
  lua_shared_dict active_conns_no 4m; lua_shared_dict cluster_avg_no 16k; lua_shared_dict lc_locks_no 1m; lua_shared_dict bad_peers_no 1m; lua_shared_dict bodylog_ctl_no 1m; lua_shared_dict cch_ctl_no 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28911; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}   -- 测试用小桶(溢出>300→P20代表=600)
    local mk={{"127.0.0.1",28911,"m1"},{"127.0.0.1",28912,"m2"},{"127.0.0.1",28913,"m3"}}
    local C={default_max=50,bodylog_default_enabled=false,health_check_interval=5,tps_window=3,tps_ttl=8,tps_probe_window=3,tps_probe_per_window=5,tps_min_decode_s=0.3,adaptive_cc=false}  -- 本套件专测硬熔断;全局默认已翻自适应,显式 false 保持硬熔断
    local function R(x) local t={} for k,v in pairs(C) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("s",  function() return R({peers=mk, tps_limit_tps=50, tps_ewma_alpha=0.3}) end)
    _G.register_route("pm", function() return R({peers_by_model={["kimi-k2.6"]={{"127.0.0.1",28911,"k1"},{"127.0.0.1",28912,"k2"}},["glm-5.1-fp8"]={{"127.0.0.1",28913,"g1"}}}, tps_limit_tps=50, tps_limit_by_model={["kimi-k2.6"]=50,["glm-5.1-fp8"]=10}}) end)
    _G.register_route("a",  function() return R({peers=mk, tps_limit_tps=50, tps_ewma_alpha=30}) end)   -- alpha 钳位
    _G.register_route("e",  function() return R({peers={{"127.0.0.1",28914,"err"}}, tps_limit_tps=50}) end)
    _G.register_route("no", function() return R({peers=mk}) end)   -- opt-in off:不配 tps_limit_tps
  }
  server { listen 19190; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.s) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.s) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.s) } }
    location = /_tps_toggle { content_by_lua_block { _G.dbg_tps_toggle(_G.__route_opts.s) } }
    location = /_tps_limit { content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts.s) } } }
  server { listen 19191; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.pm) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.pm) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.pm) } } }
  server { listen 19192; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.a) } } }
  server { listen 19193; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.e) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.e) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.e) } } }
  server { listen 19194; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.no) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.no) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.no) } }
    location = /_tps_limit { content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts.no) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28911 28912 28913; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 20 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
setsid nohup "$PY" "$MOCK" --port 28914 --name m-err --chat-status 500 >"$PREFIX/mock_28914.log" 2>&1 & disown
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
S=http://127.0.0.1:19190; PM=http://127.0.0.1:19191; AA=http://127.0.0.1:19192; E=http://127.0.0.1:19193; NO=http://127.0.0.1:19194
# fire $1=url $2=chunk_delay $3=max_tokens $4=model $5=extra_json(无逗号) $6=usage(默认1)
fire(){ local uopt='"stream_options":{"include_usage":true},'; [ "${6:-1}" = "0" ] && uopt=''
  local ex="${5:-}"; [ -n "$ex" ] && ex="$ex,"
  curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" \
    -d "{${uopt}${ex}\"model\":\"${4:-x}\",\"stream\":true,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"prefill_delay_ms\":20,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
    "$1/v1/chat/completions"; }
# 非流式
firens(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "{\"model\":\"${4:-x}\",\"stream\":false,\"chunk_delay_ms\":$2,\"max_tokens\":$3,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" "$1/v1/chat/completions"; }
ev(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['ewma_tps'].get('${2:-_}',''))"; }   # ewma 值(空=无)
nu(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['nousage_samples'].get('${2:-_}',0))"; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
# estab:一个窗口灌 6 个样本,跨窗后再发 1 个触发折叠 → ewma 落地
estab(){ align; for i in 1 2 3 4 5 6; do ( fire "$1" "$2" "$3" "${4:-x}" >/dev/null ) & done; wait; sleep 4; fire "$1" "$2" "$3" "${4:-x}" >/dev/null; sleep 1; }
expire(){ sleep 10; }   # > ttl(8s)
burst(){ local u=$1 n=$2 cd=$3 mx=$4 mdl=${5:-x} d="$PREFIX/b"; rm -f ${d}_*; for i in $(seq 1 $n); do ( fire "$u" "$cd" "$mx" "$mdl"; echo ) >${d}_$i & done; wait; cat ${d}_* | sort | uniq -c | tr '\n' ' '; }

echo "########## TPS 功能 + 边界 cases ##########"

# TP1 alpha 钳位(配 30 → 1)
[ "$(curl -s $AA/_tps_status | python3 -c 'import sys,json;print(json.load(sys.stdin)["alpha"])')" = "1" ] && ok "TP1 alpha 钳位 30→1" || no "TP1 alpha=$(curl -s $AA/_tps_status|python3 -c 'import sys,json;print(json.load(sys.stdin)["alpha"])')"

# TP2 P20(低尾非均值):一个窗口 7×快(tps100,桶150) + 3×慢(tps20,桶20) → P20 折出低尾(桶20)而非均值(~76)
expire; align; for i in 1 2 3 4 5 6 7; do ( fire $S 10 40 >/dev/null ) & done; for i in 1 2 3; do ( fire $S 50 20 >/dev/null ) & done; wait; sleep 4; fire $S 10 40 >/dev/null; sleep 1
v=$(ev $S); awk "BEGIN{exit !($v<=80)}" && ok "TP2 窗口 P20=$v(<=80 低尾,非均值~117)" || no "TP2 ewma=$v(期望 P20~20)"

# TP3 少样本 P20(1 个慢样本 tps20 → 桶20)
expire; estab $S 50 20; v=$(ev $S); awk "BEGIN{exit !($v>=20 && $v<=80)}" && ok "TP3 单样本 P20=$v(慢样本桶 40)" || no "TP3 ewma=$v"

# TP4 桶溢出(tps500 → 溢出 → P20 代表 600)
expire; estab $S 2 200; v=$(ev $S); awk "BEGIN{exit !($v>=300)}" && ok "TP4 溢出 P20=$v(>=300,代表 600)" || no "TP4 ewma=$v"

# TP5 限流 + 半开探测(慢流 tps20 < floor30 → 429 + 每窗放行 probe_per_window=5)
expire; estab $S 50 20   # 建 ewma~20
res=$(burst $S 12 50 20); ok_n=$(echo "$res"|grep -oE "[0-9]+ 200"|grep -oE "^[0-9]+"); bad_n=$(echo "$res"|grep -oE "[0-9]+ 429"|grep -oE "^[0-9]+")
{ [ -n "${bad_n:-}" ] && [ "${bad_n:-0}" -gt 0 ]; } && ok "TP5 限流+探测:$res(429 出现,200=探测放行)" || no "TP5 $res"

# TP6 窗口折叠:慢 burst 后(本窗内)ewma 空,跨窗后落地
expire; align; for i in 1 2 3 4; do ( fire $S 50 20 >/dev/null ) & done; wait
e1="$(ev $S)"; sleep 4; fire $S 50 20 >/dev/null; sleep 1; e2="$(ev $S)"
{ [ -z "$e1" ] && [ -n "$e2" ]; } && ok "TP6 窗口折叠:本窗内 ewma 空 → 跨窗后=$e2" || no "TP6 e1='$e1' e2='$e2'"

# TP7 每模型阈值(peers_by_model):kimi floor50 / glm floor10,同窗交错发桶40 → kimi 限(40<=50)/ glm 不限(40>10)
expire; align
for i in 1 2 3 4; do ( fire $PM 50 20 "kimi-k2.6" >/dev/null ) & ( fire $PM 50 20 "glm-5.1-fp8" >/dev/null ) & done; wait
sleep 4; fire $PM 50 20 "kimi-k2.6" >/dev/null; fire $PM 50 20 "glm-5.1-fp8" >/dev/null; sleep 1
ek=$(ev $PM kimi-k2.6); eg=$(ev $PM glm-5.1-fp8)
rk=$(burst $PM 8 50 20 "kimi-k2.6"); rg=$(burst $PM 8 50 20 "glm-5.1-fp8")
{ echo "$rk"|grep -q 429 && ! echo "$rg"|grep -q 429; } && ok "TP7 每模型隔离:kimi(ewma=$ek)限 rk=[$rk] / glm(ewma=$eg)不限 rg=[$rg]" || no "TP7 kimi=$ek rk=[$rk] glm=$eg rg=[$rg]"

# TP8 非流式不喂(ewma 空)
expire; for i in 1 2 3 4 5; do firens $S 50 20 >/dev/null; done; v=$(ev $S); [ -z "$v" ] && ok "TP8 非流式不喂(ewma 空)" || no "TP8 ewma=$v"

# TP9 错误(后端500)不喂(ewma 空)
expire; for i in 1 2 3 4 5; do fire $E 50 20 >/dev/null; done; v=$(ev $E); [ -z "$v" ] && ok "TP9 错误不喂(ewma 空)" || no "TP9 ewma=$v"

# TP10 toggle off → 全放行;on → 限流回来
expire; estab $S 50 20
curl -s -X POST "$S/_tps_toggle?on=0" >/dev/null; r=$(burst $S 8 50 20); echo "$r"|grep -q 429 && no "TP10 off 仍 429:$r" || ok "TP10 toggle off → 全放行:$r"
curl -s -X POST "$S/_tps_toggle?on=1" >/dev/null; estab $S 50 20; r=$(burst $S 8 50 20); echo "$r"|grep -q 429 && ok "TP10 toggle on → 限流回来:$r" || no "TP10 on 未限流:$r"

# TP11 空窗 <ttl(8s)EWMA 保持
expire; estab $S 50 20; b="$(ev $S)"; sleep 4; a="$(ev $S)"; { [ -n "$b" ] && [ "$a" = "$b" ]; } && ok "TP11 空窗<8s ewma 保持($b→$a)" || no "TP11 $b→$a"

# TP12 空窗 >ttl(8s)TTL 过期 → ewma 空
expire; estab $S 50 20; expire; v="$(ev $S)"; [ -z "$v" ] && ok "TP12 空窗>8s TTL 过期(ewma 空)" || no "TP12 ewma=$v"

# TP13 正常(tps100 > floor30)→ 200 不限流
expire; estab $S 10 40; v=$(ev $S); c=$(fire $S 10 40); { awk "BEGIN{exit !($v>50)}" && [ "$c" = 200 ]; } && ok "TP13 正常 ewma=$v(>50 floor)→ $c" || no "TP13 ewma=$v c=$c"

# TP14 在线阈值:默认慢流限 / override 到 5(tps20>5)→ 放行 / 清除 → 回限
expire; estab $S 50 20
curl -s "$S/_tps_limit?tps=5" >/dev/null; r=$(burst $S 8 50 20); echo "$r"|grep -q 429 && no "TP14 override5 仍429:$r" || ok "TP14 override→5(tps20>5)放行:$r"
curl -s "$S/_tps_limit?tps=0" >/dev/null; r=$(burst $S 8 50 20); echo "$r"|grep -q 429 && ok "TP14 清除 override → 回限:$r" || no "TP14 清除后未限:$r"

# TP15 no-usage(不带 include_usage)→ fail-open:nousage++ / ewma 空 / 全 200
expire; n0=$(nu $S); for i in 1 2 3 4 5; do fire $S 50 20 x "" 0 >/dev/null; done; n1=$(nu $S); v=$(ev $S)
r=$(burst $S 6 50 20 x); rr=$(for i in 1 2 3; do fire $S 50 20 x "" 0; echo; done | sort | uniq -c | tr '\n' ' ')
{ [ "$n1" -gt "$n0" ] && [ -z "$v" ]; } && ok "TP15 no-usage fail-open:nousage $n0→$n1,ewma 空,no-usage 流不限($rr)" || no "TP15 n0=$n0 n1=$n1 ewma=$v"

# TP16 min_tokens 边界:max=15(<16)不采(ewma 空);max=16 采
expire; for i in 1 2 3 4 5; do fire $S 50 15 >/dev/null; done; v15=$(ev $S)
expire; estab $S 50 16; v16=$(ev $S)
{ [ -z "$v15" ] && [ -n "$v16" ]; } && ok "TP16 min_tokens:max15 不采(ewma空) / max16 采(ewma=$v16)" || no "TP16 v15='$v15' v16='$v16'"

# TP17 min_decode 边界:decode<0.3s 不采(tps100 但 max=10 → decode 0.1s)
expire; for i in 1 2 3 4 5; do fire $S 10 10 >/dev/null; done; vd=$(ev $S)
[ -z "$vd" ] && ok "TP17 min_decode:decode<0.3s 不采(ewma 空)" || no "TP17 ewma=$vd"

# TP18 gmatch 末匹配(fix#3):decoy_ct=99999 在正文,真 completion_tokens=20 → 用真(tps~20),非 decoy(tps~33k 溢出)
expire; align; for i in 1 2 3 4 5 6; do ( fire $S 50 20 x '"decoy_ct":99999' >/dev/null ) & done; wait; sleep 4; fire $S 50 20 x '"decoy_ct":99999' >/dev/null; sleep 1
v=$(ev $S); awk "BEGIN{exit !($v>0 && $v<=80)}" && ok "TP18 gmatch 末匹配:decoy99999 被忽略,用真 token → ewma=$v(<=80,非溢出600)" || no "TP18 ewma=$v(若~600=误用 decoy)"

# TP19 opt-in 未配 tps_limit_tps:不限流 + /_tps_status active=false + /_tps_limit warning
expire; estab $NO 50 20 2>/dev/null; r=$(burst $NO 8 50 20); act=$(curl -s "$NO/_tps_status"|python3 -c 'import sys,json;print(json.load(sys.stdin)["active"])')
warn=$(curl -s "$NO/_tps_limit"|python3 -c 'import sys,json;print("warning" in json.load(sys.stdin))')
{ ! echo "$r"|grep -q 429 && [ "$act" = "False" ] && [ "$warn" = "True" ]; } && ok "TP19 opt-in off:不限流($r),active=$act,limit warning=$warn" || no "TP19 r=$r act=$act warn=$warn"

echo ""
echo "================ TPS 套件: PASS=$P FAIL=$F ================"
