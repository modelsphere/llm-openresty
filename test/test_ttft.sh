#!/bin/bash
# TTFT 限流功能 + corner-case 自动化测试(隔离 scratch + mock,自清理,不碰生产)。
# 为加速,把时间尺度/桶调小(逻辑与生产一致):window=3s、ttl=8s、probe_window=3s、
# 小桶 {50,100,200,300,500,800,1200}(溢出 >1200 → P80=2400)、limit=400。
# prefill:100→桶200(正常,<400 不限);800→桶1200(高,>400 限流);1500→溢出 2400。
# 覆盖:alpha钳位 / P80(非均值) / 少样本P80 / 桶溢出 / 限流+半开探测 / 每模型阈值 /
#       非流式不喂 / 错误不喂 / 运行时开关 / 窗口边界折叠 / 空窗口保持+TTL过期 / 正常放行。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/ttfttest_suite}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28901 28902 28903 28904; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
# 动态定位 init_by_lua_block(原硬编码 100 随 config 增长失效:该块函数定义才是要抽的,init_worker 由本测试自带)
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
# lua-refactor: init_by_lua_block 现只 `require "router"`,需 lua_package_path 指向 openresty/lua/ 才能加载模块
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict api_keys 1m;
  lua_shared_dict active_conns_s 4m; lua_shared_dict cluster_avg_s 16k; lua_shared_dict lc_locks_s 1m; lua_shared_dict bad_peers_s 1m; lua_shared_dict bodylog_ctl_s 1m; lua_shared_dict cch_ctl_s 1m;
  lua_shared_dict active_conns_pm 4m; lua_shared_dict cluster_avg_pm 16k; lua_shared_dict lc_locks_pm 1m; lua_shared_dict bad_peers_pm 1m; lua_shared_dict bodylog_ctl_pm 1m; lua_shared_dict cch_ctl_pm 1m;
  lua_shared_dict active_conns_a 4m; lua_shared_dict cluster_avg_a 16k; lua_shared_dict lc_locks_a 1m; lua_shared_dict bad_peers_a 1m; lua_shared_dict bodylog_ctl_a 1m; lua_shared_dict cch_ctl_a 1m;
  lua_shared_dict active_conns_e 4m; lua_shared_dict cluster_avg_e 16k; lua_shared_dict lc_locks_e 1m; lua_shared_dict bad_peers_e 1m; lua_shared_dict bodylog_ctl_e 1m; lua_shared_dict cch_ctl_e 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28901; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TTFT_BUCKETS_MS = {50,100,200,300,500,800,1200}   -- 测试用小桶(溢出>1200→P80=2400)
    local mk={{"127.0.0.1",28901,"m1"},{"127.0.0.1",28902,"m2"},{"127.0.0.1",28903,"m3"}}
    local C={default_max=50,bodylog_default_enabled=false,health_check_interval=5,ttft_window=3,ttft_ttl=8,ttft_probe_window=3,ttft_probe_per_window=5}
    local function R(x) local t={} for k,v in pairs(C) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("s",  function() return R({peers=mk, ttft_limit_ms=400, ttft_ewma_alpha=0.3}) end)
    _G.register_route("pm", function() return R({peers_by_model={["kimi-k2.6"]={{"127.0.0.1",28901,"k1"},{"127.0.0.1",28902,"k2"}},["glm-5.1-fp8"]={{"127.0.0.1",28903,"g1"}}}, ttft_limit_by_model={["kimi-k2.6"]=400,["glm-5.1-fp8"]=1500}}) end)
    _G.register_route("a",  function() return R({peers=mk, ttft_limit_ms=400, ttft_ewma_alpha=30}) end)   -- alpha 钳位
    _G.register_route("e",  function() return R({peers={{"127.0.0.1",28904,"err"}}, ttft_limit_ms=400}) end)
  }
  server { listen 19090; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.s) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.s) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.s) } }
    location = /_ttft_toggle { content_by_lua_block { _G.dbg_ttft_toggle(_G.__route_opts.s) } }
    location = /_ttft_limit { content_by_lua_block { _G.dbg_ttft_limit(_G.__route_opts.s) } } }
  server { listen 19091; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.pm) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.pm) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.pm) } } }
  server { listen 19092; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.a) } } }
  server { listen 19093; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.e) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.e) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.e) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28901 28902 28903; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 5 --chunk-delay-ms 1 --prefill-delay-ms 100 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
setsid nohup "$PY" "$MOCK" --port 28904 --name m-err --chat-status 500 >"$PREFIX/mock_28904.log" 2>&1 & disown
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
body(){ echo "{\"model\":\"${2:-x}\",\"stream\":${3:-true},\"prefill_delay_ms\":$1,\"max_tokens\":5,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"; }
fire(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "$(body "$2" "${3:-x}" "${4:-true}")" "$1/v1/chat/completions"; }   # $1=url $2=prefill $3=model $4=stream
ewv(){ curl -s "$1/_ttft_status" | grep -o "\"${2:-_}\":[0-9.]*" | head -1 | grep -o "[0-9.]*$"; }   # $1=url $2=model-key(default _)
ewj(){ curl -s "$1/_ttft_status" | grep -o '"ewma_ms":{[^}]*}'; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }   # 对齐到 3s 窗口边界
estab(){ align; for pf in $2; do ( fire "$1" "$pf" >/dev/null ) & done; wait; sleep 4; fire "$1" "$3" >/dev/null; sleep 1; }  # $1=url $2=prefill列表 $3=trigger prefill
expire(){ sleep 10; }   # > ttl(8s) 让 EWMA 过期复位
burst(){ local u=$1 n=$2 pf=$3 mdl=${4:-x} d="$PREFIX/b"; rm -f ${d}_*; for i in $(seq 1 $n); do ( fire "$u" "$pf" "$mdl"; echo ) >${d}_$i & done; wait; cat ${d}_* | sort | uniq -c | tr '\n' ' '; }
S=http://127.0.0.1:19090; PM=http://127.0.0.1:19091; AA=http://127.0.0.1:19092; E=http://127.0.0.1:19093

echo "########## 新功能 + corner cases ##########"
# T1 alpha 钳位(配 30 → 1)
[ "$(curl -s $AA/_ttft_status | grep -o '"alpha":[0-9.]*' | grep -o '[0-9.]*$')" = "1" ] && ok "T1 alpha 钳位 30→1" || no "T1 alpha=$(curl -s $AA/_ttft_status|grep -o '\"alpha\":[0-9.]*')"

# T2 P80(非均值):一个窗口 7×100 + 3×800 → 折出 P80(桶1200)而非均值(~310)
expire; align; for i in 1 2 3 4 5 6 7; do ( fire $S 100 >/dev/null ) & done; for i in 1 2 3; do ( fire $S 800 >/dev/null ) & done; wait; sleep 4; fire $S 100 >/dev/null; sleep 1
v=$(ewv $S); awk "BEGIN{exit !($v>=800)}" && ok "T2 窗口 P80=$v(>=800,非均值~310)" || no "T2 ewma=$v(期望 P80~1200)"

# T3 少样本 P80(1 个样本 → 该样本的桶)
expire; estab $S "800" 100; v=$(ewv $S); awk "BEGIN{exit !($v>=800)}" && ok "T3 单样本 P80=$v(=该样本桶 1200)" || no "T3 ewma=$v"

# T4 桶溢出(1500ms > 末桶1200 → P80=2400)
expire; estab $S "1500" 100; v=$(ewv $S); awk "BEGIN{exit !($v>=2000)}" && ok "T4 溢出 P80=$v(=末桶×2=2400)" || no "T4 ewma=$v(期望 2400)"

# T5 限流 + 半开探测(EWMA 高 → burst 精确 5 放行 + 其余 429)
expire; estab $S "800 800 800 800 800 800" 800   # EWMA→1200>400 限流
b=$(burst $S 12 800); c2=$(echo "$b"|grep -o "[0-9]* 200"|grep -o "^[0-9]*"); c4=$(echo "$b"|grep -o "[0-9]* 429"|grep -o "^[0-9]*")
[ "${c2:-0}" = "5" ] && [ "${c4:-0}" -gt 0 ] && ok "T5 限流+探测:200=$c2(=probe 5) 429=$c4" || no "T5 burst=$b"

# T6 窗口边界折叠(burst 后 EWMA 未折,跨窗后才出现)
expire; align; for i in 1 2 3 4 5 6; do ( fire $S 800 >/dev/null ) & done; wait
e1=$(ewj $S)   # 窗口未关
sleep 4; fire $S 100 >/dev/null; sleep 1; e2=$(ewj $S)
[ "$e1" = '"ewma_ms":{}' ] && [ "$e2" != '"ewma_ms":{}' ] && ok "T6 窗口边界折叠:burst后=$e1 跨窗后=$e2" || no "T6 e1=$e1 e2=$e2"

# T7 每模型阈值隔离(kimi 阈值400 / glm 阈值1500;都给 prefill 800→P80 1200)
expire
estab $PM "800 800 800 800" 800   # 这里 model 走 trigger 的默认 x —— 需要分别按 model 建立
# 分别建立两模型 EWMA
align; for i in 1 2 3 4; do ( fire $PM 800 kimi-k2.6 >/dev/null ) & done; for i in 1 2 3 4; do ( fire $PM 800 glm-5.1-fp8 >/dev/null ) & done; wait; sleep 4
fire $PM 800 kimi-k2.6 >/dev/null; fire $PM 800 glm-5.1-fp8 >/dev/null; sleep 1
ck=$(burst $PM 8 800 kimi-k2.6); cg=$(burst $PM 8 800 glm-5.1-fp8)
echo "$ck"|grep -q 429 && ! echo "$cg"|grep -q 429 && ok "T7 每模型阈值:kimi 限流($ck) / glm 不限($cg)" || no "T7 kimi=$ck glm=$cg"

# T8 非流式不喂 EWMA
expire; for i in 1 2 3; do fire $S 800 x false >/dev/null; done; sleep 4; fire $S 800 x false >/dev/null; sleep 1
[ "$(ewj $S)" = '"ewma_ms":{}' ] && ok "T8 非流式不喂(ewma 空)" || no "T8 ewma=$(ewj $S)"

# T9 错误(非2xx)不喂
st=$(fire $E 100); for i in 1 2 3; do fire $E 100 >/dev/null; done; sleep 4
[ "$st" = "500" ] && [ "$(ewj $E)" = '"ewma_ms":{}' ] && ok "T9 错误不喂(后端500, ewma 空)" || no "T9 st=$st ewma=$(ewj $E)"

# T10 运行时开关(toggle off → 旧逻辑放行;on → 限流回来)
expire; estab $S "800 800 800 800 800 800" 800   # 限流态
curl -s "$S/_ttft_toggle?on=0" >/dev/null
off=$(curl -s $S/_ttft_status|grep -o '"active":[a-z]*'); bo=$(burst $S 8 800)
echo "$off"|grep -q false && ! echo "$bo"|grep -q 429 && ok "T10 toggle off → 旧逻辑全放行($bo)" || no "T10 active=$off burst=$bo"
curl -s "$S/_ttft_toggle?on=1" >/dev/null
bn=$(burst $S 10 800); echo "$bn"|grep -q 429 && ok "T10 toggle on → 限流回来($bn)" || no "T10 on burst=$bn"

# T11 空窗口保持(<ttl 8s)
expire; estab $S "800 800 800 800" 800; before=$(ewv $S); sleep 5; held=$(ewv $S)
[ -n "$before" ] && [ "$held" = "$before" ] && ok "T11 空窗口<8s EWMA 保持($before→$held)" || no "T11 before=$before held=$held"

# T12 空窗口超 TTL 过期(>8s)
sleep 6   # 累计 ~11s > 8 TTL
[ "$(ewj $S)" = '"ewma_ms":{}' ] && ok "T12 空窗口>8s TTL 过期(ewma 空)" || no "T12 ewma=$(ewj $S)"

# T13 正常流量不限流
expire; estab $S "100 100 100 100 100 100" 100; v=$(ewv $S)
c=$(fire $S 100); { [ -z "$v" ] || awk "BEGIN{exit !($v<400)}"; } && [ "$c" = "200" ] && ok "T13 正常(ewma=$v<400)→ 200 不限流" || no "T13 ewma=$v code=$c"

# T14 在线热改阈值(/_ttft_limit override 写共享字典,免 reload;静态 limit=400)
expire; estab $S "100 100 100 100" 100; d0=$(fire $S 100)              # EWMA~200 < 静态400 → 200
ov=$(curl -s "$S/_ttft_limit?ms=100" | grep -o '"limit_override_ms":[0-9]*' | grep -o '[0-9]*$')  # override=100
estab $S "100 100 100 100" 100; bl=$(burst $S 10 100)                 # EWMA~200 > override100 → 期望 429
oc=$(curl -s "$S/_ttft_limit?ms=0" | grep -o '"limit_override_ms":[0-9]*')                          # 清除 → 回落400
estab $S "100 100 100 100" 100; bc=$(burst $S 8 100)                  # 200 < 400 → 全 200
[ "$d0" = "200" ] && [ "${ov:-0}" = "100" ] && echo "$bl" | grep -q 429 && [ -z "$oc" ] && ! echo "$bc" | grep -q 429 \
  && ok "T14 在线阈值:默认放行 / override100→限流($bl) / 清除→放行($bc)" || no "T14 d0=$d0 ov=$ov bl=$bl oc=$oc bc=$bc"

echo
echo "================ TTFT 套件: PASS=$P FAIL=$F ================"
