#!/bin/bash
# SLO(LLMSLORequirement CRD)驱动限流阈值/分位数 —— 隔离 scratch + mock,自清理,不碰生产。
#
# 覆盖 P1 的核心承诺:
#   优先级链 override > CRD > factory opts > _G;CRD 未覆盖/未接 → 回落静态(裸机零影响);
#   坏 json 不污染好数据;CRD 阈值**真的**驱动 429;avg 与多指标端到端(P0 里测不到的两条路径)。
#
# 时间尺度调小(逻辑与生产一致):ttft_window=3s / ttl=8s / probe_window=3s。
# 小桶 {50,100,200,300,500,800,1200};mock prefill 100→桶200、800→桶1200。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/slotest_suite}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28941 28942; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 1m; lua_shared_dict tps_stat 1m; lua_shared_dict api_keys 1m;
  # ⚠️ per-route dict 名由**路由名**派生(active_conns_<route> 等),必须与 register_route 的名字一致,
  #    否则 register_route 报 "required shared_dict ... not declared",请求全 500。
  lua_shared_dict active_conns_slo 4m; lua_shared_dict cluster_avg_slo 16k; lua_shared_dict lc_locks_slo 1m; lua_shared_dict bad_peers_slo 1m; lua_shared_dict bodylog_ctl_slo 1m; lua_shared_dict cch_ctl_slo 1m;
  lua_shared_dict active_conns_nocrd 4m; lua_shared_dict cluster_avg_nocrd 16k; lua_shared_dict lc_locks_nocrd 1m; lua_shared_dict bad_peers_nocrd 1m; lua_shared_dict bodylog_ctl_nocrd 1m; lua_shared_dict cch_ctl_nocrd 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TTFT_BUCKETS_MS = {50,100,200,300,500,800,1200}
    local mk={{"127.0.0.1",28941,"m1"},{"127.0.0.1",28942,"m2"}}
    local C={default_max=50,bodylog_default_enabled=false,health_check_interval=5,
             ttft_window=3,ttft_ttl=8,ttft_probe_window=3,ttft_probe_per_window=5}
    local function R(x) local t={} for k,v in pairs(C) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    -- slo:被 CRD 覆盖的路由;nocrd:CRD 里没有它 → 必须回落静态
    _G.register_route("slo",   function() return R({peers=mk, ttft_limit_ms=400, ttft_ewma_alpha=1.0}) end)
    _G.register_route("nocrd", function() return R({peers=mk, ttft_limit_ms=777, ttft_ewma_alpha=1.0}) end)
  }
  server { listen 19110; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.slo) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.slo) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.slo) } }
    location = /_ttft_limit  { content_by_lua_block { _G.dbg_ttft_limit(_G.__route_opts.slo) } }
    location = /_slo_conf    { content_by_lua_block { _G.dbg_slo_conf() } } }
  server { listen 19111; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.nocrd) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28941 28942; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 5 --chunk-delay-ms 1 --prefill-delay-ms 100 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
S=http://127.0.0.1:19110; N=http://127.0.0.1:19111

body(){ echo "{\"model\":\"x\",\"stream\":true,\"prefill_delay_ms\":$1,\"max_tokens\":5,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"; }
fire(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "$(body "$2")" "$1/v1/chat/completions"; }
# jq 不一定有 → 统一用 python 解析 JSON(别用 grep 扒原始文本,见 SKILL.md 的 ewv() 陷阱)
jget(){ curl -s "$1" | python3 -c "import sys,json;d=json.load(sys.stdin);$2" 2>/dev/null; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
estab(){ align; for pf in $2; do ( fire "$1" "$pf" >/dev/null ) & done; wait; sleep 4; fire "$1" "$3" >/dev/null; sleep 1; }
expire(){ sleep 10; }

echo "########## SLO / CRD 驱动阈值 ##########"

# ── C1-C3 未注入:全部回落静态,与接 CRD 前一致 ─────────────────────────────
src=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["source"])')
[ "$src" = "static" ] && ok "C1 未注入 CRD → source=static" || no "C1 source=$src"
th=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["items"][0]["threshold_ms"])')
[ "$th" = "400" ] && ok "C2 阈值=factory opts 的 400" || no "C2 threshold=$th"
ver=$(jget "$S/_ttft_status" 'print(d.get("slo_version"))')
[ "$ver" = "None" ] && ok "C3 slo_version 为空(未接 CRD)" || no "C3 slo_version=$ver"

# ── C4-C7 POST 注入 → CRD 生效 ─────────────────────────────────────────────
POSTJSON='{"version":42,"routes":{"slo":{"__default__":{
  "ttft":{"default":{"metrics":[{"metric":"p80","q":0.8,"threshold_ms":900}]},"ranges":[]}}}}}'
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$H" -d "$POSTJSON" "$S/_slo_conf")
[ "$code" = "200" ] && ok "C4 POST /_slo_conf 注入成功" || no "C4 code=$code"
src=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["source"])')
[ "$src" = "crd" ] && ok "C5 source 切到 crd" || no "C5 source=$src"
th=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["items"][0]["threshold_ms"])')
[ "$th" = "900" ] && ok "C6 阈值改用 CRD 的 900(压过 factory 400)" || no "C6 threshold=$th"
ver=$(jget "$S/_ttft_status" 'print(d.get("slo_version"))')
[ "$ver" = "42" ] && ok "C7 slo_version=42" || no "C7 slo_version=$ver"

# ── C8 CRD 里没有的 route 必须仍走静态(裸机/未覆盖场景的核心保证)────────────
src=$(jget "$N/_ttft_status" 'print(d["metrics"][0]["source"])')
th=$(jget "$N/_ttft_status" 'print(d["metrics"][0]["items"][0]["threshold_ms"])')
{ [ "$src" = "static" ] && [ "$th" = "777" ]; } && ok "C8 未被 CRD 覆盖的 route 仍走静态(777)" || no "C8 src=$src th=$th"

# ── C9-C10 override 压过 CRD(应急口子)────────────────────────────────────
curl -s "$S/_ttft_limit?ms=1234" >/dev/null
src=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["source"])')
th=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["items"][0]["threshold_ms"])')
{ [ "$src" = "override" ] && [ "$th" = "1234" ]; } && ok "C9 override 压过 CRD(1234)" || no "C9 src=$src th=$th"
curl -s "$S/_ttft_limit?ms=0" >/dev/null      # 清除 override
src=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["source"])')
[ "$src" = "crd" ] && ok "C10 清除 override → 回落 CRD" || no "C10 src=$src"

# ── C11-C12 坏 json 被拒且不污染现有数据 ───────────────────────────────────
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$H" -d '{"routes":"坏"}' "$S/_slo_conf")
[ "$code" = "400" ] && ok "C11 坏 json 返 400" || no "C11 code=$code"
th=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["items"][0]["threshold_ms"])')
[ "$th" = "900" ] && ok "C12 坏注入后**旧数据仍在**(900 未被清)" || no "C12 threshold=$th"

# ── C13 CRD 阈值真的驱动 429(不只是显示)──────────────────────────────────
# 注入一个极低阈值 → 正常 100ms prefill(桶200)也超标 → 应触发 TTFT 429
expire
curl -s -X POST -H "$H" -d '{"version":43,"routes":{"slo":{"__default__":{
  "ttft":{"default":{"metrics":[{"metric":"p80","q":0.8,"threshold_ms":50}]},"ranges":[]}}}}}' "$S/_slo_conf" >/dev/null
estab $S "100 100 100 100 100" 100
codes=$(for i in 1 2 3 4 5 6 7 8 9 10; do fire $S 100; echo; done | sort | uniq -c | tr '\n' ' ')
echo "$codes" | grep -q 429 && ok "C13 CRD 的低阈值(50ms)真的触发了 429 [$codes]" || no "C13 未触发 429 [$codes]"

# ── C14 高阈值 → 不限流(证明确实是阈值在起作用,不是别的原因)────────────
expire
curl -s -X POST -H "$H" -d '{"version":44,"routes":{"slo":{"__default__":{
  "ttft":{"default":{"metrics":[{"metric":"p80","q":0.8,"threshold_ms":99000}]},"ranges":[]}}}}}' "$S/_slo_conf" >/dev/null
estab $S "100 100 100" 100
codes=$(for i in 1 2 3 4 5; do fire $S 100; echo; done | sort | uniq -c | tr '\n' ' ')
# ⚠️ 必须**显式要求有 200**:只断言「没有 429」的话,请求全 500 也会假通过(踩过一次)
{ echo "$codes" | grep -q 200 && ! echo "$codes" | grep -q 429; } \
  && ok "C14 高阈值(99s)不限流,且请求确实成功 [$codes]" || no "C14 [$codes](需有 200 且无 429)"

# ── C15-C16 avg 指标端到端(P0 里没有配置能触达的路径)──────────────────────
expire
curl -s -X POST -H "$H" -d '{"version":45,"routes":{"slo":{"__default__":{
  "ttft":{"default":{"metrics":[{"metric":"avg","threshold_ms":99000}]},"ranges":[]}}}}}' "$S/_slo_conf" >/dev/null
m=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["items"][0]["metric"])')
[ "$m" = "avg" ] && ok "C15 metric 切到 avg" || no "C15 metric=$m"
# 7×100ms + 3×800ms:均值≈310(桶均值口径),P80 会是 1200 —— 两者必须明显不同
estab $S "100 100 100 100 100 100 100 800 800 800" 100
av=$(jget "$S/_ttft_status" 'print(d["metrics"][0]["items"][0]["ewma_ms"])')
awk "BEGIN{exit !($av>0 && $av<800)}" 2>/dev/null \
  && ok "C16 avg EWMA=$av(在均值量级,明显低于 P80 的 1200)" || no "C16 avg ewma=$av(期望 0<avg<800)"

# ── C17-C18 多指标 OR:两条指标,低阈值那条应触发 ──────────────────────────
expire
curl -s -X POST -H "$H" -d '{"version":46,"routes":{"slo":{"__default__":{
  "ttft":{"default":{"metrics":[{"metric":"p50","q":0.5,"threshold_ms":99000},
                                {"metric":"p95","q":0.95,"threshold_ms":50}]},"ranges":[]}}}}}' "$S/_slo_conf" >/dev/null
n=$(jget "$S/_ttft_status" 'print(len(d["metrics"][0]["items"]))')
[ "$n" = "2" ] && ok "C17 两条指标都在(共享同一份直方图)" || no "C17 指标数=$n"
estab $S "100 100 100 100 100" 100
codes=$(for i in 1 2 3 4 5 6 7 8 9 10; do fire $S 100; echo; done | sort | uniq -c | tr '\n' ' ')
echo "$codes" | grep -q 429 && ok "C18 OR 语义:p95 那条超标即触发 429 [$codes]" || no "C18 未触发 [$codes]"

echo
echo "================ SLO/CRD 套件: PASS=$P FAIL=$F ================"
[ "$F" -eq 0 ]
