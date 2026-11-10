#!/bin/bash
# Rate-limit master switch (isolated scratch openresty + mock backends; cleans up after itself).
#
# Covers the three load-based 429 exits and what must NOT change with the switch off:
#   S1-S4  static concurrency limit: on -> 429; runtime off -> all 200, counted as would-reject and NOT
#          as real 429s; ?reset=1 -> back to the route/global value; bad ?on -> 400
#   S5-S6  route conf `rate_limit = false` -> source=route, all 200; runtime on=1 wins over it
#   S7     adaptive concurrency route: off -> all 200
#   S8     the runtime override survives `openresty -s reload`
#   S9     TTFT 429: on -> 429; off -> all 200, would_reject.ttft > 0
#   S10    TPS 429 (hard, adaptive_cc=false): on -> 429; off -> all 200, would_reject.tps > 0
#   S11    all peers banned -> still 503 with the switch off (it is not a rate limit)
#   S12    /_tps_status and /_429_status report the switch; /_route_opts lists the option
#   S13    session_prio per-turn concurrency share, below the pool limit: 10 concurrent turn-1
#          requests on a pool of 10 (turn 1's share caps it at 8.5) -> 429 while on; 10 warm
#          turns -> all 200 (control: it is the share, not the pool); off -> turn 1 all 200
#   S14    session_prio graded TTFT gate (EWMA past threshold x severe): turn 1 refused while on;
#          off -> turn 1 and warm turns all 200, would_reject.ttft > 0
#   S15    session_prio turn-1 hold band (flag held, EWMA between threshold x release and 1.0x,
#          no plain trip; reached by tripping at 400ms, then moving the threshold to just above
#          the EWMA with /_ttft_limit): turn 1 refused while on; off -> 200. Only this band
#          reaches the session_prio gating with the switch off -- above 1.0x the plain trip
#          branch takes the request first.
#   S16    session_prio route, TPS hard gate: turn 1 refused while on; off -> all 200
#   S17    toggle input: GET -> 405 (POST only); ?on=0&reset=1 -> 400; ?reset=true accepted; an unparsable route value
#          falls back to the global and reports source=global
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
"$PY" -c 'import aiohttp' 2>/dev/null || { echo "PY=$PY has no aiohttp (the mock needs it); set PY=<python with aiohttp>"; exit 2; }
PREFIX="${PREFIX:-/tmp/rlswitchtest}"; KEY="${API_KEY:-}"
MOCKS="28951 28952 28953"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in $MOCKS; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
cleanup 2>/dev/null; mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

# route -> port
ROUTES="st ac rf tt tp dd sc sq sh sp rv"
port_of(){ case $1 in st) echo 19600;; ac) echo 19601;; rf) echo 19602;; tt) echo 19603;; tp) echo 19604;; dd) echo 19605;; sc) echo 19606;; sq) echo 19607;; sh) echo 19608;; sp) echo 19609;; rv) echo 19610;; esac; }
{
cat <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k; lua_shared_dict session_stat 128k;
EOF
for r in $ROUTES; do echo "  lua_shared_dict active_conns_$r 4m; lua_shared_dict cluster_avg_$r 16k; lua_shared_dict lc_locks_$r 1m; lua_shared_dict bad_peers_$r 1m; lua_shared_dict bodylog_ctl_$r 1m; lua_shared_dict cch_ctl_$r 1m;"; done
cat <<'EOF'
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28951; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.ADAPTIVE_CC_HOLD_WHEN_IDLE = false
    _G.TPS_LIMIT_TPS = 20            -- pinned, not inherited from session_base.conf
    _G.RATE_LIMIT_ENABLED = true     -- pinned likewise
    local tight = {{"127.0.0.1",28951,"m1",0,1},{"127.0.0.1",28952,"m2",0,1}}   -- static max = 2
    local wide  = {{"127.0.0.1",28951,"m1"},{"127.0.0.1",28952,"m2"},{"127.0.0.1",28953,"m3"}}
    local C = {default_max=50, bodylog_default_enabled=false, health_check_interval=2}
    local function R(x) local t={} for k,v in pairs(C) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("st", function() return R({peers=tight, rt_limit_factor=1, adaptive_cc=false}) end)
    _G.register_route("ac", function() return R({peers=tight, rt_limit_factor=1, adaptive_cc=true}) end)
    _G.register_route("rf", function() return R({peers=tight, rt_limit_factor=1, adaptive_cc=false, rate_limit=false}) end)
    _G.register_route("tt", function() return R({peers=wide, adaptive_cc=false, ttft_limit_ms=400, ttft_window=3, ttft_ttl=8,
                                                 ttft_probe_window=3, ttft_probe_per_window=2}) end)
    _G.register_route("tp", function() return R({peers=wide, adaptive_cc=false, tps_limit_tps=50, tps_window=3, tps_ttl=8,
                                                 tps_probe_window=3, tps_probe_per_window=2, tps_min_decode_s=0.3}) end)
    _G.register_route("dd", function() return R({peers={{"127.0.0.1",28959,"dead"}}, health_ban_ttl=300}) end)
    -- session_prio: pool of 10 (2 x 5); turn 1's share 0.85 caps it at 8.5 in flight
    _G.register_route("sc", function() return R({peers={{"127.0.0.1",28951,"m1",0,5},{"127.0.0.1",28952,"m2",0,5}},
                                                 rt_limit_factor=1, adaptive_cc=false, session_prio=true}) end)
    _G.register_route("sq", function() return R({peers=wide, adaptive_cc=false, session_prio=true, ttft_limit_ms=400,
                                                 ttft_window=3, ttft_ttl=8, ttft_probe_window=3, ttft_probe_per_window=2}) end)
    _G.register_route("sh", function() return R({peers=wide, adaptive_cc=false, session_prio=true, ttft_limit_ms=400,
                                                 ttft_window=3, ttft_ttl=30, ttft_probe_window=3, ttft_probe_per_window=2}) end)
    _G.register_route("sp", function() return R({peers=wide, adaptive_cc=false, session_prio=true, tps_limit_tps=50, tps_window=3,
                                                 tps_ttl=8, tps_probe_window=3, tps_probe_per_window=2, tps_min_decode_s=0.3}) end)
    _G.register_route("rv", function() return R({peers=tight, rt_limit_factor=1, adaptive_cc=false, rate_limit="maybe"}) end)
  }
EOF
for r in $ROUTES; do p=$(port_of $r); cat <<EOF
  server { listen $p; server_name _; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$r) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.$r) } }
    location = /_rate_limit_status { content_by_lua_block { _G.dbg_rate_limit_status(_G.__route_opts.$r) } }
    location = /_rate_limit_toggle { content_by_lua_block { _G.dbg_rate_limit_toggle(_G.__route_opts.$r) } }
    location = /_429_status { content_by_lua_block { _G.dbg_429_status(_G.__route_opts.$r) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.$r) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.$r) } }
    location = /_ttft_limit { content_by_lua_block { _G.dbg_ttft_limit(_G.__route_opts.$r) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.$r) } }
    location = /_route_opts { content_by_lua_block { _G.dbg_route_opts(_G.__route_opts.$r) } } }
EOF
done
echo "}"
} > "$PREFIX/nginx.conf"

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in $MOCKS; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 10 --chunk-delay-ms 5 --prefill-delay-ms 20 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
U(){ echo "http://127.0.0.1:$(port_of $1)"; }
js(){ "$PY" -c "import sys,json;d=json.load(sys.stdin);print(eval(sys.argv[1]))" "$1"; }
st(){ curl -s "$(U $1)/_rate_limit_status" | js "$2"; }
tog(){ curl -s -X POST "$(U $1)/_rate_limit_toggle?$2"; }
# one request: <route> <prefill_ms> <chunk_ms> <max_tokens> [stream=0|1] [turn: sends x-phanrouter-session]
fire(){ local s=${5:-0} uo='' ph="X-No-Session: 1"; [ "$s" = 1 ] && uo='"stream_options":{"include_usage":true},'
  [ -n "${6:-}" ] && ph="x-phanrouter-session: {\"session_id\":\"s-$6-$RANDOM$RANDOM\",\"turn\":$6}"
  curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -H "$ph" \
    -d "{${uo}\"model\":\"x\",\"stream\":$([ "$s" = 1 ] && echo true || echo false),\"prefill_delay_ms\":$2,\"chunk_delay_ms\":$3,\"max_tokens\":$4,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
    "$(U $1)/v1/chat/completions"; }
# N concurrent requests -> "count code" summary
burst(){ local r=$1 n=$2 d="$PREFIX/b_$1"; shift 2; rm -f ${d}_*
  for i in $(seq 1 $n); do ( fire "$r" "$@"; echo ) >${d}_$i & done; wait; cat ${d}_* | sort | uniq -c | tr '\n' ' '; }
has(){ echo "$1" | grep -q " $2"; }
# allok <summary> <n>: every one of n requests answered 200 (a 502 from a dead mock must not pass as "no 429")
allok(){ [ "$(echo "$1" | xargs)" = "$2 200" ]; }
# limited <summary>: some 429, and every other answer a 200
limited(){ has "$1" 429 && [ -z "$(echo "$1" | xargs -n2 | awk '$2!=200 && $2!=429')" ]; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
estab(){ local r=$1; shift; align; for i in 1 2 3 4 5 6; do ( fire "$r" "$@" >/dev/null ) & done; wait; sleep 4; fire "$r" "$@" >/dev/null; sleep 1; }
real429(){ curl -s "$(U $1)/_429_status" | js "d['by_reason']['$2']"; }

echo "############ rate-limit master switch ############"
# ── static concurrency ──
[ "$(st st "d['enabled']")" = "True" ] && [ "$(st st "d['source']")" = "global" ] && ok "S0 st: on by default, source=global" || no "S0 st default: $(curl -s $(U st)/_rate_limit_status)"
r=$(burst st 20 1500 5 5); limited "$r" && ok "S1 st on: concurrency 429s ($r)" || no "S1 st on, no 429: $r"
c0=$(real429 st concurrency)
o=$(tog st on=0); [ "$(echo "$o" | js "d['enabled']")" = "False" ] && [ "$(echo "$o" | js "d['source']")" = "runtime" ] && ok "S2 toggle on=0 -> enabled=false, source=runtime" || no "S2 toggle: $o"
r=$(burst st 20 1500 5 5); allok "$r" 20 && ok "S2 st off: all 200 ($r)" || no "S2 st off: $r"
w=$(st st "d['would_reject']['concurrency']"); [ "$w" -gt 0 ] 2>/dev/null && ok "S2 would_reject.concurrency=$w" || no "S2 would_reject not counted: $w"
c1=$(real429 st concurrency); [ "$c1" = "$c0" ] && ok "S2 real 429 counter unchanged ($c0)" || no "S2 real 429 counter moved $c0 -> $c1"
o=$(tog st reset=1); [ "$(echo "$o" | js "d['enabled']")" = "True" ] && [ "$(echo "$o" | js "d['source']")" = "global" ] && [ "$(echo "$o" | js "d.get('runtime_override')")" = "None" ] && ok "S3 reset=1 -> back to global" || no "S3 reset: $o"
r=$(burst st 20 1500 5 5); limited "$r" && ok "S3 st on again: 429 ($r)" || no "S3 not enforced after reset: $r"
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$(U st)/_rate_limit_toggle?on=maybe"); [ "$code" = 400 ] && ok "S4 ?on=maybe -> 400" || no "S4 bad on -> $code"
o=$(tog st reset_counts=1); [ "$(echo "$o" | js "d['would_reject']['concurrency']")" = "0" ] && ok "S4 reset_counts=1 zeroes the counts" || no "S4 reset_counts: $o"

# ── route conf rate_limit=false ──
[ "$(st rf "d['enabled']")" = "False" ] && [ "$(st rf "d['source']")" = "route" ] && ok "S5 rf: rate_limit=false -> enabled=false, source=route" || no "S5 rf: $(curl -s $(U rf)/_rate_limit_status)"
r=$(burst rf 20 1500 5 5); allok "$r" 20 && ok "S5 rf: all 200 ($r)" || no "S5 rf: $r"
tog rf on=1 >/dev/null; r=$(burst rf 20 1500 5 5); limited "$r" && ok "S6 runtime on=1 beats route false: 429 ($r)" || no "S6 override not applied: $r"
tog rf reset=1 >/dev/null

# ── adaptive concurrency ──
r=$(burst ac 20 1500 5 5); limited "$r" && ok "S7 ac on: 429 ($r)" || no "S7 ac on, no 429: $r"
tog ac on=0 >/dev/null; r=$(burst ac 20 1500 5 5); allok "$r" 20 && ok "S7 ac off: all 200 ($r)" || no "S7 ac off: $r"

# ── survives reload ──
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s reload; sleep 3
[ "$(st ac "d['enabled']")" = "False" ] && [ "$(st ac "d['source']")" = "runtime" ] && ok "S8 override survives reload" || no "S8 after reload: $(curl -s $(U ac)/_rate_limit_status)"
r=$(burst ac 20 1500 5 5); allok "$r" 20 && ok "S8 still let through after reload ($r)" || no "S8 after reload: $r"
tog ac reset=1 >/dev/null

# ── TTFT ── (prefill 800ms > limit 400ms)
estab tt 800 5 20 1
r=$(burst tt 10 800 5 20 1); limited "$r" && ok "S9 tt on: TTFT 429 ($r)" || no "S9 tt on, no 429 (precondition): $r"
tog tt on=0 >/dev/null; r=$(burst tt 10 800 5 20 1); allok "$r" 10 && ok "S9 tt off: all 200 ($r)" || no "S9 tt off: $r"
w=$(st tt "d['would_reject']['ttft']"); [ "$w" -gt 0 ] 2>/dev/null && ok "S9 would_reject.ttft=$w" || no "S9 would_reject.ttft=$w"
tog tt reset=1 >/dev/null

# ── TPS (hard) ── (50ms/chunk = 20 tok/s < limit 50)
estab tp 20 50 20 1
r=$(burst tp 10 20 50 20 1); limited "$r" && ok "S10 tp on: TPS 429 ($r)" || no "S10 tp on, no 429 (precondition): $r"
tog tp on=0 >/dev/null; r=$(burst tp 10 20 50 20 1); allok "$r" 10 && ok "S10 tp off: all 200 ($r)" || no "S10 tp off: $r"
w=$(st tp "d['would_reject']['tps']"); [ "$w" -gt 0 ] 2>/dev/null && ok "S10 would_reject.tps=$w" || no "S10 would_reject.tps=$w"
tog tp reset=1 >/dev/null

# ── all banned -> 503 regardless ──
tog dd on=0 >/dev/null; fire dd 20 5 5 >/dev/null; sleep 8
code=$(fire dd 20 5 5); [ "$code" = 503 ] && ok "S11 all peers banned -> 503 with the switch off" || no "S11 dead pool -> $code"

# ── reporting ──
v=$(curl -s "$(U st)/_tps_status" | js "(d['rate_limit_enabled'], d['rate_limit_source'])"); [ "$v" = "(True, 'global')" ] && ok "S12 /_tps_status reports the switch $v" || no "S12 tps_status: $v"
v=$(curl -s "$(U st)/_429_status?all=1" | js "sorted(d['rate_limit'].keys())"); echo "$v" | grep -q "'tt'" && ok "S12 /_429_status?all=1 lists every route's switch" || no "S12 429_status: $v"
v=$(curl -s "$(U st)/_429_status" | js "list(d['by_reason'].keys())"); echo "$v" | grep -q would && no "S12 would-reject leaked into by_reason: $v" || ok "S12 by_reason holds real 429s only"
v=$(curl -s "$(U rf)/_route_opts" | js "[x for x in d.get('directives',[]) if x['key']=='rate_limit'][0]['value']"); [ "$v" = "False" ] && ok "S12 /_route_opts: rate_limit value=False on rf" || no "S12 route_opts: $v"

# ── session_prio: per-turn concurrency share ── (pool of 10; turn 1 capped at 8.5)
# 10 concurrent: the 10th sees 9 in flight -- over turn 1's 8.5, under the pool's 10.
r=$(burst sc 10 1500 5 5 0 40); allok "$r" 10 && ok "S13 sc on, control: 10 warm turns all 200 ($r)" || no "S13 control, warm turns refused: $r"
r=$(burst sc 10 1500 5 5 0 1); limited "$r" && ok "S13 sc on: turn-1 share 429 below the pool limit ($r)" || no "S13 sc on, no 429 (precondition): $r"
tog sc on=0 >/dev/null; r=$(burst sc 10 1500 5 5 0 1); allok "$r" 10 && ok "S13 sc off: turn 1 all 200 ($r)" || no "S13 sc off: $r"
w=$(st sc "d['would_reject']['concurrency']"); [ "$w" -gt 0 ] 2>/dev/null && ok "S13 would_reject.concurrency=$w" || no "S13 would_reject not counted: $w"
tog sc reset=1 >/dev/null

# ── session_prio: graded TTFT ── (warm turns build EWMA ~800ms >= 400 x 1.5 = hard trip)
estab sq 800 5 20 1 40
r=$(burst sq 10 800 5 20 1 1); limited "$r" && ok "S14 sq on: turn-1 TTFT 429 ($r)" || no "S14 sq on, no 429 (precondition): $r"
tog sq on=0 >/dev/null
r=$(burst sq 10 800 5 20 1 1); allok "$r" 10 && ok "S14 sq off: turn 1 all 200 ($r)" || no "S14 sq off, turn 1: $r"
r=$(burst sq 10 800 5 20 1 40); allok "$r" 10 && ok "S14 sq off: warm turn 40 all 200 ($r)" || no "S14 sq off, turn 40: $r"
w=$(st sq "d['would_reject']['ttft']"); [ "$w" -gt 0 ] 2>/dev/null && ok "S14 would_reject.ttft=$w" || no "S14 would_reject.ttft=$w"
tog sq reset=1 >/dev/null

# ── session_prio: turn-1 hold band ── warm turns trip at 400ms and set the hold flag; then the
# threshold moves to EWMA/0.95 with /_ttft_limit, so the EWMA sits between threshold x release (0.9)
# and 1.0x: no plain trip, flag still held -> only the session_prio gating can refuse turn 1.
# (The EWMA is bucketed, so moving the threshold is the deterministic way into the band.)
ewma(){ curl -s "$(U $1)/_ttft_status" | js "(d.get('ewma_ms') or {}).get('_') or 0"; }
held(){ curl -s "$(U $1)/_session_status" | js "d['pools'][0]['turn1_hold']"; }
estab sh 800 5 20 1 40
# The flag is written by a request that sees the trip; estab's last request can arrive before the
# EWMA exists, so send warm requests until one sets it.
for i in 1 2 3 4 5; do [ "$(held sh)" = True ] && break; fire sh 800 5 20 1 40 >/dev/null; sleep 1; done
e=$(ewma sh)
lim=$("$PY" -c "import sys; print(int(float(sys.argv[1]) / 0.95))" "$e")
curl -s "$(U sh)/_ttft_limit?ms=$lim&ttl=120" >/dev/null
if [ "$(held sh)" = True ] && "$PY" -c "import sys; e,l=float(sys.argv[1]),float(sys.argv[2]); sys.exit(0 if 0.9*l < e < l else 1)" "$e" "$lim"; then
  ok "S15 precondition: hold flag set, EWMA $e in the hold band of threshold $lim"
  r=$(fire sh 20 5 20 1 1); [ "$r" = 429 ] && ok "S15 sh on: turn 1 refused in the hold band ($r)" || no "S15 sh on, turn 1 not held (precondition): $r"
  tog sh on=0 >/dev/null
  r=$(fire sh 20 5 20 1 1); [ "$r" = 200 ] && ok "S15 sh off: turn 1 let through in the hold band ($r)" || no "S15 sh off: $r"
  tog sh reset=1 >/dev/null
else
  no "S15 precondition: hold=$(held sh) EWMA $e threshold $lim"
fi
curl -s "$(U sh)/_ttft_limit?ms=0" >/dev/null

# ── session_prio: TPS hard gate ── (50ms/chunk = 20 tok/s < limit 50)
estab sp 20 50 20 1 40
r=$(burst sp 10 20 50 20 1 1); limited "$r" && ok "S16 sp on: turn-1 TPS 429 ($r)" || no "S16 sp on, no 429 (precondition): $r"
tog sp on=0 >/dev/null; r=$(burst sp 10 20 50 20 1 1); allok "$r" 10 && ok "S16 sp off: all 200 ($r)" || no "S16 sp off: $r"
w=$(st sp "d['would_reject']['tps']"); [ "$w" -gt 0 ] 2>/dev/null && ok "S16 would_reject.tps=$w" || no "S16 would_reject.tps=$w"
tog sp reset=1 >/dev/null

# ── toggle input ──
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$(U st)/_rate_limit_toggle?on=0&reset=1"); [ "$code" = 400 ] && ok "S17 ?on=0&reset=1 -> 400" || no "S17 contradictory toggle -> $code"
code=$(curl -s -o /dev/null -w "%{http_code}" "$(U st)/_rate_limit_toggle?on=0"); [ "$code" = 405 ] && ok "S17 GET toggle -> 405" || no "S17 GET toggle -> $code"
[ "$(st st "d['source']")" = "global" ] && ok "S17 refused toggle changed nothing" || no "S17 refused toggle still applied: $(st st "d['source']")"
tog st on=0 >/dev/null; o=$(tog st reset=true); [ "$(echo "$o" | js "d['source']")" = "global" ] && ok "S17 ?reset=true accepted" || no "S17 reset=true: $o"
[ "$(st rv "d['enabled']")" = "True" ] && [ "$(st rv "d['source']")" = "global" ] && ok "S17 rate_limit=\"maybe\" falls back, source=global" || no "S17 invalid route value: $(curl -s $(U rv)/_rate_limit_status)"

echo
echo "================ rate-limit switch: PASS=$P FAIL=$F ================"
[ "$F" = "0" ] && exit 0 || exit 1
