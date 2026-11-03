#!/bin/bash
# session_prio (session turn-priority admission) integration test, in the
# usual isolated scratch prefix with mock backends; self-cleaning, never
# touches production. Every scenario is written so it FAILS when the
# behaviour it pins is broken (the mutation proofs live in the task log).
#
# Coverage:
#   S1  feature off: byte-identical (no turn_bucket in bodies, header ignored,
#       zero counters)
#   S2  concurrency share grading, pool of 20 (per-peer 10 x 2): at rt_sum=17
#       turn1 429 vs turn8/turn40 200; at rt_sum=19 turn8 429 vs turn40 200
#   S3  TTFT soft zone: turn1 429 from the EWMA alone; turn8 (tol bar 520 >
#       EWMA 500) always 200 with no probe spent; turn2 (bar 440) spends the
#       probe budget then 429s
#   S4  TTFT hard trip: everyone 429; turn>=2 / nohdr keep probe slots
#   S5  turn-1 hysteresis band [release,1.0x): turn1 429 with sp_zone=hold
#       while warm turns pass; the flag clears when the EWMA falls under
#       release and turn1 is admitted again
#   S6  nohdr: bad header treated as warm, counted under bnohdr separately
#   S7  gate counters: rej_conc/rej_ttft moved during the earlier sections
#   S8  token path: a cached_tokens-emitting backend accrues exact ptok/ctok
#       and the /_session_status hit rate equals ctok/ptok; without the field
#       no_cached_field counts and ptok/ctok stay 0
#   S9  TPS gate grading (adaptive_cc off): soft zone turn1 429 vs warm 200;
#       hard trip probes only for turn>=2
#   S10 runtime toggle off restores today's behaviour
#   S11 turn-1 probe budget: N turn-1 passes per window on its own counter,
#       0 passes once a warm probe opened a slot that window, reset next window
#
# Harness notes (learned the hard way):
#   * TTFT samples only come from STREAMING requests; the concurrency holders
#     are slow streams, so their TTFT raises the EWMA. Every measurement
#     section uses DISTINCT session ids so no sticky-holder holds the EWMA.
#   * alpha=0.3 (production-style): three samples in a window fold to
#     alpha*P80 as the first EWMA, one later window folds it to full P80.
#   * A TPS-gate route must say adaptive_cc=false explicitly: with
#     _G.TPS_LIMIT_TPS set globally, the factory default otherwise picks the
#     AIMD path and the TPS 429 never fires.
# Timing: window=3s, ttl=8s. TTFT buckets {..,380,420,500,800} let the EWMA
# land exactly in the hold band [360,400) or the soft band [400,600).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/sptest}"; KEY="${API_KEY:-}"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in 28941 28942 28943 28944; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
# lua-refactor: init_by_lua_block only does `require "router"`; point lua_package_path at openresty/lua/.
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 1m; lua_shared_dict tps_stat 1m; lua_shared_dict api_keys 1m; lua_shared_dict session_stat 1m;
  lua_shared_dict active_conns_off 4m; lua_shared_dict cluster_avg_off 16k; lua_shared_dict lc_locks_off 1m; lua_shared_dict bad_peers_off 1m; lua_shared_dict bodylog_ctl_off 1m; lua_shared_dict cch_ctl_off 1m;
  lua_shared_dict active_conns_sp 4m; lua_shared_dict cluster_avg_sp 16k; lua_shared_dict lc_locks_sp 1m; lua_shared_dict bad_peers_sp 1m; lua_shared_dict bodylog_ctl_sp 1m; lua_shared_dict cch_ctl_sp 1m;
  lua_shared_dict active_conns_tpx 4m; lua_shared_dict cluster_avg_tpx 16k; lua_shared_dict lc_locks_tpx 1m; lua_shared_dict bad_peers_tpx 1m; lua_shared_dict bodylog_ctl_tpx 1m; lua_shared_dict cch_ctl_tpx 1m;
  lua_shared_dict active_conns_spc 4m; lua_shared_dict cluster_avg_spc 16k; lua_shared_dict lc_locks_spc 1m; lua_shared_dict bad_peers_spc 1m; lua_shared_dict bodylog_ctl_spc 1m; lua_shared_dict cch_ctl_spc 1m;
  lua_shared_dict active_conns_tp 4m; lua_shared_dict cluster_avg_tp 16k; lua_shared_dict lc_locks_tp 1m; lua_shared_dict bad_peers_tp 1m; lua_shared_dict bodylog_ctl_tp 1m; lua_shared_dict cch_ctl_tp 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    -- TTFT buckets chosen so a prefill delay lands on an exact EWMA:
    --   prefill 350 -> P80 380 -> fold  0.3*380=114, then 380 (hold band 360-400)
    --   prefill 450 -> P80 500 -> fold  150, then 500     (soft band 400-600)
    --   prefill 600 -> P80 800 -> fold  240, then 800     (hard trip >= 600)
    _G.TTFT_BUCKETS_MS = {50,100,200,300,380,420,500,800,1200}
    -- OTPS = output_len(20)/total_time. prefill 100 -> ~59 (soft), 310 -> ~20 (hard).
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.TPS_LIMIT_TPS = nil   -- no global TPS threshold: routes that want the
                             -- TPS gate set it themselves (tp / spc / offC)
    local mk ={{"127.0.0.1",28941,"m1"},{"127.0.0.1",28942,"m2"}}
    local mkc={{"127.0.0.1",28943,"c1"}}
    local mkt={{"127.0.0.1",28944,"t1"}}
    local C={bodylog_default_enabled=false,health_check_interval=5,
             ttft_window=3,ttft_ttl=8,ttft_probe_window=3,ttft_probe_per_window=2,
             tps_window=3,tps_ttl=8,tps_probe_window=3,tps_probe_per_window=2,
             -- MOCK responses are ~120ms with 5-20 tokens: drop both TPS noise
             -- floors or every sample is skipped (the reason S9 recorded nothing).
             tps_min_tokens=1,tps_min_decode_s=0.05,ttft_limit_ms=400}
    local function R(x) local t={} for k,v in pairs(C) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    -- off / offC: comparison routes, feature off (so the built-in adaptive
    -- concurrency default is ON; the S1 comparison stays the production shape).
    _G.register_route("off",  function() return R({peers=mk,  default_max=10}) end)
    _G.register_route("offC", function() return R({peers=mkc, default_max=10, tps_limit_tps=50}) end)
    -- sp: feature on. TPS off on this route so the TPS gate cannot interfere:
    -- adaptive_cc=false removes the TPS threshold AND the AIMD path on it.
    _G.register_route("sp",  function() return R({peers=mk,  default_max=10, session_prio=true, adaptive_cc=false,
                                                  -- S5 needs the hold band [release x 400, 400) reachable by a
                                                  -- P80 walk before the flag ttl (8s) runs out; 0.6 widens it.
                                                  session_prio_release=0.6,
                                                  -- 0 = turn 1 never probes (the shape S3-S6 pin); the
                                                  -- budget lives on tpx, exercised by S11.
                                                  session_prio_t1_probes=0}) end)
    -- spc: S8 hit-rate pool. Token accounting reads the TPS tail, which only
    -- exists while TPS measurement is on -- so this route KEEPS a TPS
    -- threshold (the AIMD default then also applies, harmless for two
    -- requests) and deliberately does NOT set adaptive_cc=false.
    _G.register_route("spc", function() return R({peers=mkc, default_max=10, session_prio=true, tps_limit_tps=50}) end)
    -- tp: S9, TPS hard-fuse gate under the feature.
    _G.register_route("tp",  function() return R({peers=mkt, default_max=10, session_prio=true,
                                                  adaptive_cc=false, tps_limit_tps=50, session_prio_t1_probes=0}) end)
    -- tpx: identical gates with the DEFAULT turn-1 probe budget (1): S11.
    _G.register_route("tpx", function() return R({peers=mkt, default_max=10, session_prio=true,
                                                  adaptive_cc=false, tps_limit_tps=50, ttft_limit_ms=400}) end)
  }
  server { listen 19100; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.off) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.off) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.off) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.off) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.off) } } }
  server { listen 19101; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.sp) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.sp) } }
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts.sp) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.sp) } }
    location = /_session_prio_toggle { content_by_lua_block { _G.dbg_session_prio_toggle(_G.__route_opts.sp) } } }
  server { listen 19102; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.spc) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.spc) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.spc) } } }
  server { listen 19103; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.tp) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.tp) } }
    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.tp) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.tp) } } }
  server { listen 19105; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.tpx) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.tpx) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.tpx) } } }
  server { listen 19104; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.offC) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.offC) } }
    location = /_session_status { content_by_lua_block { _G.dbg_session_status(_G.__route_opts.offC) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in 28941 28942 28943 28944; do
  extra=(); [ "$p" = "28943" ] && extra=(--cached-tokens 3)
  setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 100 "${extra[@]}" >"$PREFIX/mock_$p.log" 2>&1 & disown
done
sleep 2
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
# fire $1=url $2=prefill_ms $3=turn("-" = no header) $4=tag(unique session) -> http_code
# The session id is built from turn+tag, so every section can start with
# fresh sessions (critical: a sticky holder would re-feed an EWMA).
fire(){ local hdr=(); if [ "${3:-}" != "-" ]; then hdr=(-H "x-phanrouter-session: {\"session_id\":\"s-${3}-${4:-d}\",\"turn\":$3}"); fi
  curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" "${hdr[@]}" \
       -d "{\"model\":\"x\",\"stream\":true,\"stream_options\":{\"include_usage\":true},\"prefill_delay_ms\":$2,\"max_tokens\":5,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
       "$1/v1/chat/completions"; }
# firebad: structurally invalid phan header (empty session_id, turn 0)
firebad(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -H 'x-phanrouter-session: {"session_id":"","turn":0}' \
       -d "{\"model\":\"x\",\"stream\":true,\"prefill_delay_ms\":$2,\"max_tokens\":5,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
       "$1/v1/chat/completions"; }
# body429 $1=url $2=prefill_ms $3=extra-header(optional) -> response body
body429(){ curl -s -N -H "$A" -H "$H" ${3:+-H "$3"} -d "{\"model\":\"x\",\"stream\":true,\"prefill_delay_ms\":$2,\"max_tokens\":1,\"messages\":[]}" "$1/v1/chat/completions"; }
ewv(){ curl -s "$1/_ttft_status" | python3 -c "import json,sys; print((json.load(sys.stdin).get('ewma_ms') or {}).get('_',''))"; }
tpsv(){ curl -s "$1/_tps_status" | python3 -c "import json,sys; print((json.load(sys.stdin).get('ewma_tps') or {}).get('_',''))"; }
spj(){ curl -s "$1/_session_status"; }
sum(){ curl -s "$1/_session_status" | grep -o "\"${2:-admit}\":[0-9.]*" | awk -F: '{s+=$2} END {print s+0}'; }
bs(){ spj "$1" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['pools'][0]['buckets'].get('$2',{}).get('$3',0))"; }
align(){ local now=$(date +%s); sleep $((3 - now % 3)); }
# estab_ttft $1=url $2=prefill list $3=trigger -> fold samples into the TTFT
# EWMA: with alpha=0.3 the first fold lands at 0.3*P80; a second identical
# window (estab2) lands exactly at P80.
estab_ttft(){ align; for pf in $2; do ( fire "$1" "$pf" - t-$RANDOM >/dev/null ) & done; wait; sleep 4; fire "$1" "$3" - t-t >/dev/null; sleep 1; }
estab2(){ estab_ttft "$@"; estab_ttft "$@"; }
expire(){ sleep 10; }   # > ttft_ttl / tps_ttl (8s): EWMA expires
# slow $1=url $2=prefill_ms $3=tag: one background request holding a slot
slow(){ local sid="hold-${3:-x}-$RANDOM"
  curl -s -o /dev/null -N -H "$A" -H "$H" -H "x-phanrouter-session: {\"session_id\":\"$sid\",\"turn\":50}" \
  -d "{\"model\":\"x\",\"stream\":true,\"prefill_delay_ms\":$2,\"max_tokens\":5,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
  "$1/v1/chat/completions" & }
# hold $1=url $2=N $3=pre-sleep: park rt_sum at N with 5s prefills
hold(){ for i in $(seq 1 "$2"); do slow "$1" 5000 "${2}h"; done; sleep "${3:-1}"; }
drain(){ wait; sleep 1; }
OFF=http://127.0.0.1:19100; SP=http://127.0.0.1:19101; SPC=http://127.0.0.1:19102; TP=http://127.0.0.1:19103; TPX=http://127.0.0.1:19105; OFFC=http://127.0.0.1:19104

echo "########## S1 feature off: byte-identical to today ##########"
expire; c=$(fire $OFF 100 5)
[ "$c" = "200" ] && ok "S1a OFF route 200 (header ignored, unparsed, uncounted)" || no "S1a c=$c"
s1=$(spj $OFF)
echo "$s1" | grep -q '"enabled":false' && ok "S1b OFF /_session_status enabled=false" || no "S1b $(echo "$s1"|head -c 200)"
[ "$(sum $OFF admit)" = "0" ] && ok "S1c OFF zero counters (admit=0)" || no "S1c admit=$(sum $OFF admit)"
# hard gate comparison: concurrency 429 on the OFF route carries NO turn_bucket
hold $OFF 21 1
b0=$(body429 $OFF 50)
drain
case "$b0" in
  *'"concurrency limit exceeded"'*)
    case "$b0" in *turn_bucket*) no "S1d OFF 429 body carries turn_bucket: $(echo "$b0"|tail -c 120)";; \
                  *) ok "S1d OFF concurrency 429 has no turn_bucket (shape preserved)";; esac ;;
  *) no "S1d expected OFF concurrency 429, got: $(echo "$b0"|head -c 160)" ;;
esac

echo "########## S2 concurrency share grading (pool of 20) ##########"
# turn1 share 0.85 -> ceiling 17; turn8 0.94 -> 18.8; turn40 1.0 -> 20.
# Turn-50 holders also fill the TTFT EWMA (5s prefills -> overflow bucket
# 2400), so the TTFT gate is on a hard trip through S2: warm turns reject at
# best after probes; turn1 rejects on BOTH gates, which is exactly the
# grading being asserted (turn1 harder than warm).
expire
hold $SP 18 1                                    # rt_sum ~= 17
s2a1=$(fire $SP 50 1); s2a8=$(fire $SP 50 8); s2a40=$(fire $SP 50 40)
drain
{ [ "$s2a1" = "429" ] && [ "$s2a8" = "200" ] && [ "$s2a40" = "200" ]; } \
  && ok "S2a rt_sum=17: turn1 429, turn8/turn40 200 (share gate live)" \
  || no "S2a t1=$s2a1 t8=$s2a8 t40=$s2a40 (want 429/200/200)"
s2b0=$(sum $SP rej_conc)
[ "${s2b0%.*}" -ge 1 ] && ok "S2b rej_conc moved ($s2b0)" || no "S2b rej_conc=$s2b0"
# the 429 body ends with the turn_bucket field -- assert on a CONCURRENCY 429
# (the holders' TTFT fills the EWMA, so warm turns may also hard-trip; turn8
# is asserted on its concurrency body, which names its own gate).
hold $SP 19 1                                    # rt_sum ~= 19
b2=$(body429 $SP 50 'x-phanrouter-session: {"session_id":"s-2c","turn":8}')
s2c40=$(fire $SP 50 40 c2)
drain
case "$b2" in
  *'"concurrency limit exceeded"'*'"turn_bucket":8}'*) ok "S2c turn8 concurrency 429 ends with turn_bucket:8" ;;
  *) no "S2c body=$(echo "$b2"|head -c 220)" ;;
esac
[ "$s2c40" = "200" ] && ok "S2d rt_sum=19: turn40 (full share) still 200" || no "S2d t40=$s2c40"

echo "########## S3 TTFT soft zone ##########"
expire
estab2 $SP "450 450 450" 450                     # EWMA=500 (two folds at alpha=0.3)
e=$(ewv $SP)
awk "BEGIN{exit !($e>=400 && $e<600)}" && ok "S3a EWMA=$e in the soft zone [400,600)" || no "S3a ewma=$e"
s3t1=$(fire $SP 50 1 s3)
[ "$s3t1" = "429" ] && ok "S3b soft zone turn1 429 (tol=1.0, no probe)" || no "S3b t1=$s3t1"
# turn8: tol 1.3 -> bar 520 > 500 -> admitted WITHOUT spending any probe
s3t8a=$(fire $SP 50 8 s3); s3t8b=$(fire $SP 50 8 s3); s3t8c=$(fire $SP 50 8 s3)
{ [ "$s3t8a" = 200 ] && [ "$s3t8b" = 200 ] && [ "$s3t8c" = 200 ]; } \
  && ok "S3c turn8 (bar 520) is 200 every time, no probe spent" || no "S3c $s3t8a $s3t8b $s3t8c"
# turn2: tol 1.1 -> bar 440 < 500 -> rejected once probes (2 per 3s window) are spent.
p5=""; for i in 1 2 3 4 5; do p5="$p5 $(fire $SP 50 2 s3)"; done
n2ok=$(echo $p5 | tr ' ' '\n' | grep -c 200 || true); n2rj=$(echo $p5 | tr ' ' '\n' | grep -c 429 || true)
{ [ "$n2ok" -ge 1 ] && [ "$n2rj" -ge 1 ]; } \
  && ok "S3d turn2: $n2ok probe pass(es) then $n2rj rejection(s)" || no "S3d p5=$p5"

echo "########## S4 TTFT hard trip ##########"
expire
estab2 $SP "600 600 600" 600                     # EWMA=800 >= 600 (severe)
e4=$(ewv $SP)
awk "BEGIN{exit !($e4>=600)}" && ok "S4a EWMA=$e4 hard trip" || no "S4a ewma=$e4"
b4t1=$(body429 $SP 50 'x-phanrouter-session: {"session_id":"s-4","turn":1}')
echo "$b4t1" | grep -q '"sp_zone":"hard"' && echo "$b4t1" | grep -q '"turn_bucket":1' && ok "S4b turn1 hard-trip 429 body (zone=hard, bucket=1)" || no "S4b $(echo "$b4t1"|head -c 220)"
p4=""; for i in 1 2 3 4 5 6; do p4="$p4 $(fire $SP 50 40 s4)"; done
n4p=$(echo $p4 | tr ' ' '\n' | grep -c 200 || true); n4r=$(echo $p4 | tr ' ' '\n' | grep -c 429 || true)
{ [ "$n4p" -ge 1 ] && [ "$n4r" -ge 1 ]; } \
  && ok "S4c hard trip: turn40 probes $n4p x 200 then $n4r x 429" || no "S4c p4=$p4"
c4t1=$(fire $SP 50 1 s4b)
[ "$c4t1" = "429" ] && ok "S4d hard trip turn1 never probes" || no "S4d t1=$c4t1"
# nohdr keeps probe eligibility under a hard trip; wait out the window S4c
# exhausted (probe keys live 2*window) so this block gets its own budget.
sleep 7
p4n=""; for i in 1 2 3 4; do p4n="$p4n $(fire $SP 50 - s4n)"; done
n4np=$(echo $p4n | tr ' ' '\n' | grep -c 200 || true)
[ "$n4np" -ge 1 ] && ok "S4e hard trip: nohdr still gets probe slots ($n4np x 200)" || no "S4e p4n=$p4n"

echo "########## S5 turn-1 hysteresis band ##########"
expire
# (1) Set the hold flag with a 1.0x trip, (2) walk the EWMA down with pure
# prefill-350 windows (P80 380): each fold moves the EWMA a third of the way
# toward 380, and every fold request that still sees threshold*release = 240
# or more re-sets the flag on the spot, so it survives the whole walk. The
# walk ends with the EWMA under 400 but over 240: no gate rejects warm turns,
# while turn 1 is rejected by the hold flag ALONE (sp_zone=hold).
estab2 $SP "600 600 600" 600
e5=""
for i in 1 2 3 4 5 6 7; do
  estab_ttft $SP "350 350 350" 350
  e5=$(ewv $SP)
  [ -n "$e5" ] && awk "BEGIN{exit !($e5>=240 && $e5<400)}" && break
done
[ -n "$e5" ] && awk "BEGIN{exit !($e5>=240 && $e5<400)}" && ok "S5a EWMA=$e5 in the hold band [240,400)" || no "S5a ewma=${e5:-EMPTY}"
s5t40=$(fire $SP 50 40 s5)
[ "$s5t40" = "200" ] && ok "S5b hold band: turn40 is 200 (no gate trips for warm turns)" || no "S5b t40=$s5t40"
b5=$(body429 $SP 50 'x-phanrouter-session: {"session_id":"s-5","turn":1}')
echo "$b5" | grep -q '"error":"ttft limit exceeded"' && echo "$b5" | grep -q '"sp_zone":"hold"' \
  && echo "$b5" | grep -q '"turn_bucket":1' \
  && ok "S5c hold band: turn1 ttft 429 rejected by the flag alone (sp_zone=hold)" \
  || no "S5c $(echo "$b5"|head -c 220)"
# Fold fast windows (prefill 50 -> bucket 50) until the EWMA drops under 240;
# the release path then clears the flag and turn 1 passes again.
for i in 1 2 3 4 5 6 7 8; do
  e5b=$(ewv $SP)
  [ -n "$e5b" ] && awk "BEGIN{exit !($e5b<240)}" && break
  estab_ttft $SP "50 50" 50
done
s5t1=$(fire $SP 50 1 s5d)
{ [ -n "$e5b" ] && awk "BEGIN{exit !($e5b<240)}" && [ "$s5t1" = "200" ]; } \
  && ok "S5d EWMA=$e5b below release: flag cleared, turn1 200" || no "S5d ewma=${e5b:-EMPTY} t1=$s5t1"

echo "########## S6 nohdr treated as warm, counted separately ##########"
expire; estab2 $SP "450 450 450" 450   # soft zone again
c6bad=$(firebad $SP 50)
[ "$c6bad" = "200" ] && ok "S6a bad header acts as warm: admitted in the soft zone" || no "S6a c=$c6bad"
s6=$(spj $SP)
echo "$s6" | grep -q '"bad":[1-9]' && ok "S6b hdr:bad counter moved" || no "S6b $(echo "$s6"|grep -o '"header_counts":[^}]*}')"
bn6=$(bs $SP 0 admit)
[ "${bn6%.*}" -ge 1 ] && ok "S6c bnohdr:admit keeps nohdr traffic visible apart ($bn6)" || no "S6c bnohdr admit=$bn6"

echo "########## S7 gate counters ##########"
rj_c=$(sum $SP rej_conc); rj_t=$(sum $SP rej_ttft)
{ [ "${rj_c%.*}" -ge 1 ] && [ "${rj_t%.*}" -ge 1 ]; } \
  && ok "S7 rej_conc=$rj_c rej_ttft=$rj_t recorded across S2-S6" || no "S7 $rj_c/$rj_t"

echo "########## S8 token accounting with cached_tokens ##########"
expire
# No holders needed: the assertion is pure log-phase accounting. TPS is on
# for spc (its threshold is set; the AIMD default applies too), so the tail
# exists. Two measured requests, distinct buckets.
# before/after deltas: earlier sections may already have served turn-keyed
# requests on this route, so take deltas rather than absolutes.
pt1_0=$(bs $SPC 1 ptok); ct1_0=$(bs $SPC 1 ctok); pt4_0=$(bs $SPC 4 ptok)
fire $SPC 50 1 s8 >/dev/null; fire $SPC 50 4 s8 >/dev/null
sleep 2   # let the log phases land
pt1_1=$(bs $SPC 1 ptok); ct1_1=$(bs $SPC 1 ctok); pt4_1=$(bs $SPC 4 ptok)
ok8a=0
python3 - "$pt1_0" "$ct1_0" "$pt4_0" "$pt1_1" "$ct1_1" "$pt4_1" <<'PY' && ok8a=1
import sys
a = [int(float(x)) for x in sys.argv[1:7]]
pt1_0, ct1_0, pt4_0, pt1_1, ct1_1, pt4_1 = a
# The two measured requests share one body shape: same prompt_tokens on both.
# Deltas must record exactly one request each: ptok equal across buckets,
# ctok = one request's --cached-tokens (3).
dpt1, dct1, dpt4 = pt1_1 - pt1_0, ct1_1 - ct1_0, pt4_1 - pt4_0
assert dpt1 > 0 and dpt1 == dpt4, (dpt1, dpt4)
assert dct1 == 3, dct1
PY
[ "$ok8a" = "1" ] && ok "S8a cached backend: deltas ptok/ctok exact on bucket1 and bucket4" || no "S8a dpt1=$((pt1_1-pt1_0)) dct1=$((ct1_1-ct1_0)) dpt4=$((pt4_1-pt4_0))"
# hit_rate on bucket 1 equals ctok/ptok on that same bucket
hr1=$(bs $SPC 1 hit_rate)
pt1_now=$(bs $SPC 1 ptok); ct1_now=$(bs $SPC 1 ctok)
python3 - "$hr1" "$pt1_now" "$ct1_now" <<'PY' && ok "S8b bucket1 hit_rate=$hr1 == ctok/ptok" || no "S8b hr=$hr1 ptok=$pt1_now ctok=$ct1_now"
import sys
hr, pt, ct = float(sys.argv[1]), int(float(sys.argv[2])), int(float(sys.argv[3]))
assert pt > 0 and abs(hr - ct/pt) < 1e-9, (hr, pt, ct)
PY
# spc mock emits cached_tokens; the sp mock does NOT. no_cached_field must
# count on the sp route (its requests went through with usage parsed but no
# details field), and ptok/ctok must stay 0.
pncf=$(sum $SP no_cached_field); ppt=$(sum $SP ptok); pct=$(sum $SP ctok)
{ [ "${pncf%.*}" -ge 1 ] && [ "$ppt" = "0" ] && [ "$pct" = "0" ]; } \
  && ok "S8c no-cached traffic: no_cached_field=$pncf, ptok/ctok stay 0" || no "S8c ncf=$pncf ptok=$ppt ctok=$pct"
# offC: same cached backend with the feature off -> literally nothing counted
o_ncf=$(sum $OFFC no_cached_field); o_pt=$(sum $OFFC ptok); o_ad=$(sum $OFFC admit)
{ [ "$o_ncf" = "0" ] && [ "$o_pt" = "0" ] && [ "$o_ad" = "0" ]; } \
  && ok "S8d off-route + cached backend: zero counters everywhere" || no "S8d ncf=$o_ncf ptok=$o_pt admit=$o_ad"

echo "########## S9 TPS gate grading (adaptive off, hard fuse) ##########"
expire
# TPS EWMA is steered by the chunk delay (OTPS = 20 tokens / total duration)
# while prefill stays at 350 (its TTFT bucket is 380, healthy): only the TPS
# gate is live on this route. Per this mock, chunk 25 x ~20 chunks + 0.35s
# prefill lands OTPS ~40 (soft band 33.3..45.45]; chunk 80 lands ~12 (hard).
# firedc $1=url $2=prefill_ms $3=chunk_ms $4=[turn] $5=[tag]
firedc(){ local hdr=()
  if [ -n "${4:-}" ]; then hdr=(-H "x-phanrouter-session: {\"session_id\":\"s9-$5\",\"turn\":$4}"); fi
  curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" ${hdr[@]+"${hdr[@]}"} \
       -d "{\"model\":\"x\",\"stream\":true,\"stream_options\":{\"include_usage\":true},\"prefill_delay_ms\":$2,\"chunk_delay_ms\":$3,\"max_tokens\":14,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
       "$1/v1/chat/completions"; }
# estab_tps $1=url $2=prefill $3=chunk: three chunk-steered samples per fold;
# estab_tps2 folds twice (alpha 0.3 reaches full P20 only on the second fold).
estab_tps(){ align; for i in 1 2 3; do ( firedc "$1" "$2" "$3" >/dev/null ) & done; wait; sleep 4; firedc "$1" "$2" "$3" >/dev/null; sleep 1; }
estab_tps2(){ estab_tps "$@"; estab_tps "$@"; }
estab_tps2 $TP 350 25
tv=$(tpsv $TP)
[ -n "$tv" ] && awk "BEGIN{exit !($tv>33.34 && $tv<=45.45)}" && ok "S9a OTPS EWMA=$tv in the soft zone" || no "S9a ewma=${tv:-EMPTY}"
s9t1=$(firedc $TP 20 1 1 a)
[ "$s9t1" = "429" ] && ok "S9b TPS soft zone turn1 429" || no "S9b t1=$s9t1"
s9t40=$(firedc $TP 20 1 40 a)
[ "$s9t40" = "200" ] && ok "S9c TPS soft zone turn40 200" || no "S9c t40=$s9t40"
expire
estab_tps2 $TP 350 80
tv2=$(tpsv $TP)
[ -n "$tv2" ] && awk "BEGIN{exit !($tv2<=33.34)}" && ok "S9d OTPS EWMA=$tv2 hard trip" || no "S9d ewma=${tv2:-EMPTY}"
p9=""; for i in 1 2 3 4; do p9="$p9 $(firedc $TP 20 1 2 h2)"; done
n9p=$(echo $p9 | tr ' ' '\n' | grep -c 200 || true); n9r=$(echo $p9 | tr ' ' '\n' | grep -c 429 || true)
s9t1h=$(firedc $TP 20 1 1 h1)
{ [ "$n9p" -ge 1 ] && [ "$n9r" -ge 1 ] && [ "$s9t1h" = "429" ]; } \
  && ok "S9e TPS hard trip: turn2 probes $n9p then $n9r x 429; turn1 always 429" || no "S9e p9=$p9 t1=$s9t1h"
# The rejects happened across several windows; in-flight streams can still
# be releasing. Settle: poll until two consecutive reads agree (or time out),
# then require at least one rejection counted.
rj_tps=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  a=$(sum $TP rej_tps); sleep 1; b=$(sum $TP rej_tps)
  [ "$a" = "$b" ] && [ "${b%.*}" -ge 1 ] && { rj_tps=$b; break; }
  rj_tps=$b
done
[ "${rj_tps%.*}" -ge 1 ] && ok "S9f rej_tps recorded ($rj_tps after settle)" || no "S9f rej_tps=$rj_tps"

echo "########## S10 runtime toggle off restores today ##########"
curl -s "$SP/_session_prio_toggle?on=0" >/dev/null
s10=$(spj $SP)
echo "$s10" | grep -q '"enabled":false' && ok "S10a toggle off -> enabled=false" || no "S10a $(echo "$s10"|head -c 150)"
expire; estab2 $SP "600 600 600" 600   # 1.0x trip, probe budget fresh
c10=$(fire $SP 50 1 s10)
[ "$c10" = "200" ] && ok "S10b toggle off: turn1 probes like any request (today's behaviour)" || no "S10b c=$c10"
curl -s "$SP/_session_prio_toggle?on=1" >/dev/null   # restore for a clean teardown

echo "########## S11 turn-1 probe budget ##########"
# tpx carries the DEFAULT budget of 1 per probe window (sp/tp run with 0).
# Every sub-case re-establishes a FRESH hard trip first, fires the turn-1
# requests while capturing the body of ONE of them, and asserts the trip is
# live (ttft error) in that body. A bare status sweep cannot tell "the
# budget behaved" from "nothing was tripped" -- exactly how the earlier
# S11c/mutation-(a) muddle happened.
# r11: fire one turn-1 request, print "code|has-ttft-error" (one request,
# both facts, no second budget-consuming probe).
r11(){ local body code
  body=$(curl -s -N -H "$A" -H "$H" -H "x-phanrouter-session: {\"session_id\":\"s11-$3\",\"turn\":1}" \
    -d "{\"model\":\"x\",\"stream\":true,\"prefill_delay_ms\":50,\"max_tokens\":1,\"messages\":[]}" \
    -w "\n%{http_code}" "$1/v1/chat/completions")
  code=${body##*$'\n'}
  case "$body" in *"ttft limit exceeded"*) echo "$code 1";; *) echo "$code 0";; esac
}
# --- S11a: turn-1-only traffic in a hard trip: exactly 1 pass per window ---
expire; estab2 $TPX "600 600 600" 600
align                                        # start the loop at a window boundary
n200=0; n429=0; ntf=0
for i in 1 2 3 4 5; do
  read -r c tf <<<"$(r11 $TPX x a)"
  [ "$c" = 200 ] && n200=$((n200+1)); [ "$c" = 429 ] && n429=$((n429+1)); ntf=$((ntf+tf))
done
{ [ "$n200" = "1" ] && [ "$n429" = "4" ] && [ "$ntf" -ge 4 ]; } \
  && ok "S11a hard trip: 1 t1-probe pass, 4 ttft-429s ($n200/$n429, ttft bodies=$ntf)"   || no "S11a 200=$n200 429=$n429 ttft-bodies=$ntf"
# --- S11b: warm probed first in THIS window -> turn 1 gets 0 passes ---
expire; estab2 $TPX "600 600 600" 600   # re-establish, no carry-over from S11a
align
w40a=$(fire $TPX 50 40 s11b)             # warm probe consumes its own window slot
n200=0; ntf=0; n429=0
for i in 1 2 3; do
  read -r c tf <<<"$(r11 $TPX x b)"
  [ "$c" = 200 ] && n200=$((n200+1)); [ "$c" = 429 ] && n429=$((n429+1)); ntf=$((ntf+tf))
done
{ [ "$w40a" = "200" ] && [ "$n200" = "0" ] && [ "$ntf" -ge 2 ]; } \
  && ok "S11b warm probed first: 0 t1 passes, ttft bodies=$ntf ($w40a)"   || no "S11b w40a=$w40a 200=$n200 429=$n429 ttft-bodies=$ntf"
# --- S11c: self-contained window reset; trip re-established. Exactly 1 pass. ---
expire; estab2 $TPX "600 600 600" 600
align
n200=0; n429=0; ntf=0
for i in 1 2 3; do
  read -r c tf <<<"$(r11 $TPX x c)"
  [ "$c" = 200 ] && n200=$((n200+1)); [ "$c" = 429 ] && n429=$((n429+1)); ntf=$((ntf+tf))
done
{ [ "$n200" = "1" ] && [ "$n429" = "2" ] && [ "$ntf" -ge 2 ]; } \
  && ok "S11c fresh window, fresh trip: exactly 1 pass then ttft-429s ($n200/$n429, bodies=$ntf)"   || no "S11c 200=$n200 429=$n429 bodies=$ntf"


echo
echo "########## PASS=$P FAIL=$F ##########"
exit $((F>0))
