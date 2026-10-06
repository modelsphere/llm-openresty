#!/bin/bash
# adaptive_cc_init isolated check: builds its own openresty + mocks, cleans up after itself.
# init sits inside [min, max]: a fresh/expired cc starts there, low-traffic (slack) shrink stops
# there, and only overload shrink may go below it, down to min. Routes without init keep the old
# behaviour (init = min), which the in0 control route checks side by side.
#   ① derived init reported by /_tps_status (explicit / frac / invalid / clamped to max)
#   ② fresh pool admits ~init concurrent requests (control admits ~min)
#   ③ slack shrink stops at init (control keeps shrinking toward real concurrency)
#   ④ overload shrink still goes below init
#   ⑤ after an overload, a healthy trickle climbs cc back to init
#   ⑥ after the cc value expires the pool restarts from init, not min
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/acinit}"; KEY="${API_KEY:-}"
MPORTS="28971 28972"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in $MPORTS; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

ROUTES="in20 in0 inf ibad ibig"
{
cat <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
EOF
for r in $ROUTES; do
  echo "  lua_shared_dict active_conns_$r 4m; lua_shared_dict cluster_avg_$r 16k; lua_shared_dict lc_locks_$r 1m; lua_shared_dict bad_peers_$r 1m; lua_shared_dict bodylog_ctl_$r 1m; lua_shared_dict cch_ctl_$r 1m;"
done
cat <<'EOF'
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.TPS_BUCKETS = {5,10,20,40,80,150,300}
    _G.ADAPTIVE_CC_MIN_FRAC = 0.4   -- pinned: only explicit adaptive_cc_min=2 is used below
    -- static max = 2 peers x 100 = 200, min = 2, ABS = 0 so only the relative gates and init matter.
    -- Short cc TTL (6s) + tps_ttl 8s so the expiry check (⑥) fits in the run.
    local peers = {{"127.0.0.1",28971,"m1",0,100},{"127.0.0.1",28972,"m2",0,100}}
    local base = {peers=peers, tps_limit_tps=50, rt_limit_factor=1, adaptive_cc=true, adaptive_cc_min=2,
                  adaptive_cc_abs=0, adaptive_cc_interval=2, adaptive_cc_dec=0.5, adaptive_cc_inc=2.0,
                  adaptive_cc_ttl=6, default_max=200, bodylog_default_enabled=false, health_check_interval=9999,
                  tps_window=3, tps_ttl=8, tps_probe_window=3, tps_probe_per_window=5, tps_min_decode_s=0.3}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("in20", function() return R({adaptive_cc_init=20}) end)
    _G.register_route("in0",  function() return R({}) end)                       -- control: init = min
    _G.register_route("inf",  function() return R({adaptive_cc_init_frac=0.1}) end) -- 200 x 0.1 = 20
    _G.register_route("ibad", function() return R({adaptive_cc_init=0.5}) end)   -- invalid → ignored → min
    _G.register_route("ibig", function() return R({adaptive_cc_init=500}) end)   -- clamped to max 200
  }
EOF
port=19740
for r in $ROUTES; do
  echo "  server { listen $port; server_name _; set \$routed_session_id \"-\"; set \$routed_source \"-\"; set \$routed_mode \"-\"; set \$routed_peer \"-\"; set \$routed_dict \"\";"
  echo "    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$r) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.$r) } }"
  echo "    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.$r) } } }"
  port=$((port+1))
done
echo "}"
} > "$PREFIX/nginx.conf"

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in $MPORTS; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 100 --chunk-delay-ms 12 --prefill-delay-ms 15 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
IN20=http://127.0.0.1:19740; IN0=http://127.0.0.1:19741; INF=http://127.0.0.1:19742; IBAD=http://127.0.0.1:19743; IBIG=http://127.0.0.1:19744
body(){ echo "{\"stream_options\":{\"include_usage\":true},\"model\":\"x\",\"stream\":true,\"chunk_delay_ms\":$1,\"max_tokens\":$2,\"prefill_delay_ms\":15,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"; }
fire(){ curl -s -o /dev/null -N -H "$A" -H "$H" -d "$(body $2 $3)" "$1/v1/chat/completions"; }
# keep ~n overlapping requests for dur seconds (chunk ms x tokens per request)
load_n(){ local u=$1 n=$2 dur=$3 ck=${4:-12} tk=${5:-100}; local endt=$((SECONDS+dur))
  while [ "$SECONDS" -lt "$endt" ]; do for i in $(seq 1 $n); do fire "$u" $ck $tk >/dev/null 2>&1 & done; sleep 0.8; done; wait; }
# fire n simultaneous long requests (12ms x 300 = ~3.6s each) and print how many got 200
burst_ok(){ local u=$1 n=$2 d="$PREFIX/burst_$RANDOM"; mkdir -p "$d"
  for i in $(seq 1 $n); do ( curl -s -o /dev/null -N -w '%{http_code}\n' -H "$A" -H "$H" -d "$(body 12 300)" "$u/v1/chat/completions" > "$d/$i" ) & done
  wait; cat "$d"/* | grep -c '^200$'; }
fv(){ curl -s "$1/_tps_status" | python3 -c "import sys,json;d=json.load(sys.stdin);v=d.get('$2');print(v.get('_') if isinstance(v,dict) else v)"; }
num(){ [ "$1" = "None" ] && echo -1 || echo "$1"; }

echo "########## adaptive_cc_init ##########"
# ① derived init
for spec in "$IN20:20:explicit init=20" "$IN0:2:no init → min" "$INF:20:init_frac=0.1 → 20" "$IBAD:2:invalid init=0.5 → min" "$IBIG:200:init=500 → clamped to max"; do
  u=$(echo "$spec" | cut -d: -f1-3); want=$(echo "$spec" | cut -d: -f4); why=$(echo "$spec" | cut -d: -f5-)
  got=$(fv "$u" adaptive_cc_init)
  [ "$got" = "$want" ] && ok "① $why (init=$got)" || no "① $why: init=$got, want $want"
done
grep -q "adaptive_cc_init=0.5 is invalid" "$PREFIX/logs/error.log" && ok "① invalid init logged" || no "① invalid init not logged"

# ② fresh pool: 25 simultaneous requests → in20 admits ~20, control admits ~min(2)
a20=$(burst_ok $IN20 25); a0=$(burst_ok $IN0 25)
echo "  fresh burst of 25: in20 admitted=$a20  in0 admitted=$a0"
[ "$a20" -ge 17 ] && [ "$a20" -le 21 ] && ok "② fresh in20 admits ~init (=$a20)" || no "② fresh in20 admitted $a20, want 17..21"
[ "$a0" -le 4 ] && ok "② fresh control admits ~min (=$a0)" || no "② control admitted $a0, want <=4"

# ③ sustained high load (~30 in flight, healthy decode) pushes in20 above init; then light load
#   (~1-2 in flight) must bring it back to init and no further, while the control shrinks toward
#   real concurrency. (A one-shot burst is not enough: no request finishes during it, so there is
#   no TPS EWMA and the timer does not write cc at all.)
load_n $IN20 22 10
cpk=$(fv $IN20 adaptive_cc)
awk "BEGIN{exit !($(num $cpk)>20)}" && ok "③ high load pushed in20 above init first (cc=$cpk)" || no "③ in20 cc=$cpk after high load, want >20"
( load_n $IN20 1 16 ) & ( load_n $IN0 1 16 ) & wait
c20=$(fv $IN20 adaptive_cc); c0=$(fv $IN0 adaptive_cc); k20=$(fv $IN20 adaptive_cc_conc)
echo "  light load: in20 cc=$c20 (conc=$k20)  in0 cc=$c0"
[ "$c20" = "20" ] && ok "③ slack shrink stops at init (cc=$c20)" || no "③ in20 cc=$c20, want 20"
awk "BEGIN{exit !($(num $c0)>0 && $(num $c0)<10)}" && ok "③ control tracks real concurrency (cc=$c0)" || no "③ control cc=$c0, want 0<cc<10"
[ "$(fv $IN20 adaptive_cc_at_slack)" = "False" ] && ok "③ /_tps_status at_slack=false at init" || no "③ at_slack not false at init"

# ④ overload (slow decode: 60ms/token ≈ 16 tok/s < 50) → cc may go below init
load_n $IN20 2 12 60 40
c20o=$(fv $IN20 adaptive_cc); ev=$(fv $IN20 ewma_tps)
echo "  overload: in20 cc=$c20o ewma=$ev"
awk "BEGIN{exit !($(num $c20o)>=2 && $(num $c20o)<20)}" && ok "④ overload shrinks below init (cc=$c20o)" || no "④ overload cc=$c20o, want 2<=cc<20"

# ⑤ after the overload, light healthy traffic (~1-2 in flight) must bring cc back up to init.
#   The trickle keeps the EWMA alive, so cc never expires; only the climb-back can restore it.
load_n $IN20 1 20
c20r=$(fv $IN20 adaptive_cc)
echo "  light load after overload: in20 cc=$c20r (was $c20o)"
[ "$c20r" = "20" ] && ok "⑤ healthy trickle climbs back to init (cc $c20o → $c20r)" || no "⑤ cc=$c20r after recovery, want 20"


# ⑥ idle until the EWMA (8s) and the cc value (6s) expire, then a burst starts from init again
sleep 22
ce=$(fv $IN20 adaptive_cc)
[ "$ce" = "None" ] && ok "⑥ cc expired after idle" || no "⑥ cc=$ce still set after idle"
a20e=$(burst_ok $IN20 25)
echo "  burst after expiry: in20 admitted=$a20e"
[ "$a20e" -ge 17 ] && [ "$a20e" -le 21 ] && ok "⑥ restarts from init, not min (admitted=$a20e)" || no "⑥ admitted $a20e after expiry, want 17..21"

echo "==== 结果: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
