#!/bin/bash
# adaptive_cc_hold_when_idle isolated check: builds its own openresty + mocks, cleans up after itself.
# With the switch on, low or absent traffic never lowers cc: no slack shrink and cc never expires.
# Growth under pressure and overload shrink are unchanged. The ctl route (switch off) runs the same
# load side by side, so every "held" assertion has a control that does move.
#   ① /_tps_status reports the switch (on / off / invalid string → off, logged)
#   ② fresh pool still starts at min (hold does not change the start point)
#   ③ high load raises cc on both routes (hold does not block growth)
#   ④ light load: hold keeps its peak, ctl shrinks
#   ⑤ full idle past both TTLs: hold keeps its value, ctl expires
#   ⑥ overload still shrinks the held route
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/achold}"; KEY="${API_KEY:-}"
MPORTS="28981 28982"
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

ROUTES="hold ctl hbad"
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
    local peers = {{"127.0.0.1",28981,"m1",0,100},{"127.0.0.1",28982,"m2",0,100}}
    local base = {peers=peers, tps_limit_tps=50, rt_limit_factor=1, adaptive_cc=true, adaptive_cc_min=2,
                  adaptive_cc_abs=0, adaptive_cc_interval=2, adaptive_cc_dec=0.5, adaptive_cc_inc=2.0,
                  adaptive_cc_ttl=6, default_max=200, bodylog_default_enabled=false, health_check_interval=9999,
                  tps_window=3, tps_ttl=8, tps_probe_window=3, tps_probe_per_window=5, tps_min_decode_s=0.3}
    local function R(x) local t={} for k,v in pairs(base) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("hold", function() return R({adaptive_cc_hold_when_idle=true}) end)
    _G.register_route("ctl",  function() return R({}) end)                                -- control: switch off
    _G.register_route("hbad", function() return R({adaptive_cc_hold_when_idle="yes"}) end)  -- invalid → off
  }
EOF
port=19750
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
HOLD=http://127.0.0.1:19750; CTL=http://127.0.0.1:19751; HBAD=http://127.0.0.1:19752
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

echo "########## adaptive_cc_hold_when_idle ##########"
# ①
[ "$(fv $HOLD adaptive_cc_hold_when_idle)" = "True" ]  && ok "① hold route reports the switch on"  || no "① hold route: $(fv $HOLD adaptive_cc_hold_when_idle)"
[ "$(fv $CTL adaptive_cc_hold_when_idle)" = "False" ]  && ok "① ctl route reports the switch off"  || no "① ctl route: $(fv $CTL adaptive_cc_hold_when_idle)"
[ "$(fv $HBAD adaptive_cc_hold_when_idle)" = "False" ] && ok "① invalid string treated as off"     || no "① hbad route: $(fv $HBAD adaptive_cc_hold_when_idle)"
grep -q 'adaptive_cc_hold_when_idle=yes is invalid' "$PREFIX/logs/error.log" && ok "① invalid value logged" || no "① invalid value not logged"

# ② fresh pool: hold does not change the start point (min = 2)
ah=$(burst_ok $HOLD 25)
[ "$ah" -le 4 ] && ok "② fresh hold route still starts at min (admitted=$ah)" || no "② hold admitted $ah fresh, want <=4"

# ③ high load (~30 in flight) on both: both climb above 20
( load_n $HOLD 22 10 ) & ( load_n $CTL 22 10 ) & wait
hpk=$(fv $HOLD adaptive_cc); cpk=$(fv $CTL adaptive_cc)
echo "  after high load: hold cc=$hpk  ctl cc=$cpk"
awk "BEGIN{exit !($(num $hpk)>20)}" && ok "③ hold route still grows under pressure (cc=$hpk)" || no "③ hold cc=$hpk, want >20"
awk "BEGIN{exit !($(num $cpk)>20)}" && ok "③ ctl route grows too (cc=$cpk)"                  || no "③ ctl cc=$cpk, want >20"

# ④ light load (~1-2 in flight): hold keeps its peak, ctl shrinks toward real concurrency
( load_n $HOLD 1 16 ) & ( load_n $CTL 1 16 ) & wait
hl=$(fv $HOLD adaptive_cc); cl=$(fv $CTL adaptive_cc)
echo "  after light load: hold cc=$hl (peak $hpk)  ctl cc=$cl (peak $cpk)"
[ "$hl" = "$hpk" ] && ok "④ light load leaves the held cc untouched ($hl)" || no "④ hold cc $hpk → $hl under light load"
awk "BEGIN{exit !($(num $cl)>0 && $(num $cl)<10)}" && ok "④ ctl shrinks under light load ($cpk → $cl)" || no "④ ctl cc=$cl, want 0<cc<10"
[ "$(fv $HOLD adaptive_cc_at_slack)" = "False" ] && ok "④ /_tps_status at_slack=false with hold" || no "④ at_slack not false with hold"

# ⑤ full idle past tps_ttl (8s) + cc ttl (6s): hold keeps the value, ctl expires
sleep 22
hi=$(fv $HOLD adaptive_cc); ci=$(fv $CTL adaptive_cc)
echo "  after idle: hold cc=$hi  ctl cc=$ci"
[ "$hi" = "$hl" ] && ok "⑤ idle does not expire the held cc ($hi)" || no "⑤ hold cc $hl → $hi after idle"
[ "$ci" = "None" ] && ok "⑤ ctl cc expired after idle" || no "⑤ ctl cc=$ci still set after idle"

# ⑥ overload (slow decode ≈ 16 tok/s < 50) still shrinks the held route
load_n $HOLD 2 12 60 40
ho=$(fv $HOLD adaptive_cc)
echo "  overload: hold cc=$ho (was $hi)"
awk "BEGIN{exit !($(num $ho)>=2 && $(num $ho)<$(num $hi))}" && ok "⑥ overload still shrinks the held cc ($hi → $ho)" || no "⑥ hold cc=$ho after overload, want < $hi"

echo "==== result: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
