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
#   ⑦ switching hold off on an idle pool lets the held value expire (it used to stay forever:
#      no traffic means no write, so the never-expiring value was never given a TTL)
#   ⑧ switching hold on on an idle pool keeps the value (it used to expire on its old TTL). Needs a
#      route whose cc TTL outlives the TPS signal, as in production (cc 300s vs a shorter tps_ttl);
#      "late" has cc ttl 20s against tps_ttl 8s so the idle-but-still-set window exists.
#   ⑨ hold off, sparse traffic (each request arrives with nothing else in flight, so conc reads 0 while
#      the TPS signal stays alive): cc shrinks toward init. It used to stay at the last busy value,
#      because a tick with conc == 0 was exempt from the slack shrink.
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

ROUTES="hold ctl hbad late sparse"
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
    _G.ADAPTIVE_CC_HOLD_WHEN_IDLE = false   -- pinned: these checks predate the hold-when-idle default
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
    _G.register_route("hbad", function() return R({adaptive_cc_hold_when_idle="maybe"}) end)  -- unrecognised → global default (pinned false above)
    _G.register_route("late", function() return R({adaptive_cc_ttl=20}) end)              -- off, cc TTL > tps_ttl (for ⑧)
    -- off, gentler shrink and a long cc TTL so ⑨ sees steps rather than a jump to init or an expiry
    _G.register_route("sparse", function() return R({adaptive_cc_dec=0.8, adaptive_cc_ttl=60}) end)
  }
EOF
port=19750
for r in $ROUTES; do
  echo "  server { listen $port; server_name _; set \$routed_session_id \"-\"; set \$routed_source \"-\"; set \$routed_mode \"-\"; set \$routed_peer \"-\"; set \$routed_dict \"\";"
  echo "    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$r) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.$r) } }"
  echo "    location = /_tps_status { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts.$r) } }"
  # Test-only: flip the route's hold switch in place, as a config change would (one worker).
  echo "    location = /_hold_off { content_by_lua_block { _G.__route_opts.$r.adaptive_cc_hold_when_idle = false; ngx.say(\"ok\") } }"
  echo "    location = /_hold_on { content_by_lua_block { _G.__route_opts.$r.adaptive_cc_hold_when_idle = true; ngx.say(\"ok\") } } }"
  port=$((port+1))
done
echo "}"
} > "$PREFIX/nginx.conf"

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in $MPORTS; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 100 --chunk-delay-ms 12 --prefill-delay-ms 15 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
HOLD=http://127.0.0.1:19750; CTL=http://127.0.0.1:19751; HBAD=http://127.0.0.1:19752; LATE=http://127.0.0.1:19753; SPARSE=http://127.0.0.1:19754
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
[ "$(fv $HBAD adaptive_cc_hold_when_idle)" = "False" ] && ok "① unrecognised string falls back to the default (off)"     || no "① hbad route: $(fv $HBAD adaptive_cc_hold_when_idle)"
grep -q 'adaptive_cc_hold_when_idle=maybe is not a boolean' "$PREFIX/logs/error.log" && ok "① invalid value logged" || no "① invalid value not logged"

# ② fresh pool: hold does not change the start point (min = 2)
ah=$(burst_ok $HOLD 25)
[ "$ah" -le 4 ] && ok "② fresh hold route still starts at min (admitted=$ah)" || no "② hold admitted $ah fresh, want <=4"

# ③ high load (~30 in flight) on both: both climb above 20. 16s rather than 10s: on a busy test box
#   the climb is slower. The control's peak is read at once (its slack shrink starts as soon as load
#   drops). The hold route then gets a short light load before its peak is read: the in-flight count
#   from the high load (rt_sum) lingers until the next request overwrites it, and a timer tick in that
#   gap is a legitimate pressure step -- read before it settles, ④ would mistake it for light load
#   raising cc.
( load_n $HOLD 22 16 ) & ( load_n $CTL 22 16 ) & wait
cpk=$(fv $CTL adaptive_cc)
load_n $HOLD 1 6
hpk=$(fv $HOLD adaptive_cc)
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

# ⑦ hold switched off while idle: the value hold wrote (no TTL) must expire within interval + cc ttl.
#   Let ⑥'s traffic age out first (tps_ttl 8s) so the pool is idle with a held, never-expiring value.
sleep 12
hb=$(fv $HOLD adaptive_cc)
# Precondition, checked rather than assumed: the TPS signal must be gone. With a signal still
# present the next tick takes the normal write path, which applies the new switch on its own, so
# this step would pass on the unfixed code and prove nothing.
[ "$(fv $HOLD ewma_tps)" = "None" ] && ok "⑦ precondition: no TPS signal at the switch" \
                                   || no "⑦ precondition: TPS signal still present ($(fv $HOLD ewma_tps)); the step below would not test the idle path"
echo "  idle again, held cc=$hb; switching hold off"
curl -s "$HOLD/_hold_off" >/dev/null
sleep 12   # > interval (2s) + cc ttl (6s)
ha=$(fv $HOLD adaptive_cc)
[ "$(fv $HOLD adaptive_cc_hold_when_idle)" = "False" ] && ok "⑦ switch now reports off" || no "⑦ switch still on"
[ "$ha" = "None" ] && ok "⑦ held value expired after hold was switched off on an idle pool ($hb → expired)" \
                   || no "⑦ held value still $ha after hold was switched off and the pool sat idle past the TTL"

# ⑧ hold switched on while idle: a value with a TTL must stop expiring.
#   Light load writes cc on "late" (hold off, cc ttl 20s). After the TPS signal is gone (tps_ttl 8s)
#   the pool is idle but cc is still set; switch hold on and wait past the old 20s TTL.
load_n $LATE 1 6
lb=$(fv $LATE adaptive_cc)
sleep 10   # > tps_ttl (8s): no signal any more, cc still has ~10s of its 20s TTL
lm=$(fv $LATE adaptive_cc)
echo "  late route: cc=$lb after load, $lm once idle; switching hold on"
[ "$lm" != "None" ] && ok "⑧ precondition: idle pool still holds a cc ($lm)" || no "⑧ precondition: cc already gone before the switch ($lm)"
[ "$(fv $LATE ewma_tps)" = "None" ] && ok "⑧ precondition: no TPS signal at the switch" \
                                   || no "⑧ precondition: TPS signal still present ($(fv $LATE ewma_tps)); the step below would not test the idle path"
curl -s "$LATE/_hold_on" >/dev/null
sleep 22   # past the old 20s TTL
la=$(fv $LATE adaptive_cc)
[ "$la" = "$lm" ] && ok "⑧ value kept after hold was switched on on an idle pool ($la)" \
                  || no "⑧ value $lm became $la after hold was switched on and the old TTL passed"

# ⑨ hold off, sparse traffic: conc (the in-flight count a request sees on arrival) is 0 at every
#   arrival, while each request still feeds the TPS signal (12ms x 40 tokens = ~0.5s of decode, above
#   tps_min_decode_s). Every tick then sees a live signal and conc == 0; cc must shrink toward init.
seq_load(){ local u=$1 dur=$2; local endt=$((SECONDS+dur))
  while [ "$SECONDS" -lt "$endt" ]; do fire "$u" 12 40 >/dev/null 2>&1; sleep 0.4; done; }
load_n $SPARSE 22 16
seq_load $SPARSE 3    # the first sequential arrival overwrites the high-load in-flight count with 0
s0=$(fv $SPARSE adaptive_cc); sc=$(fv $SPARSE adaptive_cc_conc); se=$(fv $SPARSE ewma_tps)
echo "  sparse route: cc=$s0 conc=$sc ewma=$se"
[ "$sc" = "0" ] && ok "⑨ precondition: conc reads 0 under sparse traffic" || no "⑨ precondition: conc=$sc, want 0"
[ "$se" != "None" ] && ok "⑨ precondition: TPS signal present ($se)" \
                    || no "⑨ precondition: no TPS signal; the step below would test the idle path instead"
awk "BEGIN{exit !($(num $s0)>4)}" && ok "⑨ precondition: cc above init before the sparse phase ($s0)" \
                                 || no "⑨ precondition: cc=$s0 already near init (2); nothing left to shrink"
[ "$(fv $SPARSE adaptive_cc_at_slack)" = "True" ] && ok "⑨ /_tps_status at_slack=true at conc 0" \
                                                 || no "⑨ /_tps_status at_slack not true at conc 0"
seq_load $SPARSE 12   # ~6 ticks at x0.8
s1=$(fv $SPARSE adaptive_cc)
echo "  sparse route after 12s: cc=$s1 (was $s0)"
awk "BEGIN{exit !($(num $s1)>=2 && $(num $s1)<$(num $s0))}" && ok "⑨ sparse traffic shrinks cc ($s0 → $s1)" \
                                                           || no "⑨ cc $s0 → $s1 under sparse traffic, want lower"

echo "==== result: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
