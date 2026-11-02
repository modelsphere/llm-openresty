#!/bin/bash
# /_route_opts isolated check: builds its own openresty + mocks, cleans up after itself.
#   ① every catalog fallback equals the engine's effective value on a route that sets nothing; fallback
#     is read from the engine (a changed global shows up), derived ones keep the catalog's words;
#     catalog shape (17 operator directives + 30 advanced, groups, item fields)
#   ② new defaults: hold_when_idle on, cross_tier_fallback on, derived init == min
#   ③ strict boolean parsing: "False"/"no" read as false with a WARN, "maybe" falls back with an ERR,
#     and the same for every other boolean option
#   ④ init / init_frac outside [min, max] warns once at registration and is clamped
#   ⑤ set flags, only catalog options listed, peers reduced to counts
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"; PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/routeopts}"; KEY="${API_KEY:-}"
MPORTS="28991 28992"
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

ROUTES="plain strs junk bools"
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
    -- No adaptive_cc globals pinned here: ① compares the catalog against the real engine defaults.
    -- Two globals changed the way a host's own session_base.conf would: /_route_opts must show these,
    -- not the repo values (20 / 300).
    _G.TPS_LIMIT_TPS = 33; _G.ADAPTIVE_CC_TTL = 123
    local peers = {{"127.0.0.1",28991,"m1",0,100},{"127.0.0.1",28992,"m2",0,100}}   -- static max 200
    local dicts = {bodylog_default_enabled=false}   -- bodylog is not in the catalog; keep the mocks quiet
    local function R(x) local t={peers=peers} for k,v in pairs(dicts) do t[k]=v end for k,v in pairs(x) do t[k]=v end return t end
    _G.register_route("plain", function() return R({}) end)   -- sets nothing tunable: engine defaults throughout
    _G.register_route("strs",  function() return R({cross_tier_fallback="False", adaptive_cc_hold_when_idle="no",
                                                     adaptive_cc_init_frac=0.1, adaptive_cc_min=80, max_more_tries=3}) end)
    _G.register_route("junk",  function() return R({cross_tier_fallback="maybe", adaptive_cc_hold_when_idle="maybe",
                                                     adaptive_cc_init=500}) end)
    -- strings in every other boolean option the engine reads (review: they used plain truthiness)
    _G.register_route("bools", function() return R({adaptive_cc="off", session_affinity_enabled="false",
                                                     expose_routed_peer="on", bodylog_default_enabled="no",
                                                     ttft_429_default_enabled="False"}) end)
  }
EOF
port=19760
for r in $ROUTES; do
  echo "  server { listen $port; server_name _; set \$routed_session_id \"-\"; set \$routed_source \"-\"; set \$routed_mode \"-\"; set \$routed_peer \"-\"; set \$routed_dict \"\";"
  echo "    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.$r) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.$r) } }"
  echo "    location = /_route_opts { content_by_lua_block { _G.dbg_route_opts(_G.__route_opts.$r) } } }"
  port=$((port+1))
done
echo "}"
} > "$PREFIX/nginx.conf"

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
for p in $MPORTS; do setsid nohup "$PY" "$MOCK" --port $p --name m-$p --output-len 100 --chunk-delay-ms 12 --prefill-delay-ms 15 >"$PREFIX/mock_$p.log" 2>&1 & disown; done
sleep 3; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
PLAIN=http://127.0.0.1:19760; STRS=http://127.0.0.1:19761; JUNK=http://127.0.0.1:19762; BOOLS=http://127.0.0.1:19763
# ro <url> <python expr over d (decoded /_route_opts) and D (directives + advanced, by key)>
ro(){ curl -s "$1/_route_opts" | python3 -c "import sys,json;d=json.load(sys.stdin);D={x['key']:x for x in d['directives']+d['advanced']};r=($2);print(r if isinstance(r,tuple) else r)"; }
echo "########## /_route_opts ##########"
# ① literal catalog defaults == effective values on a route that sets nothing
drift=$(curl -s "$PLAIN/_route_opts" | python3 -c '
import sys, json
d = json.load(sys.stdin); bad = []; checked = 0
for x in d["directives"] + d["advanced"]:
    f, v = x["fallback"], x["value"]
    if f in ("on", "off"): want = (f == "on")
    elif f == "unset": want = None
    elif f.startswith("/"): want = f
    else:
        try: want = float(f)
        except ValueError: continue          # derived default ("the floor", "the TPS window" ...)
        v = None if v is None else float(v)
    checked += 1
    if v != want or x["set"]: bad.append("%s: fallback=%s value=%s set=%s" % (x["key"], f, x["value"], x["set"]))
print(checked, "|", "; ".join(bad))')
n=${drift%% |*}; rest=${drift#*| }
[ "$n" -eq 42 ] && [ -z "$rest" ] && ok "① $n catalog defaults match the engine" || no "① checked=$n (want 42) drift: $rest"
[ "$(ro $PLAIN 'D["tps_limit_tps"]["fallback"], D["adaptive_cc_ttl"]["fallback"], D["tps_limit_tps"]["value"]')" = "('33', '123', 33)" ] && ok "① fallback follows the host's globals (33 / 123, not 20 / 300)" || no "① override: $(ro $PLAIN 'D["tps_limit_tps"]["fallback"], D["adaptive_cc_ttl"]["fallback"], D["tps_limit_tps"]["value"]')"
[ "$(ro $PLAIN 'D["adaptive_cc_init"]["fallback"], D["adaptive_cc_interval"]["fallback"], all(isinstance(x.get("fallback"), str) and x["fallback"] for x in d["directives"]+d["advanced"])')" = "('the floor', 'the TPS window', True)" ] && ok "① derived defaults keep the catalog's words; every option has a fallback" || no "① derived: $(ro $PLAIN '[(x["key"], x.get("fallback")) for x in d["directives"]+d["advanced"] if not isinstance(x.get("fallback"), str) or x["key"] in ("adaptive_cc_init","adaptive_cc_interval")]')"
[ "$(ro $PLAIN 'len(d["directives"]), len(d["advanced"])')" = "(17, 30)" ] && ok "① catalog served: 17 operator directives + 30 advanced" || no "① counts: $(ro $PLAIN 'len(d["directives"]), len(d["advanced"])')"
[ "$(ro $PLAIN 'sorted({x["group"] for x in d["directives"]})')" = "['Adaptive concurrency', 'Capacity & failover', 'Observability', 'Routing', 'Session priority', 'Thresholds']" ] && ok "① operator groups" || no "① groups: $(ro $PLAIN 'sorted({x["group"] for x in d["directives"]})')"
[ "$(ro $PLAIN '"adaptive_cc_inc" in {x["key"] for x in d["advanced"]} and "adaptive_cc_inc" not in {x["key"] for x in d["directives"]}')" = "True" ] && ok "① internals listed under advanced, not directives" || no "① adaptive_cc_inc misplaced"
[ "$(ro $PLAIN 'sorted(set(k for x in d["directives"]+d["advanced"] for k in x))')" = "['detail', 'fallback', 'group', 'key', 'kind', 'label', 'needsAdaptive', 'set', 'value']" ] && ok "① item fields match the console Directive (+value, set)" || no "① fields: $(ro $PLAIN 'sorted(set(k for x in d["directives"]+d["advanced"] for k in x))')"

# ② new defaults
[ "$(ro $PLAIN 'D["adaptive_cc_hold_when_idle"]["value"], D["adaptive_cc_hold_when_idle"]["set"]')" = "(True, False)" ] && ok "② hold_when_idle defaults to on (not set by the route)" || no "② hold: $(ro $PLAIN 'D["adaptive_cc_hold_when_idle"]')"
[ "$(ro $PLAIN 'D["cross_tier_fallback"]["value"], D["cross_tier_fallback"]["fallback"]')" = "(True, 'on')" ] && ok "② cross_tier_fallback defaults to on" || no "② cross_tier: $(ro $PLAIN 'D["cross_tier_fallback"]["value"]')"
[ "$(ro $PLAIN 'D["adaptive_cc_init"]["value"] is None and d["adaptive_cc_derived"]["_"] == {"static_max": 200, "min": 80, "init": 80}')" = "True" ] && ok "② no init: derived init == min (80 of 200)" || no "② derived: $(ro $PLAIN 'd["adaptive_cc_derived"]')"

# ③ boolean parsing
[ "$(ro $STRS 'D["cross_tier_fallback"]["value"], D["adaptive_cc_hold_when_idle"]["value"]')" = "(False, False)" ] && ok "③ \"False\" and \"no\" read as false" || no "③ strs: $(ro $STRS 'D["cross_tier_fallback"]["value"], D["adaptive_cc_hold_when_idle"]["value"]')"
grep -q 'cross_tier_fallback=False (string) read as false' "$PREFIX/logs/error.log" && ok "③ WARN for the non-boolean \"False\"" || no "③ no WARN for \"False\""
[ "$(ro $JUNK 'D["cross_tier_fallback"]["value"], D["adaptive_cc_hold_when_idle"]["value"]')" = "(True, True)" ] && ok "③ \"maybe\" -> defaults (cross_tier on, hold on)" || no "③ junk: $(ro $JUNK 'D["cross_tier_fallback"]["value"], D["adaptive_cc_hold_when_idle"]["value"]')"
[ "$(grep -c 'is not a boolean - using the default' "$PREFIX/logs/error.log")" -ge 2 ] && ok "③ ERR for both unrecognised values" || no "③ ERR lines: $(grep -c 'is not a boolean' "$PREFIX/logs/error.log")"

# ③ the other boolean options parse the same way (string "off"/"false"/"no" must not mean on)
[ "$(ro $BOOLS 'D["adaptive_cc"]["value"], D["session_affinity_enabled"]["value"], D["expose_routed_peer"]["value"]')" = "(False, False, True)" ] && ok "③ adaptive_cc \"off\" -> off, session_affinity \"false\" -> off, expose_routed_peer \"on\" -> on" || no "③ bools: $(ro $BOOLS 'D["adaptive_cc"]["value"], D["session_affinity_enabled"]["value"], D["expose_routed_peer"]["value"]')"
# bodylog_default_enabled / ttft_429_default_enabled are not in the catalog: the WARN carries the parsed value
grep -q '\[bools\] bodylog_default_enabled=no (string) read as false' "$PREFIX/logs/error.log" && grep -q '\[bools\] ttft_429_default_enabled=False (string) read as false' "$PREFIX/logs/error.log" && ok "③ bodylog \"no\" and ttft_429 \"False\" read as false" || no "③ no WARN for bodylog/ttft_429 strings"
[ "$(ro $BOOLS 'd.get("adaptive_cc_derived")')" = "None" ] && ok "③ adaptive off has no derived bounds" || no "③ bools derived: $(ro $BOOLS 'd.get("adaptive_cc_derived")')"

# ④ init outside [min, max]
grep -q '\[strs\] adaptive_cc_init_frac=0.1 gives 20, outside \[min 80, max 200\] - effective init is 80' "$PREFIX/logs/error.log" && ok "④ WARN: init_frac below the floor" || no "④ no WARN for init_frac below min"
grep -q '\[junk\] adaptive_cc_init=500 gives 500, outside \[min 80, max 200\] - effective init is 200' "$PREFIX/logs/error.log" && ok "④ WARN: init above the maximum" || no "④ no WARN for init above max"
[ "$(grep -c 'outside \[min' "$PREFIX/logs/error.log")" = "2" ] && ok "④ logged once per route (2 lines, not per request)" || no "④ WARN count $(grep -c 'outside \[min' "$PREFIX/logs/error.log")"
[ "$(ro $JUNK 'd["adaptive_cc_derived"]["_"]["init"]')" = "200" ] && ok "④ derived init clamped to max (200)" || no "④ junk init $(ro $JUNK 'd["adaptive_cc_derived"]')"

# ⑤ set flags, redaction, peers
[ "$(ro $STRS 'D["max_more_tries"]["value"], D["max_more_tries"]["set"], D["tps_window"]["set"]')" = "(3, True, False)" ] && ok "⑤ set flags (max_more_tries set, tps_window default)" || no "⑤ set: $(ro $STRS 'D["max_more_tries"], D["tps_window"]["set"]')"
[ "$(ro $PLAIN 'sorted(d)')" = "['adaptive_cc_derived', 'advanced', 'directives', 'peers', 'route']" ] && ok "⑤ only catalog options, peers and derived bounds (no other_options)" || no "⑤ top-level keys: $(ro $PLAIN 'sorted(d)')"
[ "$(ro $PLAIN 'd["peers"]')" = "{'count': 2}" ] && ok "⑤ peers reduced to a count" || no "⑤ peers: $(ro $PLAIN 'd["peers"]')"

echo "==== result: PASS=$P FAIL=$F ===="
[ "$F" -eq 0 ] && echo "ALL GOOD" || echo "HAS FAILURES"
