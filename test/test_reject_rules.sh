#!/bin/bash
# reject_rules(规则化请求拒绝)集成测试:隔离 scratch openresty + mock,自清理,不碰生产/chat live 配置。
# 从 ENGINE=session_route.conf 抽 init_by_lua_block(含 _G.REJECT_RULES_DEFAULT_* 全局)+ 引擎 lua/,
# 自建一个 route "r"(peers=mock)配 reject_rules,真实走 do_route → 验证 429/自定义 status + endpoint + 热切。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/rejectrules_suite}"; KEY=REDACTED-API-KEY
MPORT=28951
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $MPORT"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"

# 抽 init_by_lua_block(动态定位,含新增的 _G.REJECT_RULES_DEFAULT_STATUS/ENABLED)+ 前置 lua_package_path
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 2048; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict reject_stat 1m; lua_shared_dict api_keys 1m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict active_conns_r 4m; lua_shared_dict cluster_avg_r 16k; lua_shared_dict lc_locks_r 1m; lua_shared_dict bad_peers_r 1m; lua_shared_dict bodylog_ctl_r 1m; lua_shared_dict cch_ctl_r 1m;
  lua_shared_dict active_conns_d 4m; lua_shared_dict cluster_avg_d 16k; lua_shared_dict lc_locks_d 1m; lua_shared_dict bad_peers_d 1m; lua_shared_dict bodylog_ctl_d 1m; lua_shared_dict cch_ctl_d 1m;
  lua_shared_dict active_conns_ops 4m; lua_shared_dict cluster_avg_ops 16k; lua_shared_dict lc_locks_ops 1m; lua_shared_dict bad_peers_ops 1m; lua_shared_dict bodylog_ctl_ops 1m; lua_shared_dict cch_ctl_ops 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    local RR = {
        { name="tiny_max_tokens", field="max_tokens", op="lt", value=10 },
        { name="streaming",       field="stream",     op="eq", value=true },
        { name="too_long",        field="input_bytes",op="gt", value=500, status=413 },
        { name="combo",           all={ {field="model",op="eq",value="danger"},
                                        {field="temperature",op="gt",value=1.5} }, message="danger+hot" },
    }
    -- route r:显式 reject_rules_default_enabled=true(规则生效,验引擎行为)
    _G.register_route("r", function() return {
      peers = {{"127.0.0.1", $MPORT, "mock"}}, default_max = 50, bodylog_default_enabled = false,
      health_check_interval = 5, reject_rules = RR, reject_rules_default_enabled = true,
    } end)
    -- route d:不设 reject_rules_default_enabled → 吃全局默认(现已改成 false)→ 规则默认【不生效】
    _G.register_route("d", function() return {
      peers = {{"127.0.0.1", $MPORT, "mock"}}, default_max = 50, bodylog_default_enabled = false,
      health_check_interval = 5, reject_rules = RR,
    } end)
    -- route ops:每操作符一条规则、用【独立字段】避免 first-match 串扰(基线全不命中,翻一个字段测一个操作符)
    _G.register_route("ops", function() return {
      peers = {{"127.0.0.1", $MPORT, "mock"}}, default_max = 50, bodylog_default_enabled = false,
      health_check_interval = 5, reject_rules_default_enabled = true, reject_rules = {
        { name="op_ge",       field="fge",       op="ge",       value=10 },
        { name="op_le",       field="fle",       op="le",       value=5 },
        { name="op_ne",       field="fne",       op="ne",       value="keep" },
        { name="op_in",       field="fin",       op="in",       value={"a","b"} },
        { name="op_nin",      field="fnin",      op="nin",      value={"a","b"} },
        { name="op_match",    field="fmatch",    op="match",    value="err%d" },
        { name="op_contains", field="fcontains", op="contains", value="danger" },
        { name="op_prefix",   field="fprefix",   op="prefix",   value="sk-" },
        { name="op_exists",   field="fexists",   op="exists" },
        { name="op_absent",   field="fabsent",   op="absent" },
      },
    } end)
  }
  server { listen 19095; server_name _; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.r) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.r) } }
    location = /_reject_rules_status { content_by_lua_block { _G.dbg_reject_rules_status(_G.__route_opts.r) } }
    location = /_reject_rules_toggle { content_by_lua_block { _G.dbg_reject_rules_toggle(_G.__route_opts.r) } }
    location = /_429_status          { content_by_lua_block { _G.dbg_429_status(_G.__route_opts.r) } } }
  server { listen 19096; server_name _; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.d) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.d) } }
    location = /_reject_rules_status { content_by_lua_block { _G.dbg_reject_rules_status(_G.__route_opts.d) } }
    location = /_reject_rules_toggle { content_by_lua_block { _G.dbg_reject_rules_toggle(_G.__route_opts.d) } } }
  server { listen 19097; server_name _; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-"; set \$routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.ops) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.ops) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
setsid nohup "$PY" "$MOCK" --port $MPORT --name mock --output-len 5 --chunk-delay-ms 1 --prefill-delay-ms 10 >"$PREFIX/mock.log" 2>&1 & disown
sleep 2
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
U=http://127.0.0.1:19095; H="Content-Type: application/json"; A="Authorization: Bearer $KEY"
# body: $1=model $2=stream $3=max_tokens $4=temperature $5=pad(content 补 x 字节数,撑 input_bytes)
body(){ local pad=""; if [ "${5:-0}" -gt 0 ]; then pad=$(head -c "$5" </dev/zero | tr '\0' 'x'); fi
  echo "{\"model\":\"$1\",\"stream\":$2,\"max_tokens\":$3,\"temperature\":$4,\"messages\":[{\"role\":\"user\",\"content\":\"hi$pad\"}]}"; }
code(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "$(body "$@")" "$U/v1/chat/completions"; }
resp(){ curl -s -N -H "$A" -H "$H" -d "$(body "$@")" "$U/v1/chat/completions"; }

echo "########## reject_rules 集成 ##########"
# 1) 叶子 max_tokens<10 → 429(model=safe,stream=false,小体 → 只命中 tiny_max_tokens)
c=$(code safe false 5 0.5 0);   [ "$c" = 429 ] && ok "R1 max_tokens<10 → 429" || no "R1 code=$c"
# 2) 叶子 stream=true → 429
c=$(code safe true 100 0.5 0);  [ "$c" = 429 ] && ok "R2 stream=true → 429" || no "R2 code=$c"
# 3) 叶子 input_bytes>500 → 自定义 413(补 600 字节 content)
c=$(code safe false 100 0.5 600); [ "$c" = 413 ] && ok "R3 input_bytes>500 → 自定义 413" || no "R3 code=$c"
# 4) 组合 AND:model=danger 且 temperature>1.5 → 429
c=$(code danger false 100 2.0 0); [ "$c" = 429 ] && ok "R4 combo(danger且hot) → 429" || no "R4 code=$c"
# 5) 组合 AND 只满足一半(danger 但 temp 低)→ 不命中 → 放行 200
c=$(code danger false 100 0.5 0); [ "$c" = 200 ] && ok "R5 combo 半命中 → 放行 200" || no "R5 code=$c"
# 6) 全不命中 → 代理 mock → 200
c=$(code safe false 100 0.5 0); [ "$c" = 200 ] && ok "R6 无命中 → 代理 200" || no "R6 code=$c"
# 7) 拒绝响应体含 rule name + type
r=$(resp safe false 5 0.5 0); echo "$r" | grep -q '"rejected_by_rule"' && echo "$r" | grep -q '"tiny_max_tokens"' && ok "R7 响应体含 type+rule name" || no "R7 resp=$r"
# 8) /_reject_rules_status:rule_count=4 + tiny_max_tokens 规则+描述+命中(键顺序无关,分别 grep)
s=$(curl -s "$U/_reject_rules_status")
echo "$s" | grep -q '"rule_count":4' \
  && echo "$s" | grep -q '"name":"tiny_max_tokens"' \
  && echo "$s" | grep -q '"cond":"max_tokens lt 10"' \
  && echo "$s" | grep -q '"cond":"ALL(model eq danger & temperature gt 1.5)"' \
  && echo "$s" | grep -q '"hits":2' \
  && ok "R8 /_reject_rules_status(rule_count=4 + cond 描述 + hits)" || no "R8 status=$s"
# 9) /_429_status:reason=rule 计数 > 0
z=$(curl -s "$U/_429_status?all=1"); echo "$z" | grep -qE '"rule":[1-9]' && ok "R9 /_429_status reason=rule >0" || no "R9 z=$z"
# 10) 热切 off → 原本 429 的请求放行(代理 200);on → 429 回来
curl -s "$U/_reject_rules_toggle?on=0" >/dev/null
c=$(code safe false 5 0.5 0); [ "$c" = 200 ] && ok "R10a toggle off → 放行 200" || no "R10a code=$c"
curl -s "$U/_reject_rules_toggle?on=1" >/dev/null
c=$(code safe false 5 0.5 0); [ "$c" = 429 ] && ok "R10b toggle on → 429 回来" || no "R10b code=$c"
# 11) GET /v1/models(探测)不被规则拒绝(is_probe 跳过 eval)
c=$(curl -s -o /dev/null -w "%{http_code}" -H "$A" "$U/v1/models"); [ "$c" = 200 ] && ok "R11 /v1/models 探测放行($c)" || no "R11 code=$c"

# 12) 安全默认(全局 REJECT_RULES_DEFAULT_ENABLED=false):route d 配了规则但没显式开 → 默认不生效 → 放行 200
UD=http://127.0.0.1:19096
c=$(curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "$(body safe false 5 0.5 0)" "$UD/v1/chat/completions")
[ "$c" = 200 ] && ok "R12 默认关:未显式开 → 规则不生效放行 200" || no "R12 code=$c"
en=$(curl -s "$UD/_reject_rules_status" | grep -o '"enabled":[a-z]*')
echo "$en" | grep -q false && ok "R12b /_reject_rules_status enabled=false" || no "R12b $en"
# 13) 运行时开启(toggle on)→ 同请求转为 429;关回 → 放行
curl -s "$UD/_reject_rules_toggle?on=1" >/dev/null
c=$(curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "$(body safe false 5 0.5 0)" "$UD/v1/chat/completions")
[ "$c" = 429 ] && ok "R13 toggle on → 规则生效 429" || no "R13 code=$c"

echo "########## 全操作符 e2e(route ops)##########"
# 基线体:10 个字段都设成【不命中】任何规则的值;每 case 只翻一个字段 → 只触发对应操作符规则(first-match 无串扰)
UO=http://127.0.0.1:19097
opsbody(){ python3 -c '
import json,sys
b={"model":"safe","stream":False,"max_tokens":100,"messages":[{"role":"user","content":"hi"}],
   "fge":1,"fle":100,"fne":"keep","fin":"z","fnin":"a","fmatch":"ok","fcontains":"safe","fprefix":"ak-x","fabsent":1}
if len(sys.argv)>=3 and sys.argv[1]: b[sys.argv[1]]=json.loads(sys.argv[2])
if len(sys.argv)>=4 and sys.argv[3]: b.pop(sys.argv[3],None)
print(json.dumps(b))' "${1:-}" "${2:-}" "${3:-}"; }
opscode(){ curl -s -o /dev/null -w "%{http_code}" -N -H "$A" -H "$H" -d "$(opsbody "$@")" "$UO/v1/chat/completions"; }
c=$(opscode);                 [ "$c" = 200 ] && ok "R14 基线(全不命中)→ 200" || no "R14 code=$c"
c=$(opscode fge 10);          [ "$c" = 429 ] && ok "R15 ge(fge>=10)→ 429" || no "R15 code=$c"
c=$(opscode fle 5);           [ "$c" = 429 ] && ok "R16 le(fle<=5)→ 429" || no "R16 code=$c"
c=$(opscode fne '"other"');   [ "$c" = 429 ] && ok "R17 ne(fne!=keep)→ 429" || no "R17 code=$c"
c=$(opscode fin '"a"');       [ "$c" = 429 ] && ok "R18 in(fin∈{a,b})→ 429" || no "R18 code=$c"
c=$(opscode fnin '"z"');      [ "$c" = 429 ] && ok "R19 nin(fnin∉{a,b})→ 429" || no "R19 code=$c"
c=$(opscode fmatch '"err5"'); [ "$c" = 429 ] && ok "R20 match(err%d)→ 429" || no "R20 code=$c"
c=$(opscode fcontains '"x danger y"'); [ "$c" = 429 ] && ok "R21 contains(danger)→ 429" || no "R21 code=$c"
c=$(opscode fprefix '"sk-abc"'); [ "$c" = 429 ] && ok "R22 prefix(sk-)→ 429" || no "R22 code=$c"
c=$(opscode fexists 1);       [ "$c" = 429 ] && ok "R23 exists(fexists 存在)→ 429" || no "R23 code=$c"
c=$(opscode "" "" fabsent);   [ "$c" = 429 ] && ok "R24 absent(fabsent 缺失)→ 429" || no "R24 code=$c"

echo
echo "================ reject_rules 集成: PASS=$P FAIL=$F ================"
exit $([ "$F" = 0 ] && echo 0 || echo 1)
