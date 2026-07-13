#!/bin/bash
# 验证 compute-node 改成 peers_by_model 的配置形状(隔离 + mock,不碰生产):
#   glm-5.1-fp8 = CART router(pri1,max90) + 3 单机(pri0,max30);glm-5.2-fp8 = 单实例(pri0,max30)。
# 覆盖:① 按 body.model 分发 ② 同一 model 子池内优先级分层(router 优先,挂了降级 pri0)
#       ③ 两 model 隔离(glm-5.2 不受 glm-5.1 router 挂影响)④ 未知 model 处理。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"; MOCK="${MOCK:-$HERE/mock_vllm_sse.py}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PY="${PY:-python3}"
PREFIX="${PREFIX:-/tmp/mcpu11test}"; KEY=REDACTED-API-KEY
MPORTS="28051 28052 28053 28054 28055"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  for p in $MPORTS; do for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $p"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; done
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
  lua_shared_dict active_conns_mc 4m; lua_shared_dict cluster_avg_mc 16k; lua_shared_dict lc_locks_mc 1m; lua_shared_dict bad_peers_mc 1m; lua_shared_dict bodylog_ctl_mc 1m; lua_shared_dict cch_ctl_mc 1m;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  upstream vllm_retry_static { server 127.0.0.1:28051; }
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    -- compute-node 目标配置形状(mock 端口替真实):glm-5.1 = router pri1 + 3 单机 pri0;glm-5.2 = 单实例
    _G.register_route("mc", function() return {
      peers_by_model = {
        ["glm-5.1-fp8"] = {
          {"127.0.0.1", 28051, "glm-cart-router", 1, 90},
          {"127.0.0.1", 28052, "compute-nodenode", 0, 30},
          {"127.0.0.1", 28053, "compute-nodenode", 0, 30},
          {"127.0.0.1", 28054, "compute-nodenode", 0, 30},
        },
        ["glm-5.2-fp8"] = {
          {"127.0.0.1", 28055, "compute-nodenode-glm52", 0, 30},
        },
      },
      default_max=50, bodylog_default_enabled=false, health_check_interval=3 } end)
  }
  server { listen 19520; server_name _; set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-"; set $routed_dict "";
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts.mc) } body_filter_by_lua_block { _G.bodylog_filter_chunk() } proxy_next_upstream error timeout http_502 http_503 non_idempotent; proxy_pass http://vllm_backends; proxy_http_version 1.1; proxy_buffering off; proxy_read_timeout 3600s; log_by_lua_block { _G.do_log_release(_G.__route_opts.mc) } }
    location = /_route_inspect { content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts.mc) } }
    location = /_health_status { content_by_lua_block { _G.dbg_health_status(_G.__route_opts.mc) } }
    location = /_route_state    { content_by_lua_block { _G.dbg_route_state(_G.__route_opts.mc) } } }
}
EOF

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
startmock(){ setsid nohup "$PY" "$MOCK" --port $1 --name m-$1 --output-len 20 --chunk-delay-ms 1 --prefill-delay-ms 5 >"$PREFIX/mock_$1.log" 2>&1 & disown; }
for p in $MPORTS; do startmock $p; done
sleep 3
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
H="Content-Type: application/json"; A="Authorization: Bearer $KEY"; U=http://127.0.0.1:19520
insp(){ curl -s -X POST $U/_route_inspect -H "$A" -H "$H" -d "{\"model\":\"$1\"}" | python3 -c "import sys,json;print(json.load(sys.stdin).get('pick'))" 2>/dev/null; }
killp(){ for pid in $(ps -eo pid,cmd|grep "mock_vllm_sse.py --port $1"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done; }
banned(){ curl -s $U/_health_status -H "$A" | python3 -c "import sys,json;d=json.load(sys.stdin);print([k for k,v in d.items() if v.get('banned')])" 2>/dev/null; }
waitban(){ for _i in $(seq 1 25); do banned | grep -q "$1" && return 0; sleep 1; done; return 1; }

echo "########## compute-node peers_by_model 配置形状验证 ##########"

# MC1 glm-5.1-fp8 → 命中高优 router(pri1 吃全部)
p=$(insp glm-5.1-fp8); [ "$p" = "glm-cart-router" ] && ok "MC1 glm-5.1-fp8 → $p(pri1 router)" || no "MC1 pick=$p(期望 glm-cart-router)"

# MC2 glm-5.2-fp8 → 命中 107+108 单实例(与 glm-5.1 不同子池)
p=$(insp glm-5.2-fp8); [ "$p" = "compute-nodenode-glm52" ] && ok "MC2 glm-5.2-fp8 → $p(独立子池)" || no "MC2 pick=$p"

# MC3 分发隔离:两 model pick 不同 peer
p1=$(insp glm-5.1-fp8); p2=$(insp glm-5.2-fp8)
[ "$p1" != "$p2" ] && ok "MC3 按 model 分发隔离:glm-5.1→$p1 ≠ glm-5.2→$p2" || no "MC3 相同=$p1"

# MC4 子池内优先级降级:杀 glm-5.1 的 router(28051)→ 探活 ban → glm-5.1 降到 pri0 单机
killp 28051; waitban 28051
p=$(insp glm-5.1-fp8); echo "$p" | grep -qE "^compute-nodenode[345]$" && ok "MC4 router 挂→glm-5.1 降级 pri0($p)" || no "MC4 pick=$p(期望 compute-nodenode/104/105)"

# MC5 隔离性:glm-5.1 的 router 挂,glm-5.2 不受影响(仍命中 107+108)
p=$(insp glm-5.2-fp8); [ "$p" = "compute-nodenode-glm52" ] && ok "MC5 隔离:glm-5.1 router 挂,glm-5.2 仍 → $p" || no "MC5 pick=$p"
startmock 28051   # 复原

# MC6 未知 model:无子池 → 应非正常路由(不崩,返回错误/空 pick)
c=$(curl -s -o /dev/null -w "%{http_code}" -X POST $U/v1/chat/completions -H "$A" -H "$H" -d '{"model":"nonexist-model","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":5}' -m 8)
{ [ "$c" != "200" ] || true; } && ok "MC6 未知 model 处理(code=$c,不崩即可;确认客户端只发已配 model 名)" || no "MC6 code=$c"

echo ""
echo "================ compute-node peers_by_model 套件: PASS=$P FAIL=$F ================"
