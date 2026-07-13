#!/bin/bash
# F5 兜底验证:路由 factory 抛错时 —— (a) /v1/ 返清晰 500(route not initialized);
# (b) 失败缓存生效 → factory 只跑一次(error.log 里 "register_route factory() failed" 恰 1 条,
# 而非每请求 1 条)。隔离 scratch openresty(listen 19570),自清理,不碰生产/LIVE。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/rifail}"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
# lua-refactor: init_by_lua_block 现只 require "router",前置 lua_package_path 指向 openresty/lua/(共享 helper)
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log info; pid logs/nginx.pid;
events { worker_connections 256; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict active_conns 4m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict cluster_avg 16k; lua_shared_dict lc_locks 1m; lua_shared_dict bad_peers 1m;
  lua_shared_dict api_keys 1m; lua_shared_dict bodylog_ctl 1m; lua_shared_dict cch_ctl 1m; lua_shared_dict reject_stat 128k;
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  include initblock.conf;
  server { listen 19570; server_name _;
    set $route "bad";
    set $routed_session_id "-"; set $routed_source "-"; set $routed_mode "-"; set $routed_peer "-";
    # 故意坏的 factory:抛错 → register_route pcall 捕获 → 返 nil + 缓存失败
    set_by_lua_block $__bad_init { _G.register_route("bad", function() error("intentional boom") end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts[ngx.var.route]) } proxy_pass http://vllm_backends; }
    location = /_health_status { content_by_lua_block { _G.dbg_health_status(_G.__route_opts[ngx.var.route]) } }
  }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q successful || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2

P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
code=""
for i in 1 2 3 4 5; do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:19570/v1/chat/completions -H "Content-Type: application/json" -d '{}')
done
[ "$code" = "500" ] && ok "坏 factory → /v1/ 返 500(route not initialized)" || no "期望 500,得 $code"
n=$(grep -c "register_route factory() failed" "$PREFIX/logs/error.log")
[ "$n" -eq 1 ] && ok "失败缓存生效:5 请求 factory 只报错 1 次" || no "factory 报错 $n 次(期望 1;未缓存=每请求 1 条)"
# dbg endpoint(走共享 _G.opts_missing)在坏路由上也返清晰 500(而非 nil-index 崩)
dcode=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:19570/_health_status)
[ "$dcode" = "500" ] && ok "dbg endpoint 坏路由 → 500(opts_missing 兜底)" || no "dbg 期望 500,得 $dcode"
echo "================ route init-fail 兜底: PASS=$P FAIL=$F ================"
[ "$F" -eq 0 ]
