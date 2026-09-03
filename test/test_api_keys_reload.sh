#!/bin/bash
# api_keys.lua 改了 key + `openresty -s reload` 后,LLM 路由必须立刻用新表。
#
# 背景:lua_shared_dict 的内容**跨 reload 存活**(实测 t33)。access.lua 原来用
# "__inited 灌过一次就不再灌",于是 VM 上"改 lua + reload"这条标准更新流程对 LLM
# 路由不生效 —— 而 video 路由是直接 require 模块查表的、立刻生效,两条路径分叉。
# 现在按 key 表指纹播种,这个脚本就是那条防线。
#
# 隔离 scratch openresty(listen 19571),自清理,不碰生产/LIVE。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/akreload}"
OLD_KEY="sk-old-aaaaaaaa"; NEW_KEY="sk-new-bbbbbbbb"
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  cp "$PREFIX/logs/error.log" /tmp/akreload-error.log 2>/dev/null; rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp" "$PREFIX/lua"
# lua/ 整份拷进 scratch:测试要改 api_keys.lua,绝不能动仓库里的原件
cp "$HERE/../lua/"*.lua "$PREFIX/lua/"
write_keys() {  # write_keys <key>
  cat > "$PREFIX/lua/api_keys.lua" <<LUA
local M = {}
M.keys = { ["$1"] = "test" }
function M.parse_bearer(h) return (h or ""):match("^Bearer%s+(.+)\$") end
function M.fingerprint(keys)
    local ks = {}
    for k in pairs(keys) do ks[#ks+1] = k end
    table.sort(ks)
    return ngx.md5(table.concat(ks, ","))
end
function M.check(h)
    local k = M.parse_bearer(h); if not k then return false, nil end
    local who = M.keys[k]; if not who then return false, nil end
    return true, who
end
return M
LUA
}
write_keys "$OLD_KEY"

# LEGACY_INIT=1:在 scratch 副本里把播种逻辑退回修复前的 "__inited 只灌一次",
# 用来确认这条测试真的能抓到那个 bug(不改仓库里的 lua)。
if [ "${LEGACY_INIT:-}" = 1 ]; then
  python3 - "$PREFIX/lua/access.lua" <<'PYX'
import sys
p = sys.argv[1]; s = open(p).read()
a = s.index("    -- 按 key 表的指纹播种")
b = s.index("    local auth = ngx.req.get_headers()")
s = s[:a] + '''    if not ak:get("__inited") then
        for k, v in pairs(opts.api_keys) do ak:set(k, v) end
        ak:set("__inited", "1")
    end
''' + s[b:]
open(p, "w").write(s)
PYX
  echo "  (LEGACY_INIT=1:用修复前的播种逻辑跑,期望后两条 FAIL)"
fi

cat > "$PREFIX/nginx.conf" <<NG
worker_processes 1; error_log logs/error.log info; pid logs/nginx.pid;
events { worker_connections 256; }
http {
  lua_package_path '$PREFIX/lua/?.lua;;';
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict active_conns 4m; lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k;
  lua_shared_dict cluster_avg 16k; lua_shared_dict lc_locks 1m; lua_shared_dict bad_peers 1m;
  lua_shared_dict api_keys 1m; lua_shared_dict bodylog_ctl 1m; lua_shared_dict cch_ctl 1m; lua_shared_dict reject_stat 128k;
  # 这些全局量平时由 session_base.conf 的 init 设,scratch 里得自己给,
  # 否则 opts 回退到 nil,access.lua 拿它做算术直接 500
  init_by_lua_block { require "router"; _G.RT_LIMIT_FACTOR = 2; _G.MAX_CONCURRENCY_PER_PEER = 100 }
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { _G.do_balancer() } }
  server { listen 19572; location / { return 200 "backend-ok"; } }
  server { listen 19571; server_name _;
    set \$route "t"; set \$routed_session_id "-"; set \$routed_source "-"; set \$routed_mode "-"; set \$routed_peer "-";
    set_by_lua_block \$__init { _G.register_route("t", function()
        -- dict 名显式指向上面声明的无后缀 dict(K2.5 历史路由就是这么用的);
        -- 不指的话 route.lua 会去找 <名>_t 后缀的 dict,缺了就 500(不是鉴权的问题)
        return { peers = { {"127.0.0.1", 19572, "mock", 0, 100} }, health_check = false,
                 active_conns_dict = "active_conns", ttft_dict = "ttft_stat", tps_dict = "tps_stat",
                 cluster_avg_dict = "cluster_avg", lc_locks_dict = "lc_locks", bad_peers_dict = "bad_peers",
                 cch_ctl_dict = "cch_ctl", bodylog_ctl_dict = "bodylog_ctl", reject_stat_dict = "reject_stat" }
    end); return "" }
    location /v1/ { access_by_lua_block { _G.do_route(_G.__route_opts[ngx.var.route]) } proxy_pass http://vllm_backends; }
  }
}
NG

command -v "$OPENRESTY" >/dev/null || { echo "SKIP: 没有 $OPENRESTY"; exit 0; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" || { echo "FAIL: 起不来"; exit 1; }
sleep 1
code() { curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" http://127.0.0.1:19571/v1/models; }

r1=$(code "$OLD_KEY"); r2=$(code "$NEW_KEY")
write_keys "$NEW_KEY"
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s reload; sleep 2
r3=$(code "$OLD_KEY"); r4=$(code "$NEW_KEY")

fail=0
chk() { if [ "$2" = "$3" ]; then echo "  PASS $1 (期望$2 实际$3)"; else echo "  FAIL $1 (期望$2 实际$3)"; fail=1; fi; }
chk "reload 前 旧key 可用"   200 "$r1"
chk "reload 前 新key 不认"   401 "$r2"
chk "reload 后 旧key 已失效" 401 "$r3"
chk "reload 后 新key 生效"   200 "$r4"
echo; [ $fail -eq 0 ] && echo "test_api_keys_reload: ALL PASS" || { echo "test_api_keys_reload: FAIL"; exit 1; }
