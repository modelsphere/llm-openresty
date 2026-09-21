#!/usr/bin/env bash
# Phase 0 · #2 探针:验证「per-model .conf 只放 6 dict + 一行 # ROUTE_DATA,共享 init_worker loader
# 读数据行 → register_route」这条注册机制(真 lua/ 引擎,无 per-model server)。
# 成功标准:/_dump 显示 glm 已注册、peers 数正确、dict 名按 <base>_<route> 派生。
# 在有 openresty 的机器上跑(chat):bash test_route_loader_probe.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BASE="$HERE/.."                                   # openresty repo 根
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
LUA="${LUA:-$BASE/lua}"
PORT="${PORT:-19791}"
PREFIX="${PREFIX:-/tmp/loader_probe}"
FAIL=0; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c nginx.conf -s stop 2>/dev/null; pkill -9 -f "$PREFIX/logs/nginx.pid" 2>/dev/null; }
trap cleanup EXIT
rm -rf "$PREFIX"; mkdir -p "$PREFIX/logs" "$PREFIX/temp" "$PREFIX/routes"

# ① 抽 session_base 的全局 dict(17-26)+ init_by_lua_block(79-198)
sed -n '17,26p' "$BASE/session_base.conf" > "$PREFIX/global_dicts.conf"
# 按【标记】提取 init_by_lua_block,不用硬编码行号。
# 原本写死 sed -n '79,198p':session_base.conf 一旦增删行,这个窗口就整体推歪,
# 截出两个块的残片 → nginx 报 unexpected "," 起不来,看着像引擎坏了。
# 2026-09-18 踩到:bodylog gate 改动让该文件净增 22 行,init_by_lua_block 实际是 112-245,
# 而窗口 79-198 的首行落在 init_worker 块中间、末行落在 init_by_lua 块里。
awk '/^init_by_lua_block \{/{f=1} f{print} f&&/^\}/{exit}' \
    "$BASE/session_base.conf" > "$PREFIX/initblock.conf"
# 截空或没截到闭合花括号 = 提取失败,立刻报错,别让它变成一个看不懂的 nginx 语法错
if [ "$(wc -l < "$PREFIX/initblock.conf")" -lt 10 ] || ! tail -1 "$PREFIX/initblock.conf" | grep -q '^}'; then
  echo "FAIL: 从 session_base.conf 提取 init_by_lua_block 失败(行数=$(wc -l < "$PREFIX/initblock.conf"))"
  exit 1
fi

# ② per-model 路由文件:只有 6 个 suffixed dict + 一行 ROUTE_DATA(无 server)
cat > "$PREFIX/routes/session_route_glm.conf" <<'EOF'
lua_shared_dict active_conns_glm 4m;
lua_shared_dict cluster_avg_glm  16k;
lua_shared_dict lc_locks_glm     1m;
lua_shared_dict bad_peers_glm    1m;
lua_shared_dict bodylog_ctl_glm  1m;
lua_shared_dict cch_ctl_glm      1m;
# ROUTE_DATA {"name":"glm","peers":[["127.0.0.1",19599,"mock-0"],["127.0.0.1",19598,"mock-1"]],"ttft_limit_ms":60000}
EOF

# ③ 组装 nginx.conf:lua_path + 全局 dict + init_by_lua + init_worker(loader)+ per-model dicts + /_dump
cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log info; pid logs/nginx.pid;
events { worker_connections 64; }
http {
  access_log off;
  client_body_temp_path temp; proxy_temp_path temp; fastcgi_temp_path temp; uwsgi_temp_path temp; scgi_temp_path temp;
  lua_package_path '$LUA/?.lua;;';
  include global_dicts.conf;
  include routes/*.conf;                 # per-model:6 dict(http 上下文)+ ROUTE_DATA 注释
  upstream vllm_backends { server 0.0.0.1:1; balancer_by_lua_block { } }
  include initblock.conf;                # init_by_lua_block { require "router" ... }

  init_worker_by_lua_block {
    -- ★ 待落地到 session_base 的 loader:扫 routes/*.conf 的 ROUTE_DATA 行 → register_route
    local cjson = require "cjson.safe"
    local dir = ngx.config.prefix() .. "routes"
    local h = io.popen("cat " .. dir .. "/*.conf 2>/dev/null")
    if h then
      local n = 0
      for line in h:lines() do
        local m = line:match("^# ROUTE_DATA%s+(.+)\$")
        if m then
          local d = cjson.decode(m)
          if d and d.name then
            _G.register_route(d.name, function() return d end)
            n = n + 1
          else
            ngx.log(ngx.ERR, "ROUTE_DATA decode failed: ", line)
          end
        end
      end
      h:close()
      ngx.log(ngx.NOTICE, "route loader: registered ", n, " routes")
    end
  }

  server {
    listen $PORT; server_name _;
    location = /_dump {
      default_type application/json;
      content_by_lua_block {
        local cjson = require "cjson.safe"
        local out = {}
        for name, opts in pairs(_G.__route_opts or {}) do
          out[name] = { peers = opts.peers and #opts.peers or 0,
                        active_conns_dict = opts.active_conns_dict,
                        bad_peers_dict = opts.bad_peers_dict,
                        ttft_limit_ms = opts.ttft_limit_ms }
        end
        ngx.say(cjson.encode(out))
      }
    }
  }
}
EOF

echo "=== nginx -t ==="
"$OPENRESTY" -p "$PREFIX" -c nginx.conf -t 2>&1 | sed 's/^/  /'
"$OPENRESTY" -p "$PREFIX" -c nginx.conf || { bad "起不来"; sed -n '1,40p' "$PREFIX/logs/error.log" 2>/dev/null; exit 1; }
sleep 1
DUMP=$(curl -s "http://127.0.0.1:$PORT/_dump"); echo "  /_dump -> $DUMP"
echo "$DUMP" | grep -q '"glm"' && ok "glm 已注册(loader 读 ROUTE_DATA)" || bad "glm 未注册"
echo "$DUMP" | grep -q '"peers":2' && ok "peers 数=2(数据行解析对)" || bad "peers 数不对"
echo "$DUMP" | grep -q 'active_conns_glm' && ok "dict 名按 <base>_glm 派生" || bad "dict 名不对"
echo "$DUMP" | grep -q '"ttft_limit_ms":60000' && ok "opts 透传(ttft=60000)" || bad "opts 未透传"
echo "  --- error.log 尾部(看 loader/register 有无报错)---"; tail -6 "$PREFIX/logs/error.log" 2>/dev/null | sed 's/^/  /'
echo "=== 结果 ==="; [ "$FAIL" = 0 ] && echo "LOADER PROBE PASS ✅" || echo "LOADER PROBE FAIL ❌"; exit $FAIL
