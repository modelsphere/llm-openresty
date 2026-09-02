#!/usr/bin/env bash
# 升级顺序安全性:**autoconfig 先升、openresty 还是旧版**时会不会出问题。
#
# 场景:operator 升到支持 SLO 的版本后,会往 session_route_<route>.conf 里渲染
#       ttft_metrics / tps_metrics 两个新键。而 openresty 若还是旧镜像,它不认识这两个键。
#
# 要回答的是:旧引擎加载这样的 conf 会不会 **报错 / 拒绝启动 / 路由挂掉**?
#
# 读代码的结论是"不会"(旧 register_route 没有未知键校验,只按名取自己认识的键),
# 但那是推理。这里用 **main 分支的引擎**真跑一遍。
#
# 前置:/tmp/oldeng/lua + /tmp/oldeng/session_base.conf 是从 main 导出的旧引擎
#       (git archive main lua session_base.conf)。
#
# 隔离 scratch(19699),不碰 LIVE。
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OLD=${OLD:-/tmp/oldeng}
ENGINE="$OLD/session_base.conf"
OPENRESTY=${OPENRESTY:-/usr/local/openresty/bin/openresty}
PREFIX=${PREFIX:-/tmp/oldcompat}; KEY=REDACTED-API-KEY
P=19699

pass=0; fail=0
ok(){ echo "  ✓ $*"; pass=$((pass+1)); }
no(){ echo "  ✗ $*"; fail=$((fail+1)); }

cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"

[ -d "$OLD/lua" ] || { echo "缺少旧引擎 $OLD/lua —— 先 git archive main lua session_base.conf"; exit 2; }

start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1)
end=$(awk -v s="${srv:-999999}" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$OLD/lua"

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 1024; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 4m; lua_shared_dict tps_stat 4m; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
  lua_shared_dict active_conns_n 4m; lua_shared_dict cluster_avg_n 16k; lua_shared_dict lc_locks_n 1m; lua_shared_dict bad_peers_n 1m; lua_shared_dict bodylog_ctl_n 1m; lua_shared_dict cch_ctl_n 1m;
  include initblock.conf;
  init_worker_by_lua_block {
    -- 这就是**新版 autoconfig 会渲染出来的 conf 形状**:静态阈值 + 两个新键。
    -- 旧引擎不认识 ttft_metrics / tps_metrics,应当直接忽略而不是报错。
    _G.register_route("n", function()
      return {
        peers = {{"127.0.0.1",28901,"m1",0,50}},
        ttft_limit_ms = 20000, tps_limit_tps = 30,
        default_max = 50, bodylog_default_enabled = false, health_check_interval = 9999,
        ttft_metrics = { { metric = "p95", q = 0.95, threshold = 35000 },
                         { metric = "avg",           threshold = 8000  } },
        tps_metrics  = { { metric = "p90", q = 0.1,  threshold = 12    } },
      }
    end)
  }
  server { listen $P; server_name n;
    set \$route "n";
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts[ngx.var.route]) } }
    location = /_tps_status  { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts[ngx.var.route]) } }
    location = /_health_status { content_by_lua_block { _G.dbg_health_status(_G.__route_opts[ngx.var.route]) } }
  }
}
EOF

echo "########## 旧引擎(main)加载含 ttft_metrics/tps_metrics 的新 conf ##########"

t=$("$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1)
echo "$t" | grep -q "successful" && ok "openresty -t 通过(新键不破坏配置语法)" || { no "配置校验失败: $t"; exit 1; }

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" || { no "启动失败"; exit 1; }
sleep 2
ok "旧引擎正常启动"

s=$(curl -s --max-time 5 -H "Host: n" "http://127.0.0.1:$P/_ttft_status")
echo "    /_ttft_status: $(echo "$s" | head -c 160)"
[ -n "$s" ] && echo "$s" | grep -q '"route"' && ok "路由注册成功,端点可用(未被未知键打死)" || no "端点无响应或异常"

# 旧引擎应当**忽略**指标表,阈值仍是静态的 20000
echo "$s" | grep -q '"ttft_limit_ms":20000' && ok "阈值仍是静态 20000(指标表被忽略,不是半生效)" \
  || no "ttft_limit_ms 不是 20000: $(echo "$s"|head -c 120)"
# 旧引擎没有 metrics 字段(那是新引擎才加的)
echo "$s" | grep -q '"metrics"' && no "旧引擎不该有 metrics 字段" || ok "无 metrics 字段(符合旧引擎)"

# error.log 不该因为未知键报错
grep -qiE "\[error\].*(ttft_metrics|tps_metrics)" "$PREFIX/logs/error.log" 2>/dev/null \
  && no "error.log 出现与新键相关的 ERROR" || ok "error.log 无新键相关报错"

echo
echo "================ 旧引擎兼容新 conf: PASS=$pass FAIL=$fail ================"
[ "$fail" -eq 0 ]
