#!/usr/bin/env bash
# 直接在 session_route_<route>.conf 的 factory 里**手写** ttft_metrics / tps_metrics。
#
# 为什么要这条:多指标 / 自定义分位不依赖任何外部工具 —— opts.*_metrics 就是 factory 表里
# 一个普通字段,与 ttft_limit_ms 地位相同。此前这只是「按代码推断可以」,而裸机
# (ts24/ts31/gateway-host)是生产,推断不够。
#
# 顺带守住 util.validate_metrics:手写是**人**在写,写错概率远高于工具渲染。坏表必须整份
# 丢弃 + 回落静态,而不是半份生效、也不能把路由打死。
#
# 隔离 scratch openresty(高位端口 19698),不碰 LIVE。
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_base.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/mhwtest}"; KEY=REDACTED-API-KEY
P=19698

pass=0; fail=0
ok(){ echo "  ✓ $*"; pass=$((pass+1)); }
no(){ echo "  ✗ $*"; fail=$((fail+1)); }

cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"

# 抽 session_base.conf 的 init_by_lua_block(引擎的 _G 配置),与其它 harness 同法
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1)
end=$(awk -v s="${srv:-999999}" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 1024; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 4m; lua_shared_dict tps_stat 4m; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
EOF
for r in good bad partial metricsonly; do cat >> "$PREFIX/nginx.conf" <<EOF
  lua_shared_dict active_conns_$r 4m; lua_shared_dict cluster_avg_$r 16k; lua_shared_dict lc_locks_$r 1m; lua_shared_dict bad_peers_$r 1m; lua_shared_dict bodylog_ctl_$r 1m; lua_shared_dict cch_ctl_$r 1m;
EOF
done
cat >> "$PREFIX/nginx.conf" <<EOF
  include initblock.conf;
  init_worker_by_lua_block {
    local base = function() return {
      peers = {{"127.0.0.1",28901,"m1",0,50}},
      ttft_limit_ms = 20000, tps_limit_tps = 30,
      default_max = 50, bodylog_default_enabled = false, health_check_interval = 9999 } end

    -- ① good:完全手写的多指标表,不经任何外部工具
    _G.register_route("good", function()
      local o = base()
      o.ttft_metrics = { { metric = "p95", q = 0.95, threshold = 35000 },
                         { metric = "avg",           threshold = 8000  } }
      o.tps_metrics  = { { metric = "p90", q = 0.1,  threshold = 12    } }
      return o
    end)

    -- ② bad:人手写错(q 越界)。必须**整份丢弃**回落静态,而不是半份生效、也不能让路由挂掉
    _G.register_route("bad", function()
      local o = base()
      o.ttft_metrics = { { metric = "p95", q = 1.5, threshold = 35000 } }
      return o
    end)

    -- ③ partial:只写了 ttft,没写 tps —— 两者应各自独立(ttft 用手写、tps 回落静态)
    _G.register_route("partial", function()
      local o = base()
      o.ttft_metrics = { { metric = "p50", q = 0.5, threshold = 9000 } }
      return o
    end)

    -- ④ metricsonly:**只有指标表,没有任何静态阈值**。
    --    生产上因为 _G.TPS_LIMIT_TPS=20 / _G.TTFT_LIMIT_MS=30000 有全局默认,这个组合不可达;
    --    这里把两个全局默认清掉,把它构造出来 —— 守住三处「闸门认指标表,端点也得认」:
    --      tps_dict_if_on 的闸门 / _tps_status 的 tps_limit_source / _ttft_status 的 enforcing。
    --    改闸门却不改端点,就会出现 active=true 而 source="none" 的自相矛盾。
    _G.TPS_LIMIT_TPS = nil
    _G.TTFT_LIMIT_MS = nil
    _G.register_route("metricsonly", function()
      return {
        peers = {{"127.0.0.1",28901,"m1",0,50}},
        default_max = 50, bodylog_default_enabled = false, health_check_interval = 9999,
        ttft_metrics = { { metric = "p90", q = 0.9, threshold = 15000 } },
        tps_metrics  = { { metric = "p95", q = 0.05, threshold = 7 } },
      }
    end)
  }
EOF
for r in good bad partial metricsonly; do cat >> "$PREFIX/nginx.conf" <<EOF
  server { listen $P; server_name $r;
    set \$route "$r";
    location = /_ttft_status { content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts[ngx.var.route]) } }
    location = /_tps_status  { content_by_lua_block { _G.dbg_tps_status(_G.__route_opts[ngx.var.route]) } }
    # ⑤ 要用,漏了会 404 → 返 HTML → JSON 解析炸,而且 override 从没设上 → 断言假过
    location = /_ttft_limit  { content_by_lua_block { _G.dbg_ttft_limit(_G.__route_opts[ngx.var.route]) } }
    location = /_tps_limit   { content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts[ngx.var.route]) } }
  }
EOF
done
echo "}" >> "$PREFIX/nginx.conf"

"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -2
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" || { echo "启动失败"; exit 1; }
sleep 2

q(){ curl -s --max-time 5 -H "Host: $1" "http://127.0.0.1:$P/_$2_status"; }
show(){ python3 -c "
import json,sys
d=json.load(sys.stdin); m=d['metrics'][0]
print(m['source'], json.dumps(m['items'], sort_keys=True))"; }

echo "########## 裸机手写 ttft_metrics / tps_metrics ##########"

echo "── ① good:手写多指标表"
s=$(q good ttft | show); echo "    ttft: $s"
case "$s" in
  declared*'"metric": "p95"'*'"q": 0.95'*'"threshold_ms": 35000'*'"metric": "avg"'*'"threshold_ms": 8000'*)
    ok "手写的两条 ttft 指标全部生效(纯 conf,无任何外部依赖)" ;;
  *) no "ttft: $s" ;;
esac
s=$(q good tps | show); echo "    tps : $s"
case "$s" in declared*'"metric": "p90"'*'"q": 0.1'*'"threshold_tps": 12'*) ok "手写 tps 指标生效(低尾 q=0.1)" ;;
  *) no "tps: $s" ;; esac

echo "── ② bad:q 越界 → 整份丢弃回落静态,路由仍可用"
s=$(q bad ttft | show); echo "    ttft: $s"
case "$s" in
  static*'"threshold_ms": 20000'*) ok "坏表被整份丢弃,回落静态 20000(不是半份生效)" ;;
  *) no "应回落 static/20000,得到: $s" ;;
esac
[ -n "$(q bad ttft)" ] && ok "路由未被打死(端点仍有响应)" || no "路由挂了"
grep -q "ttft_metrics.*整份丢弃" "$PREFIX/logs/error.log" && ok "error.log 有 ERR 说明原因" \
  || no "没打 ERR —— 静默降级,线上无从发现"

echo "── ③ partial:只写 ttft,tps 各自独立"
s=$(q partial ttft | show)
case "$s" in declared*'"metric": "p50"'*'"threshold_ms": 9000'*) ok "ttft 用手写(p50/9000)" ;; *) no "ttft: $s" ;; esac
s=$(q partial tps | show)
case "$s" in static*'"threshold_tps": 30'*) ok "tps 独立回落静态(30),不受 ttft 影响" ;; *) no "tps: $s" ;; esac

echo "── ⑤ 优先级:declared 压过 override(2026-09-01 重排)"
# 旧顺序是 override > declared。改成 declared > override 的理由:声明表是权威配置
# (k8s 上由 operator 从 CRD 渲染),不该被谁在某一台实例上手工压住而无人知晓。
# override 保留是给裸机(无 CRD)的免 reload 微调。
q(){ curl -s --max-time 5 -H "Host: $1" "http://127.0.0.1:$P/_$2_status"; }
curl -s --max-time 5 -H "Host: good" "http://127.0.0.1:$P/_ttft_limit?ms=9999" >/dev/null
s=$(q good ttft | show); echo "    设 override=9999 后: $s"
case "$s" in
  declared*'"threshold_ms": 35000'*) ok "⑤ 声明表仍然生效(override 被压住)" ;;
  override*) no "⑤ override 压过了声明表 —— 优先级没改对" ;;
  *) no "⑤ 意外: $s" ;;
esac
# 端点必须**如实说 override 没生效**,否则运维看到回显会以为压住了
echo "    原始返回: $(curl -s --max-time 5 -H "Host: good" "http://127.0.0.1:$P/_ttft_limit")"
curl -s --max-time 5 -H "Host: good" "http://127.0.0.1:$P/_ttft_limit" | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('    limit_override_ms=%s effective=%s reason=%s' % (d.get('limit_override_ms'), d.get('override_effective'), (d.get('override_ignored_reason') or '')[:48]))
sys.exit(0 if (d.get('limit_override_ms')==9999 and d.get('override_effective') is False and d.get('override_ignored_reason')) else 1)"   && ok "⑤ /_ttft_limit 如实报 effective=false + 原因" || no "⑤ 端点没说清 override 未生效(会误导运维)"
# 没有声明表的路由,override 仍然可用(裸机路径)
curl -s --max-time 5 -H "Host: bad" "http://127.0.0.1:$P/_ttft_limit?ms=7777" >/dev/null
s=$(q bad ttft | show); echo "    无声明表的路由: $s"
case "$s" in override*7777*) ok "⑤ 无声明表时 override 仍生效(裸机免 reload 微调保住)" ;; *) no "⑤ bad: $s" ;; esac
curl -s --max-time 5 -H "Host: good" "http://127.0.0.1:$P/_ttft_limit?ms=0" >/dev/null
curl -s --max-time 5 -H "Host: bad"  "http://127.0.0.1:$P/_ttft_limit?ms=0" >/dev/null

echo "── ④ metricsonly:只有指标表、无任何静态阈值"
raw(){ curl -s --max-time 5 -H "Host: metricsonly" "http://127.0.0.1:$P/_$1_status"; }
s=$(raw ttft | show); echo "    ttft: $s"
case "$s" in declared*'"metric": "p90"'*'"threshold_ms": 15000'*) ok "ttft 指标表生效(无静态兜底)" ;; *) no "ttft: $s" ;; esac
raw ttft | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('    ttft active=%s enforcing=%s ttft_limit_ms=%r' % (d['active'], d['enforcing'], d.get('ttft_limit_ms')))
sys.exit(0 if (d['active'] and d['enforcing']) else 1)"   && ok "enforcing=true(只看静态会误报 false)" || no "enforcing 与实际判定不符"
s=$(raw tps | show); echo "    tps : $s"
case "$s" in declared*'"metric": "p95"'*'"threshold_tps": 7'*) ok "tps 指标表生效" ;; *) no "tps: $s" ;; esac
raw tps | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('    tps active=%s opt_in=%s tps_limit_source=%s' % (d['active'], d['opt_in'], d['tps_limit_source']))
sys.exit(0 if (d['active'] and d['tps_limit_source']=='declared') else 1)"   && ok "闸门放行(active)且 tps_limit_source=declared —— 端点与闸门同口径"   || no "端点与闸门口径不一致(active=true 却报 none?)"

echo
echo "================ 裸机手写指标表: PASS=$pass FAIL=$fail ================"
[ "$fail" -eq 0 ]
