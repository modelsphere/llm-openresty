#!/bin/bash
# adaptive_cc TTFT-aware 收缩验证(方案②:TTFT EWMA > 阈值也触发 cc ×DEC 收缩,压过「并发429→×INC」)。
# 隔离 openresty(ENGINE=重构版),不需要 mock 后端——直接给 ttft_stat / tps_stat dict 灌 EWMA/rt_sum/rej,
# 让 do_adaptive_cc_loop 的 timer 驱动 cc 演化(比跑真流量建 EWMA 确定得多、不 flaky)。
#
# 两条路由(共享 ttft_stat/tps_stat,key 按 route 前缀隔离):
#   r    : adaptive_cc_use_ttft 缺省(=开)
#   r2   : adaptive_cc_use_ttft=false(对照:TTFT 不参与,退回旧纯 TPS 行为)
# 同样喂「TTFT=800ms(>limit 500)+ TPS=50(>阈值30,解码健康)+ 并发429 rej>0」:
#   T1  r  → cc 单调 ×DEC 往 min 收(TTFT 过载置顶收缩)         ← 方案②生效
#   T2  r2 → cc ×INC 往 max 涨(use_ttft=false → rej 分支照旧涨) ← gating/向后兼容对照
#   T3  r  TTFT 恢复到 100(<limit)后,rej>0 → cc 重新 ×INC 回涨  ← 只有 TTFT 在压 cc,恢复即松
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ENGINE="${ENGINE:-$HERE/../session_route.conf}"
OPENRESTY="${OPENRESTY:-/usr/local/openresty/bin/openresty}"
PREFIX="${PREFIX:-/tmp/ccttfttest}"; KEY=REDACTED-API-KEY
cleanup(){ "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s stop 2>/dev/null; sleep 1
  for pid in $(ps -eo pid,cmd|grep "$PREFIX/nginx"|grep -v grep|awk '{print $1}'); do kill -9 "$pid" 2>/dev/null; done
  rm -rf "$PREFIX"; }
trap cleanup EXIT
mkdir -p "$PREFIX/logs" "$PREFIX/temp"
start=$(grep -n "^init_by_lua_block {" "$ENGINE"|head -1|cut -d: -f1)
srv=$(grep -n "^server {" "$ENGINE"|head -1|cut -d: -f1); end=$(awk -v s="$srv" 'NR<s && /^}/{l=NR} END{print l}' "$ENGINE")
sed -n "${start},${end}p" "$ENGINE" > "$PREFIX/initblock.conf"
source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"
cat > "$PREFIX/nginx.conf" <<'EOF'
worker_processes 1; error_log logs/error.log warn; pid logs/nginx.pid;
events { worker_connections 1024; }
http {
  access_log off; client_body_temp_path temp/cb; proxy_temp_path temp/p; fastcgi_temp_path temp/f; uwsgi_temp_path temp/u; scgi_temp_path temp/s;
  lua_shared_dict ttft_stat 256k; lua_shared_dict tps_stat 256k; lua_shared_dict api_keys 1m; lua_shared_dict reject_stat 128k;
  lua_shared_dict active_conns_r 4m; lua_shared_dict cluster_avg_r 16k; lua_shared_dict lc_locks_r 1m; lua_shared_dict bad_peers_r 1m; lua_shared_dict bodylog_ctl_r 1m; lua_shared_dict cch_ctl_r 1m;
  lua_shared_dict active_conns_r2 4m; lua_shared_dict cluster_avg_r2 16k; lua_shared_dict lc_locks_r2 1m; lua_shared_dict bad_peers_r2 1m; lua_shared_dict bodylog_ctl_r2 1m; lua_shared_dict cch_ctl_r2 1m;
  include initblock.conf;
  init_worker_by_lua_block {
    math.randomseed(ngx.now()*1000 + ngx.worker.pid())
    _G.ADAPTIVE_CC_TTL = 60
    -- 静态 max = 200+200 = 400;min=5;阈值 tps=30;ttft_limit=500ms;interval=1s,inc=1.5/dec=0.7 加速收敛便于测
    local mk = function() return {
      peers = {{"127.0.0.1",28961,"m1",0,200},{"127.0.0.1",28962,"m2",0,200}},
      tps_limit_tps=30, ttft_limit_ms=500, rt_limit_factor=1,
      adaptive_cc=true, adaptive_cc_min=5, adaptive_cc_abs=5, adaptive_cc_interval=1,
      adaptive_cc_inc=1.5, adaptive_cc_dec=0.7,
      default_max=400, bodylog_default_enabled=false, health_check_interval=9999 } end
    _G.register_route("r",  function() local o=mk(); return o end)                                    -- use_ttft 缺省=开
    _G.register_route("r2", function() local o=mk(); o.adaptive_cc_use_ttft=false; return o end)      -- 对照:关
  }
  server { listen 19697; server_name _;
    # 灌值:/_seed?route=r&ttft=..&tps=..&cc=..&rt=..&rej=..  (缺省不设的字段不动)
    location = /_seed { content_by_lua_block {
        local a = ngx.req.get_uri_args(); local rt = a.route or "r"
        local ts = ngx.shared.ttft_stat; local tp = ngx.shared.tps_stat
        if a.ttft then ts:set(rt..":ewma", tonumber(a.ttft), 60) end
        if a.tps  then tp:set(rt..":ewma", tonumber(a.tps),  60) end
        if a.cc   then tp:set(rt..":adaptive_cc", tonumber(a.cc), 60) end
        if a.rt   then tp:set(rt..":rt_sum", tonumber(a.rt), 60) end
        if a.rej  then tp:set(rt..":rej", tonumber(a.rej), 60) end
        ngx.say("ok") } }
    # 读当前 cc:/_cc?route=r
    location = /_cc { content_by_lua_block {
        local rt = ngx.req.get_uri_args().route or "r"
        local v = ngx.shared.tps_stat:get(rt..":adaptive_cc")
        ngx.say(v and string.format("%.2f", v) or "nil") } }
  }
}
EOF
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t 2>&1 | tail -1 | grep -q "successful" || { echo "nginx -t FAILED"; "$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf" -t; exit 1; }
"$OPENRESTY" -p "$PREFIX" -c "$PREFIX/nginx.conf"; sleep 2
R=http://127.0.0.1:19697
P=0; F=0; ok(){ echo "  ✓ $1"; P=$((P+1)); }; no(){ echo "  ✗ FAIL: $1"; F=$((F+1)); }
seed(){ curl -s "$R/_seed?route=$1&ttft=$2&tps=$3&cc=$4&rt=$5&rej=$6" >/dev/null; }
# 持续 re-seed rej/rt/tps/ttft(不含 cc,让 timer 演化 cc)dur 秒
sustain(){ local rt=$1 tf=$2 tp=$3 rs=$4 rj=$5 dur=$6; local endt=$((SECONDS+dur))
  while [ "$SECONDS" -lt "$endt" ]; do curl -s "$R/_seed?route=$rt&ttft=$tf&tps=$tp&rt=$rs&rej=$rj" >/dev/null; sleep 0.4; done; }
cc(){ curl -s "$R/_cc?route=$1"; }
gt(){ awk "BEGIN{exit !($1>$2)}"; }; lt(){ awk "BEGIN{exit !($1<$2)}"; }

echo "########## 初始:两路由都置 cc=60(TPS 健康 50>30, TTFT 过载 800>500, 并发429 rej=20)##########"
seed r  800 50 60 60 20; seed r2 800 50 60 60 20
echo "  r 初始 cc=$(cc r)  r2 初始 cc=$(cc r2)"

echo "########## 持续 6s:同样 TTFT 过载 + rej>0,看两路由 cc 走向 ##########"
sustain r  800 50 60 20 6 & sustain r2 800 50 60 20 6 & wait
ccr=$(cc r); ccr2=$(cc r2)
echo "  6s 后:r cc=$ccr  |  r2 cc=$ccr2"
lt "$ccr" 60  && ok "T1 use_ttft=开: TTFT 过载 → cc 从 60 收缩 (cc=$ccr <60,朝 min=5)" || no "T1 cc=$ccr 没收缩(应 <60)"
lt "$ccr" 30  && ok "T1b cc 明显下探 (cc=$ccr <30)"                                  || no "T1b cc=$ccr 收缩不足"
gt "$ccr2" 60 && ok "T2 对照 use_ttft=false: 同样 TTFT 过载但 cc 反而涨 (cc=$ccr2 >60,rej→×INC 旧行为)" || no "T2 cc=$ccr2 没涨(对照应 >60,证 TTFT 未参与)"

echo "########## r 恢复:TTFT=100(<limit 500),rej 仍>0,持续 6s ##########"
seed r 100 50 "$ccr" 60 20    # 重置 ttft 到健康(cc 保持当前低值)
sustain r 100 50 60 20 6
ccr_rec=$(cc r)
echo "  恢复后:r cc=$ccr_rec (恢复前 $ccr)"
gt "$ccr_rec" "$ccr" && ok "T3 TTFT 恢复(<阈值)→ cc 重新回涨 (cc $ccr→$ccr_rec,rej→×INC 解锁)" || no "T3 cc 没回涨 ($ccr→$ccr_rec)"

echo "================ adaptive_cc TTFT-aware 收缩: PASS=$P FAIL=$F ================"
[ "$F" -eq 0 ] && exit 0 || exit 1
