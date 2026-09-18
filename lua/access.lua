-- openresty/lua/access.lua
-- do_route / do_log_release / do_balancer
-- 三个入口(do_route/do_log_release/do_balancer)被 router_locations.inc 的 *_by_lua_block
-- 直调 → 仍挂 _G。其余引擎函数从各模块 require 取(access 加载序在它们之后,无环)。

local cjson        = require "cjson.safe"
local util         = require "util"
local route        = require "route"
local reject_rules = require "reject_rules"
local ttft         = require "ttft"
local tps          = require "tps"
local bodylog      = require "bodylog"
local reqtransform = require "reqtransform"
local api_keys     = require "api_keys"

-- ══════════════════════════════════════════════════════════════════════
-- _G.do_route(opts) — access_by_lua_block 主体（参数化路由选址）
-- ══════════════════════════════════════════════════════════════════════

function _G.do_route(opts)
    -- H1: nil opts 防御（register_route 失败 / set $route 名不匹配时返清晰 500，而非 nil index 崩）
    if util.opts_missing(opts) then
        ngx.log(ngx.ERR, "do_route: opts is nil — route not registered? check register_route factory / set $route")
        return ngx.exit(500)
    end
    -- ── API key 鉴权（从 Bearer 头取）──
    local ak = ngx.shared[opts.api_keys_dict]
    -- 播种策略:dict 条目按 key 表指纹加前缀(`<sig>:<key>`),而不是"整份 flush 再灌"。
    --
    -- 两个原因:
    --   1) 这个 dict 是**所有路由共用**的(session_base.conf: lua_shared_dict api_keys)。
    --      而 opts.api_keys 允许 per-route 覆盖 —— 一旦有路由用了不同的 key 表,
    --      flush 式播种会让两张表互相冲刷,还会在"A 灌完 → B 冲掉 → A 查表"之间
    --      产生**假 401**。加前缀则两张表并存,互不干扰。
    --   2) 改了 key 表 = 新前缀,旧条目自然查不到(等价于失效),不必显式删。
    --
    -- 指纹每 worker 只算一次,挂在 opts 上。
    opts._api_keys_sig = opts._api_keys_sig or api_keys.fingerprint(opts.api_keys)
    local sig = opts._api_keys_sig
    if not ak:get(sig) then
        for k, v in pairs(opts.api_keys) do ak:set(sig .. ":" .. k, v) end
        ak:set(sig, "1")            -- 这份表已播种的标记
    end
    -- dict 是**纯缓存**:没有任何地方在运行时增删 key(查过,只有这里写),
    -- 权威始终是 opts.api_keys 这张 Lua 表。所以查不到时回落到表本身 ——
    -- dict 写满被驱逐 / set 失败(ak:set 会返回 false)/ 被别处 flush 掉,
    -- 都不该变成"合法 key 被拒"。少了这层兜底,dict 一满就是 401 风暴。
    -- **这条路由**的 key 表为空 => 鉴权关闭,放行(fail-open)。openresty 是公网入口,
    -- 升级时 Secret 配错导致**全站 401** 比短暂无鉴权更糟。这不是静默:init 期打 ERR,
    -- /_health_status 报 _meta.api_keys_configured=false 供告警。
    --
    -- ⚠️ 判断必须在**这里**:本函数走 dict 缓存路径,不经过 api_keys.check(),
    -- 所以 check() 里那个 fail-open 短路对主路由不生效。少了它,空 key 表会让每个
    -- 请求都"查不到"而 401 —— 实测过(tools/openresty-keys/verify_failopen_behavior.sh):
    -- 主路由 401、而 guard() 路由放行,两条路径行为相反。
    --
    -- ⚠️ 判的是 opts.api_keys 这张**本路由的表**,不是全局 api_keys.configured。
    -- 现状是所有生产路由都用同一张公用表(video 路由走 guard(),也是同一张),
    -- 两种判法此刻等价;但 opts.api_keys 在设计上允许 per-route 覆盖
    -- (route.lua:137 的 `or` 默认值 —— ModelRoute CR 里**没有** api_keys 字段,
--  CRD 白名单只认 auth / auth_public_paths,写了会被校验拒),
    -- 测试 harness 已经在这么用。看全局开关的话,一条自带 key 表的路由会被
    -- "key 文件没挂上"这个与它无关的状态把鉴权整个关掉 —— 踩到过:
    -- test_api_keys_dict_full 的非法 key 被放行。
    -- 空表结论在 register_route 时就算好了(route.lua),这里只读不算:
    -- 表在 init 之后不再变,而每请求遍历(可能上千条)太贵;放在注册时还能让
    -- /_health_status 在该路由尚未服务过任何请求时也读得到真实状态。
    local auth = ngx.req.get_headers()["authorization"] or ""
    local akey = api_keys.parse_bearer(auth)
    if not opts._api_keys_empty
       and (not akey or not (ak:get(sig .. ":" .. akey) or opts.api_keys[akey])) then
        ngx.status = 401
        ngx.header["Content-Type"] = "application/json"
        ngx.say([[{"error":"missing or invalid api key"}]])
        return ngx.exit(401)
    end

    -- 新格式聚合路由:GET /v1/models 由 openresty 直接列出 model id。
    -- 门控在 peers_by_model → 老格式(flat peers)不拦截,继续往下 proxy 到后端,
    -- /v1/models 行为完全不变。放在鉴权之后 → 自带鉴权。
    if opts.peers_by_model and ngx.var.uri == "/v1/models" then
        return route.serve_models(opts)
    end

    -- 探测类请求(健康探测 /v1/models、/health)不进限流:探测不是推理,不该被并发/TTFT/TPS
    -- 限流 429。尤其两级 nginx 串联时,内层一忙就把探测 429 掉 → 外层健康检查误判后端不可用。
    -- 仅放行三个限流出口,仍走正常 pick + proxy(拿后端真实响应);503(全 banned)不放行。
    local is_probe = (ngx.var.uri == "/v1/models" or ngx.var.uri == "/health")

    -- ★ 早设 ngx.ctx.route_opts：让 prepare_request / bodylog_* / balancer 都能拿到
    ngx.ctx.route_opts = opts

    local peers     = opts.peers
    local peer_keys = opts.peer_keys
    local dict      = ngx.shared[opts.active_conns_dict]

    -- bodylog 必须在 prepare_request 之前抓 body
    bodylog.bodylog_capture_request(opts)
    local sid, src = reqtransform.prepare_request(opts)
    sid, src = route.affinity_gate(opts, sid, src)   -- 亲和性总开关:关则丢 sid → least_conn
    ngx.ctx.session_id     = sid
    ngx.ctx.session_source = src or "none"

    -- ── 规则化请求拒绝(reject_rules)──
    -- 按请求内容(max_tokens/stream/input_bytes/...)匹配规则,命中即返回可配 status(默认 429)。
    -- 放在池评估之前:规则拒绝不该消耗池评估。命中时 eval_reject_rules 内部 ngx.exit 直接结束请求;
    -- 未配 reject_rules 的路由 no-op(零行为变化)。探测请求(/v1/models、/health)不评估。
    if not is_probe then reject_rules.eval_reject_rules(opts) end

    -- 新格式 peers_by_model：按 body.model 选子池，重绑 peers/peer_keys。
    -- 未知/缺失 model → 400 + supported 列表（严格拒绝，不 fallback）。
    -- 老格式（无 peers_by_model）跳过，peers 仍是 opts.peers，行为不变。
    -- 注:GET /v1/models 已在上方(鉴权后)由 route.serve_models 早返,不会到这里;故列模型不受此 400 影响。
    if opts.peers_by_model then
        local pk
        peers, peer_keys, pk = route.resolve_pool(opts, ngx.ctx.req_model)
        if not peers then
            ngx.status = 400
            ngx.header["Content-Type"] = "application/json"
            ngx.say(cjson.encode({
                error = {
                    type = "model_not_supported",
                    message = "model '" .. tostring(ngx.ctx.req_model) .. "' not configured on this route",
                    supported_models = pk,
                }
            }))
            return ngx.exit(400)
        end
    end
    -- 暴露到 nginx var 给 access_log 用（截断 128 防 header 变量爆）
    local log_sid = sid or "-"
    ngx.var.routed_session_id = (#log_sid > 128) and (log_sid:sub(1,128) .. "...") or log_sid
    ngx.var.routed_source     = src or "none"

    -- 评估池(无锁;与 dbg 共用 assess_pool)。先判 503/429,过载时直接返回,
    -- 不进入带锁的 pick → least_conn 锁不会上过载路径。
    local a = route.assess_pool(opts, peers, peer_keys)
    -- 全部 banned → 503
    if a.empty then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.header["Retry-After"]  = "5"
        ngx.say(string.format([[{"error":"all peers banned, no healthy upstream","route":"%s"}]], opts.route_name))
        return ngx.exit(503)
    end
    local healthy_all  = a.healthy_all
    local active_level = a.active_level
    local pool_limit   = a.limit          -- 静态健康池容量(observability + 动态封顶用)
    local limit        = pool_limit
    -- 自适应并发(opt-in,与 TPS 硬熔断互斥):把池 limit 换成动态 adaptive_cc,hit_rt/hit_avg 一起收缩。
    -- 动态值不超当前健康池容量(有 peer 被 ban → pool_limit 降 → limit 跟着降,尊重实际容量)。
    -- cc 未初始化/过期(nil)→ 从 min 起步(慢启动,不从满容量开始);有值则 min(cc, pool_limit)。
    -- mincc 从**静态** maxcc 派生(derive_mincc,与 loop/dbg 同 base,报告==强制),再由外层 min(., pool_limit)
    -- 做 ban 感知封顶 —— base 不掺 ban,避免 dbg 报的 floor 与实际强制的 floor 在 peer-ban 下分叉。
    -- ⚠️ 仅当 tps 特性生效(a.tps_on)才套 min 起步:__off/_G.TPS_ENABLED 关时 tps_on=false →
    -- 保持 pool_limit(统一关掉 tps 限流 = 回满容量,不能反而掉到 min)。
    if opts.adaptive_cc and a.tps_on then
        local mincc = route.derive_mincc(opts, route.compute_static_max_cc(opts,
            opts.peers_by_model and ngx.ctx.req_model or nil))
        limit = math.min(a.adaptive_cc or mincc, pool_limit)
        -- 把已算好的实时并发 a.rt_sum(全 peer 含 banned 的真实在途,与下方 hit_rt 判 429 同一个值)存给 timer:
        -- do_adaptive_cc_loop 据此判压力(顶到 cc 才涨、余量太大则缩)。key 复用 a.tps_prefix(assess_pool 已算)。
        -- TTL 取 max(tps_ttl, interval×3):至少活到 ewma 过期,避免"ewma 还在但 rt_sum 过期成 0 → 拒绝爬/误缩"。
        local _tpd = ngx.shared[opts.tps_dict]
        if _tpd and a.tps_prefix then
            _tpd:set(a.tps_prefix .. "rt_sum", a.rt_sum,
                     math.max(opts.tps_ttl or 60, opts.adaptive_cc_interval * 3))
        end
    end
    local hit_rt  = a.rt_sum   >= limit * opts.rt_limit_factor
    -- 2026-07-02 暂时禁用均值判断,只用实时值 rt_sum 判过载(原: a.avg_5min >= limit)
    local hit_avg = false
    if (hit_rt or hit_avg) and not is_probe then
        ngx.status = 429
        do local rj=ngx.shared.reject_stat; if rj then rj:incr((opts.route_name or "-")..":concurrency",1,0) end end
        -- 修复1:自适应路由的并发 429 = 需求超过当前 cc(被拒的量 rt_sum 看不到)。
        -- 记 per-pool 拒绝计数,do_adaptive_cc_loop 每 tick 读+清零当"上区间被压抑需求"信号 → 快涨。
        if opts.adaptive_cc and a.tps_prefix then
            local _tpd = ngx.shared[opts.tps_dict]
            if _tpd then _tpd:incr(a.tps_prefix .. "rej", 1, 0,
                    math.max(opts.tps_ttl or 60, opts.adaptive_cc_interval * 3)) end
        end
        ngx.header["Content-Type"] = "application/json"
        ngx.header["Retry-After"]  = "1"
        local trig = hit_rt and ("realtime_" .. opts.rt_limit_factor .. "x") or "avg_5min"
        if opts.adaptive_cc then
            -- 自适应路由:limit/adaptive_cc 是浮点(AIMD 乘子),统一 %.1f;附静态 pool_limit 便于看收缩幅度
            ngx.say(string.format(
                [[{"error":"concurrency limit exceeded","trigger":"%s","realtime":%d,"avg_5min":%.1f,"limit":%.1f,"pool_limit":%d,"adaptive_cc":%.1f,"rt_factor":%s,"healthy_peers":%d,"active_level":%d,"route":"%s"}]],
                trig, a.rt_sum, a.avg_5min, limit, pool_limit, (a.adaptive_cc or pool_limit),
                tostring(opts.rt_limit_factor), #a.healthy_peers, active_level, opts.route_name))
        else
            -- 非自适应路由:与改动前逐字节一致(limit=静态整数,不加新字段)
            ngx.say(string.format(
                [[{"error":"concurrency limit exceeded","trigger":"%s","realtime":%d,"avg_5min":%.1f,"limit":%d,"rt_factor":%s,"healthy_peers":%d,"active_level":%d,"route":"%s"}]],
                trig, a.rt_sum, a.avg_5min, limit,
                tostring(opts.rt_limit_factor), #a.healthy_peers, active_level, opts.route_name))
        end
        return ngx.exit(429)
    end
    -- ── TTFT 主限流(按路由 opt-in:ttft_limit_ms 未配则整段跳过,行为零变化)──
    -- EWMA 超阈值进入限流态,但走半开探测:本窗口探测名额内的请求放行(继续测 TTFT),
    -- 其余 429。后端恢复 → 探测样本拉低 EWMA → 自动解除。
    -- 判定已由 assess_pool 里的 ttft_assess 做完(遍历声明的指标列表 OR);这里只取结果。
    -- 单指标(P0 默认)时 hit_ewma/hit_limit 就是旧的 a.ttft_ewma / ttft_limit_for → 响应体逐字节不变。
    local hit_ttft   = a.ttft_hit and a.ttft_hit.hit
    local ttft_ewma  = hit_ttft and a.ttft_hit.hit_ewma  or a.ttft_ewma
    local ttft_limit = hit_ttft and a.ttft_hit.hit_limit or nil
    -- 触发的是哪条指标。多指标(OR)时,光看 ttft_limit 只能反推,两条阈值相同就完全分不出来。
    local ttft_metric = hit_ttft and a.ttft_hit.hit_metric or "-"
    -- ttft_429_disabled:独立开关关掉硬 429(EWMA 测量 + cc 收缩仍在),TTFT 高只软控不硬拒。
    if hit_ttft and not is_probe and not ttft.ttft_429_disabled(opts) and not ttft.ttft_allow_probe(opts) then
        ngx.status = 429
        do local rj=ngx.shared.reject_stat; if rj then rj:incr((opts.route_name or "-")..":ttft",1,0) end end
        ngx.header["Content-Type"] = "application/json"
        ngx.header["Retry-After"]  = "1"
        ngx.say(string.format(
            -- metric **追加在末尾**,不插中间:遵循「只增字段,不改名、不删、不重排」——
            -- 429 body 是对外契约,虽然没有测试断言精确形状,但下游可能按位置解析。
            [[{"error":"ttft limit exceeded","trigger":"ttft_ewma","ttft_ewma":%.1f,"ttft_limit":%d,"model":"%s","healthy_peers":%d,"active_level":%d,"route":"%s","metric":"%s"}]],
            ttft_ewma, ttft_limit, ngx.ctx.req_model or "-", #a.healthy_peers, active_level, opts.route_name, ttft_metric))
        return ngx.exit(429)
    end
    -- ── TPS 主限流(opt-in:tps_limit_tps 未配 → tps_dict_if_on nil → a.tps_ewma nil → 跳过)──
    -- 方向与 TTFT 相反:EWMA <= 下限 进入限流态(解码速率太低=后端过载),半开探测机制同 TTFT。
    -- ⚠️ 与自适应并发互斥:adaptive_cc=true 时该 EWMA 已用于动态调 limit(上面并发 gate),这里跳过硬 429。
    -- 同 TTFT:判定在 assess_pool 里做完(tps_assess strict=false → `<=`,与改动前一致)
    local hit_tps   = (not opts.adaptive_cc) and a.tps_hit and a.tps_hit.hit
    local tps_ewma  = hit_tps and a.tps_hit.hit_ewma  or a.tps_ewma
    local tps_limit = hit_tps and a.tps_hit.hit_limit or nil
    local tps_metric = hit_tps and a.tps_hit.hit_metric or "-"   -- 同 TTFT:多指标时指出触发那一条
    if hit_tps and not is_probe and not tps.tps_allow_probe(opts) then
        ngx.status = 429
        do local rj=ngx.shared.reject_stat; if rj then rj:incr((opts.route_name or "-")..":tps",1,0) end end
        ngx.header["Content-Type"] = "application/json"
        ngx.header["Retry-After"]  = "1"
        ngx.say(string.format(
            [[{"error":"tps limit exceeded","trigger":"tps_ewma","tps_ewma":%.1f,"tps_limit":%.1f,"model":"%s","healthy_peers":%d,"active_level":%d,"route":"%s","metric":"%s"}]],
            tps_ewma, tps_limit, ngx.ctx.req_model or "-", #a.healthy_peers, active_level, opts.route_name, tps_metric))
        return ngx.exit(429)
    end
    -- 通过容量后才做带锁选址(与 dbg 共用 pick_from)
    local chosen_hp, mode = route.pick_from(opts, a, sid)
    local peer_key  = chosen_hp[4] or (chosen_hp[1] .. ":" .. chosen_hp[2])
    dict:incr(peer_key, 1, 0)

    ngx.ctx.chosen_host      = chosen_hp[1]
    ngx.ctx.chosen_port      = chosen_hp[2]
    ngx.ctx.peer_counter_key = peer_key
    ngx.ctx.routed_peer      = peer_key
    -- routed_peer_key:恒为干净的 "ip:port"(routed_peer 在重试时会被加 " (retry#N)"
    -- 后缀,拿它查 gpu_by_key/name_by_key 会全部落空)。查表与 header 回显都用这个。
    ngx.ctx.routed_peer_key  = peer_key
    -- 该 peer 所在节点的 GPU 型号(peer 命名字段 gpu,可选)。写进 bodylog 供按卡型聚合;
    -- 没配的 peer 为 nil,bodylog 字段缺失,不影响任何路由逻辑。
    ngx.ctx.routed_gpu       = opts.gpu_by_key and opts.gpu_by_key[peer_key] or nil
    ngx.ctx.routed_mode      = mode
    ngx.var.routed_mode = mode
    ngx.var.routed_peer = peer_key

    -- fallback peers（active 升序）。默认仅同活跃层;opts.cross_tier_fallback 开时含【低优层】
    -- (高优层 5xx/连接失败 → proxy_next_upstream 单请求即刻兜到低优 VIP,不必等 health-timer ban，
    --  省掉 ~30s 空窗)。靠谱前提:高优层要能【快失败】——cart 已加短 connect_timeout,否则请求会
    --  先干等高优层挂 ~30s 再兜底、更糟(当初就是因此把跨层 fallback shelve 掉、等 cart 短超时)。
    local fallback = {}
    local rest = {}
    local fb_pool = opts.cross_tier_fallback and healthy_all or a.healthy_peers
    for _, hp in ipairs(fb_pool) do
        local k = hp[4]
        if k ~= peer_key then
            rest[#rest + 1] = {{hp[1], hp[2]}, dict:get(k) or 0, hp[5]}
        end
    end
    table.sort(rest, function(a, b)
        if a[3] ~= b[3] then return a[3] > b[3] end   -- priority(层)降序
        return a[2] < b[2]                            -- 同层内 load 升序
    end)
    -- 组装 fallback:同活跃层兄弟【全保留】(正常 pod-to-pod failover);跨层时每个【低优层】只取
    -- 一个代表(load 最小)。为什么低优层不逐个试:兜底层通常是 Service VIP(kube-proxy 已对整层
    -- 做 LB),逐个试同层死 pod 只会把 max_more_tries 预算耗光、够不到最后一层 VIP 安全网
    -- (2026-08-03:多 pod-IP 层 + max_more_tries=3 时,retry 全耗在第二层死 pod、到不了第三层 VIP)。
    -- cross_tier 关时 fb_pool=同活跃层 → 全 prio==active_level → 等价旧行为(全保留),无副作用。
    local seen_lower = {}
    for _, r in ipairs(rest) do
        local prio = r[3]
        if prio == active_level then
            fallback[#fallback + 1] = r[1]            -- 同活跃层:全保留
        elseif not seen_lower[prio] then              -- 低优层:每层一个代表
            seen_lower[prio] = true
            fallback[#fallback + 1] = r[1]
        end
    end
    ngx.ctx.fallback_peers = fallback

    local banned_count = #peers - #healthy_all
    ngx.log(ngx.INFO, "[", opts.route_name, "] route sid=", sid or "<nil>",
            " source=", src or "none",
            " mode=", mode,
            " active=", dict:get(peer_key) or 0,
            " healthy=", #a.healthy_peers, "/", #peers,
            " active_level=", active_level,
            " banned=", banned_count,
            " fallbacks=", #fallback,
            " -> ", peer_key)
end

-- ══════════════════════════════════════════════════════════════════════
-- _G.do_emit_peer_header(opts) — header_filter_by_lua_block: 可选回显后端标识
-- ──────────────────────────────────────────────────────────────────────
-- 默认【关】(opts.expose_routed_peer 不为 true 即关):与 2026-07 删除 X-Routed-*
-- 那批 header 的安全取向一致——不向外泄露内部 peer。开启后回显【固定三段、| 分隔】:
--     X-Routed-Peer: <ip:port>|<GPU型号>|<peer名>
-- 例:10.0.0.1:8050|H100|gpu-node   取不到的段留空(如 10.1.0.1:8050||)——
-- 段数恒为 3,下游按 | split 取下标即可,不必判断有没有该段。
-- peer 取值优先用上游(CART)回报的【真实后端】($upstream_http_x_routed_peer);
-- 走直连兜底时上游没这个头,退回本层选中的 peer。GPU 型号与 peer 名取该 peer 的
-- gpu / 第 3 位字段(autoconfig 逐 peer 渲染进 conf:型号来自节点 GFD label,名为节点名)。
-- ⚠️ router_locations.inc 里的 proxy_hide_header 只挡【上游】那份,本函数写的是
-- 本层自己的响应头,两者不冲突:先挡掉上游的,再按开关决定要不要回显。
-- ══════════════════════════════════════════════════════════════════════
function _G.do_emit_peer_header(opts)
    if not opts or opts.expose_routed_peer ~= true then return end
    -- CART 回报的真实后端优先;没有(直连兜底/普通 vllm)则用本层选中的 peer
    local upstream_peer = ngx.var.upstream_http_x_routed_peer
    if upstream_peer == "" then upstream_peer = nil end
    -- ★上游(CART)回的是【完整 URL】,实测形如 "http://10.0.0.5:8000";而本层 peer
    --   表的 key 是 "ip:port"。不剥 scheme 直接查 gpu_by_key/name_by_key 必然落空,
    --   型号与名字两段会永远为空(2026-08-11 在 k8s 上实测 CART 回值才发现)。
    --   剥掉 scheme 与末尾斜杠后再查,回显值也与本层格式统一。
    if upstream_peer then
        upstream_peer = upstream_peer:gsub("^%a[%w+.%-]*://", ""):gsub("/+$", "")
    end
    -- 本层 peer 用干净 key(routed_peer 在重试时带 " (retry#N)" 后缀,不能拿来查表)
    local peer = upstream_peer or ngx.ctx.routed_peer_key
    if not peer or peer == "" then return end
    local gpu, name
    if upstream_peer then
        -- 真实 vllm 通常也在本层 peer 表里作低优兜底 → 能查到;查不到该段留空
        gpu  = opts.gpu_by_key  and opts.gpu_by_key[peer]  or nil
        name = opts.name_by_key and opts.name_by_key[peer] or nil
    else
        gpu  = ngx.ctx.routed_gpu
        name = opts.name_by_key and opts.name_by_key[peer] or nil
    end
    ngx.header["X-Routed-Peer"] = peer .. "|" .. (gpu or "") .. "|" .. (name or "")
end

-- ══════════════════════════════════════════════════════════════════════
-- _G.do_log_release(opts) — log_by_lua_block: 释放计数器 + 触发 bodylog
-- ══════════════════════════════════════════════════════════════════════
function _G.do_log_release(opts)
    -- H1: nil opts 防御（log_by_lua phase 不能 exit，只 log + 静默返）
    if not opts then
        ngx.log(ngx.ERR, "do_log_release: opts is nil")
        return
    end
    if ngx.ctx.peer_counter_key then
        local d = ngx.shared[opts.active_conns_dict]
        local v = d:incr(ngx.ctx.peer_counter_key, -1, 0)
        if v and v < 0 then d:set(ngx.ctx.peer_counter_key, 0) end
    end
    -- TTFT 入账(ttft_window 秒窗口直方图 → 每窗口 P80 折进 EWMA;总开关关 / dict 未声明则跳过=特性关)。
    -- 只采流式 + 2xx + 有首-chunk 计时的请求(含半开探测放行的请求)。
    -- 窗口边界 lazy 折叠;EWMA 带 TTL 做无流量自愈(流量停 → 过期 → assess_pool 读 nil → 放行)。
    local td = ttft.ttft_dict_if_on(opts)
    if td and ngx.ctx.ttft_is_stream and ngx.ctx.ttft_first_chunk_t
       and ngx.status and ngx.status >= 200 and ngx.status < 300 then
        ttft.ttft_record(opts, td, ngx.ctx.ttft_first_chunk_t * 1000)   -- 秒 → 毫秒
    end
    -- TPS 入账:2xx 即采,**流式与非流式一视同仁**。从尾缓冲 parse completion_tokens;
    -- 拿不到(no-usage)→ fail-open:不采样、不进 EWMA、绝不因此限流(只 incr nousage 计数做可观测)。
    -- ⚠️ 2026-09-07 口径变更(两处同步改:本文件 + bodylog-exporter-go/metrics.go):
    --   ① 不再要求 ttft_is_stream —— 非流式请求同样占并发、同样消耗后端算力,把它们排除在外
    --      会让「非流式为主」的子池两路 EWMA 长期为 nil,AIMD 退化成纯并发压力跟随、过载保护失效。
    --   ② 分母从「解码时长(总时长-TTFT)」改为**总时长**(即包含 prefill/TTFT 那一段)。
    --      这正是 ① 得以成立的前提:非流式只有一个 body chunk、first_chunk_t≈总时长,
    --      旧口径下分母≈0 会算出天文数字。改用总时长后两类请求共用同一个定义。
    --   代价:同一后端在新口径下测得的值系统性低于旧口径(prefill 占比越高差得越多),
    --      **历史数据不可比,tps_limit_tps / tps_metrics 的阈值需按新口径重定**。
    local tpd = tps.tps_dict_if_on(opts)
    if tpd and ngx.status and ngx.status >= 200 and ngx.status < 300 then
        local ctok
        local tail = ngx.ctx.tps_tail
        -- 取最后一个匹配:usage chunk 永远在流末尾,而响应正文里(如 tool-call 回显的 JSON)
        -- 可能更早出现字面量 "completion_tokens",match 取最左会误命中,gmatch 迭代留最后一个才稳。
        if tail then
            for n in tail:gmatch('"completion_tokens"%s*:%s*(%d+)') do ctok = tonumber(n) end
        end
        if ctok and ctok >= opts.tps_min_tokens then
            ngx.update_time()
            -- 分母 = 请求总时长(含 prefill/TTFT),流式与非流式同一定义。
            local elapsed = ngx.now() - ngx.req.start_time()
            -- 时长下限:< tps_min_decode_s 的短快响应(如 16 token / 3ms → 5000 tok/s)速率失真,
            -- 不能代表稳态吞吐,直接跳过(不采样、不计 nousage)。配合 tps_min_tokens 双重滤噪。
            if elapsed >= opts.tps_min_decode_s then
                tps.tps_record(opts, tpd, ctok / elapsed)   -- tokens / 总秒数
            end
        elseif not ctok then
            tpd:incr(tps.tps_key_prefix(opts) .. "nousage", 1, 0, opts.tps_ttl)  -- 可观测:无 usage 样本数
        end
    end
    bodylog.bodylog_finalize(opts)
end

-- ══════════════════════════════════════════════════════════════════════
-- _G.do_balancer() — balancer_by_lua_block: 首发 + 重试切 peer
-- 从 ngx.ctx.route_opts 取 dict 名（access 阶段已设）
-- ══════════════════════════════════════════════════════════════════════
function _G.do_balancer()
    local balancer = require "ngx.balancer"
    local opts = ngx.ctx.route_opts
    local attempt = ngx.ctx.balancer_attempt or 0
    ngx.ctx.balancer_attempt = attempt + 1

    if attempt == 0 then
        local host = ngx.ctx.chosen_host
        local port = ngx.ctx.chosen_port
        if not host then
            ngx.log(ngx.ERR, "balancer: no chosen peer in ngx.ctx")
            return
        end
        local fb = ngx.ctx.fallback_peers
        if fb and #fb > 0 then
            balancer.set_more_tries(math.min(tonumber(opts and opts.max_more_tries) or 2, #fb))
        end
        local ok, err = balancer.set_current_peer(host, port)
        if not ok then
            ngx.log(ngx.ERR, "set_current_peer failed: ", err)
        end
    else
        local fb = ngx.ctx.fallback_peers or {}
        local fi = math.min(attempt, #fb)
        if fi == 0 then
            ngx.log(ngx.ERR, "balancer retry #", attempt, " but no fallback peers")
            return
        end
        local new_peer = fb[fi]
        if not opts then
            ngx.log(ngx.ERR, "balancer retry: ngx.ctx.route_opts missing, cannot adjust counter")
        else
            local dict = ngx.shared[opts.active_conns_dict]
            local old_key = ngx.ctx.peer_counter_key
            if old_key then
                local v = dict:incr(old_key, -1, 0)
                if v and v < 0 then dict:set(old_key, 0) end
            end
            local new_key = new_peer[1] .. ":" .. new_peer[2]
            dict:incr(new_key, 1, 0)
            ngx.ctx.peer_counter_key = new_key
            ngx.ctx.routed_peer      = new_key .. " (retry#" .. attempt .. ")"
            ngx.ctx.routed_peer_key  = new_key   -- 干净 key(不带 retry 后缀),供查表/回显
            -- 重试换了 peer → GPU 型号跟着换(可能跨卡型 fallback)
            ngx.ctx.routed_gpu       = opts and opts.gpu_by_key and opts.gpu_by_key[new_key] or nil
        end
        local ok, err = balancer.set_current_peer(new_peer[1], new_peer[2])
        if not ok then
            ngx.log(ngx.ERR, "set_current_peer retry failed: ", err)
        end
        ngx.log(ngx.WARN, "[", opts and opts.route_name or "?", "] balancer retry #", attempt,
                " new=", new_peer[1], ":", new_peer[2])
    end
end
