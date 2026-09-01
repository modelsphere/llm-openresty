-- openresty/lua/debug_endpoints.lua
-- 全部 _G.dbg_*:调试 / 热开关 endpoint 实现
-- dbg_* 由 router_locations.inc 的 content_by_lua_block 直调 → 仍挂 _G;
-- 内部用到的引擎函数从各模块 require 取(debug 加载序最后,无环)。

local cjson_dbg    = require "cjson.safe"
local util         = require "util"
local route        = require "route"
local ttft         = require "ttft"
local tps          = require "tps"
local bodylog      = require "bodylog"
local reqtransform = require "reqtransform"

-- ══════════════════════════════════════════════════════════════════════
-- _G.dbg_* — 调试 endpoint 实现（参数化，所有路由共享）
-- 调用方：server 块的 content_by_lua_block { _G.dbg_xxx(_G.__route_opts[ngx.var.route]) }
-- ══════════════════════════════════════════════════════════════════════

function _G.dbg_health_status(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    -- M1: peers={} 防御（K2.6 默认空 peers 时调试 endpoint 不崩）
    if not opts.peer_keys or #opts.peer_keys == 0 then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"no peers configured","route":"%s"}]], opts.route_name or "?"))
        return
    end
    local bad = ngx.shared[opts.bad_peers_dict]
    local dict = ngx.shared[opts.active_conns_dict]
    local out = {}
    for _, k in ipairs(opts.peer_keys) do
        out[k] = {
            active = dict:get(k) or 0,
            banned = bad:get(k) and true or false,
        }
    end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode(out))
end

function _G.dbg_active_conns(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    -- M1: peers={} 防御（K2.6 默认空 peers 时调试 endpoint 不崩）
    if not opts.peer_keys or #opts.peer_keys == 0 then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"no peers configured","route":"%s"}]], opts.route_name or "?"))
        return
    end
    local dict = ngx.shared[opts.active_conns_dict]
    local out = {}
    for _, k in ipairs(opts.peer_keys) do
        out[k] = dict:get(k) or 0
    end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode(out))
end

function _G.dbg_cluster_avg(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    -- M1: peers={} 防御（K2.6 默认空 peers 时调试 endpoint 不崩）
    if not opts.peer_keys or #opts.peer_keys == 0 then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"no peers configured","route":"%s"}]], opts.route_name or "?"))
        return
    end
    local ca = ngx.shared[opts.cluster_avg_dict]
    local ac = ngx.shared[opts.active_conns_dict]
    local realtime_sum = 0
    for _, k in ipairs(opts.peer_keys) do
        realtime_sum = realtime_sum + (ac:get(k) or 0)
    end
    local buckets = {}
    for i = 0, 9 do
        buckets[tostring(i)] = ca:get(tostring(i))
    end
    local _, samples, avg = route.compute_cluster_avg(opts.cluster_avg_dict)
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route          = opts.route_name,
        realtime_sum   = realtime_sum,
        buckets        = buckets,
        samples        = samples,
        avg_5min       = avg,
        cur_bucket_idx = math.floor(ngx.now() / (opts.cluster_avg_interval or 30)) % 10,
    }))
end

-- TTFT 限流观测:总开关状态 + 各池 EWMA、阈值、半开探测本窗口已用名额。
--   global_enabled=_G.TTFT_ENABLED;runtime_off=本路由热关;active=两开关都开且 dict 声明;
--   enforcing=active 且配了 ttft_limit_ms。active=false 即完全旧逻辑。
function _G.dbg_ttft_status(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.ttft_dict]   -- 原始 handle(用于展示,即便热关也能看 EWMA)
    local rp = (opts.route_name or "?") .. ":"   -- route 前缀(共享 dict)
    -- ewma_ms 保持老形状(model → 数值,单指标时取第一条),新增 metrics 展示完整指标列表。
    local ewmas, mstat = {}, {}
    if td then
        local models = {}
        if opts.peers_by_model then
            for m in pairs(opts.peers_by_model) do models[#models+1] = m end
        else
            models[1] = false
        end
        for _, m in ipairs(models) do
            local key = (m == false) and "_" or m
            local mlist, msrc = ttft.ttft_metrics(opts, m)   -- msrc: override|declared|static
            local list = {}   -- 纯数组:混入字符串 key 会让 cjson 编成对象,source 挂外层 wrapper
            for _, mt in ipairs(mlist) do
                list[#list+1] = { metric = mt.metric, q = mt.q,
                                  threshold_ms = mt.threshold,
                                  ewma_ms = td:get(ttft.ttft_ewma_key(opts, m, mt.metric)) }
            end
            ewmas[key] = list[1] and list[1].ewma_ms or nil
            -- ⚠️ metrics 用**数组**而不是「按 model 键的 map」:回归脚本 test_ttft.sh 的 ewv() 是
            --    `grep -o '"_":[0-9.]*'` 直接在原始 JSON 文本里捞,而 [0-9.]* 能匹配零个字符 ——
            --    若这里也出现 `"_":[`,head -1 可能取到它,数字部分为空 → ewma 假报空。
            --    数组形状下只有 "model":"_",不会产生裸的 `"<model>":` 键。
            -- shadowed:override 生效时报出它盖住的静态值(override 现在只压静态那一层)。
            -- ignored_override:反过来 —— 声明表把 override 压住了。这条更要报:运维设完
            -- override 看到端点回显了值,很容易以为生效,实际没有。
            local shadow, ignored
            if msrc == "override" then
                shadow = { source = "static", threshold_ms = ttft.ttft_static_limit_for(opts, m) }
            elseif msrc == "declared" then
                local o = ttft.ttft_override_for(opts, m)
                if o then ignored = { override_ms = o, reason = "declared 指标表优先,override 不生效" } end
            end
            mstat[#mstat+1] = { model = key, source = msrc, items = list, shadowed = shadow, ignored_override = ignored }
        end
    end
    local active = ttft.ttft_dict_if_on(opts) and true or false
    local win = td and math.floor(ngx.now() / opts.ttft_probe_window) or 0
    ngx.say(cjson_dbg.encode({
        route                 = opts.route_name,
        global_enabled        = _G.TTFT_ENABLED and true or false,
        dict_declared         = td and true or false,
        runtime_off           = (td and td:get(rp .. "__off")) and true or false,
        active                = active,
        -- 声明了指标表但没有静态 ttft_limit_ms 时也在判定 —— 只看静态会误报 false。
        -- (_G.TTFT_LIMIT_MS 有全局默认 30000,今天填得满,但口径要跟判定链一致。)
        enforcing             = (active and (opts.ttft_limit_ms or opts.ttft_metrics)) and true or false,
        ttft_429_enabled      = not ttft.ttft_429_disabled(opts),   -- 硬 429 独立开关(false=只软控/cc收缩,不硬拒)
        ttft_limit_ms         = opts.ttft_limit_ms or nil,          -- 路由级默认阈值
        ttft_limit_by_model   = opts.ttft_limit_by_model or nil,    -- 每模型覆盖(peers_by_model)
        ewma_ms               = ewmas,
        alpha                 = opts.ttft_ewma_alpha,
        ttl                   = opts.ttft_ttl,
        probe_window          = opts.ttft_probe_window,
        probe_per_window      = opts.ttft_probe_per_window,
        probe_used_cur_window = td and (td:get(rp .. "probe:" .. win) or 0) or 0,
        -- ↓ 新增(只增不改不重排,回归套件依赖既有字段)
        metrics               = mstat,                        -- 每模型指标列表 + source(override|declared|static)
        dict_capacity         = td and td:capacity() or nil,  -- 容量观测:直方图 key 数随模型/指标增长,
        dict_free_space       = td and td:free_space() or nil,--   一旦 LRU 淘汰 ewin/fd 锁会静默坏掉折叠
    }))
end

-- POST /_ttft_toggle?on=0 → 关本路由 TTFT(写 ttft_dict 的 "__off",免 reload);
--      on=1 → 开(删 "__off")。仅 127.0.0.1。全局 _G.TTFT_ENABLED=false 时此开关无意义(已全关)。
function _G.dbg_ttft_toggle(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.ttft_dict]
    if not td then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ route = opts.route_name, ok = false,
            error = "ttft_dict not declared" }))
        return
    end
    local offk = (opts.route_name or "?") .. ":__off"   -- route 维度热关 key(共享 dict)
    local on = ngx.var.arg_on
    if on == "0" then td:set(offk, true)
    elseif on == "1" then td:delete(offk)
    else
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ ok = false, error = "use ?on=0 (disable) | ?on=1 (enable)" }))
        return
    end
    ngx.say(cjson_dbg.encode({
        route = opts.route_name, ok = true,
        global_enabled = _G.TTFT_ENABLED and true or false,
        runtime_off    = td:get(offk) and true or false,
        active         = ttft.ttft_dict_if_on(opts) and true or false,
    }))
end

-- POST /_ttft_429_toggle?on=0 → 只关本路由的 TTFT **硬 429**(写 ttft_dict 的 "__429off",免 reload,跨 reload 持久);
--      on=1 → 恢复(删 "__429off")。与 /_ttft_toggle 不同:这里 EWMA 测量 + cc 收缩(ttft_overloaded)照常,
--      只是 TTFT 高时不再硬拒新请求(软控保留)。全局 _G.TTFT_ENABLED=false 或 __off 时此开关无意义(TTFT 已全关)。
function _G.dbg_ttft_429_toggle(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.ttft_dict]
    if not td then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ route = opts.route_name, ok = false, error = "ttft_dict not declared" }))
        return
    end
    local offk = (opts.route_name or "?") .. ":__429off"
    local on = ngx.var.arg_on
    if on == "0" then td:set(offk, 1)            -- on=0 → 关硬 429(override)
    elseif on == "1" then td:set(offk, 0)         -- on=1 → 开硬 429(override,压过 factory default-off;用 0 而非删 key)
    else
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ ok = false, error = "use ?on=0 (关硬429,保留软控) | ?on=1 (恢复硬429)" }))
        return
    end
    ngx.say(cjson_dbg.encode({
        route            = opts.route_name, ok = true,
        ttft_429_enabled = not ttft.ttft_429_disabled(opts),   -- 硬 429 当前是否开
        note             = "关掉后 EWMA/cc-shrink 仍在,只是不再硬拒",
    }))
end

-- 在线热改 TTFT 阈值(免 reload、跨 worker 一致;写共享字典 <route>[:<model>]:limit_override)。
-- GET  /_ttft_limit                  → 看当前 override + 静态默认
-- GET  /_ttft_limit?ms=45000         → 设路由级阈值 45s
-- GET  /_ttft_limit?ms=45000&model=X → 设某模型阈值(仅 peers_by_model 路由)
-- GET  /_ttft_limit?ms=0 [&model=X]  → 清除 override(回落静态默认)
-- 注意:override 存共享字典、无 TTL,会跨 reload 存活;改 conf 默认值要同时清 override 才生效。
-- 手工 override 的存活时长。**默认过期**是刻意的:
--   lua_shared_dict 跨 reload 存活,而 override 压过声明表 —— 无 TTL 的话,半夜应急设的
--   一个值会让该 route 声明的阈值**静默失效数月**,而且从任何端点都看不出来。
--   给它一个自动回落的期限,让"忘了删"从一个长期故障退化成一段有限的偏离。
-- ?ttl=0 显式表示永不过期(真需要长期钉住时用),响应里会标出来。
local OVERRIDE_TTL_DEFAULT = 7200   -- 2h

-- 返回 ttl(秒), 错误字符串;ttl==0 表示永久
local function parse_override_ttl(arg_ttl)
    if arg_ttl == nil then return OVERRIDE_TTL_DEFAULT end
    local n = tonumber(arg_ttl)
    if not n or n < 0 then return nil, "ttl must be >= 0 (0 = 永不过期)" end
    return n
end

-- shared dict 的 :ttl() 对「无过期时间」的 key 返回 0,与我们的 0 语义一致,直接透传。
local function override_ttl_of(td, k)
    if td:get(k) == nil then return nil end
    local t = td:ttl(k)
    return t or nil
end

function _G.dbg_ttft_limit(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.ttft_dict]
    if not td then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ route = opts.route_name, ok = false, error = "ttft_dict not declared" }))
        return
    end
    local model = ngx.var.arg_model
    local k = (opts.route_name or "?") .. ":" .. (model and (model .. ":") or "") .. "limit_override"
    local ms = ngx.var.arg_ms
    local ttl, terr = parse_override_ttl(ngx.var.arg_ttl)
    if terr then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ ok = false, error = terr }))
        return
    end
    if ms then
        local n = tonumber(ms)
        if not n or n < 0 then
            ngx.status = 400
            ngx.say(cjson_dbg.encode({ ok = false, error = "use ?ms=0 清除 | ?ms=<正整数> 设阈值 [&model=X] [&ttl=秒,默认7200,0=永久]" }))
            return
        end
        if n == 0 then td:delete(k) else td:set(k, n, ttl) end
    end
    -- ⚠️ 必须回答「这次设置到底生效没有」:声明了指标表(CRD 渲染 / 手写)时 override 被压住,
    -- 而端点若只回显 limit_override_ms,运维会以为压住了。effective=false 时给出原因。
    -- ⚠️ Lua 陷阱:不能写 `has_ovr and eff or nil` —— eff 为 false 时 `x and false or nil`
    --    恒得 nil,字段会整个消失,前端看到的是"没设 override",与事实相反。
    --    先算好再放进表里。
    local eff = ttft.ttft_override_effective(opts, model or false)
    local ovr_eff = nil
    if td:get(k) ~= nil then ovr_eff = eff end
    local declared = opts.ttft_metrics and true or false
    ngx.say(cjson_dbg.encode({
        route             = opts.route_name, ok = true,
        model             = model or nil,
        limit_override_ms = td:get(k) or nil,             -- 当前 override(nil=未设,回落静态)
        static_limit_ms   = opts.ttft_limit_ms or nil,    -- 路由级静态默认
        static_by_model   = opts.ttft_limit_by_model or nil,
        priority          = "declared(路由声明的指标表) > override > 静态per-model > 静态route",
        -- ↓ 新增(只增不改不重排)
        override_ttl_s    = override_ttl_of(td, k),       -- 剩余秒数(0=永不过期;nil=未设 override)
        override_effective = ovr_eff,                     -- false = 设了但被声明表压住;nil = 没设
        override_ignored_reason = (td:get(k) ~= nil and not eff)
            and "route declares ttft_metrics; declared table wins over manual override" or nil,
        -- override 生效时报出它盖住的静态值(重排后 override 只压静态那一层)
        shadowed_limit_ms = (td:get(k) ~= nil and eff)
            and ttft.ttft_static_limit_for(opts, model or false) or nil,
        declares_metrics  = declared,
    }))
end

-- ── TPS 限流观测/管理端点(镜像 dbg_ttft_*)──
-- GET /_tps_status:总开关 + 各池 EWMA(tokens/sec)+ 下限 + 本窗口探测名额 + nousage 漏采计数。
function _G.dbg_tps_status(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.tps_dict]   -- 原始 handle(即便热关也能看 EWMA)
    local rp = (opts.route_name or "?") .. ":"
    local ewmas, nousage = {}, {}
    -- 自适应并发观测:每子池当前动态上限 adaptive_cc + 生效 min(派生或显式)+ 静态 max
    -- + 当前并发 rt_sum(do_route 存的、timer 判压力用的那个,全 peer 含 banned 的真实在途)+ 是否到爬升压力门。
    local adaptive_cc, adaptive_cc_min, adaptive_cc_max, adaptive_cc_conc, adaptive_cc_at_pressure, adaptive_cc_at_slack, adaptive_cc_rej
    if opts.adaptive_cc then
        adaptive_cc, adaptive_cc_min, adaptive_cc_max = {}, {}, {}
        adaptive_cc_conc, adaptive_cc_at_pressure, adaptive_cc_at_slack, adaptive_cc_rej = {}, {}, {}, {}
        local ABS = opts.adaptive_cc_abs or 0
        local function fill_cc(mkey, model)
            local maxcc = route.compute_static_max_cc(opts, model)
            adaptive_cc_max[mkey] = maxcc
            -- 生效 min:derive_mincc 统一派生(与 do_route/do_adaptive_cc_loop 同 base+钳到 max),报告==强制
            adaptive_cc_min[mkey] = route.derive_mincc(opts, maxcc)
            local pfx = tps.tps_key_prefix(opts, model)            -- 统一走 helper
            local cc  = td and td:get(pfx .. "adaptive_cc") or nil
            local rts = td and td:get(pfx .. "rt_sum") or nil     -- timer 判压力用的实时并发(nil=无近期流量)
            adaptive_cc[mkey]      = cc
            adaptive_cc_conc[mkey] = rts
            adaptive_cc_rej[mkey]  = td and td:get(pfx .. "rej") or 0   -- 本区间被压抑需求(并发429数);只读不清零
            -- 下一 tick 走哪个分支(与 step_one 一致:相对系数 + 绝对头寸 ABS 双门):
            --   at_pressure = 顶到 cc×pressure(相对) 或 头寸不足 cc-conc<ABS(绝对)→ 涨(注:rej>0 会先于此直接快涨)
            --   at_slack    = conc<cc×slack(相对) 且 余量够 cc-conc>ABS(绝对)→ 缩;都 false=保持
            local c = rts or 0
            adaptive_cc_at_pressure[mkey] = (cc and ((c >= cc * opts.adaptive_cc_pressure_frac) or ((cc - c) < ABS))) and true or false
            adaptive_cc_at_slack[mkey]    = (cc and c > 0 and (c < cc * opts.adaptive_cc_slack_frac) and ((cc - c) > ABS)) and true or false
        end
        if opts.peers_by_model then
            for m in pairs(opts.peers_by_model) do fill_cc(m, m) end
        else
            fill_cc("_", false)
        end
    end
    -- ewma_tps 保持老形状(单指标时取第一条),新增 metrics 展示完整指标列表。同 /_ttft_status。
    local mstat = {}
    if td then
        local models = {}
        if opts.peers_by_model then
            for m in pairs(opts.peers_by_model) do models[#models+1] = m end
        else
            models[1] = false
        end
        for _, m in ipairs(models) do
            local key = (m == false) and "_" or m
            local pre = (m == false) and rp or (rp .. m .. ":")
            local mlist, msrc = tps.tps_metrics(opts, m)     -- msrc: override|declared|static
            local list = {}   -- 纯数组,理由同 /_ttft_status
            for _, mt in ipairs(mlist) do
                list[#list+1] = { metric = mt.metric, q = mt.q,
                                  threshold_tps = mt.threshold,
                                  ewma_tps = td:get(tps.tps_ewma_key(opts, m, mt.metric)) }
            end
            ewmas[key]   = list[1] and list[1].ewma_tps or nil
            local shadow, ignored   -- 语义同 /_ttft_status
            if msrc == "override" then
                shadow = { source = "static", threshold_tps = tps.tps_static_limit_for(opts, m) }
            elseif msrc == "declared" then
                local o = tps.tps_override_for(opts, m)
                if o then ignored = { override_tps = o, reason = "declared 指标表优先,override 不生效" } end
            end
            -- 数组形状,理由同 /_ttft_status
            mstat[#mstat+1] = { model = key, source = msrc, items = list, shadowed = shadow, ignored_override = ignored }
            nousage[key] = td:get(pre .. "nousage") or 0
        end
    end
    local active = tps.tps_dict_if_on(opts) and true or false
    local win = td and math.floor(ngx.now() / opts.tps_probe_window) or 0
    ngx.say(cjson_dbg.encode({
        route                 = opts.route_name,
        global_enabled        = _G.TPS_ENABLED and true or false,
        dict_declared         = td and true or false,
        runtime_off           = (td and td:get(rp .. "__off")) and true or false,
        -- ⚠️ opt_in 只看静态下限,**已经答不了「本特性是不是开着」**,两个原因叠加:
        --    ① 2026-08-25 起 _G.TPS_LIMIT_TPS 有了全局默认(20)→ 恒为 true;
        --    ② 闸门(tps_dict_if_on)现在认 tps_metrics → 只声明指标表时特性是开的、这里却是 false。
        --    字段保留是为了不破坏既有消费者(回归套件依赖字段集不变)。
        --    **要判特性开没开看 active;要判阈值哪来的看 tps_limit_source。**
        opt_in                = opts.tps_limit_tps and true or false,
        -- 与 tps_dict_if_on 的闸门同口径:declared(指标表)排在静态之前,与判定链一致。
        -- 不同口径会让端点自己打自己 —— active=true 而 source="none"。
        -- 顺序必须与 tps_metrics 的优先级链一致:declared > override > 静态。
        -- 漏了 override 会让同一个端点的两个字段打架 —— metrics[].source 报 "override",
        -- 而这里报 "route"/"global_default"。
        tps_limit_source      = (opts.tps_metrics and "declared")
                                or (tps.tps_override_for(opts, false) and "override")
                                or (opts.tps_limit_tps == nil and "none")
                                or (opts.tps_limit_explicit and "route" or "global_default"),
        active                = active,
        tps_limit_tps         = opts.tps_limit_tps or nil,             -- 路由级默认下限
        tps_limit_by_model    = opts.tps_limit_by_model or nil,        -- 每模型覆盖(peers_by_model)
        ewma_tps              = ewmas,
        nousage_samples       = nousage,                              -- 无 usage 漏采数(看覆盖率)
        alpha                 = opts.tps_ewma_alpha,
        ttl                   = opts.tps_ttl,
        min_tokens            = opts.tps_min_tokens,
        probe_window          = opts.tps_probe_window,
        probe_per_window      = opts.tps_probe_per_window,
        probe_used_cur_window = td and (td:get(rp .. "probe:" .. win) or 0) or 0,
        -- 自适应并发(adaptive_cc=true 时才有;与 TPS 硬熔断互斥)
        adaptive_cc_on        = opts.adaptive_cc and true or false,
        adaptive_cc           = adaptive_cc,         -- 各子池当前动态并发上限(nil=未初始化/过期,do_route 回退到 min 慢启动)
        adaptive_cc_min       = adaptive_cc_min,     -- 生效下限(显式配 or 静态max×frac 派生)
        adaptive_cc_max       = adaptive_cc_max,     -- 静态池容量(=AIMD max clamp)
        adaptive_cc_conc      = adaptive_cc_conc,    -- 当前并发 rt_sum(do_route 存,timer 判压力用,全 peer 含 banned;nil=无近期流量)
        adaptive_cc_at_pressure = adaptive_cc_at_pressure,  -- 健康时下一tick 会涨(conc>=cc×pressure_frac)
        adaptive_cc_at_slack    = adaptive_cc_at_slack,     -- 健康时下一tick 会缩(0<conc<cc×slack_frac;conc=0保持)
        adaptive_cc_pressure_frac = opts.adaptive_cc and opts.adaptive_cc_pressure_frac or nil,
        adaptive_cc_slack_frac    = opts.adaptive_cc and opts.adaptive_cc_slack_frac or nil,
        adaptive_cc_abs       = opts.adaptive_cc and opts.adaptive_cc_abs or nil,   -- 绝对头寸(slots)
        adaptive_cc_rej       = adaptive_cc_rej,     -- 本区间被压抑需求(并发429数);>0 → 下tick 快涨到 desired
        adaptive_cc_interval  = opts.adaptive_cc and opts.adaptive_cc_interval or nil,
        -- ↓ 新增(只增不改不重排)
        metrics               = mstat,                        -- 每模型指标列表 + source(override|declared|static)
        dict_capacity         = td and td:capacity() or nil,
        dict_free_space       = td and td:free_space() or nil,
    }))
end

-- GET /_429_status → 全路由 429 限流累计计数(按 route × reason 聚合)。
-- 计数在 do_route 三个硬 429 出口 incr("<route>:<reason>")；reason=concurrency/ttft/tps。
-- 计数存 reject_stat(全局共享 dict)：跨 reload 存活、进程重启清零。GET ?reset=1 手动清零(仅 127.0.0.1)。
function _G.dbg_429_status(opts)
    local rj = ngx.shared.reject_stat
    ngx.header["Content-Type"] = "application/json"
    if not rj then
        ngx.say([[{"error":"reject_stat dict not declared"}]])
        return
    end
    local args = ngx.req.get_uri_args()
    -- 默认只统计【本端口对应 route】(opts.route_name);?all=1 → 全路由聚合(monitor 走这个)。
    -- reject_stat 是全局 dict、key="<route>:<reason>",这里按 route 前缀过滤实现 per-route 视图。
    local want = opts and opts.route_name
    local show_all = (args.all == "1") or (not want)
    if args.reset == "1" then
        if ngx.var.remote_addr ~= "127.0.0.1" then
            ngx.status = 403
            ngx.say([[{"error":"reset allowed from 127.0.0.1 only"}]])
            return
        end
        if show_all then
            rj:flush_all()
        else
            for _, k in ipairs(rj:get_keys(0)) do        -- 只清本 route 的 key,不动别的路由
                local rname = k:match("^(.*):[^:]+$")    -- 不叫 route:避免遮蔽顶层 route 模块 handle
                if rname == want then rj:delete(k) end
            end
        end
        ngx.say(cjson_dbg.encode({ reset = true, scope = show_all and "all" or want }))
        return
    end
    local by_route, by_reason, total = {}, {concurrency=0, ttft=0, tps=0}, 0
    for _, k in ipairs(rj:get_keys(0)) do
        local rname, reason = k:match("^(.*):([^:]+)$")
        if rname and reason and (show_all or rname == want) then
            local v = rj:get(k) or 0
            by_route[rname] = by_route[rname] or {concurrency=0, ttft=0, tps=0}
            by_route[rname][reason] = (by_route[rname][reason] or 0) + v
            by_reason[reason]       = (by_reason[reason] or 0) + v
            total = total + v
        end
    end
    ngx.say(cjson_dbg.encode({
        scope     = show_all and "all" or want,          -- "all"=全路由聚合 / "<route>"=仅本路由
        total     = total,
        by_reason = by_reason,
        by_route  = by_route,
        note      = "默认仅本 route;?all=1 全路由聚合。counters since last reset/worker restart (survive reload)",
        worker_id = ngx.worker.id(),
    }))
end

-- POST /_tps_toggle?on=0 → 关本路由 TPS(写 tps_dict 的 "__off",免 reload);on=1 → 开。仅 127.0.0.1。
function _G.dbg_tps_toggle(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.tps_dict]
    if not td then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ route = opts.route_name, ok = false,
            error = "tps_dict not declared" }))
        return
    end
    local offk = (opts.route_name or "?") .. ":__off"
    local on = ngx.var.arg_on
    if on == "0" then td:set(offk, true)
    elseif on == "1" then td:delete(offk)
    else
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ ok = false, error = "use ?on=0 (disable) | ?on=1 (enable)" }))
        return
    end
    ngx.say(cjson_dbg.encode({
        route = opts.route_name, ok = true,
        global_enabled = _G.TPS_ENABLED and true or false,
        runtime_off    = td:get(offk) and true or false,
        active         = tps.tps_dict_if_on(opts) and true or false,
    }))
end

-- 在线热改 TPS 下限(tokens/sec,免 reload、跨 worker 一致;写 <route>[:<model>]:limit_override)。
-- GET /_tps_limit                  → 看当前 override + 静态默认
-- GET /_tps_limit?tps=80           → 设路由级下限 80 tok/s
-- GET /_tps_limit?tps=80&model=X   → 设某模型下限(仅 peers_by_model 路由)
-- GET /_tps_limit?tps=0 [&model=X] → 清除 override(回落静态默认)
function _G.dbg_tps_limit(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local td = ngx.shared[opts.tps_dict]
    if not td then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ route = opts.route_name, ok = false, error = "tps_dict not declared" }))
        return
    end
    local model = ngx.var.arg_model
    local k = (opts.route_name or "?") .. ":" .. (model and (model .. ":") or "") .. "limit_override"
    local arg_tps = ngx.var.arg_tps   -- ⚠️ 不能命名 tps:会遮蔽顶层 `local tps = require "tps"`(下方 tps.tps_dict_if_on)
    local ttl, terr = parse_override_ttl(ngx.var.arg_ttl)
    if terr then
        ngx.status = 400
        ngx.say(cjson_dbg.encode({ ok = false, error = terr }))
        return
    end
    if arg_tps then
        local n = tonumber(arg_tps)
        if not n or n < 0 then
            ngx.status = 400
            ngx.say(cjson_dbg.encode({ ok = false, error = "use ?tps=0 清除 | ?tps=<正数> 设下限 [&model=X] [&ttl=秒,默认7200,0=永久]" }))
            return
        end
        if n == 0 then td:delete(k) else td:set(k, n, ttl) end
    end
    -- ⚠️ opt-in 陷阱:TPS 是静态 opt-in(tps_dict_if_on 会先查 opts.tps_limit_tps)。若路由没在
    -- factory 配 tps_limit_tps,光设 override 不会生效(采样/判定整链短路)——显式 enforcing + warning,
    -- 避免运维设了 override 以为已保护、实际放行(与 TTFT 默认开不同,TTFT 无此陷阱)。
    local opted_in = opts.tps_limit_tps and true or false
    -- ⚠️ Lua 陷阱:不能写 `has_ovr and eff or nil` —— eff 为 false 时 `x and false or nil`
    --    恒得 nil,字段会整个消失,前端看到的是"没设 override",与事实相反。
    --    先算好再放进表里。
    local eff = tps.tps_override_effective(opts, model or false)
    local ovr_eff = nil
    if td:get(k) ~= nil then ovr_eff = eff end   -- 语义同 /_ttft_limit
    ngx.say(cjson_dbg.encode({
        route              = opts.route_name, ok = true,
        model              = model or nil,
        enforcing          = (opted_in and tps.tps_dict_if_on(opts)) and true or false,
        warning            = (not opted_in)
            and "route NOT opted in — set static tps_limit_tps in factory + reload; this override alone does nothing"
            or nil,
        limit_override_tps = td:get(k) or nil,             -- 当前 override(nil=未设,回落静态)
        static_limit_tps   = opts.tps_limit_tps or nil,    -- 路由级静态默认
        static_by_model    = opts.tps_limit_by_model or nil,
        priority           = "declared(路由声明的指标表) > override > 静态per-model > 静态route",
        -- ↓ 新增(只增不改不重排)
        override_ttl_s     = override_ttl_of(td, k),       -- 剩余秒数(0=永不过期;nil=未设 override)
        override_effective = ovr_eff,                      -- false = 设了但被声明表压住;nil = 没设
        override_ignored_reason = (td:get(k) ~= nil and not eff)
            and "route declares tps_metrics; declared table wins over manual override" or nil,
        shadowed_limit_tps = (td:get(k) ~= nil and eff)
            and tps.tps_static_limit_for(opts, model or false) or nil,
        declares_metrics   = opts.tps_metrics and true or false,
    }))
end

function _G.dbg_active_conns_set(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    local args = ngx.req.get_uri_args()
    local dict = ngx.shared[opts.active_conns_dict]
    local resp = { ok = true, route = opts.route_name }
    if args.flush == "1" then
        dict:flush_all(); dict:flush_expired()
        resp.action = "flush_all"
    elseif args.peer then
        if args.delete == "1" then
            dict:delete(args.peer); resp.action = "delete"; resp.peer = args.peer
        elseif args.value then
            local v = tonumber(args.value)
            if v == nil or v < 0 then
                ngx.status = 400; resp.ok = false; resp.error = "value must be non-negative integer"
            else
                dict:set(args.peer, v); resp.action = "set"; resp.peer = args.peer; resp.value = v
            end
        else
            ngx.status = 400; resp.ok = false; resp.error = "need value=N or delete=1"
        end
    else
        ngx.status = 400; resp.ok = false; resp.error = "need peer=<host:port>&value=N | peer=...&delete=1 | flush=1"
    end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode(resp))
end

function _G.dbg_route_debug(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    -- M1: peers={} 防御（K2.6 默认空 peers 时调试 endpoint 不崩）
    if not opts.peer_keys or #opts.peer_keys == 0 then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"no peers configured","route":"%s"}]], opts.route_name or "?"))
        return
    end
    local sid = ngx.var.arg_sid or "default"
    -- 新格式 peers_by_model：用 ?model=<id> 选子池(与 do_route 共用 resolve_pool)
    local peers, peer_keys, sup = route.resolve_pool(opts, ngx.var.arg_model)
    if not peers then
        ngx.header["Content-Type"] = "application/json"
        ngx.say(cjson_dbg.encode({ route = opts.route_name, error = "model_not_supported",
            hint = "新格式路由需加 &model=<id> 查询", model = ngx.var.arg_model, supported_models = sup }))
        return
    end
    local bad = ngx.shared[opts.bad_peers_dict]
    -- natural_target:rendezvous 在「全部 peer(含 banned)」上的理想目标(解释 hash 数学用)
    local full = {}
    for i, p in ipairs(peers) do full[i] = {p[1], p[2], i, peer_keys[i]} end
    local natural_idx, natural_h = util.pick_rendezvous(sid, full)
    local natural_banned = false
    if peers[natural_idx] then
        natural_banned = bad:get(peer_keys[natural_idx]) and true or false
    end
    -- actual_pick:走真实路由共享逻辑(assess_pool + pick_from),含饱和→least_conn、
    -- no-sid least_conn,与 do_route 完全一致(不再自己重算)。
    -- #5: 让 assess_pool 里的 ttft_ewma_key 能按 model 取对(dbg 用 ?model= 选池;
    -- 不设则 peers_by_model 路由的 EWMA 落到 "?:ewma" 显示 nil)
    ngx.ctx.req_model = ngx.var.arg_model
    local a = route.assess_pool(opts, peers, peer_keys)
    -- actual_pick 反映真实路由:亲和性关则丢 sid(natural_* 仍用原 sid 展示 hash 数学)
    local actual_sid = route.affinity_gate(opts, sid, nil)
    local actual_name, actual_mode, actual_h
    if not a.empty then
        local chosen_hp, m, h = route.pick_from(opts, a, actual_sid)
        actual_name = chosen_hp and peers[chosen_hp[3]][3] or nil
        actual_mode, actual_h = m, h
    end
    local all_scores = {}
    local prefix = sid .. "|"
    for i, p in ipairs(peers) do
        local k = peer_keys[i]
        local h = tonumber(string.sub(ngx.md5(prefix .. k), 1, 8), 16) or 0
        all_scores[i] = { idx = i, name = p[3], hash = h,
                          banned = bad:get(k) and true or false }
    end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route            = opts.route_name,
        session_id       = sid,
        algorithm        = "rendezvous",
        natural_target   = peers[natural_idx] and peers[natural_idx][3] or nil,
        natural_hash     = natural_h,
        natural_banned   = natural_banned,
        actual_pick      = actual_name,
        actual_hash      = actual_h,
        actual_mode      = actual_mode,
        -- 亲和性开关状态:关时 natural_* 仍展示理想哈希,但 actual_mode 会是 least_conn。
        -- 显式给出避免「有 natural_target 却走 least_conn」的排障困惑。
        session_affinity_enabled = opts.session_affinity_enabled and true or false,
        scores           = all_scores,
    }))
end

function _G.dbg_route_inspect(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    -- M1: peers={} 防御（K2.6 默认空 peers 时调试 endpoint 不崩）
    if not opts.peer_keys or #opts.peer_keys == 0 then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"no peers configured","route":"%s"}]], opts.route_name or "?"))
        return
    end
    ngx.ctx.route_opts = opts   -- 让 prepare_request 拿到本路由的 cch_ctl
    local sid, src = reqtransform.prepare_request(opts)
    sid, src = route.affinity_gate(opts, sid, src)   -- 与 do_route 一致:亲和性关则丢 sid
    -- 与 do_route 共用 resolve_pool + assess_pool + pick_from:dbg 看到的 pick 与真实路由一致
    local peers, peer_keys, sup = route.resolve_pool(opts, ngx.ctx.req_model)
    if not peers then
        ngx.header["Content-Type"] = "application/json"
        ngx.say(cjson_dbg.encode({ route = opts.route_name, session_id = sid, source = src or "none",
            error = "model_not_supported", model = ngx.ctx.req_model, supported_models = sup }))
        return
    end
    local a = route.assess_pool(opts, peers, peer_keys)
    if a.empty then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"all peers banned, no healthy upstream","route":"%s"}]], opts.route_name))
        return
    end
    local chosen_hp, mode, hash = route.pick_from(opts, a, sid)
    local pick = chosen_hp and peers[chosen_hp[3]][3] or nil
    ngx.header["X-Routed-Active-Level"] = tostring(a.active_level)
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route        = opts.route_name,
        session_id   = sid,
        source       = src or "none",
        mode         = mode,
        hash         = hash,
        pick         = pick,
        active_level = a.active_level,
        cch_stripped = ngx.ctx.cch_stripped or false,
    }))
end

function _G.dbg_cch_test(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    ngx.ctx.route_opts = opts
    bodylog.bodylog_capture_request(opts)
    local sid, src = reqtransform.prepare_request(opts)
    ngx.req.read_body()
    local body_after = ngx.req.get_body_data()
    if not body_after then
        local fp = ngx.req.get_body_file()
        if fp then local f=io.open(fp,"rb"); if f then body_after=f:read("*a"); f:close() end end
    end
    local bodylog_body = ngx.ctx.bodylog_req_body
    ngx.header["Content-Type"] = "application/json"
    local out = {
        route              = opts.route_name,
        cch_stripped       = ngx.ctx.cch_stripped or false,
        session_id         = sid,
        source             = src or "none",
        body_len           = body_after and #body_after or 0,
        body_md5           = body_after and ngx.md5(body_after) or "",
        bodylog_len        = bodylog_body and #bodylog_body or 0,
        bodylog_md5        = bodylog_body and ngx.md5(bodylog_body) or "",
        bodylog_differs    = (bodylog_body ~= body_after),
    }
    -- ?full=1 回显处理后的完整 body(仅调试:byte-preservation 精确断言用;默认不回显防泄露)
    if ngx.var.arg_full == "1" then
        out.body_after = body_after or ""
    end
    ngx.say(cjson_dbg.encode(out))
end

function _G.dbg_route_state(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    -- M1: peers={} 防御（K2.6 默认空 peers 时调试 endpoint 不崩）
    if not opts.peer_keys or #opts.peer_keys == 0 then
        ngx.status = 503
        ngx.header["Content-Type"] = "application/json"
        ngx.say(string.format([[{"error":"no peers configured","route":"%s"}]], opts.route_name or "?"))
        return
    end
    local peers = opts.peers
    local bad = ngx.shared[opts.bad_peers_dict]
    local dict = ngx.shared[opts.active_conns_dict]
    local default_max = opts.default_max
    local by_priority = {}
    local active_level = -math.huge
    for i, p in ipairs(peers) do
        local k = opts.peer_keys[i]
        local prio = tonumber(p[4]) or 0
        local pmax = tonumber(p[5]) or default_max
        local banned = bad:get(k) and true or false
        local active = dict:get(k) or 0
        local bk = tostring(prio)
        if not by_priority[bk] then
            by_priority[bk] = {priority = prio, healthy = 0, banned = 0,
                               active = 0, max = 0, peers = {}}
        end
        local b = by_priority[bk]
        b.peers[#b.peers + 1] = {name = p[3], peer = k, banned = banned,
                                  active = active, max = pmax}
        if banned then
            b.banned = b.banned + 1
        else
            b.healthy = b.healthy + 1
            b.active  = b.active + active
            b.max     = b.max + pmax
            if prio > active_level then active_level = prio end
        end
    end
    if active_level == -math.huge then active_level = 0 end
    local active_bucket = by_priority[tostring(active_level)] or {healthy = 0, max = 0}
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route                  = opts.route_name,
        active_level           = active_level,
        limit                  = active_bucket.max,
        healthy_peers_in_level = active_bucket.healthy,
        by_priority            = by_priority,
    }))
end

function _G.dbg_bodylog_status(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    local logger = require "resty.logger.socket"
    local ctl = ngx.shared[opts.bodylog_ctl_dict]
    local default_en = opts.bodylog_default_enabled
    if default_en == nil then default_en = _G.BODYLOG_DEFAULT_ENABLED end
    local default_pct = opts.bodylog_default_pct or _G.BODYLOG_DEFAULT_PCT
    local en = ctl:get("enabled")
    if en == nil then en = default_en and 1 or 0 end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route          = opts.route_name,
        enabled        = en == 1,
        sample_pct     = ctl:get("sample_pct") or default_pct,
        worker_id      = ngx.worker.id(),
        worker_pid     = ngx.worker.pid(),
        logger_initted = logger.initted(),
        write_count    = ctl:get("write_count") or 0,
        drop_count     = ctl:get("drop_count") or 0,
        encode_errs    = ctl:get("encode_errs") or 0,
        last_log_ts    = ctl:get("last_log_ts"),
        now            = ngx.now(),
    }))
end

function _G.dbg_bodylog_toggle(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    local args = ngx.req.get_uri_args()
    local ctl = ngx.shared[opts.bodylog_ctl_dict]
    local default_en = opts.bodylog_default_enabled
    if default_en == nil then default_en = _G.BODYLOG_DEFAULT_ENABLED end
    local default_pct = opts.bodylog_default_pct or _G.BODYLOG_DEFAULT_PCT
    local changes = {}
    if args.on ~= nil then
        local v
        if args.on == "1" or args.on == "true" then v = 1
        elseif args.on == "0" or args.on == "false" then v = 0 end
        if v ~= nil then ctl:set("enabled", v); changes.enabled = (v == 1) end
    end
    if args.pct ~= nil then
        local p = tonumber(args.pct)
        if p and p >= 0 and p <= 100 then ctl:set("sample_pct", p); changes.sample_pct = p end
    end
    local en2 = ctl:get("enabled")
    if en2 == nil then en2 = default_en and 1 or 0 end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route      = opts.route_name,
        ok         = true,
        changes    = changes,
        enabled    = en2 == 1,
        sample_pct = ctl:get("sample_pct") or default_pct,
    }))
end

function _G.dbg_cch_strip_status(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    local ctl = ngx.shared[opts.cch_ctl_dict]
    local default_en = opts.cch_default_enabled
    if default_en == nil then default_en = _G.CCH_STRIP_DEFAULT_ENABLED end
    local en = ctl:get("enabled")
    if en == nil then en = default_en and 1 or 0 end
    local parsed = ctl:get("parsed_total") or 0
    local stripped = ctl:get("stripped_total") or 0
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route          = opts.route_name,
        enabled        = en == 1,
        default        = default_en,
        parsed_total   = parsed,
        stripped_total = stripped,
        strip_ratio    = (parsed > 0) and (stripped / parsed) or 0,
        worker_id      = ngx.worker.id(),
    }))
end

function _G.dbg_cch_strip_toggle(opts)
    -- H1: nil opts 防御
    if util.opts_missing(opts) then return end
    local args = ngx.req.get_uri_args()
    local ctl = ngx.shared[opts.cch_ctl_dict]
    local default_en = opts.cch_default_enabled
    if default_en == nil then default_en = _G.CCH_STRIP_DEFAULT_ENABLED end
    local changes = {}
    if args.on ~= nil then
        local v
        if     args.on == "1" or args.on == "true"  then v = 1
        elseif args.on == "0" or args.on == "false" then v = 0 end
        if v ~= nil then ctl:set("enabled", v); changes.enabled = (v == 1) end
    end
    if args.reset == "1" then
        ctl:set("parsed_total", 0); ctl:set("stripped_total", 0); changes.counters_reset = true
    end
    local en2 = ctl:get("enabled")
    if en2 == nil then en2 = default_en and 1 or 0 end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route   = opts.route_name,
        ok      = true,
        changes = changes,
        enabled = en2 == 1,
    }))
end

-- /_kimi_normalize_toggle?on=0|1[&reset=1]
-- 切换 Kimi tool_call_id 规范化开关 + 重置计数器。
-- 状态写入 ngx.shared[opts.cch_ctl_dict] 复用同一 dict，key=kimi_normalize_enabled。
function _G.dbg_kimi_normalize_toggle(opts)
    if util.opts_missing(opts) then return end
    local args = ngx.req.get_uri_args()
    local ctl = ngx.shared[opts.cch_ctl_dict]
    -- 默认态取 per-route（factory 可配 false 永久关），与 prepare_request 一致;缺省回落全局。
    -- 否则 dict 未设时 status 会报全局值,与实际 per-route 门控相反(误导运维)。
    local default_en = opts.kimi_normalize_default_enabled
    if default_en == nil then default_en = _G.KIMI_NORMALIZE_DEFAULT_ENABLED end
    local changes = {}
    if args.on ~= nil then
        local v
        if     args.on == "1" or args.on == "true"  then v = 1
        elseif args.on == "0" or args.on == "false" then v = 0 end
        if v ~= nil then
            ctl:set("kimi_normalize_enabled", v)
            changes.enabled = (v == 1)
        end
    end
    if args.reset == "1" then
        ctl:set("kimi_normalize_changes_total", 0)
        ctl:set("kimi_normalize_req_count", 0)
        changes.counters_reset = true
    end
    local en2 = ctl:get("kimi_normalize_enabled")
    if en2 == nil then en2 = default_en and 1 or 0 end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route   = opts.route_name,
        ok      = true,
        changes = changes,
        enabled = en2 == 1,
        changes_total = ctl:get("kimi_normalize_changes_total") or 0,
        req_count     = ctl:get("kimi_normalize_req_count") or 0,
    }))
end

-- include_usage 注入开关(POST ?on=1 / on=0 / 不带参数查询)。默认**开**(dict 缺省即开):
-- 仅对 peers_by_model 路由的 stream 请求生效,补 stream_options.include_usage=true。
-- 承重转换(关了新格式 stream 拿不到 usage),故无 per-route factory 默认位、只有全局默认开 + 紧急关。
function _G.dbg_include_usage_toggle(opts)
    if util.opts_missing(opts) then return end
    local args = ngx.req.get_uri_args()
    local ctl = ngx.shared[opts.cch_ctl_dict]
    local changes = {}
    if args.on ~= nil then
        local v
        if     args.on == "1" or args.on == "true"  then v = 1
        elseif args.on == "0" or args.on == "false" then v = 0 end
        if v ~= nil then
            ctl:set("include_usage_enabled", v)
            changes.enabled = (v == 1)
        end
    end
    if args.reset == "1" then
        ctl:set("include_usage_injected_total", 0)
        changes.counters_reset = true
    end
    local en2 = ctl:get("include_usage_enabled")
    if en2 == nil then en2 = 1 end   -- 默认开
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route          = opts.route_name,
        ok             = true,
        changes        = changes,
        enabled        = en2 == 1,
        injected_total = ctl:get("include_usage_injected_total") or 0,
        note           = "仅 peers_by_model 路由的 stream 请求生效;默认开",
    }))
end

-- ── reject_rules(规则化请求拒绝)status / toggle ──────────────────────────
-- 规则本体配在各路由 factory 的 opts.reject_rules(见 lua/reject_rules.lua)。

-- 把一个规则节点(叶子 / all / any)渲染成可读字符串,供 status 展示。
local function _reject_rule_desc(node)
    if type(node) ~= "table" then return "?" end
    if node.all or node.any then
        local subs, sep = node.all or node.any, node.all and " & " or " | "
        local parts = {}
        for _, s in ipairs(subs) do parts[#parts + 1] = _reject_rule_desc(s) end
        return (node.all and "ALL(" or "ANY(") .. table.concat(parts, sep) .. ")"
    end
    local v = node.value
    if type(v) == "table" then
        local xs = {}
        for _, x in ipairs(v) do xs[#xs + 1] = tostring(x) end
        v = "[" .. table.concat(xs, ",") .. "]"
    end
    local vs = (node.op == "exists" or node.op == "absent") and "" or (" " .. tostring(v))
    return tostring(node.field) .. " " .. tostring(node.op) .. vs
end

-- GET /_reject_rules_status → 列出本路由已配规则 + 当前 enabled 态 + 各规则命中数。
function _G.dbg_reject_rules_status(opts)
    if util.opts_missing(opts) then return end
    ngx.header["Content-Type"] = "application/json"
    local ctl = ngx.shared[opts.cch_ctl_dict]
    local default_en = opts.reject_rules_default_enabled
    local enabled = default_en
    if ctl then
        local raw = ctl:get("reject_rules_enabled")
        if raw ~= nil then enabled = (raw == 1) end
    end
    local rules = {}
    if opts.reject_rules then
        for _, r in ipairs(opts.reject_rules) do
            rules[#rules + 1] = {
                name   = r.name,
                status = r.status or opts.reject_rules_status,
                cond   = _reject_rule_desc(r),
                hits   = ctl and (ctl:get("reject_rule_hit:" .. (r.name or "?")) or 0) or 0,
            }
        end
    end
    ngx.say(cjson_dbg.encode({
        route          = opts.route_name,
        ok             = true,
        enabled        = enabled,
        default_status = opts.reject_rules_status,
        rule_count     = #rules,
        rules          = rules,
    }))
end

-- POST /_reject_rules_toggle?on=0|1[&reset=1] → 热切本路由规则总开关 / 重置命中计数。
-- 状态写 ngx.shared[opts.cch_ctl_dict] key "reject_rules_enabled"(复用 cch_ctl,跨 reload 持久)。
function _G.dbg_reject_rules_toggle(opts)
    if util.opts_missing(opts) then return end
    local args = ngx.req.get_uri_args()
    local ctl = ngx.shared[opts.cch_ctl_dict]
    local default_en = opts.reject_rules_default_enabled
    local changes = {}
    if ctl and args.on ~= nil then
        local v
        if     args.on == "1" or args.on == "true"  then v = 1
        elseif args.on == "0" or args.on == "false" then v = 0 end
        if v ~= nil then ctl:set("reject_rules_enabled", v); changes.enabled = (v == 1) end
    end
    if ctl and args.reset == "1" then
        for _, k in ipairs(ctl:get_keys(0)) do
            if k:find("^reject_rule_hit:") then ctl:delete(k) end
        end
        changes.counters_reset = true
    end
    local en2 = ctl and ctl:get("reject_rules_enabled")
    if en2 == nil then en2 = default_en and 1 or 0 end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson_dbg.encode({
        route   = opts.route_name,
        ok      = true,
        changes = changes,
        enabled = en2 == 1,
    }))
end
