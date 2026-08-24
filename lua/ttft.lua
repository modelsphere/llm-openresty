-- openresty/lua/ttft.lua
-- TTFT 限流(池级 EWMA + 窗口 P80 + 半开探测)
-- 跨模块函数(dict_if_on/ewma_key/limit_for/record/allow_probe)挂 M;key_prefix/bucket_index/
-- window_p80 仅内部用 → local。TTFT_ENABLED / TTFT_BUCKETS_MS 是 config data 仍留 _G。

local M = {}

local util = require "util"
local slo  = require "slo"   -- CRD 下发的 SLO(未接/裸机时恒空 → 全部回落静态,行为不变)

-- ══════════════════════════════════════════════════════════════════════
-- TTFT 限流辅助：EWMA key 派生 + 半开探测令牌窗口
-- ══════════════════════════════════════════════════════════════════════
-- TTFT 总开关检查:返回可用的 ttft dict handle,或 nil(=feature 对本路由关闭)。
-- nil 时所有调用方短路 → 退回旧逻辑。关闭条件(任一):
--   ① 全局 _G.TTFT_ENABLED=false(config 级总开关)
--   ② 路由没声明 ttft_stat dict(未 opt-in)
--   ③ 运行时热关:ttft_dict 里 "__off" 被 /_ttft_toggle 置上(免 reload)

function M.ttft_dict_if_on(opts)
    if not _G.TTFT_ENABLED then return nil end
    local td = ngx.shared[opts.ttft_dict]
    if not td then return nil end
    if td:get((opts.route_name or "?") .. ":__off") then return nil end   -- 每路由热关(route 维度,共享 dict)
    return td
end

-- key 前缀:"<route>:" +(peers_by_model 时再加 "<model>:")。所有路由共用一份 ttft_stat dict,
-- 故必须带 route 前缀防跨路由串;peers_by_model 再按 model 子池隔离。EWMA / 探测计数共用此前缀。
-- model 显式传(timer 逐 model,无 ngx.ctx)优先;不传(请求路径)回落 ngx.ctx.req_model。
-- 唯一构造点 —— 与 tps.tps_key_prefix 对齐,导出供 timers 复用(以前 timers 手搓前缀,必然与真实 schema 分叉)。
function M.ttft_key_prefix(opts, model)
    local p = (opts.route_name or "?") .. ":"
    if opts.peers_by_model then
        local m = model
        if m == nil then m = ngx.ctx.req_model end
        p = p .. (m or "?") .. ":"
    end
    return p
end

-- EWMA key:`<route>[:<model>]:<range>:<metric>:ewma`。
--   <range>  恒为 "all"(ranges 未实现,预留段 —— 将来启用只换这一段,不动 key 布局);
--   <metric> 是 CRD 的 type 字面量("p80"/"p50"/"avg"),**按语义命名而非数组下标** ——
--            CRD 里 metrics[] 重排或中间插一项时,下标方案会让 p50 的历史 EWMA 被当成 p95 用满一个 TTL;
--            语义命名下 metric 一变 key 就变,旧值天然隔离、靠 TTL 自然过期。
function M.ttft_ewma_key(opts, model, metric)
    return M.ttft_key_prefix(opts, model) .. "all:" .. (metric or "p80") .. ":ewma"
end

-- 解析本请求该用的 TTFT 阈值(ms):优先级
--   ① peers_by_model 路由 + 配了 ttft_limit_by_model[<model>] → 该模型专属阈值
--   ② opts.ttft_limit_ms(路由级默认;register_route 里已 fallback 到全局 _G.TTFT_LIMIT_MS)
-- 这样同一 peers_by_model 路由内每个模型可配不同阈值(机型/模型基线不同)。
-- model 同 ttft_key_prefix:显式传优先(timer 无 ngx.ctx),不传回落 ngx.ctx.req_model。
-- 在线 override(共享字典,跨 worker 一致;免 reload,见 /_ttft_limit)。
--   peers_by_model:先查 <route>:<model>:limit_override,再查 <route>:limit_override(整路由)。
-- 单独抽出来是因为它在优先级链里**压过 CRD** —— 线上出事时能立刻手工压住,不用等 operator。
function M.ttft_override_for(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local td = ngx.shared[opts.ttft_dict]
    if not td then return nil end
    if opts.peers_by_model then
        local mo = td:get((opts.route_name or "?") .. ":" .. (m or "") .. ":limit_override")
        if mo then return mo end
    end
    return td:get((opts.route_name or "?") .. ":limit_override")
end

function M.ttft_limit_for(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local ovr = M.ttft_override_for(opts, m)
    if ovr then return ovr end
    local bym = opts.ttft_limit_by_model
    if bym and opts.peers_by_model then
        local v = bym[m or ""]
        if v then return v end
    end
    return opts.ttft_limit_ms
end

-- 本路由/模型生效的**指标列表**:{ {metric=, q=, threshold=}, ... };第二返回值是来源(供 dbg 标注)。
-- 优先级链:**手工 override > CRD 下发 > factory opts > _G 全局默认**。
--   * override 压过 CRD:留应急口子,线上出事不用等 operator;
--   * CRD 未覆盖本 route(或裸机根本没有 CRD)→ 回落静态,**与接 CRD 前逐字节一致**。
-- ⚠️ 判定必须遍历**这张声明表**,而不是遍历 dict 里现存的 EWMA key ——
--    否则删掉某个指标后,它残留的 EWMA 还会继续拒人最多 ttft_ttl 秒。
-- threshold 可能是 nil(路由没配阈值):此时**照样记 EWMA**(dbg 可见、AIMD 可读),只是不参与判定
-- —— 与改动前「ttft_record 不看有没有阈值」的行为一致。
function M.ttft_metrics(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local ovr = M.ttft_override_for(opts, m)
    if ovr then return { { metric = "p80", q = 0.8, threshold = ovr } }, "override" end
    local ms = slo.metrics_for(opts.route_name, m, "ttft")
    if ms then return ms, "crd" end
    return { { metric = "p80", q = 0.8, threshold = M.ttft_limit_for(opts, m) } }, "static"
end

-- 样本 → 直方图桶号(1..#buckets+1,最后一个是 overflow)
local function ttft_bucket_index(ms)
    local b = _G.TTFT_BUCKETS_MS
    for i = 1, #b do if ms <= b[i] then return i end end
    return #b + 1
end

-- 窗口统计量已抽到 util.window_stat(TTFT/TPS 共用),分位系数由 ttft_metrics 给。
-- ⚠️ _G.TTFT_BUCKETS_MS 在**调用点现读**,不快照进 opts —— 保持「改 _G 免 reload 立即生效」。

-- TTFT 样本入账:每 ttft_window 秒一个窗口,窗口内只对预定义桶 incr(原子,无锁);
-- 跨入新窗口时 lazy 折叠已完成窗口——取该窗口 P80 折进 EWMA(α·P80 + (1-α)·旧)。
-- dict:add 原子占位保证每窗口只折一次(免锁);长空档直接跳过(那段无流量,ewma 靠 TTL 过期)。
function M.ttft_record(opts, td, sample_ms)
    local W   = opts.ttft_window
    local win = math.floor(ngx.now() / W)
    local pre = M.ttft_key_prefix(opts)
    local ttl = opts.ttft_ttl
    local lastk = pre .. "ewin"
    local last = td:get(lastk)
    if not last then
        td:set(lastk, win, ttl)
    elseif win > last then
        -- 折叠 [last, win-1]:多 worker 下读到最老 last 的 worker 会把中间窗口一并折掉(每窗口 fd 锁
        -- 去重);单 worker 下退化为只折 last。钳制阈值跟 ttl 对齐(ceil(ttl/W),ttl=60/window=20 → 3)
        -- ——既防时钟跳变下的长循环,又不会丢弃仍在 ewin 生命期(≤ttl)内的数据窗口。比 horizon 更老的
        -- 窗口数据本就过期,直接跳过(ewma 靠 TTL 自然衰减)。
        local horizon = math.ceil(ttl / W)
        if win - last > horizon then last = win - horizon end
        local ms = M.ttft_metrics(opts)
        for w = last, win - 1 do
            if td:add(pre .. "fd:" .. w, 1, ttl) then -- 占位:本 worker 折窗口 w(锁活满折叠地平线=ttl)
                local a = opts.ttft_ewma_alpha
                -- 直方图**只有一份**:N 个指标从同一份桶各算各的,分别折进各自的 EWMA。热路径开销≈0。
                for _, mt in ipairs(ms) do
                    local v = util.window_stat(td, pre, w, _G.TTFT_BUCKETS_MS, mt.metric, mt.q)
                    if v then
                        local k   = M.ttft_ewma_key(opts, nil, mt.metric)
                        local old = td:get(k)
                        td:set(k, old and (a * v + (1 - a) * old) or v, ttl)
                    end
                end
                for b = 1, #_G.TTFT_BUCKETS_MS + 1 do td:delete(pre .. "h:" .. w .. ":" .. b) end
                td:delete(pre .. "h:" .. w .. ":sum")   -- avg 的累计器,与桶同期清理(防孤儿泄漏)
                td:delete(pre .. "h:" .. w .. ":cnt")
            end
        end
        td:set(lastk, win, ttl)
    end
    -- 第 4 参 init_ttl=ttl:给直方图桶设过期,ewin 空档过期/竞态漏折时孤儿桶自愈,不在共享字典里堆积
    td:incr(pre .. "h:" .. win .. ":" .. ttft_bucket_index(sample_ms), 1, 0, ttl)
    -- avg 指标用:sum/cnt 与桶同 TTL(同样带 init_ttl,否则孤儿 key 泄漏)
    td:incr(pre .. "h:" .. win .. ":sum", sample_ms, 0, ttl)
    td:incr(pre .. "h:" .. win .. ":cnt", 1, 0, ttl)
end

-- ── 违约判定(access 的 TTFT-429 与 timers 的 AIMD 共用同一入口)────────────────
-- 遍历**声明的**指标列表,任一 EWMA 超阈值即违约(OR)。返回:
--   { ewma = <第一条指标的 EWMA,供展示/back-compat>,
--     hit = bool, hit_ewma / hit_limit / hit_metric = 触发那一条的值(供 429 响应体 + dbg) }
-- 单指标(P0 默认)时 ewma == hit_ewma、hit_limit == 旧的 ttft_limit → 429 响应体逐字节不变。
-- ⚠️ strict 保留一个**既有的不对称**,不要"顺手统一":
--     access.lua 的 TTFT-429 用 `>=`(strict=false,默认);timers.lua 的 ttft_overloaded 用 `>`(strict=true)。
--     两者本来就不同(EWMA 恰等于阈值时:access 拒、AIMD 不收),统一会改变边界行为。
--     tps_assess 有完全对称的一处(access `<=` vs AIMD `<`)。
function M.ttft_assess(opts, td, model, strict)
    local ms = M.ttft_metrics(opts, model)
    local first
    for i, mt in ipairs(ms) do
        local ew = td:get(M.ttft_ewma_key(opts, model, mt.metric))
        if i == 1 then first = ew end
        if ew and mt.threshold
           and ((strict and ew > mt.threshold) or ((not strict) and ew >= mt.threshold)) then
            return { ewma = first, hit = true,
                     hit_ewma = ew, hit_limit = mt.threshold, hit_metric = mt.metric }
        end
    end
    return { ewma = first, hit = false }
end

-- 半开探测(circuit-breaker half-open)：限流态下不 100% 拒,每个时间窗口放行
-- 至多 ttft_probe_per_window 个探测请求打到后端,持续产生新 TTFT 样本喂 EWMA。
-- 否则全拒 → 无新样本 → EWMA 永远停在高位 → 永久限流(死锁)。
-- 用 floor(now/window) 当窗口编号,key 名随时间自动滚动,无需重置定时器;
-- incr 原子计数,无锁。返回 true=放行探测,false=该 429。
-- 配额 key 带 per-model 前缀:peers_by_model 路由下每个模型各有独立 N/窗口,
-- 多模型同时过载时互不抢探测名额(与 EWMA 同样按子池隔离)。
function M.ttft_allow_probe(opts)
    local td = ngx.shared[opts.ttft_dict]
    if not td then return true end   -- dict 没声明(不该到这),稳妥放行
    local win = math.floor(ngx.now() / opts.ttft_probe_window)
    local key = M.ttft_key_prefix(opts) .. "probe:" .. win
    local n = td:incr(key, 1, 0)
    -- dict 满/OOM 时 incr 返 nil：fail-open(放行)而不是 `nil <= N` 崩成 500。
    -- dict 满本身是病态(EWMA set 也会失败→TTL 过期→自动退出限流态),放行无害。
    if not n then return true end
    if n == 1 then td:expire(key, opts.ttft_probe_window * 2) end  -- 旧窗口 key 自动回收
    return n <= opts.ttft_probe_per_window
end

-- TTFT 硬 429 限流的**独立**开关(与 EWMA 测量 / cc 收缩解耦):返回 true = 本路由关掉 TTFT-429,
-- 但 EWMA 照记(do_log_release)、assess_pool 照读、ttft_overloaded 照收 cc(软控保留),只是不再硬拒新请求。
-- 用于「只要软控(TTFT 拉低并发上限)、不要硬拒」的场景。优先级:
--   ① 运行时热关 <route>:__429off(/_ttft_429_toggle?on=0 置上,跨 reload 持久)
--   ② factory opts.ttft_429_default_enabled == false(永久关)
-- 缺省(无 override 且 factory 未显式关)= 429 开启,行为与今天完全一致(向后兼容)。
-- ⚠️ 运行时 override 是**三态**:__429off = 1(关)/ 0(开,压过 factory-off)/ nil(不 override,看 factory)。
-- 用 0 而非删 key 表示「运行时开」,否则 factory ttft_429_default_enabled=false 的路由 on=1 删 key 后仍回落 factory-false、恢复不了。
function M.ttft_429_disabled(opts)
    local td = ngx.shared[opts.ttft_dict]
    local ovr = td and td:get((opts.route_name or "?") .. ":__429off")
    if ovr ~= nil then return ovr == 1 end                          -- 运行时 override 优先(1=关/0=开)
    if opts.ttft_429_default_enabled == false then return true end  -- 无 override → 看 factory 默认
    return false
end

return M
