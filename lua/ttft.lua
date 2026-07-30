-- openresty/lua/ttft.lua
-- TTFT 限流(池级 EWMA + 窗口 P80 + 半开探测)
-- 跨模块函数(dict_if_on/ewma_key/limit_for/record/allow_probe)挂 M;key_prefix/bucket_index/
-- window_p80 仅内部用 → local。TTFT_ENABLED / TTFT_BUCKETS_MS 是 config data 仍留 _G。

local M = {}

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
local function ttft_key_prefix(opts)
    local p = (opts.route_name or "?") .. ":"
    if opts.peers_by_model then
        p = p .. (ngx.ctx.req_model or "?") .. ":"
    end
    return p
end

-- EWMA key:新格式 "<model>:ewma",老格式 "ewma"(do_log_release / assess_pool / dbg 共用)。
function M.ttft_ewma_key(opts)
    return ttft_key_prefix(opts) .. "ewma"
end

-- 解析本请求该用的 TTFT 阈值(ms):优先级
--   ① peers_by_model 路由 + 配了 ttft_limit_by_model[<model>] → 该模型专属阈值
--   ② opts.ttft_limit_ms(路由级默认;register_route 里已 fallback 到全局 _G.TTFT_LIMIT_MS)
-- 这样同一 peers_by_model 路由内每个模型可配不同阈值(机型/模型基线不同)。
function M.ttft_limit_for(opts)
    -- 在线 override(共享字典,跨 worker 一致,优先级最高;免 reload,见 /_ttft_limit)
    --   peers_by_model:先查 <route>:<model>:limit_override,再查 <route>:limit_override(整路由)
    local td = ngx.shared[opts.ttft_dict]
    if td then
        if opts.peers_by_model then
            local mo = td:get((opts.route_name or "?") .. ":" .. (ngx.ctx.req_model or "") .. ":limit_override")
            if mo then return mo end
        end
        local ro = td:get((opts.route_name or "?") .. ":limit_override")
        if ro then return ro end
    end
    local bym = opts.ttft_limit_by_model
    if bym and opts.peers_by_model then
        local v = bym[ngx.ctx.req_model or ""]
        if v then return v end
    end
    return opts.ttft_limit_ms
end

-- 样本 → 直方图桶号(1..#buckets+1,最后一个是 overflow)
local function ttft_bucket_index(ms)
    local b = _G.TTFT_BUCKETS_MS
    for i = 1, #b do if ms <= b[i] then return i end end
    return #b + 1
end

-- 从窗口 w 的直方图算 P80(累计越过 80% 的桶上界);空窗返 nil。
local function ttft_window_p80(td, pre, w)
    local b = _G.TTFT_BUCKETS_MS
    local n = #b
    local counts, total = {}, 0
    for i = 1, n + 1 do
        local c = td:get(pre .. "h:" .. w .. ":" .. i) or 0
        counts[i] = c; total = total + c
    end
    if total == 0 then return nil end
    local target, cum = total * 0.8, 0
    for i = 1, n + 1 do
        cum = cum + counts[i]
        if cum >= target then return b[i] or (b[n] * 2) end   -- overflow 桶代表值 = 末桶×2
    end
    return b[n] * 2
end

-- TTFT 样本入账:每 ttft_window 秒一个窗口,窗口内只对预定义桶 incr(原子,无锁);
-- 跨入新窗口时 lazy 折叠已完成窗口——取该窗口 P80 折进 EWMA(α·P80 + (1-α)·旧)。
-- dict:add 原子占位保证每窗口只折一次(免锁);长空档直接跳过(那段无流量,ewma 靠 TTL 过期)。
function M.ttft_record(opts, td, sample_ms)
    local W   = opts.ttft_window
    local win = math.floor(ngx.now() / W)
    local pre = ttft_key_prefix(opts)
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
        for w = last, win - 1 do
            if td:add(pre .. "fd:" .. w, 1, ttl) then -- 占位:本 worker 折窗口 w(锁活满折叠地平线=ttl)
                local p80 = ttft_window_p80(td, pre, w)
                if p80 then
                    local old = td:get(pre .. "ewma")
                    local a   = opts.ttft_ewma_alpha
                    td:set(pre .. "ewma", old and (a * p80 + (1 - a) * old) or p80, ttl)
                end
                for b = 1, #_G.TTFT_BUCKETS_MS + 1 do td:delete(pre .. "h:" .. w .. ":" .. b) end
            end
        end
        td:set(lastk, win, ttl)
    end
    -- 第 4 参 init_ttl=ttl:给直方图桶设过期,ewin 空档过期/竞态漏折时孤儿桶自愈,不在共享字典里堆积
    td:incr(pre .. "h:" .. win .. ":" .. ttft_bucket_index(sample_ms), 1, 0, ttl)
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
    local key = ttft_key_prefix(opts) .. "probe:" .. win
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
