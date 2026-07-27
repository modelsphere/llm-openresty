-- openresty/lua/tps.lua
-- TPS 解码速率限流(池级 EWMA + 窗口 P20 + 半开探测)
-- 跨模块函数挂 M(dict_if_on/key_prefix/ewma_key/limit_for/record/allow_probe);
-- bucket_index/window_p20 仅内部用 → local。TPS_ENABLED / TPS_BUCKETS 是 config data 仍留 _G。

local M = {}

-- ══════════════════════════════════════════════════════════════════════
-- TPS 限流(解码速率)辅助:与 TTFT 同构,差三处——P20 低尾 / ewma<=下限 / opt-in。
-- 独立 tps_stat dict,key 同样带 <route>[:<model>] 前缀,与 ttft_stat 互不干扰。
-- ══════════════════════════════════════════════════════════════════════
-- 总开关 + opt-in 检查:返回可用 tps dict handle,或 nil(=特性对本路由关 → 调用方短路)。
-- 关闭条件(任一):① _G.TPS_ENABLED=false;② 路由没配 tps_limit_tps(opt-in 闸门);
--   ③ tps_dict 未声明;④ 运行时热关 <route>:__off(/_tps_toggle 置上)。

function M.tps_dict_if_on(opts)
    if not _G.TPS_ENABLED then return nil end
    if not opts.tps_limit_tps then return nil end   -- opt-in:没配下限 = 整特性对本路由关
    local td = ngx.shared[opts.tps_dict]
    if not td then return nil end
    if td:get((opts.route_name or "?") .. ":__off") then return nil end
    return td
end

-- key 前缀:"<route>:" +(peers_by_model 时再加 "<model>:")。同 TTFT。
-- model 显式传(timer 逐 model,无 ngx.ctx)优先;不传(请求路径)回落 ngx.ctx.req_model。
-- 唯一构造点 —— do_route/assess_pool/step_one/fill_cc 都走它,避免手搓前缀分叉。
function M.tps_key_prefix(opts, model)
    local p = (opts.route_name or "?") .. ":"
    if opts.peers_by_model then
        local m = model
        if m == nil then m = ngx.ctx.req_model end
        p = p .. (m or "?") .. ":"
    end
    return p
end

function M.tps_ewma_key(opts)
    return M.tps_key_prefix(opts) .. "ewma"
end

-- 解析本请求该用的 TPS 下限(tokens/sec):override > tps_limit_by_model[model] > opts.tps_limit_tps。
function M.tps_limit_for(opts)
    local td = ngx.shared[opts.tps_dict]
    if td then
        if opts.peers_by_model then
            local mo = td:get((opts.route_name or "?") .. ":" .. (ngx.ctx.req_model or "") .. ":limit_override")
            if mo then return mo end
        end
        local ro = td:get((opts.route_name or "?") .. ":limit_override")
        if ro then return ro end
    end
    local bym = opts.tps_limit_by_model
    if bym and opts.peers_by_model then
        local v = bym[ngx.ctx.req_model or ""]
        if v then return v end
    end
    return opts.tps_limit_tps
end

-- 样本(tokens/sec) → 直方图桶号(1..#buckets+1,最后一个是 overflow)。同 ttft_bucket_index。
local function tps_bucket_index(tps)
    local b = _G.TPS_BUCKETS
    for i = 1, #b do if tps <= b[i] then return i end end
    return #b + 1
end

-- 从窗口 w 的直方图算 P20(累计越过 20% 的桶上界,低尾代表值);空窗返 nil。
-- 与 ttft_window_p80 唯一区别:target=total*0.2(抓慢解码的低尾,而非 TTFT 的高尾)。
local function tps_window_p20(td, pre, w)
    local b = _G.TPS_BUCKETS
    local n = #b
    local counts, total = {}, 0
    for i = 1, n + 1 do
        local c = td:get(pre .. "h:" .. w .. ":" .. i) or 0
        counts[i] = c; total = total + c
    end
    if total == 0 then return nil end
    local target, cum = total * 0.2, 0
    for i = 1, n + 1 do
        cum = cum + counts[i]
        if cum >= target then return b[i] or (b[n] * 2) end
    end
    return b[n] * 2
end

-- TPS 样本入账:与 ttft_record 同构(每窗口直方图 → lazy 折叠 P20 折进 EWMA;fd 锁去重;TTL 自愈)。
function M.tps_record(opts, td, sample_tps)
    local W   = opts.tps_window
    local win = math.floor(ngx.now() / W)
    local pre = M.tps_key_prefix(opts)
    local ttl = opts.tps_ttl
    local lastk = pre .. "ewin"
    local last = td:get(lastk)
    if not last then
        td:set(lastk, win, ttl)
    elseif win > last then
        local horizon = math.ceil(ttl / W)
        if win - last > horizon then last = win - horizon end
        for w = last, win - 1 do
            if td:add(pre .. "fd:" .. w, 1, ttl) then
                local p20 = tps_window_p20(td, pre, w)
                if p20 then
                    local old = td:get(pre .. "ewma")
                    local a   = opts.tps_ewma_alpha
                    td:set(pre .. "ewma", old and (a * p20 + (1 - a) * old) or p20, ttl)
                end
                for b = 1, #_G.TPS_BUCKETS + 1 do td:delete(pre .. "h:" .. w .. ":" .. b) end
            end
        end
        td:set(lastk, win, ttl)
    end
    td:incr(pre .. "h:" .. win .. ":" .. tps_bucket_index(sample_tps), 1, 0, ttl)
end

-- 半开探测:逐字复制 ttft_allow_probe(机制方向无关)。限流态下每窗口放行至多 N 个探测继续测 TPS。
function M.tps_allow_probe(opts)
    local td = ngx.shared[opts.tps_dict]
    if not td then return true end
    local win = math.floor(ngx.now() / opts.tps_probe_window)
    local key = M.tps_key_prefix(opts) .. "probe:" .. win
    local n = td:incr(key, 1, 0)
    if not n then return true end
    if n == 1 then td:expire(key, opts.tps_probe_window * 2) end
    return n <= opts.tps_probe_per_window
end

return M
