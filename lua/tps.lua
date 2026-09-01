-- openresty/lua/tps.lua
-- TPS 解码速率限流(池级 EWMA + 窗口 P20 + 半开探测)
-- 跨模块函数挂 M(dict_if_on/key_prefix/ewma_key/limit_for/record/allow_probe);
-- bucket_index/window_p20 仅内部用 → local。TPS_ENABLED / TPS_BUCKETS 是 config data 仍留 _G。

local M = {}

local util = require "util"

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

-- EWMA key:`<route>[:<model>]:<range>:<metric>:ewma`,与 ttft_ewma_key 同构(range 恒 "all" 为预留段,
-- metric 按语义命名而非数组下标)。详见 ttft.lua 的注释。
function M.tps_ewma_key(opts, model, metric)
    return M.tps_key_prefix(opts, model) .. "all:" .. (metric or "p80") .. ":ewma"
end

-- 解析本请求该用的 TPS 下限(tokens/sec):override > tps_limit_by_model[model] > opts.tps_limit_tps。
-- model 显式传优先(timer 无 ngx.ctx),不传回落 ngx.ctx.req_model —— 与 tps_key_prefix 一致。
-- 在线 override(见 /_tps_limit)。抽出来的理由同 ttft_override_for:它在优先级链里压过 CRD。
function M.tps_override_for(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local td = ngx.shared[opts.tps_dict]
    if not td then return nil end
    if opts.peers_by_model then
        local mo = td:get((opts.route_name or "?") .. ":" .. (m or "") .. ":limit_override")
        if mo then return mo end
    end
    return td:get((opts.route_name or "?") .. ":limit_override")
end

-- 只看 factory opts 的那一段(不查 override、不查 CRD)。理由同 ttft_static_limit_for。
function M.tps_static_limit_for(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local bym = opts.tps_limit_by_model
    if bym and opts.peers_by_model then
        local v = bym[m or ""]
        if v then return v end
    end
    return opts.tps_limit_tps
end

function M.tps_limit_for(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local ovr = M.tps_override_for(opts, m)
    if ovr then return ovr end
    return M.tps_static_limit_for(opts, m)
end

-- 本路由/模型生效的指标列表。与 ttft_metrics 同构,唯一差别是 **q 的方向**:
-- CRD 的 `otps: p80` = 「80% 请求 OTPS ≥ threshold」→ 要取分布的**低尾 P20** → q = 0.2。
-- (TTFT 的 `p80` = 「80% 请求 ≤ threshold」→ 取高尾 P80 → q = 0.8。)
-- 方向换算只在这里/下发侧做一次,util.window_stat 只认「取第 q 分位」这一个原语。
-- q 写字面量 0.2,不写 1-0.8(= 0.19999999999999996)—— 纯卫生,实测不影响判定。
-- 优先级链同 ttft_metrics:override > CRD > factory opts > _G;第二返回值是来源。
function M.tps_metrics(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    local ovr = M.tps_override_for(opts, m)
    if ovr then return { { metric = "p80", q = 0.2, threshold = ovr } }, "override" end
    return M.tps_metrics_beneath(opts, m)
end

-- 优先级链去掉 override 那一层。只给 /_tps_status 用,同 ttft_metrics_beneath。
-- opts.tps_metrics 的 q 已是**低尾方向**(CRD 的 otps p80 → q=0.2),换算在 autoconfig 侧做。
function M.tps_metrics_beneath(opts, model)
    local m = model
    if m == nil then m = ngx.ctx.req_model end
    if opts.tps_metrics then return opts.tps_metrics, "crd" end
    return { { metric = "p80", q = 0.2, threshold = M.tps_static_limit_for(opts, m) } }, "static"
end

-- 违约判定(access 的 TPS-429 与 timers 的 AIMD 共用)。方向与 TTFT 相反:EWMA **低于**下限才是过载。
-- ⚠️ strict 参数保留一个**既有的不对称**,不要"顺手统一":
--     access.lua 用 `<=`(strict=false,默认),timers.lua 的 AIMD 用 `<`(strict=true)。
--     两者本来就不同,统一会改变边界行为(ewma 恰等于阈值时)。
function M.tps_assess(opts, td, model, strict)
    local ms = M.tps_metrics(opts, model)
    local first
    for i, mt in ipairs(ms) do
        local ew = td:get(M.tps_ewma_key(opts, model, mt.metric))
        if i == 1 then first = ew end
        if ew and mt.threshold
           and ((strict and ew < mt.threshold) or ((not strict) and ew <= mt.threshold)) then
            return { ewma = first, hit = true,
                     hit_ewma = ew, hit_limit = mt.threshold, hit_metric = mt.metric }
        end
    end
    return { ewma = first, hit = false }
end

-- 样本(tokens/sec) → 直方图桶号(1..#buckets+1,最后一个是 overflow)。同 ttft_bucket_index。
local function tps_bucket_index(tps)
    local b = _G.TPS_BUCKETS
    for i = 1, #b do if tps <= b[i] then return i end end
    return #b + 1
end

-- 窗口统计量已抽到 util.window_stat(与 TTFT 共用),分位系数由 tps_metrics 给(OTPS 取低尾)。
-- ⚠️ _G.TPS_BUCKETS 在**调用点现读**,不快照进 opts —— 保持「改 _G 免 reload 立即生效」。

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
        local ms = M.tps_metrics(opts)
        for w = last, win - 1 do
            if td:add(pre .. "fd:" .. w, 1, ttl) then
                local a = opts.tps_ewma_alpha
                for _, mt in ipairs(ms) do
                    local v = util.window_stat(td, pre, w, _G.TPS_BUCKETS, mt.metric, mt.q)
                    if v then
                        local k   = M.tps_ewma_key(opts, nil, mt.metric)
                        local old = td:get(k)
                        td:set(k, old and (a * v + (1 - a) * old) or v, ttl)
                    end
                end
                for b = 1, #_G.TPS_BUCKETS + 1 do td:delete(pre .. "h:" .. w .. ":" .. b) end
                td:delete(pre .. "h:" .. w .. ":sum")   -- avg 累计器,与桶同期清理
                td:delete(pre .. "h:" .. w .. ":cnt")
            end
        end
        td:set(lastk, win, ttl)
    end
    td:incr(pre .. "h:" .. win .. ":" .. tps_bucket_index(sample_tps), 1, 0, ttl)
    td:incr(pre .. "h:" .. win .. ":sum", sample_tps, 0, ttl)   -- avg 指标用(带 init_ttl 防孤儿泄漏)
    td:incr(pre .. "h:" .. win .. ":cnt", 1, 0, ttl)
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
