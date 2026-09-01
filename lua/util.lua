-- openresty/lua/util.lua
-- cjson array_mt 初始化 + _nonblank + pick_rendezvous + opts_missing
-- 跨模块调用:`local util = require "util"` 后用 util.foo(...)（不再挂 _G）。

local M = {}

local cjson = require "cjson.safe"
-- 让 cjson 在 decode 时给 JSON array 打 array_mt metatable，
-- 后续 encode 能区分空数组 [] 和空对象 {}（修复 cch_strip / normalize 改 body 后
-- tools[i].function.parameters.required 等空 array 被错序列化为 {} 触发 sglang 400）。
if cjson.decode_array_with_array_mt then
    cjson.decode_array_with_array_mt(true)
end
-- 非空且不全是空白才算有效 sid;非标量(table/bool/nil)直接拒绝。
-- 供各 phase(access/log 等)调用;别处要快可自行 local 别名(如 reqtransform.lua)。
function M._nonblank(s)
    if type(s) ~= "string" and type(s) ~= "number" then return false end
    local str = tostring(s)
    return str ~= "" and str:match("%S") ~= nil
end

-- Rendezvous 哈希 pick: 在 peer_list 中找 md5(sid|peer_key) 最大者。
-- 返回 best_idx (1-based index into peer_list), best_hash。
-- peer_list 每个元素 {host, port, orig_idx, cached_key}; cached_key 可选，没传则重算。
-- 三处 caller 共享此函数: 主路由 access_by_lua、/_route_debug、/_route_inspect。
function M.pick_rendezvous(sid, peer_list)
    local best_h, best_idx = -1, 1
    local prefix = sid .. "|"   -- 循环外一次构造，省 N 次小分配
    for i, hp in ipairs(peer_list) do
        local pk = hp[4] or (hp[1] .. ":" .. hp[2])
        local h = tonumber(string.sub(ngx.md5(prefix .. pk), 1, 8), 16) or 0
        if h > best_h then best_h, best_idx = h, i end
    end
    return best_idx, best_h
end

-- ══════════════════════════════════════════════════════════════════════
-- 窗口直方图 → 统计量(TTFT / TPS 共用;两边原先是 ttft_window_p80 / tps_window_p20 两份
-- 逐字同构的拷贝,只差分位系数与桶数组)
-- ══════════════════════════════════════════════════════════════════════
-- 从窗口 w 的直方图算**第 q 分位**(累计越过 total*q 的桶上界);空窗返 nil。
-- ⚠️ 空窗必须返 nil 而非 0:下游把 nil 当「无样本,不判定」(fail-open),
--    返 0 会让 TPS 侧 `0 <= limit` 恒成立 → 瞬间全拒。
-- ⚠️ buckets 由**调用方现读** _G.TTFT_BUCKETS_MS / _G.TPS_BUCKETS 传入,不要在 register_route
--    里快照进 opts —— 否则「改 _G 免 reload 立即生效」会悄悄变成「必须 reload」。
-- 桶共 n+1 个(第 n+1 个是 overflow),overflow 代表值 = 末桶 × 2。
function M.window_quantile(td, pre, w, buckets, q)
    local b = buckets
    local n = #b
    local counts, total = {}, 0
    for i = 1, n + 1 do
        local c = td:get(pre .. "h:" .. w .. ":" .. i) or 0
        counts[i] = c; total = total + c
    end
    if total == 0 then return nil end
    local target, cum = total * q, 0
    for i = 1, n + 1 do
        cum = cum + counts[i]
        if cum >= target then return b[i] or (b[n] * 2) end   -- overflow 桶代表值 = 末桶×2
    end
    return b[n] * 2
end

-- 窗口均值(CRD 的 `avg` 指标)。直方图算不出均值,故 record 时额外累计 sum/cnt 两个 key。
-- 空窗同样返 nil(语义与 window_quantile 对齐)。
function M.window_avg(td, pre, w)
    local cnt = td:get(pre .. "h:" .. w .. ":cnt") or 0
    if cnt == 0 then return nil end
    return (td:get(pre .. "h:" .. w .. ":sum") or 0) / cnt
end

-- 按 CRD 的 metric 名从窗口取值:"avg" 走均值,"pNN" 走分位(q 由调用方给,
-- 因为 TTFT 的 q=coverage 而 OTPS 的 q=1-coverage,方向换算不在引擎内做)。
function M.window_stat(td, pre, w, buckets, metric, q)
    if metric == "avg" then return M.window_avg(td, pre, w) end
    return M.window_quantile(td, pre, w, buckets, q)
end

-- 共享 nil-opts 兜底:route 未注册(register_route 失败 / server 的 set $route 与 register 名不匹配)
-- 时 _G.__route_opts[ngx.var.route] 为 nil。content/access phase 的 do_route + 各 dbg_* 统一用它返
-- JSON 500,避免同一 guard 复制到 ~20 处后漂移(code review G6)。返回 true=opts 缺失(调用方应
-- return / return ngx.exit(500))。log phase 的 do_log_release、balancer phase 不能 ngx.say,各自保留简单 guard。
function M.opts_missing(opts)
    if opts then return false end
    ngx.status = 500
    ngx.header["Content-Type"] = "application/json"
    ngx.say([[{"error":"route not initialized — check error.log for register_route failures"}]])
    return true
end

-- ── SLO 指标表校验 ────────────────────────────────────────────────────────────
-- factory opts 里的 ttft_metrics / tps_metrics(由 autoconfig 从 LLMSLORequirement 渲染进
-- session_route_<route>.conf,也可手写)长这样:
--     { { metric = "p80", q = 0.8, threshold = 20000 }, ... }
--
-- q 已经是**引擎直接用的取分位系数** —— TTFT q=coverage、OTPS q=1-coverage 的方向换算在
-- autoconfig 侧做,引擎不做方向判断(单一真相点,免得两边各写一遍导致某边写反)。
--
-- 在 register_route 里调**一次**(每 reload 一次),不在热路径上。
-- 任何一条不合法 → 整份丢弃 + ERR 日志 + 回落静态单指标。
-- **不半份生效**:半份比不生效更危险,某个指标悄悄用了默认值,没人会发现。
-- **不让路由注册失败**:阈值配错不该把整条路由打死(那是断流),回落静态是安全的降级。
function M.validate_metrics(list, kind, route)
    if list == nil then return nil end
    local bad = function(msg)
        ngx.log(ngx.ERR, "[", route or "?", "] ", kind, "_metrics ", msg,
                " —— 整份丢弃,回落静态阈值(检查 ModelRoute 的 slo 段 / autoconfig 渲染)")
        return nil
    end
    if type(list) ~= "table" or #list == 0 then return bad("不是非空数组") end
    local out = {}
    for i, m in ipairs(list) do
        if type(m) ~= "table" then return bad("[" .. i .. "] 不是对象") end
        local name = m.metric
        if type(name) ~= "string" or name == "" then
            return bad("[" .. i .. "] metric 缺失或非字符串")
        end
        -- avg 没有分位含义,不校验 q;pNN 必须带 0<q<1
        if name ~= "avg" and (type(m.q) ~= "number" or m.q <= 0 or m.q >= 1) then
            return bad("[" .. i .. "] metric=" .. name .. " 需要 0<q<1,得到 " .. tostring(m.q))
        end
        if type(m.threshold) ~= "number" or m.threshold < 0 then
            return bad("[" .. i .. "] threshold 缺失或为负: " .. tostring(m.threshold))
        end
        out[i] = { metric = name, q = m.q, threshold = m.threshold }
    end
    return out
end

return M

