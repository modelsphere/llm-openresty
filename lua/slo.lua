-- openresty/lua/slo.lua
-- LLMSLORequirement(k8s CRD)下发的 SLO 数据:加载、校验、查询。
--
-- 数据流:autoconfig operator watch CRD → 渲染 ConfigMap → 挂成文件 → 本模块 timer 读 → per-worker table。
-- **不 reload**、**不进 shared dict**、**不抢锁**:
--   * 配置只读且小(几 KB),每 worker 各存一份完全够 —— 省掉跨 worker 一致性与 lc_locks 那套
--     抢锁+续命+stale 重试(route.lua 那段逻辑相当微妙,不该为读个配置文件再复制一遍);
--   * 替换是整表引用赋值(原子,无撕裂);worker 间最多差一个 loader 周期看到不同阈值 —— 阈值渐变,无害;
--   * 读 table 比 dict:get 快,而 metrics 查询在**每请求热路径**上。
--
-- ⚠️ 裸机(gateway-host/ts31/gateway-host 无 k8s)零影响:SLO_CONFIG_PATH 未设 → 不起 timer、表恒空
--    → 所有查询 miss → 调用方回落静态配置,行为与接 CRD 前逐字节一致。

local M = {}

local cjson = require "cjson.safe"

-- ── per-worker 状态(module-local,非 _G、非 shared dict)──────────────────────
local DATA      = nil     -- 解析后的表:{ version=<n>, routes={ [route]={ [model]={ttft=…, otps=…} } } }
local RAW       = nil     -- 上一次成功读到的原文(用于"没变就不重解析")
local SRC       = "none"  -- none | file | post
local PINNED    = false   -- POST 注入过 → 在下次**文件内容变更**前不被 loader 覆盖
local LAST_ERR  = nil     -- 最近一次解析/读取失败原因(不清空 DATA)
local LAST_LOAD = nil     -- 最近一次成功加载的时间戳
local ERR_COUNT = 0

local MODEL_DEFAULT = "__default__"

-- ── 解析 + 校验 ───────────────────────────────────────────────────────────────
-- 严格校验:任何一条不合法 → 整份拒绝(返 nil, err),**绝不半份生效**。
-- 半份生效比不生效更危险:某个 route 的阈值悄悄用了默认值,没人会发现。
local function validate_metrics(list, kind, where)
    if type(list) ~= "table" or #list == 0 then
        return nil, where .. ": metrics 必须是非空数组"
    end
    local out = {}
    for i, m in ipairs(list) do
        if type(m) ~= "table" then return nil, where .. "[" .. i .. "]: 不是对象" end
        local name = m.metric
        if type(name) ~= "string" or name == "" then
            return nil, where .. "[" .. i .. "]: metric 缺失或非字符串"
        end
        -- avg 不需要 q(没有分位含义);pNN 必须带 q,且 q 已是**引擎直接用的取分位系数**
        -- (TTFT q=coverage、OTPS q=1-coverage 的方向换算在 operator 侧做,引擎不做方向判断)
        local q = m.q
        if name ~= "avg" then
            if type(q) ~= "number" or q <= 0 or q >= 1 then
                return nil, where .. "[" .. i .. "]: metric=" .. name .. " 需要 0<q<1,得到 " .. tostring(q)
            end
        end
        -- 阈值字段按 kind 取名(ttft 是毫秒、otps 是 tok/s),内部统一成 threshold
        local th = (kind == "ttft") and m.threshold_ms or m.threshold_tps
        if type(th) ~= "number" or th < 0 then
            return nil, where .. "[" .. i .. "]: 缺少合法的 " ..
                        ((kind == "ttft") and "threshold_ms" or "threshold_tps")
        end
        out[#out + 1] = { metric = name, q = q, threshold = th }
    end
    return out
end

local function validate_kind(node, kind, where)
    if node == nil then return nil end                  -- 该 kind 未声明 = 不覆盖,回落静态
    if type(node) ~= "table" then return nil, where .. ": 不是对象" end
    -- ranges 本期不实现(见 plans/openresty_llmslo_crd.md §1.4):有内容就 WARN 并忽略,不报错。
    if type(node.ranges) == "table" and #node.ranges > 0 then
        ngx.log(ngx.WARN, "[slo] ", where, ": 声明了 ", #node.ranges,
                " 条 ranges,本版本不支持按 contextLength 分段,已忽略(只用 default)")
    end
    local d = node.default
    if d == nil then return nil end                     -- 只有 ranges 没有 default → 等同未声明
    if type(d) ~= "table" then return nil, where .. ".default: 不是对象" end
    local ms, err = validate_metrics(d.metrics, kind, where .. ".default.metrics")
    if not ms then return nil, err end
    return { metrics = ms }
end

-- txt → 内部表;失败返 nil, err。纯函数,不改全局状态(便于单测)。
function M.parse(txt)
    if type(txt) ~= "string" or txt == "" then return nil, "空内容" end
    local obj, derr = cjson.decode(txt)
    if not obj then return nil, "JSON 解析失败: " .. tostring(derr) end
    if type(obj) ~= "table" then return nil, "顶层不是对象" end
    if type(obj.routes) ~= "table" then return nil, "缺少 routes 对象" end

    local routes = {}
    for rname, rnode in pairs(obj.routes) do
        if type(rname) ~= "string" then return nil, "route 名非字符串" end
        if type(rnode) ~= "table" then return nil, "routes." .. rname .. ": 不是对象" end
        local models = {}
        for mname, mnode in pairs(rnode) do
            local where = "routes." .. rname .. "." .. tostring(mname)
            if type(mnode) ~= "table" then return nil, where .. ": 不是对象" end
            local ttft, e1 = validate_kind(mnode.ttft, "ttft", where .. ".ttft")
            if e1 then return nil, e1 end
            local otps, e2 = validate_kind(mnode.otps, "otps", where .. ".otps")
            if e2 then return nil, e2 end
            models[mname] = { ttft = ttft, otps = otps }
        end
        routes[rname] = models
    end
    return { version = obj.version, routes = routes }
end

-- ── 查询(热路径)─────────────────────────────────────────────────────────────
-- 返回该 route/model 在 kind 上声明的指标列表,或 nil(= 未覆盖 → 调用方回落静态)。
-- model 查找顺序:具体 model → __default__。生产目前无 peers_by_model 路由,恒走 __default__。
function M.metrics_for(route, model, kind)
    if not DATA then return nil end
    local r = DATA.routes[route or ""]
    if not r then return nil end
    local node = (model and model ~= "" and r[model]) or r[MODEL_DEFAULT]
    if not node then return nil end
    local k = node[kind]
    return k and k.metrics or nil
end

-- ── 加载 ─────────────────────────────────────────────────────────────────────
-- 应用一份已解析的表。src 记录来源,便于 /_ttft_status 标注"当前生效值从哪来"。
local function apply(tbl, src)
    DATA, SRC, LAST_LOAD, LAST_ERR = tbl, src, ngx.now(), nil
    ngx.log(ngx.NOTICE, "[slo] 已加载 version=", tostring(tbl.version), " source=", src,
            " routes=", (function() local n = 0; for _ in pairs(tbl.routes) do n = n + 1 end; return n end)())
end

-- 供 /_slo_conf POST 用:手动注入(应急下发 / 回归测试注入)。
-- 置 PINNED —— 直到**文件内容真的变了**才让 loader 夺回控制权,否则注入会被下一个 tick 抹掉。
function M.apply_post(txt)
    local tbl, err = M.parse(txt)
    if not tbl then LAST_ERR = err; ERR_COUNT = ERR_COUNT + 1; return nil, err end
    apply(tbl, "post"); PINNED = true
    return true
end

-- 读文件一次。⚠️ 必须 pcall 包住并保证 f:close() —— 解析异常泄 fd 会在几天后耗尽。
local function read_file(path)
    local f, oerr = io.open(path, "r")
    if not f then return nil, "打开失败: " .. tostring(oerr) end
    local ok, txt = pcall(function() return f:read("*a") end)
    f:close()
    if not ok then return nil, "读取异常: " .. tostring(txt) end
    return txt
end

-- 单次 tick:读 → 内容没变就直接返回(不重解析)→ 变了才解析并替换。
-- 解析失败**不清空已有 DATA**,只记 LAST_ERR + 计数,保持上一份好数据继续服务。
function M.tick(path)
    local txt, rerr = read_file(path)
    if not txt then
        -- 文件暂时不在(ConfigMap 尚未挂上 / operator 还没渲染)不算致命:保持现状。
        LAST_ERR = rerr; ERR_COUNT = ERR_COUNT + 1
        return false, rerr
    end
    if txt == RAW then return true end          -- 没变,免解析(每 tick 的常态)
    local tbl, perr = M.parse(txt)
    if not tbl then
        LAST_ERR = "解析失败(保持上一份好数据): " .. perr; ERR_COUNT = ERR_COUNT + 1
        ngx.log(ngx.ERR, "[slo] ", LAST_ERR)
        RAW = txt                               -- 记下坏内容,避免每个 tick 重复刷同一条 ERR
        return false, perr
    end
    RAW = txt
    PINNED = false                              -- 文件内容确实变了 → 收回 POST 注入的控制权
    apply(tbl, "file")
    return true
end

-- ── loader timer ─────────────────────────────────────────────────────────────
-- 在 init_worker_by_lua_block 里**无条件**调用一次;是否真的起 timer 由 SLO_CONFIG_PATH 决定。
-- 每 worker 各起一个(不抢锁):读的是本地小文件,重复读的代价远小于抢锁那套机制的复杂度。
function M.start_loader()
    local path = os.getenv("SLO_CONFIG_PATH")
    if not path or path == "" then
        -- 裸机没有 k8s/CRD。打一条日志,否则没人知道这个特性存在与否。
        ngx.log(ngx.NOTICE, "[slo] SLO_CONFIG_PATH 未设置 → 不启动 loader,全部使用静态配置(裸机部署预期如此)")
        return
    end
    local interval = tonumber(os.getenv("SLO_RELOAD_INTERVAL")) or 5
    local function loop(premature)
        if premature then return end
        if not PINNED then
            local ok, err = pcall(M.tick, path)
            if not ok then ngx.log(ngx.ERR, "[slo] loader tick 崩溃: ", err) end
        end
        local _, terr = ngx.timer.at(interval, loop)
        if terr and not ngx.worker.exiting() then
            ngx.log(ngx.ERR, "[slo] loader 调度失败: ", terr)
        end
    end
    ngx.log(ngx.NOTICE, "[slo] loader 启动,path=", path, " interval=", interval, "s")
    ngx.timer.at(0, loop)
end

-- ── 自省(/_slo_conf GET 与 /_ttft_status 的 slo_* 字段用)────────────────────
function M.status()
    local routes = {}
    if DATA then for r in pairs(DATA.routes) do routes[#routes + 1] = r end end
    table.sort(routes)
    return {
        loaded      = DATA ~= nil,
        version     = DATA and DATA.version or nil,
        source      = SRC,
        pinned      = PINNED,
        path        = os.getenv("SLO_CONFIG_PATH") or nil,
        routes      = routes,
        last_load   = LAST_LOAD,
        last_error  = LAST_ERR,
        error_count = ERR_COUNT,
    }
end

function M.dump() return DATA end

-- 单测用:重置 per-worker 状态
function M._reset()
    DATA, RAW, SRC, PINNED, LAST_ERR, LAST_LOAD, ERR_COUNT = nil, nil, "none", false, nil, nil, 0
end

return M
