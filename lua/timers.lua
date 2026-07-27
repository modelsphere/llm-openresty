-- openresty/lua/timers.lua
-- worker0 后台 timer:health-check / cluster_avg / adaptive_cc(AIMD)
-- 三个 do_*_loop 由 route.register_route 启动 → 收敛到 M。timers 加载序在 route/tps 之后
-- (见 router.lua),故顶层 require 二者(route 已把对 timers 的 require 延后到 register_route
--  调用期,不构成加载期环)。DEFAULT_HEALTH_PROBE_PATH 是 config data 仍留 _G。

local M = {}

local route = require "route"
local tps   = require "tps"

-- ══════════════════════════════════════════════════════════════════════
-- M.do_health_check_loop(opts) — init_worker 调用，启动健康检查 timer
-- ══════════════════════════════════════════════════════════════════════

function M.do_health_check_loop(opts)
    -- 防御：若 opts 引用的 shared_dict 没声明（例如独立路由 conf 被删但 opts 注册过），
    -- 直接 skip 不启动 timer，避免 nil:set() 报错。
    if not ngx.shared[opts.bad_peers_dict] then
        ngx.log(ngx.WARN, "[", opts.route_name, "] health check skipped: shared_dict '",
                opts.bad_peers_dict, "' not declared (route conf may be missing)")
        return
    end
    local HEALTH_INTERVAL = opts.health_check_interval or 10
    local BAN_TTL         = opts.health_ban_ttl or 300
    local PROBE_PATH      = opts.health_probe_path or _G.DEFAULT_HEALTH_PROBE_PATH   -- per-route 可配探针路径
    local function check_all_peers()
        local bad = ngx.shared[opts.bad_peers_dict]
        for _, pk in ipairs(opts.peer_keys) do
            local host, port = pk:match("^(.+):(%d+)$")
            local sock = ngx.socket.tcp()
            sock:settimeout(10000)
            local ok, err = sock:connect(host, tonumber(port))
            if not ok then
                if not bad:get(pk) then
                    ngx.log(ngx.WARN, "[", opts.route_name, "] health: ", pk,
                            " DOWN (connect failed: ", err, "), banning ", BAN_TTL, "s")
                end
                bad:set(pk, true, BAN_TTL)
                sock:close()
            else
                sock:send("GET " .. PROBE_PATH .. " HTTP/1.0\r\nHost: " .. host .. "\r\n\r\n")
                local line, rerr = sock:receive("*l")
                sock:close()
                if line and line:find("200") then
                    if bad:get(pk) then
                        ngx.log(ngx.WARN, "[", opts.route_name, "] health: ", pk, " RECOVERED, unbanning")
                    end
                    bad:delete(pk)
                elseif rerr == "timeout" or (line and line:find(" 429")) then
                    -- 429=peer 限流(背压)/ timeout=后端忙:peer 有响应=活着,同 timeout 保活不 ban。
                    -- (429 是 4xx,peer HTTP 栈正常、只是正确拒绝超额流量;ban 300s 反而减容量)
                    ngx.log(ngx.WARN, "[", opts.route_name, "] health: ", pk, " busy (",
                            (rerr == "timeout" and "read timeout" or "429 rate-limited"),
                            "), keep alive")
                else
                    if not bad:get(pk) then
                        ngx.log(ngx.WARN, "[", opts.route_name, "] health: ", pk,
                                " unhealthy (line=", line or "nil",
                                ", err=", rerr or "nil", "), banning ", BAN_TTL, "s")
                    end
                    bad:set(pk, true, BAN_TTL)
                end
            end
        end
    end
    local function loop(premature)
        if premature then return end
        if #opts.peers == 0 then
            -- peer 列表为空（独立路由 conf 未填 peers），跳过本轮，仍调度下一轮（reload 后可加 peer）
        else
            local ok, err = pcall(check_all_peers)
            if not ok then
                ngx.log(ngx.ERR, "[", opts.route_name, "] health_loop crashed: ", err)
            end
        end
        local _, terr = ngx.timer.at(HEALTH_INTERVAL, loop)
        if terr and not ngx.worker.exiting() then
            ngx.log(ngx.ERR, "[", opts.route_name, "] health_loop schedule failed: ", terr)
        end
    end
    ngx.timer.at(5, loop)
end

-- ══════════════════════════════════════════════════════════════════════
-- M.do_cluster_avg_loop(opts) — init_worker 调用，启动 cluster_avg 采样 timer
-- ══════════════════════════════════════════════════════════════════════
function M.do_cluster_avg_loop(opts)
    if not (ngx.shared[opts.active_conns_dict] and ngx.shared[opts.cluster_avg_dict]) then
        ngx.log(ngx.WARN, "[", opts.route_name, "] cluster_avg skipped: shared_dict missing (route conf may be deleted)")
        return
    end
    local INTERVAL = opts.cluster_avg_interval or 30
    local function sample()
        local ac = ngx.shared[opts.active_conns_dict]
        local ca = ngx.shared[opts.cluster_avg_dict]
        local idx = math.floor(ngx.now() / INTERVAL) % 10
        if opts.peers_by_model then
            -- 新格式：各子池独立聚合到 "<model>:<idx>" bucket，
            -- 避免跨模型容量串扰（compute_cluster_avg 按 req.model 读对应桶）。
            for m, pool in pairs(opts.peers_by_model) do
                local sum = 0
                for _, p in ipairs(pool) do
                    sum = sum + (ac:get(p[1]..":"..p[2]) or 0)
                end
                ca:set(m .. ":" .. tostring(idx), sum, 360)
            end
        else
            local sum = 0
            for _, p in ipairs(opts.peers) do
                sum = sum + (ac:get(p[1]..":"..p[2]) or 0)
            end
            ca:set(tostring(idx), sum, 360)
        end
    end
    local function loop(premature)
        if premature then return end
        local ok, err = pcall(sample)
        if not ok then
            ngx.log(ngx.ERR, "[", opts.route_name, "] cluster_avg crashed: ", err)
        end
        local _, terr = ngx.timer.at(INTERVAL, loop)
        if terr and not ngx.worker.exiting() then
            ngx.log(ngx.ERR, "[", opts.route_name, "] cluster_avg schedule failed: ", terr)
        end
    end
    ngx.timer.at(INTERVAL, loop)
end

-- ══════════════════════════════════════════════════════════════════════
-- M.do_adaptive_cc_loop(opts) — AIMD 自适应并发 timer(按路由 opt-in)。
-- 复用 TPS EWMA 当反馈信号:每 interval 对每个子池(peers_by_model 逐 model / flat 单池)——
--   EWMA < 阈值 → 并发上限 ×dec(减);EWMA >= 阈值 时**按并发压力双向跟随实际并发**:
--   conc >= cc×pressure_frac(顶到边缘)→ ×inc 涨;conc < cc×slack_frac(余量太大/无流量)→ ×dec 缩;
--   中间保持(防抖带)。防轻流量下 cc 跑飞到 max、也防忙→闲后卡在旧峰值。clamp 在 [min, 静态max]。
--   写入 tps_dict "<route>[:<model>]:adaptive_cc",**带 TTL(adaptive_cc_ttl)**:有信号每 tick 刷新;
--   EWMA 过期(nil,无信号)→ 本 tick 不写 → 值持续无信号超 TTL 后老化消失 → assess_pool 读 nil →
--   do_route 回退到 min 慢启动(不从满容量开始;健康则 ×inc 逐步爬回)。短暂信号缺口内(<TTL)仍保持
--   当前值。阈值/max/min 在 timer 内解析,不读 ngx.ctx。
-- ══════════════════════════════════════════════════════════════════════
function M.do_adaptive_cc_loop(opts)
    if not opts.adaptive_cc then return end   -- 门控:未 opt-in 不启 timer
    local td = ngx.shared[opts.tps_dict]
    if not td then
        ngx.log(ngx.WARN, "[", opts.route_name, "] adaptive_cc skipped: tps_dict missing")
        return
    end
    local INTERVAL = opts.adaptive_cc_interval
    local DEC, INC = opts.adaptive_cc_dec, opts.adaptive_cc_inc
    local CC_TTL = opts.adaptive_cc_ttl
    local rname = opts.route_name or "?"

    -- 解析某 model 的 TPS 阈值(override > by_model > route),timer 版不读 ngx.ctx。
    local function thr_for(model)
        if model then
            local mo = td:get(rname .. ":" .. model .. ":limit_override")
            if mo then return mo end
        end
        local ro = td:get(rname .. ":limit_override")
        if ro then return ro end
        if model and opts.tps_limit_by_model then
            local v = opts.tps_limit_by_model[model]
            if v then return v end
        end
        return opts.tps_limit_tps
    end

    -- 单子池一步 AIMD(model=false 表 flat 路由)。
    local function step_one(model)
        local pre = tps.tps_key_prefix(opts, model)   -- 统一走 helper(flat: model=false → "route:")
        local ewma = td:get(pre .. "ewma")
        if not ewma then return end          -- 无信号 → 保持当前值不动(不写)
        local maxcc = route.compute_static_max_cc(opts, model)
        if maxcc <= 0 then return end
        local mincc = route.derive_mincc(opts, maxcc)
        local thr = thr_for(model)
        local cur = td:get(pre .. "adaptive_cc") or mincc   -- 首次/过期 → 从 min 起步(慢启动,健康则 ×inc 爬升)
        -- 修复1:读+清零本区间被压抑需求(并发 429 数)。>0 = 需求超过 cc、被拒的量 rt_sum 看不到。
        local rej = td:get(pre .. "rej") or 0
        if rej > 0 then td:delete(pre .. "rej") end
        if thr then
            local conc = td:get(pre .. "rt_sum") or 0
            local mid  = (opts.adaptive_cc_pressure_frac + opts.adaptive_cc_slack_frac) / 2
            local ABS  = opts.adaptive_cc_abs or 0
            -- 目标 cc:相对(conc/mid≈1.25×)与绝对(conc+ABS)取大 → 任何并发下都留够 ABS 绝对头寸。
            local desired = math.max(conc / mid, conc + ABS)
            if ewma < thr then
                cur = cur * DEC                              -- 解码慢(过载)→ 缩,最高优先,照常保护后端
            elseif rej > 0 then
                -- 有并发 429 = 需求 > cc。被拒的量 rt_sum 看不到 → **不能用 conc 封顶**(会低估、把 cc 锁死),
                -- 但也**不跳变**:按 ×INC 保守增量爬(与正常压力同速率),到 maxcc 由下方 clamp 兜。
                -- 429 持续 → 每 tick ×INC 稳步涨;429 停(需求满足)→ 落回下面无-rej 分支 hold/缩。
                cur = cur * INC
            else
                -- 健康、无被拒需求:并发压力 AIMD,相对系数 + 绝对头寸双门。
                --   涨门:并发顶到 cc×pressure_frac(相对) 或 绝对头寸不足 cc-conc<ABS(小并发兜底)。
                --   缩门:并发 < cc×slack_frac(相对) 且 绝对余量够 cc-conc>ABS(降也要够绝对余量才降)。
                --   涨/缩都向 desired 收(不破 conc+ABS 绝对底);conc==0 且头寸够 → 保持(不缩回 min)。
                local at_pressure = (conc >= cur * opts.adaptive_cc_pressure_frac) or ((cur - conc) < ABS)
                local at_slack    = (conc > 0) and (conc < cur * opts.adaptive_cc_slack_frac) and ((cur - conc) > ABS)
                if at_pressure then
                    cur = math.min(cur * INC, desired)        -- 涨,但不越过 desired
                elseif at_slack then
                    cur = math.max(cur * DEC, desired)        -- 缩,但不低于 desired(留 ABS 绝对底)
                end
            end
        end
        cur = math.max(mincc, math.min(maxcc, cur))
        -- 带 TTL 写入:有 EWMA 信号就刷新(值持续有效);无信号则不写 → CC_TTL 后老化消失 →
        -- assess_pool 读 nil → do_route 回退到 min 慢启动。CC_TTL 远大于 interval,短暂缺口内仍保持当前值。
        td:set(pre .. "adaptive_cc", cur, CC_TTL)
    end

    local function sample()
        if opts.peers_by_model then
            for m in pairs(opts.peers_by_model) do step_one(m) end
        else
            step_one(false)
        end
    end
    local function loop(premature)
        if premature then return end
        local ok, err = pcall(sample)
        if not ok then
            ngx.log(ngx.ERR, "[", rname, "] adaptive_cc crashed: ", err)
        end
        local _, terr = ngx.timer.at(INTERVAL, loop)
        if terr and not ngx.worker.exiting() then
            ngx.log(ngx.ERR, "[", rname, "] adaptive_cc schedule failed: ", terr)
        end
    end
    ngx.timer.at(INTERVAL, loop)
end

return M
