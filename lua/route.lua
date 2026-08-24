-- openresty/lua/route.lua
-- register_route + 池解析/评估 + pick_from + affinity_gate + cluster_avg
-- 引擎函数收敛到返回的 M(register_route 仍挂 _G:被 per-model conf 的 set_by_lua 直调)。
-- 依赖:util/reject_rules/ttft/tps 都无环(ttft/tps 是叶子,只反向出现在注释里)→ 顶层 require。
-- 仅 timers 真反向 require 本模块(compute_static_max_cc/derive_mincc)且 register_route 又调 timers.do_*_loop
-- → route↔timers 成环 → 只对 timers 用函数内 lazy require 破环(register_route 在 init 期调,全模块已载好)。

local M = {}

local cjson = require "cjson.safe"
local util  = require "util"
local reject_rules = require "reject_rules"
local ttft  = require "ttft"
local tps   = require "tps"
local timers   -- route↔timers 真环:延后到 register_route 首次用时 lazy require

-- 5min 滑动窗口 cluster_avg 聚合：返回 (sum, samples, avg)
-- - sum     = 10 个 30s 桶累加（缺失桶按 0 计入和，n 不增）
-- - samples = 实际写过的桶数（最多 10，冷启时小）
-- - avg     = sum / samples（strict 模式，n=0 时返 0）
-- 用法：access_by_lua 做 429 判断；/_cluster_avg debug endpoint
-- model 非空时读 per-model bucket（"<model>:<idx>"），用于新格式 peers_by_model
-- 各子池独立容量；为空时读 route-wide bucket（"<idx>"），老格式行为不变。

function M.compute_cluster_avg(dict_name, model)
    local ca = ngx.shared[dict_name or "cluster_avg"]
    local prefix = (model ~= nil and model ~= false) and (model .. ":") or ""
    local sum, n = 0, 0
    for i = 0, 9 do
        local v = ca:get(prefix .. tostring(i))
        if v then sum = sum + v; n = n + 1 end
    end
    local avg = n > 0 and (sum / n) or 0
    return sum, n, avg
end

-- ══════════════════════════════════════════════════════════════════════
-- _G.register_route(name, opts_factory) — 路由注册入口
-- 调用方(两条路径):① K2.5 主路由在 session_route.conf 的 init_worker_by_lua_block 里 eager 注册;
--   ② 各 per-model conf 在 server 块顶部用 set_by_lua_block 懒注册(首请求时)。两者 idempotent。
-- 行为：第一次调用时执行 factory()，自动补全默认字段，注册到 _G.__route_opts[name]，
-- 并 worker 0 上启动 health/cluster_avg timer。重复调用 idempotent (直接返 cached opts)。
-- ══════════════════════════════════════════════════════════════════════
function _G.register_route(name, opts_factory)
    _G.__route_opts = _G.__route_opts or {}
    if _G.__route_opts[name] then return _G.__route_opts[name] end
    -- 缓存注册失败:set_by_lua_block 每请求都调本函数,若 factory 坏(抛错/返非 table),不缓存的话
    -- 会每请求重跑抛错的 factory + 刷 ERR 日志。记下失败名后短路 → 只在首请求 log 一次。
    -- __route_opts[name] 仍为 nil → do_route 照旧返清晰 500(不改可观测行为);reload 重置 _G 后重试。
    _G.__route_failed = _G.__route_failed or {}
    if _G.__route_failed[name] then return nil end
    -- L1: pcall 包 factory，写错 conf 时 error_log 直接显示原因（而非各请求静默 500）
    local ok, opts_or_err = pcall(opts_factory)
    if not ok then
        ngx.log(ngx.ERR, "[", name, "] register_route factory() failed: ", opts_or_err,
                " — fix the route conf and reload")
        _G.__route_failed[name] = true
        return nil
    end
    local opts = opts_or_err
    if type(opts) ~= "table" then
        ngx.log(ngx.ERR, "[", name, "] register_route factory must return a table, got ", type(opts))
        _G.__route_failed[name] = true
        return nil
    end
    opts.route_name = name
    -- 新格式 peers_by_model：按 model 分组的子池。把 opts.peers 建成所有子池的并集
    -- （health probe / dict / 容量统计基于 opts.peers，需覆盖全部 peer），
    -- 同时建 opts.peer_keys_by_model 供 do_route 按 body.model 选子池。
    -- 老格式（只有 opts.peers）跳过此段，行为完全不变。
    if opts.peers_by_model then
        opts.peers = {}
        opts.peer_keys_by_model = {}
        local seen = {}
        for m, pool in pairs(opts.peers_by_model) do
            local mkeys = {}
            for _, p in ipairs(pool) do
                local k = p[1] .. ":" .. p[2]
                mkeys[#mkeys + 1] = k
                if not seen[k] then
                    seen[k] = true
                    opts.peers[#opts.peers + 1] = p
                end
            end
            opts.peer_keys_by_model[m] = mkeys
        end
    end
    -- 病态 conf 兜底:factory 既没返 peers 也没返 peers_by_model 时 opts.peers 为 nil,
    -- 下游 do_route/dbg/balancer 的 ipairs(peers) 会崩(ipairs nil → 500)。置空表后
    -- 退化成优雅的 503 "all peers banned"(与 peers={} 一致)。
    opts.peers = opts.peers or {}
    -- 自动构建 peer_keys + per-peer 健康探测路径映射(peer 的命名字段 probe = 探测路径,可选)。
    -- 用途:cache_aware_router 的 /v1/models 是缓存端点(worker 全挂也返 200,不能当健康信号),
    -- 故 cart peer 配 probe="/health"(worker-aware,0 healthy→503);后端 peer 不配 → 回落 health_probe_path。
    -- 用命名字段(非第 6 位)以免和位置元组 {ip,port,name,priority,max} 冲突、也不需填满前置位。
    -- gpu_by_key:peer 的命名字段 gpu = "<型号短名>"(H100/H800/A100/H200/B300…,autoconfig 从
    -- 节点 GFD label nvidia.com/gpu.product 逐 peer 推导后渲染进 conf;裸机 conf 可手写)。
    -- 可选,没配的 peer 不进表。用途:bodylog 记录请求实际落到哪种卡,供按型号聚合统计。
    -- name_by_key:peer 名(位置元组第 3 位)。autoconfig 渲染为节点名,便于定位到物理机。
    opts.peer_keys = {}
    opts.probe_path_by_key = {}
    opts.gpu_by_key = {}
    opts.name_by_key = {}
    for _, p in ipairs(opts.peers) do
        local key = p[1] .. ":" .. p[2]
        opts.peer_keys[#opts.peer_keys + 1] = key
        if p.probe then opts.probe_path_by_key[key] = p.probe end
        if p.gpu   then opts.gpu_by_key[key]        = p.gpu   end
        if p[3]    then opts.name_by_key[key]       = p[3]    end
    end
    -- dict 名约定：<base>_<route_name>（必须跟 conf 顶部 lua_shared_dict 声明一致）
    -- factory 可显式覆盖（如 api_keys_dict 跨路由共用 "api_keys"）。
    opts.active_conns_dict         = opts.active_conns_dict         or ("active_conns_" .. name)
    opts.bad_peers_dict            = opts.bad_peers_dict            or ("bad_peers_"    .. name)
    opts.lc_locks_dict             = opts.lc_locks_dict             or ("lc_locks_"     .. name)
    opts.cluster_avg_dict          = opts.cluster_avg_dict          or ("cluster_avg_"  .. name)
    opts.cch_ctl_dict              = opts.cch_ctl_dict              or ("cch_ctl_"      .. name)
    opts.bodylog_ctl_dict          = opts.bodylog_ctl_dict          or ("bodylog_ctl_"  .. name)
    -- expose_routed_peer:是否把 <ip:port>[/GPU型号] 回显进响应头 X-Routed-Peer。
    -- 默认 false(不外泄内部 peer);仅显式配 true 才开。非布尔值一律按 false 处理。
    opts.expose_routed_peer        = (opts.expose_routed_peer == true)
    opts.api_keys_dict             = opts.api_keys_dict             or "api_keys"
    opts.api_keys                  = opts.api_keys                  or { ["REDACTED-API-KEY"] = "admin" }
    opts.default_max               = opts.default_max               or _G.MAX_CONCURRENCY_PER_PEER
    -- 并发实时阈值放大倍数(每路由可配;缺省回退全局 _G.RT_LIMIT_FACTOR)
    opts.rt_limit_factor           = opts.rt_limit_factor           or _G.RT_LIMIT_FACTOR
    if type(opts.rt_limit_factor) ~= "number" or opts.rt_limit_factor <= 0 then
        ngx.log(ngx.ERR, "[", name, "] rt_limit_factor=", tostring(opts.rt_limit_factor),
                " 非法(需正数)— 回退 ", _G.RT_LIMIT_FACTOR)
        opts.rt_limit_factor = _G.RT_LIMIT_FACTOR
    end
    if opts.cch_default_enabled        == nil then opts.cch_default_enabled        = _G.CCH_STRIP_DEFAULT_ENABLED end
    -- Kimi tool_call_id normalize 每路由默认（factory 显式设 false 即永久关，重启也不恢复；缺省回落全局 _G.KIMI_NORMALIZE_DEFAULT_ENABLED）
    if opts.kimi_normalize_default_enabled == nil then opts.kimi_normalize_default_enabled = _G.KIMI_NORMALIZE_DEFAULT_ENABLED end
    if opts.bodylog_default_enabled    == nil then opts.bodylog_default_enabled    = _G.BODYLOG_DEFAULT_ENABLED end
    opts.bodylog_default_pct       = opts.bodylog_default_pct       or _G.BODYLOG_DEFAULT_PCT
    if opts.disable_body_user_affinity == nil then opts.disable_body_user_affinity = _G.DISABLE_BODY_USER_AFFINITY end
    -- session 亲和性每路由开关:factory 不设(nil)则回落全局 _G.SESSION_AFFINITY_ENABLED。
    -- 显式 false → 本路由忽略所有 sid 走 least_conn;显式 true → 即便全局关本路由仍开粘性。
    if opts.session_affinity_enabled   == nil then opts.session_affinity_enabled   = _G.SESSION_AFFINITY_ENABLED end
    opts.health_check_interval     = opts.health_check_interval     or 10
    opts.health_ban_ttl            = opts.health_ban_ttl            or 300
    opts.health_probe_path         = opts.health_probe_path         or _G.DEFAULT_HEALTH_PROBE_PATH  -- 健康探针 GET 的路径,per-route 可配(后端探测接口不同的路由可覆盖)
    opts.cluster_avg_interval      = opts.cluster_avg_interval      or 30
    -- ── 跨层单请求 fallback(默认关)──
    -- cross_tier_fallback:开时高优层 5xx/连接失败即刻兜到低优层代表(不等 health-timer ban)。
    --   bool 化:任何 truthy → true、缺省/false/nil → false,避免 factory 传字符串 "false" 被当真。
    opts.cross_tier_fallback = opts.cross_tier_fallback and true or false
    -- max_more_tries:balancer.set_more_tries 的重试预算上限(需正整数;非法回退 2)。
    local mmt = tonumber(opts.max_more_tries)
    if mmt == nil then
        opts.max_more_tries = 2
    elseif mmt < 1 or mmt ~= math.floor(mmt) then
        ngx.log(ngx.ERR, "[", name, "] max_more_tries=", tostring(opts.max_more_tries),
                " 非法(需 >=1 整数)— 回退 2")
        opts.max_more_tries = 2
    else
        opts.max_more_tries = mmt
    end
    -- ── TTFT 限流（按路由灰度）默认值 ──
    -- TTFT 限流默认值。dict 默认指 session_route.conf 里统一声明的共享 "ttft_stat"(所有路由共用,key 带
    -- <route>[:<model>] 前缀);阈值默认用全局 _G.TTFT_LIMIT_MS。两者都可在 factory 覆盖。
    opts.ttft_dict                 = opts.ttft_dict                 or "ttft_stat"
    opts.ttft_limit_ms             = opts.ttft_limit_ms             or _G.TTFT_LIMIT_MS
    opts.ttft_ewma_alpha           = opts.ttft_ewma_alpha           or 0.3
    -- alpha 必须在 [0,1];配错(如把 0.3 写成 30)会算出负/乱 EWMA → 限流静默失效
    if opts.ttft_ewma_alpha < 0 or opts.ttft_ewma_alpha > 1 then
        ngx.log(ngx.ERR, "[", name, "] ttft_ewma_alpha=", opts.ttft_ewma_alpha,
                " out of [0,1] — clamping")
        opts.ttft_ewma_alpha = math.max(0, math.min(1, opts.ttft_ewma_alpha))
    end
    opts.ttft_ttl                  = opts.ttft_ttl                  or 60
    opts.ttft_probe_window         = opts.ttft_probe_window         or 10
    opts.ttft_probe_per_window     = opts.ttft_probe_per_window     or 5
    opts.ttft_window               = opts.ttft_window               or _G.TTFT_WINDOW
    -- ── TPS 限流(解码速率,按路由 opt-in)默认值 ──
    -- tps_limit_tps 默认 nil(=opt-in:没配则 tps_dict_if_on 返 nil → 全链路短路,零行为变化);
    -- dict 默认共享 "tps_stat";其余节奏参数对位 TTFT。tps_min_tokens 滤短响应(decode_time≈0 噪声)。
    opts.tps_dict                  = opts.tps_dict                  or "tps_stat"
    opts.tps_limit_tps             = opts.tps_limit_tps             or _G.TPS_LIMIT_TPS
    opts.tps_ewma_alpha            = opts.tps_ewma_alpha            or 0.3
    if opts.tps_ewma_alpha < 0 or opts.tps_ewma_alpha > 1 then
        ngx.log(ngx.ERR, "[", name, "] tps_ewma_alpha=", opts.tps_ewma_alpha,
                " out of [0,1] — clamping")
        opts.tps_ewma_alpha = math.max(0, math.min(1, opts.tps_ewma_alpha))
    end
    opts.tps_ttl                   = opts.tps_ttl                   or 60
    opts.tps_probe_window          = opts.tps_probe_window          or 10
    opts.tps_probe_per_window      = opts.tps_probe_per_window      or 5
    opts.tps_window                = opts.tps_window                or _G.TPS_WINDOW
    opts.tps_min_tokens            = opts.tps_min_tokens            or 16
    opts.tps_min_decode_s          = opts.tps_min_decode_s          or 0.5
    -- ── 规则化请求拒绝(reject_rules,按请求内容匹配 → 可配 status,默认 429)默认值 ──
    -- opts.reject_rules 为 nil 时天然 opt-in(该路由不启用);配了则由 validate_reject_rules 校验+归一
    --   (丢弃非法规则、补 name/status 默认),存回 opts.reject_rules。命中逻辑见 lua/reject_rules.lua。
    -- 热切开关复用 cch_ctl_dict 的 "reject_rules_enabled"(见 eval_reject_rules / dbg_reject_rules_toggle)。
    opts.reject_rules_status       = opts.reject_rules_status       or _G.REJECT_RULES_DEFAULT_STATUS or 429
    if type(opts.reject_rules_status) ~= "number" or opts.reject_rules_status < 400 or opts.reject_rules_status > 599 then
        ngx.log(ngx.ERR, "[", name, "] reject_rules_status=", tostring(opts.reject_rules_status), " 非法(需 400-599)— 回退 429")
        opts.reject_rules_status = 429
    end
    if opts.reject_rules_default_enabled == nil then opts.reject_rules_default_enabled = _G.REJECT_RULES_DEFAULT_ENABLED end
    opts.reject_rules              = reject_rules.validate_reject_rules(name, opts.reject_rules)
    -- ── 自适应并发(AIMD;配 tps_limit_tps 默认开,与 TPS 硬熔断互斥)默认值 ──
    -- adaptive_cc=true 时:复用 TPS EWMA 当反馈信号,每 adaptive_cc_interval 调一次池并发上限——
    --   EWMA < 阈值(tps_limit_tps/by_model/override)→ ×dec(减);>= → ×inc(增);clamp 在 [min,静态max]。
    --   max=运行时静态 limit(不配);min 不配则按 model 从静态 max 派生(×min_frac)。开了则跳过 TPS-429。
    -- 默认模式:配了 tps_limit_tps 且未显式指定 adaptive_cc → 按全局 _G.ADAPTIVE_CC_DEFAULT 定(默认 true=自适应)。
    -- 显式 adaptive_cc=false → 硬熔断;显式 true → 自适应。没配 tps_limit_tps → 保持 nil(无 tps 限流)。
    if opts.adaptive_cc == nil and opts.tps_limit_tps then
        opts.adaptive_cc = _G.ADAPTIVE_CC_DEFAULT and true or nil
    elseif opts.adaptive_cc == false then
        opts.adaptive_cc = nil                                       -- 显式关 = 走硬熔断(与"未开"同路径)
    end
    opts.adaptive_cc_min           = opts.adaptive_cc_min           -- 可选下限;不配 = 按 min_frac 从静态 max 派生
    opts.adaptive_cc_min_frac      = opts.adaptive_cc_min_frac      or _G.ADAPTIVE_CC_MIN_FRAC  -- min 缺省 = 静态max×frac(≥1)
    opts.adaptive_cc_interval      = opts.adaptive_cc_interval      or opts.tps_window  -- AIMD 步长,默认=tps_window
    opts.adaptive_cc_dec           = opts.adaptive_cc_dec           or _G.ADAPTIVE_CC_DEC  -- EWMA<阈值 → ×dec(减)
    opts.adaptive_cc_inc           = opts.adaptive_cc_inc           or _G.ADAPTIVE_CC_INC  -- EWMA>=阈值 → ×inc(增)
    -- adaptive_cc 值的 TTL:长时间无 EWMA 信号(过期)→ 值老化消失 → 回退到 min(慢启动,不从满容量开始)。
    -- 全局默认 _G.ADAPTIVE_CC_TTL(300s/5min):短暂信号缺口内保持,持续无信号才复位到 min。
    opts.adaptive_cc_ttl           = opts.adaptive_cc_ttl           or _G.ADAPTIVE_CC_TTL
    -- 爬升压力系数:cc 只在 当前并发 >= cc×frac(顶到边缘/在造成429)时才 ×inc(防轻流量跑飞)
    opts.adaptive_cc_pressure_frac = opts.adaptive_cc_pressure_frac  or _G.ADAPTIVE_CC_PRESSURE_FRAC
    -- 空闲缩系数:并发 < cc×slack_frac(余量太大)→ cc 也往下缩(补忙→闲 gap)
    opts.adaptive_cc_slack_frac    = opts.adaptive_cc_slack_frac     or _G.ADAPTIVE_CC_SLACK_FRAC
    -- 绝对头寸(slots):相对系数在小并发下带宽太窄,再兜一个绝对头寸(见全局 _G.ADAPTIVE_CC_ABS 注释)
    if opts.adaptive_cc_abs == nil then opts.adaptive_cc_abs = _G.ADAPTIVE_CC_ABS end
    if opts.adaptive_cc then
        if not opts.tps_limit_tps then
            ngx.log(ngx.ERR, "[", name, "] adaptive_cc=true 但缺 tps_limit_tps(自适应要它当阈值+开 TPS 测量)"
                    .. " — 禁用自适应并发")
            opts.adaptive_cc = nil
        else
            -- min_frac 必须 0<frac≤1(>1 会让派生 min 越界 → mincc 钳到 max → 自适应静默变 no-op;
            -- ≤0 让 floor=1 失去保护)。非法回落全局默认,不静默生效。
            if type(opts.adaptive_cc_min_frac) ~= "number"
               or opts.adaptive_cc_min_frac <= 0 or opts.adaptive_cc_min_frac > 1 then
                ngx.log(ngx.ERR, "[", name, "] adaptive_cc_min_frac=", tostring(opts.adaptive_cc_min_frac),
                        " 非法(需 0<frac≤1)— 回落全局默认 ", tostring(_G.ADAPTIVE_CC_MIN_FRAC))
                opts.adaptive_cc_min_frac = _G.ADAPTIVE_CC_MIN_FRAC
            end
            -- pressure_frac 必须 0<frac≤1(0/负 → 门永真 → 静默退回无脑涨 no-op);非法回落全局默认
            if type(opts.adaptive_cc_pressure_frac) ~= "number"
               or opts.adaptive_cc_pressure_frac <= 0 or opts.adaptive_cc_pressure_frac > 1 then
                ngx.log(ngx.ERR, "[", name, "] adaptive_cc_pressure_frac=", tostring(opts.adaptive_cc_pressure_frac),
                        " 非法(需 0<frac≤1)— 回落全局默认 ", tostring(_G.ADAPTIVE_CC_PRESSURE_FRAC))
                opts.adaptive_cc_pressure_frac = _G.ADAPTIVE_CC_PRESSURE_FRAC
            end
            -- slack_frac 必须 0<frac<pressure_frac(留 [slack,pressure) 保持带防抖;≥pressure 会涨缩打架抖动)
            if type(opts.adaptive_cc_slack_frac) ~= "number"
               or opts.adaptive_cc_slack_frac <= 0 or opts.adaptive_cc_slack_frac >= opts.adaptive_cc_pressure_frac then
                -- 回落取 min(全局默认, pressure×0.8):保证 < pressure_frac(即使 pressure 被配得很低)
                local _sf = math.min(_G.ADAPTIVE_CC_SLACK_FRAC, opts.adaptive_cc_pressure_frac * 0.8)
                ngx.log(ngx.ERR, "[", name, "] adaptive_cc_slack_frac=", tostring(opts.adaptive_cc_slack_frac),
                        " 非法(需 0<frac<pressure_frac ", tostring(opts.adaptive_cc_pressure_frac), ")— 回落 ", tostring(_sf))
                opts.adaptive_cc_slack_frac = _sf
            end
            -- adaptive_cc_abs 必须是 ≥0 数字(负/非数 → 绝对头寸逻辑乱);非法回落全局默认
            if type(opts.adaptive_cc_abs) ~= "number" or opts.adaptive_cc_abs < 0 then
                ngx.log(ngx.ERR, "[", name, "] adaptive_cc_abs=", tostring(opts.adaptive_cc_abs),
                        " 非法(需 ≥0 数字)— 回落全局默认 ", tostring(_G.ADAPTIVE_CC_ABS))
                opts.adaptive_cc_abs = _G.ADAPTIVE_CC_ABS
            end
            if opts.adaptive_cc_min ~= nil then
                -- 下限必须 ≥1 的正整数(0.x 会让 limit<1 → 首个请求就 429,整路由挂),否则忽略回落派生
                if type(opts.adaptive_cc_min) ~= "number" or opts.adaptive_cc_min < 1 then
                    ngx.log(ngx.ERR, "[", name, "] adaptive_cc_min=", tostring(opts.adaptive_cc_min),
                            " 非法(需 ≥1)— 忽略,回落 min_frac 派生")
                    opts.adaptive_cc_min = nil
                else
                    opts.adaptive_cc_min = math.floor(opts.adaptive_cc_min)
                end
            end
        end
    end
    -- M2: dict 存在性检查（防新模型 conf 漏声明 lua_shared_dict 时调试困难）
    for _, fld in ipairs({"active_conns_dict","bad_peers_dict","lc_locks_dict",
                          "cluster_avg_dict","cch_ctl_dict","bodylog_ctl_dict","api_keys_dict"}) do
        local dname = opts[fld]
        if not ngx.shared[dname] then
            ngx.log(ngx.ERR, "[", name, "] register_route: required shared_dict '", dname,
                    "' (for opts.", fld, ") not declared — add `lua_shared_dict ", dname,
                    " <size>;` to this route's conf and reload")
        end
    end
    _G.__route_opts[name] = opts
    -- 启动 health / cluster_avg timer。
    --
    -- 时序约束：
    --   - lazy init 模式（K2.6）的 first request 可能命中任意 worker，不能用 worker.id()==0
    --     守卫（否则非 worker 0 触发时 timer 永远不启动）
    --   - reload 时新 worker 启动 init_worker → register_route → start_timers，但老 worker
    --     的 timer 还在续命 shared_dict 中的 timer_key，导致新 worker :add 失败
    --   - 老 worker graceful shutdown 后老 timer 死亡，timer_key TTL 后过期；此时若没人
    --     重试 :add，timer 就永久死亡
    --
    -- 方案：lock:add 失败时调度 wait-retry，定期检查 timer_key 是否过期 + 续命时间是否 stale
    --   - lock 持有者每 RENEW_SEC 续命
    --   - 其他 worker 每 RETRY_SEC 检查 last_alive，stale 则尝试抢锁
    local lock = ngx.shared[opts.lc_locks_dict]
    if lock then
        local timer_key = "__timer_alive_" .. name
        local TTL = 60        -- shared_dict key 过期时间
        local RENEW_SEC = 20  -- 续命周期（小于 TTL 一半）
        local RETRY_SEC = 30  -- 失败重试周期（建议 > RENEW_SEC）
        local start_timers
        start_timers = function(premature)
            if premature then return end
            if lock:add(timer_key, ngx.now(), TTL) then
                -- 抢到锁：本 worker 启 timer + 续命
                ngx.log(ngx.NOTICE, "[", name, "] timers started on worker ", ngx.worker.id())
                local function renew(p)
                    if p then return end
                    lock:set(timer_key, ngx.now(), TTL)
                    ngx.timer.at(RENEW_SEC, renew)
                end
                ngx.timer.at(RENEW_SEC, renew)
                timers = timers or require "timers"
                timers.do_health_check_loop(opts)
                timers.do_cluster_avg_loop(opts)
                timers.do_adaptive_cc_loop(opts)   -- 内部按 opts.adaptive_cc 自门控(未 opt-in 立即 return)
            else
                -- 锁被其他 worker 持有（含老 worker 续命）；周期检查 + 必要时抢锁
                -- 这保证 reload 后老 timer 死亡 + key TTL 过期后，新 worker 能接管
                local function check_and_retry(p)
                    if p then return end
                    local last = lock:get(timer_key)
                    -- key 过期 (last=nil) 或 stale (距上次续命 > 2*RENEW) → 尝试抢锁
                    if (not last) or (ngx.now() - last > RENEW_SEC * 2) then
                        return start_timers(false)
                    end
                    ngx.timer.at(RETRY_SEC, check_and_retry)
                end
                ngx.timer.at(RETRY_SEC, check_and_retry)
            end
        end
        ngx.timer.at(0, start_timers)
    else
        ngx.log(ngx.ERR, "[", name, "] cannot start timers: lc_locks_dict missing")
    end
    ngx.log(ngx.NOTICE, "[", name, "] route registered (", #opts.peers, " peers)")
    return opts
end

-- ══════════════════════════════════════════════════════════════════════
-- M.serve_models(opts) — 新格式聚合路由的 GET /v1/models：openresty 直接列出
-- 配置的 model id(vllm 兼容格式,只含 id)。仅 do_route 在 peers_by_model 路由 +
-- uri==/v1/models 时调用(已在 do_route 鉴权之后,自带鉴权)。老格式不调此函数。
-- ══════════════════════════════════════════════════════════════════════
function M.serve_models(opts)
    local ids = {}
    for k in pairs(opts.peers_by_model or {}) do ids[#ids + 1] = k end
    table.sort(ids)
    local data = {}
    for _, id in ipairs(ids) do data[#data + 1] = { id = id, object = "model" } end
    if #data == 0 then data = cjson.empty_array end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({ object = "list", data = data }))
    return ngx.exit(ngx.HTTP_OK)
end

-- ══════════════════════════════════════════════════════════════════════
-- M.resolve_pool(opts, model) — 解析有效 peer 池
--   新格式 peers_by_model：按 model 选子池;命中返 (peers, peer_keys);
--     未知/缺失 model 返 (nil, nil, supported_list)
--   老格式：返 (opts.peers, opts.peer_keys)
-- do_route 和 dbg_* 共用,保证选池逻辑一致(改一处即可)。
-- ══════════════════════════════════════════════════════════════════════
function M.resolve_pool(opts, model)
    if not opts.peers_by_model then
        return opts.peers, opts.peer_keys
    end
    local pool = model and opts.peers_by_model[model]
    if pool then return pool, opts.peer_keys_by_model[model] end
    local sup = {}
    for k in pairs(opts.peers_by_model) do sup[#sup + 1] = k end
    return nil, nil, sup
end

-- ══════════════════════════════════════════════════════════════════════
-- M.assess_pool(opts, peers, peer_keys) — 只读评估(无锁、无 pick、无副作用):
-- 算 healthy_all / 优先级活跃层 healthy_peers / 容量 limit,rt_sum,avg_5min。
-- 全 banned 时返 {empty=true}。do_route 用它先做 503/429 判定(锁不上 429 路径);
-- dbg 也用它。改一处即可,选址逻辑全共享。
-- ══════════════════════════════════════════════════════════════════════
function M.assess_pool(opts, peers, peer_keys)
    local bad  = ngx.shared[opts.bad_peers_dict]
    local dict = ngx.shared[opts.active_conns_dict]
    local default_max = opts.default_max

    local healthy_all = {}   -- {ip, port, orig_idx, "host:port", priority, max}
    for i, p in ipairs(peers) do
        local k = peer_keys[i]
        if not bad:get(k) then
            healthy_all[#healthy_all + 1] =
                {p[1], p[2], i, k, tonumber(p[4]) or 0, tonumber(p[5]) or default_max}
        end
    end
    -- tps_on=false 保持与非空 return 形状一致(do_route 现由 a.empty 503 短路先挡,不读到这里;
    -- 但若未来重排短路,a.tps_on 有确定值而非 nil,避免 `opts.adaptive_cc and a.tps_on` 静默吞掉全 ban)
    if #healthy_all == 0 then return { empty = true, healthy_all = healthy_all, tps_on = false } end

    local active_level = -math.huge
    for _, hp in ipairs(healthy_all) do if hp[5] > active_level then active_level = hp[5] end end
    local healthy_peers = {}
    for _, hp in ipairs(healthy_all) do
        if hp[5] == active_level then healthy_peers[#healthy_peers + 1] = hp end
    end

    local cap_by_prio = {}
    for _, hp in ipairs(healthy_all) do cap_by_prio[hp[5]] = (cap_by_prio[hp[5]] or 0) + hp[6] end
    local limit = 0
    for _, c in pairs(cap_by_prio) do if c > limit then limit = c end end
    -- rt_sum = 真实总在途并发,对**所有 peer(含 banned)求和**,与 /_active_conns(monitor)口径一致。
    -- 为什么含 banned(2026-07-03):banned 只是"不再往它发新流量"的路由决策,不代表它没在途负载——
    -- 尤其转发型 peer(如 CART router 127.0.0.1:8070 fan-out 到同一批 b300),被 ban 后残留的长流仍
    -- 实打实压在下游 b300 上。计数本身 ban 无关:incr 在选址阶段、decr 在 log 阶段(do_log_release,
    -- 用 admit 时存的 peer_counter_key),banned peer 的在途流结束照样减 → 计数准、非僵尸。漏算它 →
    -- gate 低估真实负载 → drain 期间过量 admit → 下游过载降速。故 rt_sum 含 banned 更正确。
    -- 注:limit(容量)仍只算 healthy(不能往 banned 发新流量);两者不对称是有意的——
    -- drain 期 rt_sum>limit → 诚实甩负载(下游确实满),换掉"悄悄超发+降速"。
    -- 无 banned 时全 peer == healthy_all,与旧行为字节等价。
    local rt_sum = 0
    for _, k in ipairs(peer_keys) do rt_sum = rt_sum + (dict:get(k) or 0) end
    local _, _, avg_5min = M.compute_cluster_avg(opts.cluster_avg_dict,
        opts.peers_by_model and ngx.ctx.req_model or nil)

    -- TTFT 违约判定(池级,按 model 分 key;总开关关 / dict 未声明 = nil = 不参与判定)。
    -- ttft_assess 遍历**声明的**指标列表做 OR;单指标(P0 默认)时 ttft_ewma 与旧行为逐字节一致。
    local ttft_ewma, ttft_hit
    local td = ttft.ttft_dict_if_on(opts)
    if td then
        ttft_hit  = ttft.ttft_assess(opts, td)
        ttft_ewma = ttft_hit.ewma
    end

    -- TPS 违约判定(解码速率,opt-in;特性关/无数据 = nil = 不参与判定,fail-open)
    -- strict=false → 用 `<=`,与改动前 access.lua 的比较符一致(timers 的 AIMD 另用 `<`,见 tps_assess)
    local tps_ewma, tps_hit
    local tpd = tps.tps_dict_if_on(opts)
    if tpd then
        tps_hit  = tps.tps_assess(opts, tpd, nil, false)
        tps_ewma = tps_hit.ewma
    end

    -- 自适应并发上限(AIMD;do_adaptive_cc_loop 每 interval 写入,带 TTL)。经 tps_dict_if_on 读:
    -- __off / _G.TPS_ENABLED 是**所有 tps 限流的统一开关**——关它则 tpd=nil → adaptive_cc=nil 且 tps_on=false,
    -- do_route 回退 pool_limit(统一关 = 回满容量,零滞后)。特性开(tps_on=true)但 cc=nil(首次/未初始化/
    -- 无信号 TTL 过期)→ do_route 回退到 min 慢启动。两种 nil 由 tps_on 区分(见 do_route)。
    local adaptive_cc, tps_prefix
    if tpd and opts.adaptive_cc then
        tps_prefix  = tps.tps_key_prefix(opts)           -- 算一次,do_route stash rt_sum 复用(免热路径重算)
        adaptive_cc = tpd:get(tps_prefix .. "adaptive_cc")
    end

    return {
        empty = false, active_level = active_level,
        healthy_all = healthy_all, healthy_peers = healthy_peers,
        limit = limit, rt_sum = rt_sum, avg_5min = avg_5min,
        ttft_ewma = ttft_ewma, tps_ewma = tps_ewma, adaptive_cc = adaptive_cc,
        ttft_hit = ttft_hit, tps_hit = tps_hit,   -- 多指标判定结果(含触发那一条的 ewma/limit/metric)
        tps_prefix = tps_prefix,  -- tps_key_prefix(opts) 缓存(仅 adaptive+tps_on 时非 nil)
        tps_on = (tpd ~= nil),   -- tps 特性对本路由是否生效(__off/_G.TPS_ENABLED/未 opt-in = false)
    }
end

-- ══════════════════════════════════════════════════════════════════════
-- M.compute_static_max_cc(opts, model) — 池标称容量(=静态 max 上限,无 ban 感知)。
-- 镜像 assess_pool 的 limit 算法(按优先级层 sum per-peer max、取层间最大),但从 opts 静态
-- peer 表算(不看 bad_peers)→ 供 do_adaptive_cc_loop 当 AIMD 的 max clamp。peers_by_model
-- 时 model 指定子池(flat 路由传 nil/false 用 opts.peers)。
-- ══════════════════════════════════════════════════════════════════════
function M.compute_static_max_cc(opts, model)
    local peers
    if opts.peers_by_model then
        peers = model and opts.peers_by_model[model] or nil
    else
        peers = opts.peers
    end
    if not peers or #peers == 0 then return opts.default_max or 0 end
    local default_max = opts.default_max
    local cap_by_prio = {}
    for _, p in ipairs(peers) do
        local prio = tonumber(p[4]) or 0
        cap_by_prio[prio] = (cap_by_prio[prio] or 0) + (tonumber(p[5]) or default_max)
    end
    local maxcc = 0
    for _, c in pairs(cap_by_prio) do if c > maxcc then maxcc = c end end
    return maxcc
end

-- ══════════════════════════════════════════════════════════════════════
-- M.derive_mincc(opts, maxcc) — AIMD 下限(慢启动 floor + clamp band 下界)。
-- 显式配 adaptive_cc_min 优先,否则从**静态** maxcc 派生(×min_frac,≥1),再钳到 maxcc。
-- 统一 3 处调用(do_route / do_adaptive_cc_loop / dbg_tps_status)同一 base(静态 maxcc),
-- 保证「报告的 min」==「强制的 min」——do_route 拿到后再用 min(., pool_limit) 做 ban 感知封顶,
-- 不在 base 里掺 ban(否则 dbg 报的 floor 与实际强制的 floor 在 peer-ban 下会分叉)。
-- ══════════════════════════════════════════════════════════════════════
function M.derive_mincc(opts, maxcc)
    local mn = opts.adaptive_cc_min or math.max(1, math.floor(maxcc * opts.adaptive_cc_min_frac))
    if mn > maxcc then mn = maxcc end     -- 配 min>max(或 frac>1 派生越界)→ 生效 min=max
    return mn
end

-- ══════════════════════════════════════════════════════════════════════
-- M.pick_from(opts, a, sid) — 在 assess_pool 结果 a 上做真正选址(带锁)。
-- 仅在 caller 已确认不 503/429 后调用 → least_conn 锁不会上过载路径。
-- 返回 (chosen_hp, mode, hash)。do_route 和 dbg 共用,选址算法逐字节一致。
-- ══════════════════════════════════════════════════════════════════════
function M.pick_from(opts, a, sid)
    local resty_lock = require "resty.lock"
    local dict = ngx.shared[opts.active_conns_dict]
    local healthy_peers = a.healthy_peers

    local function pick_least_conn_locked()
        local lock = resty_lock:new(opts.lc_locks_dict, {timeout = 0.5, exptime = 2})
        local locked = false
        if lock then
            local _, lerr = lock:lock("least_conn_pick")
            if not lerr then locked = true
            else ngx.log(ngx.WARN, "lc_lock acquire failed: ", lerr) end
        end
        local min_ratio = math.huge
        local min_hp = healthy_peers[1]
        -- 平局随机化(蓄水池抽样 size=1):严格 `<` 会让并列最小者恒选列表第一个 ——
        -- 低并发/冷启动下所有 peer 活跃连接≈0、ratio 全并列 → least-conn 退化成永远指向
        -- healthy_peers[1](A、B 请求都落同一后端,其余空转)。改为遇到第 k 个并列者以 1/tie_count
        -- 概率顶替 → 每个并列 peer 被选中概率均为 1/tie_count(均匀),单趟 O(1) 打散热点。
        local tie_count = 0
        for _, hp in ipairs(healthy_peers) do
            local ratio = (dict:get(hp[4]) or 0) / hp[6]
            if ratio < min_ratio then
                min_ratio = ratio; min_hp = hp; tie_count = 1
            elseif ratio == min_ratio then
                tie_count = tie_count + 1
                if math.random() < 1 / tie_count then min_hp = hp end
            end
        end
        if locked then lock:unlock() end
        return min_hp
    end

    if util._nonblank(sid) then
        local best_idx, h = util.pick_rendezvous(sid, healthy_peers)
        local best_hp = healthy_peers[best_idx]
        local hashed_active = dict:get(best_hp[4]) or 0
        if hashed_active < best_hp[6] then
            return best_hp, "hash", h
        end
        ngx.log(ngx.INFO, "[", opts.route_name, "] hash peer ", best_hp[4],
                " saturated (", hashed_active, "/", best_hp[6], "), fallback to least_conn")
        return pick_least_conn_locked(), "hash_fallback", h
    end
    return pick_least_conn_locked(), "least_conn", nil
end

-- session 亲和性总开关裁决:affinity 关闭时丢弃已提取的 sid → 该请求走 least_conn 打散。
-- opts.session_affinity_enabled 已在 register_route 里回落好(nil→_G.SESSION_AFFINITY_ENABLED),
-- 故这里只看 opts。返回 (sid, src);关闭且原本有 sid 时返回 (nil, "affinity_off") 便于日志观测。
-- do_route 与两个 dbg endpoint 共用此裁决,保证真实路由与调试输出一致。
function M.affinity_gate(opts, sid, src)
    if sid and opts and opts.session_affinity_enabled == false then
        return nil, "affinity_off"
    end
    return sid, src
end

return M
