-- openresty/lua/access.lua
-- do_route / do_log_release / do_balancer

local cjson = require "cjson.safe"

-- ══════════════════════════════════════════════════════════════════════
-- _G.do_route(opts) — access_by_lua_block 主体（参数化路由选址）
-- ══════════════════════════════════════════════════════════════════════

function _G.do_route(opts)
    -- H1: nil opts 防御（register_route 失败 / set $route 名不匹配时返清晰 500，而非 nil index 崩）
    if _G.opts_missing(opts) then
        ngx.log(ngx.ERR, "do_route: opts is nil — route not registered? check register_route factory / set $route")
        return ngx.exit(500)
    end
    -- ── API key 鉴权（从 Bearer 头取）──
    local ak = ngx.shared[opts.api_keys_dict]
    if not ak:get("__inited") then
        for k, v in pairs(opts.api_keys) do ak:set(k, v) end
        ak:set("__inited", "1")
    end
    local auth = ngx.req.get_headers()["authorization"] or ""
    local akey = auth:match("^Bearer%s+(.+)$")
    if not akey or not ak:get(akey) then
        ngx.status = 401
        ngx.header["Content-Type"] = "application/json"
        ngx.say([[{"error":"missing or invalid api key"}]])
        return ngx.exit(401)
    end

    -- 新格式聚合路由:GET /v1/models 由 openresty 直接列出 model id。
    -- 门控在 peers_by_model → 老格式(flat peers)不拦截,继续往下 proxy 到后端,
    -- /v1/models 行为完全不变。放在鉴权之后 → 自带鉴权。
    if opts.peers_by_model and ngx.var.uri == "/v1/models" then
        return _G.serve_models(opts)
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
    _G.bodylog_capture_request(opts)
    local sid, src = prepare_request(opts)
    sid, src = _G.affinity_gate(opts, sid, src)   -- 亲和性总开关:关则丢 sid → least_conn
    ngx.ctx.session_id     = sid
    ngx.ctx.session_source = src or "none"

    -- ── 规则化请求拒绝(reject_rules)──
    -- 按请求内容(max_tokens/stream/input_bytes/...)匹配规则,命中即返回可配 status(默认 429)。
    -- 放在池评估之前:规则拒绝不该消耗池评估。命中时 eval_reject_rules 内部 ngx.exit 直接结束请求;
    -- 未配 reject_rules 的路由 no-op(零行为变化)。探测请求(/v1/models、/health)不评估。
    if not is_probe then _G.eval_reject_rules(opts) end

    -- 新格式 peers_by_model：按 body.model 选子池，重绑 peers/peer_keys。
    -- 未知/缺失 model → 400 + supported 列表（严格拒绝，不 fallback）。
    -- 老格式（无 peers_by_model）跳过，peers 仍是 opts.peers，行为不变。
    -- 注:GET /v1/models 已在上方(鉴权后)由 _G.serve_models 早返,不会到这里;故列模型不受此 400 影响。
    if opts.peers_by_model then
        local pk
        peers, peer_keys, pk = _G.resolve_pool(opts, ngx.ctx.req_model)
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
    local a = _G.assess_pool(opts, peers, peer_keys)
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
        local mincc = _G.derive_mincc(opts, _G.compute_static_max_cc(opts,
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
    local ttft_limit = _G.ttft_limit_for(opts)   -- 按模型解析(peers_by_model 可每模型不同)
    local hit_ttft = ttft_limit and a.ttft_ewma and a.ttft_ewma >= ttft_limit
    if hit_ttft and not is_probe and not _G.ttft_allow_probe(opts) then
        ngx.status = 429
        do local rj=ngx.shared.reject_stat; if rj then rj:incr((opts.route_name or "-")..":ttft",1,0) end end
        ngx.header["Content-Type"] = "application/json"
        ngx.header["Retry-After"]  = "1"
        ngx.say(string.format(
            [[{"error":"ttft limit exceeded","trigger":"ttft_ewma","ttft_ewma":%.1f,"ttft_limit":%d,"model":"%s","healthy_peers":%d,"active_level":%d,"route":"%s"}]],
            a.ttft_ewma, ttft_limit, ngx.ctx.req_model or "-", #a.healthy_peers, active_level, opts.route_name))
        return ngx.exit(429)
    end
    -- ── TPS 主限流(opt-in:tps_limit_tps 未配 → tps_dict_if_on nil → a.tps_ewma nil → 跳过)──
    -- 方向与 TTFT 相反:EWMA <= 下限 进入限流态(解码速率太低=后端过载),半开探测机制同 TTFT。
    -- ⚠️ 与自适应并发互斥:adaptive_cc=true 时该 EWMA 已用于动态调 limit(上面并发 gate),这里跳过硬 429。
    local tps_limit = (not opts.adaptive_cc) and _G.tps_limit_for(opts) or nil
    local hit_tps = tps_limit and a.tps_ewma and a.tps_ewma <= tps_limit
    if hit_tps and not is_probe and not _G.tps_allow_probe(opts) then
        ngx.status = 429
        do local rj=ngx.shared.reject_stat; if rj then rj:incr((opts.route_name or "-")..":tps",1,0) end end
        ngx.header["Content-Type"] = "application/json"
        ngx.header["Retry-After"]  = "1"
        ngx.say(string.format(
            [[{"error":"tps limit exceeded","trigger":"tps_ewma","tps_ewma":%.1f,"tps_limit":%.1f,"model":"%s","healthy_peers":%d,"active_level":%d,"route":"%s"}]],
            a.tps_ewma, tps_limit, ngx.ctx.req_model or "-", #a.healthy_peers, active_level, opts.route_name))
        return ngx.exit(429)
    end
    -- 通过容量后才做带锁选址(与 dbg 共用 pick_from)
    local chosen_hp, mode = _G.pick_from(opts, a, sid)
    local peer_key  = chosen_hp[4] or (chosen_hp[1] .. ":" .. chosen_hp[2])
    dict:incr(peer_key, 1, 0)

    ngx.ctx.chosen_host      = chosen_hp[1]
    ngx.ctx.chosen_port      = chosen_hp[2]
    ngx.ctx.peer_counter_key = peer_key
    ngx.ctx.routed_peer      = peer_key
    ngx.ctx.routed_mode      = mode
    ngx.var.routed_mode = mode
    ngx.var.routed_peer = peer_key

    -- fallback peers（仅同活跃层，active 升序）：跨层 fallback 已废，
    -- router 饱和/连接失败不再溢出到低优层（低优层只在高优全 banned、active_level 降级时才接管）
    local fallback = {}
    local rest = {}
    for _, hp in ipairs(a.healthy_peers) do
        local k = hp[4]
        if k ~= peer_key then
            rest[#rest + 1] = {{hp[1], hp[2]}, dict:get(k) or 0, hp[5]}
        end
    end
    table.sort(rest, function(a, b)
        if a[3] ~= b[3] then return a[3] > b[3] end
        return a[2] < b[2]
    end)
    for _, r in ipairs(rest) do
        fallback[#fallback + 1] = r[1]
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
    local td = _G.ttft_dict_if_on(opts)
    if td and ngx.ctx.ttft_is_stream and ngx.ctx.ttft_first_chunk_t
       and ngx.status and ngx.status >= 200 and ngx.status < 300 then
        _G.ttft_record(opts, td, ngx.ctx.ttft_first_chunk_t * 1000)   -- 秒 → 毫秒
    end
    -- TPS 入账:只采流式 + 2xx + 有首-chunk 计时。从尾缓冲 parse completion_tokens;
    -- 拿不到(no-usage)→ fail-open:不采样、不进 EWMA、绝不因此限流(只 incr nousage 计数做可观测)。
    local tpd = _G.tps_dict_if_on(opts)
    if tpd and ngx.ctx.ttft_is_stream and ngx.ctx.ttft_first_chunk_t
       and ngx.status and ngx.status >= 200 and ngx.status < 300 then
        local ctok
        local tail = ngx.ctx.tps_tail
        -- 取最后一个匹配:usage chunk 永远在流末尾,而响应正文里(如 tool-call 回显的 JSON)
        -- 可能更早出现字面量 "completion_tokens",match 取最左会误命中,gmatch 迭代留最后一个才稳。
        if tail then
            for n in tail:gmatch('"completion_tokens"%s*:%s*(%d+)') do ctok = tonumber(n) end
        end
        if ctok and ctok >= opts.tps_min_tokens then
            ngx.update_time()
            local decode = (ngx.now() - ngx.req.start_time()) - ngx.ctx.ttft_first_chunk_t
            -- 解码时间下限:< tps_min_decode_s 的短快响应(如 16 token / 3ms → 5000 tok/s)解码速率
            -- 失真,不能代表稳态吞吐,直接跳过(不采样、不计 nousage)。配合 tps_min_tokens 双重滤噪。
            if decode >= opts.tps_min_decode_s then
                _G.tps_record(opts, tpd, ctok / decode)   -- tokens / 解码秒数
            end
        elseif not ctok then
            tpd:incr(_G.tps_key_prefix(opts) .. "nousage", 1, 0, opts.tps_ttl)  -- 可观测:无 usage 样本数
        end
    end
    _G.bodylog_finalize(opts)
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
            balancer.set_more_tries(math.min(2, #fb))
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
        end
        local ok, err = balancer.set_current_peer(new_peer[1], new_peer[2])
        if not ok then
            ngx.log(ngx.ERR, "set_current_peer retry failed: ", err)
        end
        ngx.log(ngx.WARN, "[", opts and opts.route_name or "?", "] balancer retry #", attempt,
                " new=", new_peer[1], ":", new_peer[2])
    end
end
