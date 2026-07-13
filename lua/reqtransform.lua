-- openresty/lua/reqtransform.lua
-- sid 提取 from_metadata + cch-strip + kimi tool_id 规范化 + hoist system

local cjson = require "cjson.safe"
local _nonblank = _G._nonblank

local function from_metadata(md, keyname)
    if type(md) ~= "table" then return nil end
    local prefix = "body." .. keyname

    -- 3. metadata.session_id
    if _nonblank(md.session_id) then
        return tostring(md.session_id), prefix .. ".session_id"
    end

    -- 4/5/6. metadata.user_id fallback (Claude CLI convention)
    local uid = md.user_id
    if type(uid) == "table" then
        if _nonblank(uid.session_id) then
            return tostring(uid.session_id), prefix .. ".user_id.session_id"
        end
    elseif type(uid) == "string" and _nonblank(uid) then
        local parsed = cjson.decode(uid)
        if type(parsed) == "table" and _nonblank(parsed.session_id) then
            return tostring(parsed.session_id),
                   prefix .. ".user_id[json].session_id"
        end
        -- plain string user_id used directly
        return uid, prefix .. ".user_id[plain]"
    end
    return nil
end

-- 剥掉 Claude Code 注入到 system prompt 前缀的
-- "x-anthropic-billing-header: cc_version=...; cc_entrypoint=...; cch=XXXX;"
-- 这串每次请求 cch hash 都不同，会让 vllm prefix cache 第一段直接失配。
-- 落点观察到两处：(a) Anthropic 原生 top-level system 字段
--                (b) OpenAI 格式 messages[0].content（system role 在第 0 条）
local CCH_RE = [[^x-anthropic-billing-header:\s*cc_version=[^;]*;\s*cc_entrypoint=[^;]*;\s*cch=[^;]*;]]

local function _strip_cch(s)
    if type(s) ~= "string" then return s, false end
    local new, n, err = ngx.re.sub(s, CCH_RE, "", "jo")
    if not new then return s, false end
    return new, (n and n > 0)
end

function _G.strip_cch_in_req(req)
    local changed = false
    -- (a) Anthropic 原生：top-level system，可能是 string 或 [{type=text,text=...}]
    local sys = req.system
    if type(sys) == "string" then
        local nv, ch = _strip_cch(sys); if ch then req.system = nv; changed = true end
    elseif type(sys) == "table" then
        for _, p in ipairs(sys) do
            if type(p) == "table" and type(p.text) == "string" then
                local nv, ch = _strip_cch(p.text); if ch then p.text = nv; changed = true end
            end
        end
    end
    -- (b) OpenAI 格式：messages[0].content（cch 实测落在第一条 message 开头）
    local msgs = req.messages
    if type(msgs) == "table" then
        local m = msgs[1]
        if type(m) == "table" then
            if type(m.content) == "string" then
                local nv, ch = _strip_cch(m.content); if ch then m.content = nv; changed = true end
            elseif type(m.content) == "table" then
                for _, p in ipairs(m.content) do
                    if type(p) == "table" and type(p.text) == "string" then
                        local nv, ch = _strip_cch(p.text); if ch then p.text = nv; changed = true end
                    end
                end
            end
        end
    end
    return changed
end

-- ── Kimi tool_call_id 规范化（默认关，灰度开）──
-- OpenAI 风格 id (call_xxx / Name:N / namen) 改写为 Kimi 内部期望的
-- functions.<fn_name>:<idx> 格式。修复"finish=stop 但 content 空 + raw
-- markers 没 parse" 类 bug（见 docs/openresty_normalize_plan.md）。
-- 同时把 assistant.content == cjson.null 改成 ""。
-- 切换：/_kimi_normalize_toggle?on=0|1 热切，写入 ngx.shared.cch_ctl
-- 复用同一 dict 的 kimi_normalize_enabled key，跨 reload 持久。
_G.KIMI_NORMALIZE_DEFAULT_ENABLED = true

-- 触发 normalize 的 model 白名单（兼容大小写、第三方 namespace）
_G.KIMI_NORMALIZE_MODELS = {
    ["kimi-k2.5"]                = true,
    ["kimi-k2.6"]                = true,
    ["Kimi-K2.5"]                = true,
    ["Kimi-K2.6"]                = true,
    ["moonshotai/kimi-k2.5"]     = true,
    ["moonshotai/kimi-k2.6"]     = true,
}

-- cjson.null 是 lightuserdata sentinel，require 一次拿引用（init phase 已 require 过 cjson.safe）
local _cjson_null = cjson.null

-- 把 req 里非 functions.* 的 tool_call_id 改写为 functions.<fn>:<idx>，
-- 同时同步 role=tool 的 tool_call_id 引用、assistant.content=null → ""。
-- 返回改动条目数（≥0）。req=非 table 或非 Kimi 模型时直接返 0 不动。
-- 内部用 pcall 兜底,异常时返 0,不让上游 access_by_lua 整体挂掉。
function _G.normalize_kimi_tool_ids(req)
    if type(req) ~= "table" then return 0 end
    if not _G.KIMI_NORMALIZE_MODELS[req.model] then return 0 end
    local msgs = req.messages
    if type(msgs) ~= "table" then return 0 end

    -- _nonblank 别名:过滤 nil/空/空白 id
    local nonblank = _G._nonblank

    local ok, changed = pcall(function()
        -- 第一遍：pre-scan 已存在的 functions.<fn>:<idx> ids，让 fn_next[fn] 从已用 max+1 起，
        -- 避免改写后撞已有 id。同时为非 functions.* id 分配新 id（functions.<fn>:<new_idx>）。
        local id_map = {}
        local fn_next = {}
        -- pre-scan: 找出每个 fn name 已被占用的最大 idx
        for _, m in ipairs(msgs) do
            if type(m) == "table" and type(m.tool_calls) == "table" then
                for _, tc in ipairs(m.tool_calls) do
                    local tid = type(tc) == "table" and tc.id
                    local fn  = type(tc) == "table" and type(tc["function"]) == "table"
                                and tc["function"].name
                    if type(tid) == "string" and nonblank(tid)
                       and type(fn) == "string" and nonblank(fn) then
                        -- 仅匹配安全 fn 名（[A-Za-z0-9_.-]）；其他全跳过避免 regex 注入
                        if fn:match("^[%w_%-%.]+$") then
                            local n = tid:match("^functions%." ..
                                fn:gsub("([%-%.])", "%%%1") .. ":(%d+)$")
                            if n then
                                local ni = tonumber(n)
                                if ni and (fn_next[fn] == nil or ni + 1 > fn_next[fn]) then
                                    fn_next[fn] = ni + 1
                                end
                            end
                        end
                    end
                end
            end
        end
        -- 分配新 id（避开已占用 idx）
        for _, m in ipairs(msgs) do
            if type(m) == "table" and type(m.tool_calls) == "table" then
                for _, tc in ipairs(m.tool_calls) do
                    local tid = type(tc) == "table" and tc.id
                    local fn  = type(tc) == "table" and type(tc["function"]) == "table"
                                and tc["function"].name
                    if type(tid) == "string" and nonblank(tid)
                       and type(fn) == "string" and nonblank(fn)
                       and fn:match("^[%w_%-%.]+$")
                       and not tid:find("^functions%.")
                       and not id_map[tid] then
                        local idx = fn_next[fn] or 0
                        id_map[tid] = string.format("functions.%s:%d", fn, idx)
                        fn_next[fn] = idx + 1
                    end
                end
            end
        end

        -- 第二遍：apply 改写。孤儿 tool.tool_call_id（map 里没有）保持原值不动。
        local n_changed = 0
        for _, m in ipairs(msgs) do
            if type(m) == "table" then
                if m.role == "assistant" then
                    -- content=null → ""（部分客户端这样发，会让 Kimi chat_template 渲染异常）
                    if m.content == _cjson_null then
                        m.content = ""
                        n_changed = n_changed + 1
                    end
                    if type(m.tool_calls) == "table" then
                        for _, tc in ipairs(m.tool_calls) do
                            if type(tc) == "table" and id_map[tc.id] then
                                tc.id = id_map[tc.id]
                                n_changed = n_changed + 1
                            end
                        end
                    end
                elseif m.role == "tool" then
                    if id_map[m.tool_call_id] then
                        m.tool_call_id = id_map[m.tool_call_id]
                        n_changed = n_changed + 1
                    end
                end
            end
        end
        return n_changed
    end)

    if not ok then
        ngx.log(ngx.ERR, "normalize_kimi_tool_ids pcall err: ", tostring(changed))
        return 0
    end
    return changed or 0
end

-- Anthropic /v1/messages 兼容：把 messages 里 role=system 的项抽进顶层 system。
-- 背景：Claude Code(agent 框架)除顶层 system 外,还额外往 messages 里塞一条 role:system
-- (装 "Available agent types" 那段)。真 Anthropic API 会容忍并 merge 进 system,但 sglang 的
-- anthropic-compat 端点严格校验 messages role 只能 user/assistant → 400。这里补上那步 merge。
-- content 支持 string 或 blocks;顶层 system 支持 string / list;返回移动条数。pcall 由调用方兜底。
function _G.hoist_system_msgs(req)
    local msgs = req.messages
    if type(msgs) ~= "table" then return 0 end
    local hoisted, kept, moved = {}, {}, 0
    for _, m in ipairs(msgs) do
        if type(m) == "table" and m.role == "system" then
            local c = m.content
            if type(c) == "string" then
                hoisted[#hoisted + 1] = { type = "text", text = c }
            elseif type(c) == "table" then
                for _, blk in ipairs(c) do
                    if type(blk) == "table" then
                        hoisted[#hoisted + 1] = blk
                    elseif type(blk) == "string" then
                        hoisted[#hoisted + 1] = { type = "text", text = blk }
                    end
                end
            end
            moved = moved + 1
        else
            kept[#kept + 1] = m
        end
    end
    if moved == 0 then return 0 end
    -- 顶层 system 归一成 list 再 append(原为 string 先包成一个 text block)
    local sys = req.system
    if type(sys) == "string" then
        sys = { { type = "text", text = sys } }
    elseif type(sys) ~= "table" then
        sys = {}
    end
    for _, blk in ipairs(hoisted) do sys[#sys + 1] = blk end
    req.system = sys
    req.messages = kept
    return moved
end

-- 路由前置：读 body → 剥 cch → 提取 sid。即使 sid 在 header 里命中，
-- body 也要读 + 剥（cch 命中率比 fast-path 节省的 read_body 重要得多）。
-- 返回 (sid, src)；sid 提取失败返回 nil 走 least_conn。
-- opts 可选：{ cch_ctl_dict, cch_default_enabled, disable_body_user_affinity }
-- 未传时取 ngx.ctx.route_opts；再为空取 K2.5 默认值（保持向后兼容老调用）。
function prepare_request(opts)
    opts = opts or ngx.ctx.route_opts or _G.__route_opts.k25
    local h = ngx.req.get_headers()

    -- 先从 header 取 sid（不能 return early，下面还要剥 body）
    local sid_from_header, src_from_header
    local hv = h["x-litellm-session-id"]
    if _nonblank(hv) then
        sid_from_header, src_from_header = tostring(hv), "header.x-litellm-session-id"
    else
        hv = h["x-claude-code-session-id"]
        if _nonblank(hv) then
            sid_from_header, src_from_header = tostring(hv), "header.x-claude-code-session-id"
        else
            hv = h["x-session-id"]
            if _nonblank(hv) then
                sid_from_header, src_from_header = tostring(hv), "header.x-session-id"
            end
        end
    end

    -- 必读 body：(1) 剥 cch；(2) header 没 sid 时回 body 找
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if not body or #body == 0 then
        local fname = ngx.req.get_body_file()
        if fname then
            local f = io.open(fname, "rb")
            if f then body = f:read("*a"); f:close() end
        end
    end

    -- 没 body：直接返回 header sid（若有）
    if not body or #body == 0 then
        return sid_from_header, src_from_header
    end

    local req = cjson.decode(body)
    if type(req) ~= "table" then
        return sid_from_header, src_from_header
    end

    -- model-keyed 路由(新格式 peers_by_model)用:从已 decode 的 body 取 model,
    -- 零额外成本。do_route 在 prepare_request 之后据此选子池;老格式不读此字段。
    ngx.ctx.req_model = req.model

    -- 剥 cch（受 cch_ctl 开关控制，默认开；toggle endpoint 热切，无需 reload）
    local cch_ctl = ngx.shared[opts.cch_ctl_dict or "cch_ctl"]
    local cch_default = opts.cch_default_enabled
    if cch_default == nil then cch_default = _G.CCH_STRIP_DEFAULT_ENABLED end
    local cch_enabled_raw = cch_ctl:get("enabled")
    local cch_enabled
    if cch_enabled_raw == nil then
        cch_enabled = cch_default
    else
        cch_enabled = (cch_enabled_raw == 1)
    end
    local req_dirty = false
    local cch_stripped_pending = false
    local kimi_normalized_pending = 0
    if cch_enabled then
        local ok_cch, did = pcall(_G.strip_cch_in_req, req)
        if not ok_cch then
            ngx.log(ngx.ERR, "strip_cch_in_req pcall err: ", tostring(did))
        elseif did then
            cch_stripped_pending = true
            req_dirty = true
        end
    end
    cch_ctl:incr("parsed_total", 1, 0)

    -- Kimi tool_call_id normalize（受 cch_ctl 开关 kimi_normalize_enabled 控制）
    -- 见 _G.normalize_kimi_tool_ids 注释；只对 KIMI_NORMALIZE_MODELS 命中模型生效。
    -- 优先级：dict 热切值(/_kimi_normalize_toggle) > per-route opts.kimi_normalize_default_enabled。
    -- opts.kimi_normalize_default_enabled 由 register_route 保证非 nil(factory 未配则已回落全局
    -- _G.KIMI_NORMALIZE_DEFAULT_ENABLED,见 route.lua),故此处不再重复回落全局。
    -- factory 显式设 false = 该路由默认关;master 重启清 dict 后回落此默认(热切仍可运行时临时覆盖)。
    local kn_raw = cch_ctl:get("kimi_normalize_enabled")
    local kn_enabled
    if kn_raw == nil then
        kn_enabled = opts.kimi_normalize_default_enabled
    else
        kn_enabled = (kn_raw == 1)
    end
    if kn_enabled then
        -- normalize_kimi_tool_ids 内部已 pcall 兜底,返回 0 表示无改动或异常
        local n = _G.normalize_kimi_tool_ids(req)
        if n and n > 0 then
            kimi_normalized_pending = n
            req_dirty = true
        end
    end

    -- 新格式 peers_by_model：流式请求统一强制 stream_options.include_usage=true，
    -- 让 vllm/sglang 都稳定在末尾返回 usage chunk（统一计费/token 统计）。
    -- 无论客户端没传、还是显式设 false，都覆盖为 true；老格式不处理。
    if opts.peers_by_model and req.stream then
        local so = req.stream_options
        if type(so) ~= "table" then so = {}; req.stream_options = so end
        if so.include_usage ~= true then   -- 已是 true 则不重复改（省一次 encode）
            so.include_usage = true
            req_dirty = true
            ngx.ctx.usage_injected = true
        end
    end

    -- Anthropic /v1/messages 兼容:把 messages 里 role=system 的项抽进顶层 system(补真
    -- Anthropic API 会做的 merge,绕过 sglang 对 messages role 的严格校验)。仅 /v1/messages;
    -- OpenAI /v1/chat/completions 的 system-in-messages 合法,不动。pcall 兜底防挂。
    if ngx.var.uri == "/v1/messages" and type(req.messages) == "table" then
        local ok_h, moved = pcall(_G.hoist_system_msgs, req)
        if ok_h and moved and moved > 0 then req_dirty = true end
    end

    -- 一次性 set_body_data：cch_strip / kimi normalize / include_usage 任一改了就重新 encode 一次
    -- 注意:计数器在 set_body_data 成功后才 incr,避免 encode 失败导致 counter 与实际下发 body 不一致
    if req_dirty then
        local new_body = cjson.encode(req)
        if new_body then
            ngx.req.set_body_data(new_body)
            if cch_stripped_pending then
                ngx.ctx.cch_stripped = true
                cch_ctl:incr("stripped_total", 1, 0)
            end
            if kimi_normalized_pending > 0 then
                ngx.ctx.kimi_normalized = kimi_normalized_pending
                cch_ctl:incr("kimi_normalize_changes_total", kimi_normalized_pending, 0)
                cch_ctl:incr("kimi_normalize_req_count", 1, 0)
            end
        else
            ngx.log(ngx.ERR, "cjson.encode(req) failed after rewrite; falling back to original body")
        end
    end

    -- header 已经命中：sid 用 header 的，但 body 已经剥了
    if sid_from_header then
        return sid_from_header, src_from_header
    end

    -- body sid: litellm checks both metadata and litellm_metadata
    for _, key in ipairs({"litellm_metadata", "metadata"}) do
        local sid, src = from_metadata(req[key], key)
        if sid then return sid, src end
    end

    -- OpenAI standard `user` field (survives upstream-gateway body passthrough; used by client-plugin plugin)
    -- 可通过 opts.disable_body_user_affinity（或全局 _G.DISABLE_BODY_USER_AFFINITY 兜底）关闭。
    local disable_body_user
    if opts.disable_body_user_affinity ~= nil then
        disable_body_user = opts.disable_body_user_affinity
    else
        disable_body_user = _G.DISABLE_BODY_USER_AFFINITY
    end
    if not disable_body_user then
        local u = req.user
        if type(u) == "string" and _nonblank(u) then
            return u, "body.user"
        end
    end

    return nil
end
