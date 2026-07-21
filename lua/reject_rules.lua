-- openresty/lua/reject_rules.lua
-- 规则化请求拒绝引擎:按 opts.reject_rules(配在各 session_route_<model>.conf 的 factory)对请求
-- 【内容本身】做规则匹配,命中即返回可配 status(默认 429)。区别于并发/TTFT/TPS 那三种【运行态】限流。
--
-- 设计:
--   * 纯逻辑(字段解析 / 操作符 / 节点匹配 / 校验结构)不依赖 ngx.shared/ngx.exit,便于 resty 单测;
--     只有 eval_reject_rules(拒绝出口)+ validate 的 ngx.log 才碰 ngx。
--   * 规则 = 递归节点 + 顶层动作元数据:
--       叶子   { field=<str>, op=<str>, value=<any> }
--       组合   { all={<node>,...} }(AND) / { any={<node>,...} }(OR)
--       顶层再带 name / status / message(动作)。
--   * 规则列表任一顶层规则命中即拒绝(短路,首个命中)。
--   * 全部 _G.* 导出,遵循本仓无 `return M` 约定。

local cjson = require "cjson.safe"
local ffi   = require "ffi"

-- ── 字段解析 ──────────────────────────────────────────────────────────────
-- ctx = { req = <decoded body table>, body_len = <int, 原始体字节> }
-- 计算型字段(注册表)+ 回退到 req 的点路径取值。

-- UTF-8 字符数(码点数,非字节数)= 非续字节个数(续字节 = 0x80-0xBF)。中文 #s 是字节数(汉字 3 字节),
-- 这里返回真实字符数,匹配"字符数"语义。用 FFI 逐字节数:JIT 编译近 C 速、**不分配**(gsub 版会 alloc
-- 一个新串,200KB content 上 ~3ms → FFI 版 ~百 µs;见 test/bench_reject_rules.lua)。
local function utf8_len(s)
    local len = #s
    local p = ffi.cast("const uint8_t*", s)
    local n = 0
    for i = 0, len - 1 do
        local b = p[i]
        if b < 0x80 or b >= 0xc0 then n = n + 1 end   -- 首字节(ASCII 或多字节 lead)才计数,跳续字节
    end
    return n
end

-- 遍历 messages[].content 累加【字符数】;content 为数组(多模态)时取其 text/content 字符串段。
local function count_input_chars(req)
    local total = 0
    local msgs = req and req.messages
    if type(msgs) ~= "table" then return 0 end
    for _, m in ipairs(msgs) do
        local c = m and m.content
        if type(c) == "string" then
            total = total + utf8_len(c)
        elseif type(c) == "table" then
            for _, part in ipairs(c) do
                if type(part) == "table" then
                    local t = part.text or part.content
                    if type(t) == "string" then total = total + utf8_len(t) end
                elseif type(part) == "string" then
                    total = total + utf8_len(part)
                end
            end
        end
    end
    return total
end

-- 点路径取值:"a.b.c" → req.a.b.c;单段 "max_tokens" → req.max_tokens。
-- ⚠️ 仅支持对象 key,不支持数组下标(Lua 序列表按整数 1..n 索引,"messages.0" 这种恒 nil)。
local function dotpath(req, path)
    if type(req) ~= "table" or type(path) ~= "string" then return nil end
    local cur = req
    for seg in string.gmatch(path, "[^.]+") do
        if type(cur) ~= "table" then return nil end
        cur = cur[seg]
    end
    return cur
end

function _G.reject_rules_field_value(field, ctx)
    local req = ctx.req
    if field == "input_bytes" then
        return ctx.body_len
    elseif field == "input_chars" then
        if ctx._input_chars == nil then ctx._input_chars = count_input_chars(req) end
        return ctx._input_chars
    elseif field == "messages_count" then
        return (type(req) == "table" and type(req.messages) == "table") and #req.messages or 0
    elseif field == "tools_count" then
        return (type(req) == "table" and type(req.tools) == "table") and #req.tools or 0
    else
        return dotpath(req, field)
    end
end

-- ── 操作符 ────────────────────────────────────────────────────────────────
-- 数值比较两侧须均 number,否则不命中(不崩)。eq/ne 通吃 bool/string/number。
local function both_num(a, b) return type(a) == "number" and type(b) == "number" end

function _G.reject_rules_apply_op(op, lhs, rhs)
    if     op == "gt" then return both_num(lhs, rhs) and lhs >  rhs
    elseif op == "ge" then return both_num(lhs, rhs) and lhs >= rhs
    elseif op == "lt" then return both_num(lhs, rhs) and lhs <  rhs
    elseif op == "le" then return both_num(lhs, rhs) and lhs <= rhs
    elseif op == "eq" then return lhs == rhs
    elseif op == "ne" then return lhs ~= rhs
    elseif op == "in" then
        if type(rhs) ~= "table" then return false end
        for _, v in ipairs(rhs) do if lhs == v then return true end end
        return false
    elseif op == "nin" then
        if type(rhs) ~= "table" then return false end
        for _, v in ipairs(rhs) do if lhs == v then return false end end
        return true
    elseif op == "match" then
        return type(lhs) == "string" and type(rhs) == "string" and lhs:find(rhs) ~= nil
    elseif op == "contains" then
        return type(lhs) == "string" and type(rhs) == "string" and lhs:find(rhs, 1, true) ~= nil
    elseif op == "prefix" then
        return type(lhs) == "string" and type(rhs) == "string" and lhs:sub(1, #rhs) == rhs
    elseif op == "exists" then
        return lhs ~= nil
    elseif op == "absent" then
        return lhs == nil
    end
    return false   -- 未知 op(validate 已拦,双保险)→ 不命中
end

-- 已知操作符集合(validate 用)
_G.REJECT_RULES_OPS = {
    gt = true, ge = true, lt = true, le = true, eq = true, ne = true,
    ["in"] = true, nin = true, match = true, contains = true, prefix = true,
    exists = true, absent = true,
}

-- ── 节点匹配(递归 all/any/叶子)──────────────────────────────────────────
function _G.reject_rules_node_matches(node, ctx)
    if type(node) ~= "table" then return false end
    if node.all then
        for _, sub in ipairs(node.all) do
            if not _G.reject_rules_node_matches(sub, ctx) then return false end
        end
        return true
    elseif node.any then
        for _, sub in ipairs(node.any) do
            if _G.reject_rules_node_matches(sub, ctx) then return true end
        end
        return false
    else
        local lhs = _G.reject_rules_field_value(node.field, ctx)
        return _G.reject_rules_apply_op(node.op, lhs, node.value)
    end
end

-- ── 校验(register_route 调用)─────────────────────────────────────────────
-- 返回 normalized 列表(丢弃非法规则,不崩路由);nil/空 → nil。
local function valid_node(node)
    if type(node) ~= "table" then return false end
    if node.all or node.any then
        local subs = node.all or node.any
        if type(subs) ~= "table" or #subs == 0 then return false end
        for _, s in ipairs(subs) do if not valid_node(s) then return false end end
        return true
    end
    -- 叶子
    if type(node.field) ~= "string" or #node.field == 0 then return false end   -- 空 field 会解析成整个 body,禁
    if not _G.REJECT_RULES_OPS[node.op] then return false end
    if node.op == "in" or node.op == "nin" then
        if type(node.value) ~= "table" then return false end   -- in/nin 的 value 必须是列表,漏写 = 死规则
    elseif node.op ~= "exists" and node.op ~= "absent" then
        if node.value == nil then return false end             -- 其余需要 value(允许 false/0,只查 nil)
    end
    return true
end

function _G.validate_reject_rules(name, rules)
    if rules == nil then return nil end
    if type(rules) ~= "table" then
        ngx.log(ngx.ERR, "[", name, "] reject_rules 非 table,忽略")
        return nil
    end
    local out = {}
    for i, r in ipairs(rules) do
        if type(r) ~= "table" or not valid_node(r) then
            ngx.log(ngx.ERR, "[", name, "] reject_rules[", i, "] 非法(field/op/value/组合结构)— 丢弃该条")
        else
            r.name = r.name or ("rule" .. i)
            if r.status ~= nil and (type(r.status) ~= "number" or r.status < 400 or r.status > 599) then
                ngx.log(ngx.ERR, "[", name, "] reject_rules[", i, "].status=", tostring(r.status),
                        " 非法(需 400-599)— 用路由默认")
                r.status = nil
            end
            out[#out + 1] = r
        end
    end
    if #out == 0 then return nil end
    return out
end

-- ── 主入口(do_route 在 prepare_request 之后调用)──────────────────────────
-- 命中首个规则 → reject_stat "<route>:rule" 聚合计数 + cch_ctl "reject_rule_hit:<name>" 逐规则计数
--   + ngx.status + json + ngx.exit(status)。无命中正常返回。
-- 热切开关:ngx.shared[opts.cch_ctl_dict] key "reject_rules_enabled"(dict 值 > opts.reject_rules_default_enabled)。
function _G.eval_reject_rules(opts)
    local rules = opts.reject_rules
    if not rules or #rules == 0 then return end
    -- 热切开关(不 reload 关规则)
    local ctl = ngx.shared[opts.cch_ctl_dict]
    local enabled = opts.reject_rules_default_enabled
    if ctl then
        local raw = ctl:get("reject_rules_enabled")
        if raw ~= nil then enabled = (raw == 1) end
    end
    if not enabled then return end
    -- 从 prepare_request 存的【原始】请求体字符串重新 decode(pristine,与归一化路径无关):
    -- struct 路径会就地改 req 表(hoist/strip),surgical 不改 → 若直接读 req 表,同内容规则会因路径分叉。
    -- 这里恒读客户端原始 body(set_body_data 改的是发往后端的线,不影响原始 body 字符串)。opt-in 路由才多一次 decode。
    local raw = ngx.ctx.req_body_raw
    if not raw then return end
    local req = cjson.decode(raw)
    if type(req) ~= "table" then return end
    local ctx = { req = req, body_len = ngx.ctx.req_body_len or 0 }
    for _, rule in ipairs(rules) do
        if _G.reject_rules_node_matches(rule, ctx) then
            local status = rule.status or opts.reject_rules_status or 429
            if type(status) ~= "number" then status = 429 end
            ngx.status = status
            do local rj = ngx.shared.reject_stat
               if rj then rj:incr((opts.route_name or "-") .. ":rule", 1, 0) end end
            if ctl then ctl:incr("reject_rule_hit:" .. (rule.name or "?"), 1, 0) end
            ngx.header["Content-Type"] = "application/json"
            ngx.say(cjson.encode({
                error = {
                    type    = "rejected_by_rule",
                    rule    = rule.name,
                    message = rule.message or ("request rejected by rule '" .. tostring(rule.name) .. "'"),
                    route   = opts.route_name,
                }
            }))
            return ngx.exit(status)
        end
    end
end
