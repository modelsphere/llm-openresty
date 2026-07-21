-- resty 微基准:量化 reject_rules 每请求净增开销(eval 里 cjson.decode + 规则匹配),对比三档规则集:
--   A 无 input_chars(全标量字段)—— 不触发 utf8_len,净增≈仅 decode
--   B 单个 input_chars —— 触发 1 次 utf8_len
--   C 多个 input_chars(5 条)—— 靠 ctx._input_chars 缓存,utf8_len 仍只算 1 次(应≈B)
-- rule 路由比无规则路由多的就是这一整段(feature 前不跑)。
-- 用法: resty test/bench_reject_rules.lua [lua/reject_rules.lua]
assert(loadfile(arg[1] or "lua/reject_rules.lua"))()
local cjson = require "cjson.safe"

local SETS = {
    { key = "A 无chars", rules = {
        { name = "tiny",   field = "max_tokens",  op = "lt", value = 10 },
        { name = "big",    field = "input_bytes", op = "gt", value = 500000 },
        { name = "stream", field = "stream",      op = "eq", value = true },
        { name = "combo",  all = { { field = "model", op = "eq", value = "x" },
                                   { field = "max_tokens", op = "gt", value = 99999 } } },
    } },
    { key = "B 单chars", rules = {
        { name = "tiny", field = "max_tokens",  op = "lt", value = 10 },
        { name = "long", field = "input_chars", op = "gt", value = 100000 },
        { name = "combo", all = { { field = "stream", op = "eq", value = true },
                                  { field = "model",  op = "eq", value = "x" } } },
    } },
    { key = "C 多chars(5)", rules = {
        { name = "c1", field = "input_chars", op = "gt", value = 100000 },
        { name = "c2", field = "input_chars", op = "gt", value = 200000 },
        { name = "c3", field = "input_chars", op = "lt", value = 1 },
        { name = "c4", field = "input_chars", op = "ge", value = 999999 },
        { name = "c5", field = "input_chars", op = "le", value = 1 },
    } },
}

local zh = "这是一段用于压测的中文内容占位符文本内容"   -- 20 个中文字符 = 60 字节
local function make_body(reps)
    return cjson.encode({
        model = "kimi-k2.6", stream = false, max_tokens = 1024, temperature = 0.7,
        messages = { { role = "system", content = "you are a helpful assistant" },
                     { role = "user", content = string.rep(zh, reps) } },
    })
end

local function decode_us(raw, M)
    for _ = 1, 200 do local r = cjson.decode(raw); if not r then error("x") end end
    local t0 = os.clock()
    for _ = 1, M do local r = cjson.decode(raw); if not r then error("x") end end
    return (os.clock() - t0) / M * 1e6
end

local function full_us(raw, rules, M)
    for _ = 1, 200 do
        local r = cjson.decode(raw); local ctx = { req = r, body_len = #raw }
        for _, rule in ipairs(rules) do _G.reject_rules_node_matches(rule, ctx) end
    end
    local t0 = os.clock()
    for _ = 1, M do
        local r = cjson.decode(raw); local ctx = { req = r, body_len = #raw }
        for _, rule in ipairs(rules) do _G.reject_rules_node_matches(rule, ctx) end
    end
    return (os.clock() - t0) / M * 1e6
end

print(string.format("%-10s %8s %10s  | %-32s", "body字节", "字符", "decode µs", "各规则集 full µs/req(括号=减 decode 的净增)"))
for _, reps in ipairs({ 17, 850, 3400 }) do          -- ≈1KB / 50KB / 200KB
    local raw = make_body(reps); local M = (#raw > 100000) and 3000 or 8000
    local dec = decode_us(raw, M)
    local cols = {}
    for _, s in ipairs(SETS) do
        local f = full_us(raw, s.rules, M)
        cols[#cols + 1] = string.format("%s=%.1f(+%.1f)", s.key, f, f - dec)
    end
    print(string.format("%-10d %8d %10.1f  | %s", #raw, reps * 20 + 27, dec, table.concat(cols, "  ")))
end
print("预期:A(无chars)净增≈0(仅标量比较);B、C 净增相近(input_chars 缓存 → utf8_len 只算 1 次,不随 chars 规则条数涨)。")
