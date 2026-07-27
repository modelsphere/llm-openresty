-- resty 单元测试:reject_rules 引擎(字段解析 / 操作符 / 递归 all-any 节点匹配 / validate)。
-- 用法: resty test/utest_reject_rules.lua [lua/reject_rules.lua]
-- 只测纯逻辑 + validate(validate 的 ngx.log 在 resty 下可用);eval_reject_rules 需真实请求周期,走 chat 集成测。
local rr = assert(loadfile(arg[1] or "lua/reject_rules.lua"))()

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ " .. name)
    else fail = fail + 1; print("  ✗ " .. name .. "\n      got=" .. tostring(got) .. "\n      exp=" .. tostring(exp)) end
end

local OP = rr.reject_rules_apply_op
local FV = rr.reject_rules_field_value
local NM = rr.reject_rules_node_matches
local VAL = rr.validate_reject_rules

-- ══ 操作符 ══════════════════════════════════════════════════════════════
print("== 操作符 ==")
eq("gt 真",  OP("gt", 20, 10), true)
eq("gt 假",  OP("gt", 5, 10), false)
eq("gt 类型不匹配→假", OP("gt", "a", 10), false)
eq("ge 相等真", OP("ge", 10, 10), true)
eq("lt 真",  OP("lt", 5, 10), true)
eq("le 相等真", OP("le", 10, 10), true)
eq("eq bool 真", OP("eq", true, true), true)
eq("eq string 真", OP("eq", "x", "x"), true)
eq("eq number 假", OP("eq", 5, 6), false)
eq("ne 真", OP("ne", 5, 6), true)
eq("in 命中", OP("in", "a", {"a", "b"}), true)
eq("in 未命中", OP("in", "c", {"a", "b"}), false)
eq("in value 非表→假", OP("in", "x", "notlist"), false)
eq("nin 真", OP("nin", "c", {"a", "b"}), true)
eq("nin 假", OP("nin", "a", {"a", "b"}), false)
eq("match 命中", OP("match", "hello world", "wor"), true)
eq("match 未命中", OP("match", "abc", "xyz"), false)
eq("contains 字面命中", OP("contains", "a.b.c", "."), true)
eq("prefix 命中", OP("prefix", "functions.bash:1", "functions."), true)
eq("prefix 未命中", OP("prefix", "abc", "xyz"), false)
eq("exists 有值真", OP("exists", 5, nil), true)
eq("exists nil 假", OP("exists", nil, nil), false)
eq("absent nil 真", OP("absent", nil, nil), true)
eq("absent 有值假", OP("absent", 5, nil), false)
eq("未知 op→假", OP("bogus", 1, 1), false)

-- ══ 字段解析 ════════════════════════════════════════════════════════════
print("== 字段解析 ==")
local ctx = { req = {
    max_tokens = 5, stream = true, model = "kimi-k2.6",
    messages = { { role = "user", content = "hello" } },
    tools = { {}, {} },
    nested = { a = { b = 7 } },
}, body_len = 123 }
eq("input_bytes",    FV("input_bytes", ctx), 123)
eq("max_tokens",     FV("max_tokens", ctx), 5)
eq("stream",         FV("stream", ctx), true)
eq("model",          FV("model", ctx), "kimi-k2.6")
eq("messages_count", FV("messages_count", ctx), 1)
eq("tools_count",    FV("tools_count", ctx), 2)
eq("input_chars",    FV("input_chars", ctx), 5)
eq("缺失字段→nil",   FV("does_not_exist", ctx), nil)
eq("点路径 nested.a.b", FV("nested.a.b", ctx), 7)
eq("点路径穿透非表→nil", FV("nested.a.b.c", ctx), nil)

-- 多模态 content(数组)字符数累加
local ctxmm = { req = { messages = {
    { role = "user", content = { { type = "text", text = "abc" }, { type = "image_url" } } },
    { role = "system", content = "de" },
} }, body_len = 0 }
eq("input_chars 多模态数组", FV("input_chars", ctxmm), 5)   -- "abc"(3) + "de"(2)

-- input_chars 按【字符】非字节:中文每字 3 字节但算 1 字符
eq("input_chars CJK=字符数非字节数",
   FV("input_chars", { req = { messages = { { role="user", content="你好世界" } } }, body_len = 0 }), 4)  -- 4 字符(12 字节)
eq("input_chars 中英混", FV("input_chars", { req = { messages = { { role="user", content="hi你好" } } }, body_len = 0 }), 4)  -- h,i,你,好

-- 空/缺 messages 不崩
eq("input_chars 无 messages→0", FV("input_chars", { req = {}, body_len = 0 }), 0)
eq("messages_count 无 messages→0", FV("messages_count", { req = {}, body_len = 0 }), 0)

-- ══ 节点匹配(叶子 / all / any / 嵌套)════════════════════════════════════
print("== 节点匹配 ==")
eq("叶子 max_tokens<10 真", NM({ field = "max_tokens", op = "lt", value = 10 }, ctx), true)
eq("叶子 max_tokens>10 假", NM({ field = "max_tokens", op = "gt", value = 10 }, ctx), false)
eq("叶子 stream=true 真", NM({ field = "stream", op = "eq", value = true }, ctx), true)
eq("AND 两真→真", NM({ all = {
    { field = "stream", op = "eq", value = true },
    { field = "max_tokens", op = "lt", value = 10 },
} }, ctx), true)
eq("AND 一假→假", NM({ all = {
    { field = "stream", op = "eq", value = true },
    { field = "max_tokens", op = "gt", value = 10 },
} }, ctx), false)
eq("OR 有一真→真", NM({ any = {
    { field = "max_tokens", op = "gt", value = 100 },
    { field = "stream", op = "eq", value = true },
} }, ctx), true)
eq("OR 全假→假", NM({ any = {
    { field = "max_tokens", op = "gt", value = 100 },
    { field = "model", op = "eq", value = "nope" },
} }, ctx), false)
eq("嵌套 all(leaf, any(...))→真", NM({ all = {
    { field = "stream", op = "eq", value = true },
    { any = {
        { field = "max_tokens", op = "lt", value = 10 },
        { field = "model", op = "eq", value = "nope" },
    } },
} }, ctx), true)

-- ══ validate_reject_rules ═══════════════════════════════════════════════
print("== validate ==")
eq("nil→nil", VAL("t", nil), nil)
eq("空表→nil", VAL("t", {}), nil)

local v1 = VAL("t", { { field = "max_tokens", op = "lt", value = 10 } })
eq("单条合法→len1", v1 and #v1 or 0, 1)
eq("自动补 name=rule1", v1 and v1[1].name or nil, "rule1")

eq("坏 op 全丢→nil", VAL("t", { { field = "x", op = "bogus", value = 1 } }), nil)
eq("field 非 string 丢→nil", VAL("t", { { field = 5, op = "lt", value = 1 } }), nil)
eq("空 field 丢→nil", VAL("t", { { field = "", op = "exists" } }), nil)
eq("缺 value(非 exists)丢→nil", VAL("t", { { field = "x", op = "lt" } }), nil)
eq("in 非表 value 丢→nil", VAL("t", { { field = "model", op = "in", value = "x" } }), nil)
local vin = VAL("t", { { field = "model", op = "in", value = { "a", "b" } } })
eq("in 表 value 保留→len1", vin and #vin or 0, 1)

local ve = VAL("t", { { field = "x", op = "exists" } })
eq("exists 无 value 保留→len1", ve and #ve or 0, 1)

local vmix = VAL("t", {
    { field = "max_tokens", op = "lt", value = 10 },
    { field = "x", op = "bogus", value = 1 },     -- 坏,丢
})
eq("混合 1 好 1 坏→len1", vmix and #vmix or 0, 1)

local vc = VAL("t", { { all = { { field = "a", op = "eq", value = 1 } } } })
eq("合法组合保留→len1", vc and #vc or 0, 1)
eq("空组合 subs 丢→nil", VAL("t", { { all = {} } }), nil)

local vn = VAL("t", { { name = "myrule", field = "x", op = "exists" } })
eq("name 保留", vn and vn[1].name or nil, "myrule")

local vs1 = VAL("t", { { field = "x", op = "exists", status = 999 } })
eq("status 越界→nil(规则仍保留)", vs1 and vs1[1].status or "KEPT_NIL", "KEPT_NIL")
local vs2 = VAL("t", { { field = "x", op = "exists", status = 413 } })
eq("status 合法→413", vs2 and vs2[1].status or nil, 413)

print(string.format("================ reject_rules 单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
