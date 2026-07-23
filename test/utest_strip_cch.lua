-- resty 单元测试:surgical_strip_cch(F1 [^;"]* + 位置无关 + 三段门控 + key 锚定)。
-- 用法: resty test/utest_strip_cch.lua [lua/reqtransform.lua]
package.loaded.util = { _nonblank = function(s) return type(s) == "string" and s:match("%S") ~= nil end }
local rt = assert(loadfile(arg[1] or "lua/reqtransform.lua"))()

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ "..name)
    else fail = fail + 1; print("  ✗ "..name.."\n      got="..tostring(got).."\n      exp="..tostring(exp)) end
end
local function run(name, body, exp_out, exp_n)
    local new, n = rt.surgical_strip_cch(body)
    eq(name.." 输出", new, exp_out)
    eq(name.." n="..exp_n, n, exp_n)
end

-- A:cch 在 messages[0] content 开头 → 剥(只删到 cch=…;,保留后续,含前导空格)
run("A msgs[0] 剥",
    '{"messages":[{"role":"system","content":"x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=abc; You are helpful"}]}',
    '{"messages":[{"role":"system","content":" You are helpful"}]}', 1)

-- B(H8 位置无关):cch 在 messages[1] → surgical 照剥(struct 版才只检 messages[0])
run("B msgs[1] 位置无关剥",
    '{"messages":[{"role":"user","content":"hi"},{"role":"system","content":"x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=xyz; sys"}]}',
    '{"messages":[{"role":"user","content":"hi"},{"role":"system","content":" sys"}]}', 1)

-- C:三段不全(缺 cc_entrypoint)→ 不剥
run("C 缺段不剥",
    '{"messages":[{"role":"system","content":"x-anthropic-billing-header: cc_version=1; cch=abc; body"}]}',
    '{"messages":[{"role":"system","content":"x-anthropic-billing-header: cc_version=1; cch=abc; body"}]}', 0)

-- D(H10/F1):cch 段无尾 `;`(直接闭引号)→ [^;"]* 停在引号→不匹配→漏剥,body 完整
--   (旧 [^;]* 会冲出闭引号吞到后面 "x":"a;b" 的 `;` → 损坏 body)
run("D F1 残缺cch不吞body",
    '{"messages":[{"role":"system","content":"x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=abc"}],"x":"a;b"}',
    '{"messages":[{"role":"system","content":"x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=abc"}],"x":"a;b"}', 0)

-- E(H9):cch 不在值开头(前面有 "hello ")→ key 锚定不匹配 → 不剥
run("E 中段cch不剥",
    '{"messages":[{"role":"system","content":"hello x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=abc; tail"}]}',
    '{"messages":[{"role":"system","content":"hello x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=abc; tail"}]}', 0)

-- F(H6/H7):数组/多模态 content 里的 "text" 字段同样剥(regex 收 content|text 两 key)
run("F 数组text字段剥",
    '{"messages":[{"role":"system","content":[{"type":"text","text":"x-anthropic-billing-header: cc_version=1; cc_entrypoint=cli; cch=X; hi"}]}]}',
    '{"messages":[{"role":"system","content":[{"type":"text","text":" hi"}]}]}', 1)

print(string.format("================ surgical_strip_cch 单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
