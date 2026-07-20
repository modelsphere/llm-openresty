-- resty 单元测试:surgical_normalize 的【hash 策略】(challall,保存函数)——每个 tool_call →
-- functions.<fn>:<crc32(fn+"\1"+args)>。默认策略已是 rerank(见 utest_surgical_rerank),此测显式切 hash。
-- 用法: resty test/utest_surgical_normalize.lua [lua/reqtransform.lua]
_G._nonblank = function(s) return type(s) == "string" and s:match("%S") ~= nil end
assert(loadfile(arg[1] or "lua/reqtransform.lua"))()
_G.KIMI_ID_STRATEGY = "hash"   -- 本测验证保存的 hash 策略(非默认)
local cjson = require "cjson"
local M = { ["kimi-k2.6"] = true }

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ "..name)
    else fail = fail + 1; print("  ✗ "..name.."\n      got="..tostring(got).."\n      exp="..tostring(exp)) end
end
-- 与 _stable_toolid 同源:内容哈希 = crc32(fn + "\1" + arguments),落 [1e6,2e6)
local function hashid(fn, args) return "functions."..fn..":"..tostring(1000000 + (ngx.crc32_long(fn.."\1"..args) % 1000000)) end

-- A:call_abc(read, args={})→ 内容哈希;逐字节替换 + 计数=2(id + tool_call_id)
local a = '{"model":"kimi-k2.6","messages":[{"role":"assistant","content":"","tool_calls":[{"id":"call_abc","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_abc","content":"r"}]}'
local na, ca = _G.surgical_normalize(a, cjson.decode(a), M)
eq("A call_abc→内容哈希 逐字节", na, (a:gsub("call_abc", hashid("read", "{}"))))
eq("A 计数=2", ca, 2)

-- B:challall 连 canonical 也重编(functions.Bash:5 也 → 内容哈希,不再保留)
local b = '{"model":"kimi-k2.6","messages":[{"role":"assistant","content":"","tool_calls":[{"id":"functions.Bash:5","type":"function","function":{"name":"Bash","arguments":"{}"}}]},{"role":"tool","tool_call_id":"functions.Bash:5","content":"r"}]}'
local nb, cb = _G.surgical_normalize(b, cjson.decode(b), M)
eq("B canonical 也重编成内容哈希", nb, (b:gsub("functions%.Bash:5", hashid("Bash", "{}"))))
eq("B 计数=2", cb, 2)

-- C:收敛/碰撞——同一历史两个【同内容】调用(read /a,不同原始 id)→ 塌成同一个 id
local c = '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"call_X","type":"function","function":{"name":"read","arguments":"{\\"path\\":\\"/a\\"}"}},{"id":"call_Y","type":"function","function":{"name":"read","arguments":"{\\"path\\":\\"/a\\"}"}}]}]}'
local nc, cc = _G.surgical_normalize(c, cjson.decode(c), M)
local h = hashid("read", "{\"path\":\"/a\"}")
eq("C 同内容→同id(收敛/碰撞)", nc, (c:gsub("call_X", h):gsub("call_Y", h)))
eq("C 计数=2", cc, 2)

-- D:不同内容 → 不同 id(不同 args / 不同 fn)
eq("D 不同args→不同id", hashid("read", "{\"path\":\"/a\"}") ~= hashid("read", "{\"path\":\"/b\"}"), true)
eq("D 不同fn→不同id", hashid("read", "{}") ~= hashid("write", "{}"), true)

-- E:非 Kimi 模型 → 直接不动
local e = '{"model":"other","messages":[{"role":"assistant","tool_calls":[{"id":"call_A","function":{"name":"Bash"}}]}]}'
local ne, ce = _G.surgical_normalize(e, cjson.decode(e), M)
eq("E 非 Kimi 不动", ne, e)
eq("E 计数=0", ce, 0)

-- F:hash 落 [1e6,2e6) + 确定性
local h1 = hashid("Bash", "{}")
local n1 = tonumber(h1:match(":(%d+)$"))
eq("F hash 在 [1e6,2e6)", (n1 >= 1000000 and n1 < 2000000), true)
eq("F hash 确定性", hashid("Bash", "{}"), h1)

print(string.format("================ surgical_normalize(challall)单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
