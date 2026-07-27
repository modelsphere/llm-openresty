-- resty 单元测试:surgical_normalize 默认策略【全局重排 rerank】——每个 tool_call → functions.<fn>:<全局位置序号>。
-- 位置 pos 对每个 tool_call 递增(即使跳过改写);客户端如实回传原生位置号则幂等零改动。
-- 用法: resty test/utest_surgical_rerank.lua [lua/reqtransform.lua]
package.loaded.util = { _nonblank = function(s) return type(s) == "string" and s:match("%S") ~= nil end }
local rt = assert(loadfile(arg[1] or "lua/reqtransform.lua"))()
_G.KIMI_ID_STRATEGY = "rerank"   -- 显式默认策略
local cjson = require "cjson"
local M = _G.KIMI_NORMALIZE_MODELS

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ "..name)
    else fail = fail + 1; print("  ✗ "..name.."\n      got="..tostring(got).."\n      exp="..tostring(exp)) end
end

-- A:两个 call(read/bash)→ 全局位置 functions.read:0 / functions.bash:1;逐字节 + 计数=2
local a = '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"call_x","type":"function","function":{"name":"read","arguments":"{}"}},{"id":"call_y","type":"function","function":{"name":"bash","arguments":"{}"}}]}]}'
local na, ca = rt.surgical_normalize(a, cjson.decode(a), M)
eq("A 全局位置 0/1", na, (a:gsub("call_x", "functions.read:0"):gsub("call_y", "functions.bash:1")))
eq("A 计数=2", ca, 2)

-- B:客户端如实回传原生位置号(functions.read:0 / functions.bash:1)→ 幂等零改动
local b = '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"functions.read:0","type":"function","function":{"name":"read","arguments":"{}"}},{"id":"functions.bash:1","type":"function","function":{"name":"bash","arguments":"{}"}}]}]}'
local nb, cb = rt.surgical_normalize(b, cjson.decode(b), M)
eq("B 原生位置号幂等", nb, b)
eq("B 计数=0", cb, 0)

-- C:混合——pos0 原生 functions.read:0 保持,pos1 call_z → functions.bash:1;仅 1 处改
local c = '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"functions.read:0","type":"function","function":{"name":"read","arguments":"{}"}},{"id":"call_z","type":"function","function":{"name":"bash","arguments":"{}"}}]}]}'
local nc, cc = rt.surgical_normalize(c, cjson.decode(c), M)
eq("C 混合按位置", nc, (c:gsub("call_z", "functions.bash:1")))
eq("C 计数=1", cc, 1)

-- D:tc.id 与 role=tool 的 tool_call_id 引用同步(call_a → functions.read:0,两处)
local d = '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"call_a","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_a","content":"r"}]}'
local nd, cd = rt.surgical_normalize(d, cjson.decode(d), M)
eq("D tc+ref 同步", nd, (d:gsub("call_a", "functions.read:0")))
eq("D 计数=2", cd, 2)

-- E:非 Kimi 模型 → 不动
local e = '{"model":"other","messages":[{"role":"assistant","tool_calls":[{"id":"call_a","type":"function","function":{"name":"read","arguments":"{}"}}]}]}'
local ne, ce = rt.surgical_normalize(e, cjson.decode(e), M)
eq("E 非 Kimi 不动", ne, e)
eq("E 计数=0", ce, 0)

-- F:全局计数器跨【多条 assistant 消息】连续(第二条 assistant 的 call 应是 :1 而非 :0)
local f = '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"c1","content":"r"},{"role":"assistant","tool_calls":[{"id":"c2","type":"function","function":{"name":"bash","arguments":"{}"}}]}]}'
local nf, cf = rt.surgical_normalize(f, cjson.decode(f), M)
eq("F 跨消息全局连续 0/1", nf, (f:gsub("c1", "functions.read:0"):gsub("c2", "functions.bash:1")))
eq("F 计数=3", cf, 3)   -- c1(tc)+c1(ref)+c2(tc)

print(string.format("================ surgical_normalize(rerank 默认)单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
