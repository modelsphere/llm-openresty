-- resty 单测:证 struct 路径(normalize_kimi_tool_ids,decode→改表)与 surgical 路径
-- (surgical_normalize,raw body 字符串替换)对同一请求产出【完全相同】的 tool_call id / tool_call_id。
-- 两者共用 _kimi_id_map,理论必然一致;此测把它钉死(尤其 /v1/messages hoist 走 struct 路径)。
-- 用法: resty test/utest_struct_surgical_consistency.lua [lua/reqtransform.lua]
package.loaded.util = { _nonblank = function(s) return type(s) == "string" and s:match("%S") ~= nil end }
local rt = assert(loadfile(arg[1] or "lua/reqtransform.lua"))()
local cjson = require "cjson"
local M = _G.KIMI_NORMALIZE_MODELS   -- reqtransform 加载时已设置

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ "..name)
    else fail = fail + 1; print("  ✗ "..name.."\n      struct  ="..tostring(got).."\n      surgical="..tostring(exp)) end
end

-- 收集 req 里所有 tool_call id(tc:) + role=tool 的 tool_call_id(ref:),按出现顺序拼串 → 便于比较
local function collect_ids(req)
    local out = {}
    if type(req) == "table" and type(req.messages) == "table" then
        for _, m in ipairs(req.messages) do
            if type(m) == "table" then
                if type(m.tool_calls) == "table" then
                    for _, tc in ipairs(m.tool_calls) do
                        if type(tc) == "table" then out[#out + 1] = "tc:" .. tostring(tc.id) end
                    end
                end
                if m.tool_call_id ~= nil then out[#out + 1] = "ref:" .. tostring(m.tool_call_id) end
            end
        end
    end
    return table.concat(out, "|")
end

-- 跑一个 case:分别过 struct 与 surgical,断言 id 结果一致
local function consistency(name, body)
    -- struct:normalize_kimi_tool_ids 原地改 req 表
    local req_s = cjson.decode(body)
    rt.normalize_kimi_tool_ids(req_s)
    local ids_struct = collect_ids(req_s)
    -- surgical:surgical_normalize 产出新 body 串,再 decode 读 id
    local req_g = cjson.decode(body)
    local newbody = rt.surgical_normalize(body, req_g, M)
    local ids_surg = collect_ids(cjson.decode(newbody))
    eq(name, ids_struct, ids_surg)
end

-- A:单 tool_call + 结果(call_abc → 内容哈希,tc 与 ref 同步)
consistency("A 单call+结果",
    '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"call_abc","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_abc","content":"r"}]}')

-- B:混合 canonical + 非规范,两个结果引用
consistency("B 混合canonical+非规范",
    '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"functions.Bash:5","type":"function","function":{"name":"Bash","arguments":"{}"}},{"id":"call_xyz","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"functions.Bash:5","content":"a"},{"role":"tool","tool_call_id":"call_xyz","content":"b"}]}')

-- C:碰撞——两个同内容(read /a)不同原始 id → 两路都塌成同 id
consistency("C 同内容碰撞",
    '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"call_X","type":"function","function":{"name":"read","arguments":"{\\"path\\":\\"/a\\"}"}},{"id":"call_Y","type":"function","function":{"name":"read","arguments":"{\\"path\\":\\"/a\\"}"}}]}]}')

-- D:孤儿 ref——tool_call_id 无对应 tc.id(不在 map)→ 两路都原样保留
consistency("D 孤儿ref保留",
    '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"call_A","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_ORPHAN","content":"x"},{"role":"tool","tool_call_id":"call_A","content":"y"}]}')

-- E:跨多轮多 tool_call(不同 fn 不同 args)
consistency("E 多轮多call",
    '{"model":"kimi-k2.6","messages":[{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"read","arguments":"{\\"p\\":\\"/a\\"}"}}]},{"role":"tool","tool_call_id":"c1","content":"r1"},{"role":"assistant","tool_calls":[{"id":"c2","type":"function","function":{"name":"write","arguments":"{\\"p\\":\\"/b\\"}"}}]},{"role":"tool","tool_call_id":"c2","content":"r2"}]}')

-- F:非 Kimi 模型 → 两路都不动
consistency("F 非kimi不动",
    '{"model":"other","messages":[{"role":"assistant","tool_calls":[{"id":"call_A","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_A","content":"r"}]}')

print(string.format("================ struct↔surgical 一致性单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
