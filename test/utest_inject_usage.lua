-- resty 单元测试:surgical_inject_usage 三分支(F5)。resty CLI 提供完整 ngx.re。
-- 只测该函数,故直接内联被测逻辑的依赖:reqtransform.lua 顶层需 _G._nonblank,先 stub。
_G._nonblank = function(s) return type(s)=="string" and s:match("%S") ~= nil end
-- 加载真实 reqtransform.lua(顶层只定义函数,不执行 ngx.shared)
assert(loadfile(arg[1] or "lua/reqtransform.lua"))()  -- 用法: resty test/utest_inject_usage.lua [lua/reqtransform.lua]
local cjson = require "cjson"   -- 用 cjson.null 模拟 JSON null 解码后的 sentinel

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ "..name)
    else fail = fail + 1; print("  ✗ "..name.."\n      got="..tostring(got).."\n      exp="..tostring(exp)) end
end

-- case1:无 stream_options → 末尾 } 前插整字段
local b1 = '{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}'
local n1, inj1 = _G.surgical_inject_usage(b1, { stream = true })
eq("case1 无stream_options注入", n1, '{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}')
eq("case1 injected=true", inj1, true)

-- case1b:body 尾部带空白/换行 → 仍在最外层 } 前插
local b1b = '{"a":1}\n'
local n1b = _G.surgical_inject_usage(b1b, { stream = true })
eq("case1b 尾部空白", n1b, '{"a":1,"stream_options":{"include_usage":true}}\n')

-- case2:有 stream_options 对象但缺 include_usage 键(非空)→ 插到对象开头带逗号
local b2 = '{"stream":true,"stream_options":{"foo":1}}'
local n2, inj2 = _G.surgical_inject_usage(b2, { stream = true, stream_options = { foo = 1 } })
eq("case2 非空对象插键", n2, '{"stream":true,"stream_options":{"include_usage":true,"foo":1}}')
eq("case2 injected=true", inj2, true)

-- case2b:空 stream_options 对象 → 插键不带逗号
local b2b = '{"stream":true,"stream_options":{}}'
local n2b = _G.surgical_inject_usage(b2b, { stream = true, stream_options = {} })
eq("case2b 空对象插键无逗号", n2b, '{"stream":true,"stream_options":{"include_usage":true}}')

-- case3(F5-false):显式 include_usage=false → **就地改值 true**(不再走 struct)
local b3 = '{"stream":true,"stream_options":{"include_usage":false}}'
local n3, inj3 = _G.surgical_inject_usage(b3, { stream = true, stream_options = { include_usage = false } })
eq("case3 false就地改true", n3, '{"stream":true,"stream_options":{"include_usage":true}}')
eq("case3 injected=true", inj3, true)

-- case3b:include_usage=null(cjson.null sentinel,非 nil 非 true → else 分支)→ 改 true
local b3b = '{"stream_options":{"include_usage":null},"stream":true}'
local n3b, inj3b = _G.surgical_inject_usage(b3b, { stream = true, stream_options = { include_usage = cjson.null } })
eq("case3b null改true", n3b, '{"stream_options":{"include_usage":true},"stream":true}')
eq("case3b injected=true", inj3b, true)

-- case3c:include_usage=0(标量数字)→ 改 true
local b3c = '{"stream_options":{"include_usage":0},"stream":true}'
local n3c, inj3c = _G.surgical_inject_usage(b3c, { stream = true, stream_options = { include_usage = 0 } })
eq("case3c 数字0改true", n3c, '{"stream_options":{"include_usage":true},"stream":true}')
eq("case3c injected=true", inj3c, true)

-- case3d:content 里出现 \"include_usage\":false(转义)不误命中,真键仍被改
local b3d = '{"messages":[{"role":"user","content":"设 \\"include_usage\\":false 会怎样"}],"stream_options":{"include_usage":false}}'
local n3d = _G.surgical_inject_usage(b3d, { stream = true, stream_options = { include_usage = false } })
eq("case3d 转义同串不误伤", n3d, '{"messages":[{"role":"user","content":"设 \\"include_usage\\":false 会怎样"}],"stream_options":{"include_usage":true}}')

-- byte-preservation:注入不碰 tools 声明区
local b4 = '{"tools":[{"type":"function","function":{"parameters":{"x":1},"name":"Bash"}}],"stream":true}'
local n4 = _G.surgical_inject_usage(b4, { stream = true })
eq("case4 tools区逐字节保留", n4, '{"tools":[{"type":"function","function":{"parameters":{"x":1},"name":"Bash"}}],"stream":true,"stream_options":{"include_usage":true}}')

-- case5(F5-null):body 里 "stream_options":null → 就地替换成对象,不追加(否则重复键)。
-- so 解出是 nil/null-sentinel(非 table),但 body 有该键 → 走替换分支。这里 req.stream_options 传 nil 模拟。
local b5 = '{"messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":null}'
local n5, inj5 = _G.surgical_inject_usage(b5, { stream = true })
eq("case5 null就地替换", n5, '{"messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}')
eq("case5 injected=true", inj5, true)
eq("case5 无重复键", select(2, n5:gsub('"stream_options"', "")), 1)  -- "stream_options" 只出现 1 次

-- case5b:content 里出现 "stream_options":null 字样(JSON 转义 \")不被误命中,真键仍被替换
local b5b = '{"messages":[{"role":"user","content":"设 \\"stream_options\\":null 会怎样"}],"stream_options":null}'
local n5b = _G.surgical_inject_usage(b5b, { stream = true })
eq("case5b 转义同串不误伤", n5b, '{"messages":[{"role":"user","content":"设 \\"stream_options\\":null 会怎样"}],"stream_options":{"include_usage":true}}')

-- case6:stream_options 是标量(非 null 非对象)→ 追加会重复键 → 跳过注入(injected=false),不动 body
local b6 = '{"stream":true,"stream_options":5}'
local n6, inj6 = _G.surgical_inject_usage(b6, { stream = true })
eq("case6 标量不追加不动", n6, b6)
eq("case6 injected=false", inj6, false)

print(string.format("================ surgical_inject_usage 单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
