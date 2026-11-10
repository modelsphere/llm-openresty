-- Run: resty test/utest_postprocess_sse_scan.lua [lua/postprocess.lua]
-- The SSE line scan and the per-chunk fast path of lua/postprocess.lua:
--   1. sse_payload(line) gives exactly what the pattern it replaced gave, on hand-picked edge cases and
--      on random lines;
--   2. strip_actual_usage passes a stream without the field through byte-identical however the chunks
--      are cut, and still strips it however the chunks are cut (the needle may straddle a boundary).
local pp = assert(loadfile(arg[1] or "lua/postprocess.lua"))()
local fail = 0
local function check(ok, msg) if not ok then fail = fail + 1; print("  FAIL " .. msg) end end

-- 1. sse_payload == the old pattern
local function old(line) return line:match("^data:%s*(.-)%s*\r?\n?$") end
local cases = {
    "data: {\"a\":1}\n", "data:{\"a\":1}\n", "data:  {\"a\":1}  \r\n", "data: {\"a\":1}", "data:\n", "data:",
    "data:   \n", "data: [DONE]\n", "data:\t[DONE]\r\n", ": keep-alive\n", "event: message\n", "\n", "", "d",
    "dat", "data", " data: x\n", "DATA: x\n", "data: x\r", "data: x\r\r\n", "data: \v\fx\v\f\n", "data: a b\n",
    "data: x\n\n", "datax: y\n", "data:\r\n",
}
math.randomseed(20261110)
local alphabet = { "d", "a", "t", ":", " ", "\t", "\r", "\n", "\v", "\f", "{", "}", "x", "\"", "data:", "data: " }
for _ = 1, 20000 do
    local parts = {}
    for _ = 1, math.random(0, 12) do parts[#parts + 1] = alphabet[math.random(#alphabet)] end
    cases[#cases + 1] = table.concat(parts)
end
local mismatches = 0
for _, l in ipairs(cases) do
    local a, b = old(l), pp.sse_payload(l)
    if a ~= b then
        mismatches = mismatches + 1
        if mismatches <= 5 then check(false, string.format("sse_payload(%q) = %q, pattern gives %q", l, tostring(b), tostring(a))) end
    end
end
check(mismatches == 0, mismatches .. " mismatches with the old pattern")
print(string.format("sse_payload vs pattern: %d lines, %d mismatches", #cases, mismatches))

-- 2. the fast path, through the real header/body entry points
local function run(name, pieces)
    local ctx = {}
    local prev = _G.ngx
    _G.ngx = { ctx = ctx, status = 200, header = { content_type = "text/event-stream" }, arg = {},
               ERR = "error", log = function() end }
    local opts = { postprocess = name }
    _G.do_postprocess_header(opts)
    local out = {}
    for i, p in ipairs(pieces) do
        ngx.arg[1], ngx.arg[2] = p, i == #pieces
        _G.do_postprocess_body(opts)
        out[#out + 1] = ngx.arg[1]
    end
    _G.ngx = prev
    return table.concat(out)
end
local function cut(s)   -- random chunking, including empty chunks
    local out, i = {}, 1
    while i <= #s do
        local n = math.random(0, 40)
        out[#out + 1] = s:sub(i, i + n - 1); i = i + n
    end
    out[#out + 1] = ""
    return out
end
local content = {}
for i = 1, 50 do
    content[#content + 1] = 'data: {"id":"c","choices":[{"index":0,"delta":{"content":"t' .. i .. ' a/b"},"finish_reason":null}]}\n\n'
end
content = table.concat(content)
local usage = 'data: {"id":"c","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":50},' ..
              '"actual_usage":{"prompt_tokens":10,"prompt_tokens_details":{"cached_tokens":8}}}\n\n'
local without = content .. 'data: {"id":"c","choices":[],"usage":{"prompt_tokens":10}}\n\ndata: [DONE]\n\n'
local with = content .. usage .. "data: [DONE]\n\n"
local unchanged, stripped = 0, 0
for _ = 1, 500 do
    if run("strip_actual_usage", cut(without)) == without then unchanged = unchanged + 1 end
    local o = run("strip_actual_usage", cut(with))
    if not o:find("actual_usage", 1, true) and o:sub(1, #content) == content
       and o:find('"choices":[]', 1, true) and o:sub(-14) == "data: [DONE]\n\n" then
        stripped = stripped + 1
    end
end
check(unchanged == 500, "stream without the field changed in " .. (500 - unchanged) .. "/500 random chunkings")
check(stripped == 500, "field not cleanly stripped in " .. (500 - stripped) .. "/500 random chunkings")
-- the needle split exactly across two chunks
local at = with:find("actual_usage", 1, true)
local split = run("strip_actual_usage", { with:sub(1, at + 5), with:sub(at + 6) })
check(not split:find("actual_usage", 1, true), "needle split across two chunks was not stripped")
-- vllm_format (no needle) still rewrites frames found by the line scan
local v = run("vllm_format", cut(content .. 'data: {"choices":[],"system_fingerprint":"fp"}\n\n'))
check(not v:find("system_fingerprint", 1, true), "vllm_format did not drop system_fingerprint")
print(string.format("fast path: %d/500 unchanged, %d/500 stripped", unchanged, stripped))

print(fail == 0 and "sse scan + fast path: PASS" or ("sse scan + fast path: FAIL=" .. fail))
os.exit(fail == 0 and 0 or 1)
