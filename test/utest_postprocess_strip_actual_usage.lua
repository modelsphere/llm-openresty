-- Run: resty test/utest_postprocess_strip_actual_usage.lua [lua/postprocess.lua]
local cjson = require "cjson.safe"
local pp = assert(loadfile(arg[1] or "lua/postprocess.lua"))()

-- Drive the handler through the real header/body phase entry points.
local function run(name, content_type, pieces, status)
    local ctx = {}
    local previous_ngx = _G.ngx
    _G.ngx = {
        ctx = ctx, status = status or 200,
        header = { content_type = content_type, content_length = 999 },
        arg = {}, ERR = "error", log = function() end,
    }
    local opts = { postprocess = name }
    _G.do_postprocess_header(opts)
    local out = {}
    for i, piece in ipairs(pieces) do
        ngx.arg[1], ngx.arg[2] = piece, i == #pieces
        _G.do_postprocess_body(opts)
        out[#out + 1] = ngx.arg[1]
    end
    local content_length = ngx.header.content_length
    _G.ngx = previous_ngx
    return table.concat(out), content_length
end

local function pieces_of(s, n)   -- split s into n roughly equal pieces
    local out, step = {}, math.max(1, math.floor(#s / n))
    local i = 1
    while i <= #s do out[#out + 1] = s:sub(i, i + step - 1); i = i + step end
    return out
end

local usage = '{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,'
           .. '"prompt_tokens_details":{"cached_tokens":2048,"cache_write_tokens":1792}}'
local actual = '{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,'
            .. '"prompt_tokens_details":{"cached_tokens":3072}}'

-- 1. Stream: content frames stay byte-identical; the final usage frame loses
--    actual_usage, keeps usage untouched and keeps "choices":[] an array.
local content = 'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}\n\n'
local final = 'data: {"id":"c1","object":"chat.completion.chunk","choices":[],"usage":' .. usage
           .. ',"actual_usage":' .. actual .. '}\n\n'
local stream = content .. final .. "data: [DONE]\n\n"
for _, n in ipairs({ 1, 3, 7, #stream }) do       -- whole, and split mid-line / per byte
    local out, cl = run("strip_actual_usage", "text/event-stream", pieces_of(stream, n))
    assert(cl == nil, "Content-Length must be dropped")
    assert(out:sub(1, #content) == content, "content frame changed (n=" .. n .. ")")
    assert(not out:find("actual_usage", 1, true), "actual_usage left (n=" .. n .. ")")
    assert(out:sub(-14) == "data: [DONE]\n\n", "terminator changed (n=" .. n .. ")")
    local last
    for json in out:gmatch("data: ({[^\n]*})\n") do last = json end   -- the last JSON frame
    assert(last)
    assert(last:find('"choices":[]', 1, true), "choices no longer an empty array: " .. last)
    local obj = assert(cjson.decode(last))
    assert(obj.usage.prompt_tokens_details.cached_tokens == 2048)
    assert(obj.usage.prompt_tokens_details.cache_write_tokens == 1792)
    assert(obj.usage.total_tokens == 4224 and obj.id == "c1")
end

-- 2. Non-stream JSON body, split across chunks.
local body = '{"id":"c2","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":'
          .. usage .. ',"actual_usage":' .. actual .. '}'
local out, cl = run("strip_actual_usage", "application/json; charset=utf-8", pieces_of(body, 5))
assert(cl == nil)
local obj = assert(cjson.decode(out))
assert(obj.actual_usage == nil and obj.usage.prompt_tokens_details.cached_tokens == 2048)
assert(obj.choices[1].message.content == "ok")

-- 3. Nothing to strip: byte-identical, stream and JSON.
local plain = 'data: {"choices":[],"usage":' .. usage .. '}\n\n'
assert(run("strip_actual_usage", "text/event-stream", { plain }) == plain)
local plain_json = '{ "choices": [], "usage": ' .. usage .. ' }'
assert(run("strip_actual_usage", "application/json", { plain_json }) == plain_json)
-- the name inside a string value: its quotes are escaped there, so the substring test
-- does not match and the frame is not even decoded
local in_text = 'data: {"choices":[{"delta":{"content":"\\"actual_usage\\" is a field"}}]}\n'
assert(run("strip_actual_usage", "text/event-stream", { in_text }) == in_text)
-- nested actual_usage is not the top-level field: left alone
local nested = '{"choices":[],"usage":{"actual_usage":1}}'
assert(pp.strip_actual_usage_json(nested) == nested)

-- 4. Gates: non-2xx and other media types pass through untouched.
assert(run("strip_actual_usage", "application/json", { body }, 500) == body)
assert(run("strip_actual_usage", "text/plain", { body }) == body)
-- malformed JSON: fail open
local broken = 'data: {"actual_usage": \n'
assert(run("strip_actual_usage", "text/event-stream", { broken }) == broken)

-- 5. vllm_format drops it too, and keeps "choices":[] an array on the usage frame.
local v_out = run("vllm_format", "text/event-stream",
    { 'data: {"choices":[],"system_fingerprint":"fp","usage":' .. usage .. ',"actual_usage":' .. actual .. '}\n' })
assert(not v_out:find("actual_usage", 1, true) and not v_out:find("system_fingerprint", 1, true))
assert(v_out:find('"choices":[]', 1, true), "vllm_format turned choices into an object: " .. v_out)

-- 6. Each route gets only the handler it names.
assert(pp.resolve({ postprocess = "strip_actual_usage" }) == pp.handlers.strip_actual_usage)
assert(pp.resolve({ postprocess = "vllm_format" }) == pp.handlers.vllm_format)
assert(pp.resolve({}) == nil)

print("strip_actual_usage SSE and JSON rewrite: PASS")
