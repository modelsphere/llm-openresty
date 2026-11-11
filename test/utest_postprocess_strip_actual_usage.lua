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

-- ---------------------------------------------------------------------------
-- 7-10. THE PER-CHOICE PLACEMENT.
--
-- Cases 1-5 all put actual_usage at the top level. Measured on a recent sglang build (2026-11), that
-- is only true of the trailing "choices":[] frame: the frame carrying finish_reason holds the real
-- usage AND actual_usage INSIDE choices[0], with nothing at the top level. The top-level-only version
-- therefore matched the needle on that frame, decoded it, found obj.actual_usage nil, and returned the
-- payload with actual_usage still in it. The shapes below are transcribed from that measurement.

-- 7. The finish frame: actual_usage inside choices[0] must go; the per-choice usage must survive.
local finish_frame = 'data: {"id":"c3","object":"chat.completion.chunk","choices":[{"index":0,'
                  .. '"delta":{},"finish_reason":"length","matched_stop":null,"usage":' .. usage
                  .. ',"actual_usage":' .. actual .. '}]}\n\n'
for _, n in ipairs({ 1, 4, 11, #finish_frame }) do
    local o = run("strip_actual_usage", "text/event-stream", pieces_of(finish_frame, n))
    assert(not o:find("actual_usage", 1, true),
           "per-choice actual_usage survived (n=" .. n .. "): " .. o)
    local d = assert(cjson.decode(o:match("data: ({[^\n]*})")), "frame no longer valid JSON: " .. o)
    assert(d.choices[1].actual_usage == nil)
    assert(d.choices[1].usage.total_tokens == 4224, "per-choice usage was damaged")
    assert(d.choices[1].usage.prompt_tokens_details.cached_tokens == 2048)
    assert(d.choices[1].finish_reason == "length")
    -- This is the first re-encoded shape in the suite that contains an EMPTY JSON OBJECT. cjson's
    -- encode_empty_table_as_object defaults to true, so {} survives -- but that is an instance-level
    -- default of the same kind this file already overrides one line away for arrays, and if it were
    -- ever flipped, "delta":{} would become "delta":[] on the finish frame of every stream on these
    -- routes, breaking any client that reads chunk.choices[0].delta.content. Pinned like the
    -- "usage":null placeholder in case 8, for the same reason.
    assert(o:find('"delta":{}', 1, true), "delta is no longer an empty object (n=" .. n .. "): " .. o)
end

-- 8. A continuous_usage_stats stream puts usage + actual_usage at the TOP level of the content frames
--    while choices[0] still carries the "usage":null placeholder. The placeholder is deliberately left
--    alone (clients already see it and it carries no number), so assert it is still present AND still
--    null after the re-encode: an encoder that turned it into {} or dropped it would be a silent
--    contract change for every client reading those frames.
local mid_frame = 'data: {"id":"c4","object":"chat.completion.chunk","choices":[{"index":0,'
               .. '"delta":{"content":"hi"},"finish_reason":null,"usage":null}],"usage":' .. usage
               .. ',"actual_usage":' .. actual .. '}\n\n'
local mo = run("strip_actual_usage", "text/event-stream", { mid_frame })
assert(not mo:find("actual_usage", 1, true), "top-level actual_usage survived: " .. mo)
assert(mo:find('"usage":null', 1, true), "the choices[0] usage:null placeholder was lost: " .. mo)
local md = assert(cjson.decode(mo:match("data: ({[^\n]*})")))
assert(md.usage.total_tokens == 4224, "top-level usage was damaged")
assert(md.choices[1].delta.content == "hi")

-- 9. DIVISION OF LABOUR, pinned rather than assumed. vllm_format's actual_usage entry is a TOP-LEVEL
--    drop (case 5), so on the finish frame it leaves the per-choice copy in place. That is the current
--    behaviour and this case records it, so the two handlers' scopes cannot drift unnoticed: a route
--    that needs actual_usage gone on every frame shape names strip_actual_usage.
local vf = run("vllm_format", "text/event-stream", { finish_frame })
assert(vf:find("actual_usage", 1, true),
       "vllm_format now strips the per-choice copy -- update case 9 and the README if deliberate")
local vfd = assert(cjson.decode(vf:match("data: ({[^\n]*})")))
assert(vfd.choices[1].usage.total_tokens == 4224, "vllm_format damaged the per-choice usage")

-- 10. The whole measured stream, in order, split at every boundary: content frames with the null
--     placeholder, the finish frame, the trailing usage frame, [DONE].
local measured = 'data: {"id":"c5","choices":[{"index":0,"delta":{"content":"a"},"usage":null}]}\n\n'
             .. 'data: {"id":"c5","choices":[{"index":0,"delta":{"content":"b"},"usage":null}]}\n\n'
             .. finish_frame
             .. 'data: {"id":"c5","choices":[],"usage":' .. usage
             .. ',"actual_usage":' .. actual .. '}\n\n'
             .. "data: [DONE]\n\n"
for _, n in ipairs({ 1, 2, 5, 13, #measured }) do
    local o = run("strip_actual_usage", "text/event-stream", pieces_of(measured, n))
    assert(not o:find("actual_usage", 1, true),
           "actual_usage survived somewhere in the measured stream (n=" .. n .. ")")
    assert(o:sub(-14) == "data: [DONE]\n\n", "terminator changed (n=" .. n .. ")")
    local frames = 0
    for _ in o:gmatch("data: ") do frames = frames + 1 end
    assert(frames == 5, "frame count changed from 5 to " .. frames .. " (n=" .. n .. ")")
end

print("strip_actual_usage SSE and JSON rewrite: PASS")
