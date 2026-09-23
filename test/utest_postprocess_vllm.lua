-- Run: resty test/utest_postprocess_vllm.lua [lua/postprocess.lua]
local cjson = require "cjson.safe"
local json_null = (require "cjson").null
local pp = assert(loadfile(arg[1] or "lua/postprocess.lua"))()
local body = pp.handlers.vllm_format.body

local function rewrite(input, pieces)
    local ctx = { pp_sse = true }
    local out = {}
    if pieces then
        for i, piece in ipairs(pieces) do
            out[#out + 1] = body(piece, i == #pieces, ctx, {})
        end
    else
        out[1] = body(input, true, ctx, {})
    end
    return table.concat(out)
end

local function frame(data, ending)
    return "data: " .. cjson.encode(data) .. (ending or "\n")
end

local function decoded(output)
    return assert(cjson.decode(assert(output:match("^data: (.-)\r?\n?$"))))
end

local sample = {
    id = "chatcmpl-test",
    model = "kimi-k3",
    prompt_token_ids = json_null,
    prompt_text = json_null,
    system_fingerprint = "fp-test",
    choices = {{
        index = 0,
        delta = { reasoning = "The" },
        logprobs = json_null,
        token_ids = json_null,
        finish_reason = json_null,
        stop_reason = "not-finished",
    }},
}

local input = frame(sample)
local output = rewrite(input, { input:sub(1, 18), input:sub(19) })
local obj = decoded(output)
local choice = obj.choices[1]
assert(choice.delta.reasoning == nil)
assert(choice.delta.reasoning_content == "The")
assert(obj.prompt_token_ids == nil and obj.prompt_text == nil)
assert(obj.system_fingerprint == nil)
assert(choice.logprobs == nil and choice.token_ids == nil)
assert(choice.finish_reason == json_null and choice.stop_reason == "not-finished")

local finished = {
    choices = {{
        delta = {}, finish_reason = "length", stop_reason = "max_tokens",
        logprobs = { content = {} }, token_ids = { 1, 2 },
    }},
    prompt_token_ids = { 1 }, prompt_text = "prompt",
}
local done = decoded(rewrite(frame(finished, "\r\n")))
assert(done.choices[1].stop_reason == nil)
assert(done.choices[1].finish_reason == "length")
assert(type(done.choices[1].logprobs) == "table")
assert(#done.choices[1].token_ids == 2)
assert(#done.prompt_token_ids == 1 and done.prompt_text == "prompt")
assert(rewrite(frame(finished, "\r\n")):sub(-2) == "\r\n")

local no_newline = frame({ choices = {{ delta = { reasoning = "last" } }} }, "")
assert(decoded(rewrite(no_newline)).choices[1].delta.reasoning_content == "last")
assert(rewrite("data: [DONE]\n") == "data: [DONE]\n")
local unchanged = 'data: {"choices":[{"delta":{"content":"ok"}}]}\n'
assert(rewrite(unchanged) == unchanged)

print("vllm_format SSE rewrite: PASS")
