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

local function rewrite_response(content_type, pieces, status)
    local ctx = {}
    local previous_ngx = _G.ngx
    _G.ngx = {
        ctx = ctx, status = status or 200,
        header = { content_type = content_type, content_length = 999 },
        arg = {}, ERR = "error", log = function() end,
    }
    local opts = { postprocess = "vllm_format" }
    _G.do_postprocess_header(opts)
    local out = {}
    for i, piece in ipairs(pieces) do
        ngx.arg[1], ngx.arg[2] = piece, i == #pieces
        _G.do_postprocess_body(opts)
        out[#out + 1] = ngx.arg[1]
    end
    local result = table.concat(out)
    local content_length = ngx.header.content_length
    _G.ngx = previous_ngx
    return result, content_length
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
assert(choice.finish_reason == json_null and choice.stop_reason == nil)

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

local json_body = cjson.encode({
    object = "chat.completion",
    prompt_logprobs = json_null, prompt_token_ids = json_null,
    prompt_text = json_null, metrics = json_null,
    service_tier = "default", system_fingerprint = "fp",
    kv_transfer_params = {}, ec_transfer_params = {},
    choices = {{
        reasoning = "choice reasoning", finish_reason = "stop", stop_reason = "eos",
        logprobs = json_null, token_ids = json_null, routed_experts = json_null,
        message = {
            role = "assistant", content = "answer", reasoning = "message reasoning",
            refusal = json_null, annotations = json_null, audio = json_null,
            function_call = json_null,
        },
    }},
})
local rewritten, content_length = rewrite_response("application/json; charset=utf-8",
    { json_body:sub(1, 23), json_body:sub(24) })
local full = assert(cjson.decode(rewritten))
local full_choice = full.choices[1]
assert(content_length == nil)
assert(full.prompt_logprobs == nil and full.prompt_token_ids == nil)
assert(full.prompt_text == nil and full.metrics == nil)
assert(full.service_tier == nil and full.system_fingerprint == nil)
assert(full.kv_transfer_params == nil and full.ec_transfer_params == nil)
assert(full_choice.reasoning == nil and full_choice.reasoning_content == "choice reasoning")
assert(full_choice.logprobs == nil and full_choice.token_ids == nil)
assert(full_choice.routed_experts == nil and full_choice.stop_reason == nil)
assert(full_choice.message.reasoning == nil)
assert(full_choice.message.reasoning_content == "message reasoning")
assert(full_choice.message.content == "answer")
for _, field in ipairs({ "refusal", "annotations", "audio", "function_call" }) do
    assert(full_choice.message[field] == nil)
end

local kept = cjson.encode({
    prompt_logprobs = { 1 }, metrics = { latency = 1 },
    choices = {{ message = { refusal = "no", annotations = { "note" } } }},
})
local kept_json = rewrite_response("application/json", { kept })
local kept_obj = assert(cjson.decode(kept_json))
assert(#kept_obj.prompt_logprobs == 1 and kept_obj.metrics.latency == 1)
assert(kept_obj.choices[1].message.refusal == "no")
assert(#kept_obj.choices[1].message.annotations == 1)
assert(rewrite_response("text/plain", { json_body }) == json_body)
assert(rewrite_response("application/json", { json_body }, 500) == json_body)
local untouched_json = '{ "choices": [{ "message": { "content": "ok" } }] }'
assert(rewrite_response("application/json", { untouched_json }) == untouched_json)
local old_cap = pp.DEFAULT_MAX_BUFFER
pp.DEFAULT_MAX_BUFFER = 10
assert(rewrite_response("application/json", { json_body:sub(1, 8), json_body:sub(9) })
       == json_body)
pp.DEFAULT_MAX_BUFFER = old_cap

local stream_extra = frame({
    choices = {{ delta = { content = "ok" }, stop_reason = json_null,
                 routed_experts = json_null }},
    service_tier = "default", metrics = json_null,
})
local stream_obj = decoded(rewrite(stream_extra))
assert(stream_obj.service_tier == nil and stream_obj.metrics == nil)
assert(stream_obj.choices[1].stop_reason == nil)
assert(stream_obj.choices[1].routed_experts == nil)

print("vllm_format SSE and JSON rewrite: PASS")
