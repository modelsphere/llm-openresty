-- openresty/lua/postprocess.lua
-- Response post-processing: rewrite an upstream response before it reaches the
-- client. Motivating case: normalizing vLLM response-format quirks so clients
-- see a consistent shape regardless of which backend served the request.
--
-- The plumbing handles phase hooks, per-route selection, streaming vs buffered
-- responses, status gating, Content-Length, buffer caps, and fail-open behavior.
--
-- Entry points (called from router_locations.inc, which has no module handle, so
-- they hang on _G like bodylog_filter_chunk / do_emit_peer_header):
--   _G.do_postprocess_header(opts) -- header_filter phase, once per request
--   _G.do_postprocess_body(opts)   -- body_filter phase, once per output chunk
-- Both are no-ops unless a handler is selected for the route, so wiring them into
-- the conf changes nothing until a handler is registered AND a route opts in.
-- The body phase is driven entirely by state the header phase stashed on ngx.ctx;
-- if the header phase never ran (e.g. an internal error_page short-circuit), the
-- body phase finds no handler and passes the bytes through untouched -- fail-open.
--
-- A handler is a table registered by name via M.register(name, handler):
--   handler.buffered = true|false
--       false/nil -> streaming: handler.body is called per chunk (SSE-friendly,
--                    keeps proxy_buffering off intact).
--       true      -> accumulate the whole body and call handler.body once at eof
--                    (for non-stream JSON that must be parsed as a whole). Capped:
--                    see max_buffer.
--   handler.header(ctx, opts)
--       optional; runs in header_filter. Adjust ngx.header here.
--   handler.body(chunk, eof, ctx, opts) -> string | nil
--       streaming: return the replacement for this chunk ("" drops it); nil means
--                  "leave this chunk unchanged". A non-string, non-nil return is
--                  ignored (logged) and the chunk is left unchanged.
--       buffered:  called only at eof with the full body as `chunk`; return the
--                  rewritten body, or nil to emit the original.
--   handler.rewrites_length = true|false
--       set true when the rewrite changes the body length. The framework then
--       drops Content-Length in header_filter so nginx switches to chunked (a
--       stale Content-Length would truncate or hang the client). Buffered handlers
--       are treated as length-changing automatically.
--   handler.accept_status = function(status) -> bool
--       optional gate: return false to skip post-processing for that response
--       status (e.g. only touch 2xx). Absent = act on every status.
--   handler.max_buffer = <bytes>
--       buffered only; overrides M.DEFAULT_MAX_BUFFER. If the accumulated body
--       exceeds it, the framework gives up buffering, flushes what it has and
--       passes the rest through un-transformed (fail-open, logged).
--
-- Route opt: opts.postprocess = "<name>" selects the handler for that route.
-- Global fallback: _G.POSTPROCESS_DEFAULT = "<name>". nil in both = off.

local M = {}

local cjson = require "cjson.safe"   -- decode/encode return nil on error (no throw)
local json_null = (require "cjson").null

-- Cap on how much of a response may be accumulated in memory, so a large
-- (multi-MB) LLM response cannot OOM the router. Buffered handlers may override it.
M.DEFAULT_MAX_BUFFER = 8 * 1024 * 1024   -- 8 MiB

-- Global config knob, read lazily by resolve(): _G.POSTPROCESS_DEFAULT is the
-- fallback handler name and is nil (off) unless a session/route conf sets it.
-- Intentionally not assigned here -- reading an unset global already yields nil.

-- Registry: name -> handler table. Guarded so a reload/re-require keeps entries.
M.handlers = M.handlers or {}

-- Register (or replace) a named handler. Called at module load for built-ins;
-- callers can add their own before the first request.
function M.register(name, handler)
    if type(name) ~= "string" or name == "" then
        error("postprocess.register: name must be a non-empty string", 2)
    end
    if type(handler) ~= "table" then
        error("postprocess.register: handler must be a table", 2)
    end
    M.handlers[name] = handler
    return handler
end

-- Resolve which handler applies to this request: route opt wins, then the global
-- default. Returns the handler table, or nil when post-processing is off.
local function resolve(opts)
    local name = (opts and opts.postprocess) or _G.POSTPROCESS_DEFAULT
    if not name then return nil end
    return M.handlers[name]
end
M.resolve = resolve   -- exposed for unit tests

-- header_filter phase: pick the handler once and stash it on ngx.ctx so the body
-- phase does not re-resolve per chunk. Apply the status gate and Content-Length
-- handling, then let the handler touch response headers.
function _G.do_postprocess_header(opts)
    local h = resolve(opts)
    if not h then return end            -- off for this route -> no-op

    -- Status gate: a handler may restrict itself (e.g. only rewrite 2xx bodies).
    if h.accept_status then
        local ok, keep = pcall(h.accept_status, ngx.status)
        if not ok then
            ngx.log(ngx.ERR, "postprocess accept_status error: ", keep)
            return                      -- fail open: don't post-process on a broken gate
        end
        if not keep then return end     -- gated out for this status
    end

    local ctx = ngx.ctx
    ctx.postprocess = h
    ctx.postprocess_opts = opts

    -- If the body length will change, drop Content-Length so nginx switches to
    -- chunked; a stale length truncates or hangs the client. Buffered handlers
    -- reconstruct the whole body, so treat them as length-changing.
    if h.rewrites_length or h.buffered then
        ngx.header.content_length = nil
    end

    if h.header then
        local ok, err = pcall(h.header, ctx, opts)
        if not ok then
            -- fail open: a broken handler must not break the response headers.
            ngx.log(ngx.ERR, "postprocess header handler error: ", err)
        end
    end
end

-- body_filter phase: runs once per output chunk. Only active when the header
-- phase selected a handler. Fail-open everywhere: any handler error, bad return,
-- or an over-cap body leaves the original bytes flowing to the client.
function _G.do_postprocess_body(opts)
    local ctx = ngx.ctx
    local h = ctx.postprocess
    if not h then return end            -- header phase selected nothing -> pass through

    local chunk = ngx.arg[1]
    local eof   = ngx.arg[2]

    if h.buffered then
        -- Once we've given up on buffering (over cap), let the rest stream through.
        if ctx.postprocess_overflow then return end

        local buf = ctx.postprocess_buf
        if not buf then
            buf = {}
            ctx.postprocess_buf = buf
            ctx.postprocess_buf_size = 0
        end
        if chunk and chunk ~= "" then
            buf[#buf + 1] = chunk
            ctx.postprocess_buf_size = ctx.postprocess_buf_size + #chunk
        end

        local cap = h.max_buffer or M.DEFAULT_MAX_BUFFER
        if ctx.postprocess_buf_size > cap then
            -- Too big to hold safely: flush everything buffered so far (including
            -- this chunk) and pass the remainder through un-transformed.
            ngx.log(ngx.ERR, "postprocess: buffered body over ", cap,
                    " bytes; passing through un-transformed")
            ctx.postprocess_overflow = true
            ngx.arg[1] = table.concat(buf)
            ctx.postprocess_buf = nil          -- free
            return
        end

        ngx.arg[1] = ""                        -- suppress intermediate output
        if eof then
            local body = table.concat(buf)
            local ok, out = pcall(h.body, body, true, ctx, opts)
            if ok and type(out) == "string" then
                ngx.arg[1] = out
            else
                if not ok then
                    ngx.log(ngx.ERR, "postprocess body handler error: ", out)
                elseif out ~= nil then
                    ngx.log(ngx.ERR, "postprocess body handler returned ", type(out),
                            ", expected string|nil; emitting original")
                end
                ngx.arg[1] = body              -- fail open: emit the original body
            end
        end
        return
    end

    -- Streaming: transform this chunk in place.
    local ok, out = pcall(h.body, chunk, eof, ctx, opts)
    if not ok then
        ngx.log(ngx.ERR, "postprocess body handler error: ", out)   -- fail open
        return
    end
    if out == nil then return end              -- nil = leave chunk unchanged
    if type(out) == "string" then
        ngx.arg[1] = out
    else
        ngx.log(ngx.ERR, "postprocess body handler returned ", type(out),
                ", expected string|nil; leaving chunk unchanged")
    end
end

-- ---- handlers -----------------------------------------------------------
local vllm_top_null_fields = {
    "prompt_logprobs", "prompt_token_ids", "prompt_text", "metrics",
    "logprobs", "token_ids", -- retain the existing cleanup at the top level
}
local vllm_choice_null_fields = {
    "logprobs", "token_ids", "routed_experts",
    "prompt_token_ids", "prompt_text", -- retain the existing choice cleanup
}
local vllm_message_null_fields = { "refusal", "annotations", "audio", "function_call" }
local vllm_top_drop_fields = {
    "service_tier", "system_fingerprint", "kv_transfer_params", "ec_transfer_params",
}

local function vllm_drop_null_fields(obj, fields)
    local changed = false
    for _, field in ipairs(fields) do
        if obj[field] == json_null then
            obj[field] = nil
            changed = true
        end
    end
    return changed
end

local function vllm_rename_reasoning(obj)
    if type(obj) ~= "table" or obj.reasoning == nil then return false end
    obj.reasoning_content = obj.reasoning
    obj.reasoning = nil
    return true
end

-- Apply the same field rules to a complete JSON response or one SSE chunk.
local function vllm_normalize(obj)
    if type(obj) ~= "table" or type(obj.choices) ~= "table" then
        return false
    end
    local changed = vllm_drop_null_fields(obj, vllm_top_null_fields)
    for _, field in ipairs(vllm_top_drop_fields) do
        if obj[field] ~= nil then
            obj[field] = nil
            changed = true
        end
    end
    local usage = obj.usage
    local details = type(usage) == "table" and usage.prompt_tokens_details
    if type(details) == "table" then
        if details.multimodal_tokens == json_null then
            details.multimodal_tokens = nil
            changed = true
        end
        if details.created_cache_tokens ~= nil then
            if details.cache_write_tokens == nil then
                details.cache_write_tokens = details.created_cache_tokens
            end
            details.created_cache_tokens = nil
            changed = true
        end
    end
    for _, ch in ipairs(obj.choices) do
        if type(ch) == "table" then
            if vllm_drop_null_fields(ch, vllm_choice_null_fields) then changed = true end
            if vllm_rename_reasoning(ch) then changed = true end
            if vllm_rename_reasoning(ch.delta) then changed = true end
            local message = ch.message
            if type(message) == "table" then
                if vllm_rename_reasoning(message) then changed = true end
                if vllm_drop_null_fields(message, vllm_message_null_fields) then
                    changed = true
                end
            end
            if ch.stop_reason ~= nil then
                ch.stop_reason = nil
                changed = true
            end
        end
    end
    return changed
end

local function vllm_rewrite_json(payload)
    local obj = cjson.decode(payload)
    if not vllm_normalize(obj) then return payload end
    local enc = cjson.encode(obj)
    return enc or payload                            -- fail open on an encode error
end

-- Rewrite one SSE data frame. Leave unchanged frames byte-identical.
local function vllm_rewrite_sse_line(line)
    local payload = line:match("^data:%s*(.-)%s*\r?\n?$")
    if not payload or payload == "" or payload == "[DONE]" then
        return line                                  -- comment/heartbeat/terminator/non-data
    end
    local rewritten = vllm_rewrite_json(payload)
    if rewritten == payload then return line end
    return "data: " .. rewritten .. (line:match("\r?\n$") or "")
end

-- vllm_format: normalize SSE frames and complete JSON responses. SSE is handled
-- line by line; non-stream JSON is capped and buffered until eof.
M.register("vllm_format", {
    buffered = false,
    rewrites_length = true,
    accept_status = function(status) return status >= 200 and status < 300 end,
    header = function(ctx, opts)
        local content_type = (ngx.header.content_type or ""):lower()
        ctx.pp_sse = content_type:find("text/event-stream", 1, true) ~= nil
        ctx.pp_json = not ctx.pp_sse and content_type:find("application/json", 1, true) ~= nil
    end,
    body = function(chunk, eof, ctx, opts)
        if ctx.pp_json then
            if ctx.pp_json_overflow then return nil end
            local buf = ctx.pp_json_buf
            if not buf then
                buf = {}
                ctx.pp_json_buf = buf
                ctx.pp_json_size = 0
            end
            if chunk and chunk ~= "" then
                buf[#buf + 1] = chunk
                ctx.pp_json_size = ctx.pp_json_size + #chunk
            end
            if ctx.pp_json_size > M.DEFAULT_MAX_BUFFER then
                ngx.log(ngx.ERR, "postprocess: JSON body over ", M.DEFAULT_MAX_BUFFER,
                        " bytes; passing through un-transformed")
                ctx.pp_json_overflow = true
                ctx.pp_json_buf = nil
                return table.concat(buf)
            end
            if not eof then return "" end
            ctx.pp_json_buf = nil
            return vllm_rewrite_json(table.concat(buf))
        end
        if not ctx.pp_sse then return nil end        -- other media types pass through
        local pending = (ctx.pp_leftover or "") .. (chunk or "")
        ctx.pp_leftover = nil
        local out, pos = {}, 1
        while true do
            local nl = pending:find("\n", pos, true)
            if not nl then
                local rest = pending:sub(pos)        -- incomplete trailing line
                if eof then
                    out[#out + 1] = vllm_rewrite_sse_line(rest)
                else
                    ctx.pp_leftover = rest           -- stash; the next chunk completes it
                end
                break
            end
            out[#out + 1] = vllm_rewrite_sse_line(pending:sub(pos, nl))
            pos = nl + 1
        end
        return table.concat(out)
    end,
})

return M
