-- openresty/lua/postprocess.lua
-- Response post-processing: rewrite an upstream response before it reaches the
-- client. Motivating case: normalizing vLLM response-format quirks so clients
-- see a consistent shape regardless of which backend served the request.
--
-- SKELETON ONLY. The plumbing (phase hooks, per-route selection, streaming vs
-- buffered handling, fail-open) is here; the actual rewriting lives in handlers
-- and is left as TODO.
--
-- Entry points (called from router_locations.inc, which has no module handle, so
-- they hang on _G like bodylog_filter_chunk / do_emit_peer_header):
--   _G.do_postprocess_header(opts) -- header_filter phase, once per request
--   _G.do_postprocess_body(opts)   -- body_filter phase, once per output chunk
-- Both are no-ops unless a handler is selected for the route, so wiring them into
-- the conf changes nothing until a handler is registered AND a route opts in.
--
-- A handler is a table registered by name via M.register(name, handler):
--   handler.buffered = true|false
--       false/nil -> streaming: handler.body is called per chunk (SSE-friendly,
--                    keeps proxy_buffering off intact).
--       true      -> accumulate the whole body and call handler.body once at eof
--                    (for non-stream JSON that must be parsed as a whole).
--   handler.header(ctx, opts)
--       optional; runs in header_filter. Adjust ngx.header here. If a buffered
--       handler will change the body length, clear Content-Length here so nginx
--       falls back to chunked (TODO once a real rewrite exists).
--   handler.body(chunk, eof, ctx, opts) -> string | nil
--       streaming: return the replacement for this chunk ("" drops it); nil means
--                  "leave this chunk unchanged".
--       buffered:  called only at eof with the full body as `chunk`; return the
--                  rewritten body, or nil to emit the original.
--
-- Route opt: opts.postprocess = "<name>" selects the handler for that route.
-- Global fallback: _G.POSTPROCESS_DEFAULT = "<name>". nil in both = off.

local M = {}

-- Global config surface, matching the other modules (route/session confs may set it).
_G.POSTPROCESS_DEFAULT = _G.POSTPROCESS_DEFAULT or nil   -- handler name, or nil = off

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
-- phase does not re-resolve per chunk. Let the handler touch response headers.
function _G.do_postprocess_header(opts)
    local h = resolve(opts)
    if not h then return end            -- off for this route -> no-op
    local ctx = ngx.ctx
    ctx.postprocess = h
    ctx.postprocess_opts = opts
    if h.header then
        local ok, err = pcall(h.header, ctx, opts)
        if not ok then
            -- fail open: a broken handler must not break the response headers.
            ngx.log(ngx.ERR, "postprocess header handler error: ", err)
        end
    end
end

-- body_filter phase: runs once per output chunk. Only active when the header
-- phase selected a handler. Fail-open everywhere: any handler error leaves the
-- original bytes flowing to the client.
function _G.do_postprocess_body(opts)
    local ctx = ngx.ctx
    local h = ctx.postprocess
    if not h then return end            -- header phase selected nothing -> pass through

    local chunk = ngx.arg[1]
    local eof   = ngx.arg[2]

    if h.buffered then
        -- Accumulate the whole body, suppress intermediate output, hand the full
        -- body to the handler once at eof.
        local buf = ctx.postprocess_buf
        if not buf then buf = {}; ctx.postprocess_buf = buf end
        if chunk and chunk ~= "" then buf[#buf + 1] = chunk end
        ngx.arg[1] = ""
        if eof then
            local body = table.concat(buf)
            local ok, out = pcall(h.body, body, true, ctx, opts)
            if ok and out ~= nil then
                ngx.arg[1] = out
            else
                if not ok then ngx.log(ngx.ERR, "postprocess body handler error: ", out) end
                ngx.arg[1] = body       -- fail open: emit the original body
            end
        end
        return
    end

    -- Streaming: transform this chunk in place.
    local ok, out = pcall(h.body, chunk, eof, ctx, opts)
    if ok then
        if out ~= nil then ngx.arg[1] = out end   -- nil = leave chunk unchanged
    else
        ngx.log(ngx.ERR, "postprocess body handler error: ", out)  -- fail open
    end
end

-- ---- handlers -----------------------------------------------------------
-- vllm_format: placeholder for normalizing vLLM response-format quirks. Both
-- hooks are no-op pass-throughs for now so a route can already select it; the
-- real logic lands later.
M.register("vllm_format", {
    buffered = false,
    header = function(ctx, opts)
        -- TODO: inspect/adjust response headers (content-type, length) if the
        -- body rewrite below ends up changing them.
    end,
    body = function(chunk, eof, ctx, opts)
        -- TODO: rewrite the vLLM response body here. Pass through unchanged.
        return nil   -- nil = leave the chunk as-is
    end,
})

return M
