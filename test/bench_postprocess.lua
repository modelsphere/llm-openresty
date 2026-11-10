-- Micro-benchmark of lua/postprocess.lua's body filter: the cost per SSE chunk of each handler, against
-- the same stream with post-processing off (the default). Pure CPU in one worker; no network.
--   resty test/bench_postprocess.lua [lua/postprocess.lua] [frames=2000] [rounds=3] [frames_per_chunk=1]
-- The stream mimics an sglang chat completion: <frames> content frames of a few tokens each, the
-- finish frame, the usage frame (with actual_usage) and [DONE]. Each upstream chunk carries
-- <frames_per_chunk> frames, as the engine's flushes do. Reports ns per frame: each case runs the
-- stream until at least 2 s have passed (clocks under resty are coarse: os.clock ticks in 10 ms, so a
-- single 10 ms pass reads as 0 or one tick), best of <rounds> such batches.
local ngx_real = ngx             -- run() swaps in a fake ngx; keep the real one for the clock
local pp = assert(loadfile(arg[1] or "lua/postprocess.lua"))()
local FRAMES, ROUNDS, PER = tonumber(arg[2] or 2000), tonumber(arg[3] or 3), tonumber(arg[4] or 1)

local function frame(s) return "data: " .. s .. "\n\n" end
local frames = {}
for i = 1, FRAMES do
    frames[#frames + 1] = frame(string.format(
        '{"id":"chatcmpl-b3a1f0c2d4e5","object":"chat.completion.chunk","created":1794300000,"model":"kimi-k3",' ..
        '"choices":[{"index":0,"delta":{"content":"token %d of the answer"},"logprobs":null,"finish_reason":null}]}', i))
end
frames[#frames + 1] = frame('{"id":"chatcmpl-b3a1f0c2d4e5","object":"chat.completion.chunk","created":1794300000,' ..
    '"model":"kimi-k3","choices":[{"index":0,"delta":{},"logprobs":null,"finish_reason":"stop"}]}')
frames[#frames + 1] = frame('{"id":"chatcmpl-b3a1f0c2d4e5","object":"chat.completion.chunk","created":1794300000,' ..
    '"model":"kimi-k3","choices":[],"usage":{"prompt_tokens":4096,"completion_tokens":2000,"total_tokens":6096,' ..
    '"prompt_tokens_details":{"cached_tokens":2048,"cache_write_tokens":1792}},"actual_usage":{"prompt_tokens":4096,' ..
    '"completion_tokens":2000,"total_tokens":6096,"prompt_tokens_details":{"cached_tokens":3072}}}')
frames[#frames + 1] = "data: [DONE]\n\n"
local chunks = {}
for i = 1, #frames, PER do chunks[#chunks + 1] = table.concat(frames, "", i, math.min(i + PER - 1, #frames)) end

local function run(name)   -- one pass of the whole stream through the real header/body entry points
    local ctx = {}
    _G.ngx = { ctx = ctx, status = 200, header = { content_type = "text/event-stream" }, arg = {},
               ERR = "error", log = function() end }
    local opts = { postprocess = name }
    _G.do_postprocess_header(opts)
    local bytes = 0
    for i, c in ipairs(chunks) do
        ngx.arg[1], ngx.arg[2] = c, i == #chunks
        _G.do_postprocess_body(opts)
        bytes = bytes + #ngx.arg[1]
    end
    return bytes
end

local function now() ngx_real.update_time(); return ngx_real.now() end
local function bench(name)   -- seconds per stream pass
    local best = math.huge
    for _ = 1, ROUNDS do
        local n, t0 = 0, now()
        repeat run(name); n = n + 1 until n % 10 == 0 and now() - t0 >= 2
        local per = (now() - t0) / n
        if per < best then best = per end
    end
    return best
end

for _ = 1, 3 do run(nil); run("strip_actual_usage"); run("vllm_format") end   -- warm the JIT
local base = bench(nil)
print(string.format("stream: %d frames, %d chunks (%d frames/chunk), %d bytes", #frames, #chunks, PER, run(nil)))
for _, name in ipairs({ "off", "strip_actual_usage", "vllm_format" }) do
    local t = name == "off" and base or bench(name)
    print(string.format("  %-20s %8.0f ns/frame   +%7.0f ns/frame vs off   %6.2f ms per stream",
        name, t / #frames * 1e9, (t - base) / #frames * 1e9, t * 1e3))
end

-- Where strip_actual_usage's time goes: the per-line pattern that rewrite_sse_line runs on every frame,
-- against a plain substring test on the same lines.
local lines = {}
for _, f in ipairs(frames) do lines[#lines + 1] = f:match("^[^\n]*\n") end
local function per_line(fn)
    local best = math.huge
    for _ = 1, ROUNDS do
        local n, t0 = 0, now()
        repeat for _, l in ipairs(lines) do fn(l) end; n = n + 1 until n % 10 == 0 and now() - t0 >= 1
        local per = (now() - t0) / (n * #lines)
        if per < best then best = per end
    end
    return best * 1e9
end
print(string.format("per line (%d bytes avg):", math.floor(#table.concat(lines) / #lines)))
print(string.format("  match ^data:%%s*(.-)%%s*\\r?\\n?$  %8.0f ns", per_line(function(l) return l:match("^data:%s*(.-)%s*\r?\n?$") end)))
print(string.format("  find '\"actual_usage\"' (plain) %8.0f ns", per_line(function(l) return l:find('"actual_usage"', 1, true) end)))
