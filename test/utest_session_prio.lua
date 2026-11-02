-- resty unit tests: session_prio (header parsing / bucket mapping / derived
-- table values / override validation / assess mult / usage-tail token regex).
-- Run: from the repo root, `resty -I lua test/utest_session_prio.lua`
-- Pure logic only (no request cycle); the do_route integration is covered by
-- test/test_session_prio.sh.
local sp  = assert(loadfile("lua/session_prio.lua"))()
local ttft = assert(loadfile("lua/ttft.lua"))()
local tps  = assert(loadfile("lua/tps.lua"))()

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ " .. name)
    else fail = fail + 1; print("  ✗ " .. name .. "\n      got=" .. tostring(got) .. "\n      exp=" .. tostring(exp)) end
end
local function truthy(name, got)
    if got then pass = pass + 1; print("  ✓ " .. name)
    else fail = fail + 1; print("  ✗ " .. name .. " (expected truthy, got " .. tostring(got) .. ")") end
end

-- == header parsing ==
print("== parse_header ==")
local info, class = sp.parse_header(nil)
eq("nil header -> missing", class, "missing"); eq("nil header -> info nil", info, nil)
info, class = sp.parse_header("")
eq("empty header -> missing", class, "missing")
info, class = sp.parse_header("not json")
eq("non-JSON -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"s1"}')
eq("missing turn -> bad", class, "bad")
info, class = sp.parse_header('{"turn":3}')
eq("missing session_id -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"s1","turn":0}')
eq("turn=0 -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"s1","turn":-2}')
eq("negative turn -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"s1","turn":1.5}')
eq("fractional turn -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"s1","turn":"5"}')
eq("string turn -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"  ","turn":5}')
eq("blank session_id -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"","turn":5}')
eq("empty session_id -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"' .. string.rep("a", 257) .. '","turn":5}')
eq("session_id 257 chars -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"' .. string.rep("a", 256) .. '","turn":5}')
eq("session_id 256 chars -> ok", class, "ok")
truthy("valid header -> info", info)
eq("turn kept", info and info.turn, 5)
eq("session_id kept", info and #info.session_id, 256)
info, class = sp.parse_header('{"session_id":"abc","turn":68}')
eq("typical header -> ok", class, "ok")
eq("typical header turn", info.turn, 68)
eq("typical header session_id", info.session_id, "abc")
info, class = sp.parse_header('{"session_id":42,"turn":3}')
eq("number session_id -> bad", class, "bad")
info, class = sp.parse_header('["array"]')
eq("JSON array -> bad", class, "bad")
info, class = sp.parse_header('{"session_id":"s","turn":2,"extra":true}')
eq("extra fields ok", class, "ok")

-- == derived table values (defaults r_new=0.85 severe=1.5 full_turn=32) ==
print("== derived table (defaults) ==")
-- validate_opts wants ngx.log / ngx.shared; under resty ngx.log exists but
-- there is no shared dict. Build a minimal opts and validate it.
local function mkopts(t)
    local o = { route_name = "u", ttft_dict = "ttft_stat" }
    for k, v in pairs(t or {}) do o[k] = v end
    sp.validate_opts(o, "u")
    return o
end
local o = mkopts()
eq("default bucket count", #o._sp_table, 6)
local exp_share  = { [1] = 0.85, [2] = 0.88, [4] = 0.91, [8] = 0.94, [16] = 0.97, [32] = 1.0 }
local exp_tol    = { [1] = 1.0,  [2] = 1.1,  [4] = 1.2,  [8] = 1.3,  [16] = 1.4,  [32] = 1.5 }
for _, b in ipairs(o._sp_table) do
    eq("share@" .. b.min_turn, b.share, exp_share[b.min_turn])
    eq("tol@" .. b.min_turn, b.tol, exp_tol[b.min_turn])
end
eq("top index", o._sp_top, 6)
eq("first min_turn", o._sp_table[1].min_turn, 1)
eq("last min_turn", o._sp_table[6].min_turn, 32)

-- full_turn not a power of two (24): the last bound clamps to it
local o24 = mkopts({ session_prio_full_turn = 24 })
eq("full_turn=24 last bound", o24._sp_table[#o24._sp_table].min_turn, 24)
eq("full_turn=24 last share == 1", o24._sp_table[#o24._sp_table].share, 1.0)
eq("full_turn=24 last tol == severe", o24._sp_table[#o24._sp_table].tol, 1.5)

-- == bucket mapping ==
print("== bucket_of ==")
local tbl = o._sp_table
eq("turn 1 -> b1", sp.bucket_of(tbl, 1), 1)
eq("turn 2 -> b2", sp.bucket_of(tbl, 2), 2)
eq("turn 3 -> b2", sp.bucket_of(tbl, 3), 2)
eq("turn 4 -> b4", sp.bucket_of(tbl, 4), 3)
eq("turn 7 -> b4", sp.bucket_of(tbl, 7), 3)
eq("turn 8 -> b8", sp.bucket_of(tbl, 8), 4)
eq("turn 15 -> b8", sp.bucket_of(tbl, 15), 4)
eq("turn 16 -> b16", sp.bucket_of(tbl, 16), 5)
eq("turn 31 -> b16", sp.bucket_of(tbl, 31), 5)
eq("turn 32 -> b32", sp.bucket_of(tbl, 32), 6)
eq("turn 33 -> b32 (clamped top)", sp.bucket_of(tbl, 33), 6)
eq("turn 1000 -> b32 (clamped top)", sp.bucket_of(tbl, 1000), 6)

-- == session_prio_table override ==
print("== session_prio_table override ==")
local ot = mkopts({ session_prio_table = {
    { min_turn = 1, share = 0.5, tol = 1.0 },
    { min_turn = 10, share = 1.0, tol = 2.0 },
} })
eq("override bucket count", #ot._sp_table, 2)
eq("override kept", ot._sp_table[2].min_turn, 10)
eq("override turn 5 -> b1", sp.bucket_of(ot._sp_table, 5), 1)
eq("override turn 10 -> b10", sp.bucket_of(ot._sp_table, 10), 2)
eq("override turn 99 -> b10", sp.bucket_of(ot._sp_table, 99), 2)

local function invalid_table(name, t)
    local bad = mkopts({ session_prio_table = t })
    eq("invalid table falls back to derived: " .. name, #bad._sp_table, 6)   -- derived default has 6 buckets
end
invalid_table("first min_turn ~= 1", { { min_turn = 2, share = 0.5, tol = 1 } })
invalid_table("not strictly ascending", { { min_turn = 1, share = 0.5, tol = 1 }, { min_turn = 1, share = 0.6, tol = 1 } })
invalid_table("out of order", { { min_turn = 1, share = 0.5, tol = 1 }, { min_turn = 4, share = 0.6, tol = 1 }, { min_turn = 2, share = 0.7, tol = 1 } })
invalid_table("share=0", { { min_turn = 1, share = 0, tol = 1 } })
invalid_table("share>1", { { min_turn = 1, share = 1.2, tol = 1 } })
invalid_table("tol<1", { { min_turn = 1, share = 0.5, tol = 0.9 } })
invalid_table("empty list", {})
invalid_table("not a table", 42)

-- == numeric validation fallback ==
print("== numeric validation ==")
local bad1 = mkopts({ session_prio_r_new = 0 })
eq("r_new=0 -> default 0.85", bad1.session_prio_r_new, 0.85)
local bad2 = mkopts({ session_prio_r_new = 1.5 })
eq("r_new=1.5 -> default", bad2.session_prio_r_new, 0.85)
local bad3 = mkopts({ session_prio_severe = 1 })
eq("severe=1 -> default 1.5", bad3.session_prio_severe, 1.5)
local bad4 = mkopts({ session_prio_full_turn = 1 })
eq("full_turn=1 -> default 32", bad4.session_prio_full_turn, 32)
local bad5 = mkopts({ session_prio_full_turn = 3.7 })
eq("full_turn=3.7 -> default 32", bad5.session_prio_full_turn, 32)
local bad6 = mkopts({ session_prio_release = 1.0 })
eq("release=1.0 -> default 0.9", bad6.session_prio_release, 0.9)
local bad7 = mkopts({ session_prio_warn_frac = 0 })
eq("warn_frac=0 -> default 0.2", bad7.session_prio_warn_frac, 0.2)
local ok1 = mkopts({ session_prio_r_new = 0.5, session_prio_severe = 2, session_prio_full_turn = 8,
                     session_prio_release = 0.8, session_prio_warn_frac = 0.1 })
eq("valid config keeps r_new", ok1.session_prio_r_new, 0.5)
eq("valid config keeps severe", ok1.session_prio_severe, 2)
eq("valid config keeps full_turn", ok1.session_prio_full_turn, 8)
eq("valid config keeps release", ok1.session_prio_release, 0.8)
eq("valid config keeps warn_frac", ok1.session_prio_warn_frac, 0.1)

-- == assess mult parameter (mult=nil is mult=1) ==
print("== assess mult ==")
-- Fake dict: get returns a fixed EWMA; thresholds come from the static
-- metrics chain (opts.ttft_limit_ms / opts.tps_limit_tps).
local function fake_dict(ewma)
    return { get = function(_, _) return ewma end }
end
_G.TTFT_LIMIT_MS = nil   -- keep thresholds out of the way: everything reads opts
_G.TPS_LIMIT_TPS = nil
local t_opts = { route_name = "r", ttft_limit_ms = 400 }
local p_opts = { route_name = "r", tps_limit_tps = 50 }
local d = fake_dict(500)   -- EWMA 500 ≥ 400
local r_nil = ttft.ttft_assess(t_opts, d, nil, false)
local r_one = ttft.ttft_assess(t_opts, d, nil, false, 1)
eq("ttft mult=nil == mult=1 (hit)", r_nil.hit, r_one.hit)
eq("ttft mult=nil hit", r_nil.hit, true)
eq("ttft mult=1.5 soft zone passes (500<600)", ttft.ttft_assess(t_opts, d, nil, false, 1.5).hit, false)
eq("ttft mult=1.2 hits (500>=480)", ttft.ttft_assess(t_opts, d, nil, false, 1.2).hit, true)
eq("ttft mult=0.9 release hits (500>=360)", ttft.ttft_assess(t_opts, d, nil, false, 0.9).hit, true)
eq("ttft hit_limit reports raw threshold", r_nil.hit_limit, 400)
local dnil = fake_dict(nil)
eq("ttft nil EWMA does not hit at mult=0.9 (flag-clear path)", ttft.ttft_assess(t_opts, dnil, nil, false, 0.9).hit, false)

local dp = fake_dict(40)   -- OTPS 40 <= 50 -> trips
local p_nil = tps.tps_assess(p_opts, dp, nil, false)
local p_one = tps.tps_assess(p_opts, dp, nil, false, 1)
eq("tps mult=nil == mult=1 (hit)", p_nil.hit, p_one.hit)
eq("tps mult=nil hit", p_nil.hit, true)
eq("tps mult=1.5 soft zone passes (40>33.3)", tps.tps_assess(p_opts, dp, nil, false, 1.5).hit, false)
eq("tps mult=1.1 hits (40<=45.45)", tps.tps_assess(p_opts, dp, nil, false, 1.1).hit, true)
eq("tps hit_limit reports raw threshold", p_nil.hit_limit, 50)
eq("tps nil EWMA does not hit", tps.tps_assess(p_opts, fake_dict(nil), nil, false).hit, false)

-- == parse_usage (the actual production function, the same one access.lua calls) ==
print("== parse_usage ==")
local PU = sp.parse_usage
-- prompt_tokens_details must NOT match as prompt_tokens (closing-quote guard)
local p1, c1 = PU('{"usage":{"prompt_tokens":120,"prompt_tokens_details":{"cached_tokens":80}}}')
eq("prompt_tokens_details not parsed as prompt", p1, 120)
eq("cached_tokens parsed", c1, 80)
-- the LAST occurrence wins (decoy earlier in the tail)
local p2, c2 = PU('"prompt_tokens":5 ... "prompt_tokens":42,"cached_tokens":7')
eq("last prompt_tokens wins", p2, 42)
eq("last cached_tokens wins", c2, 7)
-- only prompt_tokens_details (no prompt_tokens): ptok is nil (no_cached_field)
local p3, c3 = PU('{"usage":{"prompt_tokens_details":{"cached_tokens":80},"completion_tokens":3}}')
eq("missing prompt_tokens -> nil", p3, nil)
eq("cached_tokens still parsed", c3, 80)
-- non-string defence
local p4, c4 = PU(nil)
eq("nil tail -> nil, nil", p4 == nil and c4 == nil, true)
local p5, c5 = PU("no tokens here")
eq("tail without fields -> nil, nil", p5 == nil and c5 == nil, true)

-- == pure entry points backing the factory path (derive_table / validate_table) ==
print("== pure entry points ==")
local dt = sp.derive_table(0.85, 1.5, 32)
eq("derive_table matches validate_opts (bucket count)", #dt, 6)
eq("derive_table share@2", dt[2].share, 0.88)
truthy("validate_table valid -> list", sp.validate_table({ { min_turn = 1, share = 0.5, tol = 1 } }))
eq("validate_table invalid -> nil", sp.validate_table({ { min_turn = 2, share = 0.5, tol = 1 } }), nil)

print(string.format("\n%d passed, %d failed", pass, fail))
os.exit(fail == 0 and 0 or 1)
