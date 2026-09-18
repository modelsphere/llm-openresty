-- Unit test: an unset BODYLOG_LISTENER_HOST disables body logging entirely.
--
-- Previously the code fell back to a hardcoded listener (10.0.0.1) whenever
-- BODYLOG_LISTENER_HOST was unset, which meant any deployment that never
-- configured bodylog would still ship full request and response bodies --
-- prompts included -- to a host in another network. The fallback is gone: no
-- host means no logging.
--
-- The gate lives in bodylog_should_sample, which is `local`, so it is exercised
-- through its only caller, M.bodylog_capture_request. Disabled must return
-- BEFORE anything is captured (no body held in ngx.ctx, no frame assembled),
-- rather than failing later at the send site, where it would merely increment
-- drop_count and look indistinguishable from a listener outage.
--
-- Run:  resty -I ../lua --shdict 'bodylog_ctl 1m' utest_bodylog_gate.lua

local ok_count, fail_count = 0, 0
local function ok(msg) ok_count = ok_count + 1; print("  ok   - " .. msg) end
local function nok(msg) fail_count = fail_count + 1; print("  FAIL - " .. msg) end
local function eq(a, b, msg)
    if a == b then ok(msg) else nok(msg .. " (expected " .. tostring(b) .. ", got " .. tostring(a) .. ")") end
end

local bodylog = require "bodylog"

-- Minimal route opts: only the fields the sampling decision reads.
local opts = { bodylog_ctl_dict = "bodylog_ctl", bodylog_default_enabled = true, bodylog_default_pct = 100 }

-- Sampling is enabled at 100% throughout, so the ONLY thing under test is the
-- host gate. Without it these cases would both capture.
print("== host unset => logging off ==")
_G.BODYLOG_LISTENER_HOST = nil
ngx.ctx.bodylog_active = nil
-- pcall because, with the gate working, capture returns before touching the
-- request body; if the gate regressed it would reach ngx.req.read_body(), which
-- errors in the resty CLI. Either way bodylog_active tells us what happened.
local called_ok, err = pcall(bodylog.bodylog_capture_request, opts)
eq(ngx.ctx.bodylog_active, nil, "no host => request is NOT captured")
eq(called_ok, true, "no host => returns cleanly, does not read the body")
if not called_ok then print("       (error was: " .. tostring(err) .. ")") end

print("== empty host is treated as unset by session_base ==")
-- session_base.conf normalises "" to nil before assigning the global; mirror
-- that here so an empty env value can never be read as a hostname.
local raw = ""
_G.BODYLOG_LISTENER_HOST = (raw ~= "" and raw) or nil
ngx.ctx.bodylog_active = nil
pcall(bodylog.bodylog_capture_request, opts)
eq(ngx.ctx.bodylog_active, nil, "empty host => request is NOT captured")

print("== whitespace-only host is also treated as unset (the trim) ==")
-- session_base.conf trims before the empty check. Without the trim, a value of
-- spaces passes the ~= "" test and gets used as a hostname, failing later inside
-- logger.init with an unhelpful error. This mirrors that exact normalisation --
-- it is the only case the trim exists for, and it had no coverage until now.
for _, raw_ws in ipairs({ "   ", "\t", " \n " }) do
    local norm = raw_ws:match("^%s*(.-)%s*$")
    if norm == "" then norm = nil end
    _G.BODYLOG_LISTENER_HOST = norm
    ngx.ctx.bodylog_active = nil
    pcall(bodylog.bodylog_capture_request, opts)
    eq(ngx.ctx.bodylog_active, nil,
       "whitespace-only host (" .. string.format("%q", raw_ws) .. ") => NOT captured")
end

print("== host set => gate passes (capture proceeds) ==")
-- With a host configured the gate must NOT short-circuit. Proof that it
-- proceeded: it gets as far as the request-body call, which the resty CLI has no
-- request for -- so it either sets bodylog_active or raises from read_body.
-- A silent clean return with nothing set would mean the gate wrongly blocked.
_G.BODYLOG_LISTENER_HOST = "127.0.0.1"
ngx.ctx.bodylog_active = nil
local proceeded_ok = pcall(bodylog.bodylog_capture_request, opts)
local proceeded = (ngx.ctx.bodylog_active == true) or (proceeded_ok == false)
eq(proceeded, true, "host set => gate passes, capture is attempted")

print("== sampling switch still independent of the gate ==")
-- Host set but sampling disabled => still no capture, proving the new gate did
-- not replace or bypass the existing enabled/sample_pct decision.
_G.BODYLOG_LISTENER_HOST = "127.0.0.1"
ngx.ctx.bodylog_active = nil
pcall(bodylog.bodylog_capture_request,
      { bodylog_ctl_dict = "bodylog_ctl", bodylog_default_enabled = false, bodylog_default_pct = 100 })
eq(ngx.ctx.bodylog_active, nil, "sampling disabled => not captured even with a host")

print(string.format("\nSummary: %d ok, %d FAIL", ok_count, fail_count))
if fail_count > 0 then os.exit(1) end
