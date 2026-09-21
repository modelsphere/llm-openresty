-- Unit test: api_keys.lua loads keys from a FILE (not an environment variable).
--
-- Why this matters: keys used to come from OPENRESTY_API_KEYS. nginx reads `env`
-- declarations only at master startup and `-s reload` does not re-read them, so
-- rotating a key meant restarting the master and dropping every in-flight
-- stream. Reading from a file makes rotation a graceful reload, because a reload
-- re-executes init_by_lua and re-reads the file.
--
-- Run:  resty -I ../lua utest_api_keys_file.lua
-- (Pure functions plus a real temp file; no nginx instance, no shared dicts.)

local ok_count, fail_count = 0, 0
local function ok(msg) ok_count = ok_count + 1; print("  ok   - " .. msg) end
local function nok(msg) fail_count = fail_count + 1; print("  FAIL - " .. msg) end
local function eq(a, b, msg)
    if a == b then ok(msg) else nok(msg .. " (expected " .. tostring(b) .. ", got " .. tostring(a) .. ")") end
end

local function tmpname()
    return "/tmp/utest_api_keys_" .. tostring(math.random(1e9)) .. ".txt"
end
local function write(path, body)
    local fh = assert(io.open(path, "w")); fh:write(body); fh:close()
end

local api_keys = require "api_keys"

print("== read_file ==")
-- A missing file must be an ordinary "no keys" outcome, not a crash: that is the
-- fail-open path that keeps a misconfigured deployment serving instead of 401ing
-- the whole site.
local spec, err, kind = api_keys.read_file("/nonexistent/definitely/not/here")
eq(spec, nil, "missing file returns nil")
eq(type(err), "string", "missing file reports a reason")
-- The third return value separates "never configured" from "configured but
-- broken". Both fail open, but only the second is a misconfiguration to chase,
-- and collapsing them leaves an operator unable to tell which one they have.
eq(kind, "missing", "missing file is classified as missing, not unreadable")

print("== read_file: unreadable is NOT the same as missing ==")
-- Build a file this process cannot open. Running as root defeats it (root reads
-- mode 000), so skip rather than assert something untrue.
local nop = "/tmp/utest_noperm_" .. tostring(math.random(1e9))
local nfh = assert(io.open(nop, "w")); nfh:write("sk-x:owner\n"); nfh:close()
os.execute("chmod 000 '" .. nop .. "'")
local probe = io.open(nop, "r")
if probe then
    probe:close()
    print("  skip - 当前用户能读 mode 000(多半是 root),该分支无法在此验证")
else
    local s2, e2, k2 = api_keys.read_file(nop)
    eq(s2, nil, "unreadable file returns nil")
    eq(type(e2), "string", "unreadable file reports a reason")
    eq(k2, "unreadable", "unreadable file is classified as unreadable, not missing")
end
os.execute("chmod 644 '" .. nop .. "' 2>/dev/null; rm -f '" .. nop .. "'")

-- Mounted Secrets and hand-edited files almost always end in a newline; it must
-- not become part of the last key.
local p = tmpname()
write(p, "sk-a:admin,sk-b:teamB\n\n")
eq(api_keys.read_file(p), "sk-a:admin,sk-b:teamB", "trailing newlines are stripped")

local keys, n = api_keys.parse(api_keys.read_file(p))
eq(n, 2, "two keys parsed from file content")
eq(keys["sk-a"], "admin", "owner label preserved")
eq(keys["sk-b"], "teamB", "second owner label preserved")
eq(keys["sk-b\n"], nil, "no key carries a stray newline")
os.remove(p)

print("== empty / whitespace-only file ==")
local p2 = tmpname()
write(p2, "\n   \n")
local _, n2 = api_keys.parse(api_keys.read_file(p2))
eq(n2, 0, "whitespace-only file yields no keys (authentication off)")
os.remove(p2)

print("== path resolution ==")
-- The path may come from the environment (static config); the KEYS may not.
eq(type(api_keys.path), "string", "module records the path it loaded from")
local want = os.getenv("OPENRESTY_API_KEYS_FILE")
if want and want ~= "" then
    eq(api_keys.path, want, "OPENRESTY_API_KEYS_FILE is honoured")
else
    eq(api_keys.path, "/etc/openresty/api-keys/keys", "falls back to the default path")
end

print("== check() fail-open when unconfigured ==")
-- This test process has no key file, so M.configured must be false and check()
-- must let requests through while reporting them as unauthenticated.
if api_keys.configured then
    nok("expected no keys to be configured in this test process")
else
    local allowed, who = api_keys.check("Bearer anything")
    eq(allowed, true, "unconfigured => request allowed (deliberate fail-open)")
    eq(who, "unauthenticated", "unconfigured => reported as unauthenticated")
    local allowed2 = api_keys.check(nil)
    eq(allowed2, true, "unconfigured => even a missing header is allowed")
end

print("== check() enforces when configured ==")
-- Simulate a loaded state without reloading the module.
api_keys.keys, api_keys.configured = { ["sk-good"] = "admin" }, true
local a1, w1 = api_keys.check("Bearer sk-good")
eq(a1, true, "valid key accepted"); eq(w1, "admin", "owner returned")
eq(api_keys.check("Bearer sk-bad"), false, "unknown key rejected")
eq(api_keys.check("sk-good"), false, "non-Bearer header rejected")
eq(api_keys.check(nil), false, "missing header rejected when configured")

print(string.format("\nSummary: %d ok, %d FAIL", ok_count, fail_count))
if fail_count > 0 then os.exit(1) end
