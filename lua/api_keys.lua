-- The single source of API keys, read from a file on disk. Keys are never baked
-- into the image.
--
-- Keeping them out of the image means whoever can pull the image cannot read the
-- production keys, and rotating a key no longer requires a rebuild and a release.
--
-- Why a file and not an environment variable: nginx reads `env` declarations
-- only when the master process starts. `openresty -s reload` does NOT re-read
-- them, so rotating a key held in an environment variable would require killing
-- the master -- dropping every in-flight request, which for LLM traffic means
-- streams that have been running for minutes. A reload, by contrast, re-executes
-- init_by_lua and therefore re-reads this file, so a key change costs nothing
-- but a graceful reload. (Both halves measured, not assumed.)
--
-- The file path comes from OPENRESTY_API_KEYS_FILE, defaulting to the path
-- below. The path is static configuration, so taking it from the environment is
-- fine -- it is the key *content* that has to survive a reload.
--
-- Format: `key1:owner1,key2:owner2`
--   * The owner label records who holds the key, so it can be traced when
--     something misbehaves and revoked individually.
--   * Supporting several keys at once is what makes rotation possible: callers
--     cannot all switch at the same instant. Issue the new key alongside the old
--     one, move callers over, then retire the old one. With a single key every
--     rotation would be an outage, which means nobody would ever rotate.
--   * The owner may be omitted (`key1,key2`); it is then recorded as "unnamed".
--
-- When no key file is readable, or it yields no keys, requests are ALLOWED THROUGH
-- rather than rejected. This is a deliberate fail-open: openresty is the public
-- entry point, and a misconfigured Secret returning 401 for the whole site is
-- worse than a short window without authentication. The fail-open is never
-- silent, though:
--   1. an error is logged during init;
--   2. /_health_status exposes _meta.api_keys_configured=false for monitoring.
-- The real danger of a silent fail-open is not the missing auth itself but that
-- nobody notices: the endpoint could stay open to the world for months.
local M = {}

-- Parse "k1:owner1,k2:owner2" into { [k1]="owner1", [k2]="owner2" }.
-- Tolerates surrounding whitespace, skips entries with an empty key, and records
-- "unnamed" when no colon is present.
function M.parse(spec)
    local keys, n = {}, 0
    for item in tostring(spec or ""):gmatch("[^,]+") do
        item = item:match("^%s*(.-)%s*$")            -- trim
        if item ~= "" then
            local k, who = item:match("^([^:]+):(.*)$")
            if not k then k, who = item, "unnamed" end
            k = k:match("^%s*(.-)%s*$")
            who = (who or ""):match("^%s*(.-)%s*$")
            if k ~= "" then
                keys[k] = (who ~= "") and who or "unnamed"
                n = n + 1
            end
        end
    end
    return keys, n
end

-- Default location. On Kubernetes a Secret is mounted here; on bare metal the
-- file is placed here by whatever provisions the host.
local DEFAULT_PATH = "/etc/openresty/api-keys/keys"

-- Read the key file. Returns the spec string, or nil plus a reason. A missing
-- file is an ordinary outcome (authentication simply off), not an error worth
-- distinguishing from an unreadable one at the call site.
-- Returns (spec, err, kind) where kind is one of:
--   "missing"    -- no such file; this deployment simply did not configure keys
--   "unreadable" -- the file is there but could not be opened or read
--
-- The distinction matters and must not be collapsed. Both end in fail-open, but
-- they mean opposite things: "missing" is a deployment that never intended to
-- authenticate, while "unreadable" is one that meant to and failed -- a wrong
-- defaultMode on the Secret, a bad mount, an ownership mistake. Reporting both
-- as "no keys loaded" leaves an operator unable to tell a deliberate open
-- endpoint from a broken one, which is exactly when they need to know.
--
-- io.open's third return value is errno on POSIX; 2 is ENOENT. Using the code
-- rather than matching the message text keeps this working under any locale.
function M.read_file(path)
    local fh, oerr, errno = io.open(path, "r")
    if not fh then
        return nil, oerr or "cannot open", (errno == 2) and "missing" or "unreadable"
    end
    local body = fh:read("*a")
    fh:close()
    if not body then return nil, "cannot read", "unreadable" end
    -- Trailing newlines are near-universal in mounted Secrets and in anything a
    -- human edits; parse() trims each entry but never sees them otherwise.
    return (body:gsub("%s+$", ""))
end

-- Loaded on every init_by_lua, which a reload re-executes -- that is precisely
-- what lets a key change take effect without restarting the master.
M.path = os.getenv("OPENRESTY_API_KEYS_FILE") or DEFAULT_PATH
local spec, read_err, read_kind = M.read_file(M.path)
local parsed, count = M.parse(spec)

M.keys = parsed
-- Read by /_health_status and by the access guard: false means authentication is
-- currently disabled and everything is let through.
M.configured = count > 0
-- Why there are no keys, for /_health_status to expose:
--   "ok"         -- keys loaded
--   "missing"    -- no key file; presumably deliberate
--   "unreadable" -- the file exists but could not be read; presumably a mistake
--   "empty"      -- readable but contained no usable entry
M.file_status = M.configured and "ok" or (read_kind or "empty")

if not M.configured then
    -- ngx.log is available during init_by_lua; operators must see this one.
    -- An unreadable file gets its own wording: that is a misconfiguration to fix,
    -- not a deployment that chose to run open, and the two must not read alike.
    if M.file_status == "unreadable" then
        ngx.log(ngx.ERR,
            "[api_keys] key file ", M.path, " EXISTS BUT COULD NOT BE READ (", read_err or "?",
            ") -- authentication is DISABLED and every request is allowed through.",
            " This looks like a broken mount or wrong file permissions, not an",
            " intentionally open endpoint. Check the Secret's defaultMode and mount.")
    else
        ngx.log(ngx.ERR,
            "[api_keys] no keys loaded from ", M.path, " (", read_err or "file present but yielded no keys",
            ") -- authentication is DISABLED, all requests are allowed through.",
            " In production mount it from a Kubernetes Secret (format key1:owner1,key2:owner2).")
    end
else
    ngx.log(ngx.INFO, "[api_keys] loaded ", count, " key(s) from ", M.path)
end

-- Extract the key from the Authorization header. Only "Bearer <key>" is
-- accepted, matching the MiniMax / OpenAI convention.
function M.parse_bearer(auth_header)
    return (auth_header or ""):match("^Bearer%s+(.+)$")
end

-- Fingerprint of a key table. access.lua uses it to tell whether the shared dict
-- still holds the current table: lua_shared_dict contents survive
-- `openresty -s reload`, so the old "seed once, never again" flag meant that
-- changing a key and reloading had no effect on LLM routes, while video routes --
-- which read the table directly -- picked it up immediately. Seeding by
-- fingerprint makes a reload reseed automatically.
--
-- The fingerprint covers the values as well as the keys. A value is the owner
-- label; hashing only key names would mean an owner change never triggers a
-- reseed and the dict keeps serving a stale value. Authentication only checks
-- for presence today, but leaving that kind of silent staleness around invites
-- a subtle bug later.
function M.fingerprint(keys)
    local ks = {}
    for k in pairs(keys) do ks[#ks + 1] = k end
    table.sort(ks)
    local parts = {}
    for i = 1, #ks do parts[i] = ks[i] .. "=" .. tostring(keys[ks[i]]) end
    return ngx.md5(table.concat(parts, ","))
end

-- Validate an Authorization header. Returns (ok, owner label).
-- With no keys configured everything is allowed through (see the fail-open note
-- at the top of this file) and the owner is reported as "unauthenticated".
function M.check(auth_header)
    if not M.configured then return true, "unauthenticated" end
    local key = M.parse_bearer(auth_header)
    if not key then return false, nil end
    local who = M.keys[key]
    if not who then return false, nil end
    return true, who
end

-- Access-phase guard for routes that do not go through the Lua routing engine
-- (the video routes rendered by autoconfig). Keeping it here rather than
-- rendering it into every conf means the 401 body and the Bearer parsing rule
-- exist in exactly one place.
--
-- public_re: regex (ngx.re syntax) for paths that skip authentication;
-- nil or "" means every path requires a key.
function M.guard(public_re)
    if public_re and public_re ~= "" and ngx.re.find(ngx.var.uri, public_re, "jo") then
        return
    end
    if M.check(ngx.req.get_headers()["authorization"]) then
        return
    end
    ngx.status = 401
    ngx.header["Content-Type"] = "application/json"
    ngx.say([[{"error":"missing or invalid api key"}]])
    return ngx.exit(401)
end

return M
