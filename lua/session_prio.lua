-- openresty/lua/session_prio.lua
-- Session turn-priority admission (feature "session_prio", default OFF).
--
-- The client gateway pins a session to the provider that served it, so a 429 on
-- turn N loses the whole session and its KV cache. The gateway marks every
-- request with:
--     x-phanrouter-session: {"session_id":"...","turn":N}
-- When enabled, this module sheds low turns first once a gate starts rejecting:
-- turn buckets 1 / 2-3 / 4-7 / ... map to a concurrency share (r_new -> 1.0) and
-- a TTFT/TPS tolerance (1.0 -> severe). No/malformed header counts as warm (top
-- bucket, separate counters) — no state is kept per session.
--
-- Gates decide *how many* requests get in (unchanged adaptive cc / EWMA
-- measurement); the turn decides *which* ones. Nothing below runs at all when
-- the feature is off: no header parse, no dict traffic, byte-identical 429s.
--
-- Pure helpers (parse/bucket/table build) take no ngx state; unit tests load
-- this file directly under resty.

local M = {}

local cjson = require "cjson.safe"

M.HEADER = "x-phanrouter-session"

-- Hardcoded fallbacks; route.lua M.DEFAULTS carries the same values so
-- /_route_opts shows one source of truth. A conf error must never break a
-- route, so validate_* logs ERR and swaps in these.
local DEF_R_NEW, DEF_SEVERE, DEF_FULL_TURN = 0.85, 1.5, 32
local DEF_RELEASE, DEF_WARN_FRAC = 0.9, 0.2
local DEF_T1_PROBES = 1

M.DEFAULTS = {
    r_new     = DEF_R_NEW,
    severe    = DEF_SEVERE,
    full_turn = DEF_FULL_TURN,
    release   = DEF_RELEASE,
    warn_frac = DEF_WARN_FRAC,
    t1_probes = DEF_T1_PROBES,
}

-- ── Effective on/off ─────────────────────────────────────────────────────────
-- Runtime toggle (cch_ctl key "session_prio_enabled", /_session_prio_toggle)
-- overrides the factory value opts.session_prio_default_enabled (route.lua
-- already normalised it, falling back to _G.SESSION_PRIO_DEFAULT). Tri-state:
-- 1 = on, 0 = off, nil = follow the factory. Same shape as reject_rules.
local function dict_enabled(opts)
    local ovr
    local ctl = opts.cch_ctl_dict and ngx.shared[opts.cch_ctl_dict]
    if ctl then ovr = ctl:get("session_prio_enabled") end
    if ovr ~= nil then return ovr == 1 end
    return opts.session_prio_default_enabled == true
end

-- Per-request gate: single shared-dict read (same cost profile as the
-- reject_rules/kimi/bodylog toggles), the ONLY cost when the feature is off.
function M.enabled(opts)
    return dict_enabled(opts)
end

-- ── Header parsing ──────────────────────────────────────────────────────────
-- Returns (info, class) where info = { turn = <int>, session_id = <str> } for
-- class "ok", else nil. class: "ok" | "missing" | "bad".
-- Valid = turn integer in [1, 1e9] AND session_id non-blank string of <= 256 chars.
-- Anything else (absent header, malformed JSON, wrong shape) does not block
-- the request: it lands in the "nohdr" pseudo-bucket treated as warm.
function M.parse_header(hv)
    if type(hv) ~= "string" or hv == "" then
        return nil, "missing"
    end
    local obj = cjson.decode(hv)
    if type(obj) ~= "table" then
        return nil, "bad"
    end
    local t, sid = obj.turn, obj.session_id
    -- The upper cap matters: cjson decodes 1e999 to inf, which passes the
    -- integer check (inf == floor(inf)), and cjson.encode later refuses inf,
    -- so the bodylog record carrying phan_turn would be lost.
    if type(t) ~= "number" or t < 1 or t > 1e9 or t ~= math.floor(t) then
        return nil, "bad"
    end
    if type(sid) ~= "string" or not sid:match("%S") or #sid > 256 then
        return nil, "bad"
    end
    return { turn = t, session_id = sid }, "ok"
end

-- ── Bucket table ────────────────────────────────────────────────────────────
-- Derived table: bounds 1, 2, 4, 8, ... up to full_turn. For bound b:
--   frac  = min(1, log2(b) / log2(full_turn))
--   share = r_new + (1 - r_new) * frac      -- concurrency ceiling factor
--   tol   = 1 + (severe - 1) * frac         -- EWMA threshold multiplier
-- With the defaults this gives share 0.85/0.88/0.91/0.94/0.97/1.0 and
-- tol 1.0/1.1/1.2/1.3/1.4/1.5 over the 6 buckets.
-- A session_prio_table override replaces this wholesale (validated first).
local function round3(x) return math.floor(x * 1000 + 0.5) / 1000 end

-- Pure (no ngx access), so unit tests and route registration both use it.
function M.derive_table(r_new, severe, full_turn)
    local t = {}
    -- log2 of the full-turn bound; per bound b, frac = log2(b)/log2(full_turn).
    local ltop = math.log(full_turn) / math.log(2)
    local b = 1
    while true do
        local frac = b > 1 and (math.log(b) / math.log(2)) / ltop or 0
        t[#t + 1] = {
            min_turn = b,
            frac     = round3(frac),
            share    = round3(r_new + (1 - r_new) * frac),
            tol      = round3(1 + (severe - 1) * frac),
        }
        if b >= full_turn then break end
        b = math.min(b * 2, full_turn)
    end
    return t
end

-- Session-table validation. A table is a list of { min_turn, share, tol } with
-- min_turn == 1 first, strictly ascending, share in (0,1], tol >= 1.
-- Pure: returns the list when valid, nil otherwise.
function M.validate_table(list)
    if type(list) ~= "table" or #list < 1 then return nil end
    local prev = 0
    for i, e in ipairs(list) do
        if type(e) ~= "table" then return nil end
        local b, s, tol = e.min_turn, e.share, e.tol
        if type(b) ~= "number" or b < 1 or b ~= math.floor(b) then return nil end
        if i == 1 and b ~= 1 then return nil end
        if b <= prev then return nil end
        prev = b
        if type(s) ~= "number" or s <= 0 or s > 1 then return nil end
        if type(tol) ~= "number" or tol < 1 then return nil end
    end
    return list
end

-- Called once from register_route (per reload), not on the request path.
-- Normalises the session_prio_* options onto opts, building opts._sp_table
-- (bucket list) and opts._sp_top (index of the top bucket). Invalid values get
-- an ERR + the hardcoded default, same style as the adaptive_cc_* checks.
function M.validate_opts(opts, name)
    local function num(key, def, lo, hi, lo_open, hi_open, integer)
        local v = opts[key]
        if v == nil then return def end
        if type(v) ~= "number"
           or (lo and (lo_open and v <= lo or v < lo))
           or (hi and (hi_open and v >= hi or v > hi))
           or (integer and v ~= math.floor(v)) then
            ngx.log(ngx.ERR, "[", name, "] ", key, "=", tostring(v),
                    " is invalid - falling back to ", def)
            return def
        end
        return v
    end
    opts.session_prio_r_new     = num("session_prio_r_new", DEF_R_NEW, 0, 1, true, false)
    opts.session_prio_severe    = num("session_prio_severe", DEF_SEVERE, 1, nil, true)
    opts.session_prio_full_turn = num("session_prio_full_turn", DEF_FULL_TURN, 2, nil, false, false, true)
    opts.session_prio_release   = num("session_prio_release", DEF_RELEASE, 0, 1, true, true)
    opts.session_prio_warn_frac = num("session_prio_warn_frac", DEF_WARN_FRAC, 0, 1, true, true)
    opts.session_prio_t1_probes = num("session_prio_t1_probes", DEF_T1_PROBES, 0, nil, false, false, true)
    if opts.session_prio_table ~= nil then
        if M.validate_table(opts.session_prio_table) then
            opts._sp_table = opts.session_prio_table
        else
            ngx.log(ngx.ERR, "[", name, "] session_prio_table is invalid (need "
                    .. "{min_turn,share,tol} entries, first min_turn==1, strictly ascending, "
                    .. "share in (0,1], tol >= 1) - using the derived table")
            opts._sp_table = M.derive_table(opts.session_prio_r_new,
                opts.session_prio_severe, opts.session_prio_full_turn)
        end
    else
        opts._sp_table = M.derive_table(opts.session_prio_r_new,
            opts.session_prio_severe, opts.session_prio_full_turn)
    end
    opts._sp_top = #opts._sp_table
    -- Feature factory-on but no counter dict declared: everything still works
    -- (counters are nil-safe no-ops) but the operator loses all visibility.
    if opts.session_prio_default_enabled and not ngx.shared.session_stat then
        ngx.log(ngx.ERR, "[", name, "] session_prio is on but lua_shared_dict session_stat "
                .. "is not declared - admission still graded, but counters/status are lost. "
                .. "Add `lua_shared_dict session_stat 1m;` to this route's conf")
    end
end

-- Bucket index for a turn: the largest min_turn <= turn. Turns beyond the top
-- bound clamp to the top bucket. nohdr traffic uses the top bucket's policy but
-- is counted separately (see counter functions below).
function M.bucket_of(table_, turn)
    local idx = 1
    for i, b in ipairs(table_) do
        if turn < b.min_turn then break end
        idx = i
    end
    return idx
end

-- ── Counters (session_stat dict) ────────────────────────────────────────────
-- Key layout, all prefixed like tps_key_prefix ("<route>:" [+ "<model>:"]):
--   <prefix>hdr:{ok|missing|bad}            header parse classes
--   <prefix>b<min_turn>:<what>              per-bucket cumulative counters
--   <prefix>bnohdr:<what>                   nohdr pseudo-bucket (same policy as
--                                            top bucket, kept visible apart)
-- what: admit, rej_conc, rej_ttft, rej_tps, ptok, ctok, no_cached_field.
-- No TTL: counters accumulate until a process restart (reject_stat semantics).
-- The dict is shared by every route; 1m holds thousands of keys, a route
-- needs (buckets+1) x 7 + 3 + 2 x ~a dozen minute keys — far below that.
-- Missing dict (a route conf without the declaration) => every call is a
-- nil-safe no-op; admission itself never depends on this dict.
local function incr(opts, key, n)
    local d = ngx.shared.session_stat
    if d then d:incr(key, n or 1, 0) end
end

local function key_prefix(opts)
    local p = (opts.route_name or "?") .. ":"
    if opts.peers_by_model then
        local m = ngx.ctx and ngx.ctx.req_model
        p = p .. (m or "?") .. ":"
    end
    return p
end

-- Per-bucket counter key. bucket: index into _sp_table, or the string "nohdr".
function M.ctr_key(opts, bucket, what)
    local pre = key_prefix(opts)
    if bucket == "nohdr" then
        return pre .. "bnohdr:" .. what
    end
    return pre .. "b" .. tostring(opts._sp_table[bucket].min_turn) .. ":" .. what
end

function M.count_hdr(opts, class)
    if class ~= "ok" and class ~= "bad" then class = "missing" end
    incr(opts, key_prefix(opts) .. "hdr:" .. class)
end
function M.count(opts, bucket, what, n) incr(opts, M.ctr_key(opts, bucket, what), n) end

-- ── Usage parsing (do_log_release ptok/ctok) ────────────────────────────────
-- Pull the LAST "prompt_tokens" and "cached_tokens" out of the TPS response
-- tail. Same rule as completion_tokens upstream: the trailing usage chunk is
-- the authority, and response text can carry decoy literals earlier (tool-call
-- JSON echo), so gmatch keeps the final occurrence. The closing quote in
-- "prompt_tokens" is load-bearing: without it "prompt_tokens_details" matches.
-- Returns (ptok, ct); either is nil when absent from the tail.
-- The TPS tail is ~2 KB and the usage chunk is by contract the LAST chunk, so
-- anchor on the last literal `"usage":{` with plain finds and only gmatch from
-- there on. The object brace is part of the anchor so a bare-content mention
-- like "usage": null (or a string echo) cannot steer the scan; with no
-- `"usage":{` at all, fall back to the full scan (the old semantics).
-- Measured at ~21 us for the two full-tail gmatch scans vs ~2 us anchored.
function M.parse_usage(tail)
    if type(tail) ~= "string" then return nil, nil end
    local pos
    local i = 1
    while true do
        local j = tail:find('"usage":{', i, true)
        if not j then break end
        pos = j
        i = j + 1
    end
    local scan = pos and tail:sub(pos) or tail
    local ptok, ct
    for n in scan:gmatch('"prompt_tokens"%s*:%s*(%d+)') do ptok = tonumber(n) end
    for n in scan:gmatch('"cached_tokens"%s*:%s*(%d+)') do ct = tonumber(n) end
    return ptok, ct
end

-- ── Turn-1 probe budget (separate counter, per window) ──────────────────────
-- A tripped gate gives turn 1 no probe slots, so a pool of almost-only-new
-- sessions records no samples until the EWMA expires, then opens all at once.
-- This lets up to session_prio_t1_probes turn-1 requests per window through,
-- ONLY while no warm request has hit the gate in the current window (a warm
-- probe means samples are flowing already). The budget lives on its OWN
-- counter and never touches the warm one.
-- kind: "ttft" or "tps" (dict, window and key prefix follow the gate).
-- Returns true if this turn-1 request may pass as a probe.
function M.t1_allow_probe(opts, kind)
    local budget = tonumber(opts.session_prio_t1_probes) or 0
    if budget <= 0 then return false end
    local dict_name = kind == "tps" and opts.tps_dict or opts.ttft_dict
    local window    = kind == "tps" and opts.tps_probe_window or opts.ttft_probe_window
    local prefix_fn = kind == "tps"
        and require("tps").tps_key_prefix or require("ttft").ttft_key_prefix
    local td = dict_name and ngx.shared[dict_name]
    if not td then return false end
    local win = math.floor(ngx.now() / window)
    local pre = prefix_fn(opts)
    -- Warm probe counter for THIS window: if anything above zero exists, warm
    -- traffic has already re-opened a slot and turn 1 stays out.
    local warm = td:get(pre .. "probe:" .. win)
    if warm and warm > 0 then return false end
    -- Same window/expiry handling as the warm counter: rolling key, fresh keys
    -- expire after 2 windows. incr is atomic; a nil (full dict) fails closed
    -- here -- refusing a probe slot is safe, unlike the warm path's fail-open.
    local key = pre .. "probe_t1:" .. win
    local n = td:incr(key, 1, 0)
    if not n then return false end
    if n == 1 then td:expire(key, window * 2) end
    return n <= budget
end

-- ── Turn-1 hysteresis flag (ttft_dict, per pool) ────────────────────────────
-- Set when the plain 1.0x TTFT assess trips (any declared metric, EWMA known);
-- cleared when the release-level assess (mult = release < 1) no longer trips,
-- which also covers EWMA expired/nil (a nil EWMA cannot trip). While set,
-- turn-1 requests are rejected even in the band between release and 1.0x.
-- Read cheaply every request; WRITTEN ONLY ON STATE CHANGE (dict get before
-- set) so the hot path never writes. The flag carries ttft_ttl: if the feature
-- is hot-toggled off mid-trip the flag ages away instead of holding forever.
function M.turn1_flag_key(opts, model)
    local ttft = require "ttft"
    return ttft.ttft_key_prefix(opts, model) .. "turn1_hold"
end

-- Returns (held:bool, td, key). td/key are nil when the ttft dict is missing.
-- model is optional: on peers_by_model routes pass it explicitly. Without a
-- request context (admin endpoint, timer) ngx.ctx.req_model is nil and the
-- key would silently land on the "?"-prefixed layout that never holds flags.
function M.turn1_held(opts, model)
    local td = opts.ttft_dict and ngx.shared[opts.ttft_dict]
    if not td then return false, nil, nil end
    local key = M.turn1_flag_key(opts, model)
    return td:get(key) == 1, td, key
end

-- ── No-header WARN rate limiting (session_stat, per-minute keys) ────────────
-- Per-minute buckets <prefix>wm:<epoch_min>:{total,nohdr} with a short TTL.
-- When the nohdr share of the CURRENT last minute exceeds warn_frac, log a
-- WARN at most once per minute (dict:add with TTL 60 acts as the throttle).
-- No new timers: everything is driven by requests.
function M.note_nohdr_minute(opts, class)
    local d = ngx.shared.session_stat
    if not d then return end
    local pre = key_prefix(opts)
    local min = math.floor(ngx.time() / 60)
    local mk = pre .. "wm:" .. min .. ":"
    d:incr(mk .. "total", 1, 0, 130)
    if class == "ok" then return end
    d:incr(mk .. "nohdr", 1, 0, 130)
    local total = d:get(mk .. "total") or 0
    local nohdr = d:get(mk .. "nohdr") or 0
    -- Judge on the previous, complete minute once both counters exist there;
    -- judging the minute in progress would warn on the first nohdr request of
    -- a quiet pool. Fall back to the current minute when there is no history.
    local prev = pre .. "wm:" .. (min - 1) .. ":"
    local ptotal, pnohdr = d:get(prev .. "total"), d:get(prev .. "nohdr")
    if ptotal and ptotal > 0 then
        total, nohdr = ptotal, pnohdr or 0
    end
    if total > 0 and nohdr / total > (opts.session_prio_warn_frac or DEF_WARN_FRAC) then
        if d:add(pre .. "warn:nohdr", 1, 60) then
            ngx.log(ngx.WARN, "[", opts.route_name or "?", "] session_prio: ",
                    string.format("%.0f%%", 100 * nohdr / total),
                    " of requests arrived without a usable ", M.HEADER,
                    " header in the last minute (threshold ",
                    tostring(opts.session_prio_warn_frac or DEF_WARN_FRAC),
                    "); they are admitted as warm (top bucket)")
        end
    end
end

return M
