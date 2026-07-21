-- openresty/lua/reqtransform.lua
-- sid 提取 from_metadata + cch-strip + kimi tool_id 规范化 + hoist system

local cjson = require "cjson.safe"
local _nonblank = _G._nonblank

local function from_metadata(md, keyname)
    if type(md) ~= "table" then return nil end
    local prefix = "body." .. keyname

    -- 3. metadata.session_id
    if _nonblank(md.session_id) then
        return tostring(md.session_id), prefix .. ".session_id"
    end

    -- 4/5/6. metadata.user_id fallback (Claude CLI convention)
    local uid = md.user_id
    if type(uid) == "table" then
        if _nonblank(uid.session_id) then
            return tostring(uid.session_id), prefix .. ".user_id.session_id"
        end
    elseif type(uid) == "string" and _nonblank(uid) then
        local parsed = cjson.decode(uid)
        if type(parsed) == "table" and _nonblank(parsed.session_id) then
            return tostring(parsed.session_id),
                   prefix .. ".user_id[json].session_id"
        end
        -- plain string user_id used directly
        return uid, prefix .. ".user_id[plain]"
    end
    return nil
end

-- 剥掉 Claude Code 注入到 system prompt 前缀的
-- "x-anthropic-billing-header: cc_version=...; cc_entrypoint=...; cch=XXXX;"
-- 这串每次请求 cch hash 都不同，会让 vllm prefix cache 第一段直接失配。
-- 落点观察到两处：(a) Anthropic 原生 top-level system 字段
--                (b) OpenAI 格式 messages[0].content（system role 在第 0 条）
local CCH_RE = [[^x-anthropic-billing-header:\s*cc_version=[^;]*;\s*cc_entrypoint=[^;]*;\s*cch=[^;]*;]]

local function _strip_cch(s)
    if type(s) ~= "string" then return s, false end
    local new, n, err = ngx.re.sub(s, CCH_RE, "", "jo")
    if not new then return s, false end
    return new, (n and n > 0)
end

function _G.strip_cch_in_req(req)
    local changed = false
    -- (a) Anthropic 原生：top-level system，可能是 string 或 [{type=text,text=...}]
    local sys = req.system
    if type(sys) == "string" then
        local nv, ch = _strip_cch(sys); if ch then req.system = nv; changed = true end
    elseif type(sys) == "table" then
        for _, p in ipairs(sys) do
            if type(p) == "table" and type(p.text) == "string" then
                local nv, ch = _strip_cch(p.text); if ch then p.text = nv; changed = true end
            end
        end
    end
    -- (b) OpenAI 格式：messages[0].content（cch 实测落在第一条 message 开头）
    local msgs = req.messages
    if type(msgs) == "table" then
        local m = msgs[1]
        if type(m) == "table" then
            if type(m.content) == "string" then
                local nv, ch = _strip_cch(m.content); if ch then m.content = nv; changed = true end
            elseif type(m.content) == "table" then
                for _, p in ipairs(m.content) do
                    if type(p) == "table" and type(p.text) == "string" then
                        local nv, ch = _strip_cch(p.text); if ch then p.text = nv; changed = true end
                    end
                end
            end
        end
    end
    return changed
end

-- ── Kimi tool_call_id 规范化（默认关，灰度开）──
-- OpenAI 风格 id (call_xxx / Name:N / namen) 改写为 Kimi 内部期望的
-- functions.<fn_name>:<idx> 格式。修复"finish=stop 但 content 空 + raw
-- markers 没 parse" 类 bug（见 docs/openresty_normalize_plan.md）。
-- 切换：/_kimi_normalize_toggle?on=0|1 热切，写入 ngx.shared.cch_ctl
-- 复用同一 dict 的 kimi_normalize_enabled key，跨 reload 持久。
_G.KIMI_NORMALIZE_DEFAULT_ENABLED = true

-- 触发 normalize 的 model 白名单（兼容大小写、第三方 namespace）
_G.KIMI_NORMALIZE_MODELS = {
    ["kimi-k2.5"]                = true,
    ["kimi-k2.6"]                = true,
    ["Kimi-K2.5"]                = true,
    ["Kimi-K2.6"]                = true,
    ["moonshotai/kimi-k2.5"]     = true,
    ["moonshotai/kimi-k2.6"]     = true,
}

-- ── tool_call id 编号策略:两种(默认 rerank 全局重排 / hash 内容哈希)────────────────
-- _G.KIMI_ID_STRATEGY:"rerank"(默认,全局顺位,匹配 Kimi 服务原生编号规则)| "hash"(内容哈希/challall)。
-- 两种策略都返回 (id_map, order),只收 new!=old 的条目;由 _kimi_id_map 按策略分派,供 struct/surgical 共用。
_G.KIMI_ID_STRATEGY = _G.KIMI_ID_STRATEGY or "rerank"

-- 计算单个 tool_call id 的内容哈希目标 → functions.<fn>:<1e6+crc32(fn+args)%1e6>(hash 策略用)。
local function _stable_toolid(tid, fn, args)
    local content = fn .. "\1" .. (type(args) == "string" and args or "")
    return "functions." .. fn .. ":" .. tostring(1000000 + (ngx.crc32_long(content) % 1000000))
end

-- 策略①【默认】全局重排 → functions.<fn>:<全局位置序号>。
-- 「全局位置序号」= 该 tool_call 在整段对话所有 tool_calls 里的顺位(0,1,2…,跨函数共用一个计数器)——
-- 与 Kimi 服务原生 id 规则同源(实测原生单响应内 get_weather:0/add:1/get_weather:2 全局+1;跨轮非连续见
-- docs「b300 kimi 原生 tool_call id 编号实测」)。客户端如实回传原生位置号 → new==old → 天然幂等零改动;
-- 发 call_x/read_1/toolu_… → 按位置编成正确 functions.fn:位置。pos 对每个 tool_call 递增(即使跳过改写)。
local function _kimi_id_map_rerank(msgs)
    local id_map, order, pos = {}, {}, 0
    for _, m in ipairs(msgs) do
        if type(m) == "table" and type(m.tool_calls) == "table" then
            for _, tc in ipairs(m.tool_calls) do
                if type(tc) == "table" then
                    local tid = tc.id
                    local fn  = type(tc["function"]) == "table" and tc["function"].name
                    -- fn 仅收安全名 [A-Za-z0-9_.-],避免 regex 注入;id 需非空字符串
                    if type(tid) == "string" and _nonblank(tid)
                       and type(fn) == "string" and _nonblank(fn) and fn:match("^[%w_%-%.]+$") then
                        local newid = string.format("functions.%s:%d", fn, pos)
                        if newid ~= tid and id_map[tid] == nil then
                            id_map[tid] = newid
                            order[#order + 1] = tid
                        end
                    end
                    pos = pos + 1   -- 每个 tool_call 都占一个位置(即使跳过改写),保持顺位与 Kimi 一致
                end
            end
        end
    end
    return id_map, order
end

-- 策略②【保存,非默认】全内容哈希(challall)→ functions.<fn>:<crc32(fn+args)>。
-- 内容寻址、稳定、不漂、收敛(同内容同 id);代价=同内容调用塌成同 id(良性,靠消息顺序区分)。
local function _kimi_id_map_hash(msgs)
    local id_map, order = {}, {}
    for _, m in ipairs(msgs) do
        if type(m) == "table" and type(m.tool_calls) == "table" then
            for _, tc in ipairs(m.tool_calls) do
                if type(tc) == "table" then
                    local tid = tc.id
                    local f   = tc["function"]
                    local fn  = type(f) == "table" and f.name
                    local args = type(f) == "table" and f.arguments   -- 参数字符串(供内容哈希)
                    if type(tid) == "string" and _nonblank(tid)
                       and type(fn) == "string" and _nonblank(fn) and fn:match("^[%w_%-%.]+$") then
                        local newid = _stable_toolid(tid, fn, args)
                        if newid ~= tid and id_map[tid] == nil then
                            id_map[tid] = newid
                            order[#order + 1] = tid
                        end
                    end
                end
            end
        end
    end
    return id_map, order
end

-- 策略③【实验】rerank_maxlast:前 n-1 个 tool_call 按全局位置 0,1,…,n-2 重排,
-- 最后一个 tool_call → functions.<fn>:<请求里所有原始 id 的最大 idx>(无 conforming idx 时兜底用位置 n-1)。
local function _kimi_id_map_rerank_maxlast(msgs)
    local calls, max_idx = {}, -1
    for _, m in ipairs(msgs) do
        if type(m) == "table" and type(m.tool_calls) == "table" then
            for _, tc in ipairs(m.tool_calls) do
                if type(tc) == "table" then
                    local tid = tc.id
                    local fn  = type(tc["function"]) == "table" and tc["function"].name
                    if type(tid) == "string" and _nonblank(tid)
                       and type(fn) == "string" and _nonblank(fn) and fn:match("^[%w_%-%.]+$") then
                        calls[#calls + 1] = { tid = tid, fn = fn }
                        local prefix = "functions." .. fn .. ":"
                        if tid:sub(1, #prefix) == prefix then
                            local rest = tid:sub(#prefix + 1)
                            if rest:match("^%d+$") then
                                local v = tonumber(rest)
                                if v and v > max_idx then max_idx = v end
                            end
                        end
                    end
                end
            end
        end
    end
    local nc = #calls
    if nc == 0 then return {}, {} end
    if max_idx < 0 then max_idx = nc - 1 end
    local id_map, order = {}, {}
    for i, c in ipairs(calls) do
        local pos = (i == nc) and max_idx or (i - 1)   -- 最后一个用 max_idx,其余用全局位置
        local newid = string.format("functions.%s:%d", c.fn, pos)
        if newid ~= c.tid and id_map[c.tid] == nil then
            id_map[c.tid] = newid
            order[#order + 1] = c.tid
        end
    end
    return id_map, order
end

-- 策略④【实验】keepinc:尽量保留原本递增的 id(即使非连续,如原生 0,2,5,9,14),只重写破坏递增的。
-- 维护 nxt 计数器:某 tool_call 的 id 若是 functions.<本fn>:<int> 且 idx>=nxt(维持递增)→ 保留不改(nxt=idx+1);
-- 否则(非规范 / fn 不符 / idx<nxt)→ 重写为 functions.<fn>:<nxt>(nxt+=1)。
-- 例:原 id 0,1,call_x,call_y,3 → 0,1,2,3,4(0/1 保留;call_x→2,call_y→3;末尾 3<4 →4)。
local function _kimi_id_map_keepinc(msgs)
    local id_map, order, nxt = {}, {}, 0
    for _, m in ipairs(msgs) do
        if type(m) == "table" and type(m.tool_calls) == "table" then
            for _, tc in ipairs(m.tool_calls) do
                if type(tc) == "table" then
                    local tid = tc.id
                    local fn  = type(tc["function"]) == "table" and tc["function"].name
                    if type(tid) == "string" and _nonblank(tid)
                       and type(fn) == "string" and _nonblank(fn) and fn:match("^[%w_%-%.]+$") then
                        local idx = nil
                        local prefix = "functions." .. fn .. ":"
                        if tid:sub(1, #prefix) == prefix then
                            local rest = tid:sub(#prefix + 1)
                            if rest:match("^%d+$") then idx = tonumber(rest) end
                        end
                        if idx ~= nil and idx >= nxt then
                            nxt = idx + 1                      -- 维持递增 → 保留(不进 map)
                        else
                            local newid = string.format("functions.%s:%d", fn, nxt)
                            if newid ~= tid and id_map[tid] == nil then
                                id_map[tid] = newid
                                order[#order + 1] = tid
                            end
                            nxt = nxt + 1
                        end
                    end
                end
            end
        end
    end
    return id_map, order
end

-- 分派:按 _G.KIMI_ID_STRATEGY 选策略(默认 rerank 全局重排)。struct 与 surgical 两条路径共用,保证一致。
local function _kimi_id_map(msgs)
    if _G.KIMI_ID_STRATEGY == "hash" then
        return _kimi_id_map_hash(msgs)
    elseif _G.KIMI_ID_STRATEGY == "rerank_maxlast" then
        return _kimi_id_map_rerank_maxlast(msgs)
    elseif _G.KIMI_ID_STRATEGY == "keepinc" then
        return _kimi_id_map_keepinc(msgs)
    end
    return _kimi_id_map_rerank(msgs)
end

-- 把 req 里的 tool_call id 规范化为 functions.<fn>:<idx>(编号见 _stable_toolid / _kimi_id_map),
-- 同时同步 role=tool 的 tool_call_id 引用。返回改动条目数（≥0）。
-- req=非 table 或非 Kimi 模型时直接返 0 不动。内部 pcall 兜底,异常返 0 不让上游整体挂掉。
function _G.normalize_kimi_tool_ids(req)
    if type(req) ~= "table" then return 0 end
    if not _G.KIMI_NORMALIZE_MODELS[req.model] then return 0 end
    local msgs = req.messages
    if type(msgs) ~= "table" then return 0 end

    local ok, changed = pcall(function()
        local id_map = _kimi_id_map(msgs)
        -- apply:遍历所有 message,改 tool_calls[].id 与 tool_call_id 引用(map 里没有的孤儿保持原值)
        local n_changed = 0
        for _, m in ipairs(msgs) do
            if type(m) == "table" then
                if type(m.tool_calls) == "table" then
                    for _, tc in ipairs(m.tool_calls) do
                        if type(tc) == "table" and id_map[tc.id] then
                            tc.id = id_map[tc.id]
                            n_changed = n_changed + 1
                        end
                    end
                end
                if id_map[m.tool_call_id] then
                    m.tool_call_id = id_map[m.tool_call_id]
                    n_changed = n_changed + 1
                end
            end
        end
        return n_changed
    end)

    if not ok then
        ngx.log(ngx.ERR, "normalize_kimi_tool_ids pcall err: ", tostring(changed))
        return 0
    end
    return changed or 0
end

-- ── 字符串级(byte-preserving)transform ──────────────────────────────────────
-- 动机:strip_cch / normalize 若走 decode→改 table→cjson.encode,会重排请求里
-- 每个 JSON 对象的 key 序(Lua table 无序)。而 chat template 的 `tools|tojson` 把工具 schema
-- 原样序列化进 prompt 前部 → 重排即 prefix-cache 从工具声明处大面积 miss(token 级实测:
-- 见 docs/openresty_normalize_plan.md「重编码掉缓存」)。故改为在 raw body 字符串上定点替换,
-- 保住除改动点外全部字节 = 工具声明不重排 = 不掉缓存。decode 仍用于「分析」(算 id_map),不 encode。
-- 依赖 ngx.re(PCRE)。

-- strip_cch(surgical):删紧跟某个 content/text 值开头的 cch 前缀,保留该值其余内容。
-- 与结构版 CCH_RE 对齐:只删到 `cch=…;`,不吞后续空白(严格行为等价)。
-- ⚠️ 三段值用 [^;"]*(不是 [^;]*):既排 `;` 分隔符,也排闭引号 `"`——否则残缺 cch
--    (如缺尾 `;`)会让 [^;]* 冲出 JSON 字符串边界,吞到 body 里下一个 `;` 为止 → 静默
--    损坏请求(F1)。代价:cch 值内含转义引号 `\"` 时该段会 match 失败而漏剥;漏剥
--    (原样透传)远好于吞 body,可接受。
local _CCH_RAW = [[("(?:content|text)"\s*:\s*")x-anthropic-billing-header:\s*cc_version=[^;"]*;\s*cc_entrypoint=[^;"]*;\s*cch=[^;"]*;]]
function _G.surgical_strip_cch(body)
    local new, n = ngx.re.gsub(body, _CCH_RAW, "$1", "jo")
    if not new then return body, 0 end
    return new, (n or 0)
end

-- normalize(surgical):用已 decode 的 req 算 id_map(与结构版共用 _kimi_id_map,分配值逐字节一致),
-- 再在 raw body 上 key+值锚定替换 id 子串。返回 (new_body, 真实替换处数, id_map)。
-- 仅改 tool_call id;content:null 不动(原样透传)。
-- ⚠️ 单趟 gsub + 查表 replacer(不是逐条 old 循环 gsub):逐条循环时若某个后出现的 old
--    恰等于前一条的 new(客户端混发原生 functions.Bash:0 + 会被映射成 functions.Bash:0 的
--    原始 id),后一轮会把前一轮刚写进去的串再改掉 → 静默串号(F3)。单趟一次扫描整个
--    body、每个匹配点独立查表替换,无链式效应。
-- ⚠️ pattern 用**静态常量**(不把 id 拼进 alternation):dynamic pattern 每请求都是新串 →
--    ngx.re 的 `o`(compile-once)缓存永远命不中、只增,填满 lua_regex_cache_max_entries(默认
--    1024)后刷 WARN 且把真正静态的热 pattern 挤掉。静态 pattern 全局仅 1 条、JIT 编译一次永久
--    复用。代价:它会匹配 body 里**所有** "id"/"tool_call_id" 字段(非 map 的回调原样返回),故
--    gsub 的返回 cnt 含未改项,**不能拿它当替换数** → 用闭包 n_repl 只数真正改掉的(否则计数虚高)。
--    [^"]*:tool_call id 从不含引号(call_x / functions.fn:N / toolu_…),与原 alternation 等价。
local _ID_NORM_PAT = [[("(?:id|tool_call_id)"\s*:\s*")([^"]*)"]]
function _G.surgical_normalize(body, req, models)
    if type(req) ~= "table" then return body, 0 end
    if not models[req.model] then return body, 0 end
    local msgs = req.messages
    if type(msgs) ~= "table" then return body, 0 end

    local id_map, order = _kimi_id_map(msgs)
    if #order == 0 then return body, 0, id_map end

    local n_repl = 0
    local new = ngx.re.gsub(body, _ID_NORM_PAT, function(m)
        local repl = id_map[m[2]]
        if not repl then return m[0] end   -- 不在 map(孤儿/已规范/其它 "id" 字段)→ 原样,不计数
        n_repl = n_repl + 1
        return m[1] .. repl .. "\""
    end, "jo")
    if not new then return body, 0, id_map end
    return new, n_repl, id_map
end

-- inject_usage(surgical):stream 请求补 stream_options.include_usage=true,byte-preserving。
-- 覆盖情形(caller 已保证 req.stream==true 且 include_usage≠true):
--   ① so 非 table(无 stream_options / null / 标量):
--        a. body 里已有 "stream_options":null → **就地替换**成对象(不追加,避免重复键 F5-null);
--        b. body 里有 "stream_options" 但非 null(标量/异常)→ 追加会造成重复键 → 跳过注入(仅告警);
--        c. body 里无 stream_options 键 → 在最外层对象结尾 `}` 前追加整个字段。锚点用**物理末尾 }**
--           (而非 "stream":true——后者可能先命中 messages content 里的同串);最外层闭括号是 body
--           最后一个非空白字符,唯一确定,不会误伤 content。
--   ② 有 stream_options 对象但无 include_usage 键 → 插到该对象开头(空对象不加逗号,非空加)。
--   ③ include_usage 已存在但非 true(false/null/标量)→ **就地改值** true(F5-false;全 byte-preserving,
--      不再回退 struct)。仅 include_usage 值是对象/数组等异常形态才跳过不注入。
-- 返回 (new_body, injected_bool)。
function _G.surgical_inject_usage(body, req)
    local so = req.stream_options
    if type(so) ~= "table" then
        -- a. 就地替换 "stream_options":null → 对象(content 里的同串被 JSON 转义 \" 打断,不会误命中)
        local new, n = ngx.re.sub(body, [[("stream_options"\s*:\s*)null\b]],
            [[$1{"include_usage":true}]], "jo")
        if new and n and n > 0 then return new, true end
        -- b. 有 stream_options 键但不是 null(标量/异常)→ 不敢追加(会重复键),跳过
        if ngx.re.find(body, [["stream_options"\s*:]], "jo") then
            return body, false
        end
        -- c. 无 stream_options 键 → 末尾最外层 } 前追加
        local new2, n2 = ngx.re.sub(body, [[(\})(\s*)$]],
            [[,"stream_options":{"include_usage":true}$1$2]], "jo")
        if new2 and n2 and n2 > 0 then return new2, true end
        return body, false
    elseif so.include_usage == nil then
        -- ② 有对象缺 include_usage 键 → 插到对象开头
        local new, n = ngx.re.sub(body, [[("stream_options"\s*:\s*\{)(\s*)(["}])]], function(m)
            if m[3] == "}" then return m[1] .. [["include_usage":true]] .. m[2] .. m[3] end
            return m[1] .. [["include_usage":true,]] .. m[2] .. m[3]
        end, "jo")
        if new and n and n > 0 then return new, true end
        return body, false
    else
        -- ③ include_usage 已存在但非 true(false/null/标量)→ **就地改值** true(不走 struct/不重排/不掉缓存)。
        --    key 锚定 + JSON 转义 → content 里 \"include_usage\":false 不误命中;首个匹配即真键。
        local new, n = ngx.re.sub(body,
            [[("include_usage"\s*:\s*)(?:false|null|"[^"]*"|-?[0-9][0-9.eE+\-]*)]],
            [[$1true]], "jo")
        if new and n and n > 0 then return new, true end
        return body, false   -- 值形态异常(对象/数组)→ 跳过(极罕见 client 错误)
    end
end

-- Anthropic /v1/messages 兼容：把 messages 里 role=system 的项抽进顶层 system。
-- 背景：Claude Code(agent 框架)除顶层 system 外,还额外往 messages 里塞一条 role:system
-- (装 "Available agent types" 那段)。真 Anthropic API 会容忍并 merge 进 system,但 sglang 的
-- anthropic-compat 端点严格校验 messages role 只能 user/assistant → 400。这里补上那步 merge。
-- content 支持 string 或 blocks;顶层 system 支持 string / list;返回移动条数。pcall 由调用方兜底。
function _G.hoist_system_msgs(req)
    local msgs = req.messages
    if type(msgs) ~= "table" then return 0 end
    local hoisted, kept, moved = {}, {}, 0
    for _, m in ipairs(msgs) do
        if type(m) == "table" and m.role == "system" then
            local c = m.content
            if type(c) == "string" then
                hoisted[#hoisted + 1] = { type = "text", text = c }
            elseif type(c) == "table" then
                for _, blk in ipairs(c) do
                    if type(blk) == "table" then
                        hoisted[#hoisted + 1] = blk
                    elseif type(blk) == "string" then
                        hoisted[#hoisted + 1] = { type = "text", text = blk }
                    end
                end
            end
            moved = moved + 1
        else
            kept[#kept + 1] = m
        end
    end
    if moved == 0 then return 0 end
    -- 顶层 system 归一成 list 再 append(原为 string 先包成一个 text block)
    local sys = req.system
    if type(sys) == "string" then
        sys = { { type = "text", text = sys } }
    elseif type(sys) ~= "table" then
        sys = {}
    end
    for _, blk in ipairs(hoisted) do sys[#sys + 1] = blk end
    req.system = sys
    req.messages = kept
    return moved
end

-- 路由前置：读 body → 剥 cch → 提取 sid。即使 sid 在 header 里命中，
-- body 也要读 + 剥（cch 命中率比 fast-path 节省的 read_body 重要得多）。
-- 返回 (sid, src)；sid 提取失败返回 nil 走 least_conn。
-- opts 可选：{ cch_ctl_dict, cch_default_enabled, disable_body_user_affinity }
-- 未传时取 ngx.ctx.route_opts；再为空取 K2.5 默认值（保持向后兼容老调用）。
function prepare_request(opts)
    opts = opts or ngx.ctx.route_opts or _G.__route_opts.k25
    local h = ngx.req.get_headers()

    -- 先从 header 取 sid（不能 return early，下面还要剥 body）
    local sid_from_header, src_from_header
    local hv = h["x-litellm-session-id"]
    if _nonblank(hv) then
        sid_from_header, src_from_header = tostring(hv), "header.x-litellm-session-id"
    else
        hv = h["x-claude-code-session-id"]
        if _nonblank(hv) then
            sid_from_header, src_from_header = tostring(hv), "header.x-claude-code-session-id"
        else
            hv = h["x-session-id"]
            if _nonblank(hv) then
                sid_from_header, src_from_header = tostring(hv), "header.x-session-id"
            end
        end
    end

    -- 必读 body：(1) 剥 cch；(2) header 没 sid 时回 body 找
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if not body or #body == 0 then
        local fname = ngx.req.get_body_file()
        if fname then
            local f = io.open(fname, "rb")
            if f then body = f:read("*a"); f:close() end
        end
    end

    -- 没 body：直接返回 header sid（若有）
    if not body or #body == 0 then
        return sid_from_header, src_from_header
    end

    local req = cjson.decode(body)
    if type(req) ~= "table" then
        return sid_from_header, src_from_header
    end

    -- model-keyed 路由(新格式 peers_by_model)用:从已 decode 的 body 取 model,
    -- 零额外成本。do_route 在 prepare_request 之后据此选子池;老格式不读此字段。
    ngx.ctx.req_model = req.model

    -- 供 reject_rules 规则引擎读取(do_route 在本函数返回后调用 _G.eval_reject_rules):
    -- 存【原始 body 字符串】(此刻 body 尚未被下方 struct 路径就地改 req 表 / set_body_data 改线体污染),
    -- eval 按需重新 decode → 恒看客户端原始请求,不受归一化路径(struct 改表 vs surgical 不改)影响。
    -- req_body_len = 原始体字节数(input_bytes 规则用)。无 body 时上方已提前返回 → 保持 nil,eval 自动 no-op。
    ngx.ctx.req_body_raw = body
    ngx.ctx.req_body_len = #body

    -- 剥 cch（受 cch_ctl 开关控制，默认开；toggle endpoint 热切，无需 reload）
    local cch_ctl = ngx.shared[opts.cch_ctl_dict or "cch_ctl"]
    local cch_default = opts.cch_default_enabled
    if cch_default == nil then cch_default = _G.CCH_STRIP_DEFAULT_ENABLED end
    local cch_enabled_raw = cch_ctl:get("enabled")
    local cch_enabled
    if cch_enabled_raw == nil then
        cch_enabled = cch_default
    else
        cch_enabled = (cch_enabled_raw == 1)
    end
    local cch_stripped_pending = false
    local kimi_normalized_pending = 0
    local usage_injected_pending = false

    -- Kimi tool_call_id normalize 开关（受 cch_ctl 开关 kimi_normalize_enabled 控制）
    -- 优先级：dict 热切值(/_kimi_normalize_toggle) > per-route opts.kimi_normalize_default_enabled。
    -- opts.kimi_normalize_default_enabled 由 register_route 保证非 nil(factory 未配则已回落全局
    -- _G.KIMI_NORMALIZE_DEFAULT_ENABLED,见 route.lua)。factory 显式设 false = 该路由默认关。
    local kn_raw = cch_ctl:get("kimi_normalize_enabled")
    local kn_enabled
    if kn_raw == nil then
        kn_enabled = opts.kimi_normalize_default_enabled
    else
        kn_enabled = (kn_raw == 1)
    end
    local kn_model = kn_enabled and _G.KIMI_NORMALIZE_MODELS[req.model] and true or false

    cch_ctl:incr("parsed_total", 1, 0)

    -- include_usage 注入开关(默认开;/_include_usage_toggle 可关。承重转换:off = 新格式 stream 拿不到 usage,
    -- 监控/计费会瞎——故默认开、dict 缺省即开;仅紧急时手动关)。与 cch/normalize 一样存 cch_ctl dict。
    local usage_raw = cch_ctl:get("include_usage_enabled")
    local usage_on = (usage_raw == nil) or (usage_raw == 1)
    -- include_usage 注入现**全 surgical**(byte-preserving):无 stream_options→追加、缺键→插键、
    -- 非 true(false/null/标量)→就地改值。均不重排 key、不掉 prefix cache。
    -- usage_mode: nil=无需 / "surgical"=注入。(2026-07 起不再有 "struct" 分支;唯一 struct 触发是 hoist)
    local usage_mode = nil
    if usage_on and opts.peers_by_model and req.stream then
        local so = req.stream_options
        if type(so) ~= "table" or so.include_usage ~= true then
            usage_mode = "surgical"
        end
    end
    local want_hoist = false
    if ngx.var.uri == "/v1/messages" and type(req.messages) == "table" then
        for _, m in ipairs(req.messages) do
            if type(m) == "table" and m.role == "system" then want_hoist = true; break end
        end
    end

    if want_hoist then
        -- ── 结构路径(decode→encode,会重排 key → 掉缓存。**唯一触发 = /v1/messages hoist**;
        --    include_usage 已全 surgical 化,不再进这里。usage_mode=="surgical" 若与 hoist 同现,
        --    则顺带在本路径注入,反正已 encode)──
        local req_dirty = false
        if cch_enabled then
            local ok_cch, did = pcall(_G.strip_cch_in_req, req)
            if not ok_cch then
                ngx.log(ngx.ERR, "strip_cch_in_req pcall err: ", tostring(did))
            elseif did then
                cch_stripped_pending = true; req_dirty = true
            end
        end
        if kn_enabled then
            local n = _G.normalize_kimi_tool_ids(req)   -- 内部已按 model 门控 + pcall 兜底
            if n and n > 0 then kimi_normalized_pending = n; req_dirty = true end
        end
        if usage_mode then   -- 仅 "surgical"+want_hoist 会落这(hoist 已 decode→encode,顺带注入不亏)
            local so = req.stream_options
            if type(so) ~= "table" then so = {}; req.stream_options = so end
            so.include_usage = true; req_dirty = true; usage_injected_pending = true
        end
        if want_hoist then
            local ok_h, moved = pcall(_G.hoist_system_msgs, req)
            if ok_h and moved and moved > 0 then req_dirty = true end
        end
        if req_dirty then
            local new_body = cjson.encode(req)
            if new_body then
                ngx.req.set_body_data(new_body)
            else
                ngx.log(ngx.ERR, "cjson.encode(req) failed after rewrite; falling back to original body")
                cch_stripped_pending = false; kimi_normalized_pending = 0; usage_injected_pending = false
            end
        end
    else
        -- ── surgical 路径(byte-preserving,保序,不掉 prefix cache)──
        -- 全部在 raw body 字符串上定点替换;req(已 decode)仅供 surgical_normalize 算 id_map。
        local newbody = body
        if cch_enabled then
            local nb, ncch = _G.surgical_strip_cch(newbody)
            newbody = nb
            if ncch and ncch > 0 then cch_stripped_pending = true end
        end
        if kn_model then
            local nb, nid = _G.surgical_normalize(newbody, req, _G.KIMI_NORMALIZE_MODELS)
            newbody = nb
            kimi_normalized_pending = kimi_normalized_pending + (nid or 0)
        end
        if usage_mode == "surgical" then
            local nb, injected = _G.surgical_inject_usage(newbody, req)
            if injected then
                newbody = nb; usage_injected_pending = true
            else
                -- 极端:stream 请求但正则没定位到注入点(body 结构异常)。不回退结构路径
                -- (回退会重排掉缓存,得不偿失),仅告警;include_usage 本轮未注入。
                ngx.log(ngx.WARN, "surgical_inject_usage: no injection point, include_usage skipped")
            end
        end
        if newbody ~= body then
            ngx.req.set_body_data(newbody)
        end
    end

    -- 计数器统一在此 incr(两路径共用;set_body_data 失败的结构路径已把 pending 清零)
    if cch_stripped_pending then
        ngx.ctx.cch_stripped = true
        cch_ctl:incr("stripped_total", 1, 0)
    end
    if kimi_normalized_pending > 0 then
        ngx.ctx.kimi_normalized = kimi_normalized_pending
        cch_ctl:incr("kimi_normalize_changes_total", kimi_normalized_pending, 0)
        cch_ctl:incr("kimi_normalize_req_count", 1, 0)
    end
    if usage_injected_pending then
        ngx.ctx.usage_injected = true
        cch_ctl:incr("include_usage_injected_total", 1, 0)   -- 与 cch/normalize 对齐的可观测计数
    end

    -- header 已经命中：sid 用 header 的，但 body 已经剥了
    if sid_from_header then
        return sid_from_header, src_from_header
    end

    -- body sid: litellm checks both metadata and litellm_metadata
    for _, key in ipairs({"litellm_metadata", "metadata"}) do
        local sid, src = from_metadata(req[key], key)
        if sid then return sid, src end
    end

    -- OpenAI standard `user` field (survives upstream-gateway body passthrough; used by client-plugin plugin)
    -- 可通过 opts.disable_body_user_affinity（或全局 _G.DISABLE_BODY_USER_AFFINITY 兜底）关闭。
    local disable_body_user
    if opts.disable_body_user_affinity ~= nil then
        disable_body_user = opts.disable_body_user_affinity
    else
        disable_body_user = _G.DISABLE_BODY_USER_AFFINITY
    end
    if not disable_body_user then
        local u = req.user
        if type(u) == "string" and _nonblank(u) then
            return u, "body.user"
        end
    end

    return nil
end
