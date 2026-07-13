-- openresty/lua/bodylog.lua
-- 请求/响应全量落盘(采样、抓取、SSE chunk 累积、二进制帧 finalize)

local cjson = require "cjson.safe"

-- ★ 默认配置（生产改这两行即可，master 重启后生效；toggle endpoint 仍可运行时覆盖）★
-- toggle（/_bodylog_toggle）写入 ngx.shared.bodylog_ctl，跨 reload 持久;
-- 只有 master 重启（不是 SIGHUP reload）才重置回下面的 DEFAULT。
_G.BODYLOG_DEFAULT_ENABLED = true    -- 默认开启（head+tail + 异步 timer 三层解耦，对请求路径零拖累）
_G.BODYLOG_DEFAULT_PCT     = 100     -- 默认 100% 采样；如需降采样改这里或 toggle 改

-- ★ listener 地址已移到机器级 env（见 init_worker_by_lua_block）★
-- 各机在 nginx.conf 用 `env BODYLOG_LISTENER_HOST=...;` 设置（默认 gateway-host
-- 10.0.0.1：集中式 listener，多 OpenResty 共用，落盘 gateway-host:/mnt/nvme1n1/nginx/bodylog/）。
-- cloud-b 网（gateway-host/12）应设 10.0.0.2。应急回退本机收数：env 设 127.0.0.1 +
-- systemctl start bodylog-listener。这样本文件跨所有部署字节一致，机器差异只在 nginx.conf。

_G.bodylog_max_req         = 2 * 1024 * 1024    -- 2 MB 请求 body 上限
-- 响应 body 双区缓存（head + tail ring buffer），总和 = head + tail。
-- head 装满后切到 tail 环形 buffer，最旧的 chunk 被淘汰，最终保留"开头 + 结尾"。
-- 这样长 SSE 流截断后仍能保留尾部 finish_reason / usage chunk。
_G.bodylog_max_resp_head   = 1536 * 1024        -- 1.5 MB
_G.bodylog_max_resp_tail   = 512 * 1024         -- 512 KB（足够装末尾几十 chunk + finish_reason + usage）
-- 注：worker 内 buffer + drop_limit 由 logger.init 在 init_worker 配置（256MB drop）
_G.bodylog_strip_hdrs  = {
    ["authorization"] = true,
    ["cookie"] = true,
    ["x-api-key"] = true,
    ["x-litellm-api-key"] = true,
    ["x-auth-token"] = true,
    ["proxy-authorization"] = true,
}

-- 决策：本次请求是否被采样。
-- 优先级：toggle endpoint 设过的 shared dict 值 > opts.bodylog_default_* > 全局常量 fallback。
-- opts 可选；未传时取 ngx.ctx.route_opts 或默认 K2.5。
function _G.bodylog_should_sample(opts)
    opts = opts or ngx.ctx.route_opts or _G.__route_opts.k25
    local ctl = ngx.shared[opts.bodylog_ctl_dict or "bodylog_ctl"]
    local default_en = opts.bodylog_default_enabled
    if default_en == nil then default_en = _G.BODYLOG_DEFAULT_ENABLED end
    local default_pct = opts.bodylog_default_pct or _G.BODYLOG_DEFAULT_PCT
    local en = ctl:get("enabled")
    if en == nil then en = default_en and 1 or 0 end
    if en ~= 1 then return false end
    local pct = ctl:get("sample_pct") or default_pct
    if pct >= 100 then return true end
    if pct <= 0 then return false end
    return math.random(100) <= pct
end

-- 在 access 阶段调用：决策 + 暂存 raw req body
-- 调用顺序：bodylog_capture_request → prepare_request。这里抓的是
-- 客户端原始 body（cch 还在）；prepare_request 之后会通过 set_body_data
-- 改写 nginx 内部 buffer，但已经存进 ngx.ctx 的字符串引用不受影响。
-- 落盘内容 = 客户端实际发送内容，可用于审计 / 排查 / replay。
function _G.bodylog_capture_request(opts)
    if not _G.bodylog_should_sample(opts) then return end
    ngx.ctx.bodylog_active = true
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if body then
        if #body > _G.bodylog_max_req then
            ngx.ctx.bodylog_req_body = body:sub(1, _G.bodylog_max_req)
            ngx.ctx.bodylog_req_truncated = true
        else
            ngx.ctx.bodylog_req_body = body
            ngx.ctx.bodylog_req_truncated = false
        end
    else
        -- get_body_data() 返回 nil 有两种原因，要区分：
        --   1. 请求本身就没 body（GET / Content-Length=0 / chunked-empty）→ truncated=false
        --   2. body 大于 client_body_buffer_size (2m) 被写入 temp file → truncated=true
        ngx.ctx.bodylog_req_body = ""
        local m = ngx.req.get_method()
        local hdrs = ngx.req.get_headers()
        local cl = tonumber(hdrs["content-length"] or "")
        local te = hdrs["transfer-encoding"]
        -- 无 body 的 method（GET/HEAD/OPTIONS/DELETE/TRACE）→ 不标 truncated
        local no_body_method = (m == "GET" or m == "HEAD" or m == "OPTIONS"
                                or m == "DELETE" or m == "TRACE")
        if no_body_method or (cl and cl == 0) then
            ngx.ctx.bodylog_req_truncated = false
        elseif te == "chunked" or (cl and cl > 0) then
            ngx.ctx.bodylog_req_truncated = true   -- 有 body 但 nginx 写到 temp file 没读
        else
            ngx.ctx.bodylog_req_truncated = false  -- 既无 CL 也非 chunked，按"无 body"处理
        end
    end
end

-- body_filter_by_lua 调用：累积响应 chunk 到 ngx.ctx (local Lua table，无锁)
-- 双区策略：
--   1) head 区（普通数组）累积到 _G.bodylog_max_resp_head 为止
--   2) head 满后开始向 tail 环形 deque 写新 chunk，超过 _G.bodylog_max_resp_tail
--      时从队首淘汰最旧 chunk —— O(1) 摊销（只移动 head_idx，不 table.remove）
--   3) finalize 时拼接 head + gap_marker + tail，保证末尾 finish_reason / usage 落盘
function _G.bodylog_filter_chunk()
    -- TTFT 测量(不受 bodylog 采样门控):仅对声明了 ttft_dict 的路由采。
    -- 首个非空响应 chunk 到达时记一次首-token 延迟,并探测是否流式响应
    -- (只有 text/event-stream 的 TTFT 才有意义;非流式首 chunk == 整个响应)。
    -- 首-chunk 计时 + 流式判定为 TTFT/TPS 共用(TPS 的 decode_time=总耗时-TTFT 依赖它),
    -- 故任一特性开就跑;ttft_first_chunk_t / ttft_is_stream 两个 ctx 变量两特性共享。
    local _ro = ngx.ctx.route_opts
    -- 缓存 TPS on/off 决策:body_filter 每 chunk 调一次,而 tps_dict_if_on 含 shared-dict get(__off),
    -- 长流(64K token / 数千 chunk)下会累积上千次无谓 dict 读。决策对单请求恒定,只在首次算一次。
    if _ro and ngx.ctx.tps_on == nil then
        ngx.ctx.tps_on = (_G.tps_dict_if_on(_ro) and true) or false
    end
    local _tps_on = ngx.ctx.tps_on
    if _ro and not ngx.ctx.ttft_first_chunk_t and (_G.ttft_dict_if_on(_ro) or _tps_on) then
        local c0 = ngx.arg[1]
        if c0 and #c0 > 0 then
            ngx.update_time()
            ngx.ctx.ttft_first_chunk_t = ngx.now() - ngx.req.start_time()
            local ct = ngx.header["Content-Type"]
            ngx.ctx.ttft_is_stream =
                (type(ct) == "string" and ct:find("event-stream", 1, true)) and true or false
        end
    end
    -- TPS:维护 ~2KB 滚动尾缓冲(usage chunk 永远在流末尾;不在单 chunk 上 find,防 usage 被切到
    -- 两个 body_filter chunk 边界上漏匹配)。真正 parse completion_tokens 放到 log 阶段(do_log_release)。
    if _tps_on then
        local c = ngx.arg[1]
        if c and #c > 0 then
            local t = (ngx.ctx.tps_tail or "") .. c
            ngx.ctx.tps_tail = #t > 2048 and t:sub(#t - 2048 + 1) or t
        end
    end
    if not ngx.ctx.bodylog_active then return end
    local chunk = ngx.arg[1]
    local eof   = ngx.arg[2]
    if chunk and #chunk > 0 then
        ngx.ctx.bodylog_chunk_count = (ngx.ctx.bodylog_chunk_count or 0) + 1
        -- bodylog_total_bytes：upstream 实际发来的原始 SSE wire 总字节（无 cap），
        -- 用于 meta.resp_bytes，可超 head+tail 总和。
        ngx.ctx.bodylog_total_bytes = (ngx.ctx.bodylog_total_bytes or 0) + #chunk
        if not ngx.ctx.bodylog_first_chunk_t then
            -- #8: TTFT 测量已在本函数顶部对同一首 chunk 算过则复用,省一次 update_time + 减法
            if ngx.ctx.ttft_first_chunk_t then
                ngx.ctx.bodylog_first_chunk_t = ngx.ctx.ttft_first_chunk_t
            else
                ngx.update_time()
                ngx.ctx.bodylog_first_chunk_t = ngx.now() - ngx.req.start_time()
            end
        end
        local head_size = ngx.ctx.bodylog_head_size or 0
        if head_size + #chunk <= _G.bodylog_max_resp_head then
            ngx.ctx.bodylog_chunks = ngx.ctx.bodylog_chunks or {}
            table.insert(ngx.ctx.bodylog_chunks, chunk)
            ngx.ctx.bodylog_head_size = head_size + #chunk
        else
            -- head 已满，切到 tail 环形 deque（{head_idx, tail_idx, bytes, [i]=chunk}）
            ngx.ctx.bodylog_resp_truncated = true
            local ring = ngx.ctx.bodylog_tail_ring
            if not ring then
                ring = { head_idx = 1, tail_idx = 0, bytes = 0 }
                ngx.ctx.bodylog_tail_ring = ring
            end
            ring.tail_idx = ring.tail_idx + 1
            ring[ring.tail_idx] = chunk
            ring.bytes = ring.bytes + #chunk
            local cap = _G.bodylog_max_resp_tail
            while ring.bytes > cap and ring.head_idx <= ring.tail_idx do
                local oldest = ring[ring.head_idx]
                if not oldest then break end
                ring[ring.head_idx] = nil
                ring.head_idx = ring.head_idx + 1
                ring.bytes = ring.bytes - #oldest
            end
        end
    end
    if eof then
        ngx.update_time()
        ngx.ctx.bodylog_last_chunk_t = ngx.now() - ngx.req.start_time()
    end
end

-- 工具：判断 string 是否合法 utf-8
function _G.bodylog_is_utf8(s)
    if not s or s == "" then return true end
    local i, n = 1, #s
    while i <= n do
        local b = s:byte(i)
        local need
        if b < 0x80 then need = 0
        elseif b < 0xC0 then return false
        elseif b < 0xE0 then need = 1
        elseif b < 0xF0 then need = 2
        elseif b < 0xF8 then need = 3
        else return false end
        for j = 1, need do
            if i + j > n then return false end
            local c = s:byte(i + j)
            if c < 0x80 or c >= 0xC0 then return false end
        end
        i = i + 1 + need
    end
    return true
end

-- log_by_lua 调用：cjson.encode 小 meta + 拼 binary 帧 → logger.log 异步发到 listener。
-- req_body / resp_body 走裸字节（绕过 cjson string escape 扫描），256K body 整 entry
-- 编码省 5+ms event-loop 阻塞。listener 端做 utf-8/SSE 抽取。
local _bit = require "bit"
local function _pack_u16(n)
    return string.char(_bit.band(_bit.rshift(n, 8), 0xff), _bit.band(n, 0xff))
end
local function _pack_u32(n)
    return string.char(
        _bit.band(_bit.rshift(n, 24), 0xff),
        _bit.band(_bit.rshift(n, 16), 0xff),
        _bit.band(_bit.rshift(n, 8), 0xff),
        _bit.band(n, 0xff))
end

function _G.bodylog_finalize(opts)
    if not ngx.ctx.bodylog_active then return end
    opts = opts or ngx.ctx.route_opts or _G.__route_opts.k25
    local ctl = ngx.shared[opts.bodylog_ctl_dict or "bodylog_ctl"]
    local ctl_dict_name = opts.bodylog_ctl_dict or "bodylog_ctl"

    -- 只保留小元数据；header utf-8 base64 处理仍在 Lua 做（header 总量 < 几 KB）
    local hdrs_in = ngx.req.get_headers() or {}
    local hdrs_out = {}
    for k, v in pairs(hdrs_in) do
        if not _G.bodylog_strip_hdrs[k:lower()] then
            if type(v) == "string" and not _G.bodylog_is_utf8(v) then
                hdrs_out[k] = "base64:" .. ngx.encode_base64(v)
            else
                hdrs_out[k] = v
            end
        end
    end

    -- 注意：req_body / resp_body 不进入 meta，作为 binary 帧的尾部裸字节，
    -- 跳过 cjson string escape 扫描（256K body 在大 entry 时本来要 ~5ms）。
    -- listener 端做 utf-8 检测 + base64 fallback + SSE 抽取。

    -- ts 用 ISO 8601 + 北京时区（CST/UTC+8），人读 + 标准库可解析
    local _stt = ngx.req.start_time()
    local _stt_s = math.floor(_stt)
    local _stt_ms = math.floor((_stt - _stt_s) * 1000)
    local _t = os.date("!*t", _stt_s + 8*3600)
    local _ts_iso = string.format("%04d-%02d-%02dT%02d:%02d:%02d.%03d+08:00",
        _t.year, _t.month, _t.day, _t.hour, _t.min, _t.sec, _stt_ms)
    -- 浮点减法（ngx.now() - start_time）有精度噪声，统一截到毫秒（3 位小数）
    local function _round_ms(v)
        if v == nil then return nil end
        return math.floor(v * 1000 + 0.5) / 1000
    end
    -- 拼接 resp_body：head + (gap marker) + tail。
    -- 截断时插入一行非 `data:` 前缀的 marker，listener 端 SSE 行扫描会自动跳过；
    -- 若有 chunk 在 tail buffer 边界被切成半行，则它对应的 JSON 行会解析失败被跳过。
    local resp_body
    local head_chunks = ngx.ctx.bodylog_chunks or {}
    local ring = ngx.ctx.bodylog_tail_ring
    if ring and ring.tail_idx >= ring.head_idx then
        local total_bytes = ngx.ctx.bodylog_total_bytes or 0
        local kept_bytes  = (ngx.ctx.bodylog_head_size or 0) + ring.bytes
        local missing     = total_bytes - kept_bytes
        local parts = {}
        for i = 1, #head_chunks do parts[#parts + 1] = head_chunks[i] end
        parts[#parts + 1] = string.format(
            "\n[BODYLOG_TRUNCATED %d BYTES MISSING IN MIDDLE]\n", missing)
        for i = ring.head_idx, ring.tail_idx do
            if ring[i] then parts[#parts + 1] = ring[i] end
        end
        resp_body = table.concat(parts)
    else
        resp_body = table.concat(head_chunks)
    end

    local meta = {
        ts            = _ts_iso,
        request_id    = ngx.var.request_id,
        client_ip     = ngx.var.remote_addr,
        method        = ngx.req.get_method(),
        uri           = ngx.var.request_uri,
        req_headers   = hdrs_out,
        req_body_truncated  = ngx.ctx.bodylog_req_truncated or false,
        peer          = ngx.ctx.routed_peer,
        -- 如果 upstream 是 router（自身也是反代），它在响应里回 X-Routed-Peer
        -- 标识真实后端 vllm。透传到 bodylog，让 monitor 能按真实 vllm 聚合 TPM。
        -- 普通 vllm 不会回这个 header，字段会是 nil/missing。
        forwarded_to  = ngx.var.upstream_http_x_routed_peer,
        session_id    = (ngx.ctx.session_id ~= "" and ngx.ctx.session_id) or nil,
        session_src   = ngx.ctx.session_source,
        mode          = ngx.ctx.routed_mode,
        status        = ngx.status,
        resp_bytes    = ngx.ctx.bodylog_total_bytes or 0,    -- upstream 原始 SSE wire 总字节（无 cap）
        resp_body_truncated = ngx.ctx.bodylog_resp_truncated or false,
        first_chunk_t = _round_ms(ngx.ctx.bodylog_first_chunk_t),
        last_chunk_t  = _round_ms(ngx.ctx.bodylog_last_chunk_t),
        chunk_count   = ngx.ctx.bodylog_chunk_count or 0,
        rt            = tonumber(ngx.var.request_time),
    }
    local req_body  = ngx.ctx.bodylog_req_body or ""

    -- cjson.encode 只编码 ~500B-1KB meta，不再扫描大 body string，~50µs。
    -- meta 编码在 access 路径上做（小开销），帧组装 + send 推到 timer.at(0) 异步。
    local ok, terr = ngx.timer.at(0, function(premature, m, rb, sb, dname)
        if premature then return end
        local cjson_safe = require "cjson.safe"
        local logger     = require "resty.logger.socket"
        local dict       = ngx.shared[dname]
        local meta_json, encerr = cjson_safe.encode(m)
        if not meta_json then
            dict:incr("encode_errs", 1, 0)
            ngx.log(ngx.WARN, "bodylog meta encode err: ", encerr)
            return
        end
        -- 帧格式（big-endian）：
        --   [u32 total_len][u16 meta_len][meta_json][u32 req_len][req_bytes][u32 resp_len][resp_bytes]
        -- total_len 不含开头 4 字节自身。
        local payload = _pack_u16(#meta_json) .. meta_json
                        .. _pack_u32(#rb) .. rb
                        .. _pack_u32(#sb) .. sb
        local frame = _pack_u32(#payload) .. payload
        local bytes, lerr = logger.log(frame)
        dict:set("last_log_ts", ngx.now())
        if not bytes or bytes == 0 then
            dict:incr("drop_count", 1, 0)
            if (dict:get("drop_count") or 0) % 1000 == 1 then
                ngx.log(ngx.WARN, "bodylog logger.log failed (drop): ", lerr or "buffer full")
            end
        else
            dict:incr("write_count", 1, 0)
        end
    end, meta, req_body, resp_body, ctl_dict_name)
    if not ok then
        ctl:incr("drop_count", 1, 0)
        ngx.log(ngx.WARN, "bodylog timer.at failed: ", terr)
    end
end
