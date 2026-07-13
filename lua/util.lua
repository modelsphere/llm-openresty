-- openresty/lua/util.lua
-- cjson array_mt 初始化 + _nonblank + pick_rendezvous + opts_missing

local cjson = require "cjson.safe"
-- 让 cjson 在 decode 时给 JSON array 打 array_mt metatable，
-- 后续 encode 能区分空数组 [] 和空对象 {}（修复 cch_strip / normalize 改 body 后
-- tools[i].function.parameters.required 等空 array 被错序列化为 {} 触发 sglang 400）。
if cjson.decode_array_with_array_mt then
    cjson.decode_array_with_array_mt(true)
end
-- 非空且不全是空白才算有效 sid;非标量(table/bool/nil)直接拒绝。
-- 挂在 _G 上供各 phase(access/log 等)调用;别处要快可自行 local 别名(如 reqtransform.lua)。
function _G._nonblank(s)
    if type(s) ~= "string" and type(s) ~= "number" then return false end
    local str = tostring(s)
    return str ~= "" and str:match("%S") ~= nil
end

-- Rendezvous 哈希 pick: 在 peer_list 中找 md5(sid|peer_key) 最大者。
-- 返回 best_idx (1-based index into peer_list), best_hash。
-- peer_list 每个元素 {host, port, orig_idx, cached_key}; cached_key 可选，没传则重算。
-- 三处 caller 共享此函数: 主路由 access_by_lua、/_route_debug、/_route_inspect。
function _G.pick_rendezvous(sid, peer_list)
    local best_h, best_idx = -1, 1
    local prefix = sid .. "|"   -- 循环外一次构造，省 N 次小分配
    for i, hp in ipairs(peer_list) do
        local pk = hp[4] or (hp[1] .. ":" .. hp[2])
        local h = tonumber(string.sub(ngx.md5(prefix .. pk), 1, 8), 16) or 0
        if h > best_h then best_h, best_idx = h, i end
    end
    return best_idx, best_h
end

-- 共享 nil-opts 兜底:route 未注册(register_route 失败 / server 的 set $route 与 register 名不匹配)
-- 时 _G.__route_opts[ngx.var.route] 为 nil。content/access phase 的 do_route + 各 dbg_* 统一用它返
-- JSON 500,避免同一 guard 复制到 ~20 处后漂移(code review G6)。返回 true=opts 缺失(调用方应
-- return / return ngx.exit(500))。log phase 的 do_log_release、balancer phase 不能 ngx.say,各自保留简单 guard。
function _G.opts_missing(opts)
    if opts then return false end
    ngx.status = 500
    ngx.header["Content-Type"] = "application/json"
    ngx.say([[{"error":"route not initialized — check error.log for register_route failures"}]])
    return true
end

