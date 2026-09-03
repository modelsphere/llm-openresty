-- API key 的**唯一来源**。
--
-- 以前这张表写死在 route.lua 的 opts 默认值里,只有走 lua 引擎的 LLM 路由用得到;
-- video 这类纯反向代理的路由要鉴权就只能自带一份 key —— 于是密钥进了 ModelRoute
-- 和 chart 的 values(也就进了 git),而且和这里成了两个真相源,轮换时必漏一个。
--
-- 抽成模块后:LLM 路由(route.lua 的默认值)和 video 路由(access_by_lua_block)
-- 读同一张表,密钥只存在于 openresty 镜像里。
--
-- 加/换 key 改这里一处即可;调用方都用 M.keys 或 M.check(auth_header)。
local M = {}

M.keys = {
    ["REDACTED-API-KEY"] = "admin",
}

-- 从 Authorization 头取 key。只认 "Bearer <key>" —— 与 MiniMax / OpenAI 的约定一致。
function M.parse_bearer(auth_header)
    return (auth_header or ""):match("^Bearer%s+(.+)$")
end

-- 一份 key 表的指纹。access.lua 拿它判断 shared dict 里缓存的是不是当前这份表:
-- lua_shared_dict 的内容**跨 `openresty -s reload` 存活**(实测,见
-- tools/minimax-h3/t33_shared_dict_survives_reload.sh),所以原来那个"灌过一次就
-- 不再灌"的 __inited 标志会让"改 key + reload"对 LLM 路由不生效 —— 而 video 路由
-- 是直接查表的、立刻生效,两条路径就分叉了。按指纹播种则 reload 后自动重灌。
-- 指纹要**连 value 一起算**:value 是这条 key 的备注(归属方),
-- 只按 key 名算的话,改备注不会触发重灌,dict 里会一直留着旧值。
-- 今天鉴权只看条目在不在、不看 value,但别留这种静默陈旧点。
function M.fingerprint(keys)
    local ks = {}
    for k in pairs(keys) do ks[#ks + 1] = k end
    table.sort(ks)
    local parts = {}
    for i = 1, #ks do parts[i] = ks[i] .. "=" .. tostring(keys[ks[i]]) end
    return ngx.md5(table.concat(parts, ","))
end

-- 校验 Authorization 头。返回 (ok, 该 key 的备注)。
function M.check(auth_header)
    local key = M.parse_bearer(auth_header)
    if not key then return false, nil end
    local who = M.keys[key]
    if not who then return false, nil end
    return true, who
end

-- access 阶段的鉴权守卫,给**不走 lua 路由引擎**的路由用(autoconfig 渲染的 video 路由)。
-- 逻辑放这里而不是渲染进每份 conf:401 的响应体、Bearer 的解析规则只有一份。
--
-- public_re:免鉴权路径的正则(ngx.re 语法);nil / "" = 所有路径都要 key。
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
