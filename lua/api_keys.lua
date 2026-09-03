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
