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

-- 校验 Authorization 头。返回 (ok, 该 key 的备注)。
-- 只认 "Bearer <key>" —— 与 MiniMax / OpenAI 的约定一致。
function M.check(auth_header)
    local key = (auth_header or ""):match("^Bearer%s+(.+)$")
    if not key then return false, nil end
    local who = M.keys[key]
    if not who then return false, nil end
    return true, who
end

return M
