-- openresty/lua/router.lua
-- 引擎库入口:按序 require 各模块(副作用:填充 _G.* 函数表)。
-- session_route.conf 的 init_by_lua_block 只需 `require "router"`。

require "util"
require "reqtransform"
require "bodylog"
require "route"
require "ttft"
require "tps"
require "access"
require "timers"
require "debug_endpoints"
