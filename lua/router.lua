-- openresty/lua/router.lua
-- 引擎库入口:按序 require 各模块(副作用:填充 _G.* 函数表)。
-- session_route.conf 的 init_by_lua_block 只需 `require "router"`。
--
-- TODO(重构,@qiliguo MR review 提出):大部分 _G.* function 应收缩到 module scope
--   (内部 helper → local;跨模块调用 → `local x = require("mod")` 用 x.foo),不放 global ——
--   减少全局命名空间污染 + 消除 `_G write guard` warning + 降并发写风险。
--   边界:被 nginx.conf / router_locations.inc 的 *_by_lua_block 直接调的入口点
--   (do_route / do_balancer / do_log_release / bodylog_filter_chunk / register_route / dbg_*)
--   仍需保留全局或模块句柄。属整仓一次性 sweep(全模块 return M + require 化 + 同步测试),非 per-feature。

require "util"
require "reqtransform"
require "bodylog"
require "route"
require "reject_rules"
require "ttft"
require "tps"
require "access"
require "timers"
require "debug_endpoints"
