-- openresty/lua/router.lua
-- 引擎库入口:按序 require 各模块。session_route.conf 的 init_by_lua_block 只需 `require "router"`。
--
-- 收敛现状(2026-07,@qiliguo MR review 提出的 sweep 已完成):
--   * 各模块 `local M = {} … return M`;跨模块调用改 `local mod = require "mod"` 用 mod.foo。
--   * 内部 helper 用 local;仅这些【入口点】仍挂 _G——被 nginx.conf / router_locations.inc 的
--     *_by_lua_block(或 per-model conf 的 set_by_lua)直接按名调用,无 module 句柄可用:
--       do_route / do_log_release / do_balancer(access)、bodylog_filter_chunk(bodylog)、
--       register_route(route)、dbg_*(debug_endpoints)。
--   * config 数据(PEERS / __route_opts / TTFT_* / TPS_* / ADAPTIVE_CC_* / KIMI_* / BODYLOG_* / …)
--     仍留 _G:由 session_route.conf / 各 session_route_<model>.conf 的 lua block 直读直写(用户配置面)。
--   * 循环依赖(route↔timers)用函数内 lazy `require`(见 route.lua assess_pool / register_route)破环。
-- 本文件的显式 require 顺序仍必要:access/debug_endpoints/timers/bodylog/route 定义的入口点靠这里
--   被 eager 加载(否则没人 require 它们 → _G 入口点不注册)。各模块自身的 require 负责按需拉依赖。

require "util"
require "slo"
require "reqtransform"
require "bodylog"
require "route"
require "reject_rules"
require "ttft"
require "tps"
require "access"
require "timers"
require "debug_endpoints"
