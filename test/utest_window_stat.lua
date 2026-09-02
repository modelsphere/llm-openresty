-- resty 单元测试:窗口统计量(util.window_quantile / window_avg / window_stat)
-- + TTFT 多指标折叠(一份直方图 → N 份 EWMA)。
--
-- 为什么需要:P0 的验收标准是「零行为变化」,这恰恰意味着 avg 与多指标两条代码路径
-- **一次都没被跑过** —— 因为 ttft_metrics() 目前恒返回单条 p80,没有配置能触达它们。
-- 本单测直接注入指标列表来覆盖,不必等 P1 接上 CRD。
--
-- 用法: resty test/utest_window_stat.lua [../lua]     (arg[1] = lua 目录,默认 ../lua)

local LUA = arg[1] or "../lua"

-- ── 假 shared dict(按 ngx.shared.DICT 语义实现所需子集)────────────────────────
-- 用假的而不是 --shdict:① 免去 resty 启动参数,与既有 utest 调用方式一致;
-- ② 能直接断言"写了哪些 key"——这正是本测试要证的(一份直方图 vs N 份)。
local function newdict()
    local d = { _kv = {} }
    function d:get(k) return self._kv[k] end
    function d:set(k, v) self._kv[k] = v; return true end
    function d:delete(k) self._kv[k] = nil end
    function d:add(k, v)                      -- 已存在返 false(ttft_record 用它做折叠锁)
        if self._kv[k] ~= nil then return false, "exists" end
        self._kv[k] = v; return true
    end
    function d:incr(k, v, init)
        local cur = self._kv[k]
        if cur == nil then
            if init == nil then return nil, "not found" end
            cur = init
        end
        self._kv[k] = cur + v
        return self._kv[k]
    end
    function d:keys_matching(pat)             -- 测试辅助:数 key
        local n, hit = 0, {}
        for k in pairs(self._kv) do if k:find(pat) then n = n + 1; hit[#hit+1] = k end end
        table.sort(hit)
        return n, hit
    end
    return d
end

-- ttft.lua 顶层 require 的模块都要先手动装进 package.loaded ——
-- 单测用 loadfile 直接装载,不走 lua_package_path,require 找不到文件会直接报错。
local util = assert(loadfile(LUA .. "/util.lua"))()
package.loaded.util = util
local ttft = assert(loadfile(LUA .. "/ttft.lua"))()

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ " .. name)
    else fail = fail + 1
        print("  ✗ " .. name .. "\n      got=" .. tostring(got) .. "\n      exp=" .. tostring(exp)) end
end

-- ══════════════════════════════════════════════════════════════════════
-- 1) window_quantile:分位、溢出桶、空窗
-- ══════════════════════════════════════════════════════════════════════
local B = {100, 200, 400, 800, 1600}          -- n=5,第 6 个是 overflow
local d = newdict()
local pre = "t:"
-- 装 5 个样本:4 个落桶1(<=100),1 个落桶4(<=800)
d:set(pre .. "h:0:1", 4)
d:set(pre .. "h:0:4", 1)

eq("Q1 P80 = 100(累计到桶1 已达 5*0.8=4)", util.window_quantile(d, pre, 0, B, 0.8), 100)
eq("Q2 P20 = 100(低尾同样落桶1)",          util.window_quantile(d, pre, 0, B, 0.2), 100)
-- 高尾:把分布反过来,4 个在桶4、1 个在桶1
local d2 = newdict()
d2:set(pre .. "h:0:1", 1); d2:set(pre .. "h:0:4", 4)
eq("Q3 P80 = 800(4/5 在桶4)", util.window_quantile(d2, pre, 0, B, 0.8), 800)
eq("Q4 P20 = 100(低尾抓到桶1)", util.window_quantile(d2, pre, 0, B, 0.2), 100)
-- 溢出桶(idx = n+1 = 6)代表值 = 末桶×2 = 3200
local d3 = newdict(); d3:set(pre .. "h:0:6", 3)
eq("Q5 溢出桶代表值 = 末桶×2", util.window_quantile(d3, pre, 0, B, 0.8), 3200)
-- 空窗必须返 nil 而不是 0(返 0 会让 TPS 侧 `0 <= limit` 恒成立、瞬间全拒)
eq("Q6 空窗返 nil(不是 0)", util.window_quantile(newdict(), pre, 0, B, 0.8), nil)

-- ══════════════════════════════════════════════════════════════════════
-- 2) window_avg:精确均值,且与分位数**明显不同**(这正是 avg 存在的意义)
-- ══════════════════════════════════════════════════════════════════════
local da = newdict()
da:set(pre .. "h:0:sum", 1100)   -- 4×100 + 1×700
da:set(pre .. "h:0:cnt", 5)
eq("A1 avg = 1100/5 = 220", util.window_avg(da, pre, 0), 220)
eq("A2 空窗(cnt=0)返 nil",  util.window_avg(newdict(), pre, 0), nil)
-- cnt 缺失但 sum 在 → 仍按空窗处理,不能除零
local db = newdict(); db:set(pre .. "h:0:sum", 999)
eq("A3 只有 sum 无 cnt 返 nil(不除零)", util.window_avg(db, pre, 0), nil)

-- ══════════════════════════════════════════════════════════════════════
-- 3) window_stat 分派:avg 走均值,pNN 走分位
-- ══════════════════════════════════════════════════════════════════════
local dc = newdict()
dc:set(pre .. "h:0:1", 4); dc:set(pre .. "h:0:4", 1)
dc:set(pre .. "h:0:sum", 1100); dc:set(pre .. "h:0:cnt", 5)
eq("S1 metric=avg → 220",  util.window_stat(dc, pre, 0, B, "avg", 0.8), 220)
eq("S2 metric=p80 → 100",  util.window_stat(dc, pre, 0, B, "p80", 0.8), 100)
eq("S3 同一份桶,avg≠p80(220≠100)", util.window_stat(dc, pre, 0, B, "avg", 0.8)
                                     ~= util.window_stat(dc, pre, 0, B, "p80", 0.8), true)

-- ══════════════════════════════════════════════════════════════════════
-- 4) ttft_record 端到端:一份直方图 → 两份 EWMA(p80 + avg)
--    这是 P0 里"写了但没被跑过"的那条路径。
-- ══════════════════════════════════════════════════════════════════════
_G.TTFT_BUCKETS_MS = B
local NOW = 5
local real_now = ngx.now
ngx.now = function() return NOW end            -- 控制窗口号,避免依赖真实时钟

local opts = {
    route_name = "t", ttft_dict = "ttft_stat",
    ttft_window = 10, ttft_ttl = 60,
    ttft_ewma_alpha = 1.0,                     -- alpha=1 → EWMA 直接等于窗口值,便于精确断言
    ttft_limit_ms = 30000,
}
-- 注入两个指标(P0 的 ttft_metrics 恒返回单条 p80,这里覆盖它以覆盖多指标路径)
ttft.ttft_metrics = function() return {
    { metric = "p80", q = 0.8, threshold = 30000 },
    { metric = "avg",          threshold = 30000 },
} end

local td = newdict()
-- 窗口 0(now=5 → floor(5/10)=0):4 个 100ms + 1 个 700ms
for _ = 1, 4 do ttft.ttft_record(opts, td, 100) end
ttft.ttft_record(opts, td, 700)

-- 折叠前:直方图只应有**一份**(桶 + sum + cnt),没有任何 EWMA
local nh = select(1, td:keys_matching("^t:h:0:"))
eq("E1 折叠前只有一份直方图(桶2 + sum + cnt = 4 个 key)", nh, 4)
eq("E2 折叠前无 p80 EWMA", td:get("t:all:p80:ewma"), nil)

-- 跨进窗口 1 触发折叠
NOW = 15
ttft.ttft_record(opts, td, 100)

eq("E3 p80 EWMA = 100", td:get("t:all:p80:ewma"), 100)
eq("E4 avg EWMA = 220(=1100/5)", td:get("t:all:avg:ewma"), 220)
eq("E5 两份 EWMA 来自同一份桶,值确实不同", td:get("t:all:avg:ewma") ~= td:get("t:all:p80:ewma"), true)
-- 折叠后窗口 0 的桶 + sum/cnt 必须全清(否则孤儿 key 在共享字典里堆积)
eq("E6 折叠后窗口0 的 key 全部清理", select(1, td:keys_matching("^t:h:0:")), 0)
-- 窗口 1 的样本已入账(桶 + sum + cnt = 3 个 key)
eq("E7 窗口1 已开始入账", select(1, td:keys_matching("^t:h:1:")), 3)

ngx.now = real_now

-- ══════════════════════════════════════════════════════════════════════
-- 4) validate_metrics:CRD 渲染进 opts 的指标表校验(register_route 里每 reload 跑一次)
--    坏输入必须**整份丢弃**回落静态,而不是半份生效 —— 半份生效时某个指标悄悄用了
--    默认值,线上没有任何迹象。
-- ══════════════════════════════════════════════════════════════════════
local logged = 0
ngx.log = function() logged = logged + 1 end

eq("V1 nil 原样返回(未配 = 走静态,不算错、不打日志)", util.validate_metrics(nil, "ttft", "r"), nil)

local good = util.validate_metrics({ { metric = "p80", q = 0.8, threshold = 20000 },
                                     { metric = "avg", threshold = 5000 } }, "ttft", "r")
eq("V2 合法表通过", good and #good, 2)
eq("V3 avg 不要求 q", good and good[2].q, nil)

logged = 0
eq("V4 空数组丢弃", util.validate_metrics({}, "ttft", "r"), nil)
eq("V5 q 越界(=1)丢弃", util.validate_metrics({ { metric = "p80", q = 1, threshold = 1 } }, "ttft", "r"), nil)
eq("V6 q 缺失丢弃",     util.validate_metrics({ { metric = "p80", threshold = 1 } }, "ttft", "r"), nil)
eq("V7 threshold 非数丢弃", util.validate_metrics({ { metric = "p80", q = 0.8, threshold = "20s" } }, "ttft", "r"), nil)
eq("V8 metric 名缺失丢弃", util.validate_metrics({ { q = 0.8, threshold = 1 } }, "ttft", "r"), nil)
eq("V9 每次失败都打了 ERR 日志(V4-V8 共 5 次;否则线上静默回落无从发现)", logged, 5)

-- 一条坏的就整份丢,不留下前面那条好的
eq("V10 混合表整份丢弃(不半份生效)",
   util.validate_metrics({ { metric = "p80", q = 0.8, threshold = 1 },
                           { metric = "p95", q = 9,   threshold = 1 } }, "ttft", "r"), nil)

print(string.format(
    "================ window_stat / 多指标折叠 单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
