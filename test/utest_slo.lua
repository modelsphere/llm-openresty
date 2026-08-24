-- resty 单元测试:slo.lua —— CRD 下发数据的解析、校验、查询、加载语义。
-- 重点覆盖「坏数据不能污染好数据」这条:解析失败必须保持上一份,且**整份拒绝**不能半份生效。
--
-- 用法: resty test/utest_slo.lua [../lua]

local LUA = arg[1] or "../lua"
local slo = assert(loadfile(LUA .. "/slo.lua"))()

local pass, fail = 0, 0
local function eq(name, got, exp)
    if got == exp then pass = pass + 1; print("  ✓ " .. name)
    else fail = fail + 1
        print("  ✗ " .. name .. "\n      got=" .. tostring(got) .. "\n      exp=" .. tostring(exp)) end
end
local function truthy(name, v) eq(name, v and true or false, true) end

local GOOD = [[{
  "version": 17,
  "routes": {
    "kimi-k2.5": {
      "__default__": {
        "ttft": { "default": { "metrics": [ {"metric":"p80","q":0.8,"threshold_ms":20000} ] }, "ranges": [] },
        "otps": { "default": { "metrics": [ {"metric":"p80","q":0.2,"threshold_tps":30} ] }, "ranges": [] }
      }
    }
  }
}]]

-- ── 1) 正常解析 ───────────────────────────────────────────────────────────────
local t, err = slo.parse(GOOD)
truthy("P1 合法 json 解析成功", t)
eq("P2 version 读到", t and t.version, 17)
local ms = t and t.routes["kimi-k2.5"]["__default__"].ttft.metrics
eq("P3 ttft 指标数=1", ms and #ms, 1)
eq("P4 ttft threshold_ms → threshold", ms and ms[1].threshold, 20000)
eq("P5 ttft q 保留", ms and ms[1].q, 0.8)
local os_ = t and t.routes["kimi-k2.5"]["__default__"].otps.metrics
eq("P6 otps threshold_tps → threshold", os_ and os_[1].threshold, 30)
eq("P7 otps q=0.2(方向换算在 operator 侧,引擎只认 q)", os_ and os_[1].q, 0.2)

-- ── 2) 校验:非法数据必须**整份**拒绝 ────────────────────────────────────────
local function bad(name, js, expect_word)
    local r, e = slo.parse(js)
    if r ~= nil then fail = fail + 1; print("  ✗ " .. name .. " 应该被拒绝但通过了")
    elseif expect_word and not tostring(e):find(expect_word, 1, true) then
        fail = fail + 1; print("  ✗ " .. name .. " 拒绝了但原因不对: " .. tostring(e))
    else pass = pass + 1; print("  ✓ " .. name .. "(" .. tostring(e) .. ")") end
end
bad("V1 非 json", "{not json", "JSON")
bad("V2 缺 routes", '{"version":1}', "routes")
bad("V3 pNN 缺 q", '{"routes":{"r":{"__default__":{"ttft":{"default":{"metrics":[{"metric":"p80","threshold_ms":1}]}}}}}}', "q")
bad("V4 q 越界", '{"routes":{"r":{"__default__":{"ttft":{"default":{"metrics":[{"metric":"p80","q":1.5,"threshold_ms":1}]}}}}}}', "q")
bad("V5 缺 threshold_ms", '{"routes":{"r":{"__default__":{"ttft":{"default":{"metrics":[{"metric":"p80","q":0.8}]}}}}}}', "threshold_ms")
bad("V6 metrics 空数组", '{"routes":{"r":{"__default__":{"ttft":{"default":{"metrics":[]}}}}}}', "非空数组")
-- avg 不需要 q(没有分位含义)
local ta = slo.parse('{"routes":{"r":{"__default__":{"ttft":{"default":{"metrics":[{"metric":"avg","threshold_ms":9}]}}}}}}')
truthy("V7 avg 无需 q,应通过", ta)
eq("V8 avg 的 threshold", ta and ta.routes.r.__default__.ttft.metrics[1].threshold, 9)

-- 一条坏的就整份拒(不能只生效好的那条)—— 半份生效比不生效更危险
bad("V9 两条指标里坏一条 → 整份拒",
    '{"routes":{"r":{"__default__":{"ttft":{"default":{"metrics":[' ..
    '{"metric":"p50","q":0.5,"threshold_ms":5},{"metric":"p95","threshold_ms":9}]}}}}}}', "q")

-- ── 3) ranges 本期忽略,但不报错 ──────────────────────────────────────────────
local tr = slo.parse('{"routes":{"r":{"__default__":{"ttft":{' ..
    '"default":{"metrics":[{"metric":"p80","q":0.8,"threshold_ms":7}]},' ..
    '"ranges":[{"id":"ctx0-4096","low":0,"high":4096,"metrics":[]}]}}}}}')
truthy("R1 带 ranges 仍解析成功(忽略 ranges)", tr)
eq("R2 仍用 default 的阈值", tr and tr.routes.r.__default__.ttft.metrics[1].threshold, 7)

-- ── 4) 查询 metrics_for + 未接 CRD 时全 miss ────────────────────────────────
slo._reset()
eq("Q1 未加载时查询返 nil(→ 调用方回落静态)", slo.metrics_for("kimi-k2.5", nil, "ttft"), nil)
truthy("Q2 未加载时 status.loaded=false", slo.status().loaded == false)

truthy("Q3 POST 注入成功", slo.apply_post(GOOD))
local q = slo.metrics_for("kimi-k2.5", nil, "ttft")
eq("Q4 注入后能查到 ttft", q and q[1].threshold, 20000)
eq("Q5 查不存在的 route 返 nil", slo.metrics_for("no-such-route", nil, "ttft"), nil)
eq("Q6 查未声明的 kind 返 nil",
   slo.metrics_for("kimi-k2.5", nil, "nope"), nil)
eq("Q7 source=post", slo.status().source, "post")
truthy("Q8 pinned(防被下个 tick 抹掉)", slo.status().pinned)

-- ── 5) 坏数据不得污染好数据 ─────────────────────────────────────────────────
local ok2, e2 = slo.apply_post('{"routes": "不是对象"}')
eq("B1 坏注入被拒", ok2, nil)
truthy("B2 拒绝时给了原因", e2 ~= nil)
local still = slo.metrics_for("kimi-k2.5", nil, "ttft")
eq("B3 **旧的好数据仍在**(未被清空)", still and still[1].threshold, 20000)
eq("B4 version 未变", slo.status().version, 17)
truthy("B5 last_error 记下了", slo.status().last_error ~= nil)

-- ── 6) tick:文件不在 / 内容没变 / 坏内容 ────────────────────────────────────
slo._reset()
local TMP = "/tmp/utest_slo_" .. tostring(ngx.worker.pid()) .. ".json"
os.remove(TMP)
local okt, et = slo.tick(TMP)
eq("T1 文件不存在 → 不致命,返 false", okt, false)
truthy("T2 给了原因", et ~= nil)
eq("T3 且**没有**清空(本来就空,查询仍 nil)", slo.metrics_for("kimi-k2.5", nil, "ttft"), nil)

local f = assert(io.open(TMP, "w")); f:write(GOOD); f:close()
truthy("T4 文件出现 → tick 成功", slo.tick(TMP))
eq("T5 生效", (slo.metrics_for("kimi-k2.5", nil, "ttft") or {})[1].threshold, 20000)
eq("T6 source=file", slo.status().source, "file")
truthy("T7 内容没变时 tick 仍成功(走免解析短路)", slo.tick(TMP))

-- 写坏内容:必须保持上一份好数据
local f2 = assert(io.open(TMP, "w")); f2:write("{坏"); f2:close()
local okb = slo.tick(TMP)
eq("T8 坏内容 tick 返 false", okb, false)
eq("T9 **仍用上一份好数据**", (slo.metrics_for("kimi-k2.5", nil, "ttft") or {})[1].threshold, 20000)
eq("T10 version 保持", slo.status().version, 17)
os.remove(TMP)

print(string.format("================ slo(CRD 加载/校验)单测: PASS=%d FAIL=%d ================", pass, fail))
os.exit(fail == 0 and 0 or 1)
