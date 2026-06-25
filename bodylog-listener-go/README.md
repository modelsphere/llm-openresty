# bodylog-listener

接收 openresty(`log_by_lua` + lua-resty-logger-socket)推来的长度前缀二进制帧,解析后落盘。
部署在 gateway-host `/root/bodylog-listener/`,systemd `User=root`。权威源码即本目录 `main.go`。

## 落盘产物(都在 `BODYLOG_DIR/` 下)

| 产物 | 路径 | 内容 | 大小量级 |
|---|---|---|---|
| 全量 bodylog | `YYYY-MM-DD/HH.jsonl`(跨天 `→ YYYY-MM-DD.tar.gz`) | 每请求一行,**含 req/resp 正文** | ~84KB/行,~18GB/天 |
| 分钟聚合 | `metrics/YYYY-MM-DD.jsonl` | per-minute × per-peer × **stream** 汇总(`/summary` 用) | ~29MB/天 |
| **每请求 metrics 明细** | `metrics/details/YYYY-MM-DD.jsonl`(跨天 `→ .parquet`) | 每请求一行,**剥所有正文** | ~400-500B/行,~187MB/天 jsonl / ~25MB parquet |

明细字段(无正文):`ts, request_id, source_addr, uri, method, stream(从 req_body 提取的请求参数,null=未传), status, peer, forwarded_to, backend(归一化真实后端 host:port), mode, session_src, model, finish_reason, frt(=first_chunk_t/TTFT), lct, rt, chunk_count, req_bytes, resp_bytes, prompt_tokens, completion_tokens, cached_tokens, total_tokens, reasoning_tokens`。
**显式不含** `req_body/resp_body/req_headers/resp_meta.reasoning/resp_meta.tool_calls`。

明细当天用 jsonl 热写(append 友好);跨天封口后由 `housekeep`(跨天 + 每 6h)经进程内 DuckDB
`COPY ... TO ... (FORMAT parquet, COMPRESSION zstd)` 转 parquet,成功后删 jsonl。parquet 保留
`BODYLOG_DETAILS_KEEP_DAYS` 天(默认 365)。

## HTTP 接口(端口 9998)

**鉴权**:`BODYLOG_HTTP_TOKEN` 设置后,`/summary` 与 `/metrics` 需带 `Authorization: Bearer <token>`(或 `?token=<token>`),否则 401;`/healthz` 始终开放。**不设则不鉴权**(opt-in,向后兼容)。⚠️ 启用后调用方必须带 token —— 尤其 **monitor 拉 `/summary`** 要同步配上同一个 token(见 `monitor` 的 `BODYLOG_SUMMARY_URL`),否则 TPM/TTFT 断采。

- `GET /healthz` → `OK`(无需鉴权)
- `GET /summary?minutes=5[&breakdown=true][&stream=true|false]` → N 分钟 per-peer 分钟聚合
  - 内部按 (minute, peer, **stream**) 分桶。**默认跨 stream 合并**回 (minute,peer)(`buckets[]` 仍一行一 (minute,peer),monitor 兼容);`stream=true|false` 时只返回该类请求的聚合。stream 取自请求参数(从 req_body 提取,未传/未知归入空桶,默认合并里照常计入)。
- `GET /metrics?start=&end=[&model=&peer=&status=&min_frt=&stream=&limit=]` → **时间窗内每条请求的 metrics 明细**
  - `start`/`end`:RFC3339(`2026-06-18T12:48:00+08:00`)、`YYYY-MM-DDTHH:MM:SS`(按本地时区)或 Unix 秒。必填。
  - 可选过滤:`model`、`peer`(匹配 backend 或原始 peer)、`status`、`min_frt`(秒)、`stream`(`true`只流式/`false`只非流式/`null`只未传 stream)、`limit`(默认=上限 5000)。
  - 历史天读 `metrics/details/<date>.parquet`,当天读 live `<date>.jsonl`,两支 **`UNION ALL BY NAME`** 合并(DuckDB 无单函数同吃两格式)。
  - 结果**按 `(ts, request_id)` 升序**(确定全序;`ts` 不唯一,`request_id`=nginx `$request_id` 唯一兜底)。`ts` = openresty `bodylog_finalize` 写的请求时间戳(ISO8601 +08:00)。
  - 返回 `{"count":N,"truncated":bool,"rows":[…]}`;**硬上限 5000 条**(≈2.5MB),超限只回前 5000 + `truncated:true`。
  - **截断时**额外回 `returned_from`/`returned_to`(本段 ts 区间,人看)+ `next_cursor`(keyset 游标)。
  - **分段续取(零重叠、无需去重)**:把 `next_cursor` 原样回灌 → `?cursor=<next_cursor>&end=<同 end>`,直到 `truncated:false`。游标 = `base64url("<ts>|<request_id>")`,URL 安全;带 `cursor` 时 `start` 可省。底层用元组 `(ts,request_id) > (cursor_ts,cursor_id)` 精确接续。
  - 例:`curl 'http://127.0.0.1:9998/metrics?start=2026-06-18T12:48:00%2B08:00&end=2026-06-18T12:55:00%2B08:00&min_frt=60'`(注意 tz 的 `+` 在 URL 里要写 `%2B`;`next_cursor` 是 base64url,无此问题)
  - DuckDB 打开失败(无 CGO)时 `/metrics` 返回 503,明细仍照常写 jsonl(不丢数据,只是不转 parquet)。
  - **开关** `BODYLOG_DETAILS=off`:不建 detailsWriter → 每请求零额外开销(kill-switch)。压测(chat+mock)on vs off 最大饱和吞吐 ~-11% / +26µs per frame,生产速率(均值 ~6 req/s)下可忽略。

## 构建 ⚠️(已不是静态二进制)

内嵌 DuckDB(`go-duckdb`)用于 `/metrics` + parquet 转换,需 **CGO_ENABLED=1**:
- 产物动态链接 glibc/libstdc++(~46MB),**绑定构建机 glibc 版本** → 必须在 glibc ≤ 目标机 的环境编。
- CGO 不便跨平台交叉编译 → **直接在目标 gateway-host(linux/amd64)上 build**(需 go + gcc),或用匹配的 linux/amd64 容器。
- `bash compile.sh` 默认 `CGO_ENABLED=1 GOOS=linux GOARCH=amd64`。

**回退**:若目标机装 CGO 工具链困难,可改回 `CGO_ENABLED=0` 纯静态二进制(去掉 `_ "github.com/marcboeker/go-duckdb"` 导入 + `/metrics`/parquet 逻辑),或把 `/metrics` 查询与 parquet 转换改成 shell-out 调外部 `duckdb` CLI(同样的 SQL),保持 Go 二进制静态。

## 部署

1. 在 gateway-host 上 `bash compile.sh` 出 `bodylog-listener`(linux/amd64 CGO)。
2. 替换 `/root/bodylog-listener/bodylog-listener` + `systemctl restart bodylog-listener`。
3. 重启窗口 openresty 端有 256MB buffer + 1s 重连(`drop_limit`/`periodic_flush`),短暂重启不丢帧。
   **listener 是生产服务,重启前单独确认。**
4. 验证:`journalctl -u bodylog-listener` 看 `duckdb ready` / `opened …/metrics/details/…` / `details rotated …`;
   `curl :9998/healthz`、`curl :9998/metrics?start=…&end=…`。

环境变量见 `bodylog-listener.service`。
