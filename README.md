# OpenResty Session Routing 部署文档

OpenResty 作为 vllm 集群前的反向代理，把客户端请求按 `session_id` 路由到**固定的** vllm peer（提高 prompt-cache 命中率）。没有 session_id 的请求走 least-conn 负载均衡。

文档描述 `:18080` 的 session routing 路径，适用于部署此配置的任意机器（生产 + 测试环境）。

## 总体架构

```
                     ┌──────────────────────────────┐
                     │  ngx.shared.active_conns     │ ← least_conn 计数
client ── :18080 ──► │  ngx.shared.bad_peers        │ ← 健康检查 ban 列表
(HTTP)               │  ngx.shared.api_keys         │ ← API key 缓存
                     │  ngx.shared.lc_locks         │ ← least_conn 选择锁
                     │  (4 个均在 session_route.conf │
                     │   顶部声明,非 nginx.conf)     │
                     └───────────┬──────────────────┘
                                 │
                   access_by_lua_block
                   ├── API key 鉴权
                   ├── extract_session_id()
                   ├── 选 peer（hash / least_conn / fallback）
                   └── 写 ngx.ctx.chosen_host/port
                                 │
                   balancer_by_lua_block
                                 │
                                 ▼
                         16 套 vllm peer（10.0.0.1:8050）
```

## 文件布局

```
主机端（生产部署后）
  /usr/local/openresty/
    ├── bin/openresty                    (二进制)
    └── nginx/
        ├── conf/
        │   ├── nginx.conf               (主配置，含 shared_dict 声明)
        │   └── conf.d/
        │       └── session_route.conf   (18080 session 路由 + API key)
        └── logs/
            ├── access.log
            └── error.log
```

**本地仓库权威副本**（`/path/to/repo/`）：

```
openresty/
  ├── nginx.conf                        (主配置，所有部署通用)
  ├── session_route.conf                (session 路由 + API key，所有部署通用)
  ├── llm-gateway-1.example.com.conf     (对外 HTTPS 网关 site 配置，仅生产 gateway-host 使用)
  ├── install_openresty.sh              (全新机器部署脚本)
  ├── logrotate-openresty.conf          (日志轮转)
  └── bin/or                            (运维快捷脚本源)
  test/                                 (本地 mock + 回归测试脚本)
    ├── mock_vllm.py
    └── test_rendezvous*.py / test_stress.py
```

## 端口与路径

| 端口 | 入口路径 | 去向 |
|------|---------|------|
| 18080 | `/v1/*` | session 路由 → 16 套 vllm peer（API key 必填） |
| 18080 | `/_active_conns` / `/_route_debug` / `/_route_inspect` / `/_health_status` | 调试端点，不需要 API key |

生产环境 `gateway-host` 同一个 OpenResty 进程还监听 `:80`/`:443`（公网 HTTPS 网关），那部分用 `include /etc/nginx/sites-enabled/*;` 独立加载，**与本文档 `:18080` session 路由无关**，不要动。

## Peer 列表（16 套）

`session_route.conf` 的 `init_by_lua_block` 里 `_G.PEERS` 是唯一定义，其他 6 处全部引用 `_G.PEERS` / `_G.PEER_KEYS`：

```lua
_G.PEERS = {
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H100
    {"10.0.0.1", 8050, "gpu-node"},   -- H800
    {"10.0.0.1", 8050, "gpu-node"},   -- H800
    {"10.0.0.1", 8050, "gpu-node"},   -- H800
    {"10.0.0.1", 8050, "gpu-node"},   -- H800
    {"10.0.0.1", 8050, "gpu-node"},   -- A100
    {"10.0.0.1", 8050, "gpu-node"},   -- A100
    {"10.0.0.1",  8050, "gpu-node"},    -- A100
    {"10.0.0.1", 8050, "gpu-node"},   -- A100
    {"10.0.0.1", 8050, "gpu-node"},   -- A100
}
```

**新增/删除 peer 流程**：

1. 编辑本地 `openresty/session_route.conf` 的 `_G.PEERS` 表
2. 先在非生产环境验证（chat / gateway-host）：`scp` + `openresty -t` + `openresty -s reload`，`curl /_route_debug?sid=xxx` 检查新 peer 可命中
3. 生产（gateway-host）同步：`scp` 到 `/tmp/`，备份现有 conf，`sudo cp`，`openresty -t`，`openresty -s reload`
4. `reload` **不断现有连接**（旧 worker 继续服务已建立连接到自然结束，新 worker 跑新配置）

## 路由策略

### 1. Session ID 提取（按优先级 6 个来源）

- header `x-litellm-session-id`
- header `x-claude-code-session-id`
- header `x-session-id`
- body `metadata.session_id` / `litellm_metadata.session_id`
- body `metadata.user_id.session_id`（dict 或 JSON 字符串）
- body `metadata.user_id`（plain 字符串）
- body `user`（OpenAI 标准字段）

空白字符串（纯空格）**不算**有效 sid —— 通过 `_nonblank()` 过滤，防止所有 whitespace sid 哈希到同一 peer。

### 2. 路由决策

- **有 sid**：`crc32(sid) % #peers` 选固定 peer。命中 banned peer 时 fallback 到 healthy 集合的 least_conn
- **无 sid**：用 `resty.lock` 在 `access_by_lua_block` 里序列化决策，在健康 peer 中选 `active_conns` 最少的

### 3. 主动健康检查

worker 0 每 10s TCP connect + `GET /health`，失败 → ban 300s（自动续期），恢复 → 立即解 ban。Banned peer 列表通过 `ngx.shared.bad_peers` 跨 worker 共享。

### 4. Retry / Failover

- `proxy_next_upstream error timeout http_502 http_503 non_idempotent`
- `proxy_connect_timeout 10s`
- `proxy_next_upstream_timeout 60s`
- peer 连接失败/超时 → 按 `active_conns` 升序选最空闲的 2 个 fallback peer 依次重试

## 响应头（调试用）

每个 `/v1/*` 响应都带：

| Header | 含义 |
|--------|------|
| `X-Routed-Session` | 提取到的 session_id（>128 字符截断 + `...`） |
| `X-Routed-Source` | sid 来源（如 `header.x-litellm-session-id` / `body.metadata.user_id[json].session_id` / `none`） |
| `X-Routed-Mode` | `hash` / `least_conn` / `hash_fallback` |
| `X-Routed-Peer` | 最终选中的上游 `ip:port`（重试时后缀 `(retry#N)`） |
| `X-Routed-Retries` | 重试次数（0 = 首次命中） |

## 辅助端点（免 API key）

- `GET /_active_conns` — 返回 JSON，每 peer 当前 in-flight 计数
- `GET /_health_status` — 返回 JSON，每 peer 的 `{active, banned}` 状态
- `GET /_route_debug?sid=xxx` — 静态 hash 预览，不发往后端
- `POST /_route_inspect` — 按真实请求 extract sid 并返回决策，不发后端
- `GET /_active_conns_set` — 矫正 active_conns 计数器（仅 127.0.0.1 可访问）

### `/_active_conns_set` —— 矫正 phantom 计数

**何时需要**：用 `kill -TERM` / `kill -9` 砍 worker 时，被砍掉的 in-flight 请求不会
跑到 log_by_lua 阶段执行 `dict:incr(peer, -1)`，导致 `active_conns` 计数永久 +1。
查 `/_active_conns` 返回值与实际 ESTAB 连接数对不上时就是 leak。

```bash
# 先查实际连接数
sudo ss -tn | grep '10.0.0.1:8050' | wc -l            # 跨机连接：直接计数
sudo ss -tn | grep '127.0.0.1:8060' | wc -l              # loopback：除以 2

# 矫正单个 peer 的计数
curl 'http://127.0.0.1:18080/_active_conns_set?peer=10.0.0.1:8050&value=2'
# {"ok":true,"action":"set","peer":"10.0.0.1:8050","value":2}

# 删除某个 peer 的 key（PEERS 里删除某 peer 后清理残留 key）
curl 'http://127.0.0.1:18080/_active_conns_set?peer=127.0.0.1:8060&delete=1'

# 全部清空（慎用，所有 peer 计数清零）
curl 'http://127.0.0.1:18080/_active_conns_set?flush=1'
```

**预防 phantom 计数**：以后停 worker 改用 `kill -QUIT <pid>`（graceful，等连接结束）
或者 nginx.conf 加 `worker_shutdown_timeout 600s;` —— reload 时 graceful 等连接
最多 10 分钟，不至于无限挂。

## API Key 鉴权

**位置**：`session_route.conf` 的 `location /v1/` 里 `access_by_lua_block` 开头
**作用范围**：仅 `:18080` 的 `/v1/*`，白名单：`/_*` / `/health` / `/healthz`

```lua
access_by_lua_block {
    local ak = ngx.shared.api_keys
    if not ak:get('__inited') then
        ak:set('REDACTED-API-KEY', 'admin')
        ak:set('__inited', '1')
    end
    local auth = ngx.req.get_headers()['authorization'] or ''
    local akey = auth:match('^Bearer%s+(.+)$')
    if not akey or not ak:get(akey) then
        ngx.status = 401
        ngx.header['Content-Type'] = 'application/json'
        ngx.say('{"error":"missing or invalid api key"}')
        return ngx.exit(401)
    end
    -- 下面继续 session 提取 + peer 选择
}
```

### 测试

```bash
# 不带 key → 401
curl -v http://127.0.0.1:18080/v1/completions -X POST \
  -H 'Content-Type: application/json' \
  -d '{"model":"kimi-k2.5","prompt":[1,2,3],"max_tokens":3}'

# 带正确 key → 透传到后端
curl -v http://127.0.0.1:18080/v1/completions -X POST \
  -H 'Authorization: Bearer REDACTED-API-KEY' \
  -H 'Content-Type: application/json' \
  -d '{"model":"kimi-k2.5","prompt":[1,2,3],"max_tokens":3}'

# 调试端点不需要 key
curl http://127.0.0.1:18080/_active_conns
```

### 注意

- **http 级别的 `access_by_lua_block` 不生效**：会被下层 location 的 access_by_lua 覆盖。必须放在 `location /v1/` 里
- `lua_shared_dict api_keys 1m;`（以及 `active_conns` / `lc_locks` / `bad_peers`）**全部在 `session_route.conf` 顶部声明**（集中管理），`nginx.conf` 不再写 shared_dict
- 要改 key：修改 `access_by_lua_block` 里硬编码的 `ak:set(...)`，reload 即可

## 部署/更新流程

**本地权威副本**：`openresty/nginx.conf` + `openresty/session_route.conf`

```bash
# 1. 备份
ssh <host> "sudo cp /usr/local/openresty/nginx/conf/nginx.conf /usr/local/openresty/nginx/conf/nginx.conf.bak.\$(date +%s)"
ssh <host> "sudo cp /usr/local/openresty/nginx/conf/conf.d/session_route.conf /usr/local/openresty/nginx/conf/conf.d/session_route.conf.bak.\$(date +%s)"

# 2. 上传
scp openresty/nginx.conf openresty/session_route.conf <host>:/tmp/

# 3. 替换
ssh <host> "sudo cp /tmp/nginx.conf /usr/local/openresty/nginx/conf/nginx.conf"
ssh <host> "sudo cp /tmp/session_route.conf /usr/local/openresty/nginx/conf/conf.d/session_route.conf"

# 4. 语法校验（失败不要 reload！）
ssh <host> "sudo /usr/local/openresty/bin/openresty -t"

# 5. 优雅 reload（不断现有连接）
ssh <host> "sudo /usr/local/openresty/bin/openresty -s reload"

# 6. 验证
ssh <host> "curl -s http://127.0.0.1:18080/_health_status | python3 -m json.tool"
```

**注意**：

- `nginx.conf` 包含 `lua_shared_dict bad_peers 1m` 等共享字典声明，必须和 `session_route.conf` **一起同步**
- 生产环境 gateway-host 的 `nginx.conf` 比其他实例多一行 `include /etc/nginx/sites-enabled/*;`（加载公网网关 site），本地权威副本已包含此行
- 验证失败时立即回滚：`sudo cp /usr/local/openresty/nginx/conf/conf.d/session_route.conf.bak.<ts> /usr/local/openresty/nginx/conf/conf.d/session_route.conf && sudo openresty -s reload`

### 回滚

```bash
ssh <host> "sudo cp /usr/local/openresty/nginx/conf/conf.d/session_route.conf.bak.<ts> \
                    /usr/local/openresty/nginx/conf/conf.d/session_route.conf && \
           sudo /usr/local/openresty/bin/openresty -s reload"
```

## 日常运维

### `~/bin/or` 脚本

生产机器上装了 `/root/bin/or`（源码：`openresty/bin/or`）：

```bash
or test        # 语法校验
or reload      # 优雅重载（不断现有连接）
or status      # 进程 + 端口 + worker 数
or conns       # GET /_active_conns
or log         # 看最近 access/error log
or log -f      # tail -F access.log
or log err -f  # tail -F error.log
or start       # 进程没起时启动
or stop        # 优雅停止
or kill        # 强制停（断连）
```

### 手动操作

```bash
sudo /usr/local/openresty/bin/openresty -t                 # 校验
sudo /usr/local/openresty/bin/openresty -s reload          # reload
sudo /usr/local/openresty/bin/openresty                    # 启动
sudo /usr/local/openresty/bin/openresty -T                 # 看完整运行配置
```

### 日志

- access：`/usr/local/openresty/nginx/logs/access.log`
- error：`/usr/local/openresty/nginx/logs/error.log`

Log format (`session_log`)：

```
$remote_addr - [$time_local] "$request" sid="$routed_session_id" src=$routed_source mode=$routed_mode peer=$routed_peer upstream="$upstream_addr" status=$status bytes=$body_bytes_sent rt=$request_time urt=$upstream_response_time uht=$upstream_header_time
```

过滤技巧：

```bash
# 按 peer 过滤
sudo grep 'peer=10.0.0.1:8050' /usr/local/openresty/nginx/logs/access.log

# 长请求（rt > 60s）
sudo awk -F'rt=' '{split($2,a," ");if(a[1]+0>60)print}' /usr/local/openresty/nginx/logs/access.log

# error log 去掉 info 级别
sudo grep -vE '\[info\]' /usr/local/openresty/nginx/logs/error.log | tail -50
```

日志轮转配置：`openresty/logrotate-openresty.conf`（装到 `/etc/logrotate.d/openresty`）。

## 常见故障排查

### 1. `openresty -t` 报 `"init_by_lua_block" is duplicate`

同一 http 块里 `init_by_lua_block` 只能有一个。如果 `nginx.conf` 和 `session_route.conf` 各写了一个就会冲突。解决：合并到一处，或用 `init_worker_by_lua_block` / shared_dict 懒初始化。

### 2. error log 刷 `failed to run balancer_by_lua*: API disabled`

在 `balancer_by_lua_block` 里调用了受限 API（`resty.lock` / `ngx.sleep` / `ngx.socket.tcp`）。解决：把决策逻辑挪到 `access_by_lua_block`（可用 resty.lock），`balancer_by_lua_block` 只留 `balancer.set_current_peer`。

### 3. 客户端报 `Chunk too big`（aiohttp 等）

客户端默认 `read_bufsize=65536` 太小，openresty 高并发下合并 SSE chunk 可能超过。解决：客户端改大（aiohttp: `read_bufsize=2**20`），或在 openresty 加 `postpone_output 0`（已加）。

### 4. 纯空白 sid 全部哈希到同一 peer

原代码 `if sid ~= ""` 只过滤空字符串，没过滤 `"   "` / `"\t"`。已在 `extract_session_id` 用 `_nonblank()` 统一过滤（要求 sid 至少含一个非空白字符）。

### 5. 老 worker 不退出

reload 后旧 worker 继续服务已建立的长连接（SSE 长请求常见几千秒）。正常现象，可以等到连接自然结束，或手动 `kill -QUIT <old-worker-pid>`（优雅）/ `kill -TERM <pid>`（暴力）。

### 6. Systemd 自启 / 启不起来

OpenResty 的 systemd unit **已 enabled**（`systemctl is-enabled openresty` → `enabled`），机器重启后会自动拉起，加载 `/usr/local/openresty/nginx/conf/nginx.conf` + `conf.d/*`。不需要人工干预。

**注意**：日常 `or reload` / 手动 `openresty -s reload` 出来的 master 进程不归 systemd 管（`systemctl status` 会显示 `inactive` 但 `netstat -ltnp` 显示进程在跑，PPID=1）。这是正常现象，systemd unit 和手动启动的进程共用同一份配置文件，行为上等价；只是 `systemctl stop/reload` 对手动起的进程无效，此时用 `or kill` + `or start` 或 `openresty -s reload`。

**apt 升级风险**：`apt upgrade openresty` 可能触发 systemd 重启，把手动起的进程杀掉换成 systemd 管的。配置本身没变所以服务不会中断，但 pid 会变。如果在意这点，升级前 `systemctl disable openresty` 再升级。

## 全新机器部署

参考 `openresty/install_openresty.sh`，大致流程：

1. `apt install` 编译依赖（libssl-dev / zlib1g-dev / pcre-dev 等）
2. 下载 OpenResty 源码，`./configure --with-http_ssl_module ...`，`make && make install`
3. 创建 `/etc/systemd/system/openresty.service`（可选，我们手动管）
4. 从本地 `openresty/` 拷 nginx.conf + session_route.conf 到 `/usr/local/openresty/nginx/conf/`
5. 校验 + 启动：`openresty -t` + `openresty`
