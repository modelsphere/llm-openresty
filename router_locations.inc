# openresty/router_locations.inc
# 所有路由共享的 server-body + /v1/ + 全部 debug location,用 $route 参数化
# (_G.__route_opts[ngx.var.route])。被每个路由的 server{} 于 server 上下文 include
# (该 server 必须先 `set $route "<name>";`)。**这是权威源码,新增/改公共 location 直接改这里
# → 全部路由同时生效**(最初由 split_lua.py 从 K2.5 server 块一次性搬出,该脚本已不再运行)。
# ⚠️ 相比重构前 per-model conf 各自的 location,本共享 include 让所有路由统一暴露同一套 endpoint:
#    - /_429_status、/_kimi_normalize_toggle 原仅 k25 有 → 现所有端口都有(前者读全局 reject_stat、
#      **无 allow 限制**;后者 127.0.0.1-only)。写类 endpoint 均带 allow 127.0.0.1。
# 注:原 K2.5 server 里的 @vllm_retry / proxy_intercept_errors / error_page 400 死路已删
#    (自 2026-05-18 关、vllm_retry_static 早 down);连带删 do_log_release_retry_byvar 与 routed_dict。
    access_log  logs/access.log  session_log;

    # allow up to 1m JSON body fully in memory (get_body_data works up to this size)
    client_body_buffer_size 2m;
    client_max_body_size    50m;

    # stream responses without accumulating — minimize SSE chunk coalescing
    postpone_output 0;

    # 暴露给 log_format 的自定义变量（由 access_by_lua_block 填充）
    # 必须在 access 阶段前 set，log 阶段才能读
    set $routed_session_id "-";
    set $routed_source     "-";
    set $routed_mode       "-";
    set $routed_peer       "-";

    # --- session-routed vLLM-style endpoints -------------------------------
    location /v1/ {
        access_by_lua_block { _G.do_route(_G.__route_opts[ngx.var.route]) }

        # body-log: 累积 SSE 响应 chunk 到 ngx.ctx (无锁，仅 active 时执行)
        body_filter_by_lua_block {
            _G.bodylog_filter_chunk()
        }

        # 路由决策不再写到响应 header（X-Routed-Session/Source/Mode/Peer/Retries 已删除，
        # 防止外部泄露 sid / 后端 peer / 内部模式）。需要 debug 时改用 /_route_inspect。
        # peer 连接失败/超时/502/503 时自动重试下一个 fallback peer
        # （配合 balancer_by_lua 的 set_more_tries 生效）
        # non_idempotent: 允许 POST 在 upstream 发送请求阶段失败时重试。vllm 所有请求都是 POST，
        # 不带这个标志的话 mock/vllm reset TCP 会直接回 502 给客户端。
        proxy_next_upstream error timeout http_502 http_503 non_idempotent;
        proxy_next_upstream_timeout 60s;   # 整个重试链条最多 60s（需 > connect_timeout × 重试次数）

        proxy_pass http://vllm_backends;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        # router 在 response 里回 X-Routed-Peer 标识真实后端 vllm；nginx 默认透传给
        # 客户端会泄露内部 IP。$upstream_http_x_routed_peer 在 access_log / bodylog
        # 里仍可读到，proxy_hide_header 只影响发给下游客户端的响应头。
        proxy_hide_header X-Routed-Peer;
        proxy_connect_timeout 10s;         # 10s 连不上就判定挂了（原 30s 太慢，会耗完重试窗口）
        proxy_send_timeout   3600s;
        proxy_read_timeout   3600s;   # long generation (64k tokens) may take many minutes

        # streaming (SSE) support: don't buffer response, disable gzip
        proxy_buffering off;
        proxy_cache off;
        proxy_request_buffering on;  # keep request buffered so we can read body
        gzip off;
        chunked_transfer_encoding on;

        # release the active_conns counter when this request finishes + 触发 bodylog 异步落盘
        log_by_lua_block { _G.do_log_release(_G.__route_opts[ngx.var.route]) }
    }

    # 查看所有 peer 健康状态（active_conns + banned）
    location = /_health_status {
        content_by_lua_block { _G.dbg_health_status(_G.__route_opts[ngx.var.route]) }
    }

    # Dump current least_conn counters — useful for debugging load distribution
    location = /_active_conns {
        content_by_lua_block { _G.dbg_active_conns(_G.__route_opts[ngx.var.route]) }
    }

    # 查看 5min 滑动窗 cluster_avg 状态：10 个 30s 桶 + 实时 sum + 当前桶位
    location = /_cluster_avg {
        content_by_lua_block { _G.dbg_cluster_avg(_G.__route_opts[ngx.var.route]) }
    }

    # 矫正 active_conns 计数器（kill -9 / SIGTERM 砍 worker 时 log_by_lua 不跑会
    # leak phantom 计数）。仅 127.0.0.1 可访问。三种模式：
    #   GET /_active_conns_set?peer=<host:port>&value=<N>   — 把单个 key 设成 N
    #   GET /_active_conns_set?peer=<host:port>&delete=1    — 删除单个 key
    #   GET /_active_conns_set?flush=1                       — 清空全部计数器
    location = /_active_conns_set {
        allow 127.0.0.1;
        deny all;
        content_by_lua_block { _G.dbg_active_conns_set(_G.__route_opts[ngx.var.route]) }
    }

    # /_route_debug?sid=<x>    — hash preview, 考虑 bad_peers
    # POST /_route_inspect     — returns what prepare_request() would pick
    #                            for the given request (header + body), without proxying
    location = /_route_debug {
        content_by_lua_block { _G.dbg_route_debug(_G.__route_opts[ngx.var.route]) }
    }

    location = /_route_inspect {
        content_by_lua_block { _G.dbg_route_inspect(_G.__route_opts[ngx.var.route]) }
    }

    # /_cch_test — 诊断用：跑 prepare_request() 后回显处理后 body 的元信息（md5/len + cch_stripped 标志）
    # 安全：仅本机访问。gateway-host 的 :443 llm-gateway 站点把 /llm/* 反代到本 server，
    # 没这条 allow/deny 就把"用我的 cch strip 函数处理你的 body"暴露成公网接口了。
    # 也不再回显 body 本身，避免被当成 body 镜像滥用。
    location = /_cch_test {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_cch_test(_G.__route_opts[ngx.var.route]) }
    }

    # /_route_state — 集群路由层级状态诊断
    # 返回：{active_level, limit, healthy_peers_in_level, by_priority:{prio:{healthy,active,max}}}
    location = /_route_state {
        content_by_lua_block { _G.dbg_route_state(_G.__route_opts[ngx.var.route]) }
    }
    location = /_ttft_status {
        content_by_lua_block { _G.dbg_ttft_status(_G.__route_opts[ngx.var.route]) }
    }
    location = /_ttft_toggle {
        allow 127.0.0.1; deny all;
        content_by_lua_block { _G.dbg_ttft_toggle(_G.__route_opts[ngx.var.route]) }
    }
    location = /_ttft_limit {
        allow 127.0.0.1; deny all;
        content_by_lua_block { _G.dbg_ttft_limit(_G.__route_opts[ngx.var.route]) }
    }
    location = /_tps_status {
        content_by_lua_block { _G.dbg_tps_status(_G.__route_opts[ngx.var.route]) }
    }
    location = /_tps_toggle {
        allow 127.0.0.1; deny all;
        content_by_lua_block { _G.dbg_tps_toggle(_G.__route_opts[ngx.var.route]) }
    }
    location = /_tps_limit {
        allow 127.0.0.1; deny all;
        content_by_lua_block { _G.dbg_tps_limit(_G.__route_opts[ngx.var.route]) }
    }
    # GET /_429_status → 全路由 429 累计计数(route × reason 聚合);?reset=1 清零(仅 127.0.0.1)。
    # reject_stat 是全局共享 dict,dbg_429_status 忽略 opts、读全局聚合 → 每个路由端口返回相同结果。
    # (重构前只在 k25:18080 注册;现经共享 include 在所有路由端口都可访问,输出一致。)
    location = /_429_status {
        content_by_lua_block { _G.dbg_429_status(_G.__route_opts[ngx.var.route]) }
    }

    # body-log 控制 endpoint（仅 127.0.0.1）
    # GET  /_bodylog_status                  → 当前开关 / 采样率 / 计数
    # POST /_bodylog_toggle?on=1             → 开（采样率沿用，默认 100）
    # POST /_bodylog_toggle?on=0             → 关
    # POST /_bodylog_toggle?on=1&pct=10      → 开 + 设采样率 10%
    # POST /_bodylog_toggle?pct=N            → 仅改采样率（不改 enabled）
    location = /_bodylog_status {
        allow 127.0.0.1;
        deny all;
        content_by_lua_block { _G.dbg_bodylog_status(_G.__route_opts[ngx.var.route]) }
    }

    location = /_bodylog_toggle {
        allow 127.0.0.1;
        deny all;
        content_by_lua_block { _G.dbg_bodylog_toggle(_G.__route_opts[ngx.var.route]) }
    }

    # /_cch_strip_status   — 查 cch-strip 开关 + 累计计数
    # /_cch_strip_toggle   — 切开关: POST /_cch_strip_toggle?on=1 / on=0 / 不带参数仅查询
    # 仅本机访问；shared dict 跨 reload 持久，nginx master 重启才回默认
    location = /_cch_strip_status {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_cch_strip_status(_G.__route_opts[ngx.var.route]) }
    }

    location = /_cch_strip_toggle {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_cch_strip_toggle(_G.__route_opts[ngx.var.route]) }
    }

    # /_kimi_normalize_toggle   — Kimi tool_call_id 规范化开关 (POST ?on=1 / on=0 / 不带参数查询)
    # 启用后,对 KIMI_NORMALIZE_MODELS 命中的请求,把 assistant.tool_calls[].id
    # 从 call_xxx / Name:N / namen 改写为 functions.<fn>:<idx>,同步更新 tool.tool_call_id;
    # 并把 assistant.content == null 改成 ""。
    # 计数器：ngx.shared.cch_ctl 内 kimi_normalize_changes_total / kimi_normalize_req_count
    location = /_kimi_normalize_toggle {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_kimi_normalize_toggle(_G.__route_opts[ngx.var.route]) }
    }

    # /_include_usage_toggle — 新格式 peers_by_model + stream 的 include_usage 注入开关
    # (POST ?on=1 / on=0 / 不带参数查询)。默认开;承重转换,关了新格式 stream 拿不到 usage。
    location = /_include_usage_toggle {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_include_usage_toggle(_G.__route_opts[ngx.var.route]) }
    }

    # /_reject_rules_status  — 查本路由规则化拒绝:规则列表 + enabled 态 + 各规则命中数
    # /_reject_rules_toggle  — 热切规则总开关: POST ?on=1 / on=0[&reset=1] / 不带参数查询
    # 两者都仅本机:reject_rules 是反滥用控制,规则阈值/模式不应对外暴露(否则可被探出后精准绕过);
    # 聚合命中数走公开的 /_429_status(reason=rule)。规则本体配在各 conf 的 factory;实现 lua/reject_rules.lua。
    location = /_reject_rules_status {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_reject_rules_status(_G.__route_opts[ngx.var.route]) }
    }

    location = /_reject_rules_toggle {
        allow 127.0.0.1;
        allow ::1;
        deny  all;
        content_by_lua_block { _G.dbg_reject_rules_toggle(_G.__route_opts[ngx.var.route]) }
    }
