#!/bin/bash
# 编译 bodylog-listener。
#
# ⚠️ 自从内嵌 DuckDB（go-duckdb，用于 /metrics 查询 + metrics 明细 jsonl→parquet 转换）后，
#    本程序需要 CGO_ENABLED=1，不再是「无 glibc 依赖的纯静态二进制」：
#      - 产物动态链接 libc / libstdc++ / libm / libdl（~46MB）
#      - 绑定构建机的 glibc 版本：必须在 glibc ≤ 目标机 的环境上编译，否则运行报
#        `version GLIBC_x.xx not found`
#      - CGO 不能简单跨平台交叉编译（需对应平台的 C 工具链）
#
# 因此部署 gateway-host（linux/amd64）时，**直接在 gateway-host 上 build**（或用与之 glibc 匹配的
# linux/amd64 容器），不要在 mac 上交叉编译。gateway-host 需有 go + gcc。
#
# 回退：若目标环境装 CGO 工具链困难，可改用 shell-out 调 duckdb CLI 的版本（见 README）。
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# 在 Linux/amd64 目标机上跑：原生 CGO 构建
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o bodylog-listener .

ls -la bodylog-listener
file bodylog-listener
echo "--- runtime libs ---"
ldd bodylog-listener 2>/dev/null || otool -L bodylog-listener 2>/dev/null || true
