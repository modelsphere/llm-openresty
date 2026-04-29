#!/bin/bash
# 编译 Linux x86_64 静态二进制 → bodylog-listener
# 输出：~2.8MB，无 glibc 依赖（CGO_ENABLED=0），可直接 scp 到任何 x86_64 Linux 跑
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
    go build -ldflags="-s -w" -o bodylog-listener .

ls -la bodylog-listener
file bodylog-listener
