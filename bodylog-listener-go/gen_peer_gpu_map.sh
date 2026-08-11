#!/usr/bin/env bash
# 从 monitor.conf 生成 bodylog-listener 的「后端 → GPU 型号」映射文件。
#
# 背景:明细里的 backend_gpu 由 listener 查表得到,不由 openresty 逐请求传输
# ——GPU 型号是低频变化的静态属性,逐请求传纯属浪费;且同一映射放两处必然走偏
# (openresty 侧一度按"本层选中的 peer"取型号,而生产主路径本层选中的是 CART
#  中间层,取到的永远为空)。listener 按归一化后的真实后端查,口径与 backend 一致。
#
# 权威来源 = monitor.conf 的 service 行:
#     service: <name> | <url> | <model> | <gpu_type>
# 取 url 的 host:port 与 gpu_type 两列。
#
# 用法:
#   bash gen_peer_gpu_map.sh /path/to/monitor.conf > /data/bodylog/peer_gpu.map
#   # listener 侧:BODYLOG_PEER_GPU_FILE=/data/bodylog/peer_gpu.map
# 映射在 listener 启动时加载一次,改表后需重启 listener 生效。
set -euo pipefail
CONF="${1:-monitor.conf}"
[ -f "$CONF" ] || { echo "找不到 $CONF" >&2; exit 1; }

echo "# 由 gen_peer_gpu_map.sh 从 $(basename "$CONF") 生成,勿手改"
echo "# 格式:<ip:port> <GPU型号>"
awk -F'|' '
  /^[[:space:]]*service:/ && NF >= 4 {
    url = $2; gpu = $4
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", url)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", gpu)
    sub(/^https?:\/\//, "", url)     # 剥 scheme
    sub(/\/.*$/, "", url)            # 剥 path,留 host:port
    # gpu_type 允许带占卡后缀(如 "B300:0-7" = 该服务占 0-7 号卡),统计只要型号 → 去掉后缀
    sub(/:.*$/, "", gpu)
    if (url != "" && gpu != "") print url, gpu
  }
' "$CONF" | sort -u
