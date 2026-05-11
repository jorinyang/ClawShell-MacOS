#!/usr/bin/env bash
set -euo pipefail
# 手动触发同步
GATEWAY_PID=$(cat "$HOME/.clawshell-local/state/gateway.pid" 2>/dev/null || echo "")
if [[ -n "$GATEWAY_PID" ]] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    echo "Edge Gateway 运行中 (PID: $GATEWAY_PID)"
    python3 "$HOME/.clawshell-local/edge-gateway/src/sync_engine.py" --full-sync
else
    echo "Edge Gateway 未运行，请先启动"
fi
