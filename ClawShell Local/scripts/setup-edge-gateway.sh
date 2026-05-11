#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClawShell Local — Edge Gateway 安装脚本
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
GATEWAY_DIR="$HOME/.clawshell-local/edge-gateway"

echo -e "${GREEN}安装 ClawShell Edge Gateway...${NC}"

# 创建 Python venv
python3 -m venv "$GATEWAY_DIR/venv"
"$GATEWAY_DIR/venv/bin/pip" install --quiet websockets pyjwt aiofiles

# 复制 gateway 代码
mkdir -p "$GATEWAY_DIR/src"
cp -r "$(dirname "$0")/../edge-gateway/src/"* "$GATEWAY_DIR/src/" 2>/dev/null || true
cp "$(dirname "$0")/../edge-gateway/config.yaml" "$GATEWAY_DIR/config.yaml" 2>/dev/null || true

# 创建启动脚本
cat > "$GATEWAY_DIR/run.sh" << 'RUNEOF'
#!/usr/bin/env bash
cd "$(dirname "$0")"
source venv/bin/activate
exec python src/gateway.py "$@"
RUNEOF
chmod +x "$GATEWAY_DIR/run.sh"

echo -e "${GREEN}✓ Edge Gateway 安装完成${NC}"
echo "  安装目录: $GATEWAY_DIR"
echo "  启动: $GATEWAY_DIR/run.sh"
