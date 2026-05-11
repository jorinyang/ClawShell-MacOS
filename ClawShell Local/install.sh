#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClawShell Local — 端侧安装脚本
# 支持 macOS，自动发现并适配 Hermes/OpenClaw/悟空
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLAWSHELL_LOCAL_DIR="$HOME/.clawshell-local"
CONFIG_DIR="$HOME/.clawshell-local/config"

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  ClawShell Local — 端侧安装程序${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Step 1: 检查依赖
echo -e "${GREEN}[1/7] 检查系统依赖...${NC}"
MISSING_DEPS=()
command -v python3 >/dev/null 2>&1 || MISSING_DEPS+=("python3")
command -v pip3 >/dev/null 2>&1 || MISSING_DEPS+=("pip3")
command -v jq >/dev/null 2>&1 || MISSING_DEPS+=("jq")

if [[ ${#MISSING_DEPS[@]} -gt 0 ]]; then
    echo -e "${RED}缺少依赖: ${MISSING_DEPS[*]}${NC}"
    echo -e "${YELLOW}请先安装: brew install ${MISSING_DEPS[*]}${NC}"
    exit 1
fi
echo -e "${GREEN}  依赖检查通过${NC}"

# Step 2: 创建目录结构
echo -e "${GREEN}[2/7] 创建目录结构...${NC}"
mkdir -p "$CLAWSHELL_LOCAL_DIR"/{cache/{memory,skills,kanban},sync,state,logs}
mkdir -p "$CONFIG_DIR"/{platforms,templates}
echo "  安装目录: $CLAWSHELL_LOCAL_DIR"

# Step 3: 平台自动发现
echo -e "${GREEN}[3/7] 正在自动发现已安装的平台...${NC}"
if [[ -x "$SCRIPT_DIR/scripts/discover.sh" ]]; then
    bash "$SCRIPT_DIR/scripts/discover.sh"
else
    echo -e "${YELLOW}  discover.sh 未找到，跳过自动发现${NC}"
fi

# Step 4: 生态组件安装
echo -e "${GREEN}[4/7] 生态组件安装...${NC}"
if [[ -x "$SCRIPT_DIR/scripts/install-ecosystem.sh" ]]; then
    bash "$SCRIPT_DIR/scripts/install-ecosystem.sh
else
    echo -e "${YELLOW}  install-ecosystem.sh 未找到，跳过${NC}"
fi

# Step 5: Edge Gateway 安装
echo -e "${GREEN}[5/7] 安装 Edge Gateway...${NC}"
if [[ -x "$SCRIPT_DIR/scripts/setup-edge-gateway.sh" ]]; then
    bash "$SCRIPT_DIR/scripts/setup-edge-gateway.sh
else
    echo -e "${YELLOW}  setup-edge-gateway.sh 未找到，跳过${NC}"
fi

# Step 6: 云端连接配置
echo -e "${GREEN}[6/7] 配置云端连接...${NC}"
read -rp "请输入 Cloud Hub URL [wss://your-cloud.com/hub]: " CLOUD_URL
CLOUD_URL=${CLOUD_URL:-wss://your-cloud.com/hub}

read -rp "请输入 JWT Token (从云端控制台获取): " JWT_TOKEN
if [[ -z "$JWT_TOKEN" ]]; then
    echo -e "${RED}JWT Token 不能为空${NC}"
    exit 1
fi

cat > "$CONFIG_DIR/cloud.json" << EOF
{
    "cloud_url": "$CLOUD_URL",
    "jwt_token": "$JWT_TOKEN",
    "sync_interval_seconds": 60,
    "offline_cache_enabled": true
}
EOF
echo "  云端配置已保存"

# Step 7: 启动 Edge Gateway
echo -e "${GREEN}[7/7] 启动 Edge Gateway...${NC}"
if [[ -f "$HOME/.clawshell-local/edge-gateway/gateway.py" ]]; then
    nohup python3 "$HOME/.clawshell-local/edge-gateway/gateway.py"         >> "$HOME/.clawshell-local/logs/gateway.log" 2>&1 &
    echo $! > "$HOME/.clawshell-local/state/gateway.pid"
    echo -e "${GREEN}  Edge Gateway 已启动 (PID: $(cat $HOME/.clawshell-local/state/gateway.pid))${NC}"
else
    echo -e "${YELLOW}  gateway.py 未找到，请手动启动${NC}"
fi

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  ClawShell Local 安装完成！${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo -e "配置文件: $CONFIG_DIR/cloud.json"
echo -e "日志目录: $HOME/.clawshell-local/logs/"
echo -e "同步状态: $HOME/.clawshell-local/sync/"
echo ""
echo -e "常用命令:"
echo -e "  查看状态:  python3 $HOME/.clawshell-local/edge-gateway/gateway.py status"
echo -e "  手动同步:  bash $SCRIPT_DIR/scripts/sync-now.sh"
echo -e "  查看日志:  tail -f $HOME/.clawshell-local/logs/gateway.log"
echo ""
