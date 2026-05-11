#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClawShell Local — 平台自动发现脚本
# 检测 Hermes / OpenClaw / 悟空 是否已安装
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
CONFIG_DIR="$HOME/.clawshell-local/config/platforms"
mkdir -p "$CONFIG_DIR"

detect_hermes() {
    echo -e "${BLUE}  检测 Hermes...${NC}"
    HERMES_STATUS="未检测到"
    HERMES_VERSION=""
    HERMES_ROOT=""

    # 方法1: 检查 launchd plist
    if [[ -f "$HOME/Library/LaunchAgents/ai.hermes.gateway.plist" ]]; then
        HERMES_STATUS="已安装 (plist)"
        HERMES_ROOT="$HOME/.hermes"
    fi

    # 方法2: 检查 hermes 命令
    if command -v hermes &>/dev/null; then
        HERMES_VERSION=$(hermes --version 2>/dev/null || echo "unknown")
        HERMES_STATUS="命令行可用"
    fi

    # 方法3: 检查进程
    if pgrep -f "hermes.*gateway" &>/dev/null; then
        HERMES_STATUS="运行中"
    fi

    # 方法4: 检查目录
    if [[ -d "$HOME/.hermes" ]]; then
        HERMES_ROOT="$HOME/.hermes"
    fi

    cat > "$CONFIG_DIR/hermes.yaml" << EOF
platform: hermes
detected: $([[ "$HERMES_STATUS" != "未检测到" ]] && echo "true" || echo "false")
status: $HERMES_STATUS
version: $HERMES_VERSION
config_root: ${HERMES_ROOT:-}
launchctl_name: ai.hermes.gateway
mcp_config_path: ${HERMES_ROOT:-/Users/yangyang/.hermes}/config/mcp.yaml
EOF

    if [[ "$HERMES_STATUS" != "未检测到" ]]; then
        echo -e "${GREEN}    ✓ Hermes: $HERMES_STATUS${NC}"
    else
        echo -e "${YELLOW}    ✗ Hermes: 未检测到${NC}"
    fi
}

detect_openclaw() {
    echo -e "${BLUE}  检测 OpenClaw...${NC}"
    OC_STATUS="未检测到"
    OC_VERSION=""
    OC_ROOT=""

    # 方法1: 检查目录
    if [[ -d "$HOME/.openclaw" ]]; then
        OC_STATUS="已安装"
        OC_ROOT="$HOME/.openclaw"
    fi

    # 方法2: 检查命令行
    if command -v openclaw &>/dev/null; then
        OC_VERSION=$(openclaw --version 2>/dev/null || echo "unknown")
        OC_STATUS="命令行可用"
    fi

    # 方法3: 检查 /usr/local/bin
    if [[ -x "/usr/local/bin/openclaw" ]]; then
        OC_STATUS="已安装 (bin)"
        OC_ROOT="/usr/local/bin"
    fi

    cat > "$CONFIG_DIR/openclaw.yaml" << EOF
platform: openclaw
detected: $([[ "$OC_STATUS" != "未检测到" ]] && echo "true" || echo "false")
status: $OC_STATUS
version: $OC_VERSION
root_path: ${OC_ROOT:-}
cli_path: $(command -v openclaw 2>/dev/null || echo "")
EOF

    if [[ "$OC_STATUS" != "未检测到" ]]; then
        echo -e "${GREEN}    ✓ OpenClaw: $OC_STATUS${NC}"
    else
        echo -e "${YELLOW}    ✗ OpenClaw: 未检测到${NC}"
    fi
}

detect_wukong() {
    echo -e "${BLUE}  检测 悟空...${NC}"
    WK_STATUS="未检测到"
    WK_VERSION=""
    WK_ROOT=""

    # 方法1: 检查应用
    if [[ -d "/Applications/悟空.app" ]] || [[ -d "$HOME/Applications/悟空.app" ]]; then
        WK_STATUS="应用已安装"
        WK_ROOT="$HOME/Applications/悟空.app"
    fi

    # 方法2: 检查命令行
    if command -v wukong &>/dev/null; then
        WK_VERSION=$(wukong --version 2>/dev/null || echo "unknown")
        WK_STATUS="命令行可用"
    fi

    # 方法3: 检查配置目录
    if [[ -d "$HOME/.wukong" ]]; then
        WK_STATUS="配置目录存在"
        WK_ROOT="$HOME/.wukong"
    fi

    cat > "$CONFIG_DIR/wukong.yaml" << EOF
platform: wukong
detected: $([[ "$WK_STATUS" != "未检测到" ]] && echo "true" || echo "false")
status: $WK_STATUS
version: $WK_VERSION
app_path: ${WK_ROOT:-}
config_path: $HOME/.wukong
EOF

    if [[ "$WK_STATUS" != "未检测到" ]]; then
        echo -e "${GREEN}    ✓ 悟空: $WK_STATUS${NC}"
    else
        echo -e "${YELLOW}    ✗ 悟空: 未检测到${NC}"
    fi
}

echo -e "\n${GREEN}=== 平台自动发现 ===${NC}"
detect_hermes
detect_openclaw
detect_wukong
echo -e "${GREEN}检测完成，配置已写入 $CONFIG_DIR/${NC}"

# 输出汇总
echo -e "\n${GREEN}检测结果汇总:${NC}"
DETECTED=$(grep "detected: true" "$CONFIG_DIR"/*.yaml 2>/dev/null | wc -l | tr -d ' ')
echo "  共检测到 $DETECTED 个平台"
