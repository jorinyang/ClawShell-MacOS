#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClawShell Local — 生态组件安装脚本
# 安装 MemPalace / Memos / n8n / ChromaDB
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

show_menu() {
    echo -e "${BLUE}ClawShell Local 生态组件安装${NC}"
    echo -e "${BLUE}============================${NC}"
    echo "  [1] MemPalace (记忆中枢)        $(command -v mempalace &>/dev/null && echo ✓ 已安装 || echo - 未安装)"
    echo "  [2] Memos (轻量笔记)            $(command -v memos &>/dev/null && echo ✓ 已安装 || echo - 未安装)"
    echo "  [3] n8n (工作流自动化)          $(command -v n8n &>/dev/null && echo ✓ 已安装 || echo - 未安装)"
    echo "  [4] ChromaDB (本地向量缓存)      $(python3 -c 'import chromadb' 2>/dev/null && echo ✓ 已安装 || echo - 未安装)"
    echo "  [5] 全部安装 (推荐)"
    echo "  [6] 自定义选择"
    echo "  [0] 跳过"
    echo ""
}

install_mempalace() {
    echo -e "${GREEN}  安装 MemPalace...${NC}"
    pip3 install --quiet mempalace
    mkdir -p "$HOME/.mempalace"
    echo -e "${GREEN}  ✓ MemPalace 安装完成${NC}"
}

install_memos() {
    echo -e "${GREEN}  安装 Memos...${NC}"
    if command -v brew &>/dev/null; then
        brew install memos
    else
        echo -e "${YELLOW}  警告: 需要 Homebrew，请从 https://brew.sh 安装${NC}"
        echo -e "${YELLOW}  或使用 Docker: docker run -d --name memos -p 5230:5230 ghcr.io/usememos/memos:latest${NC}"
    fi
    echo -e "${GREEN}  ✓ Memos 安装完成${NC}"
}

install_n8n() {
    echo -e "${GREEN}  安装 n8n...${NC}"
    if command -v npm &>/dev/null; then
        npm install -g n8n
    else
        echo -e "${YELLOW}  警告: 需要 npm，请安装 Node.js${NC}"
        echo -e "${YELLOW}  或使用 Docker: docker run -d --name n8n -p 5678:5678 n8nio/n8n${NC}"
    fi
    echo -e "${GREEN}  ✓ n8n 安装完成${NC}"
}

install_chromadb() {
    echo -e "${GREEN}  安装 ChromaDB...${NC}"
    pip3 install --quiet chromadb
    echo -e "${GREEN}  ✓ ChromaDB 安装完成${NC}"
}

show_menu
read -rp "请选择 [0-6]: " CHOICE

case $CHOICE in
    1) install_mempalace ;;
    2) install_memos ;;
    3) install_n8n ;;
    4) install_chromadb ;;
    5)
        install_mempalace
        install_memos
        install_n8n
        install_chromadb
        ;;
    6)
        echo "请输入要安装的组件编号（空格分隔，如 '1 3 4'）:"
        read -ra COMPONENTS
        for c in "${COMPONENTS[@]}"; do
            case $c in
                1) install_mempalace ;;
                2) install_memos ;;
                3) install_n8n ;;
                4) install_chromadb ;;
            esac
        done
        ;;
    0) echo "跳过安装" ;;
    *) echo -e "${RED}无效选择${NC}" ;;
esac

echo -e "\n${GREEN}生态组件安装完成${NC}"
