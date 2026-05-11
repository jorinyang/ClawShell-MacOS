#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClawShell — 本地配置迁移脚本
# 将本地 ~/.clawshell-local 迁移到新环境或云端
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

CLAWSHELL_LOCAL="$HOME/.clawshell-local"
BACKUP_DIR="${CLAWSHELL_LOCAL}.backup.$(date +%Y%m%d_%H%M%S)"
OSS_BUCKET="${OSS_BUCKET:-clawshell-vault}"
RCLONE_REMOTE="${RCLONE_REMOTE:-clawshell-vault}"

echo -e "${BLUE}=== ClawShell 本地迁移工具 ===${NC}\n"

# ─── 导出 ───────────────────────────────────────────────────

export_from_local() {
    echo -e "${GREEN}[1/3] 导出本地配置...${NC}"

    mkdir -p "$BACKUP_DIR"
    mkdir -p "$BACKUP_DIR"/{config,cache,logs,state}

    # 关键配置文件
    cp -r "$CLAWSHELL_LOCAL/config"/* "$BACKUP_DIR/config/" 2>/dev/null || true
    echo "  已备份配置到 $BACKUP_DIR/config/"

    # 上传 vault 到 OSS
    if command -v rclone &>/dev/null; then
        echo -e "${YELLOW}  上传 vault 到 OSS...${NC}"
        rclone copy "$HOME/Documents/Obsidian" "${RCLONE_REMOTE}:${OSS_BUCKET}/vault/" \
            --exclude "*.DS_Store" --transfers 4
        echo -e "${GREEN}  ✓ Vault 已同步到 OSS${NC}"
    else
        echo -e "${YELLOW}  rclone 未安装，跳过 vault 同步${NC}"
    fi

    echo -e "${GREEN}  导出完成: $BACKUP_DIR${NC}"
}

# ─── 导入 ───────────────────────────────────────────────────

import_to_local() {
    echo -e "${GREEN}[2/3] 导入到本地...${NC}"

    if [[ ! -d "$BACKUP_DIR/config" ]]; then
        echo -e "${RED}  备份目录无效: $BACKUP_DIR${NC}"
        exit 1
    fi

    mkdir -p "$CLAWSHELL_LOCAL"/{config,cache,logs,state}
    cp -r "$BACKUP_DIR/config/"* "$CLAWSHELL_LOCAL/config/" || true

    # 从 OSS 拉取 vault
    if command -v rclone &>/dev/null; then
        echo -e "${YELLOW}  从 OSS 拉取 vault...${NC}"
        rclone copy "${RCLONE_REMOTE}:${OSS_BUCKET}/vault/" "$HOME/Documents/Obsidian" \
            --create-empty-src-dirs --transfers 4
        echo -e "${GREEN}  ✓ Vault 已同步到本地${NC}"
    fi

    echo -e "${GREEN}  导入完成${NC}"
}

# ─── 清理 ───────────────────────────────────────────────────

cleanup_backup() {
    echo -e "${GREEN}[3/3] 清理备份...${NC}"
    if [[ -d "$BACKUP_DIR" ]]; then
        rm -rf "$BACKUP_DIR"
        echo "  已删除临时备份: $BACKUP_DIR"
    fi
}

# ─── 菜单 ───────────────────────────────────────────────────

show_usage() {
    echo "用法: $0 <command>"
    echo ""
    echo "命令:"
    echo "  export   导出本地配置到备份目录，并同步 vault 到 OSS"
    echo "  import  从备份目录恢复到本地，并从 OSS 拉取 vault"
    echo "  clean   删除临时备份"
    echo ""
}

case "${1:-}" in
    export)
        export_from_local
        ;;
    import)
        import_to_local
        ;;
    clean)
        cleanup_backup
        ;;
    *)
        show_usage
        ;;
esac
