#!/bin/bash
#============================================================
# ClawShell Vault OSS 双向同步脚本
# 监听本地 Obsidian Vault 变更 → 上传至 OSS
# 定期从 OSS 拉取变更 → 合并到本地
#============================================================
set -euo pipefail

# ── 配置 ───────────────────────────────────────────────────
VAULT_DIR="${VAULT_DIR:-$HOME/Documents/Obsidian}"
REMOTE="clawshell-vault"
BUCKET="clawshell-vault"
OSS_PREFIX="vault"
LOCAL_PREFIX="Obsidian"
LOCK_FILE="$HOME/.clawshell-local/sync/vault-oss.lock"
STATE_DIR="$HOME/.clawshell-local/sync"
LOG_FILE="$STATE_DIR/vault-oss.log"

# OSS endpoint (香港节点)
OSS_ENDPOINT="oss-cn-hongkong.aliyuncs.com"

# ── 颜色日志 ───────────────────────────────────────────────
log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
warn() { echo "[$(date '+%H:%M:%S')] WARN: $*" | tee -a "$LOG_FILE" >&2; }
err()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2; exit 1; }

# ── 前置检查 ────────────────────────────────────────────────
init() {
  mkdir -p "$STATE_DIR"
  touch "$LOG_FILE"

  if [[ ! -d "$VAULT_DIR" ]]; then
    err "Vault 目录不存在: $VAULT_DIR"
  fi

  # 检查 rclone
  if ! command -v rclone &>/dev/null; then
    err "rclone 未安装: https://rclone.org/install/"
  fi

  # 检查 rclone config
  if ! rclone listremotes 2>/dev/null | grep -q "^${REMOTE}:$"; then
    err "rclone remote '$REMOTE' 未配置: rclone config create $REMOTE s3 ..."
  fi

  log "Vault: $VAULT_DIR"
  log "Remote: ${REMOTE}:${BUCKET}/${OSS_PREFIX}/"
  log "Endpoint: $OSS_ENDPOINT"
}

# ── 写入锁 ─────────────────────────────────────────────────
acquire_lock() {
  mkdir -p "$(dirname "$LOCK_FILE")"
  if mkdir "$LOCK_FILE" 2>/dev/null; then
    trap 'release_lock' EXIT
    return 0
  fi
  err "同步进程已在运行，锁文件存在: $LOCK_FILE"
}

release_lock() {
  rm -rf "$LOCK_FILE"
}

# ── OSS 上传 ───────────────────────────────────────────────
# 将本地 vault 目录中变更的文件同步到 OSS
# 用 md5 检查：本地 md5 ≠ OSS md5 时才上传（避免无意义上传）
sync_to_oss() {
  log "→ 上传本地变更到 OSS..."

  # 获取远程文件列表（用于 md5 比对）
  local tmp_remote_list="/tmp/oss-vault-list-$$.txt"
  rclone ls "${REMOTE}:${BUCKET}/${OSS_PREFIX}/" --no-modtime 2>/dev/null | awk '{print $2}' > "$tmp_remote_list" || true

  local changed=0
  local skipped=0

  while IFS= read -r rel_path; do
    [[ -z "$rel_path" ]] && continue

    local local_file="$VAULT_DIR/$rel_path"
    local oss_key="${OSS_PREFIX}/$rel_path"

    if [[ ! -f "$local_file" ]]; then
      # 文件被删除 → 从 OSS 删除
      rclone deletefile "${REMOTE}:${BUCKET}/${oss_key}" 2>/dev/null && \
        log "  删除 OSS: $rel_path" || true
      continue
    fi

    # 计算本地 md5
    local local_md5=$(md5 -q "$local_file")

    # 获取 OSS md5（如果文件存在）
    local oss_md5=$(rclone hash MD5 "${REMOTE}:${BUCKET}/${oss_key}" 2>/dev/null | awk '{print $1}' || echo "")

    if [[ "$local_md5" != "$oss_md5" ]]; then
      # 上传（目录结构保持：Obsidian/ 开头的路径）
      rclone copyto "$local_file" "${REMOTE}:${BUCKET}/${oss_key}" --one-way 2>/dev/null
      log "  上传: $rel_path ($(wc -c < "$local_file") bytes)"
      ((changed++)) || true
    else
      ((skipped++)) || true
    fi

  done < <(cd "$VAULT_DIR" && find . -type f ! -name '.DS_Store' ! -name '*.icloud' -print | sed 's|^\./||')

  rm -f "$tmp_remote_list"
  log "  上传完成: $changed 个文件变更, $skipped 个跳过"
}

# ── OSS 下载 ───────────────────────────────────────────────
# 从 OSS 拉取新文件到本地（其他设备修改的内容）
# 本地已存在的文件以本地为准（local-first）
sync_from_oss() {
  log "← 从 OSS 拉取变更到本地..."

  local changed=0

  while IFS= read -r object_key; do
    # 去掉 OSS_PREFIX/
    local rel_path="${object_key#${OSS_PREFIX}/}"
    [[ -z "$rel_path" ]] && continue

    local local_file="$VAULT_DIR/$rel_path"

    # 本地不存在 → 下载
    if [[ ! -f "$local_file" ]]; then
      mkdir -p "$(dirname "$local_file")"
      rclone copyto "${REMOTE}:${BUCKET}/${object_key}" "$local_file" 2>/dev/null
      log "  新增: $rel_path"
      ((changed++)) || true
    fi

  done < <(rclone ls "${REMOTE}:${BUCKET}/${OSS_PREFIX}/" 2>/dev/null | awk '{print $2}')

  log "  拉取完成: $changed 个新文件"
}

# ── 监听模式（后台守护） ───────────────────────────────────
daemon() {
  log "🚀 守护进程模式: watchman 监听 $VAULT_DIR"
  log "   (watchmedo 需要 pip install watchdog)"

  # 检查 watchdog
  if ! python3 -c "import watchdog" 2>/dev/null; then
    warn "watchdog 未安装，改为定期同步模式（每 60 秒）"
    daemon_poll
    return
  fi

  log "使用 watchdog 实时监听..."
  python3 - <<'PYEOF'
import time, hashlib, logging, os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

VAULT_DIR = os.environ.get("VAULT_DIR", os.path.expanduser("~/Documents/Obsidian"))
REMOTE = os.environ.get("REMOTE", "clawshell-vault")
BUCKET = os.environ.get("BUCKET", "clawshell-vault")
OSS_PREFIX = os.environ.get("OSS_PREFIX", "vault")
STATE_DIR = os.path.expanduser("~/.clawshell-local/sync")
LOG_FILE = os.path.join(STATE_DIR, "vault-oss.log")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("vault-sync")

class VaultSyncHandler(FileSystemEventHandler):
    def __init__(self):
        self.pending = set()
        self.debounce_time = 2.0  # seconds
        self.last_modified = {}

    def _should_sync(self, path):
        if path.endswith(".DS_Store") or path.endswith(".icloud"):
            return False
        if "/.obsidian/plugins/" in path:
            return False  # skip plugin cache
        return True

    def _upload_file(self, rel_path):
        import subprocess
        local_path = os.path.join(VAULT_DIR, rel_path)
        oss_key = f"{OSS_PREFIX}/{rel_path}"
        try:
            result = subprocess.run(
                ["rclone", "copyto", local_path, f"{REMOTE}:{BUCKET}/{oss_key}"],
                capture_output=True, timeout=30
            )
            if result.returncode == 0:
                log.info(f"→ 上传: {rel_path}")
            else:
                log.warning(f"上传失败: {result.stderr.decode()}")
        except Exception as e:
            log.error(f"上传异常: {e}")

    def on_modified(self, event):
        if event.is_directory:
            return
        path = event.src_path
        rel_path = os.path.relpath(path, VAULT_DIR)
        if not self._should_sync(rel_path):
            return
        # debounce: ignore events within 2s for the same file
        now = time.time()
        last = self.last_modified.get(rel_path, 0)
        if now - last < self.debounce_time:
            return
        self.last_modified[rel_path] = now
        self._upload_file(rel_path)

    def on_created(self, event):
        if event.is_directory:
            return
        path = event.src_path
        rel_path = os.path.relpath(path, VAULT_DIR)
        if not self._should_sync(rel_path):
            return
        self._upload_file(rel_path)

    def on_deleted(self, event):
        if event.is_directory:
            return
        path = event.src_path
        rel_path = os.path.relpath(path, VAULT_DIR)
        if not self._should_sync(rel_path):
            return
        import subprocess
        oss_key = f"{OSS_PREFIX}/{rel_path}"
        try:
            subprocess.run(["rclone", "deletefile", f"{REMOTE}:{BUCKET}/{oss_key}"], capture_output=True, timeout=10)
            log.info(f"  删除 OSS: {rel_path}")
        except Exception as e:
            log.error(f"删除异常: {e}")

observer = Observer()
observer.schedule(VaultSyncHandler(), VAULT_DIR, recursive=True)
observer.start()
log.info(f"监听中: {VAULT_DIR} → {REMOTE}:{BUCKET}/{OSS_PREFIX}/")
try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    observer.stop()
observer.join()
PYEOF
}

daemon_poll() {
  log "定期同步模式: 每 60 秒执行一次"
  while true; do
    sync_to_oss
    sleep 60 &
    wait $!
  done
}

# ── 一次性同步 ─────────────────────────────────────────────
sync_once() {
  acquire_lock
  sync_to_oss
  sync_from_oss
  log "✓ 同步完成"
}

# ── 帮助 ───────────────────────────────────────────────────
usage() {
  cat <<EOF
ClawShell Vault OSS 同步脚本
用法: ./sync.sh <command>

命令:
  sync       一次性同步（上传 + 下载）
  daemon     守护进程模式（监听文件变更实时上传）
  poll       定期同步模式（每60秒）
  pull       仅从 OSS 拉取
  push       仅上传到 OSS
  init       初始化（创建 rclone remote）
  status     查看同步状态

环境变量:
  VAULT_DIR   本地 Vault 目录 (默认: ~/Documents/Obsidian)
  REMOTE      rclone remote 名 (默认: clawshell-vault)
  BUCKET      OSS Bucket 名 (默认: clawshell-vault)
  OSS_PREFIX  OSS 内前缀 (默认: vault)

示例:
  VAULT_DIR=~/my-vault ./sync.sh daemon
EOF
}

# ── 初始化 rclone remote ───────────────────────────────────
init_remote() {
  echo "创建 rclone remote '${REMOTE}' ..."
  rclone config create "${REMOTE}" s3 \
    provider Alibaba \
    access_key_id "${OSS_ACCESS_KEY_ID}" \
    secret_access_key "${OSS_ACCESS_KEY_SECRET}" \
    endpoint "${OSS_ENDPOINT:-oss-cn-hongkong.aliyuncs.com}" \
    acl private \
    region cn-hongkong
  echo "完成！可用 remotes:"
  rclone listremotes
}

# ── 状态检查 ───────────────────────────────────────────────
status_check() {
  echo "=== Vault OSS 同步状态 ==="
  echo "Vault:  $VAULT_DIR"
  echo "Remote: ${REMOTE}:${BUCKET}/${OSS_PREFIX}/"
  echo "Endpoint: $OSS_ENDPOINT"
  echo ""
  echo "本地文件数: $(find "$VAULT_DIR" -type f ! -name '.DS_Store' | wc -l | tr -d ' ')"
  echo "OSS 对象数: $(rclone ls "${REMOTE}:${BUCKET}/${OSS_PREFIX}/" 2>/dev/null | wc -l | tr -d ' ')"
  echo "同步锁: $([ -d "$LOCK_FILE" ] && echo '运行中' || echo '未运行')"
  echo "日志: $LOG_FILE"
}

# ── 主入口 ─────────────────────────────────────────────────
COMMAND="${1:-usage}"
case "$COMMAND" in
  sync)   init; sync_once ;;
  daemon) init; acquire_lock; daemon ;;
  poll)   init; acquire_lock; daemon_poll ;;
  pull)   init; acquire_lock; sync_from_oss ;;
  push)   init; acquire_lock; sync_to_oss ;;
  init)   init_remote ;;
  status) status_check ;;
  help|--help|-h) usage ;;
  *)      usage ;;
esac
