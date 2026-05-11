#!/usr/bin/env python3
"""
ClawShell Edge — Self-Healing Module
=====================================

端侧自愈模块（适配自 Windows lib/layer2/self_healing.py）

与 Cloud 版差异：
- 无 cron 守护，依赖 SyncEngine 心跳驱动
- 通过 Local EventBus 发布/订阅事件
- 轻量化（不备份整个系统，只备份关键状态）

核心能力：
- 心跳监控（通过 EventBus 接收 heartbeat 事件）
- 故障检测（超时未收到心跳 → 触发自愈）
- 自动检查点（定期创建 ~/.clawshell-local/checkpoints/）
- 配置备份与恢复
"""

import asyncio
import json
import logging
import time
import shutil
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from threading import Lock, Thread

logger = logging.getLogger("edge.self_healing")


# ─── 路径配置 ────────────────────────────────────────────────────────────────

EDGE_STATE_DIR = Path.home() / ".clawshell-local"
HEALING_STATE_FILE = EDGE_STATE_DIR / ".healing_state.json"
CHECKPOINT_DIR = EDGE_STATE_DIR / "checkpoints"
BACKUP_DIR = EDGE_STATE_DIR / "backups"

# 关键配置文件（仅备份这些）
CRITICAL_PATHS = [
    ".clawshell-local/config/cloud.json",
    ".clawshell-local/config/platform.json",
]


# ─── 数据结构 ────────────────────────────────────────────────────────────────

@dataclass
class HealingAction:
    action: str       # "checkpoint" | "restore" | "backup" | "restart"
    target: str
    status: str = "pending"   # pending | running | completed | failed
    result: Optional[str] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class HealingState:
    last_heartbeat: float = 0.0
    last_checkpoint: float = 0.0
    consecutive_failures: int = 0
    is_healthy: bool = True
    actions: List[Dict] = field(default_factory=list)


# ─── 工具函数 ───────────────────────────────────────────────────────────────

def _checksum(path: Path) -> str:
    """计算文件校验和（MD5）"""
    if path.is_file():
        with open(path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    elif path.is_dir():
        h = hashlib.md5()
        for item in sorted(path.rglob("*")):
            if item.is_file():
                h.update(item.name.encode())
                h.update(open(item, 'rb').read())
        return h.hexdigest()
    return ""


def _dir_size(path: Path) -> int:
    """计算目录大小"""
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


# ─── Edge SelfHealing ────────────────────────────────────────────────────────

class EdgeSelfHealing:
    """
    端侧自愈引擎
    
    与 Local EventBus 集成：
    - 订阅 heartbeat 事件（判断是否存活）
    - 发布 healing.checkpoint_created / healing.failure_detected 等事件
    
    与 SyncEngine 集成：
    - 通过心跳计时器检测故障（无 cron）
    - 触发检查点创建、配置备份等自愈动作
    """

    HEARTBEAT_TIMEOUT = 120      # 秒（未收到心跳 → 降级）
    CHECKPOINT_INTERVAL = 3600   # 秒（每小时最多一次检查点）
    MAX_CONSECUTIVE_FAILURES = 3

    def __init__(self, eventbus=None, on_healing_action: Callable = None):
        self.eventbus = eventbus
        self.on_healing_action = on_healing_action  # 回调（如重启服务）
        self.state = self._load_state()
        self._lock = Lock()
        self._running = False
        self._heartbeat_timer: Optional[Thread] = None

        # 确保目录存在
        EDGE_STATE_DIR.mkdir(parents=True, exist_ok=True)
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("EdgeSelfHealing initialized (timeout=%ds)", self.HEARTBEAT_TIMEOUT)

    # ─── 状态管理 ─────────────────────────────────────────────────────────

    def _load_state(self) -> HealingState:
        if HEALING_STATE_FILE.exists():
            try:
                with open(HEALING_STATE_FILE) as f:
                    data = json.load(f)
                    return HealingState(**{k: v for k, v in data.items() if k in HealingState.__dataclass_fields__})
            except Exception:
                pass
        return HealingState()

    def _save_state(self):
        with self._lock:
            data = {
                "last_heartbeat": self.state.last_heartbeat,
                "last_checkpoint": self.state.last_checkpoint,
                "consecutive_failures": self.state.consecutive_failures,
                "is_healthy": self.state.is_healthy,
                "actions": self.state.actions[-50:],  # 保留最近50条
            }
            with open(HEALING_STATE_FILE, 'w') as f:
                json.dump(data, f, indent=2)

    # ─── 心跳处理 ─────────────────────────────────────────────────────────

    def record_heartbeat(self):
        """记录心跳（由外部调用，如 SyncEngine 心跳）"""
        with self._lock:
            self.state.last_heartbeat = time.time()
            if not self.state.is_healthy:
                self.state.is_healthy = True
                self.state.consecutive_failures = 0
                self._log_action("recover", "cloud", "heartbeat restored")
                self._publish_event("healing.recovered", {"timestamp": self.state.last_heartbeat})
                logger.info("Edge recovered, heartbeat restored")
        self._save_state()

    def _check_heartbeat(self):
        """检查心跳是否超时（后台线程调用）"""
        now = time.time()
        with self._lock:
            last = self.state.last_heartbeat
            elapsed = now - last if last > 0 else 0

        if elapsed > self.HEARTBEAT_TIMEOUT and self.state.is_healthy:
            self.state.consecutive_failures += 1
            self.state.is_healthy = False
            self._log_action("failure", "heartbeat_timeout",
                            f"no heartbeat for {elapsed:.0f}s (failure #{self.state.consecutive_failures})")
            self._publish_event("healing.failure_detected", {
                "elapsed": elapsed,
                "consecutive_failures": self.state.consecutive_failures,
            })
            logger.warning("Heartbeat timeout: %.0fs (failure #%d)", elapsed, self.state.consecutive_failures)
            self._save_state()

            # 触发自愈
            if self.state.consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                self._trigger_self_healing()

    def _trigger_self_healing(self):
        """触发自愈流程"""
        logger.warning("Triggering self-healing after %d consecutive failures", self.state.consecutive_failures)
        self._publish_event("healing.started", {"failures": self.state.consecutive_failures})

        # 1. 创建紧急检查点
        checkpoint_id = self._create_checkpoint("emergency", "auto-healing checkpoint")

        # 2. 尝试备份关键配置
        backup_id = self._backup_critical_configs()

        # 3. 回调通知外部（如重启连接）
        if self.on_healing_action:
            try:
                self.on_healing_action({
                    "type": "self_healing",
                    "checkpoint_id": checkpoint_id,
                    "backup_id": backup_id,
                    "failures": self.state.consecutive_failures,
                })
            except Exception as e:
                logger.error("Self-healing callback failed: %s", e)

        self._log_action("heal", "system",
                        f"healing triggered (checkpoint={checkpoint_id}, backup={backup_id})")
        self._publish_event("healing.completed", {
            "checkpoint_id": checkpoint_id,
            "backup_id": backup_id,
        })

    # ─── 检查点 ───────────────────────────────────────────────────────────

    def _create_checkpoint(self, name: str, description: str) -> Optional[str]:
        """创建检查点（备份关键配置）"""
        checkpoint_id = f"cp_{int(time.time())}"
        checkpoint_path = CHECKPOINT_DIR / checkpoint_id
        checkpoint_path.mkdir(exist_ok=True)

        components = []
        for rel_path in CRITICAL_PATHS:
            src = Path.home() / rel_path
            if src.exists():
                dst = checkpoint_path / src.name
                try:
                    if src.is_dir():
                        shutil.copytree(src, dst, dirs_exist_ok=True)
                    else:
                        shutil.copy2(src, dst)
                    components.append(str(src))
                    logger.debug("Checkpoint backed up: %s", src)
                except Exception as e:
                    logger.warning("Failed to checkpoint %s: %s", src, e)

        metadata = {
            "name": name,
            "description": description,
            "components": components,
            "size": _dir_size(checkpoint_path),
            "checksum": _checksum(checkpoint_path),
        }

        # 保存元数据
        with open(checkpoint_path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

        with self._lock:
            self.state.last_checkpoint = time.time()

        self._log_action("checkpoint", checkpoint_id, f"created: {len(components)} components")
        self._publish_event("healing.checkpoint_created", {
            "checkpoint_id": checkpoint_id,
            "components": len(components),
        })

        self._save_state()
        return checkpoint_id

    def _backup_critical_configs(self) -> Optional[str]:
        """备份关键配置（临时快照）"""
        backup_id = f"backup_{int(time.time())}"
        backup_path = BACKUP_DIR / backup_id
        backup_path.mkdir(exist_ok=True)

        count = 0
        for rel_path in CRITICAL_PATHS:
            src = Path.home() / rel_path
            if src.exists():
                dst = backup_path / src.name
                try:
                    if src.is_dir():
                        shutil.copytree(src, dst, dirs_exist_ok=True)
                    else:
                        shutil.copy2(src, dst)
                    count += 1
                except Exception:
                    pass

        if count > 0:
            self._log_action("backup", backup_id, f"backed up {count} items")
            return backup_id
        return None

    # ─── EventBus 集成 ─────────────────────────────────────────────────────

    def _publish_event(self, event_type: str, data: Dict):
        """通过 EventBus 发布事件"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import Event, EventType
            event = Event(
                type=EventType.CUSTOM,
                source="edge_self_healing",
                data={**data, "_event_type": event_type}
            )
            self.eventbus.publish(event)
        except Exception as e:
            logger.warning("Failed to publish EventBus event: %s", e)

    def subscribe_to_events(self):
        """订阅 EventBus 事件（heartbeat 等）"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import EventType
            # 订阅心跳事件
            self.eventbus.subscribe(EventType.CUSTOM, self._on_custom_event)
            logger.info("Subscribed to EventBus for self-healing events")
        except Exception as e:
            logger.warning("Failed to subscribe to EventBus: %s", e)

    def _on_custom_event(self, event):
        """处理 EventBus 自定义事件"""
        data = event.data or {}
        et = data.get("_event_type", "")
        if et == "heartbeat":
            self.record_heartbeat()
        elif et == "force_heal":
            logger.info("Force healing triggered via event")
            self._trigger_self_healing()

    # ─── 动作日志 ─────────────────────────────────────────────────────────

    def _log_action(self, action: str, target: str, result: str):
        """记录自愈动作"""
        ha = HealingAction(action=action, target=target, status="completed", result=result)
        self.state.actions.append(ha.to_dict())
        if len(self.state.actions) > 100:
            self.state.actions = self.state.actions[-100:]
        self._save_state()

    # ─── 生命周期 ─────────────────────────────────────────────────────────

    def start(self):
        """启动自愈引擎（后台心跳检测线程）"""
        if self._running:
            return
        self._running = True
        self._heartbeat_timer = Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_timer.start()
        logger.info("EdgeSelfHealing started")

    def stop(self):
        """停止自愈引擎"""
        self._running = False
        if self._heartbeat_timer:
            self._heartbeat_timer.join(timeout=5)
        logger.info("EdgeSelfHealing stopped")

    def _heartbeat_loop(self):
        """心跳检测循环（替代 cron）"""
        while self._running:
            try:
                self._check_heartbeat()
                # 定期检查点（每小时一次）
                now = time.time()
                if self.state.last_checkpoint > 0:
                    elapsed = now - self.state.last_checkpoint
                    if elapsed > self.CHECKPOINT_INTERVAL:
                        self._create_checkpoint("scheduled", "periodic checkpoint")
                elif self.state.last_checkpoint == 0:
                    # 首次启动，创建初始检查点
                    self._create_checkpoint("init", "first checkpoint")
                time.sleep(30)  # 每30秒检测一次
            except Exception as e:
                logger.error("Heartbeat loop error: %s", e)
                time.sleep(30)

    # ─── 外部接口 ─────────────────────────────────────────────────────────

    def force_checkpoint(self, name: str = "manual") -> Optional[str]:
        """手动触发检查点"""
        return self._create_checkpoint(name, "manual trigger")

    def get_status(self) -> Dict:
        """获取自愈系统状态"""
        with self._lock:
            return {
                "is_healthy": self.state.is_healthy,
                "last_heartbeat": self.state.last_heartbeat,
                "last_checkpoint": self.state.last_checkpoint,
                "consecutive_failures": self.state.consecutive_failures,
                "recent_actions": self.state.actions[-5:],
            }
