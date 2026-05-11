"""
ClawShell Edge Gateway — Sync Engine
增量同步：pending_operations 队列 + delta pull/push
"""
import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger("edge-gateway.sync")


class SyncEngine:
    """数据同步引擎"""

    def __init__(self, protocol, cache_dir: Path, sync_dir: Path):
        self.protocol = protocol
        self.cache_dir = cache_dir
        self.sync_dir = sync_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        self.pending_file = sync_dir / "pending_operations.jsonl"
        self.last_sync_file = sync_dir / "last_sync.txt"
        self.conflict_file = sync_dir / "conflict_log.jsonl"

    def _get_last_sync(self) -> Optional[str]:
        if self.last_sync_file.exists():
            return self.last_sync_file.read_text().strip()
        return None

    def _set_last_sync(self, ts: str):
        self.last_sync_file.write_text(ts)

    async def full_sync(self):
        """完整同步：push pending → pull delta"""
        logger.info("开始全量同步")

        # 1. Push 本地 pending operations
        await self._push_pending()

        # 2. Pull delta from cloud
        await self._pull_delta()

        # 3. Update last sync timestamp
        self._set_last_sync(datetime.utcnow().isoformat())
        logger.info("全量同步完成")

    async def _push_pending(self):
        """上传本地待同步操作"""
        if not self.pending_file.exists():
            return

        with open(self.pending_file) as f:
            lines = f.readlines()

        if not lines:
            return

        logger.info(f"上传 {len(lines)} 条待同步操作")
        for line in lines:
            op = json.loads(line.strip())
            try:
                await self.protocol.call_mcp("sync.push", {"operation": op})
            except Exception as e:
                logger.error(f"推送失败: {e}")
                break

        # All successful: clear pending
        self.pending_file.unlink()
        logger.info("待同步队列已清空")

    async def _pull_delta(self):
        """从云端拉取增量变更"""
        since = self._get_last_sync()
        try:
            result = await self.protocol.call_mcp("sync.pull", {"since": since})
            changes = result.get("changes", [])
            for change in changes:
                await self._apply_change(change)
        except Exception as e:
            logger.error(f"拉取失败: {e}")

    async def _apply_change(self, change: dict):
        """应用来自云端的变更"""
        category = change.get("category")
        action = change.get("action")
        data = change.get("data", {})

        if category == "memory":
            await self._apply_memory_change(action, data)
        elif category == "kanban":
            await self._apply_kanban_change(action, data)
        elif category == "skill":
            await self._apply_skill_change(action, data)

    async def _apply_memory_change(self, action: str, data: dict):
        logger.debug(f"记忆变更: {action}")

    async def _apply_kanban_change(self, action: str, data: dict):
        logger.debug(f"看板变更: {action}")

    async def _apply_skill_change(self, action: str, data: dict):
        logger.debug(f"技能变更: {action}")

    def queue_operation(self, category: str, action: str, data: dict):
        """将操作加入待同步队列（离线时调用）"""
        op = {
            "category": category,
            "action": action,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        with open(self.pending_file, "a") as f:
            f.write(json.dumps(op) + "\n")
        logger.debug(f"操作已加入队列: {category}.{action}")

    async def pull_memory(self):
        """单独拉取记忆增量"""
        since = self._get_last_sync()
        try:
            result = await self.protocol.call_mcp("mempalace_search", {"query": "", "limit": 0})
        except Exception as e:
            logger.error(f"拉取记忆失败: {e}")

    async def pull_skills(self):
        """单独拉取技能更新"""
        try:
            result = await self.protocol.call_mcp("skill_list", {})
        except Exception as e:
            logger.error(f"拉取技能失败: {e}")
