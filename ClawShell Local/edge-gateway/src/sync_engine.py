"""
ClawShell Edge Gateway — Sync Engine v1.3
增量同步 + 事件驱动协同

新增 v1.3 能力：
- pending_operations 队列（离线操作）
- Event Replay Queue（离线期间 cloud-hub 推送的事件缓存）
- 冲突仲裁（云端权威 + 版本号）
- reconnect 后 replay 补齐离线窗口
"""
import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

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

    # ─── Event Replay Queue（v1.3 新增）─────────────────────────────────────
    # 离线期间 cloud-hub 推送的事件暂存这里，reconnect 后 replay 补齐

    def get_replay_queue_path(self) -> Path:
        return self.sync_dir / "event_replay_queue.jsonl"

    def queue_event(self, event: Dict[str, Any]) -> None:
        """将事件加入 replay 队列（离线时调用）"""
        with open(self.get_replay_queue_path(), "a") as f:
            f.write(json.dumps(event) + "\n")
        logger.debug(f"Event queued for replay: {event.get('topic')}")

    def get_queued_events(self) -> List[Dict[str, Any]]:
        """获取所有待 replay 的事件"""
        path = self.get_replay_queue_path()
        if not path.exists():
            return []
        events = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return events

    def clear_replay_queue(self) -> None:
        """清空 replay 队列（replay 成功后调用）"""
        path = self.get_replay_queue_path()
        if path.exists():
            path.unlink()
        logger.debug("Replay queue cleared")

    def get_last_seq(self) -> int:
        """获取本端已处理的最大序列号"""
        seq_file = self.sync_dir / "last_seq.txt"
        if seq_file.exists():
            try:
                return int(seq_file.read_text().strip())
            except ValueError:
                return 0
        return 0

    def set_last_seq(self, seq: int) -> None:
        """更新已处理的序列号"""
        seq_file = self.sync_dir / "last_seq.txt"
        seq_file.write_text(str(seq))

    # ─── 冲突仲裁（v1.3 扩展）───────────────────────────────────────────────

    def resolve_conflict(self, category: str, entity_id: str,
                        local_ver: int, cloud_ver: int,
                        local_data: Any, cloud_data: Any) -> Dict[str, Any]:
        """
        冲突解决策略：
        - kanban: 云端权威（last-write-wins with version check）
        - skill: 版本号比对，提示用户手动选择
        - memory: 云端权威（Memos 结构化数据）
        - workflow: 云端权威

        返回: {"resolution": "cloud_wins" | "local_wins" | "manual",
               "winning_data": ...}
        """
        if category == "kanban":
            # 云端权威
            self._write_conflict(category, entity_id, local_ver, cloud_ver, "cloud_wins")
            return {"resolution": "cloud_wins", "winning_data": cloud_data}
        elif category == "skill":
            if cloud_ver >= local_ver:
                self._write_conflict(category, entity_id, local_ver, cloud_ver, "cloud_wins")
                return {"resolution": "cloud_wins", "winning_data": cloud_data}
            else:
                self._write_conflict(category, entity_id, local_ver, cloud_ver, "local_wins")
                return {"resolution": "local_wins", "winning_data": local_data}
        elif category == "memory":
            self._write_conflict(category, entity_id, local_ver, cloud_ver, "cloud_wins")
            return {"resolution": "cloud_wins", "winning_data": cloud_data}
        else:
            self._write_conflict(category, entity_id, local_ver, cloud_ver, "cloud_wins")
            return {"resolution": "cloud_wins", "winning_data": cloud_data}

    # ─── 离线操作（原有能力保留）─────────────────────────────────────────────

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

    def _write_conflict(self, category: str, entity_id: str,
                         local_ver: int, cloud_ver: int, resolution: str):
        """写入冲突日志"""
        import datetime
        entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "category": category,
            "entity_id": entity_id,
            "local_version": local_ver,
            "cloud_version": cloud_ver,
            "resolution": resolution
        }
        with open(self.conflict_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
        logger.warning(f"冲突记录: {category}/{entity_id} local={local_ver} cloud={cloud_ver} → {resolution}")

    async def _apply_memory_change(self, action: str, data: dict):
        logger.debug(f"记忆变更: {action}")
        # 离线队列：写入 pending
        await self._offline_safe(self.protocol, "memory", action, data)

    async def _apply_kanban_change(self, action: str, data: dict):
        logger.debug(f"看板变更: {action}")
        # 云端权威：版本冲突时以云端为准
        entity_id = data.get("task_id") or data.get("board_id", "unknown")
        local_ver = data.get("version", 0)
        cloud_ver = data.get("cloud_version", 0)
        if cloud_ver > local_ver:
            self._write_conflict("kanban", entity_id, local_ver, cloud_ver, "cloud_wins")
        await self._offline_safe(self.protocol, "kanban", action, data)

    async def _apply_skill_change(self, action: str, data: dict):
        logger.debug(f"技能变更: {action}")
        await self._offline_safe(self.protocol, "skill", action, data)

    async def _offline_safe(self, protocol, category: str, action: str, data: dict):
        """离线安全：网络不可用时写入 pending queue"""
        try:
            await protocol.call_mcp(f"{category}.{action}", data)
        except Exception:
            # 网络不可用，加入本地队列
            self.queue_operation(category, action, data)

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
