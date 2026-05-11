#!/usr/bin/env python3
"""
ClawShell Edge — Knowledge Puller Module (D3)
==========================================

端侧知识拉取模块：从云端 MemPalace 拉取知识到本地

核心能力：
- 从云端 MemPalace 拉取知识图谱
- 增量拉取（基于时间戳）
- 语义搜索集成
- 本地缓存管理
- 通过 EventBus 发布知识同步事件
- 与 SyncEngine 协同处理冲突

依赖：
- MemPalace MCP 协议支持
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from threading import Lock, Thread
from datetime import datetime, timedelta

logger = logging.getLogger("edge.knowledge_puller")


# ─── 路径配置 ────────────────────────────────────────────────────────────────

EDGE_STATE_DIR = Path.home() / ".clawshell-local"
KNOWLEDGE_CACHE_DIR = EDGE_STATE_DIR / "knowledge"
KNOWLEDGE_INDEX_FILE = KNOWLEDGE_CACHE_DIR / "index.json"
KNOWLEDGE_DATA_DIR = KNOWLEDGE_CACHE_DIR / "data"
KNOWLEDGE_SYNC_FILE = KNOWLEDGE_CACHE_DIR / "sync_state.json"


# ─── 数据结构 ────────────────────────────────────────────────────────────────

@dataclass
class KnowledgeEntry:
    """知识条目"""
    id: str
    wing: str
    room: str
    drawer_id: str
    content: str
    timestamp: str
    version: int = 1
    tags: List[str] = field(default_factory=list)
    sync_status: str = "synced"  # synced | pending | conflict

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "wing": self.wing,
            "room": self.room,
            "drawer_id": self.drawer_id,
            "content": self.content,
            "timestamp": self.timestamp,
            "version": self.version,
            "tags": self.tags,
            "sync_status": self.sync_status,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "KnowledgeEntry":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class SyncState:
    """同步状态"""
    last_sync: float = 0.0
    last_full_sync: float = 0.0
    synced_entities: Set[str] = field(default_factory=set)
    pending_uploads: List[Dict] = field(default_factory=list)
    conflict_count: int = 0

    def to_dict(self) -> Dict:
        return {
            "last_sync": self.last_sync,
            "last_full_sync": self.last_full_sync,
            "synced_entities": list(self.synced_entities),
            "pending_uploads": self.pending_uploads,
            "conflict_count": self.conflict_count,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SyncState":
        synced = set(data.get("synced_entities", []))
        return cls(
            last_sync=data.get("last_sync", 0.0),
            last_full_sync=data.get("last_full_sync", 0.0),
            synced_entities=synced,
            pending_uploads=data.get("pending_uploads", []),
            conflict_count=data.get("conflict_count", 0),
        )


# ─── 知识拉取引擎 ───────────────────────────────────────────────────────────

class KnowledgePuller:
    """
    端侧知识拉取引擎

    功能：
    - 从云端 MemPalace 拉取知识图谱
    - 增量拉取（基于时间戳）
    - 语义搜索集成
    - 本地缓存管理
    - 通过 EventBus 发布知识同步事件
    - 与 SyncEngine 协同处理冲突
    """

    SYNC_INTERVAL = 300            # 5 分钟同步一次
    FULL_SYNC_INTERVAL = 3600      # 1 小时全量同步一次
    MAX_PENDING = 100               # 最大待处理条目

    def __init__(
        self,
        eventbus=None,
        sync_engine=None,
        protocol=None,
    ):
        self.eventbus = eventbus
        self.sync_engine = sync_engine
        self.protocol = protocol  # EdgeProtocol for MCP calls

        self._knowledge_index: Dict[str, KnowledgeEntry] = {}
        self._sync_state: SyncState = self._load_sync_state()
        self._lock = Lock()
        self._running = False
        self._sync_thread: Optional[Thread] = None

        # 确保目录存在
        KNOWLEDGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        KNOWLEDGE_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # 加载本地索引
        self._load_index()

        logger.info("KnowledgePuller initialized (last_sync=%s)",
                    datetime.fromtimestamp(self._sync_state.last_sync).isoformat()
                    if self._sync_state.last_sync else "never")

    # ─── 持久化 ─────────────────────────────────────────────────────────────

    def _load_sync_state(self) -> SyncState:
        """加载同步状态"""
        if KNOWLEDGE_SYNC_FILE.exists():
            try:
                with open(KNOWLEDGE_SYNC_FILE) as f:
                    data = json.load(f)
                    return SyncState.from_dict(data)
            except Exception as e:
                logger.warning("Failed to load sync state: %s", e)
        return SyncState()

    def _save_sync_state(self):
        """保存同步状态"""
        with self._lock:
            KNOWLEDGE_SYNC_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(KNOWLEDGE_SYNC_FILE, 'w') as f:
                json.dump(self._sync_state.to_dict(), f, indent=2)

    def _load_index(self):
        """加载知识索引"""
        if KNOWLEDGE_INDEX_FILE.exists():
            try:
                with open(KNOWLEDGE_INDEX_FILE) as f:
                    data = json.load(f)
                    for entry_data in data.values():
                        entry = KnowledgeEntry.from_dict(entry_data)
                        self._knowledge_index[entry.id] = entry
                logger.info("Loaded %d knowledge entries from cache", len(self._knowledge_index))
            except Exception as e:
                logger.warning("Failed to load knowledge index: %s", e)

    def _save_index(self):
        """保存知识索引"""
        with self._lock:
            data = {k: v.to_dict() for k, v in self._knowledge_index.items()}
            KNOWLEDGE_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(KNOWLEDGE_INDEX_FILE, 'w') as f:
                json.dump(data, f, indent=2)

    # ─── MCP 协议调用 ──────────────────────────────────────────────────────

    async def _call_mcp(self, method: str, params: Dict) -> Optional[Dict]:
        """调用 MCP 协议"""
        if self.protocol is None:
            logger.warning("Protocol not available for MCP call")
            return None
        try:
            result = await self.protocol.call_mcp(method, params)
            return result
        except Exception as e:
            logger.error("MCP call %s failed: %s", method, e)
            return None

    async def _pull_by_query(self, query: str, limit: int = 50, since: Optional[str] = None) -> List[Dict]:
        """从云端拉取知识（通过搜索）"""
        params = {
            "query": query,
            "limit": limit,
        }
        if since:
            params["since"] = since

        result = await self._call_mcp("mempalace_search", params)
        if result and "results" in result:
            return result["results"]
        return []

    async def _pull_timeline(self, entity: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """从云端拉取时间线"""
        params = {"limit": limit}
        if entity:
            params["entity"] = entity

        result = await self._call_mcp("mempalace_kg_timeline", params)
        if result and "timeline" in result:
            return result["timeline"]
        return []

    async def _pull_graph_stats(self) -> Dict:
        """从云端拉取图谱统计"""
        result = await self._call_mcp("mempalace_kg_stats", {})
        if result:
            return result
        return {}

    # ─── 知识同步 ──────────────────────────────────────────────────────────

    async def _incremental_sync(self) -> int:
        """增量同步（仅拉取变更）"""
        if self.sync_engine is None:
            return 0

        since = None
        if self._sync_state.last_sync > 0:
            since = datetime.fromtimestamp(self._sync_state.last_sync).isoformat()

        logger.info("Starting incremental sync (since=%s)", since or "beginning")

        # 通过 MemPalace 搜索拉取更新的条目
        new_entries = 0

        # 拉取所有 wings
        wings_result = await self._call_mcp("mempalace_list_wings", {})
        if wings_result and "wings" in wings_result:
            for wing_info in wings_result["wings"]:
                wing = wing_info.get("wing", "default")

                # 拉取每个 wing 的 rooms
                rooms_result = await self._call_mcp("mempalace_list_rooms", {"wing": wing})
                if rooms_result and "rooms" in rooms_result:
                    for room_info in rooms_result["rooms"]:
                        room = room_info.get("room", "general")

                        # 拉取这个 room 的 drawers
                        drawers_result = await self._call_mcp("mempalace_list_drawers", {
                            "wing": wing,
                            "room": room,
                            "limit": 100,
                        })
                        if drawers_result and "drawers" in drawers_result:
                            for drawer_info in drawers_result["drawers"]:
                                drawer_id = drawer_info.get("id")
                                if drawer_id:
                                    # 检查是否需要更新
                                    if drawer_id not in self._sync_state.synced_entities:
                                        # 拉取完整内容
                                        drawer_detail = await self._call_mcp("mempalace_get_drawer", {
                                            "drawer_id": drawer_id
                                        })
                                        if drawer_detail:
                                            entry = self._create_entry_from_drawer(drawer_detail)
                                            self._update_local_entry(entry)
                                            new_entries += 1

        # 更新同步状态
        self._sync_state.last_sync = time.time()
        self._save_sync_state()
        self._save_index()

        return new_entries

    async def _full_sync(self) -> int:
        """全量同步"""
        logger.info("Starting full knowledge sync")

        # 获取图谱统计
        stats = await self._pull_graph_stats()
        logger.info("Cloud knowledge graph: %s", stats)

        # 清空同步状态
        self._sync_state.synced_entities.clear()
        self._sync_state.last_full_sync = time.time()

        # 全量拉取
        count = await self._incremental_sync()

        self._sync_state.last_sync = time.time()
        self._save_sync_state()

        return count

    def _create_entry_from_drawer(self, drawer_data: Dict) -> KnowledgeEntry:
        """从 drawer 数据创建知识条目"""
        return KnowledgeEntry(
            id=drawer_data.get("id", ""),
            wing=drawer_data.get("wing", "default"),
            room=drawer_data.get("room", "general"),
            drawer_id=drawer_data.get("drawer_id", drawer_data.get("id", "")),
            content=drawer_data.get("content", ""),
            timestamp=drawer_data.get("timestamp", datetime.utcnow().isoformat()),
            version=drawer_data.get("version", 1),
            tags=drawer_data.get("tags", []),
            sync_status="synced",
        )

    def _update_local_entry(self, entry: KnowledgeEntry):
        """更新本地知识条目"""
        with self._lock:
            existing = self._knowledge_index.get(entry.id)
            if existing:
                # 检查版本冲突
                if entry.version > existing.version:
                    entry.sync_status = "updated"
                elif entry.version < existing.version:
                    entry.sync_status = "conflict"
                    self._sync_state.conflict_count += 1
                else:
                    entry.sync_status = "synced"
            else:
                entry.sync_status = "new"

            self._knowledge_index[entry.id] = entry
            self._sync_state.synced_entities.add(entry.id)

        # 保存到文件
        self._save_entry_to_disk(entry)

    def _save_entry_to_disk(self, entry: KnowledgeEntry):
        """保存条目到磁盘"""
        entry_file = KNOWLEDGE_DATA_DIR / f"{entry.id}.json"
        with open(entry_file, 'w') as f:
            json.dump(entry.to_dict(), f, indent=2)

    # ─── 搜索接口 ──────────────────────────────────────────────────────────

    def search_local(self, query: str, limit: int = 10) -> List[KnowledgeEntry]:
        """本地语义搜索"""
        # 简化实现：基于关键词匹配
        results = []
        query_lower = query.lower()

        with self._lock:
            for entry in self._knowledge_index.values():
                if query_lower in entry.content.lower():
                    results.append(entry)
                elif any(query_lower in tag.lower() for tag in entry.tags):
                    results.append(entry)

        # 按时间排序
        results.sort(key=lambda e: e.timestamp, reverse=True)
        return results[:limit]

    def get_entries_by_wing(self, wing: str) -> List[KnowledgeEntry]:
        """获取指定 wing 的所有条目"""
        with self._lock:
            return [e for e in self._knowledge_index.values() if e.wing == wing]

    def get_entries_by_room(self, wing: str, room: str) -> List[KnowledgeEntry]:
        """获取指定 room 的所有条目"""
        with self._lock:
            return [e for e in self._knowledge_index.values()
                    if e.wing == wing and e.room == room]

    def get_entry(self, entry_id: str) -> Optional[KnowledgeEntry]:
        """获取指定条目"""
        return self._knowledge_index.get(entry_id)

    def get_all_entries(self) -> List[KnowledgeEntry]:
        """获取所有条目"""
        with self._lock:
            return list(self._knowledge_index.values())

    # ─── EventBus 集成 ─────────────────────────────────────────────────────

    def _publish_event(self, event_type: str, data: Dict):
        """通过 EventBus 发布事件"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import Event, EventType
            event = Event(
                type=EventType.CUSTOM,
                source="knowledge_puller",
                payload={**data, "_event_type": event_type}
            )
            self.eventbus.publish(event)
        except Exception as e:
            logger.warning("Failed to publish EventBus event: %s", e)

    def _subscribe_to_events(self):
        """订阅 EventBus 事件"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import EventType
            self.eventbus.subscribe(EventType.CUSTOM, self._on_custom_event)
            logger.info("Subscribed to EventBus for knowledge puller")
        except Exception as e:
            logger.warning("Failed to subscribe to EventBus: %s", e)

    def _on_custom_event(self, event):
        """处理 EventBus 自定义事件"""
        data = event.payload or {}
        et = data.get("_event_type", "")
        if et == "force_sync":
            logger.info("Force knowledge sync triggered via event")
            asyncio.create_task(self.trigger_sync())
        elif et == "search":
            query = data.get("query", "")
            limit = data.get("limit", 10)
            results = self.search_local(query, limit)
            self._publish_event("search.results", {
                "query": query,
                "results": [r.to_dict() for r in results],
            })
        elif et == "get_timeline":
            entity = data.get("entity")
            limit = data.get("limit", 100)
            asyncio.create_task(self._fetch_timeline(entity, limit))

    async def _fetch_timeline(self, entity: Optional[str], limit: int):
        """获取并缓存时间线"""
        timeline = await self._pull_timeline(entity, limit)
        self._publish_event("timeline.fetched", {
            "entity": entity,
            "timeline": timeline,
        })

    # ─── 同步触发 ──────────────────────────────────────────────────────────

    async def trigger_sync(self, full: bool = False) -> int:
        """触发同步"""
        try:
            if full:
                count = await self._full_sync()
                self._publish_event("sync.completed", {
                    "type": "full",
                    "count": count,
                })
            else:
                count = await self._incremental_sync()
                self._publish_event("sync.completed", {
                    "type": "incremental",
                    "count": count,
                })
            logger.info("Knowledge sync completed: %d entries", count)
            return count
        except Exception as e:
            logger.error("Knowledge sync failed: %s", e)
            self._publish_event("sync.failed", {"error": str(e)})
            return 0

    # ─── 同步循环 ──────────────────────────────────────────────────────────

    def _sync_loop(self):
        """后台同步循环"""
        while self._running:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                now = time.time()
                needs_full_sync = (
                    self._sync_state.last_full_sync == 0 or
                    (now - self._sync_state.last_full_sync) > self.FULL_SYNC_INTERVAL
                )

                if needs_full_sync:
                    loop.run_until_complete(self.trigger_sync(full=True))
                else:
                    loop.run_until_complete(self.trigger_sync(full=False))

                loop.close()

                time.sleep(self.SYNC_INTERVAL)
            except Exception as e:
                logger.error("Sync loop error: %s", e)
                time.sleep(60)

    # ─── 生命周期 ───────────────────────────────────────────────────────────

    def start(self):
        """启动知识拉取"""
        if self._running:
            return
        self._running = True
        self._subscribe_to_events()
        self._sync_thread = Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()
        logger.info("KnowledgePuller started (interval=%ds, full_sync=%ds)",
                    self.SYNC_INTERVAL, self.FULL_SYNC_INTERVAL)

    def stop(self):
        """停止知识拉取"""
        self._running = False
        if self._sync_thread:
            self._sync_thread.join(timeout=10)
        self._save_sync_state()
        self._save_index()
        logger.info("KnowledgePuller stopped")

    # ─── 外部接口 ───────────────────────────────────────────────────────────

    def get_status(self) -> Dict:
        """获取同步状态"""
        with self._lock:
            return {
                "total_entries": len(self._knowledge_index),
                "synced_entities": len(self._sync_state.synced_entities),
                "last_sync": self._sync_state.last_sync,
                "last_full_sync": self._sync_state.last_full_sync,
                "pending_uploads": len(self._sync_state.pending_uploads),
                "conflict_count": self._sync_state.conflict_count,
            }

    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        total_size = 0
        file_count = 0
        for f in KNOWLEDGE_DATA_DIR.glob("*.json"):
            total_size += f.stat().st_size
            file_count += 1

        return {
            "cache_dir": str(KNOWLEDGE_DATA_DIR),
            "entry_count": len(self._knowledge_index),
            "file_count": file_count,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
        }
