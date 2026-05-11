"""
ClawShell Cloud Hub — OSS Event Store
事件持久化存储，支持 append（追加事件）和 replay_by_seq（按序列号重放）
"""
import json
import logging
import time
from pathlib import PurePosixPath
from typing import AsyncIterator, List, Optional

from ..storage import OssStore
from .schema import Event

logger = logging.getLogger("cloud-hub.event-store")

# OSS 路径前缀
EVENTS_PREFIX = "events/"


class SequenceGenerator:
    """
    全局单调递增序列号生成器。
    序列号存储在 OSS 一个固定 key 上，每次 append 后 +1。
    极端情况下多个进程同时 append 会产生重复序列号（可接受，业务侧幂等处理）。
    """
    SEQ_KEY = f"{EVENTS_PREFIX}_meta/seq.json"

    def __init__(self, store: OssStore):
        self.store = store

    async def get_next(self) -> int:
        """获取下一个序列号（原子 +1）"""
        raw = await self.store.load(self.SEQ_KEY)
        if raw:
            current = json.loads(raw).get("seq", 0)
        else:
            current = 0
        next_seq = current + 1
        await self.store.save(self.SEQ_KEY, json.dumps({"seq": next_seq}))
        return next_seq

    async def get_current(self) -> int:
        """获取当前最大序列号"""
        raw = await self.store.load(self.SEQ_KEY)
        if raw:
            return json.loads(raw).get("seq", 0)
        return 0


class OssEventStore:
    """
    OSS 事件存储。
    路径结构: events/{YYYY-MM-DD}/{HH}/{event_id}.json
    """

    def __init__(self, store: OssStore, seq_gen: SequenceGenerator):
        self.store = store
        self.seq_gen = seq_gen

    def _event_path(self, event: Event) -> str:
        """按序列号扁平存储: events/seq/000123.json (replay 时可按 seq 前缀高效筛选)"""
        return f"{EVENTS_PREFIX}seq/{event.seq:010d}.json"

    async def append(self, event: Event) -> Event:
        """
        追加事件到 OSS（assign 序列号 + 写文件）。
        线程安全：由 SequenceGenerator 保障。
        """
        event.seq = await self.seq_gen.get_next()
        path = self._event_path(event)
        await self.store.save(path, json.dumps(event.to_dict()))
        # 更新全局序列号索引（用于 replay 定位）
        await self._update_seq_index(event.seq, path)
        logger.debug(f"Event stored: seq={event.seq} topic={event.topic} path={path}")
        return event

    async def _update_seq_index(self, seq: int, path: str) -> None:
        """维护序列号 → 文件路径 的索引"""
        idx_key = f"{EVENTS_PREFIX}_seq_index.json"
        raw = await self.store.load(idx_key)
        idx = json.loads(raw) if raw else {}
        idx[str(seq)] = path
        await self.store.save(idx_key, json.dumps(idx))

    async def replay_by_seq(self, since_seq: int, limit: int = 1000) -> List[Event]:
        """
        从指定序列号开始 replay 事件。
        通过 seq_index 快速定位，跳过扫描目录。
        """
        idx_key = f"{EVENTS_PREFIX}_seq_index.json"
        raw = await self.store.load(idx_key)
        if not raw:
            return []
        idx = json.loads(raw)  # { "123": "events/seq/0000123.json", ... }

        events: List[Event] = []
        for seq_str in sorted(idx.keys(), key=int):
            seq = int(seq_str)
            if seq <= since_seq:
                continue
            if len(events) >= limit:
                break
            path = idx[seq_str]
            ev_raw = await self.store.load(path)
            if not ev_raw:
                continue
            try:
                events.append(Event.from_dict(json.loads(ev_raw)))
            except (json.JSONDecodeError, KeyError):
                continue

        return events

    async def get_event(self, event_id: str) -> Optional[Event]:
        """通过 event_id 精确获取单个事件（走 seq_index）"""
        idx_key = f"{EVENTS_PREFIX}_seq_index.json"
        raw = await self.store.load(idx_key)
        if not raw:
            return None
        idx = json.loads(raw)
        for path in idx.values():
            ev_raw = await self.store.load(path)
            if ev_raw:
                ev = Event.from_dict(json.loads(ev_raw))
                if ev.event_id == event_id:
                    return ev
        return None

    async def get_latest_seq(self) -> int:
        """获取当前最新序列号"""
        return await self.seq_gen.get_current()
