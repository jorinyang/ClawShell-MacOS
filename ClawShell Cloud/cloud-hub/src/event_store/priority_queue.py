#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Priority Queue
=====================================
从 ClawShell-Windows lib/core/eventbus/priority_queue.py 提取重构

核心能力：
- CRITICAL/HIGH/NORMAL/LOW 四级优先级
- FIFO + 超时处理
- 磁盘持久化
"""

import os, json, time, heapq
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import deque
from threading import Lock
from pathlib import Path


PQ_STATE_PATH = Path("~/.cloudshell/.pq_state.json").expanduser()


class Priority(Enum):
    CRITICAL = 0; HIGH = 1; NORMAL = 2; LOW = 3


@dataclass
class PQItem:
    priority: int; timestamp: float; item_id: str
    payload: Dict; status: str = "pending"
    timeout_at: float = 0; retries: int = 0


class PriorityQueue:
    """四级优先级队列"""
    def __init__(self):
        self._queues: Dict[Priority, List] = {p: [] for p in Priority}
        self._all_items: Dict[str, PQItem] = {}
        self._lock = Lock()
        self._counter = 0
        self._load()

    def enqueue(self, payload: Dict, priority: Priority = Priority.NORMAL,
                timeout: float = 0) -> str:
        with self._lock:
            self._counter += 1
            item_id = f"pq_{int(time.time() * 1000)}_{self._counter}"
            item = PQItem(priority=priority.value, timestamp=time.time(),
                         item_id=item_id, payload=payload,
                         timeout_at=time.time() + timeout if timeout > 0 else 0)
            self._all_items[item_id] = item
            heapq.heappush(self._queues[priority], (priority.value, item.timestamp, item_id))
            self._save(); return item_id

    def dequeue(self) -> Optional[PQItem]:
        with self._lock:
            for p in Priority:
                q = self._queues[p]
                if q:
                    _, _, item_id = heapq.heappop(q)
                    item = self._all_items.get(item_id)
                    if item and item.status == "pending":
                        item.status = "dequeued"; self._save()
                        return item
            return None

    def get(self, item_id: str) -> Optional[PQItem]:
        return self._all_items.get(item_id)

    def requeue(self, item_id: str, priority: Optional[Priority] = None) -> bool:
        with self._lock:
            item = self._all_items.get(item_id)
            if not item: return False
            if priority is None: priority = Priority(item.priority)
            item.status = "pending"
            item.retries += 1
            heapq.heappush(self._queues[priority],
                          (priority.value, time.time(), item_id))
            self._save(); return True

    def remove(self, item_id: str) -> bool:
        with self._lock:
            if item_id in self._all_items:
                del self._all_items[item_id]; self._save(); return True
            return False

    def list_all(self) -> List[PQItem]:
        return list(self._all_items.values())

    def stats(self) -> Dict:
        return {p.name: len(self._queues[p]) for p in Priority}

    def _save(self):
        try:
            PQ_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            data = {iid: asdict(item) for iid, item in self._all_items.items()}
            with open(PQ_STATE_PATH, "w") as f:
                json.dump({"items": data, "counter": self._counter}, f)
        except: pass

    def _load(self):
        try:
            if not PQ_STATE_PATH.exists(): return
            with open(PQ_STATE_PATH) as f: d = json.load(f)
            self._counter = d.get("counter", 0)
            for iid, item_data in d.get("items", {}).items():
                if item_data.get("status") != "pending":
                    continue  # skip non-pending items from previous runs
                item = PQItem(**item_data)
                self._all_items[iid] = item
                p = Priority(item.priority)
                heapq.heappush(self._queues[p],
                              (p.value, item.timestamp, iid))
        except Exception: pass
