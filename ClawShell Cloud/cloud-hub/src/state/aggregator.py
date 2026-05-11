"""
ClawShell Cloud Hub — State Aggregator
从 Event Store 聚合节点当前状态和任务当前状态。
提供节点透视查询（任意时刻谁在哪、干什么、进度多少）。
"""
import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from ..event_store.schema import Event, Topic

logger = logging.getLogger("cloud-hub.state")


class StateAggregator:
    """
    状态聚合器。
    维护内存中的当前状态快照，所有事件都经过这里更新。

    状态类型：
    - node_states:  node_id → NodeState
    - task_states:  task_id → TaskState
    - skill_states: skill_id → SkillState
    """

    def __init__(self):
        # node_id → { status, current_task, progress, active_tasks, last_seen, last_event_seq }
        self.node_states: Dict[str, Dict] = {}
        # task_id → { title, status, node_id, progress, last_event_seq }
        self.task_states: Dict[str, Dict] = {}
        # skill_id → { name, version, node_id, registered_at }
        self.skill_states: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    # ─── apply_event: 处理每个事件，更新内存状态 ────────────────────────────────

    async def apply_event(self, event: Event) -> None:
        """将单个事件应用到状态机"""
        async with self._lock:
            handler = self._dispatch(event.topic)
            if handler:
                await handler(event)
            else:
                logger.debug(f"No state handler for topic: {event.topic}")

    async def apply_events(self, events: List[Event]) -> None:
        """批量应用事件（用于 replay）"""
        for ev in events:
            await self.apply_event(ev)

    # ─── Node State ────────────────────────────────────────────────────────────

    async def _on_node_state(self, ev: Event) -> None:
        p = ev.payload
        node_id = p.get("node_id", ev.source)
        self.node_states[node_id] = {
            "node_id": node_id,
            "status": p.get("status", "online"),
            "current_task": p.get("current_task", ""),
            "progress": p.get("progress", 0),
            "active_tasks": p.get("active_tasks", []),
            "last_event_seq": ev.seq,
            "last_updated": ev.timestamp,
        }

    async def _on_node_heartbeat(self, ev: Event) -> None:
        node_id = ev.source
        if node_id in self.node_states:
            self.node_states[node_id]["status"] = "online"
            self.node_states[node_id]["last_event_seq"] = ev.seq
            self.node_states[node_id]["last_updated"] = ev.timestamp
        else:
            # 没见过这个节点，先初始化
            self.node_states[node_id] = {
                "node_id": node_id,
                "status": "online",
                "current_task": "",
                "progress": 0,
                "active_tasks": [],
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            }

    async def _on_node_offline(self, ev: Event) -> None:
        node_id = ev.source
        if node_id in self.node_states:
            self.node_states[node_id]["status"] = "offline"
            self.node_states[node_id]["last_event_seq"] = ev.seq

    async def _on_node_registered(self, ev: Event) -> None:
        p = ev.payload
        node_id = p.get("node_id", ev.source)
        if node_id in self.node_states:
            # 更新已有节点
            self.node_states[node_id].update({
                "status": "online",
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            })
        else:
            self.node_states[node_id] = {
                "node_id": node_id,
                "status": "online",
                "current_task": "",
                "progress": 0,
                "active_tasks": [],
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            }

    # ─── Task State ────────────────────────────────────────────────────────────

    async def _on_task_created(self, ev: Event) -> None:
        p = ev.payload
        task_id = p.get("task_id")
        if task_id:
            self.task_states[task_id] = {
                "task_id": task_id,
                "title": p.get("title", ""),
                "status": "pending",
                "node_id": "",
                "progress": 0,
                "dispatch_mode": p.get("dispatch_mode", "open_claim"),
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            }

    async def _on_task_claimed(self, ev: Event) -> None:
        p = ev.payload
        task_id = p.get("task_id")
        if task_id and task_id in self.task_states:
            self.task_states[task_id].update({
                "status": "claimed",
                "node_id": p.get("node_id", ""),
                "last_event_seq": ev.seq,
            })

    async def _on_task_assigned(self, ev: Event) -> None:
        p = ev.payload
        task_id = p.get("task_id")
        if task_id and task_id in self.task_states:
            self.task_states[task_id].update({
                "status": "assigned",
                "node_id": p.get("node_id", ""),
                "last_event_seq": ev.seq,
            })

    async def _on_task_progress(self, ev: Event) -> None:
        p = ev.payload
        task_id = p.get("task_id")
        if not task_id:
            return
        if task_id not in self.task_states:
            # 没见过这个任务，自动创建（task.created 事件可能晚到或丢失）
            self.task_states[task_id] = {
                "task_id": task_id,
                "title": p.get("title", ""),
                "status": "in_progress",
                "node_id": p.get("node_id", ""),
                "progress": p.get("progress", 0),
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            }
        else:
            self.task_states[task_id].update({
                "progress": p.get("progress", 0),
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            })

    async def _on_task_done(self, ev: Event) -> None:
        p = ev.payload
        task_id = p.get("task_id")
        if task_id and task_id in self.task_states:
            self.task_states[task_id].update({
                "status": "done",
                "progress": 100,
                "result": p.get("result", ""),
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
            })

    async def _on_task_failed(self, ev: Event) -> None:
        p = ev.payload
        task_id = p.get("task_id")
        if task_id and task_id in self.task_states:
            self.task_states[task_id].update({
                "status": "failed",
                "last_event_seq": ev.seq,
                "last_updated": ev.timestamp,
                "error": p.get("error", ""),
            })

    # ─── Skill State ───────────────────────────────────────────────────────────

    async def _on_skill_registered(self, ev: Event) -> None:
        p = ev.payload
        skill_id = p.get("skill_id")
        if skill_id:
            self.skill_states[skill_id] = {
                "skill_id": skill_id,
                "name": p.get("name", ""),
                "version": p.get("version", ""),
                "node_id": p.get("node_id", ev.source),
                "description": p.get("description", ""),
                "registered_at": ev.timestamp,
                "last_event_seq": ev.seq,
            }

    async def _on_skill_unregistered(self, ev: Event) -> None:
        p = ev.payload
        skill_id = p.get("skill_id")
        if skill_id and skill_id in self.skill_states:
            del self.skill_states[skill_id]

    # ─── Query ─────────────────────────────────────────────────────────────────

    async def get_node_state(self, node_id: str) -> Optional[Dict]:
        async with self._lock:
            return self.node_states.get(node_id)

    async def get_all_node_states(self) -> List[Dict]:
        async with self._lock:
            return list(self.node_states.values())

    async def get_task_state(self, task_id: str) -> Optional[Dict]:
        async with self._lock:
            return self.task_states.get(task_id)

    async def get_all_task_states(self) -> List[Dict]:
        async with self._lock:
            return list(self.task_states.values())

    async def get_skill_state(self, skill_id: str) -> Optional[Dict]:
        async with self._lock:
            return self.skill_states.get(skill_id)

    async def get_all_skill_states(self) -> List[Dict]:
        async with self._lock:
            return list(self.skill_states.values())

    # ─── Event Topic 路由 ─────────────────────────────────────────────────────

    def _dispatch(self, topic: str):
        """将 topic 路由到对应的处理函数"""
        if topic == Topic.NODE_STATE:
            return self._on_node_state
        if topic == Topic.NODE_HEARTBEAT:
            return self._on_node_heartbeat
        if topic == Topic.NODE_OFFLINE:
            return self._on_node_offline
        if topic == Topic.NODE_REGISTERED:
            return self._on_node_registered
        if topic == Topic.TASK_CREATED:
            return self._on_task_created
        if topic == Topic.TASK_CLAIMED:
            return self._on_task_claimed
        if topic == Topic.TASK_ASSIGNED:
            return self._on_task_assigned
        if topic == Topic.TASK_PROGRESS:
            return self._on_task_progress
        if topic == Topic.TASK_DONE:
            return self._on_task_done
        if topic == Topic.TASK_FAILED:
            return self._on_task_failed
        if topic == Topic.SKILL_REGISTERED:
            return self._on_skill_registered
        if topic == Topic.SKILL_UNREGISTERED:
            return self._on_skill_unregistered
        return None
