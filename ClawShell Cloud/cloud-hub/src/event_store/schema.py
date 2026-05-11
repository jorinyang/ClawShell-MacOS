"""
ClawShell Cloud Hub — 事件 Schema
所有事件统一定义，Event Store 和 Pub/Sub 共用此 Schema
"""
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Optional


def new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:16]}"


def now_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime())


# ─── Topic 常量 ───────────────────────────────────────────────────────────────

class Topic:
    # 技能域
    SKILL_REGISTERED   = "skill.registered"
    SKILL_INVOKED     = "skill.invoked"
    SKILL_RESULT      = "skill.result"
    SKILL_UNREGISTERED = "skill.unregistered"

    # 任务域
    TASK_CREATED   = "task.created"
    TASK_CLAIMED   = "task.claimed"
    TASK_ASSIGNED  = "task.assigned"
    TASK_PROGRESS  = "task.progress"
    TASK_DONE      = "task.done"
    TASK_FAILED    = "task.failed"

    # 节点域
    NODE_REGISTERED = "node.registered"
    NODE_HEARTBEAT  = "node.heartbeat"
    NODE_STATE      = "node.state"          # node.state.{node_id}
    NODE_OFFLINE    = "node.offline"

    # 系统域
    SYSTEM_RECONNECT = "system.reconnect"  # 广播全局序列号，离线端补齐用
    SYSTEM_BROADCAST = "system.broadcast"  # 任意系统广播

    # 工作流域
    WORKFLOW_STARTED  = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED    = "workflow.failed"
    WORKFLOW_STEP_LOG  = "workflow.step_log"

    # 知识库域
    KNOWLEDGE_ADDED  = "knowledge.added"
    KNOWLEDGE_UPDATED = "knowledge.updated"

    # 任务域（补充）
    TASK_DONE         = "task.done"


def node_state_topic(node_id: str) -> str:
    return f"node.state.{node_id}"


# ─── Event Dataclass ───────────────────────────────────────────────────────────

@dataclass
class Event:
    event_id : str           # 全局唯一事件 ID
    seq      : int           # 全局单调递增序列号
    timestamp: str           # UTC 时间戳 ISO 格式
    topic    : str           # 事件主题
    source   : str           # 事件来源: node_id / "cloud-hub"
    payload  : dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        return cls(
            event_id=d["event_id"],
            seq=d["seq"],
            timestamp=d["timestamp"],
            topic=d["topic"],
            source=d["source"],
            payload=d.get("payload", {}),
        )

    @classmethod
    def make(cls, topic: str, source: str, payload: dict = None, seq: int = 0) -> "Event":
        """工厂方法，创建新事件（seq 由 EventStore 填充）"""
        return cls(
            event_id=new_event_id(),
            seq=seq,
            timestamp=now_ts(),
            topic=topic,
            source=source,
            payload=payload or {},
        )


# ─── Skill Events ─────────────────────────────────────────────────────────────

@dataclass
class SkillRegisteredEvent(Event):
    skill_id    : str = ""
    name        : str = ""
    version     : str = ""
    node_id     : str = ""
    description : str = ""

    @classmethod
    def create(cls, skill_id, name, version, node_id, description="", seq=0):
        return cls(
            event_id=new_event_id(), seq=seq,
            timestamp=now_ts(), topic=Topic.SKILL_REGISTERED, source=node_id,
            payload={"skill_id": skill_id, "name": name, "version": version,
                     "node_id": node_id, "description": description},
        )


# ─── Task Events ──────────────────────────────────────────────────────────────

@dataclass
class TaskCreatedEvent(Event):
    task_id      : str = ""
    title        : str = ""
    dispatch_mode: str = "open_claim"

    @classmethod
    def create(cls, task_id, title, dispatch_mode="open_claim", seq=0):
        return cls(
            event_id=new_event_id(), seq=seq,
            timestamp=now_ts(), topic=Topic.TASK_CREATED, source="cloud-hub",
            payload={"task_id": task_id, "title": title, "dispatch_mode": dispatch_mode},
        )


@dataclass
class TaskProgressEvent(Event):
    task_id   : str = ""
    node_id   : str = ""
    progress  : int = 0
    notes     : str = ""

    @classmethod
    def create(cls, task_id, node_id, progress, notes="", seq=0):
        return cls(
            event_id=new_event_id(), seq=seq,
            timestamp=now_ts(), topic=Topic.TASK_PROGRESS, source=node_id,
            payload={"task_id": task_id, "node_id": node_id,
                     "progress": progress, "notes": notes},
        )


@dataclass
class TaskDoneEvent(Event):
    task_id : str = ""
    node_id : str = ""
    result  : str = ""

    @classmethod
    def create(cls, task_id, node_id, result="", seq=0):
        return cls(
            event_id=new_event_id(), seq=seq,
            timestamp=now_ts(), topic=Topic.TASK_DONE, source=node_id,
            payload={"task_id": task_id, "node_id": node_id, "result": result},
        )


# ─── Node Events ───────────────────────────────────────────────────────────────

@dataclass
class NodeStateEvent(Event):
    node_id       : str = ""
    status        : str = ""   # online / idle / busy / offline
    current_task  : str = ""
    progress      : int = 0
    active_tasks  : list = field(default_factory=list)

    @classmethod
    def create(cls, node_id, status, current_task="", progress=0,
               active_tasks=None, seq=0):
        return cls(
            event_id=new_event_id(), seq=seq,
            timestamp=now_ts(), topic=Topic.NODE_STATE, source=node_id,
            payload={"node_id": node_id, "status": status,
                     "current_task": current_task, "progress": progress,
                     "active_tasks": active_tasks or []},
        )
