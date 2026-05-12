"""
ClawShell Cloud Hub — SwarmDomain
================================

集群协调 Domain（从 ClawShell-Windows lib/layer4/swarm.py 提取重构）

核心能力：
- NodeRegistry: 节点注册、心跳、超时管理
- TrustManager: 信任评估（基于历史行为）
- EcologyMatcher: 生态位匹配（能力互补）

事件类型：
- swarm.node_register   → 节点注册
- swarm.node_heartbeat → 心跳
- swarm.node_offline   → 节点下线
- swarm.trust_evaluate → 信任评估更新
- swarm.ecology_match  → 生态位匹配结果
"""

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ..event_store.schema import Event, Topic
from ..event_store.store import OssEventStore
from ..pubsub.manager import PubSubManager

logger = logging.getLogger("swarm_domain")


# ─── Types ─────────────────────────────────────────────────────────────────────

class NodeType(str, Enum):
    OPENCLAW = "openclaw"
    HERMES = "hermes"
    WUKONG = "wukong"
    EDGE_MAC = "edge-mac"
    EDGE_WIN = "edge-win"
    EDGE_LINUX = "edge-linux"
    N8N = "n8n"
    MEMOS = "memos"
    SKILL = "skill"
    UNKNOWN = "unknown"


class NodeStatus(str, Enum):
    UNKNOWN = "unknown"
    ACTIVE = "active"
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"


# ─── Node ─────────────────────────────────────────────────────────────────────

@dataclass
class Node:
    id: str
    name: str
    type: NodeType
    endpoint: Optional[str] = None
    status: NodeStatus = NodeStatus.UNKNOWN
    capabilities: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_interval: int = 30
    trust_score: float = 1.0  # 0.0-1.0
    failed_tasks: int = 0
    successful_tasks: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.value if isinstance(self.type, NodeType) else self.type,
            "endpoint": self.endpoint,
            "status": self.status.value if isinstance(self.status, NodeStatus) else self.status,
            "capabilities": self.capabilities,
            "metadata": self.metadata,
            "registered_at": self.registered_at,
            "last_heartbeat": self.last_heartbeat,
            "heartbeat_interval": self.heartbeat_interval,
            "trust_score": self.trust_score,
            "failed_tasks": self.failed_tasks,
            "successful_tasks": self.successful_tasks,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Node":
        t = d.get("type", "unknown")
        s = d.get("status", "unknown")
        return cls(
            id=d["id"],
            name=d["name"],
            type=NodeType(t) if t in NodeType._value2member_map_ else NodeType.UNKNOWN,
            endpoint=d.get("endpoint"),
            status=NodeStatus(s) if s in NodeStatus._value2member_map_ else NodeStatus.UNKNOWN,
            capabilities=d.get("capabilities", []),
            metadata=d.get("metadata", {}),
            registered_at=d.get("registered_at", time.time()),
            last_heartbeat=d.get("last_heartbeat", time.time()),
            heartbeat_interval=d.get("heartbeat_interval", 30),
            trust_score=d.get("trust_score", 1.0),
            failed_tasks=d.get("failed_tasks", 0),
            successful_tasks=d.get("successful_tasks", 0),
        )


# ─── Trust Manager ────────────────────────────────────────────────────────────

class TrustManager:
    """
    信任评估器。

    基于行为历史计算节点信任分：
    - 成功任务 → +分
    - 失败任务 → -分
    - 超时无心跳 → -分
    - 欺骗行为 → 大幅 -分
    """

    TRUST_INITIAL = 1.0
    TRUST_MIN = 0.0
    TRUST_MAX = 1.0
    SUCCESS_BONUS = 0.02
    FAILURE_PENALTY = 0.05
    HEARTBEAT_PENALTY = 0.10
    DECAY_PER_HOUR = 0.01  # 自然衰减

    def evaluate(self, node: Node, event_type: str = None) -> float:
        """计算更新后的信任分"""
        now = time.time()
        hours_offline = (now - node.last_heartbeat) / 3600

        score = node.trust_score

        # 自然衰减（每小时）
        score -= hours_offline * self.DECAY_PER_HOUR

        # 心跳超时惩罚
        if hours_offline > 2:
            score -= self.HEARTBEAT_PENALTY

        return max(self.TRUST_MIN, min(self.TRUST_MAX, score))

    def record_success(self, node: Node) -> float:
        """记录成功任务"""
        node.successful_tasks += 1
        node.trust_score = min(
            self.TRUST_MAX,
            node.trust_score + self.SUCCESS_BONUS
        )
        return node.trust_score

    def record_failure(self, node: Node) -> float:
        """记录失败任务"""
        node.failed_tasks += 1
        node.trust_score = max(
            self.TRUST_MIN,
            node.trust_score - self.FAILURE_PENALTY
        )
        return node.trust_score


# ─── Ecology Matcher ───────────────────────────────────────────────────────────

@dataclass
class EcologySlot:
    """生态位槽位"""
    role: str  # "coordinator", "executor", "observer"
    required_capabilities: List[str]
    filled_by: Optional[str] = None  # node_id


class EcologyMatcher:
    """
    生态位匹配器（借鉴 ClawShell-Deep TerminalManager）。

    评分公式：score = 0.4×能力匹配 + 0.3×负载均衡 + 0.3×信任分

    能力匹配 = (节点能力 ∩ 角色所需能力) / (角色所需能力数量)
    负载均衡 = 1 - (节点失败任务数 / 所有节点总任务数)
    信任分 = 节点信任分

    信任门槛：0.3（低于此直接排除）
    """

    # 权重（与 Deep TerminalManager 一致）
    WEIGHT_CAPABILITY = 0.4
    WEIGHT_LOAD = 0.3
    WEIGHT_TRUST = 0.3

    TRUST_THRESHOLD = 0.3  # 信任门槛，低于此排除

    ROLE_CAPABILITIES = {
        "coordinator": ["planning", "routing", "strategy"],
        "executor": ["coding", "writing", "analysis"],
        "observer": ["monitoring", "logging", "alerting"],
        "memory": ["storage", "retrieval", "indexing"],
        "communicator": ["messaging", "notification", "reporting"],
    }

    def match(self, required_roles: List[str],
              available_nodes: List[Node]) -> Dict[str, Optional[str]]:
        """
        匹配节点到角色（三因子加权评分）。

        Returns:
            {role: node_id or None}
        """
        # 计算全局负载基准
        total_tasks = sum(n.failed_tasks + n.successful_tasks for n in available_nodes) or 1

        assignments: Dict[str, Optional[str]] = {}
        assigned_nodes: set = set()

        for role in required_roles:
            required = set(self.ROLE_CAPABILITIES.get(role, []))
            best_node: Optional[str] = None
            best_score: float = -1.0

            for node in available_nodes:
                if node.id in assigned_nodes:
                    continue
                if node.status == NodeStatus.OFFLINE:
                    continue
                if node.trust_score < self.TRUST_THRESHOLD:
                    continue

                # 因子1：能力匹配（交集/并集）
                node_caps = set(node.capabilities)
                cap_overlap = len(required & node_caps)
                cap_score = cap_overlap / len(required) if required else 0.0

                # 因子2：负载均衡（失败任务占比越低越好）
                node_load = (node.failed_tasks + node.successful_tasks) / total_tasks
                load_score = 1.0 - min(node_load, 1.0)

                # 因子3：信任分（直接使用）
                trust_score = node.trust_score

                # 综合评分
                score = (
                    self.WEIGHT_CAPABILITY * cap_score +
                    self.WEIGHT_LOAD * load_score +
                    self.WEIGHT_TRUST * trust_score
                )

                if score > best_score:
                    best_score = score
                    best_node = node.id

            if best_node:
                assigned_nodes.add(best_node)
            assignments[role] = best_node

        return assignments


# ─── Node Registry ─────────────────────────────────────────────────────────────

class NodeRegistry:
    """节点注册表"""

    NODE_TIMEOUT = 120  # 秒

    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.trust_manager = TrustManager()
        self.ecology_matcher = EcologyMatcher()

    def register(self, name: str, node_type: NodeType,
                 endpoint: Optional[str] = None,
                 capabilities: List[str] = None,
                 metadata: Dict[str, Any] = None) -> Node:
        node_id = f"{node_type.value}-{int(time.time())}"
        node = Node(
            id=node_id,
            name=name,
            type=node_type,
            endpoint=endpoint,
            status=NodeStatus.ACTIVE,
            capabilities=capabilities or [],
            metadata=metadata or {},
        )
        self.nodes[node_id] = node
        return node

    def unregister(self, node_id: str) -> bool:
        return bool(self.nodes.pop(node_id, None))

    def heartbeat(self, node_id: str, status: NodeStatus = None) -> bool:
        node = self.nodes.get(node_id)
        if not node:
            return False
        node.last_heartbeat = time.time()
        if status:
            node.status = status
        elif node.status == NodeStatus.OFFLINE:
            node.status = NodeStatus.ACTIVE
        return True

    def get_active_nodes(self) -> List[Node]:
        now = time.time()
        active = []
        for node in self.nodes.values():
            if now - node.last_heartbeat < self.NODE_TIMEOUT:
                if node.status != NodeStatus.OFFLINE:
                    active.append(node)
        return active

    def get_node(self, node_id: str) -> Optional[Node]:
        return self.nodes.get(node_id)

    def update_trust(self, node_id: str, event_type: str) -> Optional[float]:
        node = self.nodes.get(node_id)
        if not node:
            return None
        if event_type == "success":
            return self.trust_manager.record_success(node)
        elif event_type == "failure":
            return self.trust_manager.record_failure(node)
        else:
            node.trust_score = self.trust_manager.evaluate(node, event_type)
            return node.trust_score


# ─── Domain ───────────────────────────────────────────────────────────────────

class SwarmDomain:
    """
    集群协调 Domain。

    整合 NodeRegistry + TrustManager + EcologyMatcher。

    事件类型：
    - swarm.node_register   → 节点注册
    - swarm.node_heartbeat  → 心跳
    - swarm.node_offline    → 节点下线
    - swarm.trust_update    → 信任分更新
    - swarm.ecology_match   → 生态位匹配
    """

    def __init__(self, store: OssEventStore, pubsub: PubSubManager):
        self.store = store
        self.pubsub = pubsub
        self.registry = NodeRegistry()

    async def _emit(self, topic: str, data: dict, source: str = "cloud-hub"):
        ev = Event.make(topic, source, data)
        await self.store.append(ev)
        await self.pubsub.publish(ev)
        return ev

    # ─── API ─────────────────────────────────────────────────────────────────

    async def node_register(self, params: dict) -> dict:
        """node_register: 注册新节点"""
        name = params["name"]
        node_type = NodeType(params.get("type", "unknown"))
        endpoint = params.get("endpoint")
        capabilities = params.get("capabilities", [])
        metadata = params.get("metadata", {})

        node = self.registry.register(name, node_type, endpoint, capabilities, metadata)

        await self._emit("swarm.node_register", {
            "node": node.to_dict(),
        })

        return {"success": True, "node": node.to_dict()}

    async def node_heartbeat(self, params: dict) -> dict:
        """node_heartbeat: 节点心跳"""
        node_id = params["node_id"]
        status = NodeStatus(params.get("status", "active"))

        ok = self.registry.heartbeat(node_id, status)
        if ok:
            await self._emit("swarm.node_heartbeat", {
                "node_id": node_id, "status": status.value,
            })
        return {"success": ok}

    async def node_unregister(self, params: dict) -> dict:
        """node_unregister: 注销节点"""
        node_id = params["node_id"]
        node = self.registry.get_node(node_id)
        ok = self.registry.unregister(node_id)
        if ok and node:
            await self._emit("swarm.node_offline", {
                "node_id": node_id, "name": node.name,
            })
        return {"success": ok}

    async def list_nodes(self, params: dict) -> dict:
        """list_nodes: 列出所有节点"""
        active_only = params.get("active_only", False)
        if active_only:
            nodes = self.registry.get_active_nodes()
        else:
            nodes = list(self.registry.nodes.values())
        return {
            "success": True,
            "nodes": [n.to_dict() for n in nodes],
            "count": len(nodes),
        }

    async def trust_evaluate(self, params: dict) -> dict:
        """trust_evaluate: 评估节点信任分"""
        node_id = params["node_id"]
        event_type = params.get("event_type")  # success / failure / None(计算)

        if event_type:
            score = self.registry.update_trust(node_id, event_type)
        else:
            node = self.registry.get_node(node_id)
            if not node:
                return {"success": False, "error": "node not found"}
            score = self.registry.trust_manager.evaluate(node)

        await self._emit("swarm.trust_update", {
            "node_id": node_id, "trust_score": score,
        })

        return {"success": True, "trust_score": score}

    async def ecology_match(self, params: dict) -> dict:
        """ecology_match: 生态位匹配"""
        required_roles = params.get("roles", [])
        active_nodes = self.registry.get_active_nodes()

        assignments = self.registry.ecology_matcher.match(required_roles, active_nodes)

        await self._emit("swarm.ecology_match", {
            "roles": required_roles,
            "assignments": assignments,
        })

        return {
            "success": True,
            "assignments": assignments,
            "unassigned": [r for r, n in assignments.items() if n is None],
        }

    async def broadcast_skill_version(self, params: dict) -> dict:
        """
        broadcast_skill_version: 广播技能版本信息到所有订阅者。

        发布到 Topic.SKILL_REGISTERED 供节点订阅。
        记录版本变更日志但不修改注册表。

        Args:
            params: dict with skill_id and version (required),
                    node_id (optional, defaults to "broadcast"),
                    name (optional), description (optional)
        """
        skill_id = params.get("skill_id")
        version = params.get("version")
        if not skill_id or not version:
            return {"success": False, "error": "skill_id and version are required"}

        node_id = params.get("node_id", "broadcast")
        name = params.get("name", skill_id)
        description = params.get("description", "")

        from ..event_store.schema import Topic

        payload = {
            "skill_id": skill_id,
            "version": version,
            "node_id": node_id,
            "name": name,
            "description": description,
        }

        ev = Event.make(Topic.SKILL_REGISTERED, source=node_id, payload=payload)
        await self.store.append(ev)
        await self.pubsub.publish(ev)

        logger.info(f"[broadcast] skill_version broadcast: skill_id={skill_id} version={version} node_id={node_id}")

        return {
            "success": True,
            "skill_id": skill_id,
            "version": version,
            "topic": Topic.SKILL_REGISTERED,
        }

    def sync_broadcast_skill_version(self, params: dict) -> dict:
        """同步封装 broadcast_skill_version"""
        return asyncio.run(self.broadcast_skill_version(params))
