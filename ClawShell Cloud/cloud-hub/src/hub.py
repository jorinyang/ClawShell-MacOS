"""
ClawShell Cloud Hub — 协同调度中枢 v1.3
事件驱动架构：
  1. 所有操作产生事件 → Event Store (OSS)
  2. State Aggregator 从事件聚合状态
  3. PubSub Manager 通过 WS 向所有订阅者广播
  4. 支持离线端 reconnect + replay 补齐

┌─────────────────────────────────────────────────────────────────┐
│                         Cloud Hub                                │
│                                                                 │
│   WS Clients ─── PubSubManager ── StateAggregator              │
│                        │                                        │
│                   EventStore (OSS)                              │
│                        │                                        │
│                   DomainHandlers ── StateAggregator            │
└─────────────────────────────────────────────────────────────────┘
"""
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Optional
from dataclasses import asdict

import aiohttp
import jwt
import websockets
from aiohttp import web
from websockets.server import WebSocketServerProtocol

from .auth import create_token, create_refresh_token, verify_token
from .domains import (
    MemoryDomain, KanbanDomain, SkillDomain, NodeDomain,
    WorkflowDomain, GenomeDomain, AdaptiveDomain, SwarmDomain,
    DeepThinkEngine, ReviewDomain,
)
from .domains.scheduler import SchedulerDomain
from .event_store.schema import (
    Event, Topic, node_state_topic,
    SkillRegisteredEvent, TaskCreatedEvent,
    TaskProgressEvent, TaskDoneEvent,
    NodeStateEvent,
)
from .event_store.store import OssEventStore, SequenceGenerator
from .pubsub.manager import PubSubManager
from .state.aggregator import StateAggregator
from .storage import OssStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud-hub")

# ─── WS 消息类型 ──────────────────────────────────────────────────────────────
MSG_TYPE_AUTH            = "auth"
MSG_TYPE_AUTH_OK         = "auth_ok"
MSG_TYPE_MCP_REQUEST    = "mcp_request"
MSG_TYPE_MCP_RESPONSE   = "mcp_response"
MSG_TYPE_PING           = "ping"
MSG_TYPE_PONG           = "pong"
MSG_TYPE_EDGE_REGISTER  = "edge_register"
MSG_TYPE_EDGE_INFO      = "edge_info"
MSG_TYPE_SUBSCRIBE      = "subscribe"
MSG_TYPE_EVENT          = "event"
MSG_TYPE_NODE_STATE     = "node_state"
MSG_TYPE_SYSTEM         = "system"


# ─── Hub 核心 ────────────────────────────────────────────────────────────────

class CloudHub:
    """
    云端协同调度中枢（事件驱动版）。

    组件：
    - OssStore: OSS 统一存储
    - OssEventStore: 事件持久化（append + replay）
    - PubSubManager: WS 订阅-发布管理
    - StateAggregator: 从事件流聚合节点/任务状态
    - DomainHandlers: Kanban/Skill/Node/Memory 领域处理器
    """

    def __init__(self):
        self.store: OssStore = OssStore()
        self.seq_gen = SequenceGenerator(self.store)
        self.event_store = OssEventStore(self.store, self.seq_gen)
        self.pubsub = PubSubManager()
        self.state = StateAggregator()

        # Domain handlers
        self.node_domain = NodeDomain(self.store)
        self.kanban_domain = KanbanDomain(self.store)
        self.skill_domain = SkillDomain(self.store)
        self.memory_domain = MemoryDomain(self.store)
        self.workflow_domain = WorkflowDomain(self.store, self.pubsub)
        self.genome_domain = GenomeDomain(self.store, self.pubsub)
        self.adaptive_domain = AdaptiveDomain(self.store, self.pubsub)
        self.swarm_domain = SwarmDomain(self.store, self.pubsub)
        self.deep_think_engine = DeepThinkEngine(self.store)
        self.review_domain = ReviewDomain(self.store, self.pubsub)
        self.scheduler_domain = SchedulerDomain(self.store)

        # Phase 1 增强组件（从 Windows 移植）
        from .event_store.knowledge_graph import KnowledgeGraph
        from .event_store.pattern_miner import PatternMiner
        from .event_store.dead_letter_queue import DeadLetterQueue
        from .event_store.event_tracer import EventTracer
        from .event_store.event_aggregator import EventAggregator
        from .event_store.event_metrics import EventMetrics
        from .event_store.quality_evaluator import QualityEvaluator
        from .domains.self_healing import SelfHealingEngine
        from .domains.trust_manager import TrustManager
        from .domains.failure_detector import FailureDetector
        from .domains.swarm_discovery import SwarmDiscovery
        from .domains.metrics_collector import MetricsCollector
        from .domains.skill_market import SkillMarket
        from .domains.n8n import N8NBridgeDomain
        from .domains.adaptive_controller import AdaptiveController
        from .event_store.relation_engine import RelationEngine
        from .event_store.semantic_search import SemanticSearch
        from .event_store.metadata_index import MetadataIndex
        from .event_store.priority_queue import PriorityQueue
        from .event_store.lifecycle_hooks import MemPalaceHook
        from .event_store.condition_engine import ConditionEngine
        from .event_store.ml_engine import MLEngine
        from .event_store.strategy_registry import StrategyRegistry
        from .event_store.strategy_switcher import StrategySwitcher
        self.knowledge_graph = KnowledgeGraph()
        self.pattern_miner = PatternMiner()
        self.dlq = DeadLetterQueue()
        self.tracer = EventTracer()
        self.aggregator = EventAggregator()
        self.event_metrics = EventMetrics()
        self.quality_evaluator = QualityEvaluator()
        self.self_healing = SelfHealingEngine()
        self.trust_manager = TrustManager()
        self.failure_detector = FailureDetector()
        self.swarm_discovery = SwarmDiscovery(node_id=f"cloud-{uuid.uuid4().hex[:8]}")
        self.metrics_collector = MetricsCollector()
        self.skill_market = SkillMarket()
        self.n8n_bridge = N8NBridgeDomain()
        self.adaptive_controller = AdaptiveController()
        self.relation_engine = RelationEngine()
        self.semantic_search = SemanticSearch()
        self.metadata_index = MetadataIndex()
        self.priority_queue = PriorityQueue()
        self.lifecycle_hooks = MemPalaceHook()
        self.condition_engine = ConditionEngine()
        self.ml_engine = MLEngine()
        self.strategy_registry = StrategyRegistry()
        self.strategy_switcher = StrategySwitcher(self.strategy_registry)

        # JWT
        self.jwt_secret = os.environ.get("JWT_SECRET", "change-me-in-production")
        self.jwt_issuer = os.environ.get("JWT_ISSUER", "clawshell-cloud-hub")
        self.jwt_expires_hours = int(os.environ.get("JWT_EXPIRES_HOURS", "24"))

        # 连接管理
        self._connections: Dict[str, WebSocketServerProtocol] = {}  # node_id → ws
        self._conn_lock = asyncio.Lock()
        self._running = False

    # ─── 组件初始化 ────────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """初始化异步资源"""
        await self.store.initialize()
        await self.review_domain.start()
        logger.info("CloudHub initialized")

    async def shutdown(self) -> None:
        """关闭所有连接"""
        self._running = False
        await self.review_domain.stop()
        async with self._conn_lock:
            for node_id, ws in list(self._connections.items()):
                try:
                    await ws.close(1001, "Server shutdown")
                except Exception:
                    pass
            self._connections.clear()
        logger.info("CloudHub shutdown")

    # ─── emit_event: 所有事件的统一出口 ────────────────────────────────────────

    async def emit(self, topic: str, source: str, payload: dict) -> Event:
        """
        创建事件 → 写入 OSS → 更新状态 → 推送订阅者。
        这是所有 domain 操作的标准路径。
        """
        event = Event.make(topic=topic, source=source, payload=payload)
        # 1. 持久化到 OSS
        stored = await self.event_store.append(event)
        # 2. 更新内存状态
        await self.state.apply_event(stored)
        # 3. 推送在线订阅者
        await self.pubsub.publish(stored)
        logger.debug(f"Event emitted: {topic} seq={stored.seq}")
        return stored

    # ─── 快捷 emit ─────────────────────────────────────────────────────────────

    async def emit_skill_registered(self, skill_id: str, name: str, version: str,
                                     node_id: str, description: str = "") -> Event:
        return await self.emit(
            Topic.SKILL_REGISTERED, node_id,
            {"skill_id": skill_id, "name": name, "version": version,
             "node_id": node_id, "description": description},
        )

    async def emit_task_created(self, task_id: str, title: str,
                                 dispatch_mode: str = "open_claim") -> Event:
        return await self.emit(
            Topic.TASK_CREATED, "cloud-hub",
            {"task_id": task_id, "title": title, "dispatch_mode": dispatch_mode},
        )

    async def emit_task_progress(self, task_id: str, node_id: str,
                                  progress: int, notes: str = "") -> Event:
        return await self.emit(
            Topic.TASK_PROGRESS, node_id,
            {"task_id": task_id, "node_id": node_id,
             "progress": progress, "notes": notes},
        )

    async def emit_task_done(self, task_id: str, node_id: str,
                              result: str = "") -> Event:
        return await self.emit(
            Topic.TASK_DONE, node_id,
            {"task_id": task_id, "node_id": node_id, "result": result},
        )

    async def emit_node_state(self, node_id: str, status: str,
                               current_task: str = "", progress: int = 0,
                               active_tasks: list = None) -> Event:
        return await self.emit(
            Topic.NODE_STATE, node_id,
            {"node_id": node_id, "status": status,
             "current_task": current_task, "progress": progress,
             "active_tasks": active_tasks or []},
        )

    async def emit_node_offline(self, node_id: str) -> Event:
        return await self.emit(Topic.NODE_OFFLINE, node_id, {"node_id": node_id})

    # ─── WS 消息处理 ──────────────────────────────────────────────────────────

    async def handle_client(self, ws: WebSocketServerProtocol, path: str) -> None:
        """
        处理 WS 客户端连接。
        流程：
        1. 等待 auth (edge_register 或 JWT)
        2. 进入订阅模式
        3. 处理消息（mcp_request / node_state_update / ping）
        4. 异常时优雅关闭
        """
        node_id = None
        auth_mode = None

        try:
            # ── 第一步：认证 ─────────────────────────────────────────────────
            msg = await asyncio.wait_for(ws.recv(), timeout=30)
            msg_obj = json.loads(msg)
            msg_type = msg_obj.get("type")

            if msg_type == MSG_TYPE_EDGE_REGISTER:
                node_id, auth_mode = await self._handle_edge_register(ws, msg_obj)
            elif msg_type == MSG_TYPE_AUTH:
                node_id, auth_mode = await self._handle_auth(ws, msg_obj)
            else:
                await ws.send(json.dumps({"type": "error", "message": "Expected auth or edge_register first"}))
                await ws.close(1008, "Authentication required")
                return

            # ── 第二步：注册 WS 连接 ────────────────────────────────────────
            async with self._conn_lock:
                self._connections[node_id] = ws

            # ── 第三步：订阅 topics ─────────────────────────────────────────
            # 默认订阅所有公开事件 topics
            default_topics = [
                Topic.SKILL_REGISTERED, Topic.SKILL_INVOKED, Topic.SKILL_RESULT,
                Topic.TASK_CREATED, Topic.TASK_CLAIMED, Topic.TASK_ASSIGNED,
                Topic.TASK_PROGRESS, Topic.TASK_DONE, Topic.TASK_FAILED,
                Topic.NODE_REGISTERED, Topic.NODE_STATE,
                "node.state.*",       # 动态节点状态
                Topic.SYSTEM_RECONNECT,
            ]
            last_seq = msg_obj.get("last_seq", 0)
            sub = await self.pubsub.subscribe(ws, default_topics, last_seq)

            # 如果有离线期间的事件需要 replay
            if last_seq > 0:
                await self._replay_events(ws, last_seq)

            # 发送 edge_info 确认
            latest_seq = await self.event_store.get_latest_seq()
            await ws.send(json.dumps({
                "type": MSG_TYPE_EDGE_INFO,
                "node_id": node_id,
                "registered": True,
                "last_seq": latest_seq,
                "auth_mode": auth_mode,
            }))

            logger.info(f"Edge connected: node_id={node_id} mode={auth_mode}")

            # ── 第四步：消息循环 ─────────────────────────────────────────────
            async for raw in ws:
                try:
                    msg_obj = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                msg_type = msg_obj.get("type")

                if msg_type == MSG_TYPE_MCP_REQUEST:
                    await self._handle_mcp_request(ws, node_id, msg_obj, sub)

                elif msg_type == MSG_TYPE_NODE_STATE:
                    await self._handle_node_state(ws, node_id, msg_obj)

                elif msg_type == MSG_TYPE_PING:
                    await ws.send(json.dumps({"type": MSG_TYPE_PONG}))

                elif msg_type == MSG_TYPE_SUBSCRIBE:
                    topics = msg_obj.get("topics", [])
                    await self.pubsub.subscribe(ws, topics, sub.last_seq)

                elif msg_type == "replay_request":
                    # 客户端主动请求 replay
                    since = msg_obj.get("last_seq", 0)
                    await self._replay_events(ws, since)

                elif msg_type == "edge_disconnect":
                    # 端侧主动断开
                    logger.info(f"Edge disconnected gracefully: node_id={node_id}")
                    break

                else:
                    await ws.send(json.dumps({
                        "type": "error",
                        "code": -32601,
                        "message": f"Unknown method: {msg_type}",
                    }))

        except asyncio.TimeoutError:
            logger.warning(f"Auth timeout for {ws.remote_address}")
            try:
                await ws.close(1008, "Auth timeout")
            except Exception:
                pass
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"WS closed: node_id={node_id}")
        except Exception as e:
            logger.exception(f"Error in handle_client node_id={node_id}: {e}")
        finally:
            # 清理连接
            if node_id:
                await self.pubsub.unsubscribe(ws)
                async with self._conn_lock:
                    self._connections.pop(node_id, None)
                # 标记节点离线
                await self.emit_node_offline(node_id)

    # ─── 认证处理 ─────────────────────────────────────────────────────────────

    async def _handle_edge_register(self, ws: WebSocketServerProtocol,
                                     msg_obj: dict) -> tuple[str, str]:
        """处理 edge_register：节点注册协议"""
        node_id = msg_obj.get("node_id")
        node_info = msg_obj.get("node_info", {})

        if not node_id:
            node_id = str(uuid.uuid4())

        # 注册节点到 NodeDomain
        await self.node_domain.node_register({
            "node_id": node_id,
            "node_info": node_info,
        })

        # 发送 node.registered 事件（广播给所有端）
        await self.emit(Topic.NODE_REGISTERED, node_id, {
            "node_id": node_id,
            "name": node_info.get("name", f"node-{node_id[:8]}"),
            "platform": node_info.get("platform", "unknown"),
            "capabilities": node_info.get("capabilities", []),
        })

        return node_id, "node_register"

    async def _handle_auth(self, ws: WebSocketServerProtocol,
                           msg_obj: dict) -> tuple[str, str]:
        """处理 JWT 认证"""
        token = msg_obj.get("token")
        if not token:
            raise ValueError("Token required")

        payload = verify_token(token, self.jwt_secret)
        if not payload:
            raise ValueError("Invalid token")

        return payload.get("user_id", "anonymous"), "jwt"

    # ─── MCP 请求处理 ─────────────────────────────────────────────────────────

    async def _handle_mcp_request(self, ws: WebSocketServerProtocol,
                                    node_id: str,
                                    msg_obj: dict,
                                    sub) -> None:
        """
        处理 MCP 请求：路由到 domain handler，
        将结果包装为响应，并触发相关事件。
        """
        method = msg_obj.get("method", "")
        params = msg_obj.get("params", {})

        try:
            result = await self._route_mcp(method, params, node_id)

            # 更新订阅者的 seq（客户端确认）
            await self.pubsub.update_seq(ws, sub.last_seq)

            await ws.send(json.dumps({
                "type": MSG_TYPE_MCP_RESPONSE,
                "request_id": msg_obj.get("id"),
                "result": result,
            }))

        except Exception as e:
            logger.exception(f"MCP error {method}: {e}")
            await ws.send(json.dumps({
                "type": MSG_TYPE_MCP_RESPONSE,
                "request_id": msg_obj.get("id"),
                "error": {"code": -32603, "message": str(e)},
            }))

    async def _route_mcp(self, method: str, params: dict,
                          node_id: str) -> dict:
        """
        路由 MCP method 到对应 domain handler。
        读操作（get/list/query）直接返回结果。
        写操作（create/update/register/invoke）触发事件。
        """

        # ── Skill Domain ────────────────────────────────────────────────────
        if method == "skill_register":
            r = await self.skill_domain.skill_register(params)
            # 触发事件
            skill_def = params.get("skill_def", {})
            await self.emit_skill_registered(
                skill_id=params.get("skill_id", r.get("skill_id", "")),
                name=skill_def.get("name", ""),
                version=skill_def.get("version", ""),
                node_id=node_id,
                description=skill_def.get("description", ""),
            )
            return r

        if method == "skill_invoke":
            r = await self.skill_domain.skill_invoke(params)
            # 触发 skill.invoked 事件，推送给目标端
            if r.get("dispatched"):
                await self.emit(
                    Topic.SKILL_INVOKED, "cloud-hub",
                    {"skill_id": params.get("skill_id"),
                     "invoke_id": r.get("invoke_id"),
                     "target_node": params.get("target_node"),
                     "input": params.get("input", {})},
                )
            return r

        if method == "skill_result":
            r = await self.skill_domain.skill_result_callback(params)
            # 触发 skill.result 事件
            await self.emit(Topic.SKILL_RESULT, node_id, {
                "invoke_id": params.get("invoke_id"),
                "node_id": node_id,
                "result": params.get("result", ""),
                "success": r.get("success", True),
            })
            return r

        if method == "skill_discover":
            # 读操作，从 StateAggregator 直接查
            skills = await self.state.get_all_skill_states()
            return {"skills": skills}

        if method == "skill_get":
            return await self.skill_domain.skill_get(params)

        # ── Kanban Domain ────────────────────────────────────────────────────
        if method == "kanban_board_create":
            r = await self.kanban_domain.kanban_board_create(params)
            return r

        if method == "kanban_board_list":
            return await self.kanban_domain.kanban_board_list(params)

        if method == "kanban_board_get":
            return await self.kanban_domain.kanban_board_get(params)

        if method == "kanban_task_create":
            r = await self.kanban_domain.kanban_task_create(params)
            # 触发 task.created 事件
            task = r.get("task", {})
            await self.emit_task_created(
                task_id=task.get("task_id", ""),
                title=task.get("title", ""),
                dispatch_mode=task.get("dispatch_mode", "open_claim"),
            )
            return r

        if method == "kanban_task_list":
            return await self.kanban_domain.kanban_task_list(params)

        if method == "kanban_task_move":
            r = await self.kanban_domain.kanban_task_move(params)
            # 触发 task.progress 事件
            task = r.get("task", {})
            status = params.get("status")
            if status in ("working", "done"):
                await self.emit_task_progress(
                    task_id=task.get("task_id", ""),
                    node_id=node_id,
                    progress=100 if status == "done" else 0,
                    notes=f"status → {status}",
                )
            return r

        if method == "kanban_task_assign":
            r = await self.kanban_domain.kanban_task_assign(params)
            # 触发 task.assigned 事件
            task = r.get("task", {})
            await self.emit(
                Topic.TASK_ASSIGNED, "cloud-hub",
                {"task_id": task.get("task_id", ""),
                 "node_id": params.get("node_id"),
                 "title": task.get("title", "")},
            )
            return r

        if method == "kanban_task_claim":
            r = await self.kanban_domain.kanban_task_claim(params)
            if r.get("claimed"):
                task = r.get("task", {})
                await self.emit(
                    Topic.TASK_CLAIMED, node_id,
                    {"task_id": task.get("task_id", ""),
                     "node_id": node_id, "status": "claimed"},
                )
            return r

        if method == "kanban_task_work_done":
            r = await self.kanban_domain.kanban_task_work_done(params)
            if r.get("done"):
                await self.emit_task_done(
                    task_id=params.get("task_id", ""),
                    node_id=node_id,
                    result=params.get("result", ""),
                )
            return r

        if method == "kanban_task_work_fail":
            r = await self.kanban_domain.kanban_task_work_fail(params)
            if r.get("failed"):
                await self.emit(
                    Topic.TASK_FAILED, node_id,
                    {"task_id": params.get("task_id", ""),
                     "node_id": node_id,
                     "error": params.get("error", "")},
                )
            return r

        if method == "kanban_task_update":
            return await self.kanban_domain.kanban_task_update(params)

        # ── Node Domain ──────────────────────────────────────────────────────
        if method == "node_register":
            r = await self.node_domain.node_register(params)
            await self.emit(Topic.NODE_REGISTERED, node_id, {
                "node_id": node_id,
                "name": params.get("node_info", {}).get("name", ""),
                "platform": params.get("node_info", {}).get("platform", ""),
                "capabilities": params.get("node_info", {}).get("capabilities", []),
            })
            return r

        if method == "node_list":
            return await self.node_domain.node_list(params)

        if method == "node_discover":
            return await self.node_domain.node_discover(params)

        if method == "node_state_query":
            # 从 StateAggregator 查询节点状态
            nid = params.get("node_id")
            state = await self.state.get_node_state(nid)
            if state:
                return {"node_state": state}
            return {"error": f"Node '{nid}' not found"}

        if method == "all_node_states":
            # 查询所有节点当前状态
            return {"nodes": await self.state.get_all_node_states()}

        if method == "node_heartbeat":
            r = await self.node_domain.node_heartbeat(params)
            await self.emit(Topic.NODE_HEARTBEAT, node_id, {"node_id": node_id})
            return r

        # ── Memory Domain ────────────────────────────────────────────────────
        if method == "memory_sync":
            return await self.memory_domain.memory_sync(params)

        if method == "memory_query":
            return await self.memory_domain.memory_query(params)

        if method == "knowledge_index":
            r = await self.memory_domain.knowledge_index(params)
            return r

        if method == "knowledge_search":
            return await self.memory_domain.knowledge_search(params)

        if method == "knowledge_list":
            return await self.memory_domain.knowledge_list(params)

        # ── Vault ─────────────────────────────────────────────────────────────
        if method == "vault_upload":
            key = params.get("key", "")
            content = params.get("content", "")
            await self.store.vault_upload(key, content)
            return {"success": True, "key": key}

        if method == "vault_download":
            key = params.get("key", "")
            content = await self.store.vault_download(key)
            return {"content": content}

        if method == "vault_list":
            return await self.store.vault_list(params.get("prefix", ""))

        # ── Scheduler Domain ──────────────────────────────────────────────────
        if method == "scheduler_register":
            return await self.scheduler_domain.scheduler_register(params)

        if method == "scheduler_unregister":
            return await self.scheduler_domain.scheduler_unregister(params)

        if method == "scheduler_list":
            return await self.scheduler_domain.scheduler_list(params)

        if method == "scheduler_log":
            return await self.scheduler_domain.scheduler_log(params)

        if method == "scheduler_trigger":
            return await self.scheduler_domain.scheduler_trigger(params)

        if method == "scheduler_start":
            return await self.scheduler_domain.scheduler_start(params)

        if method == "scheduler_shutdown":
            return await self.scheduler_domain.scheduler_shutdown(params)

        # ── Workflow Domain ──────────────────────────────────────────────────
        if method == "workflow_define":
            return await self.workflow_domain.workflow_define(params)

        if method == "workflow_get":
            return await self.workflow_domain.workflow_get(params)

        if method == "workflow_execute":
            r = await self.workflow_domain.workflow_execute(params)
            return r

        if method == "workflow_status":
            return await self.workflow_domain.workflow_status(params)

        if method == "workflow_cancel":
            return await self.workflow_domain.workflow_cancel(params)

        if method == "workflow_list":
            return await self.workflow_domain.workflow_list(params)

        if method == "workflow_wait":
            return await self.workflow_domain.workflow_wait(params)

        if method == "workflow_confirm":
            return await self.workflow_domain.workflow_confirm(params)

        # ── Genome Domain ───────────────────────────────────────────────────
        if method == "genome_get":
            return await self.genome_domain.genome_get(params)
        if method == "knowledge_add":
            return await self.genome_domain.knowledge_add(params)
        if method == "knowledge_query":
            return await self.genome_domain.knowledge_query(params)
        if method == "error_pattern_add":
            return await self.genome_domain.error_pattern_add(params)
        if method == "error_resolve":
            return await self.genome_domain.error_resolve(params)
        if method == "skill_update":
            return await self.genome_domain.skill_update(params)
        if method == "evolve":
            return await self.genome_domain.evolve(params)
        if method == "heritage":
            return await self.genome_domain.heritage(params)
        if method == "genome_stats":
            return await self.genome_domain.genome_stats(params)

        # ── Adaptive Domain ─────────────────────────────────────────────────
        if method == "health_check":
            return await self.adaptive_domain.health_check(params)
        if method == "rule_evaluate":
            return await self.adaptive_domain.rule_evaluate(params)
        if method == "strategy_switch":
            return await self.adaptive_domain.strategy_switch(params)
        if method == "system_heal":
            return await self.adaptive_domain.system_heal(params)
        if method == "get_current_strategy":
            return await self.adaptive_domain.get_current_strategy(params)

        # ── Swarm Domain ────────────────────────────────────────────────────
        if method == "swarm_node_register":
            return await self.swarm_domain.node_register(params)
        if method == "swarm_node_heartbeat":
            return await self.swarm_domain.node_heartbeat(params)
        if method == "swarm_node_unregister":
            return await self.swarm_domain.node_unregister(params)
        if method == "swarm_list_nodes":
            return await self.swarm_domain.list_nodes(params)
        if method == "swarm_trust_evaluate":
            return await self.swarm_domain.trust_evaluate(params)
        if method == "swarm_ecology_match":
            return await self.swarm_domain.ecology_match(params)

        # ── Deep Think Engine ────────────────────────────────────────────────
        if method == "deep_think":
            return await self.deep_think_engine.deep_think(params)
        if method == "deep_think_get":
            return await self.deep_think_engine.deep_think_get(params)
        if method == "deep_think_cancel":
            return await self.deep_think_engine.deep_think_cancel(params)

        # ── Review Domain ─────────────────────────────────────────────────────
        if method == "review_generate":
            return await self.review_domain.review_generate(params)
        if method == "review_get":
            return await self.review_domain.review_get(params)
        if method == "review_list":
            return await self.review_domain.review_list(params)

        # ── Phase 1 增强组件 ─────────────────────────────────────────────
        # Knowledge Graph
        if method == "kg_entity_add":
            return self._kg_entity_add(params)
        if method == "kg_relation_add":
            return self._kg_relation_add(params)
        if method == "kg_query":
            return self._kg_query(params)
        if method == "kg_infer":
            return self._kg_infer(params)
        if method == "kg_stats":
            return self._kg_stats(params)
        # Pattern Miner
        if method == "pm_transaction_add":
            return self._pm_transaction_add(params)
        if method == "pm_mine":
            return self._pm_mine(params)
        if method == "pm_association_rules":
            return self._pm_association_rules(params)
        if method == "pm_stats":
            return self._pm_stats(params)
        # Dead Letter Queue
        if method == "dlq_add":
            return self._dlq_add(params)
        if method == "dlq_retry":
            return self._dlq_retry(params)
        if method == "dlq_list":
            return self._dlq_list(params)
        if method == "dlq_stats":
            return self._dlq_stats(params)
        # Event Tracer
        if method == "tracer_start":
            return self._tracer_start(params)
        if method == "tracer_end":
            return self._tracer_end(params)
        if method == "tracer_get":
            return self._tracer_get(params)
        if method == "tracer_stats":
            return self._tracer_stats(params)
        # Event Aggregator
        if method == "aggr_create_rule":
            return self._aggr_create_rule(params)
        if method == "aggr_receive":
            return self._aggr_receive(params)
        if method == "aggr_stats":
            return self._aggr_stats(params)

        # Phase 2: Self-Healing
        if method == "heal_auto_backup":
            return self._heal_auto_backup(params)
        if method == "heal_rollback":
            return self._heal_rollback(params)
        if method == "heal_health_report":
            return self._heal_health_report(params)

        # Phase 2: Trust Manager
        if method == "trust_evaluate":
            return self._trust_evaluate(params)
        if method == "trust_record_success":
            self.trust_manager.record_success(params.get("node_id", ""), params.get("details"))
            return {"success": True}
        if method == "trust_record_failure":
            self.trust_manager.record_failure(params.get("node_id", ""), params.get("details"))
            return {"success": True}
        if method == "trust_leaderboard":
            return {"success": True, "leaderboard": self.trust_manager.get_leaderboard()}

        # Phase 2: Event Metrics
        if method == "metrics_record":
            self.event_metrics.record(params.get("event_type", ""),
                                    size=params.get("size", 0),
                                    latency=params.get("latency", 0.0),
                                    is_error=params.get("is_error", False))
            return {"success": True}
        if method == "metrics_snapshot":
            return {"success": True, "snapshot": self.event_metrics.get_snapshot()}
        if method == "metrics_top":
            top = self.event_metrics.get_top_events(limit=params.get("limit", 10),
                                                     by=params.get("by", "count"))
            return {"success": True, "top": [vars(t) for t in top]}
        if method == "metrics_anomalies":
            return {"success": True, "anomalies": self.event_metrics.detect_anomalies()}

        # Phase 2: Quality Evaluator
        if method == "quality_evaluate":
            score = self.quality_evaluator.evaluate(params.get("entry", {}))
            return {"success": True, "score": score.to_dict()}
        if method == "quality_stats":
            return {"success": True, "stats": self.quality_evaluator.get_stats()}

        # Phase 3: Relation Engine
        if method == "rel_add":
            rel = self.relation_engine.add_relation(
                params.get("relation_type", ""), params.get("source", ""),
                params.get("target", ""), params.get("confidence", 1.0))
            return {"success": True, "relation": rel.to_dict()}
        if method == "rel_opposite":
            return {"success": True, "result": self.relation_engine.find_opposite(params.get("entity", ""))}
        if method == "rel_causes":
            return {"success": True, "result": self.relation_engine.find_causes(params.get("entity", ""))}
        if method == "rel_transitive":
            return {"success": True, "result": list(
                self.relation_engine.transitive_inference(params.get("entity", ""),
                                                        params.get("relation_type", "cause_effect")))}
        if method == "rel_deduce":
            return {"success": True, "result": self.relation_engine.deduce_from_opposites(params.get("entity", ""))}
        if method == "rel_stats":
            return {"success": True, "stats": self.relation_engine.get_stats()}

        # Phase 3: Semantic Search
        if method == "semantic_index":
            self.semantic_search.index_document(params.get("entity_id", ""),
                                             params.get("text", ""),
                                             params.get("metadata"))
            return {"success": True}
        if method == "semantic_search":
            results = self.semantic_search.search(params.get("query", ""),
                                               top_k=params.get("top_k", 10))
            return {"success": True, "results": [
                {"entity_id": r.entity_id, "score": r.score,
                 "highlights": r.highlights} for r in results]}
        if method == "semantic_similar":
            return {"success": True, "results": self.semantic_search.get_similar(
                params.get("entity_id", ""), params.get("top_k", 5))}
        if method == "semantic_stats":
            return {"success": True, "stats": self.semantic_search.get_stats()}

        # Phase 3: Metadata Index
        if method == "meta_add":
            entry = self.metadata_index.add(params.get("entity_id", ""),
                                          params.get("entity_type", ""),
                                          params.get("key", ""),
                                          params.get("value"))
            return {"success": True, "entry": entry.to_dict()}
        if method == "meta_search":
            results = self.metadata_index.search(params.get("key", ""),
                                               params.get("value"))
            return {"success": True, "results": [r.to_dict() for r in results]}
        if method == "meta_range":
            results = self.metadata_index.range_query(params.get("key", ""),
                                                     min_val=params.get("min"),
                                                     max_val=params.get("max"))
            return {"success": True, "results": [r.to_dict() for r in results]}
        if method == "meta_entity":
            return {"success": True, "results": [
                r.to_dict() for r in self.metadata_index.get_entity_metadata(params.get("entity_id", ""))]}
        if method == "meta_stats":
            return {"success": True, "stats": self.metadata_index.get_stats()}

        # Phase 3: Priority Queue
        if method == "pq_enqueue":
            from .event_store.priority_queue import Priority
            p = getattr(Priority, params.get("priority", "NORMAL"), Priority.NORMAL)
            item_id = self.priority_queue.enqueue(params.get("payload", {}),
                                                priority=p,
                                                timeout=params.get("timeout", 0))
            return {"success": True, "item_id": item_id}
        if method == "pq_dequeue":
            item = self.priority_queue.dequeue()
            return {"success": True, "item": item.to_dict() if item else None}
        if method == "pq_stats":
            return {"success": True, "stats": self.priority_queue.stats()}

        # Phase 3: Condition Engine
        if method == "cond_add_rule":
            from .event_store.condition_engine import Condition, Rule
            rule = Rule(rule_id=params.get("rule_id", f"rule_{int(time.time()*1000)}"),
                       name=params.get("name", ""),
                       condition=Condition(**params.get("condition", {})))
            self.condition_engine.add_rule(rule)
            return {"success": True, "rule_id": rule.rule_id}
        if method == "cond_evaluate":
            result = self.condition_engine.evaluate(params.get("rule_id", ""),
                                                  params.get("value", 0))
            return {"success": True, "result": result}
        if method == "cond_stats":
            return {"success": True, "stats": self.condition_engine.get_stats()}

        # Phase 3: ML Engine
        if method == "ml_add_sample":
            self.ml_engine.add_sample(params.get("metric", ""), params.get("value", 0.0))
            return {"success": True}
        if method == "ml_detect_anomaly":
            result = self.ml_engine.detect_anomaly(params.get("metric", ""),
                                                  params.get("value", 0.0),
                                                  threshold=params.get("threshold", 3.0))
            return {"success": True, "anomaly": result.to_dict() if result else None}
        if method == "ml_predict":
            result = self.ml_engine.predict_trend(params.get("metric", ""),
                                                steps=params.get("steps", 1))
            return {"success": True, "trend": result.__dict__ if result else None}
        if method == "ml_find_root_cause":
            return {"success": True, "causes": self.ml_engine.find_root_cause(
                params.get("symptom_metric", ""),
                params.get("candidate_metrics", []))}
        if method == "ml_stats":
            return {"success": True, "anomalies_count": len(self.ml_engine._anomalies)}

        # Phase 4: Strategy
        if method == "strategy_register":
            from .event_store.strategy_registry import Strategy
            s = Strategy(name=params.get("name", ""),
                        strategy_type=params.get("strategy_type", ""),
                        config=params.get("config", {}),
                        description=params.get("description", ""))
            self.strategy_registry.register(s)
            return {"success": True, "name": s.name}
        if method == "strategy_list":
            return {"success": True, "strategies": [
                s.to_dict() for s in self.strategy_registry.list_all()]}
        if method == "strategy_set_active":
            self.strategy_switcher.set_active(params.get("name", ""))
            return {"success": True}
        if method == "strategy_get_active":
            return {"success": True, "active": self.strategy_switcher.get_active()}

        # Phase 4: Failure Detector
        if method == "failure_record":
            from .domains.failure_detector import FailureType
            ft = getattr(FailureType, params.get("failure_type", "ERROR").upper(),
                        FailureType.ERROR)
            alert = self.failure_detector.record(params.get("node_id", ""),
                                               ft, params.get("details", ""))
            return {"success": True, "alert": alert.__dict__ if alert else None}
        if method == "failure_resolve":
            count = self.failure_detector.resolve(params.get("node_id", ""))
            return {"success": True, "resolved": count}
        if method == "failure_stats":
            return {"success": True, "stats": self.failure_detector.get_stats()}

        # Phase 4: Swarm Discovery
        if method == "discovery_announce":
            self.swarm_discovery.announce(params.get("addr", "localhost"),
                                       params.get("port", 9999),
                                       params.get("metadata"))
            return {"success": True}
        if method == "discovery_peers":
            return {"success": True, "peers": [
                {"node_id": p.node_id, "addr": p.addr, "port": p.port}
                for p in self.swarm_discovery.get_peers()]}
        if method == "discovery_stats":
            return {"success": True, "stats": self.swarm_discovery.get_stats()}

        # Phase 4: Metrics Collector
        if method == "swarm_metrics_record":
            from .domains.metrics_collector import PerformanceMetrics
            m = PerformanceMetrics(node_id=params.get("node_id", ""),
                                  timestamp=time.time(),
                                  requests_total=params.get("requests_total", 0),
                                  requests_success=params.get("requests_success", 0),
                                  avg_response_time_ms=params.get("avg_response_time_ms", 0))
            self.metrics_collector.record(params.get("node_id", ""), m)
            return {"success": True}
        if method == "swarm_metrics_agg":
            return {"success": True, "stats": self.metrics_collector.get_aggregated(
                params.get("node_id", ""))}
        if method == "swarm_metrics_top":
            return {"success": True, "top": self.metrics_collector.get_top_by_success_rate(
                limit=params.get("limit", 5))}

        # Phase 4: Skill Market
        if method == "skill_publish":
            from .domains.skill_market import MarketSkill
            s = MarketSkill(skill_id=params.get("skill_id", ""),
                           name=params.get("name", ""),
                           version=params.get("version", "1.0.0"),
                           description=params.get("description", ""),
                           content=params.get("content", ""),
                           author=params.get("author", ""),
                           tags=params.get("tags", []),
                           category=params.get("category", "general"))
            sid = self.skill_market.publish(s)
            return {"success": True, "skill_id": sid}
        if method == "skill_discover":
            results = self.skill_market.discover(params.get("query", ""),
                                               limit=params.get("limit", 10))
            return {"success": True, "skills": [s.to_dict() for s in results]}
        if method == "skill_stats":
            return {"success": True, "stats": self.skill_market.get_stats()}

        # Phase 4: Adaptive Controller
        if method == "adaptive_record":
            signals = self.adaptive_controller.record(
                cpu=params.get("cpu_percent", 0),
                memory=params.get("memory_percent", 0),
                response_time=params.get("response_time", 0),
                error_rate=params.get("error_rate", 0),
                throughput=params.get("throughput", 0))
            return {"success": True, "signals": [s.__dict__ for s in signals]}
        if method == "adaptive_set_threshold":
            self.adaptive_controller.set_threshold(
                params.get("metric", ""),
                warn=params.get("warn", 0),
                critical=params.get("critical", 0),
                target=params.get("target"))
            return {"success": True}
        if method == "adaptive_stats":
            return {"success": True, "stats": self.adaptive_controller.get_stats()}

        # P1b: N8N Bridge Domain
        if method == "n8n_add_route":
            pattern = params.get("event_pattern", "")
            url = params.get("webhook_url", "")
            result = self.n8n_bridge.add_route(pattern, url)
            return {"success": True, "pattern": result}
        if method == "n8n_remove_route":
            pattern = params.get("event_pattern", "")
            removed = self.n8n_bridge.remove_route(pattern)
            return {"success": removed}
        if method == "n8n_list_routes":
            return {"success": True, "routes": self.n8n_bridge.list_routes()}
        if method == "n8n_trigger":
            results = self.n8n_bridge.trigger(params.get("event", {}))
            return {"success": True, "results": results}
        if method == "n8n_trigger_workflow":
            result = self.n8n_bridge.trigger_workflow(
                params.get("webhook_url", ""), params.get("payload", {}))
            return {"success": True, "result": result}
        if method == "n8n_health_check":
            return {"success": True, "health": self.n8n_bridge.health_check()}
        if method == "n8n_trigger_log":
            return {"success": True, "log": self.n8n_bridge.get_trigger_log(
                params.get("limit", 50))}

        # ── 未知 method ──────────────────────────────────────────────────────
        return {"error": f"Method not found: {method}", "code": -32601}

    # ─── 节点状态更新 ─────────────────────────────────────────────────────────

    async def _handle_node_state(self, ws: WebSocketServerProtocol,
                                   node_id: str,
                                   msg_obj: dict) -> None:
        """处理节点状态上报"""
        payload = msg_obj.get("state", {})
        status = payload.get("status", "online")
        current_task = payload.get("current_task", "")
        progress = payload.get("progress", 0)
        active_tasks = payload.get("active_tasks", [])

        # 写入 NodeDomain
        await self.node_domain.node_update_status({
            "node_id": node_id,
            "status": status,
        })

        # 触发 node.state 事件
        await self.emit_node_state(
            node_id, status, current_task, progress, active_tasks,
        )

        await ws.send(json.dumps({
            "type": MSG_TYPE_NODE_STATE,
            "node_id": node_id,
            "received": True,
        }))

    # ─── Replay ───────────────────────────────────────────────────────────────

    async def _replay_events(self, ws: WebSocketServerProtocol,
                               last_seq: int) -> None:
        """
        将 last_seq 之后的所有事件 replay 给客户端。
        客户端收到后更新本地缓存。
        """
        events = await self.event_store.replay_by_seq(last_seq, limit=1000)
        if not events:
            return

        logger.info(f"Replaying {len(events)} events to conn={id(ws)} since seq={last_seq}")

        # 批量发送（减少 WS 往返）
        batch = [e.to_dict() for e in events]
        await ws.send(json.dumps({
            "type": "event_replay",
            "events": batch,
            "from_seq": last_seq + 1,
            "to_seq": events[-1].seq,
            "count": len(events),
        }))

        # 发送 system.reconnect 事件（含最新序列号）
        await ws.send(json.dumps({
            "type": "system",
            "topic": Topic.SYSTEM_RECONNECT,
            "payload": {
                "latest_seq": events[-1].seq,
                "replay_count": len(events),
            },
        }))

    # ─── Phase 1 增强组件 handlers ────────────────────────────────────────

    def _kg_entity_add(self, params: dict) -> dict:
        entity = self.knowledge_graph.add_entity(
            name=params.get("name", ""),
            entity_type=params.get("entity_type", "concept"),
            properties=params.get("properties", {}),
            entity_id=params.get("entity_id"),
        )
        return {"success": True, "entity": entity.to_dict()}

    def _kg_relation_add(self, params: dict) -> dict:
        rel = self.knowledge_graph.add_relation(
            source_id=params.get("source_id", ""),
            target_id=params.get("target_id", ""),
            relation_type=params.get("relation_type", "related_to"),
            weight=params.get("weight", 1.0),
            properties=params.get("properties", {}),
        )
        if rel is None:
            return {"success": False, "error": "source or target entity not found"}
        return {"success": True, "relation": rel.to_dict()}

    def _kg_query(self, params: dict) -> dict:
        result = self.knowledge_graph.query(
            start_id=params.get("start_id", ""),
            depth=params.get("depth", 2),
            relation_type=params.get("relation_type"),
        )
        return {
            "success": True,
            "entities": [e.to_dict() for e in result.entities],
            "relations": [r.to_dict() for r in result.relations],
            "paths": result.paths,
            "depth": result.depth,
        }

    def _kg_infer(self, params: dict) -> dict:
        inferences = self.knowledge_graph.infer(params.get("entity_id", ""))
        return {
            "success": True,
            "inferences": [
                {"entity": e.to_dict(), "type": t, "confidence": c}
                for e, t, c in inferences
            ],
        }

    def _kg_stats(self, params: dict) -> dict:
        return {"success": True, "stats": self.knowledge_graph.get_stats()}

    # Pattern Miner

    def _pm_transaction_add(self, params: dict) -> dict:
        items = params.get("items", [])
        if not isinstance(items, list):
            return {"success": False, "error": "items must be a list"}
        self.pattern_miner.add_transaction(items)
        return {"success": True, "stats": self.pattern_miner.get_stats()}

    def _pm_mine(self, params: dict) -> dict:
        patterns = self.pattern_miner.mine_frequent_itemsets()
        return {
            "success": True,
            "patterns": [p.to_dict() for p in patterns],
            "count": len(patterns),
        }

    def _pm_association_rules(self, params: dict) -> dict:
        rules = self.pattern_miner.mine_association_rules()
        return {
            "success": True,
            "rules": [r.to_dict() for r in rules],
            "count": len(rules),
        }

    def _pm_stats(self, params: dict) -> dict:
        return {"success": True, "stats": self.pattern_miner.get_stats()}

    # Dead Letter Queue

    def _dlq_add(self, params: dict) -> dict:
        from .event_store.dead_letter_queue import DLQReason
        reason_str = params.get("reason", "unknown")
        try:
            reason = DLQReason(reason_str)
        except Exception:
            reason = DLQReason.UNKNOWN
        dl_id = self.dlq.add(
            event=params.get("event", {}),
            reason=reason,
            error_message=params.get("error_message", ""),
            metadata=params.get("metadata"),
        )
        return {"success": True, "dlq_id": dl_id}

    def _dlq_retry(self, params: dict) -> dict:
        def processor(event):
            return True  # 模拟处理成功
        success = self.dlq.retry(params.get("dlq_id", ""), processor)
        return {"success": success}

    def _dlq_list(self, params: dict) -> dict:
        pending = self.dlq.get_pending(limit=params.get("limit", 100))
        return {
            "success": True,
            "dead_letters": [d.to_dict() for d in pending],
            "count": len(pending),
        }

    def _dlq_stats(self, params: dict) -> dict:
        stats = self.dlq.get_stats()
        return {"success": True, "stats": asdict(stats)}

    # Event Tracer

    def _tracer_start(self, params: dict) -> dict:
        span_id = self.tracer.start_trace(
            trace_id=params.get("trace_id", ""),
            event_id=params.get("event_id", ""),
            operation=params.get("operation", ""),
            parent_span_id=params.get("parent_span_id"),
            tags=params.get("tags"),
        )
        return {"success": True, "span_id": span_id}

    def _tracer_end(self, params: dict) -> dict:
        self.tracer.end_span(
            trace_id=params.get("trace_id", ""),
            span_id=params.get("span_id", ""),
            tags=params.get("tags"),
        )
        return {"success": True}

    def _tracer_get(self, params: dict) -> dict:
        result = self.tracer.get_trace(params.get("trace_id", ""))
        if result is None:
            return {"success": False, "error": "trace not found"}
        return {
            "success": True,
            "trace_id": result.trace_id,
            "total_duration": result.total_duration,
            "event_count": result.event_count,
            "spans": [s.to_dict() for s in result.spans],
        }

    def _tracer_stats(self, params: dict) -> dict:
        return {"success": True, "stats": self.tracer.get_stats()}

    # Event Aggregator

    def _aggr_create_rule(self, params: dict) -> dict:
        rule = self.aggregator.create_rule(
            name=params.get("name", ""),
            event_types=params.get("event_types", []),
            time_window=params.get("time_window", 60.0),
            count_threshold=params.get("count_threshold", 10),
            aggregation_key=params.get("aggregation_key"),
        )
        return {"success": True, "rule": {"id": rule.id, "name": rule.name}}

    def _aggr_receive(self, params: dict) -> dict:
        event = params.get("event", {})
        result = self.aggregator.receive_event(event)
        if result is None:
            return {"success": True, "aggregated": None}
        return {"success": True, "aggregated": result.to_dict()}

    def _aggr_stats(self, params: dict) -> dict:
        return {"success": True, "stats": self.aggregator.get_stats()}

    # ── Phase 2: Self-Healing ─────────────────────────────────────────

    def _heal_auto_backup(self, params: dict) -> dict:
        backup = self.self_healing.auto_backup(
            components=params.get("components")
        )
        if backup:
            return {"success": True, "backup": backup.to_dict()}
        return {"success": False, "error": "backup failed"}

    def _heal_rollback(self, params: dict) -> dict:
        ok = self.self_healing.auto_rollback(params.get("checkpoint_id", ""))
        return {"success": ok}

    def _heal_health_report(self, params: dict) -> dict:
        return {"success": True, "report": self.self_healing.get_health_report()}

    # ── Phase 2: Trust Manager ────────────────────────────────────────

    def _trust_evaluate(self, params: dict) -> dict:
        return {"success": True, **self.trust_manager.evaluate(params.get("node_id", ""))}

    # ── Phase 2: Event Metrics ────────────────────────────────────────

    def _metrics_snapshot(self, params: dict) -> dict:
        return {"success": True, "snapshot": self.event_metrics.get_snapshot()}

    # ── Phase 2: Quality Evaluator ────────────────────────────────────

    def _quality_evaluate(self, params: dict) -> dict:
        score = self.quality_evaluator.evaluate(params.get("entry", {}))
        return {"success": True, "score": score.to_dict()}

    def _quality_stats(self, params: dict) -> dict:
        return {"success": True, "stats": self.quality_evaluator.get_stats()}

    async def _handle_health(self, request: web.Request) -> web.Response:
        """健康检查端点（docker-compose healthcheck）"""
        return web.Response(text="ok\n", content_type="text/plain")

    # ─── REST API ─────────────────────────────────────────────────────────────

    async def _handle_rest(self, request: web.Request) -> web.Response:
        """REST API 入口（/api/v1/*）"""
        path = request.path
        method = request.method

        try:
            body = await request.json() if request.can_read_body else {}
        except Exception:
            body = {}

        # REST → 内部路由（node_id 固定为 "rest-client"）
        if path.startswith("/api/v1/skill"):
            result = await self._route_rest(path, body)
        elif path.startswith("/api/v1/node"):
            result = await self._route_rest(path, body)
        elif path.startswith("/api/v1/kanban"):
            result = await self._route_rest(path, body)
        elif path.startswith("/api/v1/vault"):
            result = await self._route_rest(path, body)
        elif path.startswith("/api/v1/deep_think"):
            result = await self._route_rest(path, body)
        else:
            return web.json_response({"error": "Not found"}, status=404)

        return web.json_response(result)

    async def _route_rest(self, path: str, body: dict) -> dict:
        """REST 到 MCP method 的简单映射"""
        if "/skill/discover" in path:
            skills = await self.state.get_all_skill_states()
            return {"skills": skills}
        if "/skill/broadcast" in path:
            result = await self.swarm_domain.broadcast_skill_version(body)
            return result
        if "/node/states" in path:
            return {"nodes": await self.state.get_all_node_states()}
        if "/event/stats" in path:
            latest = await self.event_store.get_latest_seq()
            return {"latest_seq": latest, "subscriber_count": self.pubsub.get_subscription_count()}
        if "/deep_think" in path:
            return await self.deep_think_engine.deep_think(body)
        return {"error": "Not implemented"}

    # ─── WebSocket 入口 ───────────────────────────────────────────────────────

    async def ws_handler(self, ws: WebSocketServerProtocol, path: str) -> None:
        await self.handle_client(ws, path)

    # ─── 启动 ─────────────────────────────────────────────────────────────────

    async def run(self, host: str = "0.0.0.0", ws_port: int = 8443,
                   rest_port: int = 8080) -> None:
        """启动 Cloud Hub（WS + REST）"""
        await self.initialize()
        self._running = True

        # REST API
        rest_app = web.Application()
        rest_app.router.add_route("/*", self._handle_rest)
        rest_app.router.add_route("GET", "/health", self._handle_health)
        rest_runner = web.AppRunner(rest_app)
        await rest_runner.setup()
        rest_site = web.TCPSite(rest_runner, host, rest_port)
        await rest_site.start()
        logger.info(f"REST API: http://{host}:{rest_port}")

        # WebSocket
        async with websockets.serve(self.ws_handler, host, ws_port):
            logger.info(f"WebSocket: ws://{host}:{ws_port}")
            while self._running:
                await asyncio.sleep(1)
