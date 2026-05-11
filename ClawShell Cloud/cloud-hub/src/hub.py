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

import aiohttp
import jwt
import websockets
from aiohttp import web
from websockets.server import WebSocketServerProtocol

from .auth import create_token, create_refresh_token, verify_token
from .domains import MemoryDomain, KanbanDomain, SkillDomain, NodeDomain
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
        logger.info("CloudHub initialized")

    async def shutdown(self) -> None:
        """关闭所有连接"""
        self._running = False
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
        else:
            return web.json_response({"error": "Not found"}, status=404)

        return web.json_response(result)

    async def _route_rest(self, path: str, body: dict) -> dict:
        """REST 到 MCP method 的简单映射"""
        if "/skill/discover" in path:
            skills = await self.state.get_all_skill_states()
            return {"skills": skills}
        if "/node/states" in path:
            return {"nodes": await self.state.get_all_node_states()}
        if "/event/stats" in path:
            latest = await self.event_store.get_latest_seq()
            return {"latest_seq": latest, "subscriber_count": self.pubsub.get_subscription_count()}
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
