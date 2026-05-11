"""
ClawShell Cloud Hub — 协同调度中枢
集成所有 domain handler，维护节点注册表，支持云端主动推送调度指令
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

from .domains import MemoryDomain, KanbanDomain, SkillDomain, NodeDomain
from .auth import create_token, create_refresh_token, verify_token
from .protocol import (
    MSG_TYPE_AUTH, MSG_TYPE_AUTH_OK, MSG_TYPE_MCP_REQUEST,
    MSG_TYPE_PING, MSG_TYPE_PONG, MSG_TYPE_BROADCAST,
    MSG_TYPE_DISPATCH, MSG_TYPE_TASK_EVENT, MSG_TYPE_EDGE_REGISTER,
    MSG_TYPE_EDGE_INFO, MSG_TYPE_CLAIM, MSG_TYPE_CLAIM_OK, MSG_TYPE_CLAIM_REJECT,
    METHOD_PREFIX_MEMORY, METHOD_PREFIX_KNOWLEDGE,
    METHOD_PREFIX_KANBAN, METHOD_PREFIX_SKILL, METHOD_PREFIX_NODE, METHOD_PREFIX_VAULT,
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud-hub")


# ─────────────────────────────────────────────────────────────────
# Hub 核心
# ─────────────────────────────────────────────────────────────────

class CloudHub:
    """
    云端协同调度中枢。
    承载所有 domain handler，维护节点注册表，提供 WS + REST API。
    """

    def __init__(self, jwt_secret: str, user_id: str, port: int = 8443):
        self.jwt_secret = jwt_secret
        self.user_id = user_id
        self.port = port

        # ── Storage ────────────────────────────────────────────────
        oss_config = {
            "endpoint": os.environ.get("OSS_ENDPOINT", ""),
            "bucket": os.environ.get("OSS_BUCKET", ""),
            "access_key": os.environ.get("OSS_ACCESS_KEY_ID", ""),
            "secret_key": os.environ.get("OSS_ACCESS_KEY_SECRET", ""),
            "vault_prefix": os.environ.get("OSS_VAULT_PREFIX", "vault/"),
        }
        from .storage import OssStore
        self.store = OssStore(oss_config)

        # ── Domain Handlers ────────────────────────────────────────
        self.memory_domain = MemoryDomain(self.store)
        self.kanban_domain = KanbanDomain(self.store)
        self.skill_domain = SkillDomain(self.store)
        self.node_domain = NodeDomain(self.store)

        # ── 节点 WS 连接表 (node_id → ws) ─────────────────────────
        # 仅存储通过 edge.register 注册的节点（非简单 WS 客户端）
        self._node_ws: Dict[str, WebSocketServerProtocol] = {}
        self._node_ws_lock = asyncio.Lock()

        # ── WS Server ──────────────────────────────────────────────
        self._ws_server: Optional[Any] = None
        self._app: Optional[web.Application] = None

    # ─── WS 入口 ─────────────────────────────────────────────────────────────

    async def handle_client(self, ws: WebSocketServerProtocol):
        """处理端侧 WS 连接（支持两种协议：edge_register 和标准 MCP）"""
        client_id = str(uuid.uuid4())
        node_id = None
        authed = False

        try:
            # ── 认证帧 ────────────────────────────────────────────
            auth_frame = await asyncio.wait_for(ws.recv(), timeout=10)
            auth_data = json.loads(auth_frame)

            if auth_data.get("type") == MSG_TYPE_AUTH:
                token = auth_data.get("token", "")
                payload = verify_token(token, self.jwt_secret)
                if payload is None:
                    await ws.send(json.dumps({"type": "error", "message": "Invalid token"}))
                    await ws.close()
                    return
                user_id = payload.get("user_id")
                if user_id != self.user_id:
                    await ws.send(json.dumps({"type": "error", "message": "Invalid user"}))
                    await ws.close()
                    return
                authed = True
                await ws.send(json.dumps({"type": MSG_TYPE_AUTH_OK, "client_id": client_id}))
                logger.info(f"Client {client_id} authenticated (user={user_id})")

            elif auth_data.get("type") == MSG_TYPE_EDGE_REGISTER:
                # edge_register: 端侧直接注册，不需要 JWT
                node_id = auth_data.get("node_id")
                node_info = auth_data.get("node_info", {})
                if not node_id:
                    await ws.send(json.dumps({"error": "node_id required"}))
                    await ws.close()
                    return
                # 注册节点
                result = await self.node_domain.node_register({
                    "node_id": node_id,
                    "node_info": node_info,
                })
                async with self._node_ws_lock:
                    self._node_ws[node_id] = ws
                await ws.send(json.dumps({
                    "type": MSG_TYPE_EDGE_INFO,
                    "node_id": node_id,
                    "registered": True,
                    "cloud_hub_version": "2.0",
                }))
                authed = True
                logger.info(f"Edge node registered: {node_id} [{node_info.get('platform')}]")

            else:
                await ws.send(json.dumps({"error": f"Unknown auth type: {auth_data.get('type')}"}))
                await ws.close()
                return

        except asyncio.TimeoutError:
            logger.warning(f"Client {client_id} auth timeout")
            return
        except Exception as e:
            logger.error(f"Auth error: {e}")
            return

        # ── 消息循环 ──────────────────────────────────────────────
        try:
            async for raw in ws:
                await self._handle_ws_message(client_id, ws, raw, node_id)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if node_id:
                async with self._node_ws_lock:
                    self._node_ws.pop(node_id, None)
                await self.node_domain.node_unregister({"node_id": node_id})
                logger.info(f"Edge node disconnected: {node_id}")

    async def _handle_ws_message(self, client_id: str, ws: WebSocketServerProtocol,
                                  raw: str, node_id: Optional[str]):
        """路由 WS 消息"""
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send(json.dumps({"error": "Invalid JSON"}))
            return

        msg_type = req.get("type", "")

        # ── Ping ─────────────────────────────────────────────────
        if msg_type == MSG_TYPE_PING:
            await ws.send(json.dumps({"type": MSG_TYPE_PONG}))
            return

        # ── MCP 请求 ────────────────────────────────────────────
        if msg_type == MSG_TYPE_MCP_REQUEST:
            method = req.get("method", "")
            params = req.get("params", {})
            req_id = req.get("id", str(uuid.uuid4()))
            result = await self._route_mcp(method, params, req_id, node_id)
            await ws.send(json.dumps({
                "type": "mcp_response",
                "id": req_id,
                "result": result,
            }))
            return

        # ── 节点主动上报任务状态（回调云端）───────────────────────
        if msg_type == "task_callback":
            # 端侧完成任务/失败后回调云端
            await self._handle_task_callback(params=req.get("params", {}))
            return

        # ── 技能结果回调 ────────────────────────────────────────
        if msg_type == "skill_callback":
            await self.skill_domain.skill_result(req.get("params", {}))
            await ws.send(json.dumps({"type": "ack", "message": "callback recorded"}))
            return

        # ── 节点心跳 ────────────────────────────────────────────
        if msg_type == "node_heartbeat" and node_id:
            await self.node_domain.node_heartbeat({"node_id": node_id})
            return

        # ── 广播 ────────────────────────────────────────────────
        if msg_type == MSG_TYPE_BROADCAST:
            msg = req.get("message", {})
            await self._broadcast_to_nodes(msg, exclude_node=node_id)
            return

        await ws.send(json.dumps({"type": "echo", "data": req}))

    async def _route_mcp(self, method: str, params: dict, req_id: str,
                          node_id: Optional[str]) -> dict:
        """将 MCP 方法路由到对应 domain handler"""
        try:
            # ── 记忆域 + 知识域 ──────────────────────────────────
            if method.startswith(METHOD_PREFIX_MEMORY) or method.startswith(METHOD_PREFIX_KNOWLEDGE):
                handler = getattr(self.memory_domain, method, None)
                if handler:
                    result = await handler(params)
                    return {"jsonrpc": "2.0", "id": req_id, "result": result}

            # ── 任务域 ──────────────────────────────────────────
            if method.startswith(METHOD_PREFIX_KANBAN):
                handler = getattr(self.kanban_domain, method, None)
                if handler:
                    result = await handler(params)
                    # 调度模式下，需要推送任务给指定节点
                    if method == "kanban_task_assign":
                        target_node = params.get("node_id")
                        task_data = result.get("task", {})
                        await self._push_dispatch(target_node, {
                            "type": MSG_TYPE_TASK_EVENT,
                            "event": "task_assigned",
                            "task": task_data,
                        })
                    return {"jsonrpc": "2.0", "id": req_id, "result": result}

            # ── 技能域 ──────────────────────────────────────────
            if method.startswith(METHOD_PREFIX_SKILL):
                handler = getattr(self.skill_domain, method, None)
                if handler:
                    result = await handler(params)
                    # skill_invoke → 推送调度指令到目标节点
                    if method == "skill_invoke":
                        target = result.get("target_node")
                        if target:
                            await self._push_dispatch(target, {
                                "type": MSG_TYPE_DISPATCH,
                                "invoke_id": result.get("invoke_id"),
                                "skill_id": result.get("skill_id"),
                                "skill_name": result.get("skill_name"),
                                "input": result.get("input", {}),
                            })
                    return {"jsonrpc": "2.0", "id": req_id, "result": result}

            # ── 节点域 ──────────────────────────────────────────
            if method.startswith(METHOD_PREFIX_NODE):
                handler = getattr(self.node_domain, method, None)
                if handler:
                    result = await handler(params)
                    return {"jsonrpc": "2.0", "id": req_id, "result": result}

            # ── 存储域 ──────────────────────────────────────────
            if method.startswith(METHOD_PREFIX_VAULT):
                return await self._vault_handler(method, params, req_id)

            return {
                "jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32601, "message": f"Unknown method: {method}"}
            }

        except Exception as e:
            logger.error(f"MCP route error [{method}]: {e}")
            return {
                "jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32603, "message": str(e)}
            }

    async def _vault_handler(self, method: str, params: dict, req_id: str) -> dict:
        """vault_* 通过 OssStore 处理"""
        op = method.replace("vault_", "")
        try:
            if op == "list":
                result = await self.store.vault_list(**params)
            elif op == "upload":
                result = await self.store.vault_upload(**params)
            elif op == "download":
                result = await self.store.vault_download(**params)
            elif op == "delete":
                result = await self.store.vault_delete(**params)
            else:
                return {"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": -32601, "message": f"Unknown vault op: {op}"}}
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except Exception as e:
            return {"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32603, "message": str(e)}}

    async def _handle_task_callback(self, params: dict):
        """处理端侧的任务完成/失败回调"""
        callback_type = params.get("callback_type")
        task_id = params.get("task_id")
        node_id = params.get("node_id")

        if callback_type == "done":
            await self.kanban_domain.kanban_task_work_done({
                "task_id": task_id, "node_id": node_id, "result": params.get("result", "")
            })
        elif callback_type == "fail":
            await self.kanban_domain.kanban_task_work_fail({
                "task_id": task_id, "node_id": node_id, "error": params.get("error", "")
            })

    # ─── 推送给端侧节点 ─────────────────────────────────────────────────────

    async def _push_dispatch(self, node_id: str, message: dict):
        """向指定节点推送调度指令（云端主动）"""
        async with self._node_ws_lock:
            ws = self._node_ws.get(node_id)

        if ws is None:
            # 节点不在线，调度指令暂存到 OSS（节点下次上线时拉取）
            await self._queue_dispatch(node_id, message)
            logger.warning(f"Node {node_id} offline, dispatch queued")
            return

        try:
            await ws.send(json.dumps(message))
            logger.info(f"Dispatch pushed to node {node_id}: {message.get('type')}")
        except Exception as e:
            logger.error(f"Push to node {node_id} failed: {e}")
            await self._queue_dispatch(node_id, message)

    async def _queue_dispatch(self, node_id: str, message: dict):
        """离线节点的调度指令存入 OSS 队列"""
        import time
        queue_key = f"dispatch_queue/{node_id}/{int(time.time()*1000)}.json"
        await self.store.vault_upload(queue_key, json.dumps(message))

    async def _broadcast_to_nodes(self, message: dict, exclude_node: Optional[str] = None):
        """广播消息给所有在线节点"""
        async with self._node_ws_lock:
            targets = {
                nid: ws for nid, ws in self._node_ws.items()
                if nid != exclude_node
            }
        for nid, ws in targets.items():
            try:
                await ws.send(json.dumps(message))
            except Exception as e:
                logger.warning(f"Broadcast to {nid} failed: {e}")

    # ─── REST API ─────────────────────────────────────────────────────────────

    async def _handle_auth_token(self, request: web.Request) -> web.Response:
        """POST /api/v1/auth/token"""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        grant = body.get("grant_type", "password")
        if grant == "password":
            user_id = body.get("user_id") or body.get("username")
            if not user_id:
                return web.json_response({"error": "user_id required"}, status=400)
            at = create_token(user_id, self.jwt_secret, expires_hours=1)
            rt = create_refresh_token(user_id, self.jwt_secret, expires_days=7)
            return web.json_response({
                "access_token": at, "refresh_token": rt,
                "token_type": "Bearer", "expires_in": 3600,
            })
        elif grant == "refresh":
            rtk = body.get("refresh_token", "")
            payload = verify_token(rtk, self.jwt_secret)
            if not payload or payload.get("type") != "refresh":
                return web.json_response({"error": "Invalid refresh token"}, status=401)
            nat = create_token(payload["user_id"], self.jwt_secret, expires_hours=1)
            return web.json_response({"access_token": nat, "token_type": "Bearer", "expires_in": 3600})
        return web.json_response({"error": "Unsupported grant_type"}, status=400)

    async def _handle_status(self, request: web.Request) -> web.Response:
        """GET /api/v1/status"""
        nodes = self.node_domain.get_online_nodes()
        return web.json_response({
            "status": "ok",
            "version": "2.0",
            "user_id": self.user_id,
            "online_nodes": len(nodes),
            "nodes": [{"node_id": n["node_id"], "platform": n.get("platform"),
                       "status": n.get("status")} for n in nodes],
            "timestamp": time.time(),
        })

    async def _handle_health(self, request: web.Request) -> web.Response:
        """GET /health"""
        return web.Response(text="ok\n", content_type="text/plain")

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/api/v1/auth/token", self._handle_auth_token)
        app.router.add_get("/api/v1/status", self._handle_status)
        app.router.add_get("/health", self._handle_health)
        return app

    # ─── Server start ─────────────────────────────────────────────────────────

    async def start(self):
        """启动 WS server + REST API"""
        self._app = self._build_app()

        api_runner = web.AppRunner(self._app)
        await api_runner.setup()
        api_site = web.TCPSite(api_runner, "0.0.0.0", 8080)
        await api_site.start()
        logger.info("REST API listening on http://0.0.0.0:8080")

        self._ws_server = await websockets.serve(
            self.handle_client, "0.0.0.0", self.port
        )
        logger.info(f"WebSocket server listening on ws://0.0.0.0:{self.port}")
        await asyncio.Future()  # run forever


def main():
    jwt_secret = os.environ.get("JWT_SECRET", "dev-secret-change-me")
    user_id = os.environ.get("USER_ID", "default")
    port = int(os.environ.get("PORT", "8443"))

    hub = CloudHub(jwt_secret=jwt_secret, user_id=user_id, port=port)
    asyncio.run(hub.start())


if __name__ == "__main__":
    main()
