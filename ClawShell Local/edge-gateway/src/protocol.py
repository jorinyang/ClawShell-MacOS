"""
ClawShell Edge Protocol — 端侧协议实现
支持两种连接模式：
- node 模式：edge_register 协议（节点注册，主动接收云端调度）
- tool 模式：JWT auth 协议（工具调用，被动响应）

discover.sh 扫描结果通过 node 模式注册到云端。
"""
import asyncio
import json
import logging
import uuid
from typing import Callable, Optional

import websockets

logger = logging.getLogger("edge-protocol")

# ─── 消息类型 ────────────────────────────────────────────────────────────────
MSG_TYPE_AUTH          = "auth"
MSG_TYPE_AUTH_OK       = "auth_ok"
MSG_TYPE_MCP_REQUEST   = "mcp_request"
MSG_TYPE_PING          = "ping"
MSG_TYPE_PONG          = "pong"
MSG_TYPE_EDGE_REGISTER = "edge_register"
MSG_TYPE_EDGE_INFO     = "edge_info"
MSG_TYPE_DISPATCH      = "dispatch"
MSG_TYPE_TASK_EVENT    = "task_event"
MSG_TYPE_TASK_CALLBACK = "task_callback"
MSG_TYPE_SKILL_CALLBACK= "skill_callback"
NODE_STATUS_ONLINE     = "online"
NODE_STATUS_IDLE       = "idle"
NODE_STATUS_BUSY       = "busy"


class EdgeProtocol:
    """
    Edge 侧协议实现。
    支持两种连接模式：
      1. node 模式（edge_register）— 节点注册，接收云端主动推送
      2. tool 模式（JWT auth）— 工具调用，被动响应
    """

    def __init__(
        self,
        cloud_url: str,
        jwt_token: Optional[str] = None,
        node_id: Optional[str] = None,
        node_info: Optional[dict] = None,
        on_push: Optional[Callable] = None,
    ):
        """
        Args:
            cloud_url: 云端 WS 地址，如 wss://your-cloud.com/hub
            jwt_token: JWT 访问令牌（tool 模式）
            node_id: 端侧节点 ID（node 模式）
            node_info: 节点能力描述（node 模式）
            on_push: 收到云端推送时的回调函数
        """
        self.cloud_url = cloud_url
        self.jwt_token = jwt_token
        self.node_id = node_id or str(uuid.uuid4())
        self.node_info = node_info or {}
        self.on_push = on_push or (lambda msg: None)
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._pending: dict[str, asyncio.Future] = {}
        self._auth_mode: Optional[str] = None  # "node" or "tool"

    # ─── 连接与认证 ───────────────────────────────────────────────────────────

    async def connect(self):
        """建立 WS 连接"""
        self.ws = await websockets.connect(
            self.cloud_url,
            ping_interval=20,
            ping_timeout=10,
        )
        logger.info(f"Connected to {self.cloud_url}")

    async def authenticate(self):
        """认证：自动选择 node 模式或 tool 模式"""
        if self.ws is None:
            raise RuntimeError("Not connected")

        if self.node_info:
            # ── node 模式：edge_register ───────────────────────────────
            await self.ws.send(json.dumps({
                "type": MSG_TYPE_EDGE_REGISTER,
                "node_id": self.node_id,
                "node_info": self.node_info,
            }))
            resp = await asyncio.wait_for(self.ws.recv(), timeout=10)
            data = json.loads(resp)
            if data.get("type") != MSG_TYPE_EDGE_INFO:
                raise Exception(f"Expected edge_info, got: {data}")
            logger.info(f"Node registered: {data.get('node_id')}")
            self._auth_mode = "node"

        elif self.jwt_token:
            # ── tool 模式：JWT auth ───────────────────────────────────
            await self.ws.send(json.dumps({
                "type": MSG_TYPE_AUTH,
                "token": self.jwt_token,
                "platform": "edge-gateway",
                "version": "2.0",
            }))
            resp = await asyncio.wait_for(self.ws.recv(), timeout=10)
            data = json.loads(resp)
            if data.get("type") != MSG_TYPE_AUTH_OK:
                raise Exception(f"Auth failed: {data}")
            logger.info(f"Authenticated as user")
            self._auth_mode = "tool"

        else:
            raise ValueError("Must provide either node_info (node mode) or jwt_token (tool mode)")

    async def close(self):
        """关闭连接（node 模式会向云端注销）"""
        if self._auth_mode == "node" and self.ws:
            try:
                await self.ws.send(json.dumps({
                    "type": "node_unregister",
                    "node_id": self.node_id,
                }))
            except Exception:
                pass
        self._running = False
        if self.ws:
            await self.ws.close()
            self.ws = None

    # ─── 主动发送 ────────────────────────────────────────────────────────────

    async def mcp_request(self, method: str, params: dict = None) -> dict:
        """
        发送 MCP 请求，等待响应（tool 模式使用）。
        node 模式也可以用，但推荐用 handle_push 接收推送。
        """
        if self.ws is None:
            raise RuntimeError("Not connected")
        req_id = str(uuid.uuid4())
        future = asyncio.Future()
        self._pending[req_id] = future

        await self.ws.send(json.dumps({
            "type": MSG_TYPE_MCP_REQUEST,
            "id": req_id,
            "method": method,
            "params": params or {},
        }))

        try:
            result = await asyncio.wait_for(future, timeout=30)
            return result
        finally:
            self._pending.pop(req_id, None)

    async def send_heartbeat(self):
        """发送心跳（node 模式）"""
        if self.ws and self._auth_mode == "node":
            await self.ws.send(json.dumps({
                "type": "node_heartbeat",
                "node_id": self.node_id,
                "status": NODE_STATUS_ONLINE,
            }))

    async def task_callback(self, callback_type: str, task_id: str,
                             result: str = "", error: str = ""):
        """向云端报告任务执行结果"""
        if self.ws is None:
            raise RuntimeError("Not connected")
        await self.ws.send(json.dumps({
            "type": MSG_TYPE_TASK_CALLBACK,
            "callback_type": callback_type,
            "task_id": task_id,
            "node_id": self.node_id,
            "result": result,
            "error": error,
        }))

    async def skill_callback(self, invoke_id: str, success: bool,
                              output: dict = None, error: str = ""):
        """向云端报告技能执行结果"""
        if self.ws is None:
            raise RuntimeError("Not connected")
        await self.ws.send(json.dumps({
            "type": MSG_TYPE_SKILL_CALLBACK,
            "invoke_id": invoke_id,
            "node_id": self.node_id,
            "success": success,
            "output": output or {},
            "error": error,
        }))

    # ─── 监听循环 ────────────────────────────────────────────────────────────

    async def listen(self):
        """监听云端推送消息（node 模式主循环）"""
        if self.ws is None:
            raise RuntimeError("Not connected")
        self._running = True
        last_heartbeat = 0

        while self._running:
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=30)
                msg = json.loads(raw)
                await self._handle_message(msg)
                last_heartbeat += 30

                # 每 60s 发送一次心跳
                if last_heartbeat >= 60:
                    await self.send_heartbeat()
                    last_heartbeat = 0

            except asyncio.TimeoutError:
                await self.send_heartbeat()
                last_heartbeat = 0
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Connection closed")
                break

    async def _handle_message(self, msg: dict):
        """处理接收到的消息"""
        msg_type = msg.get("type", "")

        # ── 调度指令（云端 → 端侧）──────────────────────────────────
        if msg_type == MSG_TYPE_DISPATCH:
            logger.info(f"收到调度指令: skill={msg.get('skill_name')}, invoke_id={msg.get('invoke_id')}")
            # 实际执行由外部 handler 负责
            await self.on_push(msg)
            return

        # ── 任务事件 ──────────────────────────────────────────────
        if msg_type == MSG_TYPE_TASK_EVENT:
            event = msg.get("event", "")
            logger.info(f"任务事件: {event} — task_id={msg.get('task', {}).get('task_id')}")
            await self.on_push(msg)
            return

        # ── 心跳响应 ─────────────────────────────────────────────
        if msg_type == MSG_TYPE_PONG:
            return

        # ── MCP 响应 ─────────────────────────────────────────────
        if msg_type == "mcp_response":
            req_id = msg.get("id")
            if req_id in self._pending:
                self._pending[req_id].set_result(msg.get("result", {}))
            return

        # ── 其他 ─────────────────────────────────────────────────
        logger.debug(f"Received: {msg_type}")
