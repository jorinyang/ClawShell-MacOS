"""
ClawShell Edge Gateway — MCP over WebSocket Protocol
"""
import asyncio
import json
import logging
import uuid
from typing import Callable, Dict, Optional
import websockets
import jwt

logger = logging.getLogger("edge-gateway.protocol")


class EdgeProtocol:
    """WebSocket 客户端，负责 MCP over WS 协议实现"""

    def __init__(self, cloud_url: str, jwt_token: str, on_push: Callable):
        self.cloud_url = cloud_url
        self.jwt_token = jwt_token
        self.on_push = on_push
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.pending: Dict[str, asyncio.Future] = {}
        self.listen_task: Optional[asyncio.Task] = None
        self._connected = False

    async def connect(self):
        self.ws = await websockets.connect(self.cloud_url)
        self._connected = True
        logger.info(f"已连接到 {self.cloud_url}")

    async def authenticate(self):
        await self.ws.send(json.dumps({
            "type": "auth",
            "token": self.jwt_token
        }))
        resp = await asyncio.wait_for(self.ws.recv(), timeout=10)
        data = json.loads(resp)
        if data.get("type") == "auth_ok":
            logger.info("云端认证成功")
        else:
            raise Exception(f"认证失败: {data}")

    async def call_mcp(self, method: str, params: dict = None) -> dict:
        """调用云端 MCP 工具"""
        if not self.ws or not self._connected:
            raise Exception("未连接到云端")

        req_id = str(uuid.uuid4())
        request = {
            "type": "mcp_request",
            "id": req_id,
            "method": method,
            "params": params or {}
        }

        await self.ws.send(json.dumps(request))

        # Wait for response
        while True:
            resp = await self.ws.recv()
            data = json.loads(resp)
            if data.get("type") == "mcp_response" and data.get("id") == req_id:
                return data.get("result", {})

    async def listen(self):
        """Listen for push messages from cloud."""
        try:
            async for msg in self.ws:
                data = json.loads(msg)
                if data.get("type") == "mcp_push":
                    await self.on_push(data.get("params", {}))
                elif data.get("type") == "pong":
                    pass  # heartbeat ack
                else:
                    logger.debug(f"收到消息: {data.get('type')}")
        except Exception as e:
            logger.error(f"监听错误: {e}")
            self._connected = False
            raise

    async def close(self):
        self._connected = False
        if self.ws:
            await self.ws.close()
