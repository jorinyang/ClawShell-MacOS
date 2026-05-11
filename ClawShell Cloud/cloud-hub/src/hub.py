"""
ClawShell Cloud Hub — MCP over WebSocket Router
"""
import asyncio
import json
import logging
import uuid
from typing import Dict, Optional, Set
import websockets
from websockets.server import WebSocketServerProtocol
import jwt
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud-hub")


class CloudHub:
    def __init__(self, jwt_secret: str, user_id: str, port: int = 8443):
        self.jwt_secret = jwt_secret
        self.user_id = user_id
        self.port = port
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.mcp_servers = {
            "mempalace": os.environ.get("MEMPALACE_URL", "ws://mempalace:8444"),
            "skill":     os.environ.get("SKILL_URL", "ws://skill-registry:8445"),
            "kanban":    os.environ.get("KANBAN_URL", "ws://kanban:8446"),
        }

    async def register_client(self, ws: WebSocketServerProtocol, user_id: str):
        client_id = str(uuid.uuid4())
        self.clients[client_id] = ws
        logger.info(f"Client {client_id} ({user_id}) connected")
        return client_id

    async def unregister_client(self, client_id: str):
        if client_id in self.clients:
            del self.clients[client_id]
            logger.info(f"Client {client_id} disconnected")

    async def handle_client(self, ws: WebSocketServerProtocol):
        """Handle a client connection with JWT auth."""
        try:
            # Wait for auth frame
            auth_frame = await asyncio.wait_for(ws.recv(), timeout=10)
            auth_data = json.loads(auth_frame)

            if auth_data.get("type") != "auth":
                await ws.send(json.dumps({"error": "Expected auth frame first"}))
                await ws.close()
                return

            token = auth_data.get("token", "")
            try:
                payload = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
                user_id = payload.get("user_id")
                if user_id != self.user_id:
                    await ws.send(json.dumps({"error": "Invalid user"}))
                    await ws.close()
                    return
            except jwt.InvalidTokenError:
                await ws.send(json.dumps({"error": "Invalid token"}))
                await ws.close()
                return

            client_id = await self.register_client(ws, user_id)
            await ws.send(json.dumps({"type": "auth_ok", "client_id": client_id}))

        except asyncio.TimeoutError:
            logger.warning("Client auth timeout")
            await ws.close()
            return
        except Exception as e:
            logger.error(f"Auth error: {e}")
            await ws.close()
            return

        try:
            async for msg in ws:
                await self.handle_message(client_id, ws, msg)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.unregister_client(client_id)

    async def handle_message(self, client_id: str, ws: WebSocketServerProtocol, raw: str):
        """Route MCP request to appropriate server."""
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send(json.dumps({"error": "Invalid JSON"}))
            return

        # Handle ping/pong
        if req.get("type") == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        # MCP request routing
        if req.get("type") == "mcp_request":
            method = req.get("method", "")
            req_id = req.get("id", str(uuid.uuid4()))

            # Route by method prefix
            if method.startswith("mempalace_"):
                target_url = self.mcp_servers["mempalace"]
            elif method.startswith("skill_"):
                target_url = self.mcp_servers["skill"]
            elif method.startswith("kanban_"):
                target_url = self.mcp_servers["kanban"]
            else:
                await ws.send(json.dumps({
                    "type": "mcp_response", "id": req_id,
                    "error": f"Unknown method: {method}"
                }))
                return

            # Forward to MCP server
            try:
                async with websockets.connect(target_url) as mcp_ws:
                    await mcp_ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "method": method,
                        "params": req.get("params", {})
                    }))
                    response = await asyncio.wait_for(mcp_ws.recv(), timeout=30)
                    await ws.send(json.dumps({
                        "type": "mcp_response",
                        "id": req_id,
                        "result": json.loads(response)
                    }))
            except Exception as e:
                logger.error(f"MCP server error: {e}")
                await ws.send(json.dumps({
                    "type": "mcp_response", "id": req_id,
                    "error": str(e)
                }))

            return

        # Broadcast push messages to all connected clients
        if req.get("type") == "broadcast":
            msg = req.get("message", {})
            for cid, client_ws in list(self.clients.items()):
                if cid != client_id:
                    try:
                        await client_ws.send(json.dumps(msg))
                    except:
                        pass
            return

        # Default: echo back as pong
        await ws.send(json.dumps({"type": "echo", "data": req}))

    async def broadcast_skill_update(self, skill_id: str):
        """Called when a skill is updated — push to all clients."""
        for cid, ws in list(self.clients.items()):
            try:
                await ws.send(json.dumps({
                    "type": "skill_updated",
                    "skill_id": skill_id
                }))
            except:
                pass

    async def start(self):
        async with websockets.serve(self.handle_client, "0.0.0.0", self.port):
            logger.info(f"Cloud Hub listening on ws://0.0.0.0:{self.port}")
            await asyncio.Future()  # run forever


def main():
    jwt_secret = os.environ.get("JWT_SECRET", "changeme")
    user_id = os.environ.get("USER_ID", "default")
    port = int(os.environ.get("PORT", "8443"))

    hub = CloudHub(jwt_secret=jwt_secret, user_id=user_id, port=port)
    asyncio.run(hub.start())


if __name__ == "__main__":
    main()
