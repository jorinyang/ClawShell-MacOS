"""
ClawShell Cloud Hub — MCP over WebSocket Router + REST API
"""
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Optional, Dict, Any
from aiohttp import web
import jwt
import boto3
from botocore.exceptions import ClientError
import websockets
from websockets.server import WebSocketServerProtocol

from .router import Router
from .registry import ClientRegistry
from .auth import create_token, create_refresh_token, verify_token

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud-hub")


# ─────────────────────────────────────────────────────────────────
# Vault OSS Handler
# ─────────────────────────────────────────────────────────────────

class VaultHandler:
    """Handles vault_* methods via boto3 S3-compatible OSS API."""

    def __init__(self, oss_config: dict):
        self.cfg = oss_config
        self._client: Optional[Any] = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.cfg.get("endpoint") or None,
                aws_access_key_id=self.cfg.get("access_key"),
                aws_secret_access_key=self.cfg.get("secret_key"),
                region_name="auto",
            )
        return self._client

    def _vault_path(self, key: str) -> str:
        """Prepend vault prefix to a key."""
        prefix = self.cfg.get("vault_prefix", "vault/")
        return f"{prefix}{key}".lstrip("/")

    async def list(self, method: str, params: dict, req_id: str) -> dict:
        """vault_list — list objects in the vault."""
        try:
            prefix = params.get("prefix", "")
            limit = params.get("limit", 100)
            response = self.client.list_objects_v2(
                Bucket=self.cfg["bucket"],
                Prefix=self._vault_path(prefix),
                MaxKeys=limit,
            )
            contents = response.get("Contents", [])
            return {
                "objects": [
                    {"key": obj["Key"][len(self.cfg["vault_prefix"]):], "size": obj["Size"], "modified": obj["LastModified"].isoformat()}
                    for obj in contents
                ],
                "is_truncated": response.get("IsTruncated", False),
            }
        except ClientError as e:
            raise Exception(f"OSS list error: {e}")

    async def upload(self, method: str, params: dict, req_id: str) -> dict:
        """vault_upload — upload a file to the vault."""
        key = params.get("key", "")
        content = params.get("content", "")
        if not key:
            raise ValueError("key is required")
        try:
            self.client.put_object(
                Bucket=self.cfg["bucket"],
                Key=self._vault_path(key),
                Body=content.encode("utf-8") if isinstance(content, str) else content,
            )
            return {"success": True, "key": key}
        except ClientError as e:
            raise Exception(f"OSS upload error: {e}")

    async def download(self, method: str, params: dict, req_id: str) -> dict:
        """vault_download — download a file from the vault."""
        key = params.get("key", "")
        if not key:
            raise ValueError("key is required")
        try:
            response = self.client.get_object(
                Bucket=self.cfg["bucket"],
                Key=self._vault_path(key),
            )
            body = response["Body"].read()
            # Try utf-8 decode, fall back to base64
            try:
                content = body.decode("utf-8")
            except UnicodeDecodeError:
                import base64
                content = base64.b64encode(body).decode()
            return {"key": key, "content": content}
        except ClientError as e:
            raise Exception(f"OSS download error: {e}")

    async def delete(self, method: str, params: dict, req_id: str) -> dict:
        """vault_delete — delete a file from the vault."""
        key = params.get("key", "")
        if not key:
            raise ValueError("key is required")
        try:
            self.client.delete_object(Bucket=self.cfg["bucket"], Key=self._vault_path(key))
            return {"success": True, "key": key}
        except ClientError as e:
            raise Exception(f"OSS delete error: {e}")


# ─────────────────────────────────────────────────────────────────
# Cloud Hub Core
# ─────────────────────────────────────────────────────────────────

class CloudHub:
    def __init__(self, jwt_secret: str, user_id: str, port: int = 8443):
        self.jwt_secret = jwt_secret
        self.user_id = user_id
        self.port = port
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.registry = ClientRegistry()
        self.router = Router()

        # OSS config
        self.oss_config = {
            "endpoint": os.environ.get("OSS_ENDPOINT", ""),
            "bucket": os.environ.get("OSS_BUCKET", ""),
            "access_key": os.environ.get("OSS_ACCESS_KEY_ID", ""),
            "secret_key": os.environ.get("OSS_ACCESS_KEY_SECRET", ""),
            "vault_prefix": os.environ.get("OSS_VAULT_PREFIX", "vault/"),
        }
        self.vault_handler = VaultHandler(self.oss_config)

        # MCP server targets
        self.mcp_servers = {
            "skill": os.environ.get("SKILL_URL", "ws://skill-registry:8445"),
            "kanban": os.environ.get("KANBAN_URL", "ws://kanban:8446"),
        }

        self._ws_server: Optional[Any] = None
        self._app: Optional[web.Application] = None

        # Register vault handlers
        self.router.register_handler("vault_list", self._vault_handler_wrapper("list"))
        self.router.register_handler("vault_upload", self._vault_handler_wrapper("upload"))
        self.router.register_handler("vault_download", self._vault_handler_wrapper("download"))
        self.router.register_handler("vault_delete", self._vault_handler_wrapper("delete"))

    def _vault_handler_wrapper(self, op: str):
        async def wrapper(method, params, req_id):
            return await getattr(self.vault_handler, op)(method, params, req_id)
        return wrapper

    # ─── WebSocket client handling ───────────────────────────────

    async def handle_client(self, ws: WebSocketServerProtocol):
        """Handle a client WebSocket connection with JWT auth."""
        client_id = None
        try:
            # Wait for auth frame
            auth_frame = await asyncio.wait_for(ws.recv(), timeout=10)
            auth_data = json.loads(auth_frame)

            if auth_data.get("type") != "auth":
                await ws.send(json.dumps({"error": "Expected auth frame first"}))
                await ws.close()
                return

            token = auth_data.get("token", "")
            payload = verify_token(token, self.jwt_secret)
            if payload is None:
                await ws.send(json.dumps({"error": "Invalid token"}))
                await ws.close()
                return

            user_id = payload.get("user_id")
            if user_id != self.user_id:
                await ws.send(json.dumps({"error": "Invalid user"}))
                await ws.close()
                return

            client_id = await self.registry.register(
                ws, user_id,
                platform=auth_data.get("platform"),
                version=auth_data.get("version"),
            )
            await ws.send(json.dumps({"type": "auth_ok", "client_id": client_id}))
            logger.info(f"Client {client_id} authenticated (user={user_id})")

        except asyncio.TimeoutError:
            logger.warning("Client auth timeout")
            return
        except Exception as e:
            logger.error(f"Auth error: {e}")
            return

        try:
            async for msg in ws:
                await self._handle_ws_message(client_id, ws, msg)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if client_id:
                await self.registry.unregister(client_id)

    async def _handle_ws_message(self, client_id: str, ws: WebSocketServerProtocol, raw: str):
        """Route incoming WebSocket messages."""
        await self.registry.update_activity(client_id)
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            await ws.send(json.dumps({"error": "Invalid JSON"}))
            return

        # Ping/pong
        if req.get("type") == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        # MCP request
        if req.get("type") == "mcp_request":
            method = req.get("method", "")
            req_id = req.get("id", str(uuid.uuid4()))
            params = req.get("params", {})

            # Route vault_* through local handler (no WS hop)
            if method.startswith("vault_"):
                result = await self.router.route(method, params, req_id)
                await ws.send(json.dumps({"type": "mcp_response", "id": req_id, "result": result}))
                return

            # Route skill_* / kanban_* through MCP servers
            target_url = None
            if method.startswith("skill_"):
                target_url = self.mcp_servers["skill"]
            elif method.startswith("kanban_"):
                target_url = self.mcp_servers["kanban"]
            else:
                await ws.send(json.dumps({
                    "type": "mcp_response", "id": req_id,
                    "error": {"code": -32601, "message": f"Unknown method: {method}"}
                }))
                return

            try:
                async with websockets.connect(target_url) as mcp_ws:
                    await mcp_ws.send(json.dumps({
                        "jsonrpc": "2.0", "id": req_id,
                        "method": method, "params": params
                    }))
                    response = await asyncio.wait_for(mcp_ws.recv(), timeout=30)
                    await ws.send(json.dumps({
                        "type": "mcp_response", "id": req_id,
                        "result": json.loads(response)
                    }))
            except Exception as e:
                logger.error(f"MCP server error: {e}")
                await ws.send(json.dumps({
                    "type": "mcp_response", "id": req_id,
                    "error": {"code": -32603, "message": str(e)}
                }))
            return

        # Broadcast
        if req.get("type") == "broadcast":
            msg = req.get("message", {})
            await self.registry.broadcast(msg, exclude_client_id=client_id)
            return

        await ws.send(json.dumps({"type": "echo", "data": req}))

    # ─── REST API ───────────────────────────────────────────────

    async def _handle_auth_token(self, request: web.Request) -> web.Response:
        """POST /api/v1/auth/token — issue JWT access token."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        grant_type = body.get("grant_type", "password")
        if grant_type == "password":
            user_id = body.get("user_id") or body.get("username")
            password = body.get("password")
            # Simple user_id==password check for local dev; replace with real auth
            if not user_id:
                return web.json_response({"error": "user_id required"}, status=400)
            # In dev mode: any non-empty password is accepted
            access_token = create_token(user_id, self.jwt_secret, expires_hours=1)
            refresh_token = create_refresh_token(user_id, self.jwt_secret, expires_days=7)
            return web.json_response({
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_in": 3600,
            })
        elif grant_type == "refresh":
            refresh_tkn = body.get("refresh_token", "")
            payload = verify_token(refresh_tkn, self.jwt_secret)
            if payload is None or payload.get("type") != "refresh":
                return web.json_response({"error": "Invalid refresh token"}, status=401)
            new_access = create_token(payload["user_id"], self.jwt_secret, expires_hours=1)
            return web.json_response({"access_token": new_access, "token_type": "Bearer", "expires_in": 3600})
        else:
            return web.json_response({"error": "Unsupported grant_type"}, status=400)

    async def _handle_status(self, request: web.Request) -> web.Response:
        """GET /api/v1/status — cloud hub status."""
        count = await self.registry.client_count()
        return web.json_response({
            "status": "ok",
            "version": "1.0.0",
            "connected_clients": count,
            "user_id": self.user_id,
            "timestamp": time.time(),
        })

    async def _handle_sync_push(self, request: web.Request) -> web.Response:
        """POST /api/v1/sync/push — receive offline operations from edge."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        operations = body.get("operations", [])
        # For now, just log and return success
        for op in operations:
            logger.info(f"Sync push: {op.get('type')} op {op.get('id')}")
        return web.json_response({"received": len(operations), "status": "ok"})

    async def _handle_sync_pull(self, request: web.Request) -> web.Response:
        """GET /api/v1/sync/pull/:since — delta pull since timestamp."""
        since = request.match_info.get("since", "0")
        try:
            since_ts = float(since)
        except ValueError:
            return web.json_response({"error": "Invalid since timestamp"}, status=400)

        # TODO: query MCP servers for changes since since_ts
        return web.json_response({"changes": [], "timestamp": time.time(), "synced": True})

    async def _handle_health(self, request: web.Request) -> web.Response:
        """GET /health — liveness probe."""
        return web.Response(text="ok\n", content_type="text/plain")

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/api/v1/auth/token", self._handle_auth_token)
        app.router.add_get("/api/v1/status", self._handle_status)
        app.router.add_post("/api/v1/sync/push", self._handle_sync_push)
        app.router.add_get("/api/v1/sync/pull/{since}", self._handle_sync_pull)
        app.router.add_get("/health", self._handle_health)
        return app

    # ─── Server start ───────────────────────────────────────────

    async def start(self):
        """Start both WebSocket server and REST API."""
        self._app = self._build_app()

        # REST API runner
        api_runner = web.AppRunner(self._app)
        await api_runner.setup()
        api_site = web.TCPSite(api_runner, "0.0.0.0", 8080)
        await api_site.start()
        logger.info("REST API listening on http://0.0.0.0:8080")

        # WebSocket server
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
