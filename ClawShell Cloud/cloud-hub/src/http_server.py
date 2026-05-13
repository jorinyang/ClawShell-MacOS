"""CloudHub HTTP 备用通道 — Port 8082
当 MCP Server 不可用时，Hermes 通过此通道写入数据

端点：
  POST /cloudbrain/write     — 写入洞察/复盘/策略（备用通道）
  GET  /cloudbrain/status   — 健康检查
"""
import asyncio
import json
import logging
import time
from aiohttp import web
from typing import Optional

logger = logging.getLogger("http-server")

_handshake: Optional[object] = None
_hub_instance = None


def init(cloudhub_instance, handshake_manager):
    global _hub_instance, _handshake
    _hub_instance = cloudhub_instance
    _handshake = handshake_manager
    logger.info("HTTP Server initialized")


async def handle_write(request: web.Request) -> web.Response:
    """POST /cloudbrain/write"""
    try:
        message = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    message_id = message.get("message_id", "")
    seq = message.get("seq", 0)
    msg_type = message.get("type", "")
    payload = message.get("payload", {})
    timestamp = message.get("timestamp", time.time())
    retry_count = message.get("retry_count", 0)

    if not message_id:
        return web.json_response({"error": "message_id required"}, status=400)

    if _handshake is None:
        return web.json_response({"error": "HandshakeManager not initialized"}, status=500)

    # 标记为 HTTP 通道
    ack = await _handshake.receive(
        message_id=message_id,
        seq=seq,
        channel="http",
        msg_type=msg_type,
        payload=payload,
        timestamp=timestamp,
        retry_count=retry_count,
    )

    return web.json_response(ack)


async def handle_status(request: web.Request) -> web.Response:
    """GET /cloudbrain/status"""
    healthy = True
    detail = "ok"

    if _hub_instance is None:
        healthy = False
        detail = "CloudHub not initialized"
    elif not hasattr(_hub_instance, "running") or not _hub_instance.running:
        healthy = False
        detail = "CloudHub not running"

    return web.json_response({
        "status": "ok" if healthy else "error",
        "detail": detail,
        "channel": "http",
    }, status=200 if healthy else 503)


async def handle_health(request: web.Request) -> web.Response:
    """GET /health"""
    return await handle_status(request)


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/cloudbrain/write", handle_write)
    app.router.add_get("/cloudbrain/status", handle_status)
    app.router.add_get("/health", handle_health)
    return app


async def start(port: int = 8082):
    """启动 HTTP 备用通道"""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"HTTP Server (backup) started on port {port}")
    return runner


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start())
    loop.run_forever()
