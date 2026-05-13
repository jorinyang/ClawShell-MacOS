"""CloudHub MCP Server — Port 8081
Hermes 云端大脑通过此通道与 CloudHub 通信（主通道）

工具清单：
  cloudbrain.write      — Hermes 写入洞察/复盘/策略
  cloudbrain.read       — Hermes 读取最近的写回记录
  cloudbrain.status     — CloudHub 健康状态
  cloudbrain.event_subscribe — Hermes 订阅事件流（WS 升级）
"""
import asyncio
import json
import logging
import uuid
import time
from aiohttp import web, ClientSession, WSMsgType
from typing import Optional

logger = logging.getLogger("mcp-server")

# 全局引用（由 hub.py 注入）
_hub_instance = None
_handshake: Optional[object] = None


def init(cloudhub_instance, handshake_manager):
    global _hub_instance, _handshake
    _hub_instance = cloudhub_instance
    _handshake = handshake_manager
    logger.info("MCP Server initialized with CloudHub reference")


# ── HTTP Server（aiohttp）───────────────────────────────────────────────────


async def handle_write(request: web.Request) -> web.Response:
    """POST /cloudbrain/write — Hermes 写消息主入口"""
    try:
        message = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    message_id = message.get("message_id", "")
    seq = message.get("seq", 0)
    channel = message.get("channel", "mcp")
    msg_type = message.get("type", "")
    payload = message.get("payload", {})
    timestamp = message.get("timestamp", time.time())
    retry_count = message.get("retry_count", 0)

    if not message_id:
        return web.json_response({"error": "message_id required"}, status=400)

    # 调用握手管理器处理
    if _handshake is None:
        return web.json_response({"error": "HandshakeManager not initialized"}, status=500)

    ack = await _handshake.receive(
        message_id=message_id,
        seq=seq,
        channel=channel,
        msg_type=msg_type,
        payload=payload,
        timestamp=timestamp,
        retry_count=retry_count,
    )

    return web.json_response(ack)


async def handle_read(request: web.Request) -> web.Response:
    """GET /cloudbrain/read — Hermes 读取最近的写回记录"""
    limit = int(request.query.get("limit", 20))
    offset = int(request.query.get("offset", 0))

    # 从 Hub 读取最近的事件（如果 Hub 有 history）
    history = []
    if _hub_instance and hasattr(_hub_instance, "_cloudbrain_history"):
        history = _hub_instance._cloudbrain_history[offset:offset + limit]

    return web.json_response({
        "success": True,
        "count": len(history),
        "items": history,
    })


async def handle_status(request: web.Request) -> web.Response:
    """GET /health — 健康检查"""
    healthy = True
    detail = "ok"

    if _hub_instance is None:
        healthy = False
        detail = "CloudHub not initialized"
    elif not hasattr(_hub_instance, "running") or not _hub_instance.running:
        healthy = False
        detail = "CloudHub not running"

    status_code = 200 if healthy else 503
    return web.json_response({
        "status": "ok" if healthy else "error",
        "detail": detail,
        "uptime": getattr(_hub_instance, "_start_time", 0),
    }, status=status_code)


async def handle_tools_list(request: web.Request) -> web.Response:
    """GET /mcp/tools — 列出所有可用 MCP 工具"""
    tools = [
        {
            "name": "cloudbrain.write",
            "description": "Hermes 写入洞察/复盘/策略到 CloudHub",
            "input_schema": {
                "type": "object",
                "required": ["message_id", "seq", "type", "payload"],
                "properties": {
                    "message_id": {"type": "string", "description": "全局唯一消息ID"},
                    "seq": {"type": "integer", "description": "递增序号"},
                    "type": {"type": "string", "enum": ["insight_add", "review_publish", "strategy_update", "deep_think_result"]},
                    "payload": {"type": "object", "description": "消息负载"},
                    "timestamp": {"type": "number", "description": "Unix 时间戳"},
                    "retry_count": {"type": "integer", "description": "重试次数"},
                },
            },
        },
        {
            "name": "cloudbrain.read",
            "description": "Hermes 读取最近的写回记录",
            "input_schema": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 20},
                    "offset": {"type": "integer", "default": 0},
                },
            },
        },
        {
            "name": "cloudbrain.status",
            "description": "CloudHub 健康状态",
            "input_schema": {"type": "object", "properties": {}},
        },
    ]

    return web.json_response({"tools": tools})


async def handle_mcp_tool(request: web.Request) -> web.Response:
    """POST /mcp/<tool_name> — MCP 工具调用入口（统一格式）"""
    tool_name = request.match_info["tool_name"]
    try:
        message = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    message["channel"] = "mcp"

    if tool_name == "cloudbrain.write":
        return await handle_write(request)
    elif tool_name == "cloudbrain.read":
        return await handle_read(request)
    elif tool_name == "cloudbrain.status":
        return await handle_status(request)
    else:
        return web.json_response({"error": f"Unknown tool: {tool_name}"}, status=404)


# ── WS 事件订阅（Hermes 订阅 CloudHub 事件流）──────────────────────────────


_async_ws_subscribers: set = set()


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """GET /ws — Hermes WS 事件订阅（CloudHub → Hermes）"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session_id = str(uuid.uuid4())
    _async_ws_subscribers.add(ws)
    logger.info(f"WS subscriber connected: {session_id} (total={len(_async_ws_subscribers)})")

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Hermes 可以发送控制消息（ping / subscribe）
                try:
                    data = json.loads(msg.data)
                    cmd = data.get("cmd")
                    if cmd == "ping":
                        await ws.send_json({"cmd": "pong"})
                except Exception:
                    pass
            elif msg.type == WSMsgType.ERROR:
                logger.error(f"WS error: {ws.exception()}")
    finally:
        _async_ws_subscribers.discard(ws)
        logger.info(f"WS subscriber disconnected: {session_id} (total={len(_async_ws_subscribers)})")

    return ws


async def broadcast_event(event_type: str, payload: dict):
    """向所有 WS 订阅者广播事件（由 CloudHub 调用）"""
    if not _async_ws_subscribers:
        return

    message = {
        "type": "event",
        "event": event_type,
        "payload": payload,
        "timestamp": time.time(),
    }

    disconnected = set()
    for ws in _async_ws_subscribers:
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.add(ws)

    for ws in disconnected:
        _async_ws_subscribers.discard(ws)


# ── 服务器启动 ──────────────────────────────────────────────────────────────


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/cloudbrain/write", handle_write)
    app.router.add_get("/cloudbrain/read", handle_read)
    app.router.add_get("/health", handle_status)
    app.router.add_get("/mcp/tools", handle_tools_list)
    app.router.add_post("/mcp/{tool_name}", handle_mcp_tool)
    app.router.add_get("/ws", websocket_handler)
    return app


async def start(port: int = 8081):
    """启动 MCP Server"""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"MCP Server started on port {port}")
    return runner


if __name__ == "__main__":
    import asyncio
    from shared.handshake import HandshakeManager

    logging.basicConfig(level=logging.INFO)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    hs = HandshakeManager()
    loop.run_until_complete(start())
    loop.run_forever()
