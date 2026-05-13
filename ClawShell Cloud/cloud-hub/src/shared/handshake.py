"""Handshake Protocol — CloudHub ↔ Hermes 双通道握手协议

消息格式：
{
    "message_id": "uuid-v4",      # 全局唯一
    "seq": 1001,                  # 递增序号
    "channel": "mcp",            # "mcp" | "http"
    "type": "insight_add",        # 消息类型
    "payload": {...},             # 实际数据
    "timestamp": 1718000000,     # Unix 时间戳
    "retry_count": 0             # 重试次数
}

ACK 格式：
{
    "message_id": "uuid-v4",
    "ack_seq": 1001,
    "status": "ok",              # "ok" | "error" | "duplicate"
    "channel": "mcp",
    "detail": ""                 # 错误时详细信息
}
"""
import asyncio
import json
import uuid
import time
import logging
from pathlib import Path
from typing import Optional, Callable, Awaitable
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("handshake")


class Channel(Enum):
    MCP = "mcp"
    HTTP = "http"


class MessageStatus(Enum):
    OK = "ok"
    ERROR = "error"
    DUPLICATE = "duplicate"


@dataclass
class OutgoingMessage:
    message_id: str
    seq: int
    channel: Channel
    payload: dict
    timestamp: float
    retry_count: int = 0
    future: asyncio.Future = field(default_factory=asyncio.Future)
    created_at: float = field(default_factory=time.time)


class HandshakeManager:
    """CloudHub 侧握手管理器"""

    def __init__(self, dlq_path: str = "/opt/clawshell/data/dlq"):
        # 已处理的 message_id（去重）
        self._processed_ids: set[str] = set()
        # 待确认的消息 {message_id: OutgoingMessage}
        self._pending: dict[str, OutgoingMessage] = {}
        # seq 计数器
        self._seq: int = 0
        # 锁
        self._lock = asyncio.Lock()
        # DLQ 路径
        self._dlq_path = Path(dlq_path)
        self._dlq_path.mkdir(parents=True, exist_ok=True)
        # 清理 old ACK 线程
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """启动清理任务"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("HandshakeManager started")

    async def stop(self):
        """停止清理任务"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        logger.info("HandshakeManager stopped")

    # ── Seq 管理 ──────────────────────────────────────────────────────────────

    def next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    # ── 接收 Hermes 消息（CloudHub 侧）─────────────────────────────────────

    async def receive(
        self,
        message_id: str,
        seq: int,
        channel: str,
        msg_type: str,
        payload: dict,
        timestamp: float,
        retry_count: int = 0,
    ) -> dict:
        """处理 Hermes 发来的消息，返回 ACK"""
        ack = {
            "message_id": message_id,
            "ack_seq": seq,
            "status": "ok",
            "channel": channel,
            "detail": "",
        }

        async with self._lock:
            # 去重检查
            if message_id in self._processed_ids:
                ack["status"] = "duplicate"
                logger.debug(f"Duplicate message: {message_id}")
                return ack

            # 存入已处理
            self._processed_ids.add(message_id)

        try:
            # 路由到对应 handler
            result = await self._route_message(msg_type, payload)
            ack["status"] = "ok"
            logger.debug(f"Routed message {message_id} type={msg_type} -> ok")
            return ack

        except Exception as e:
            ack["status"] = "error"
            ack["detail"] = str(e)
            logger.error(f"Route message {message_id} failed: {e}")
            return ack

    def _pending_add(self, msg: OutgoingMessage):
        """登记待确认消息（CloudHub 发给 Hermes 的消息）"""
        with self._lock:
            self._pending[msg.message_id] = msg

    def _pending_remove(self, message_id: str):
        """移除已确认消息"""
        with self._lock:
            self._pending.pop(message_id, None)

    def _pending_get(self, message_id: str) -> Optional[OutgoingMessage]:
        return self._pending.get(message_id)

    # ── Hermes 发给 CloudHub 的消息路由 ──────────────────────────────────

    async def _route_message(self, msg_type: str, payload: dict) -> None:
        """根据消息类型路由到对应 Domain"""
        from ..hub import CloudHub  # 延迟导入避免循环

        # 这里需要 CloudHub 实例来路由，实际由 MCP/HTTP Server 传入 handler
        # CloudHub._route_cloudbrain() 会调用此方法
        pass

    # ── DLQ ────────────────────────────────────────────────────────────────

    async def dlq_write(self, message: dict) -> str:
        """消息写入 DLQ"""
        message_id = message.get("message_id", str(uuid.uuid4()))
        dlq_file = self._dlq_path / f"{message_id}.json"

        async with self._lock:
            with open(dlq_file, "w") as f:
                json.dump(message, f, ensure_ascii=False, indent=2)

        logger.warning(f"Message written to DLQ: {message_id}")
        return message_id

    async def dlq_retry(self, message: dict, handler: Callable) -> bool:
        """重试 DLQ 消息"""
        try:
            await handler(message)
            return True
        except Exception as e:
            logger.error(f"DLQ retry failed for {message.get('message_id')}: {e}")
            return False

    async def dlq_load_pending(self) -> list[dict]:
        """加载所有待重试的 DLQ 消息"""
        messages = []
        for f in self._dlq_path.glob("*.json"):
            try:
                with open(f) as fp:
                    messages.append(json.load(fp))
            except Exception:
                pass
        return messages

    async def dlq_clear(self, message_id: str):
        """清除已处理的 DLQ 消息"""
        dlq_file = self._dlq_path / f"{message_id}.json"
        if dlq_file.exists():
            dlq_file.unlink()

    # ── 清理过期已处理 ID ────────────────────────────────────────────────

    async def _cleanup_loop(self, interval: int = 300):
        """每 5 分钟清理一次已处理的 message_id，防止内存无限增长"""
        while True:
            await asyncio.sleep(interval)
            try:
                size_before = len(self._processed_ids)
                # 保留最近 10000 条
                if len(self._processed_ids) > 10000:
                    # 简单策略：保留一半
                    to_remove = list(self._processed_ids)[: len(self._processed_ids) // 2]
                    for mid in to_remove:
                        self._processed_ids.discard(mid)
                    logger.info(f"Cleaned {size_before - len(self._processed_ids)} processed IDs")
            except Exception as e:
                logger.error(f"Cleanup error: {e}")


# ── Hermes 侧发送器 ────────────────────────────────────────────────────────


class HermesSender:
    """Hermes 侧消息发送器（主通道 MCP / 备通道 HTTP）"""

    def __init__(self, mcp_client, http_url: str = "http://localhost:8082/cloudbrain/write"):
        self.mcp_client = mcp_client          # MCP 客户端实例
        self.http_url = http_url              # HTTP 备用通道
        self._seq: int = 0
        self._lock = asyncio.Lock()
        self._pending: dict[str, OutgoingMessage] = {}
        self._retry_count: dict[str, int] = defaultdict(int)
        self._dlq_path: Optional[Path] = None

    def set_dlq_path(self, path: str):
        self._dlq_path = Path(path)
        self._dlq_path.mkdir(parents=True, exist_ok=True)

    def next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    async def send(
        self,
        msg_type: str,
        payload: dict,
        mcp_tool: str = "cloudbrain.write",
        http_client = None,
        timeout: float = 3.0,
        max_retries: int = 3,
    ) -> dict:
        """发送消息，自动主通道 MCP → 降级 HTTP → DLQ"""
        import aiohttp

        message_id = str(uuid.uuid4())
        seq = self.next_seq()
        timestamp = time.time()

        message = {
            "message_id": message_id,
            "seq": seq,
            "channel": "mcp",
            "type": msg_type,
            "payload": payload,
            "timestamp": timestamp,
            "retry_count": self._retry_count[message_id],
        }

        channels = [
            ("mcp", lambda: self._send_mcp(message, mcp_tool, timeout)),
            ("http", lambda: self._send_http(message, http_client or aiohttp, timeout)),
        ]

        for channel_name, sender_fn in channels:
            for attempt in range(max_retries):
                try:
                    response = await sender_fn()
                    if response and response.get("status") in ("ok", "duplicate"):
                        # 写成功，记录 ACK
                        logger.debug(f"Send via {channel_name}: ok (seq={seq})")
                        return response
                    else:
                        logger.warning(f"{channel_name} returned {response}")
                except Exception as e:
                    logger.warning(f"{channel_name} attempt {attempt + 1} failed: {e}")

            # 通道全部尝试失败，记录
            logger.warning(f"All attempts on {channel_name} failed for {message_id}")

        # 全部失败 → DLQ
        await self._write_dlq(message)
        return {"message_id": message_id, "status": "dlq", "detail": "All channels failed"}

    async def _send_mcp(self, message: dict, tool: str, timeout: float) -> dict:
        """主通道：MCP"""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            # MCP over HTTP：模拟 MCP 协议
            async with session.post(
                f"http://localhost:8081/mcp/{tool}",
                json=message,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                return await resp.json()

    async def _send_http(self, message: dict, http_module, timeout: float) -> dict:
        """备用通道：HTTP"""
        # message["channel"] = "http"  # 已自动
        async with http_module.ClientSession() as session:
            async with session.post(
                self.http_url,
                json=message,
                timeout=http_module.ClientTimeout(total=timeout),
            ) as resp:
                return await resp.json()

    async def _write_dlq(self, message: dict):
        """写入 DLQ"""
        if self._dlq_path:
            message_id = message.get("message_id", "unknown")
            dlq_file = self._dlq_path / f"{message_id}.json"
            with open(dlq_file, "w") as f:
                json.dump(message, f, ensure_ascii=False, indent=2)
            logger.warning(f"Message {message_id} written to DLQ")
