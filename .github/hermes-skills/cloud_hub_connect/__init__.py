"""CloudHub Connect — Hermes Skill 入口

订阅 CloudHub WS 事件流，触发 LLM 分析，双通道写回 CloudHub
"""
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Optional

logger = logging.getLogger("cloud-hub-connect")

# ── 双通道配置 ──────────────────────────────────────────────────────────────

MCP_URL = os.getenv("CLOUDHUB_MCP_URL", "http://localhost:8081")
HTTP_URL = os.getenv("CLOUDHUB_HTTP_URL", "http://localhost:8082/cloudbrain/write")
WS_URL = os.getenv("CLOUDHUB_WS_URL", "ws://localhost:8080/ws")

# LLM 配置
MINIMAX_KEY = os.getenv("MINIMAX_API_KEY", "")
DASHSCOPE_KEY = os.getenv("DASHSCOPE_API_KEY", "")

# ── Hermes Sender（双通道）───────────────────────────────────────────────────


class CloudBrainSender:
    """Hermes 侧双通道发送器"""

    def __init__(self):
        self._seq = 0
        self._lock = asyncio.Lock()
        self._dlq_path = Path(os.getenv("DLQ_PATH", "/opt/clawshell/data/dlq"))
        self._dlq_path.mkdir(parents=True, exist_ok=True)

    def next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    async def send(self, msg_type: str, payload: dict) -> dict:
        """发送消息：主 MCP → 备 HTTP → DLQ"""
        import aiohttp

        message_id = str(uuid.uuid4())
        seq = self.next_seq()
        timestamp = time.time()

        channels = [
            ("mcp", self._send_mcp),
            ("http", self._send_http),
        ]

        for channel_name, sender_fn in channels:
            for attempt in range(3):
                try:
                    resp = await sender_fn({
                        "message_id": message_id,
                        "seq": seq,
                        "channel": channel_name,
                        "type": msg_type,
                        "payload": payload,
                        "timestamp": timestamp,
                        "retry_count": attempt,
                    })
                    if resp and resp.get("status") in ("ok", "duplicate"):
                        logger.debug(f"[{channel_name}] {msg_type} sent, ack={resp.get('ack_seq')}")
                        return resp
                except Exception as e:
                    logger.warning(f"[{channel_name}] attempt {attempt+1} failed: {e}")
                    await asyncio.sleep(0.5 * (attempt + 1))

        # 全部失败 → DLQ
        await self._write_dlq({
            "message_id": message_id,
            "seq": seq,
            "type": msg_type,
            "payload": payload,
            "timestamp": timestamp,
        })
        return {"status": "dlq", "message_id": message_id}

    async def _send_mcp(self, message: dict) -> dict:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{MCP_URL}/cloudbrain/write",
                json=message,
                timeout=aiohttp.ClientTimeout(total=3.0),
            ) as resp:
                return await resp.json()

    async def _send_http(self, message: dict) -> dict:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(
                HTTP_URL,
                json=message,
                timeout=aiohttp.ClientTimeout(total=3.0),
            ) as resp:
                return await resp.json()

    async def _write_dlq(self, message: dict):
        dlq_file = self._dlq_path / f"{message['message_id']}.json"
        with open(dlq_file, "w") as f:
            json.dump(message, f, ensure_ascii=False)


sender = CloudBrainSender()


# ── LLM 调用 ─────────────────────────────────────────────────────────────────


async def llm_insight_fast(events: list[dict]) -> str:
    """MiniMax 快速分析：错误分类 + 根因简述"""
    if not MINIMAX_KEY:
        return "[MiniMax API Key not set]"

    import aiohttp
    prompt = (
        f"你是一个运维分析助手。根据以下错误事件，给出简洁的根因分析和严重程度。\n"
        f"格式：{{\"insight\": \"...\", \"severity\": \"high/medium/low\"}}\n\n"
        f"事件：{json.dumps(events, ensure_ascii=False, indent=2)}"
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.minimax.chat/v1/text/chatcompletion_v2",
                headers={
                    "Authorization": f"Bearer {MINIMAX_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "mini-max-02-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 256,
                },
                timeout=aiohttp.ClientTimeout(total=10.0),
            ) as resp:
                result = await resp.json()
                return result["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"MiniMax call failed: {e}")
        return f"[error: {e}]"


async def llm_review_deep(summary: dict) -> str:
    """Qwen-Max 深度复盘：生成结构化复盘报告"""
    if not DASHSCOPE_KEY:
        return "[DashScope API Key not set]"

    import aiohttp
    prompt = (
        f"你是一个项目复盘专家。根据以下数据，生成一份结构化复盘报告。\n"
        f"包括：成就、问题、根因分析、改进建议。\n\n"
        f"数据：{json.dumps(summary, ensure_ascii=False, indent=2)}"
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
                headers={
                    "Authorization": f"Bearer {DASHSCOPE_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "qwen-max",
                    "input": {"prompt": prompt},
                    "parameters": {"max_tokens": 1024},
                },
                timeout=aiohttp.ClientTimeout(total=15.0),
            ) as resp:
                result = await resp.json()
                return result["output"]["text"]
    except Exception as e:
        logger.error(f"DashScope call failed: {e}")
        return f"[error: {e}]"


# ── 事件处理 ─────────────────────────────────────────────────────────────────


async def on_error_event(event: dict):
    """处理 error.* 事件 → 触发快速洞察分析"""
    logger.info(f"Error event received: {event.get('type')}")

    events = [event]
    insight_text = await llm_insight_fast(events)

    await sender.send("insight_add", {
        "content": insight_text,
        "source": event.get("source", "unknown"),
        "event_type": event.get("type"),
        "severity": event.get("severity", "unknown"),
        "generated_at": time.time(),
    })


async def on_task_done(event: dict):
    """处理 task.done 事件 → 记录完成"""
    logger.info(f"Task done: {event.get('task_id')}")
    # 任务完成暂不做 LLM 分析，等周期性复盘


async def on_node_offline(event: dict):
    """处理 node.offline 事件 → 节点离线分析"""
    logger.info(f"Node offline: {event.get('node_id')}")
    await sender.send("insight_add", {
        "content": f"节点 {event.get('node_id')} 已离线",
        "source": "cloudhub",
        "event_type": "node.offline",
        "severity": "medium",
        "generated_at": time.time(),
    })


# ── WS 订阅循环 ──────────────────────────────────────────────────────────────


async def subscribe_loop():
    """WS 订阅 CloudHub 事件流（永续运行）"""
    import aiohttp
    from aiohttp import WSMsgType

    logger.info(f"Connecting to CloudHub WS: {WS_URL}")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL) as ws:
                    logger.info("WS connected to CloudHub")

                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                event_type = data.get("event", "")
                                payload = data.get("payload", {})

                                # 路由事件
                                if event_type.startswith("error."):
                                    await on_error_event({"type": event_type, **payload})
                                elif event_type == "task.done":
                                    await on_task_done(payload)
                                elif event_type == "node.offline":
                                    await on_node_offline(payload)

                            except Exception as e:
                                logger.error(f"Event handle error: {e}")

                        elif msg.type == WSMsgType.ERROR:
                            logger.error(f"WS error: {ws.exception()}")

        except Exception as e:
            logger.warning(f"WS disconnected: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)


# ── 定时任务 ─────────────────────────────────────────────────────────────────


async def daily_review():
    """每天 08:00：生成并发送每日复盘报告"""
    logger.info("Running daily review...")

    summary = {
        "date": time.strftime("%Y-%m-%d"),
        "tasks_completed": 0,      # TODO: 从 CloudHub 查询
        "tasks_failed": 0,
        "errors": [],
        "nodes": [],
    }

    report = await llm_review_deep(summary)
    await sender.send("review_publish", {
        "content": report,
        "date": summary["date"],
        "generated_at": time.time(),
    })
    logger.info(f"Daily review published: {len(report)} chars")


async def hourly_insight():
    """每小时：生成小时洞察摘要"""
    logger.info("Running hourly insight...")
    # TODO: 聚合本小时内的事件做摘要
    await sender.send("insight_add", {
        "content": "小时洞察：系统运行正常",
        "source": "cloudhub",
        "event_type": "hourly_summary",
        "severity": "low",
        "generated_at": time.time(),
    })


# ── Skill 入口 ───────────────────────────────────────────────────────────────


async def run():
    """Skill 主入口：同时运行 WS 订阅 + 定时任务"""
    await asyncio.gather(
        subscribe_loop(),
        # TODO: cron 调度
    )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()

from pathlib import Path
