"""
ClawShell Edge Gateway — 云端协同端侧入口
维护与 Cloud Hub 的 WebSocket 长连接
"""
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from protocol import EdgeProtocol
from sync_engine import SyncEngine

# 目录初始化（必须在 logging 之前）
CONFIG_PATH = Path.home() / ".clawshell-local" / "config" / "cloud.json"
STATE_DIR = Path.home() / ".clawshell-local" / "state"
LOGS_DIR = Path.home() / ".clawshell-local" / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "gateway.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("edge-gateway")


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        logger.error(f"配置文件不存在: {CONFIG_PATH}")
        logger.info("请先运行 install.sh 完成配置")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        return json.load(f)


class EdgeGateway:
    def __init__(self, config: dict):
        self.cloud_url = config["cloud_url"]
        self.jwt_token = config["jwt_token"]
        self.sync_interval = config.get("sync_interval_seconds", 60)
        self.protocol = EdgeProtocol(
            cloud_url=self.cloud_url,
            jwt_token=self.jwt_token,
            on_push=self.handle_push
        )
        self.sync_engine = SyncEngine(
            protocol=self.protocol,
            cache_dir=Path.home() / ".clawshell-local" / "cache",
            sync_dir=Path.home() / ".clawshell-local" / "sync"
        )
        self.running = False

    async def handle_push(self, message: dict):
        """Handle push notifications from cloud."""
        msg_type = message.get("type", "")
        if msg_type == "skill_updated":
            logger.info(f"技能更新通知: {message.get('skill_id')}")
            await self.sync_engine.pull_skills()
        elif msg_type == "memory_updated":
            logger.info("记忆更新通知，触发增量同步")
            await self.sync_engine.pull_memory()
        else:
            logger.debug(f"收到推送: {msg_type}")

    async def connect_loop(self):
        """Maintain connection with automatic reconnection."""
        while self.running:
            try:
                await self.protocol.connect()
                await self.protocol.authenticate()
                await self.sync_engine.full_sync()
                await self.protocol.listen()
            except Exception as e:
                logger.error(f"连接错误: {e}")
                await asyncio.sleep(5)

    async def start(self):
        self.running = True
        logger.info(f"Edge Gateway 启动")
        logger.info(f"  Cloud: {self.cloud_url}")
        logger.info(f"  Sync: 每 {self.sync_interval}s")
        await self.connect_loop()

    async def stop(self):
        self.running = False
        await self.protocol.close()


def main():
    config = load_config()
    gateway = EdgeGateway(config)

    import signal
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("收到停止信号")
        asyncio.create_task(gateway.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        loop.run_until_complete(gateway.start())
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Edge Gateway 已停止")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        pid_file = STATE_DIR / "gateway.pid"
        if pid_file.exists():
            pid = pid_file.read_text().strip()
            print(f"Edge Gateway PID: {pid}")
        else:
            print("Edge Gateway 未运行")
    else:
        main()
