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
from eventbus import EventBus, Event, EventType, configure_eventbus
from edge_self_healing import EdgeSelfHealing
from network_discovery import NetworkDiscovery
from device_monitor import DeviceMonitor
from knowledge_puller import KnowledgePuller
from ide_bridge import IDEOrchestrator, detect_ide_tools
from platform_detectors import DetectorManager, get_detector_manager, detect_all_platforms

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
        self.eventbus = EventBus(sync_engine=self.sync_engine)
        self.eventbus.start_async_processing()
        configure_eventbus(self.sync_engine)

        # Edge SelfHealing（心跳驱动，无 cron）
        self.self_healing = EdgeSelfHealing(eventbus=self.eventbus)
        self.self_healing.subscribe_to_events()
        self.self_healing.start()

        # D1: Network Discovery（网络发现）
        self.network_discovery = NetworkDiscovery(
            eventbus=self.eventbus,
            sync_engine=self.sync_engine,
        )
        self.network_discovery.start()

        # D2: Device Monitor（设备监控）
        self.device_monitor = DeviceMonitor(
            eventbus=self.eventbus,
            sync_engine=self.sync_engine,
        )
        self.device_monitor.start()

        # D3: Knowledge Puller（知识拉取）
        self.knowledge_puller = KnowledgePuller(
            eventbus=self.eventbus,
            sync_engine=self.sync_engine,
            protocol=self.protocol,
        )
        self.knowledge_puller.start()

        # IDE Bridge Orchestrator (Harness Engineering)
        self.ide_orchestrator = IDEOrchestrator()
        available_ides = detect_ide_tools()
        if available_ides:
            logger.info(f"检测到可用 IDE 工具: {', '.join(available_ides)}")

        # Platform Detectors (P0-Detector)
        self.detector_manager = DetectorManager()

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
        """Maintain connection with automatic reconnection (inspired by ClawShell-Deep GanglionTransport).

        重连策略（借鉴 Deep）：
        - 指数退避：初始 2s，最大 60s，抖动 ±1s
        - 区分主动关闭（graceful）vs 意外断开（需重连）
        - reconnect 后 replay 补齐离线期间的事件
        """
        BACKOFF_INIT = 2
        BACKOFF_MAX = 60
        backoff = BACKOFF_INIT
        was_connected = False

        while self.running:
            try:
                await self.protocol.connect()
                await self.protocol.authenticate()
                await self.sync_engine.full_sync()

                # Replay 离线期间积压的事件
                queued = self.sync_engine.get_queued_events()
                if queued:
                    logger.info(f"Replaying {len(queued)} queued events after reconnect")
                    for ev in queued:
                        try:
                            await self.protocol.mcp_request("sync.replay_event", {"event": ev})
                        except Exception:
                            pass
                    self.sync_engine.clear_replay_queue()

                was_connected = True
                backoff = BACKOFF_INIT
                logger.info("Connected and synced, entering listen loop")
                await self.protocol.listen()

            except asyncio.CancelledError:
                logger.info("connect_loop cancelled (shutdown)")
                break

            except websockets.exceptions.ConnectionClosed as e:
                # 区分主动关闭（code=1000）vs 意外断开
                if e.code == 1000:
                    logger.info("Connection gracefully closed by server")
                    break
                logger.warning(f"Connection closed unexpectedly (code={e.code}), reconnecting...")
                if was_connected:
                    logger.info("Reconnecting after disconnect...")

            except Exception as e:
                logger.warning(f"Connection error: {e}")

            if not self.running:
                break

            # 指数退避（借鉴 Deep 重连逻辑）
            jitter = __import__("random").uniform(-1, 1)
            wait = min(backoff + jitter, BACKOFF_MAX)
            logger.debug(f"Reconnecting in {wait:.1f}s (backoff={backoff}s)")
            await asyncio.sleep(wait)
            backoff = min(backoff * 1.5, BACKOFF_MAX)

    async def start(self):
        self.running = True
        logger.info(f"Edge Gateway 启动")
        logger.info(f"  Cloud: {self.cloud_url}")
        logger.info(f"  Sync: 每 {self.sync_interval}s")

        # 初始化平台检测器
        await self.detector_manager.initialize()
        available_platforms = self.detector_manager.get_available_platforms()
        logger.info(f"检测到 {len(available_platforms)} 个可用平台: {', '.join(available_platforms) if available_platforms else '无'}")

        await self.connect_loop()

    async def stop(self):
        self.running = False
        if hasattr(self, 'self_healing'):
            self.self_healing.stop()
        if hasattr(self, 'network_discovery'):
            self.network_discovery.stop()
        if hasattr(self, 'device_monitor'):
            self.device_monitor.stop()
        if hasattr(self, 'knowledge_puller'):
            self.knowledge_puller.stop()
        if hasattr(self, 'ide_orchestrator'):
            logger.info("IDE Bridge 已停止")
        if hasattr(self, 'eventbus'):
            self.eventbus.stop_async_processing()
        logger.info("Platform Detectors 已停止")
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
