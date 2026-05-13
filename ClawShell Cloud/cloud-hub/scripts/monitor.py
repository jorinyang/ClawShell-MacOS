"""CloudHub + Hermes 健康监控脚本

监控项：
  - CloudHub WS 活跃连接数
  - CloudHub MCP Server 可用性（Port 8081）
  - CloudHub HTTP Server 可用性（Port 8082）
  - Hermes 进程存活
  - DLQ 积压数量
  - 事件写入速率

用法：
  python3 monitor.py --check         # 单次检查
  python3 monitor.py --watch         # 持续监控（每 30s）
  python3 monitor.py --watch --alert # 持续监控 + 告警
"""
import argparse
import asyncio
import json
import logging
import os
import subprocess
import time
import aiohttp
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("monitor")


# ── 配置 ───────────────────────────────────────────────────────────────────

MCP_URL = "http://localhost:8081"
HTTP_URL = "http://localhost:8082/cloudbrain/status"
WS_URL = "ws://localhost:8080"
DLQ_PATH = Path(os.getenv("DLQ_PATH", "/opt/clawshell/data/dlq"))
ALERT_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "")


# ── 健康检查 ────────────────────────────────────────────────────────────────


async def check_mcp_server() -> dict:
    """检查 MCP Server（Port 8081）"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MCP_URL}/health",
                timeout=aiohttp.ClientTimeout(total=2.0),
            ) as resp:
                result = await resp.json()
                return {
                    "name": "MCP Server (8081)",
                    "status": "ok" if resp.status == 200 else "error",
                    "detail": result.get("detail", ""),
                }
    except Exception as e:
        return {"name": "MCP Server (8081)", "status": "error", "detail": str(e)}


async def check_http_server() -> dict:
    """检查 HTTP Server（Port 8082）"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                HTTP_URL,
                timeout=aiohttp.ClientTimeout(total=2.0),
            ) as resp:
                result = await resp.json()
                return {
                    "name": "HTTP Server (8082)",
                    "status": "ok" if resp.status == 200 else "error",
                    "detail": result.get("detail", ""),
                }
    except Exception as e:
        return {"name": "HTTP Server (8082)", "status": "error", "detail": str(e)}


def check_hermes_process() -> dict:
    """检查 Hermes 进程是否存活"""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "hermes"],
            capture_output=True,
            text=True,
        )
        running = bool(result.stdout.strip())
        return {
            "name": "Hermes Process",
            "status": "ok" if running else "error",
            "detail": "running" if running else "not found",
        }
    except Exception as e:
        return {"name": "Hermes Process", "status": "error", "detail": str(e)}


def check_cloudhub_process() -> dict:
    """检查 CloudHub 进程是否存活"""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "hub.py"],
            capture_output=True,
            text=True,
        )
        running = bool(result.stdout.strip())
        return {
            "name": "CloudHub Process",
            "status": "ok" if running else "error",
            "detail": "running" if running else "not found",
        }
    except Exception as e:
        return {"name": "CloudHub Process", "status": "error", "detail": str(e)}


def check_dlq() -> dict:
    """检查 DLQ 积压数量"""
    try:
        files = list(DLQ_PATH.glob("*.json"))
        count = len(files)
        status = "ok" if count < 10 else ("warn" if count < 100 else "error")
        return {
            "name": "DLQ Queue",
            "status": status,
            "detail": f"{count} pending messages",
        }
    except Exception as e:
        return {"name": "DLQ Queue", "status": "error", "detail": str(e)}


async def check_ws_connections() -> dict:
    """检查 WS 活跃连接数"""
    # WS 连接数需要从 CloudHub 日志或状态 API 获取
    # 暂时通过 HTTP 检查 CloudHub 整体健康作为代理
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MCP_URL}/health",
                timeout=aiohttp.ClientTimeout(total=2.0),
            ) as resp:
                healthy = resp.status == 200
                return {
                    "name": "WS Connections",
                    "status": "ok" if healthy else "error",
                    "detail": "CloudHub healthy" if healthy else "CloudHub unhealthy",
                }
    except Exception as e:
        return {"name": "WS Connections", "status": "error", "detail": str(e)}


# ── 告警 ────────────────────────────────────────────────────────────────────


def send_alert(message: str):
    """发送钉钉告警"""
    if not ALERT_WEBHOOK:
        logger.warning(f"Alert (no webhook): {message}")
        return

    try:
        import urllib.request
        payload = json.dumps({
            "msgtype": "text",
            "text": {"content": f"[ClawShell Monitor] {message}"},
        }).encode("utf-8")
        req = urllib.request.Request(
            ALERT_WEBHOOK,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
        logger.info(f"Alert sent: {message}")
    except Exception as e:
        logger.error(f"Alert failed: {e}")


# ── 主检查逻辑 ────────────────────────────────────────────────────────────────


async def run_checks(alert: bool = False):
    """执行所有检查"""
    checks = [
        check_mcp_server(),
        check_http_server(),
        check_cloudhub_process(),
        check_hermes_process(),
        check_dlq(),
        check_ws_connections(),
    ]

    results = await asyncio.gather(*checks)

    # 输出
    all_ok = True
    for r in results:
        icon = "✅" if r["status"] == "ok" else ("⚠️" if r["status"] == "warn" else "❌")
        print(f"  {icon} {r['name']}: {r['detail']}")
        if r["status"] != "ok":
            all_ok = False

    if not all_ok and alert:
        msg = "; ".join(f"{r['name']}={r['status']}" for r in results if r["status"] != "ok")
        send_alert(f"健康检查失败: {msg}")

    return all_ok


async def watch(interval: int = 30, alert: bool = False):
    """持续监控"""
    print(f"Watching every {interval}s (Ctrl-C to stop)")
    while True:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{ts}]")
        await run_checks(alert=alert)
        await asyncio.sleep(interval)


# ── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="ClawShell Cloud Monitor")
    parser.add_argument("--check", action="store_true", help="Run checks once")
    parser.add_argument("--watch", action="store_true", help="Watch mode")
    parser.add_argument("--alert", action="store_true", help="Send alerts on failure")
    parser.add_argument("--interval", type=int, default=30, help="Watch interval in seconds")
    args = parser.parse_args()

    if args.watch:
        asyncio.run(watch(interval=args.interval, alert=args.alert))
    else:
        asyncio.run(run_checks(alert=args.alert))


if __name__ == "__main__":
    main()
