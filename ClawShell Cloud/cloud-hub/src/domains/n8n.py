"""
ClawShell Cloud Hub — N8N Bridge Domain
=======================================
P1b: 从 cloud/engines/n8n_bridge.py 移植，适配 Domain 架构

功能：
- 事件类型 → webhook URL 映射
- 通配符模式匹配事件路由
- 健康检查 via HEAD request
- 异步方法 + 同步包装器
- 注册到 hub.py
"""

import asyncio
import fnmatch
import json
import logging
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import aiohttp
import urllib.request
import urllib.error

logger = logging.getLogger("n8n_bridge")


class N8NBridgeDomain:
    """
    N8N 网桥领域处理器：将云端事件路由到 N8N 工作流。
    
    适配 cloud-hub Domain 架构：
    - 所有 IO 方法提供 async 版本（MacOS 原生异步）
    - 提供 sync 包装器用于兼容
    - 通过 register_domain() 注册到 hub.py
    """

    DEFAULT_TIMEOUT = 10

    def __init__(self, n8n_base_url: str = "http://localhost:5678"):
        self._base_url = n8n_base_url.rstrip("/")
        self._lock = threading.RLock()
        self._routes: Dict[str, str] = {}   # event_pattern → webhook_url
        self._trigger_log: List[dict] = []

    # ── Route Management ────────────────────────────────────────────────────

    def add_route(self, event_pattern: str, webhook_url: str) -> str:
        """同步：添加事件模式 → webhook URL 路由。返回 pattern。"""
        with self._lock:
            self._routes[event_pattern] = webhook_url
            return event_pattern

    def remove_route(self, event_pattern: str) -> bool:
        """同步：移除路由。成功返回 True。"""
        with self._lock:
            if event_pattern in self._routes:
                del self._routes[event_pattern]
                return True
            return False

    def list_routes(self) -> List[dict]:
        """同步：列出所有路由。"""
        with self._lock:
            return [
                {"pattern": k, "webhook_url": v}
                for k, v in self._routes.items()
            ]

    # ── Async Route Management ───────────────────────────────────────────────

    async def add_route_async(self, event_pattern: str, webhook_url: str) -> str:
        """异步：添加事件模式 → webhook URL 路由。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.add_route, event_pattern, webhook_url)

    async def remove_route_async(self, event_pattern: str) -> bool:
        """异步：移除路由。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.remove_route, event_pattern)

    async def list_routes_async(self) -> List[dict]:
        """异步：列出所有路由。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.list_routes)

    # ── Event Routing ────────────────────────────────────────────────────────

    def match_routes(self, event_type: str) -> List[str]:
        """同步：查找匹配事件类型的所有 webhook URL。"""
        with self._lock:
            urls = []
            for pattern, url in self._routes.items():
                if fnmatch.fnmatch(event_type, pattern):
                    urls.append(url)
            return urls

    async def match_routes_async(self, event_type: str) -> List[str]:
        """异步：查找匹配事件类型的所有 webhook URL。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.match_routes, event_type)

    def trigger(self, event: dict) -> List[dict]:
        """
        同步：触发事件匹配的 N8N 工作流。返回结果列表。
        注意：这是同步阻塞调用，生产环境优先用 trigger_async。
        """
        event_type = event.get("event_type", "")
        urls = self.match_routes(event_type)

        results = []
        for url in urls:
            result = self._call_webhook_sync(url, event)
            results.append(result)
            self._log_trigger(event_type, url, result.get("status", "error"))

        return results

    async def trigger_async(self, event: dict) -> List[dict]:
        """
        异步：触发事件匹配的 N8N 工作流。返回结果列表。
        适配 MacOS 异步架构，使用 aiohttp。
        """
        event_type = event.get("event_type", "")
        urls = await self.match_routes_async(event_type)

        results = []
        for url in urls:
            result = await self._call_webhook_async(url, event)
            results.append(result)
            self._log_trigger(event_type, url, result.get("status", "error"))

        return results

    def trigger_workflow(self, webhook_url: str, payload: dict) -> dict:
        """同步：直接触发指定工作流。"""
        return self._call_webhook_sync(webhook_url, payload)

    async def trigger_workflow_async(self, webhook_url: str, payload: dict) -> dict:
        """异步：直接触发指定工作流。"""
        return await self._call_webhook_async(webhook_url, payload)

    # ── Health Check ────────────────────────────────────────────────────────

    def health_check(self) -> dict:
        """同步：检查 N8N 可用性。"""
        try:
            req = urllib.request.Request(
                f"{self._base_url}/healthz",
                method="HEAD"
            )
            resp = urllib.request.urlopen(req, timeout=5)
            return {"status": "healthy", "code": resp.status}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def health_check_async(self) -> dict:
        """异步：检查 N8N 可用性。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._health_check_sync)

    def _health_check_sync(self) -> dict:
        """health_check 的同步实现（供 executor 调用）。"""
        try:
            req = urllib.request.Request(
                f"{self._base_url}/healthz",
                method="HEAD"
            )
            resp = urllib.request.urlopen(req, timeout=5)
            return {"status": "healthy", "code": resp.status}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    # ── Trigger Log ─────────────────────────────────────────────────────────

    def get_trigger_log(self, limit: int = 50) -> List[dict]:
        """同步：获取触发日志。"""
        with self._lock:
            return self._trigger_log[-limit:]

    async def get_trigger_log_async(self, limit: int = 50) -> List[dict]:
        """异步：获取触发日志。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_trigger_log, limit)

    def clear_trigger_log(self) -> None:
        """同步：清空触发日志。"""
        with self._lock:
            self._trigger_log.clear()

    # ── Internal ────────────────────────────────────────────────────────────

    def _call_webhook_sync(self, url: str, payload: dict) -> dict:
        """同步调用 webhook（使用 urllib）。"""
        start = time.time()
        try:
            data = json.dumps(payload, default=str).encode()
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            resp = urllib.request.urlopen(req, timeout=self.DEFAULT_TIMEOUT)
            body = resp.read().decode()
            return {
                "url": url,
                "status": "ok",
                "code": resp.status,
                "body": body,
                "duration_ms": (time.time() - start) * 1000,
            }
        except urllib.error.HTTPError as e:
            return {
                "url": url,
                "status": "http_error",
                "code": e.code,
                "error": str(e),
                "duration_ms": (time.time() - start) * 1000,
            }
        except Exception as e:
            return {
                "url": url,
                "status": "error",
                "error": str(e),
                "duration_ms": (time.time() - start) * 1000,
            }

    async def _call_webhook_async(self, url: str, payload: dict) -> dict:
        """异步调用 webhook（使用 aiohttp）。"""
        start = time.time()
        try:
            data = json.dumps(payload, default=str)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
                ) as resp:
                    body = await resp.text()
                    return {
                        "url": url,
                        "status": "ok",
                        "code": resp.status,
                        "body": body,
                        "duration_ms": (time.time() - start) * 1000,
                    }
        except aiohttp.ClientError as e:
            return {
                "url": url,
                "status": "http_error",
                "error": str(e),
                "duration_ms": (time.time() - start) * 1000,
            }
        except Exception as e:
            return {
                "url": url,
                "status": "error",
                "error": str(e),
                "duration_ms": (time.time() - start) * 1000,
            }

    def _log_trigger(self, event_type: str, url: str, status: str) -> None:
        """记录触发日志。"""
        with self._lock:
            self._trigger_log.append({
                "event_type": event_type,
                "url": url,
                "status": status,
                "timestamp": time.time(),
            })
            if len(self._trigger_log) > 200:
                self._trigger_log = self._trigger_log[-100:]

    # ── Domain Handler Registration ─────────────────────────────────────────

    @property
    def domain_name(self) -> str:
        return "n8n"

    async def handle(self, method: str, params: dict) -> dict:
        """
        Domain handler 入口（hub.py 路由调用）。
        将 MCP 方法路由到对应方法。
        """
        if method == "n8n_add_route":
            pattern = params.get("event_pattern", "")
            url = params.get("webhook_url", "")
            result = await self.add_route_async(pattern, url)
            return {"success": True, "pattern": result}

        if method == "n8n_remove_route":
            pattern = params.get("event_pattern", "")
            removed = await self.remove_route_async(pattern)
            return {"success": removed}

        if method == "n8n_list_routes":
            routes = await self.list_routes_async()
            return {"success": True, "routes": routes}

        if method == "n8n_trigger":
            event = params.get("event", {})
            results = await self.trigger_async(event)
            return {"success": True, "results": results}

        if method == "n8n_trigger_workflow":
            url = params.get("webhook_url", "")
            payload = params.get("payload", {})
            result = await self.trigger_workflow_async(url, payload)
            return {"success": True, "result": result}

        if method == "n8n_health_check":
            health = await self.health_check_async()
            return {"success": True, "health": health}

        if method == "n8n_trigger_log":
            log = await self.get_trigger_log_async(params.get("limit", 50))
            return {"success": True, "log": log}

        return {"success": False, "error": f"Unknown method: {method}"}
