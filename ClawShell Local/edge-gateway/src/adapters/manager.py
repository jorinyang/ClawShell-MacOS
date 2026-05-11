"""
ClawShell Edge Gateway — Adapter Manager
管理所有平台适配器，将云端 skill_invoke 路由到正确的本地平台。
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

from .base import PlatformAdapter
from .hermes_adapter import HermesAdapter
from .openclaw_adapter import OpenClawAdapter
from .wukong_adapter import WukongAdapter

logger = logging.getLogger("adapter.manager")

class AdapterManager:
    """
    适配器管理器：
    1. 扫描所有平台适配器
    2. 按优先级尝试调用
    3. 支持平台能力查询
    """

    def __init__(self):
        self._adapters: Dict[str, PlatformAdapter] = {}
        self._priority: List[str] = ["hermes", "openclaw", "wukong"]
        self._initialized = False

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        初始化所有适配器并检测可用性。
        config 格式：
        {
            "hermes": {...},
            "openclaw": {...},
            "wukong": {...},
        }
        """
        if self._initialized:
            return

        adapter_map = {
            "hermes": HermesAdapter,
            "openclaw": OpenClawAdapter,
            "wukong": WukongAdapter,
        }

        for name, adapter_cls in adapter_map.items():
            cfg = config.get(name, {})
            adapter = adapter_cls(cfg)
            is_available = await adapter.check_availability()
            adapter.is_available = is_available
            self._adapters[name] = adapter
            logger.info(f"Platform adapter [{name}]: {'available' if is_available else 'not available'}")

        self._initialized = True

    def get_adapter(self, platform: str) -> Optional[PlatformAdapter]:
        """获取指定平台的适配器"""
        return self._adapters.get(platform)

    def get_available_adapters(self) -> List[PlatformAdapter]:
        """获取所有可用的适配器（按优先级排序）"""
        available = []
        for name in self._priority:
            adapter = self._adapters.get(name)
            if adapter and adapter.is_available:
                available.append(adapter)
        return available

    async def invoke_skill(
        self,
        skill_id: str,
        params: Dict[str, Any],
        preferred_platform: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        调用技能，自动路由到合适的平台。

        skill_id 格式：
        - hermes.mcp.<tool>     → Hermes
        - openclaw.skill.<name> → OpenClaw
        - wukong.skill.<name>    → Wukong
        - <generic>             → 按优先级尝试可用平台

        preferred_platform: 优先使用的平台
        """
        # 1. 从 skill_id 解析平台
        platform = self._platform_from_skill_id(skill_id)
        if platform:
            adapter = self._adapters.get(platform)
            if not adapter or not adapter.is_available:
                return {
                    "success": False,
                    "error": f"platform {platform} not available for skill {skill_id}",
                }
            return await adapter.invoke_skill(skill_id, params)

        # 2. 使用 preferred_platform
        if preferred_platform:
            adapter = self._adapters.get(preferred_platform)
            if adapter and adapter.is_available:
                return await adapter.invoke_skill(skill_id, params)
            return {"success": False, "error": f"preferred platform {preferred_platform} not available"}

        # 3. 按优先级尝试所有可用平台
        for name in self._priority:
            adapter = self._adapters.get(name)
            if adapter and adapter.is_available:
                result = await adapter.invoke_skill(skill_id, params)
                if result.get("success"):
                    return result

        return {"success": False, "error": "no available platform can handle this skill"}

    def _platform_from_skill_id(self, skill_id: str) -> Optional[str]:
        """从 skill_id 解析平台名"""
        if skill_id.startswith("hermes."):
            return "hermes"
        if skill_id.startswith("openclaw."):
            return "openclaw"
        if skill_id.startswith("wukong."):
            return "wukong"
        return None

    async def create_task(self, title: str, description: str = "") -> Dict[str, Any]:
        """在第一个可用的平台创建任务"""
        for name in self._priority:
            adapter = self._adapters.get(name)
            if adapter and adapter.is_available:
                result = await adapter.create_task(title, description)
                if result.get("success"):
                    return result
        return {"success": False, "error": "no platform available for task creation"}

    async def search_memory(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """在第一个可用的平台搜索记忆"""
        for name in self._priority:
            adapter = self._adapters.get(name)
            if adapter and adapter.is_available:
                result = await adapter.search_memory(query, limit)
                if result.get("success"):
                    return result
        return {"success": False, "error": "no platform available for memory search"}

    async def send_notification(self, title: str, body: str) -> Dict[str, Any]:
        """在所有可用平台发送通知"""
        results = []
        for name in self._priority:
            adapter = self._adapters.get(name)
            if adapter and adapter.is_available:
                result = await adapter.send_notification(title, body)
                results.append({"platform": name, **result})
        return {"success": True, "notifications": results}

    async def get_all_status(self) -> List[Dict[str, Any]]:
        """获取所有平台状态"""
        status = []
        for name in self._priority:
            adapter = self._adapters.get(name)
            if adapter:
                s = await adapter.get_status()
                status.append(s)
        return status
