"""
ClawShell Edge Gateway — OpenClaw Adapter
将云端调用映射为 OpenClaw Agent 的执行能力。
"""
import asyncio
import json
import logging
import os
import subprocess
from typing import Any, Dict

from .base import PlatformAdapter

logger = logging.getLogger("adapter.openclaw")

class OpenClawAdapter(PlatformAdapter):
    """
    OpenClaw 平台适配器。
    
    检测方式：
    - ~/.openclaw/ 目录存在
    - openclaw 命令在 PATH 中
    - openclaw --version 能输出版本
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.platform_name = "openclaw"
        self.openclaw_dir = os.path.expanduser(config.get("openclaw_dir", "~/.openclaw"))
        self.openclaw_cmd = config.get("openclaw_cmd", "openclaw")

    async def check_availability(self) -> bool:
        """检测 OpenClaw 是否可用"""
        # 1. 检查目录
        if not os.path.isdir(os.path.expanduser(self.openclaw_dir)):
            logger.debug(f"OpenClaw dir not found: {self.openclaw_dir}")
            self.is_available = False
            return False

        # 2. 检查 openclaw 命令
        r = self._run_sync([self.openclaw_cmd, "--version"])
        if not r.get("success"):
            r = self._run_sync(["which", self.openclaw_cmd])

        self.is_available = r.get("success")
        if not self.is_available:
            logger.debug(f"OpenClaw command not available: {self.openclaw_cmd}")
        return self.is_available

    async def invoke_skill(self, skill_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        通过 OpenClaw 调用技能。
        skill_id 格式：openclaw.skill.<name>
        """
        if not self.is_available:
            return {"success": False, "error": "openclaw not available"}

        parts = skill_id.split(".", 3)
        if len(parts) < 3:
            return {"success": False, "error": f"invalid skill_id format: {skill_id}"}

        _, _, skill_name = parts

        try:
            cmd = [
                self.openclaw_cmd,
                "skill",
                "run",
                skill_name,
                "--params", json.dumps(params),
            ]
            result = self._run_sync(cmd, timeout=60)

            if result.get("success"):
                try:
                    output = json.loads(result.get("stdout", "{}"))
                    return {"success": True, "result": output}
                except json.JSONDecodeError:
                    return {"success": True, "result": result.get("stdout", "")}
            else:
                return {"success": False, "error": result.get("stderr", "unknown")}
        except Exception as e:
            logger.exception(f"Failed to invoke skill {skill_id}")
            return {"success": False, "error": str(e)}

    async def create_task(self, title: str, description: str = "") -> Dict[str, Any]:
        """通过 OpenClaw 创建任务"""
        if not self.is_available:
            return {"success": False, "error": "openclaw not available"}

        cmd = [self.openclaw_cmd, "task", "create", "--title", title]
        if description:
            cmd.extend(["--description", description])

        r = self._run_sync(cmd)
        return {
            "success": r.get("success", False),
            "result": r.get("stdout", ""),
            "error": r.get("stderr", "") if not r.get("success") else "",
        }

    async def search_memory(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """通过 OpenClaw 搜索记忆"""
        return await self.invoke_skill(
            "openclaw.skill.search",
            {"query": query, "limit": limit}
        )

    async def send_notification(self, title: str, body: str) -> Dict[str, Any]:
        """通过 OpenClaw 发送通知"""
        if not self.is_available:
            return {"success": False, "error": "openclaw not available"}

        cmd = [self.openclaw_cmd, "notify", "--title", title, "--body", body]
        r = self._run_sync(cmd)
        return {
            "success": r.get("success", False),
            "result": r.get("stdout", ""),
        }
