"""
ClawShell Edge Gateway — Wukong Adapter
将云端调用映射为 Wukong Agent 的执行能力。
"""
import asyncio
import json
import logging
import os
import subprocess
from typing import Any, Dict

from .base import PlatformAdapter

logger = logging.getLogger("adapter.wukong")

class WukongAdapter(PlatformAdapter):
    """
    Wukong 平台适配器。

    检测方式：
    - /Applications/悟空.app 或 ~/Applications/悟空.app 存在
    - ~/.wukong/ 配置目录存在
    - wukong 命令在 PATH 中
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.platform_name = "wukong"
        self.wukong_app_paths = [
            "/Applications/悟空.app",
            os.path.expanduser("~/Applications/悟空.app"),
            "/Applications/Wukong.app",
        ]
        self.wukong_dir = os.path.expanduser(config.get("wukong_dir", "~/.wukong"))
        self.wukong_cmd = config.get("wukong_cmd", "wukong")

    async def check_availability(self) -> bool:
        """检测 Wukong 是否可用"""
        # 1. 检查 App
        app_found = any(os.path.isdir(p) for p in self.wukong_app_paths)

        # 2. 检查配置目录
        if not os.path.isdir(os.path.expanduser(self.wukong_dir)):
            logger.debug(f"Wukong dir not found: {self.wukong_dir}")

        # 3. 检查命令
        r = self._run_sync([self.wukong_cmd, "--version"])
        if not r.get("success"):
            r = self._run_sync(["which", self.wukong_cmd])

        self.is_available = app_found or r.get("success")
        if not self.is_available:
            logger.debug(f"Wukong not available")
        return self.is_available

    async def invoke_skill(self, skill_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        通过 Wukong 调用技能。
        skill_id 格式：wukong.skill.<name>
        """
        if not self.is_available:
            return {"success": False, "error": "wukong not available"}

        parts = skill_id.split(".", 3)
        if len(parts) < 3:
            return {"success": False, "error": f"invalid skill_id format: {skill_id}"}

        _, _, skill_name = parts

        try:
            cmd = [
                self.wukong_cmd,
                "skill",
                "run",
                skill_name,
                "--json",
            ]
            # 将 params 作为 stdin 或参数传入
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=json.dumps(params).encode()),
                timeout=60,
            )
            if proc.returncode == 0:
                try:
                    return {"success": True, "result": json.loads(stdout.decode())}
                except json.JSONDecodeError:
                    return {"success": True, "result": stdout.decode()}
            else:
                return {"success": False, "error": stderr.decode() or "command failed"}
        except asyncio.TimeoutExpired:
            return {"success": False, "error": "command timeout"}
        except Exception as e:
            logger.exception(f"Failed to invoke skill {skill_id}")
            return {"success": False, "error": str(e)}

    async def create_task(self, title: str, description: str = "") -> Dict[str, Any]:
        """通过 Wukong 创建任务"""
        if not self.is_available:
            return {"success": False, "error": "wukong not available"}

        cmd = [self.wukong_cmd, "task", "create", "--title", title]
        if description:
            cmd.extend(["--description", description])

        r = self._run_sync(cmd)
        return {
            "success": r.get("success", False),
            "result": r.get("stdout", ""),
            "error": r.get("stderr", "") if not r.get("success") else "",
        }

    async def search_memory(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """通过 Wukong 搜索记忆"""
        return await self.invoke_skill(
            "wukong.skill.search",
            {"query": query, "limit": limit}
        )

    async def send_notification(self, title: str, body: str) -> Dict[str, Any]:
        """通过 Wukong 发送通知"""
        if not self.is_available:
            return {"success": False, "error": "wukong not available"}

        cmd = [self.wukong_cmd, "notify", "--title", title, "--body", body]
        r = self._run_sync(cmd)
        return {
            "success": r.get("success", False),
            "result": r.get("stdout", ""),
        }
