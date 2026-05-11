"""
ClawShell Edge Gateway — Hermes Adapter
将云端调用映射为 Hermes Agent 的执行能力。
"""
import asyncio
import json
import logging
import os
import subprocess
from typing import Any, Dict

from .base import PlatformAdapter

logger = logging.getLogger("adapter.hermes")

class HermesAdapter(PlatformAdapter):
    """
    Hermes 平台适配器。
    
    检测方式：
    - ~/.hermes/ 目录存在
    - hermes 命令在 PATH 中
    - LaunchAgent 运行中
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.platform_name = "hermes"
        self.hermes_dir = os.path.expanduser(config.get("hermes_dir", "~/.hermes"))
        self.hermes_cmd = config.get("hermes_cmd", "hermes")
        self.mcp_config_path = config.get(
            "mcp_config_path",
            os.path.expanduser("~/.hermes/config/mcp.yaml")
        )

    async def check_availability(self) -> bool:
        """检测 Hermes 是否可用"""
        # 1. 检查目录
        if not os.path.isdir(os.path.expanduser(self.hermes_dir)):
            logger.debug(f"Hermes dir not found: {self.hermes_dir}")
            self.is_available = False
            return False
        
        # 2. 检查 hermes 命令
        r = self._run_sync(["which", self.hermes_cmd])
        if not r.get("success"):
            # 尝试带 -V 检查
            r = self._run_sync([self.hermes_cmd, "--version"])
        
        self.is_available = r.get("success")
        if not self.is_available:
            logger.debug(f"Hermes command not available: {self.hermes_cmd}")
        return self.is_available

    async def invoke_skill(self, skill_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        通过 Hermes 调用技能。
        
        skill_id 格式：hermes.mcp.<tool_name>
        例如：hermes.mcp.memos_search
        """
        if not self.is_available:
            return {"success": False, "error": "hermes not available"}

        # 解析 skill_id
        parts = skill_id.split(".", 3)
        if len(parts) < 4:
            return {"success": False, "error": f"invalid skill_id format: {skill_id}"}
        
        _, _, domain, tool_name = parts
        
        # 通过 hermes mcp call 调用
        try:
            # 构造 hermes mcp 命令
            cmd = [
                self.hermes_cmd,
                "mcp",
                "call",
                "--domain", domain,
                "--tool", tool_name,
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
                return {"success": False, "error": result.get("stderr", result.get("error", "unknown"))}
        except Exception as e:
            logger.exception(f"Failed to invoke skill {skill_id}")
            return {"success": False, "error": str(e)}

    async def create_task(self, title: str, description: str = "") -> Dict[str, Any]:
        """通过 Hermes 创建任务"""
        if not self.is_available:
            return {"success": False, "error": "hermes not available"}
        
        # 通过 hermes todo create
        cmd = [
            self.hermes_cmd,
            "todo",
            "create",
            "--title", title,
        ]
        if description:
            cmd.extend(["--description", description])
        
        r = self._run_sync(cmd)
        return {
            "success": r.get("success", False),
            "result": r.get("stdout", ""),
            "error": r.get("stderr", "") if not r.get("success") else "",
        }

    async def search_memory(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """通过 Hermes MCP 搜索记忆"""
        # 尝试通过 memos MCP 工具搜索
        r = await self.invoke_skill(
            "hermes.mcp.memos_search",
            {"query": query, "limit": limit}
        )
        return r

    async def send_notification(self, title: str, body: str) -> Dict[str, Any]:
        """通过 Hermes 发送通知"""
        if not self.is_available:
            return {"success": False, "error": "hermes not available"}
        
        cmd = [
            self.hermes_cmd,
            "notify",
            "--title", title,
            "--body", body,
        ]
        r = self._run_sync(cmd)
        return {
            "success": r.get("success", False),
            "result": r.get("stdout", ""),
        }

    async def get_hermes_info(self) -> Dict[str, Any]:
        """获取 Hermes 详细信息"""
        info = {
            "platform": "hermes",
            "available": self.is_available,
            "hermes_dir": self.hermes_dir,
            "mcp_config_path": self.mcp_config_path,
        }
        
        if self.is_available:
            # 获取版本
            r = self._run_sync([self.hermes_cmd, "--version"])
            if r.get("success"):
                info["version"] = r.get("stdout", "").split("\n")[0]
            
            # 获取运行状态
            r = self._run_sync(["ps", "aux"])
            info["running"] = "hermes" in r.get("stdout", "")
        
        return info
