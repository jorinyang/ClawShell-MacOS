"""
ClawShell Edge Gateway — Hermes Detector
检测 Hermes Agent 平台是否可用。

检测方式：
- ~/.hermes/ 目录存在
- hermes 命令在 PATH 中
- LaunchAgent 运行中
- MCP 配置存在
"""
import asyncio
import logging
import os
import subprocess
from typing import Any, Dict, Optional

from .base import PlatformDetector, DetectionResult

logger = logging.getLogger("detector.hermes")


class HermesDetector(PlatformDetector):
    """
    Hermes Agent 平台检测器。
    
    Hermes 是 ClawShell 的核心 AI Agent，提供 MCP 工具调用能力。
    """

    def __init__(self):
        super().__init__()
        self.platform_name = "hermes"
        self.hermes_dir = os.path.expanduser("~/.hermes")
        self.hermes_cmd = "hermes"
        self.mcp_config_path = os.path.expanduser("~/.hermes/config/mcp.yaml")
        self.launch_agent_path = os.path.expanduser(
            "~/Library/LaunchAgents/com.clawshell.hermes.plist"
        )

    async def detect(self) -> DetectionResult:
        """
        检测 Hermes 平台是否可用。
        
        检测步骤：
        1. 检查 ~/.hermes/ 目录
        2. 检查 hermes 命令
        3. 获取版本信息
        4. 检查 MCP 配置
        5. 检查 LaunchAgent 运行状态
        """
        details = {}
        version = None
        available = False
        error = None

        # 1. 检查目录
        dir_exists = self._check_dir_exists(self.hermes_dir)
        details["dir_exists"] = dir_exists
        details["hermes_dir"] = self.hermes_dir

        if not dir_exists:
            error = f"Hermes directory not found: {self.hermes_dir}"
            logger.debug(error)
            return DetectionResult(
                platform=self.platform_name,
                available=False,
                version=None,
                details=details,
                error=error,
            )

        # 2. 检查 hermes 命令
        cmd_exists = self._check_command_exists(self.hermes_cmd)
        details["command_exists"] = cmd_exists

        if not cmd_exists:
            # 尝试带 -V 检查
            r = self._run_sync([self.hermes_cmd, "--version"])
            if r.get("success"):
                cmd_exists = True
                version = r.get("stdout", "").split("\n")[0]
                details["version_raw"] = version

        if not cmd_exists:
            error = f"hermes command not found in PATH"
            logger.debug(error)
            return DetectionResult(
                platform=self.platform_name,
                available=False,
                version=None,
                details=details,
                error=error,
            )

        # 3. 获取版本信息
        if not version:
            version = self._get_version_from_command([self.hermes_cmd, "--version"])

        # 4. 检查 MCP 配置
        mcp_config_exists = self._check_file_exists(self.mcp_config_path)
        details["mcp_config_exists"] = mcp_config_exists
        if mcp_config_exists:
            details["mcp_config_path"] = self.mcp_config_path

        # 5. 检查 LaunchAgent
        launch_agent_exists = self._check_file_exists(self.launch_agent_path)
        details["launch_agent_exists"] = launch_agent_exists

        # 6. 检查运行状态
        try:
            r = self._run_sync(["ps", "aux"])
            details["running"] = "hermes" in r.get("stdout", "").lower()
        except Exception:
            details["running"] = False

        # 7. 检查 LaunchAgent 是否加载
        if launch_agent_exists:
            try:
                r = self._run_sync(["launchctl", "list", "com.clawshell.hermes"])
                details["launchctl_loaded"] = r.get("returncode") == 0
            except Exception:
                details["launchctl_loaded"] = False

        available = True
        logger.info(f"Hermes detected: version={version}, dir={self.hermes_dir}")

        return DetectionResult(
            platform=self.platform_name,
            available=available,
            version=version,
            details=details,
            error=None,
        )

    async def check_mcp_tools(self) -> Dict[str, Any]:
        """检查 MCP 工具可用性"""
        if not self._check_command_exists(self.hermes_cmd):
            return {"available": False, "error": "hermes command not found"}

        try:
            # 尝试获取 MCP 工具列表
            r = await self._run_async([self.hermes_cmd, "mcp", "list"], timeout=10)
            if r.get("success"):
                tools = r.get("stdout", "").split("\n")
                return {"available": True, "tools": [t for t in tools if t.strip()]}
            return {"available": False, "error": r.get("stderr", "unknown")}
        except Exception as e:
            return {"available": False, "error": str(e)}

    def get_mcp_config_path(self) -> str:
        """获取 MCP 配置文件路径"""
        return self.mcp_config_path

    def get_launch_agent_path(self) -> str:
        """获取 LaunchAgent plist 路径"""
        return self.launch_agent_path

    def get_logs_path(self) -> str:
        """获取日志目录路径"""
        return os.path.join(self.hermes_dir, "logs")
