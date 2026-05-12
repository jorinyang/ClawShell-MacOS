"""
ClawShell Edge Gateway — OpenClaw Detector
检测 OpenClaw 平台是否可用。

检测方式：
- ~/.openclaw/ 目录存在
- openclaw 命令在 PATH 中
- openclaw --version 能输出版本
"""
import asyncio
import logging
import os
import subprocess
from typing import Any, Dict

from .base import PlatformDetector, DetectionResult

logger = logging.getLogger("detector.openclaw")


class OpenClawDetector(PlatformDetector):
    """
    OpenClaw 平台检测器。
    
    OpenClaw 是 ClawShell 的开源版本，提供本地 Agent 执行能力。
    """

    def __init__(self):
        super().__init__()
        self.platform_name = "openclaw"
        self.openclaw_dir = os.path.expanduser("~/.openclaw")
        self.openclaw_cmd = "openclaw"

    async def detect(self) -> DetectionResult:
        """
        检测 OpenClaw 平台是否可用。
        
        检测步骤：
        1. 检查 ~/.openclaw/ 目录
        2. 检查 openclaw 命令
        3. 获取版本信息
        """
        details = {}
        version = None
        available = False
        error = None

        # 1. 检查目录
        dir_exists = self._check_dir_exists(self.openclaw_dir)
        details["dir_exists"] = dir_exists
        details["openclaw_dir"] = self.openclaw_dir

        if not dir_exists:
            error = f"OpenClaw directory not found: {self.openclaw_dir}"
            logger.debug(error)
            return DetectionResult(
                platform=self.platform_name,
                available=False,
                version=None,
                details=details,
                error=error,
            )

        # 2. 检查 openclaw 命令
        cmd_exists = self._check_command_exists(self.openclaw_cmd)
        details["command_exists"] = cmd_exists

        if not cmd_exists:
            # 尝试直接执行
            r = self._run_sync([self.openclaw_cmd, "--version"])
            cmd_exists = r.get("success")
            if cmd_exists:
                version = r.get("stdout", "").split("\n")[0]
                details["version_raw"] = version

        if not cmd_exists:
            error = f"openclaw command not found in PATH"
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
            version = self._get_version_from_command([self.openclaw_cmd, "--version"])

        # 4. 检查配置文件
        config_path = os.path.join(self.openclaw_dir, "config.json")
        if self._check_file_exists(config_path):
            details["config_exists"] = True
            details["config_path"] = config_path
        else:
            details["config_exists"] = False

        # 5. 检查运行状态（可选）
        try:
            r = self._run_sync(["ps", "aux"])
            details["running"] = "openclaw" in r.get("stdout", "").lower()
        except Exception:
            details["running"] = False

        available = True
        logger.info(f"OpenClaw detected: version={version}, dir={self.openclaw_dir}")

        return DetectionResult(
            platform=self.platform_name,
            available=available,
            version=version,
            details=details,
            error=None,
        )

    async def check_update_available(self) -> bool:
        """检查是否有可用更新（通过 npm outdated）"""
        if not self._check_command_exists("npm"):
            return False
        
        try:
            r = await self._run_async(
                ["npm", "outdated", "-g", "openclaw"],
                timeout=30
            )
            return r.get("success") and "UNMET" not in r.get("stdout", "")
        except Exception:
            return False

    def get_config_path(self) -> str:
        """获取配置文件路径"""
        return os.path.join(self.openclaw_dir, "config.json")

    def get_logs_path(self) -> str:
        """获取日志目录路径"""
        return os.path.join(self.openclaw_dir, "logs")
