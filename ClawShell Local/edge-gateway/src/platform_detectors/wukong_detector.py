"""
ClawShell Edge Gateway — Wukong Detector
检测 Wukong (悟空) 平台是否可用。

检测方式：
- /Applications/悟空.app 或 ~/Applications/悟空.app 存在
- ~/.wukong/ 配置目录存在
- wukong 命令在 PATH 中
"""
import asyncio
import logging
import os
import subprocess
from typing import Any, Dict, List

from .base import PlatformDetector, DetectionResult

logger = logging.getLogger("detector.wukong")


class WukongDetector(PlatformDetector):
    """
    Wukong (悟空) 平台检测器。
    
    Wukong 是 ClawShell 的本地 AI Agent，提供智能助手能力。
    """

    def __init__(self):
        super().__init__()
        self.platform_name = "wukong"
        self.wukong_app_paths = [
            "/Applications/悟空.app",
            os.path.expanduser("~/Applications/悟空.app"),
            "/Applications/Wukong.app",
            os.path.expanduser("~/Applications/Wukong.app"),
        ]
        self.wukong_dir = os.path.expanduser("~/.wukong")
        self.wukong_cmd = "wukong"

    async def detect(self) -> DetectionResult:
        """
        检测 Wukong 平台是否可用。
        
        检测步骤：
        1. 检查 App 路径
        2. 检查配置目录
        3. 检查 wukong 命令
        4. 获取版本信息
        """
        details = {}
        version = None
        available = False
        error = None

        # 1. 检查 App 路径
        app_found = False
        found_app_path = None
        for app_path in self.wukong_app_paths:
            if self._check_dir_exists(app_path):
                app_found = True
                found_app_path = app_path
                break
        details["app_exists"] = app_found
        details["app_path"] = found_app_path

        # 2. 检查配置目录
        dir_exists = self._check_dir_exists(self.wukong_dir)
        details["dir_exists"] = dir_exists
        details["wukong_dir"] = self.wukong_dir

        if not app_found and not dir_exists:
            error = f"Wukong not found: no app and no config dir"
            logger.debug(error)
            return DetectionResult(
                platform=self.platform_name,
                available=False,
                version=None,
                details=details,
                error=error,
            )

        # 3. 检查 wukong 命令
        cmd_exists = self._check_command_exists(self.wukong_cmd)
        details["command_exists"] = cmd_exists

        if not cmd_exists:
            # 尝试带 --version 检查
            r = self._run_sync([self.wukong_cmd, "--version"])
            if r.get("success"):
                cmd_exists = True
                version = r.get("stdout", "").split("\n")[0]
                details["version_raw"] = version

        if not cmd_exists:
            error = f"wukong command not found in PATH"
            logger.debug(error)
            return DetectionResult(
                platform=self.platform_name,
                available=False,
                version=None,
                details=details,
                error=error,
            )

        # 4. 获取版本信息
        if not version:
            version = self._get_version_from_command([self.wukong_cmd, "--version"])

        # 5. 检查配置文件
        config_paths = [
            os.path.join(self.wukong_dir, "config.json"),
            os.path.join(self.wukong_dir, "config.yaml"),
            os.path.join(self.wukong_dir, "config.toml"),
        ]
        config_found = None
        for config_path in config_paths:
            if self._check_file_exists(config_path):
                config_found = config_path
                break
        details["config_exists"] = config_found is not None
        details["config_path"] = config_found

        # 6. 检查运行状态
        try:
            r = self._run_sync(["ps", "aux"])
            details["running"] = "wukong" in r.get("stdout", "").lower() or "悟空" in r.get("stdout", "")
        except Exception:
            details["running"] = False

        available = True
        logger.info(f"Wukong detected: version={version}, app={found_app_path}")

        return DetectionResult(
            platform=self.platform_name,
            available=available,
            version=version,
            details=details,
            error=None,
        )

    async def check_skills(self) -> Dict[str, Any]:
        """检查可用技能"""
        if not self._check_command_exists(self.wukong_cmd):
            return {"available": False, "error": "wukong command not found"}

        try:
            r = await self._run_async([self.wukong_cmd, "skill", "list"], timeout=15)
            if r.get("success"):
                skills = [s.strip() for s in r.get("stdout", "").split("\n") if s.strip()]
                return {"available": True, "skills": skills}
            return {"available": False, "error": r.get("stderr", "unknown")}
        except Exception as e:
            return {"available": False, "error": str(e)}

    def get_app_path(self) -> str:
        """获取 Wukong App 路径"""
        for app_path in self.wukong_app_paths:
            if self._check_dir_exists(app_path):
                return app_path
        return self.wukong_app_paths[0]

    def get_config_path(self) -> str:
        """获取配置文件路径"""
        for config_name in ["config.json", "config.yaml", "config.toml"]:
            path = os.path.join(self.wukong_dir, config_name)
            if self._check_file_exists(path):
                return path
        return os.path.join(self.wukong_dir, "config.json")

    def get_logs_path(self) -> str:
        """获取日志目录路径"""
        return os.path.join(self.wukong_dir, "logs")
