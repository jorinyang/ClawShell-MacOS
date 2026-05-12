"""
ClawShell Edge Gateway — Platform Detector Base
所有平台检测器的基类，定义统一接口和检测结果格式。
"""
import asyncio
import logging
import os
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("detector.base")


@dataclass
class DetectionResult:
    """平台检测结果"""
    platform: str  # 平台名称 (openclaw, hermes, wukong, system)
    available: bool  # 是否可用
    version: Optional[str] = None  # 版本号
    details: Dict[str, Any] = field(default_factory=dict)  # 额外详情
    error: Optional[str] = None  # 检测错误信息

    def to_dict(self) -> Dict[str, Any]:
        return {
            "platform": self.platform,
            "available": self.available,
            "version": self.version,
            "details": self.details,
            "error": self.error,
        }


class PlatformDetector(ABC):
    """
    平台检测器基类。
    
    每个子类负责检测一个特定平台是否可用，并收集其元数据。
    采用 macOS async patterns，使用 asyncio 执行命令。
    """

    def __init__(self):
        self.platform_name: str = "unknown"
        self.result: Optional[DetectionResult] = None

    @abstractmethod
    async def detect(self) -> DetectionResult:
        """
        执行平台检测。
        
        子类必须实现此方法，执行具体的检测逻辑。
        返回 DetectionResult 对象。
        """
        ...

    async def is_available(self) -> bool:
        """快速检查平台是否可用"""
        result = await self.detect()
        return result.available

    def _run_sync(self, cmd: List[str], timeout: int = 30) -> Dict[str, Any]:
        """同步执行命令（供子类使用）"""
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "returncode": result.returncode,
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "command timeout"}
        except FileNotFoundError:
            return {"success": False, "error": f"command not found: {cmd[0]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _run_async(self, cmd: List[str], timeout: int = 30) -> Dict[str, Any]:
        """异步执行命令（macOS async pattern）"""
        loop = asyncio.get_event_loop()
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return {
                "success": proc.returncode == 0,
                "stdout": stdout.decode().strip() if stdout else "",
                "stderr": stderr.decode().strip() if stderr else "",
                "returncode": proc.returncode,
            }
        except asyncio.TimeoutExpired:
            proc.kill()
            return {"success": False, "error": "command timeout"}
        except FileNotFoundError:
            return {"success": False, "error": f"command not found: {cmd[0]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _check_dir_exists(self, path: str) -> bool:
        """检查目录是否存在"""
        return os.path.isdir(os.path.expanduser(path))

    def _check_file_exists(self, path: str) -> bool:
        """检查文件是否存在"""
        return os.path.isfile(os.path.expanduser(path))

    def _check_command_exists(self, cmd: str) -> bool:
        """检查命令是否在 PATH 中"""
        r = self._run_sync(["which", cmd])
        return r.get("success", False)

    def _get_version_from_command(self, cmd: List[str]) -> Optional[str]:
        """从命令输出中提取版本号"""
        r = self._run_sync(cmd)
        if r.get("success"):
            output = r.get("stdout", "")
            # 尝试提取版本号
            for line in output.split("\n"):
                if "version" in line.lower() or any(c.isdigit() for c in line):
                    return line.strip()
            return output.split("\n")[0] if output else None
        return None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} platform={self.platform_name}>"
