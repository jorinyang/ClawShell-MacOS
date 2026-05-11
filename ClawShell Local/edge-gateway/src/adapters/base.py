"""
ClawShell Edge Gateway — Platform Adapter Base
所有平台适配器的基类，定义统一接口。
"""
import asyncio
import logging
import subprocess
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

logger = logging.getLogger("adapter")

class PlatformAdapter(ABC):
    """
    平台适配器基类。
    每个已检测到的平台都有一个对应的 Adapter，将云端调用映射为本地执行。
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.platform_name: str = "unknown"
        self.is_available: bool = False

    @abstractmethod
    async def check_availability(self) -> bool:
        """检测平台是否可用"""
        ...

    @abstractmethod
    async def invoke_skill(self, skill_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        调用本地技能。
        skill_id: 技能标识符（如 hermes.mcp.memos_search）
        params: 技能参数
        返回: {"success": bool, "result": Any, "error": str}
        """
        ...

    @abstractmethod
    async def create_task(self, title: str, description: str = "") -> Dict[str, Any]:
        """在本地平台创建任务"""
        ...

    @abstractmethod
    async def search_memory(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """搜索本地记忆"""
        ...

    @abstractmethod
    async def send_notification(self, title: str, body: str) -> Dict[str, Any]:
        """发送本地通知"""
        ...

    async def get_status(self) -> Dict[str, Any]:
        """获取平台状态"""
        return {
            "platform": self.platform_name,
            "available": self.is_available,
            "config": self.config,
        }

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
        """异步执行命令"""
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
