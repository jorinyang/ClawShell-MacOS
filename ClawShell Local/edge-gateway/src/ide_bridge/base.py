"""IDE Bridge 基础模块 — 抽象基类 (异步版本)

Harness Engineering 方法论:
Edge Brain 可将代码开发任务委托给多个 Agent CLI IDE (Codex, Claude Code, Kimi Code, DeepSeek TUI, Copilot),
根据能力进行生态位匹配 (ecological niche matching)。

标准接口:
- detect(): 检查 IDE 是否安装
- invoke_async(task): 异步执行编码任务
- get_capabilities(): 返回 IDE 能力用于匹配
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class IDETask:
    """委托给 Agent CLI IDE 的编码任务"""
    task_id: str = ""
    description: str = ""
    task_type: str = "code"          # code / review / debug / refactor / test
    language: str = ""               # python / javascript / go / etc.
    context: str = ""                # IDE 的额外上下文
    files: List[str] = field(default_factory=list)
    working_dir: str = ""
    timeout_seconds: int = 300
    priority: int = 0


@dataclass
class IDEResult:
    """IDE 任务执行结果"""
    task_id: str = ""
    ide_name: str = ""
    success: bool = False
    output: str = ""
    error: str = ""
    duration_seconds: float = 0.0
    files_modified: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseIDEBridge(ABC):
    """Agent CLI IDE 的抽象 Bridge 基类 (异步版本)"""

    IDE_NAME: str = "unknown"
    CLI_COMMAND: str = ""           # CLI 命令名 (如 "codex", "claude")
    CHECK_COMMAND: str = ""        # 检查安装的命令

    # ── 抽象方法 ────────────────────────────────

    @abstractmethod
    def detect(self) -> bool:
        """检查此 IDE CLI 是否已安装且可用"""
        ...

    @abstractmethod
    async def invoke_async(self, task: IDETask) -> IDEResult:
        """异步执行编码任务"""
        ...

    # ── 能力接口 ────────────────────────────────

    def get_capabilities(self) -> List[str]:
        """返回用于生态位匹配的 IDE 能力"""
        return ["code"]  # 子类覆盖

    def get_name(self) -> str:
        return self.IDE_NAME

    # ── 异步执行辅助 ─────────────────────────────

    async def invoke(self, task: IDETask) -> IDEResult:
        """同步包装器 (兼容旧代码)"""
        return await self.invoke_async(task)

    async def _run_command_async(
        self,
        cmd: List[str],
        cwd: str = ".",
        timeout: int = 300,
        stdin_data: Optional[str] = None,
    ) -> Tuple[int, str, str]:
        """异步运行 shell 命令并返回 (exit_code, stdout, stderr)"""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=cwd,
                stdin=asyncio.subprocess.PIPE if stdin_data else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(input=stdin_data.encode() if stdin_data else None),
                timeout=timeout,
            )
            stdout = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
            stderr = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
            return process.returncode, stdout, stderr
        except asyncio.TimeoutError:
            try:
                process.kill()
            except Exception:
                pass
            return -1, "", f"Command timed out after {timeout}s"
        except FileNotFoundError:
            return -1, "", f"Command not found: {cmd[0] if cmd else 'unknown'}"
        except Exception as e:
            return -1, "", str(e)

    # ── 同步执行辅助 (用于兼容) ────────────────────

    def _run_command_sync(
        self,
        cmd: List[str],
        cwd: str = ".",
        timeout: int = 300,
    ) -> Tuple[int, str, str]:
        """同步运行 shell 命令 (兼容旧代码)"""
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=cwd,
                timeout=timeout,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
        except FileNotFoundError:
            return -1, "", f"Command not found: {cmd[0] if cmd else 'unknown'}"
        except Exception as e:
            return -1, "", str(e)

    @staticmethod
    def _check_command(cmd: str) -> bool:
        """检查命令是否在 PATH 中"""
        return shutil.which(cmd) is not None
