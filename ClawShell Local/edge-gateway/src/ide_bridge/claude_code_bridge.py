"""Claude Code CLI Bridge — async 版本

支持 Claude Code CLI (claude), Kimi Code, DeepSeek TUI
"""

from __future__ import annotations

import time
from typing import List

from .base import BaseIDEBridge, IDETask, IDEResult


class ClaudeCodeBridge(BaseIDEBridge):
    """Claude Code CLI Bridge (异步版本)"""

    IDE_NAME = "claude_code"
    CLI_COMMAND = "claude"

    def detect(self) -> bool:
        return self._check_command("claude")

    def get_capabilities(self) -> List[str]:
        return ["code", "debug", "refactor", "review", "test", "explain", "architect"]

    async def invoke_async(self, task: IDETask) -> IDEResult:
        start = time.time()

        if not self.detect():
            return IDEResult(
                task_id=task.task_id,
                ide_name=self.IDE_NAME,
                success=False,
                error="Claude Code CLI not installed",
                duration_seconds=time.time() - start,
            )

        prompt = task.description
        if task.context:
            prompt = f"{task.context}\n\n---\n\nTask: {prompt}"

        cmd = ["claude", "--print", "--output-format", "text", prompt]
        exit_code, stdout, stderr = await self._run_command_async(
            cmd, cwd=task.working_dir or ".", timeout=task.timeout_seconds
        )

        return IDEResult(
            task_id=task.task_id,
            ide_name=self.IDE_NAME,
            success=(exit_code == 0),
            output=stdout[:5000],
            error=stderr[:2000],
            duration_seconds=time.time() - start,
        )

    def invoke(self, task: IDETask) -> IDEResult:
        """同步包装器"""
        return super().invoke(task)


class KimiCodeBridge(BaseIDEBridge):
    """Kimi Code Agent CLI Bridge (异步版本)"""

    IDE_NAME = "kimi_code"
    CLI_COMMAND = "kimi"

    def detect(self) -> bool:
        return self._check_command("kimi")

    def get_capabilities(self) -> List[str]:
        return ["code", "debug", "explain"]

    async def invoke_async(self, task: IDETask) -> IDEResult:
        start = time.time()

        if not self.detect():
            return IDEResult(
                task_id=task.task_id,
                ide_name=self.IDE_NAME,
                success=False,
                error="Kimi Code CLI not installed",
                duration_seconds=time.time() - start,
            )

        cmd = ["kimi", "agent", task.description]
        exit_code, stdout, stderr = await self._run_command_async(
            cmd, cwd=task.working_dir or ".", timeout=task.timeout_seconds
        )

        return IDEResult(
            task_id=task.task_id,
            ide_name=self.IDE_NAME,
            success=(exit_code == 0),
            output=stdout[:5000],
            error=stderr[:2000],
            duration_seconds=time.time() - start,
        )

    def invoke(self, task: IDETask) -> IDEResult:
        """同步包装器"""
        return super().invoke(task)


class DeepSeekTUIBridge(BaseIDEBridge):
    """DeepSeek TUI CLI Bridge (异步版本)"""

    IDE_NAME = "deepseek_tui"
    CLI_COMMAND = "deepseek"

    def detect(self) -> bool:
        return self._check_command("deepseek")

    def get_capabilities(self) -> List[str]:
        return ["code", "debug", "explain", "review"]

    async def invoke_async(self, task: IDETask) -> IDEResult:
        start = time.time()

        if not self.detect():
            return IDEResult(
                task_id=task.task_id,
                ide_name=self.IDE_NAME,
                success=False,
                error="DeepSeek TUI not installed",
                duration_seconds=time.time() - start,
            )

        # DeepSeek TUI 使用 --non-interactive 进行 CLI 模式
        cmd = ["deepseek", "--non-interactive", "--prompt", task.description]
        exit_code, stdout, stderr = await self._run_command_async(
            cmd, cwd=task.working_dir or ".", timeout=task.timeout_seconds
        )

        return IDEResult(
            task_id=task.task_id,
            ide_name=self.IDE_NAME,
            success=(exit_code == 0),
            output=stdout[:5000],
            error=stderr[:2000],
            duration_seconds=time.time() - start,
        )

    def invoke(self, task: IDETask) -> IDEResult:
        """同步包装器"""
        return super().invoke(task)
