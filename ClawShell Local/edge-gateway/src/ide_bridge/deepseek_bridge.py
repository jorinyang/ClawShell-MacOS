"""DeepSeek TUI Bridge — 独立模块 (异步版本)

DeepSeek TUI 是 DeepSeek 推出的终端 AI 编码界面
"""

from __future__ import annotations

import time
from typing import List

from .base import BaseIDEBridge, IDETask, IDEResult


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
