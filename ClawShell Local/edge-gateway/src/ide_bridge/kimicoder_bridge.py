"""Kimi Code Bridge — 独立模块 (异步版本)

Kimi Code 是月之暗面推出的 AI 编码助手
"""

from __future__ import annotations

import time
from typing import List

from .base import BaseIDEBridge, IDETask, IDEResult


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
