"""OpenAI Codex CLI Bridge — async 版本

支持 Codex CLI 和 GitHub Copilot CLI
"""

from __future__ import annotations

import time
from typing import List

from .base import BaseIDEBridge, IDETask, IDEResult


class CodexBridge(BaseIDEBridge):
    """OpenAI Codex CLI Bridge (异步版本)"""

    IDE_NAME = "codex"
    CLI_COMMAND = "codex"

    def detect(self) -> bool:
        return self._check_command("codex")

    def get_capabilities(self) -> List[str]:
        return ["code", "debug", "refactor", "review", "test", "explain"]

    async def invoke_async(self, task: IDETask) -> IDEResult:
        start = time.time()

        if not self.detect():
            return IDEResult(
                task_id=task.task_id,
                ide_name=self.IDE_NAME,
                success=False,
                error="Codex CLI not installed",
                duration_seconds=time.time() - start,
            )

        # 构建 prompt
        prompt = f"Task: {task.description}"
        if task.context:
            prompt += f"\n\nContext: {task.context}"
        if task.files:
            prompt += f"\n\nRelevant files: {', '.join(task.files)}"

        cmd = ["codex", "exec", prompt]
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


class CopilotBridge(BaseIDEBridge):
    """GitHub Copilot CLI Bridge (异步版本, 通过 --acp 协议)"""

    IDE_NAME = "copilot"
    CLI_COMMAND = "copilot"

    def detect(self) -> bool:
        return self._check_command("copilot")

    def get_capabilities(self) -> List[str]:
        return ["code", "explain", "suggest"]

    async def invoke_async(self, task: IDETask) -> IDEResult:
        start = time.time()

        if not self.detect():
            return IDEResult(
                task_id=task.task_id,
                ide_name=self.IDE_NAME,
                success=False,
                error="Copilot CLI not installed",
                duration_seconds=time.time() - start,
            )

        prompt = task.description
        cmd = ["copilot", "--acp", "--stdio"]
        exit_code, stdout, stderr = await self._run_command_async(
            cmd, cwd=task.working_dir or ".", timeout=task.timeout_seconds, stdin_data=prompt
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
