"""IDE Orchestrator — 多 IDE 任务匹配与并行执行 (异步版本)

Harness Engineering: 根据任务类型、语言和 IDE 能力进行生态位匹配 (ecological niche matching),
在多个 IDE 上并行执行任务,收集结果并聚合。
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional

from .base import BaseIDEBridge, IDETask, IDEResult


# IDE 能力矩阵 (生态位)
IDE_CAPABILITY_MATRIX = {
    "codex": {
        "primary": ["code", "debug", "refactor"],
        "languages": ["*"],
        "strength": "full_stack",
    },
    "claude_code": {
        "primary": ["architect", "code", "review", "refactor"],
        "languages": ["*"],
        "strength": "architecture",
    },
    "kimi_code": {
        "primary": ["code", "debug"],
        "languages": ["python", "javascript", "typescript"],
        "strength": "rapid_prototyping",
    },
    "deepseek_tui": {
        "primary": ["code", "review", "explain"],
        "languages": ["*"],
        "strength": "code_generation",
    },
    "copilot": {
        "primary": ["code", "suggest", "explain"],
        "languages": ["*"],
        "strength": "inline_suggestions",
    },
}


class IDEOrchestrator:
    """多 IDE 任务编排器 (异步版本)"""

    def __init__(self):
        self._bridges: Dict[str, BaseIDEBridge] = {}
        self._results: List[IDEResult] = []
        self._max_workers = 3

    def register_bridge(self, bridge: BaseIDEBridge):
        """注册 IDE Bridge"""
        self._bridges[bridge.get_name()] = bridge

    def detect_available_ides(self) -> List[str]:
        """检测已安装的 IDE"""
        available = []
        for name, bridge in self._bridges.items():
            if bridge.detect():
                available.append(name)
        return available

    def get_available_bridges(self) -> Dict[str, BaseIDEBridge]:
        """获取所有已检测到的 Bridge"""
        available = {}
        for name, bridge in self._bridges.items():
            if bridge.detect():
                available[name] = bridge
        return available

    def match_ide(self, task: IDETask) -> List[str]:
        """使用生态位匹配将任务分配给最佳 IDE"""
        available = self.get_available_bridges()
        scores = []

        for name, bridge in available.items():
            caps = bridge.get_capabilities()
            profile = IDE_CAPABILITY_MATRIX.get(name, {})

            score = 0
            # 任务类型匹配
            if task.task_type in caps:
                score += 3
            if task.task_type in profile.get("primary", []):
                score += 5

            # 语言匹配
            if task.language:
                plangs = profile.get("languages", ["*"])
                if "*" in plangs or task.language in plangs:
                    score += 2

            # 优先级加成
            score += task.priority * 0.1

            scores.append((name, score))

        # 按分数降序排序
        scores.sort(key=lambda x: x[1], reverse=True)
        return [name for name, score in scores if score > 0]

    async def execute_async(self, task: IDETask, ide_names: Optional[List[str]] = None) -> IDEResult:
        """异步执行任务到最佳匹配 IDE"""
        if ide_names is None:
            ide_names = self.match_ide(task)

        if not ide_names:
            return IDEResult(
                task_id=task.task_id,
                ide_name="none",
                success=False,
                error="No matching IDE available",
            )

        # 使用最佳匹配 IDE
        best_ide = ide_names[0]
        bridge = self._bridges.get(best_ide)
        if not bridge:
            return IDEResult(
                task_id=task.task_id,
                ide_name="unknown",
                success=False,
                error=f"IDE '{best_ide}' not registered",
            )

        result = await bridge.invoke_async(task)
        self._results.append(result)
        return result

    def execute(self, task: IDETask, ide_names: Optional[List[str]] = None) -> IDEResult:
        """同步包装器"""
        return asyncio.run(self.execute_async(task, ide_names))

    async def execute_parallel_async(self, tasks: List[IDETask]) -> List[IDEResult]:
        """异步并行执行多个任务"""
        results = []
        semaphore = asyncio.Semaphore(self._max_workers)

        async def run_task(task: IDETask) -> IDEResult:
            async with semaphore:
                ide_names = self.match_ide(task)
                if ide_names:
                    bridge = self._bridges.get(ide_names[0])
                    if bridge:
                        return await bridge.invoke_async(task)
                return IDEResult(
                    task_id=task.task_id,
                    ide_name="none",
                    success=False,
                    error="No matching IDE available",
                )

        futures = [run_task(task) for task in tasks]
        results = await asyncio.gather(*futures, return_exceptions=True)

        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(IDEResult(
                    task_id=tasks[i].task_id,
                    ide_name="error",
                    success=False,
                    error=str(result),
                ))
            else:
                processed_results.append(result)
                self._results.append(result)

        return processed_results

    def execute_parallel(self, tasks: List[IDETask]) -> List[IDEResult]:
        """并行执行同步包装器"""
        return asyncio.run(self.execute_parallel_async(tasks))

    def get_results(self, limit: int = 50) -> List[IDEResult]:
        """获取最近执行结果"""
        return self._results[-limit:]

    def get_stats(self) -> dict:
        """获取编排器统计信息"""
        results = self._results
        total = len(results)
        successful = sum(1 for r in results if r.success)
        return {
            "total_tasks": total,
            "successful": successful,
            "failed": total - successful,
            "success_rate": round(successful / total * 100, 1) if total else 0,
            "available_ides": self.detect_available_ides(),
        }
