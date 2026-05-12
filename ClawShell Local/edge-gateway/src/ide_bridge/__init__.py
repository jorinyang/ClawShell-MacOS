"""IDE Bridge — Agent CLI IDE 集成模块 (Harness Engineering)

支持多种 Agent CLI IDE 的任务分发与并行执行:
- Claude Code (claude)
- OpenAI Codex (codex)
- Kimi Code (kimi)
- DeepSeek TUI (deepseek)
- GitHub Copilot (copilot)

生态位匹配: 根据任务类型、语言、能力选择最优 IDE
"""

from __future__ import annotations

from .base import (
    BaseIDEBridge,
    IDETask,
    IDEResult,
)
from .orchestrator import IDEOrchestrator
from .claude_code_bridge import ClaudeCodeBridge
from .codex_bridge import CodexBridge
from .kimicoder_bridge import KimiCodeBridge
from .deepseek_bridge import DeepSeekTUIBridge

# 所有可用 Bridge
ALL_BRIDGES = [
    ClaudeCodeBridge(),
    CodexBridge(),
    KimiCodeBridge(),
    DeepSeekTUIBridge(),
]


def create_orchestrator() -> IDEOrchestrator:
    """创建 IDE Orchestrator，自动注册所有可用的 Bridge"""
    orch = IDEOrchestrator()
    for bridge in ALL_BRIDGES:
        orch.register_bridge(bridge)
    return orch


def detect_ide_tools() -> list[str]:
    """检测系统中可用的 Agent CLI IDE"""
    available = []
    for bridge in ALL_BRIDGES:
        if bridge.detect():
            available.append(bridge.get_name())
    return available


__all__ = [
    "BaseIDEBridge",
    "IDETask",
    "IDEResult",
    "IDEOrchestrator",
    "ClaudeCodeBridge",
    "CodexBridge",
    "KimiCodeBridge",
    "DeepSeekTUIBridge",
    "create_orchestrator",
    "detect_ide_tools",
    "ALL_BRIDGES",
]
