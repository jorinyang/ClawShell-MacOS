#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Lifecycle Hooks
======================================
从 ClawShell-Windows lib/core/eventbus/lifecycle_hooks.py 提取重构

核心能力：
- 会话生命周期钩子（session_start/session_end/session_error）
- MemPalace 自动记忆保存
"""

import threading, time
from typing import Optional, Dict, Any, Callable


_HOOK_STATE_PATH = "~/.cloudshell/.lifecycle_hooks.json"


class MemPalaceHook:
    """MemPalace 自动记忆钩子"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized: return
        self._initialized = True
        self._hooks: Dict[str, list] = {
            "session_start": [], "session_end": [], "session_error": []}
        self._history: list = []

    def on(self, event_type: str, callback: Callable):
        if event_type in self._hooks:
            self._hooks[event_type].append(callback)

    def trigger(self, event_type: str, context: Optional[Dict[str, Any]] = None):
        context = context or {}
        for cb in self._hooks.get(event_type, []):
            try: cb(context)
            except Exception: pass
        self._history.append({"type": event_type, "context": context,
                             "timestamp": time.time()})

    def get_history(self, limit: int = 50) -> list:
        return self._history[-limit:]

    def clear_history(self):
        self._history = []
