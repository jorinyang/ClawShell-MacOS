#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Strategy Switcher
========================================
从 ClawShell-Windows lib/core/strategy/switcher.py 提取重构

核心能力：
- 条件评估 + 策略切换
- 切换历史追踪
- 回调通知
"""

import time, logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field


logger = logging.getLogger(__name__)


@dataclass
class SwitchCondition:
    metric: str; operator: str; threshold: float
    # operator: ">"/"<"/">="/"<="/"=="/"!="
    def evaluate(self, value: float) -> bool:
        ops = {">": lambda a, b: a > b, "<": lambda a, b: a < b,
               ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b,
               "==": lambda a, b: a == b, "!=": lambda a, b: a != b}
        return ops.get(self.operator, lambda a, b: False)(value, self.threshold)


@dataclass
class SwitchRecord:
    from_strategy: str; to_strategy: str; condition: Dict
    triggered_at: float; reason: str = ""


class StrategySwitcher:
    """策略切换器"""
    def __init__(self, registry: Any = None):
        self._registry = registry
        self._active_strategy: Optional[str] = None
        self._switch_conditions: Dict[str, SwitchCondition] = {}
        self._history: List[SwitchRecord] = []
        self._callbacks: List[Callable] = []

    def set_active(self, strategy_name: str):
        prev = self._active_strategy
        self._active_strategy = strategy_name
        if prev and prev != strategy_name:
            self._history.append(SwitchRecord(
                from_strategy=prev, to_strategy=strategy_name,
                condition={}, triggered_at=time.time()))
        for cb in self._callbacks:
            try: cb(prev, strategy_name)
            except: pass

    def get_active(self) -> Optional[str]:
        return self._active_strategy

    def register_condition(self, strategy_name: str, cond: SwitchCondition):
        self._switch_conditions[strategy_name] = cond

    def evaluate_and_switch(self, metric: str, value: float) -> Optional[str]:
        for name, cond in self._switch_conditions.items():
            if cond.evaluate(value):
                if self._active_strategy != name:
                    self.set_active(name)
                    return name
        return None

    def on_switch(self, callback: Callable):
        self._callbacks.append(callback)

    def get_history(self, limit: int = 50) -> List[SwitchRecord]:
        return self._history[-limit:]

    def get_stats(self) -> Dict:
        return {"active": self._active_strategy,
                "conditions_registered": len(self._switch_conditions),
                "total_switches": len(self._history)}
