#!/usr/bin/env python3
"""
ClawShell 条件触发引擎 - 简化版
保留：Condition/ConditionTrigger、阈值/组合/逆向评估、ACTION_REGISTRY
"""

import re, time, operator
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum


# ============ 条件类型 ============

class ConditionType(Enum):
    THRESHOLD = "threshold"       # 阈值比较
    CHANGE = "change"             # 变化检测
    TIME_WINDOW = "time_window"   # 时间窗口
    COMPOSITE = "composite"       # 组合条件 (AND/OR)
    NEGATION = "negation"         # 逆向条件 (从坏变好)


# ============ 数据结构 ============

@dataclass
class Condition:
    type: str
    target_metric: str
    comparison: str = ">"
    threshold: float = 0.0
    time_window: Optional[int] = None
    consecutive: bool = True
    expression: str = ""
    target_metrics: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "Condition":
        return cls(**data)


@dataclass
class ConditionTrigger:
    id: str
    name: str
    condition: Condition
    action_type: str
    action_config: Dict = field(default_factory=dict)
    cooldown: int = 60
    last_triggered: Optional[float] = None
    triggered_count: int = 0
    enabled: bool = True
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "id": self.id, "name": self.name,
            "condition": self.condition.to_dict(),
            "action_type": self.action_type,
            "action_config": self.action_config,
            "cooldown": self.cooldown,
            "last_triggered": self.last_triggered,
            "triggered_count": self.triggered_count,
            "enabled": self.enabled, "tags": self.tags
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "ConditionTrigger":
        data["condition"] = Condition.from_dict(data["condition"])
        return cls(**data)


# ============ 动作注册表 ============

class TriggerActions:
    @staticmethod
    def send_alert(trigger, ctx): print(f"🚨 ALERT: {trigger.name} | Value: {ctx.get('current_value')}")
    @staticmethod
    def switch_strategy(trigger, ctx): print(f"🔄 Strategy: {trigger.action_config.get('strategy')}")
    @staticmethod
    def scale_up(trigger, ctx): print(f"📈 Scale up: {trigger.name}")
    @staticmethod
    def scale_down(trigger, ctx): print(f"📉 Scale down: {trigger.name}")
    @staticmethod
    def switch_to_backup(trigger, ctx): print(f"🔀 Backup: {trigger.name}")
    @staticmethod
    def restore_normal(trigger, ctx): print(f"✅ Normal: {trigger.name}")
    @staticmethod
    def log_event(trigger, ctx): print(f"📝 Log: {trigger.name}")


ACTION_REGISTRY: Dict[str, Callable] = {
    "send_alert": TriggerActions.send_alert,
    "switch_strategy": TriggerActions.switch_strategy,
    "scale_up": TriggerActions.scale_up,
    "scale_down": TriggerActions.scale_down,
    "switch_to_backup": TriggerActions.switch_to_backup,
    "restore_normal": TriggerActions.restore_normal,
    "log_event": TriggerActions.log_event,
}


# ============ 运算符映射 ============

OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq, "!=": operator.ne}


# ============ 条件评估器 ============

class ConditionEvaluator:
    """条件评估器：阈值/变化/组合/逆向/时间窗口评估"""

    def __init__(self):
        self.metrics_cache: Dict[str, Dict[str, Any]] = {}

    def update_metric(self, name: str, value: Any, timestamp: Optional[float] = None):
        ts = timestamp or time.time()
        old = self.metrics_cache.get(name, {}).get("value")
        self.metrics_cache[name] = {
            "value": value, "timestamp": ts,
            "old_value": old, "delta": abs(value - old) if old is not None else None
        }

    def evaluate_condition(self, cond: Condition, metric_name: str) -> bool:
        m = self.metrics_cache.get(metric_name, {})
        cur, old = m.get("value"), m.get("old_value")
        if cur is None:
            return False

        ct = ConditionType(cond.type)
        if ct == ConditionType.THRESHOLD:
            return self._eval_threshold(cond, cur)
        if ct == ConditionType.CHANGE:
            d = m.get("delta")
            return self._eval_threshold(cond, d) if d is not None else False
        if ct == ConditionType.COMPOSITE:
            return self._eval_composite(cond)
        if ct == ConditionType.NEGATION:
            return self._eval_negation(cond, old, cur)
        if ct == ConditionType.TIME_WINDOW:
            return self._eval_time_window(cond, m.get("timestamp"))
        return False

    def _eval_threshold(self, cond: Condition, value: Any) -> bool:
        try:
            return OPS.get(cond.comparison, operator.gt)(float(value), float(cond.threshold))
        except (TypeError, ValueError):
            return False

    def _eval_composite(self, cond: Condition) -> bool:
        if not cond.expression:
            return False
        expr = cond.expression
        for mn in cond.target_metrics:
            v = self.metrics_cache.get(mn, {}).get("value", 0)
            expr = re.sub(rf'\b{mn}\b', str(float(v)), expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bAND\b', ' and ', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bOR\b', ' or ', expr, flags=re.IGNORECASE)
        try:
            if re.match(r'^[\d\.\s\+\-\*\/\(\)\>\<\=\&\|\andor]+$', expr, re.IGNORECASE):
                return eval(expr)
        except Exception:
            pass
        return False

    def _eval_negation(self, cond: Condition, old: Any, cur: Any) -> bool:
        try:
            ov, cv, t = float(old), float(cur), float(cond.threshold)
            return ov > t and cv <= t
        except (TypeError, ValueError):
            return False

    def _eval_time_window(self, cond: Condition, ts: Optional[float]) -> bool:
        if ts is None or cond.time_window is None:
            return False
        return time.time() - ts <= cond.time_window

    def execute_action(self, trigger: ConditionTrigger, metric_name: str):
        m = self.metrics_cache.get(metric_name, {})
        if fn := ACTION_REGISTRY.get(trigger.action_type):
            fn(trigger, {
                "trigger_id": trigger.id, "metric_name": metric_name,
                "current_value": m.get("value"), "old_value": m.get("old_value"),
                "threshold": trigger.condition.threshold, "timestamp": time.time()
            })
        trigger.last_triggered = time.time()
        trigger.triggered_count += 1

    def check_cooldown(self, trigger: ConditionTrigger) -> bool:
        if trigger.last_triggered is None:
            return False
        return time.time() - trigger.last_triggered < trigger.cooldown
