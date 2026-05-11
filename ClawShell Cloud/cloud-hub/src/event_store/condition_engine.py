#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Condition Engine
========================================
从 ClawShell-Windows lib/core/eventbus/condition_engine.py 提取重构

核心能力：
- 条件类型: THRESHOLD/CHANGE/RATE/TIMEOUT/PATTERN/AND/OR/NOT
- 条件组: 组合条件（AND/OR/NOT）
- 规则订阅: 条件满足时触发回调
- 评估历史记录
"""

import json, time, re
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
import operator


class ConditionType(Enum):
    THRESHOLD = "threshold"; CHANGE = "change"; RATE = "rate"
    TIMEOUT = "timeout"; PATTERN = "pattern"
    AND = "and"; OR = "or"; NOT = "not"


OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge,
       "<=": operator.le, "==": operator.eq, "!=": operator.ne}


@dataclass
class Condition:
    type: str; metric: str = ""; operator: str = ">"
    threshold: float = 0; window: float = 60; pattern: str = ""
    conditions: List["Condition"] = field(default_factory=list)
    def to_dict(self) -> Dict: return asdict(self)


@dataclass
class Rule:
    rule_id: str; name: str; condition: Condition
    action: str = ""; action_params: Dict = field(default_factory=dict)
    enabled: bool = True; last_triggered: float = 0
    trigger_count: int = 0


class ConditionEngine:
    """条件触发引擎"""
    def __init__(self):
        self._rules: Dict[str, Rule] = {}
        self._state: Dict[str, float] = defaultdict(float)
        self._history: Dict[str, List[Dict]] = defaultdict(list)

    def add_rule(self, rule: Rule) -> str:
        self._rules[rule.rule_id] = rule; return rule.rule_id

    def evaluate(self, rule_id: str, current_value: Any) -> bool:
        rule = self._rules.get(rule_id)
        if not rule or not rule.enabled: return False
        result = self._eval_condition(rule.condition, float(current_value))
        if result:
            rule.last_triggered = time.time()
            rule.trigger_count += 1
        return result

    def _eval_condition(self, cond: Condition, value: float) -> bool:
        if cond.type == "threshold":
            op = OPS.get(cond.operator, operator.gt)
            return op(value, cond.threshold)
        if cond.type == "change":
            prev = self._state.get(cond.metric, 0)
            return abs(value - prev) > cond.threshold
        if cond.type == "rate":
            now = time.time()
            self._history[cond.metric].append({"time": now, "value": value})
            cutoff = now - cond.window
            self._history[cond.metric] = [
                h for h in self._history[cond.metric] if h["time"] >= cutoff]
            if len(self._history[cond.metric]) < 2: return False
            rate = (self._history[cond.metric][-1]["value"] -
                    self._history[cond.metric][0]["value"]) / cond.window
            return abs(rate) > cond.threshold
        if cond.type == "and":
            return all(self._eval_condition(c, value) for c in cond.conditions)
        if cond.type == "or":
            return any(self._eval_condition(c, value) for c in cond.conditions)
        if cond.type == "not":
            return not any(self._eval_condition(c, value) for c in cond.conditions)
        return False

    def set_state(self, metric: str, value: float):
        self._state[metric] = value

    def get_state(self, metric: str) -> float:
        return self._state.get(metric, 0)

    def get_rule(self, rule_id: str) -> Optional[Rule]:
        return self._rules.get(rule_id)

    def list_rules(self) -> List[Rule]:
        return list(self._rules.values())

    def remove_rule(self, rule_id: str) -> bool:
        return bool(self._rules.pop(rule_id, None))

    def get_stats(self) -> Dict:
        return {"total_rules": len(self._rules),
                "enabled_rules": sum(1 for r in self._rules.values() if r.enabled),
                "total_triggers": sum(r.trigger_count for r in self._rules.values())}
