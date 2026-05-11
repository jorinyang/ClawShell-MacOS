"""
ClawShell Cloud Hub — AdaptiveDomain
====================================

自适应引擎（从 ClawShell-Windows lib/core/ 提取重构）

核心能力：
- ConditionEngine: 条件规则 DSL 求值
- StrategySwitcher: 策略加载 + 自动切换
- SelfHealing: 系统自愈（诊断 → 修复 → 验证）

所有操作产生事件 → EventStore 持久化
"""

import asyncio
import json
import logging
import operator
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..event_store.schema import Event, Topic
from ..event_store.store import OssEventStore
from ..pubsub.manager import PubSubManager

logger = logging.getLogger("adaptive_domain")


# ─── Condition Engine Types ────────────────────────────────────────────────────

class ComparisonOp(str, Enum):
    GT = ">"
    LT = "<"
    GE = ">="
    LE = "<="
    EQ = "=="
    NE = "!="

    def evaluate(self, left: Any, right: Any) -> bool:
        ops = {
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
            "==": operator.eq,
            "!=": operator.ne,
        }
        return ops[self.value](left, right)


class ConditionType(str, Enum):
    THRESHOLD = "threshold"     # value > threshold
    CHANGE = "change"           # |delta| > threshold
    TIME_WINDOW = "time_window" # within time range
    COMPOSITE = "composite"     # AND/OR expression
    NEGATION = "negation"      # 从坏变好


@dataclass
class Condition:
    """单个条件定义"""
    type: str
    target_metric: str
    comparison: str = ">"
    threshold: float = 0.0
    time_window: Optional[int] = None
    expression: str = ""
    target_metrics: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "target_metric": self.target_metric,
            "comparison": self.comparison,
            "threshold": self.threshold,
            "time_window": self.time_window,
            "expression": self.expression,
            "target_metrics": self.target_metrics,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Condition":
        return cls(
            type=d["type"],
            target_metric=d.get("target_metric", ""),
            comparison=d.get("comparison", ">"),
            threshold=d.get("threshold", 0.0),
            time_window=d.get("time_window"),
            expression=d.get("expression", ""),
            target_metrics=d.get("target_metrics", []),
        )


@dataclass
class HealthReport:
    """健康诊断报告"""
    healthy: bool
    score: float  # 0-1
    issues: List[str]
    recommendations: List[str]


# ─── Condition Engine ───────────────────────────────────────────────────────────

class ConditionEngine:
    """
    条件引擎：评估指标是否满足条件。

    支持条件类型：
    - threshold:  value > threshold
    - change:    |current - previous| > threshold
    - time_window: 时间窗口内满足
    - composite:  AND/OR 组合表达式
    - negation:   逆向条件（从坏变好）
    """

    OP_MAP = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }

    def evaluate(self, condition: Condition, context: Dict[str, Any]) -> Tuple[bool, float]:
        """
        评估条件。

        Returns:
            (satisfied, score) — score 是 0.0-1.0 的满足程度
        """
        try:
            if condition.type == ConditionType.THRESHOLD:
                return self._eval_threshold(condition, context)
            elif condition.type == ConditionType.CHANGE:
                return self._eval_change(condition, context)
            elif condition.type == ConditionType.COMPOSITE:
                return self._eval_composite(condition, context)
            elif condition.type == ConditionType.NEGATION:
                return self._eval_negation(condition, context)
            else:
                return False, 0.0
        except Exception as e:
            logger.warning(f"Condition evaluation error: {e}")
            return False, 0.0

    def _eval_threshold(self, cond: Condition, ctx: Dict) -> Tuple[bool, float]:
        value = ctx.get(cond.target_metric, 0)
        op = self.OP_MAP.get(cond.comparison, operator.gt)
        satisfied = op(value, cond.threshold)
        # score: 离 threshold 越远越满足
        if cond.threshold == 0:
            score = 1.0 if satisfied else 0.0
        else:
            score = min(abs(value) / abs(cond.threshold), 1.0) if satisfied else 0.0
        return satisfied, score

    def _eval_change(self, cond: Condition, ctx: Dict) -> Tuple[bool, float]:
        current = ctx.get(cond.target_metric, 0)
        previous = ctx.get(f"_prev_{cond.target_metric}", current)
        delta = abs(current - previous)
        satisfied = delta > cond.threshold
        score = min(delta / (cond.threshold * 2), 1.0) if satisfied else 0.0
        return satisfied, score

    def _eval_composite(self, cond: Condition, ctx: Dict) -> Tuple[bool, float]:
        """评估组合条件: "metric1 > 0.5 AND metric2 < 0.3" """
        expr = cond.expression
        # 简单解析: 替换 metric 占位符为值
        def replacer(m):
            metric = m.group(1)
            return str(ctx.get(metric, 0))
        # 替换 {metric_name} 为数值
        expr_clean = re.sub(r'\{(\w+)\}', replacer, expr)
        # 替换 AND/OR
        expr_clean = expr_clean.replace("AND", " and ").replace("OR", " or ")
        # 只支持比较运算
        result = bool(eval(expr_clean, {"__builtins__": {}}, {}))
        return result, 1.0 if result else 0.0

    def _eval_negation(self, cond: Condition, ctx: Dict) -> Tuple[bool, float]:
        """逆向：从坏变好"""
        current = ctx.get(cond.target_metric, 0)
        previous = ctx.get(f"_prev_{cond.target_metric}", current)
        improved = current < previous
        satisfied = improved and abs(current - previous) > cond.threshold
        return satisfied, 1.0 if satisfied else 0.0


# ─── Strategy Switcher ────────────────────────────────────────────────────────

class StrategyType(str, Enum):
    DEFAULT = "default"
    ECONOMY = "economy"
    PERFORMANCE = "performance"
    RECOVERY = "recovery"
    EMERGENCY = "emergency"


@dataclass
class Strategy:
    name: str
    type: str
    priority: int = 0
    cooldown: int = 60
    conditions: List[Condition] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class StrategySwitcher:
    """
    策略切换器。

    根据条件自动或手动切换策略。
    策略定义了不同运行模式下的行为集合。
    """

    def __init__(self):
        self._strategies: Dict[str, Strategy] = {}
        self._current: str = StrategyType.DEFAULT.value
        self._last_switch_time: float = 0
        # 默认内置策略
        self._register_defaults()

    def _register_defaults(self):
        """注册内置默认策略"""
        defaults = [
            Strategy(
                name=StrategyType.DEFAULT.value,
                type=StrategyType.DEFAULT.value,
                priority=0,
                conditions=[],
                actions=["mode:normal"],
            ),
            Strategy(
                name=StrategyType.ECONOMY.value,
                type=StrategyType.ECONOMY.value,
                priority=1,
                cooldown=300,
                conditions=[
                    Condition(type=ConditionType.THRESHOLD,
                             target_metric="cpu_usage", comparison="<", threshold=20.0),
                ],
                actions=["schedule:low_power"],
            ),
            Strategy(
                name=StrategyType.RECOVERY.value,
                type=StrategyType.RECOVERY.value,
                priority=3,
                cooldown=120,
                conditions=[
                    Condition(type=ConditionType.THRESHOLD,
                             target_metric="error_rate", comparison=">", threshold=0.05),
                ],
                actions=["mode:repair", "alert:high"],
            ),
            Strategy(
                name=StrategyType.EMERGENCY.value,
                type=StrategyType.EMERGENCY.value,
                priority=10,
                cooldown=60,
                conditions=[
                    Condition(type=ConditionType.THRESHOLD,
                             target_metric="error_rate", comparison=">", threshold=0.20),
                ],
                actions=["mode:emergency", "alert:critical"],
            ),
        ]
        for s in defaults:
            self._strategies[s.name] = s

    def get_current(self) -> str:
        return self._current

    def switch_to(self, name: str) -> bool:
        """手动切换到指定策略"""
        if name not in self._strategies:
            return False
        self._current = name
        self._last_switch_time = datetime.now().timestamp()
        logger.info(f"Strategy switched to: {name}")
        return True

    def evaluate_and_switch(self, metrics: Dict[str, Any]) -> Tuple[str, bool]:
        """
        评估所有策略条件，自动切换。

        Returns:
            (strategy_name, switched)
        """
        now = datetime.now().timestamp()

        # 按优先级排序（高优先级优先）
        candidates = sorted(
            [(k, v) for k, v in self._strategies.items() if k != self._current],
            key=lambda x: x[1].priority,
            reverse=True,
        )

        engine = ConditionEngine()
        for name, strategy in candidates:
            # 检查 cooldown
            if now - self._last_switch_time < strategy.cooldown:
                continue
            # 评估所有条件
            all_satisfied = True
            for cond in strategy.conditions:
                sat, _ = engine.evaluate(cond, metrics)
                if not sat:
                    all_satisfied = False
                    break
            if all_satisfied:
                self._current = name
                self._last_switch_time = now
                logger.info(f"Auto-switched strategy to: {name}")
                return name, True

        return self._current, False


# ─── SelfHealing ──────────────────────────────────────────────────────────────

@dataclass
class HealingAction:
    action: str
    description: str
    script: str
    args: Dict[str, Any] = field(default_factory=dict)


class SelfHealing:
    """
    自修复系统。

    诊断 → 识别 → 修复 → 验证 闭环。
    """

    # 预定义修复动作库
    HEALING_ACTIONS = {
        "restart_component": HealingAction(
            action="restart_component",
            description="重启指定组件",
            script="systemctl restart {component}",
        ),
        "clear_cache": HealingAction(
            action="clear_cache",
            description="清理缓存",
            script="rm -rf ~/.cache/*",
        ),
        "restart_hermes": HealingAction(
            action="restart_hermes",
            description="重启 Hermes Gateway",
            script="launchctl kickstart -k gui/{uid}/ai.hermes.gateway",
        ),
        "increase_memory_limit": HealingAction(
            action="increase_memory_limit",
            description="增加内存限制",
            script="sysctl -w {param}={value}",
        ),
    }

    def diagnose(self, metrics: Dict[str, Any]) -> HealthReport:
        """
        诊断系统健康状态。

        检查维度：cpu / memory / disk / error_rate / latency
        """
        issues = []
        recommendations = []
        score = 1.0

        if metrics.get("cpu_usage", 0) > 90:
            issues.append("CPU 使用率过高 (>90%)")
            recommendations.append("考虑切换到 economy 策略或增加计算资源")
            score -= 0.2

        if metrics.get("memory_usage", 0) > 85:
            issues.append("内存使用率过高 (>85%)")
            recommendations.append("清理缓存或增加内存")
            score -= 0.2

        if metrics.get("error_rate", 0) > 0.05:
            issues.append(f"错误率过高 ({metrics['error_rate']:.1%})")
            recommendations.append("检查最近部署的变更并回滚")
            score -= 0.3

        if metrics.get("disk_usage", 0) > 90:
            issues.append("磁盘使用率过高 (>90%)")
            recommendations.append("清理临时文件和旧日志")
            score -= 0.15

        if metrics.get("latency_p99", 0) > 1000:
            issues.append(f"P99 延迟过高 ({metrics['latency_p99']}ms)")
            recommendations.append("检查网络或数据库性能瓶颈")
            score -= 0.15

        return HealthReport(
            healthy=len(issues) == 0,
            score=max(score, 0.0),
            issues=issues,
            recommendations=recommendations,
        )

    def get_healing_plan(self, report: HealthReport) -> List[HealingAction]:
        """根据诊断报告生成修复计划"""
        plan = []

        if "CPU 使用率过高" in " ".join(report.issues):
            plan.append(self.HEALING_ACTIONS["clear_cache"])

        if "错误率过高" in " ".join(report.issues):
            plan.append(self.HEALING_ACTIONS.get("restart_hermes",
                      self.HEALING_ACTIONS["restart_component"]))

        return plan


# ─── Domain ───────────────────────────────────────────────────────────────────

class AdaptiveDomain:
    """
    自适应 Domain。

    整合 ConditionEngine + StrategySwitcher + SelfHealing。

    事件类型：
    - adaptive.strategy_switch  → 策略切换
    - adaptive.health_check     → 健康检查结果
    - adaptive.heal_request     → 修复请求
    - adaptive.heal_complete    → 修复完成
    """

    def __init__(self, store: OssEventStore, pubsub: PubSubManager):
        self.store = store
        self.pubsub = pubsub
        self.condition_engine = ConditionEngine()
        self.strategy_switcher = StrategySwitcher()
        self.self_healing = SelfHealing()
        self._health_history: List[HealthReport] = []

    async def _emit(self, topic: str, data: dict, source: str = "cloud-hub"):
        ev = Event.make(topic, source, data)
        await self.store.append(ev)
        self.pubsub.publish(ev)
        return ev

    # ─── API ─────────────────────────────────────────────────────────────────

    async def health_check(self, params: dict) -> dict:
        """health_check: 健康诊断"""
        metrics = params.get("metrics", {})
        report = self.self_healing.diagnose(metrics)
        self._health_history.append(report)

        await self._emit("adaptive.health_check", {
            "healthy": report.healthy,
            "score": report.score,
            "issues": report.issues,
            "recommendations": report.recommendations,
        })

        return {
            "success": True,
            "healthy": report.healthy,
            "score": report.score,
            "issues": report.issues,
            "recommendations": report.recommendations,
        }

    async def rule_evaluate(self, params: dict) -> dict:
        """rule_evaluate: 评估条件规则"""
        cond_data = params.get("condition", {})
        context = params.get("context", {})

        cond = Condition.from_dict(cond_data)
        satisfied, score = self.condition_engine.evaluate(cond, context)

        return {"success": True, "satisfied": satisfied, "score": score}

    async def strategy_switch(self, params: dict) -> dict:
        """strategy_switch: 手动或自动切换策略"""
        mode = params.get("mode", "auto")

        if mode == "manual":
            name = params.get("strategy_name", "default")
            ok = self.strategy_switcher.switch_to(name)
            if ok:
                await self._emit("adaptive.strategy_switch", {
                    "strategy": name, "mode": "manual",
                })
            return {"success": ok, "current": self.strategy_switcher.get_current()}
        else:
            metrics = params.get("metrics", {})
            name, switched = self.strategy_switcher.evaluate_and_switch(metrics)
            if switched:
                await self._emit("adaptive.strategy_switch", {
                    "strategy": name, "mode": "auto",
                })
            return {"success": True, "current": name, "switched": switched}

    async def system_heal(self, params: dict) -> dict:
        """system_heal: 执行自修复"""
        metrics = params.get("metrics", {})
        report = self.self_healing.diagnose(metrics)

        if report.healthy:
            return {"success": True, "action": "none", "message": "System healthy"}

        plan = self.self_healing.get_healing_plan(report)

        await self._emit("adaptive.heal_request", {
            "issues": report.issues,
            "plan": [a.action for a in plan],
        })

        # 执行修复动作（这里只是记录，实际执行由 Edge 侧负责）
        for action in plan:
            logger.info(f"Healing action: {action.description}")

        await self._emit("adaptive.heal_complete", {
            "issues_resolved": report.issues,
            "actions_taken": [a.action for a in plan],
        })

        return {
            "success": True,
            "action": "planned",
            "plan": [{"action": a.action, "description": a.description} for a in plan],
            "report": {
                "healthy": report.healthy,
                "score": report.score,
            }
        }

    async def get_current_strategy(self, params: dict) -> dict:
        """get_current_strategy: 获取当前策略"""
        return {"success": True, "strategy": self.strategy_switcher.get_current()}
