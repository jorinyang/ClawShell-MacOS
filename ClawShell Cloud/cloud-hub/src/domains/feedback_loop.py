"""
ClawShell Cloud Hub — FeedbackControlLoop
=========================================

反馈控制环（从 ClawShell-Windows lib/core/ 提取重构）

核心能力：
- MetricCollector: 指标收集（CPU/内存/延迟/错误率）
- FeedbackEvaluator: 闭环评估（基于 ConditionEngine）
- ControlSignal: 生成控制信号（扩容/缩容/切换策略）

事件类型：
- feedback.metric_collected   → 指标收集完成
- feedback.evaluation_done    → 评估完成
- feedback.control_signal    → 控制信号发出
- feedback.threshold_breach  → 阈值突破告警
"""

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ..event_store.schema import Event, Topic
from ..event_store.store import OssEventStore
from ..pubsub.manager import PubSubManager

from .adaptive import (
    AdaptiveDomain, ConditionEngine, Condition,
    ConditionType, HealthReport,
)
from .swarm import SwarmDomain, NodeRegistry

logger = logging.getLogger("feedback_loop")


# ─── Control Signal Types ────────────────────────────────────────────────────

class ControlAction(str, Enum):
    SCALE_UP = "scale_up"           # 增加节点/资源
    SCALE_DOWN = "scale_down"       # 减少节点/资源
    STRATEGY_SWITCH = "strategy_switch"  # 切换策略
    ALERT = "alert"                 # 告警
    DRAIN = "drain"                 # 排空/下线
    HEAL = "heal"                   # 自愈触发
    THROTTLE = "throttle"           # 限流
    NOOP = "noop"                   # 无操作


@dataclass
class ControlSignal:
    """控制信号"""
    signal_id: str
    action: str
    target: str                     # 节点ID / 策略名 / 域名
    reason: str
    params: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    executed: bool = False
    execution_result: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "action": self.action,
            "target": self.target,
            "reason": self.reason,
            "params": self.params,
            "priority": self.priority,
            "created_at": self.created_at,
            "executed": self.executed,
            "execution_result": self.execution_result,
        }


@dataclass
class MetricSample:
    """指标采样"""
    metric_name: str
    value: float
    unit: str = ""
    timestamp: float = field(default_factory=time.time)
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "value": self.value,
            "unit": self.unit,
            "timestamp": self.timestamp,
            "tags": self.tags,
        }


# ─── Metric Collector ─────────────────────────────────────────────────────────

class MetricCollector:
    """
    指标收集器。

    从系统/应用层收集关键指标：
    - cpu_usage, memory_usage, disk_usage
    - request_latency_p99, request_count
    - error_rate, success_rate
    - active_nodes, pending_tasks
    """

    def __init__(self):
        self._samples: Dict[str, List[MetricSample]] = {}
        self._windows: Dict[str, int] = {}  # metric_name -> window seconds

    def register_metric(self, metric_name: str, window_seconds: int = 60):
        """注册指标及滑动窗口大小"""
        self._windows[metric_name] = window_seconds
        if metric_name not in self._samples:
            self._samples[metric_name] = []

    def record(self, metric_name: str, value: float, unit: str = "", tags: Dict[str, str] = None):
        """记录指标采样"""
        if metric_name not in self._samples:
            self.register_metric(metric_name)
        sample = MetricSample(
            metric_name=metric_name,
            value=value,
            unit=unit,
            timestamp=time.time(),
            tags=tags or {},
        )
        self._samples[metric_name].append(sample)
        # 清理过期样本（保留 2x window）
        window = self._windows.get(metric_name, 60)
        cutoff = time.time() - window * 2
        self._samples[metric_name] = [
            s for s in self._samples[metric_name] if s.timestamp > cutoff
        ]

    def get_current(self, metric_name: str) -> Optional[float]:
        """获取最新值"""
        samples = self._samples.get(metric_name, [])
        if not samples:
            return None
        return samples[-1].value

    def get_average(self, metric_name: str, window_seconds: Optional[int] = None) -> Optional[float]:
        """获取滑动窗口平均值"""
        samples = self._samples.get(metric_name, [])
        if not samples:
            return None
        window = window_seconds or self._windows.get(metric_name, 60)
        cutoff = time.time() - window
        window_samples = [s.value for s in samples if s.timestamp > cutoff]
        return sum(window_samples) / len(window_samples) if window_samples else None

    def get_context(self, metric_names: List[str]) -> Dict[str, Any]:
        """构建评估上下文（含历史值）"""
        ctx = {}
        for name in metric_names:
            current = self.get_current(name)
            previous_samples = self._samples.get(name, [])
            prev_val = previous_samples[-2].value if len(previous_samples) >= 2 else current
            ctx[name] = current
            ctx[f"_prev_{name}"] = prev_val
        return ctx


# ─── Feedback Control Loop ────────────────────────────────────────────────────

class FeedbackControlLoop:
    """
    反馈控制环。

    闭环流程：
    1. collect(metrics)    → MetricCollector 收集指标
    2. evaluate(conditions) → ConditionEngine 评估条件
    3. decide(signals)      → 生成 ControlSignal
    4. execute(signals)     → 执行控制动作

    支持：
    - 阈值触发自动扩缩容
    - 错误率突增自动切换策略
    - 延迟升高自动限流
    - 节点失联自动下线
    """

    def __init__(
        self,
        event_store: OssEventStore,
        pubsub: PubSubManager,
        adaptive_domain: AdaptiveDomain,
        swarm_domain: Optional[SwarmDomain] = None,
        check_interval: int = 10,
    ):
        self.event_store = event_store
        self.pubsub = pubsub
        self.adaptive = adaptive_domain
        self.swarm = swarm_domain
        self.check_interval = check_interval

        self.collector = MetricCollector()
        self.condition_engine = ConditionEngine()

        self._conditions: List[Condition] = []
        self._control_signals: Dict[str, ControlSignal] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # 默认注册常用指标
        self._register_default_metrics()

    def _register_default_metrics(self):
        """注册默认监控指标"""
        defaults = [
            ("cpu_usage", 60),
            ("memory_usage", 60),
            ("error_rate", 30),
            ("request_latency_p99", 60),
            ("active_nodes", 30),
            ("pending_tasks", 30),
            ("success_rate", 60),
        ]
        for name, window in defaults:
            self.collector.register_metric(name, window)

    # ── Condition Management ─────────────────────────────────────────────────

    def add_condition(self, condition: Condition):
        """添加控制条件"""
        self._conditions.append(condition)

    def remove_condition(self, condition: Condition):
        """移除控制条件"""
        if condition in self._conditions:
            self._conditions.remove(condition)

    # ── Metric Collection ──────────────────────────────────────────────────────

    def collect_metric(self, metric_name: str, value: float, unit: str = "", tags: Dict[str, str] = None):
        """外部注入指标采样"""
        self.collector.record(metric_name, value, unit, tags)

    def collect_from_swarm(self):
        """从 SwarmDomain 同步节点指标"""
        if not self.swarm:
            return
        registry = self.swarm.registry
        active_nodes = sum(1 for n in registry.values() if n.status.value == "active")
        total_nodes = len(registry)
        self.collector.record("active_nodes", float(active_nodes), "count")
        self.collector.record("total_nodes", float(total_nodes), "count")

        # 聚合节点健康分
        if registry:
            avg_trust = sum(n.trust_score for n in registry.values()) / len(registry)
            self.collector.record("avg_node_trust", avg_trust, "score")

    # ── Evaluation & Decision ─────────────────────────────────────────────────

    def evaluate_conditions(self) -> List[Tuple[Condition, bool, float]]:
        """评估所有条件，返回 (condition, satisfied, score) 列表"""
        metric_names = list(set(c.target_metric for c in self._conditions))
        ctx = self.collector.get_context(metric_names)

        results = []
        for cond in self._conditions:
            satisfied, score = self.condition_engine.evaluate(cond, ctx)
            results.append((cond, satisfied, score))
        return results

    def decide(self, evaluation_results: List[Tuple[Condition, bool, float]]) -> List[ControlSignal]:
        """根据评估结果生成控制信号"""
        signals = []
        for cond, satisfied, score in evaluation_results:
            if cond.type == ConditionType.THRESHOLD:
                sig = self._decide_threshold(cond, satisfied, score)
            elif cond.type == ConditionType.CHANGE:
                sig = self._decide_change(cond, satisfied, score)
            elif cond.type == ConditionType.NEGATION:
                sig = self._decide_negation(cond, satisfied, score)
            else:
                sig = None

            if sig:
                signals.append(sig)

        # 按优先级排序
        signals.sort(key=lambda s: s.priority, reverse=True)
        return signals

    def _decide_threshold(self, cond: Condition, satisfied: bool, score: float) -> Optional[ControlSignal]:
        """阈值条件决策"""
        if not satisfied:
            return None

        metric = cond.target_metric
        value = self.collector.get_current(metric)

        if metric == "cpu_usage" and cond.comparison == ">":
            if value and value > cond.threshold:
                return ControlSignal(
                    signal_id=f"scale_up_cpu_{int(time.time())}",
                    action=ControlAction.SCALE_UP.value,
                    target="auto",
                    reason=f"CPU {value:.1f}% > threshold {cond.threshold}",
                    params={"metric": metric, "value": value},
                    priority=3 if value > cond.threshold * 1.5 else 1,
                )
        elif metric == "error_rate" and cond.comparison == ">":
            if value and value > cond.threshold:
                return ControlSignal(
                    signal_id=f"alert_error_{int(time.time())}",
                    action=ControlAction.ALERT.value,
                    target="ops",
                    reason=f"Error rate {value:.3f} > threshold {cond.threshold}",
                    params={"metric": metric, "value": value},
                    priority=5,
                )
        elif metric == "memory_usage" and cond.comparison == ">":
            if value and value > cond.threshold:
                return ControlSignal(
                    signal_id=f"scale_up_mem_{int(time.time())}",
                    action=ControlAction.SCALE_UP.value,
                    target="auto",
                    reason=f"Memory {value:.1f}% > threshold {cond.threshold}",
                    params={"metric": metric, "value": value},
                    priority=2,
                )
        elif metric == "active_nodes" and cond.comparison == "<":
            if value is not None and value < cond.threshold:
                return ControlSignal(
                    signal_id=f"scale_down_nodes_{int(time.time())}",
                    action=ControlAction.SCALE_DOWN.value,
                    target="auto",
                    reason=f"Active nodes {value:.0f} < threshold {cond.threshold}",
                    params={"metric": metric, "value": value},
                    priority=2,
                )
        return None

    def _decide_change(self, cond: Condition, satisfied: bool, score: float) -> Optional[ControlSignal]:
        """变化率条件决策"""
        if not satisfied:
            return None
        return ControlSignal(
            signal_id=f"change_{cond.target_metric}_{int(time.time())}",
            action=ControlAction.ALERT.value,
            target="ops",
            reason=f"Metric {cond.target_metric} changed significantly",
            params={"metric": cond.target_metric, "score": score},
            priority=2,
        )

    def _decide_negation(self, cond: Condition, satisfied: bool, score: float) -> Optional[ControlSignal]:
        """逆向条件决策（从坏变好）"""
        if not satisfied:
            return None
        return ControlSignal(
            signal_id=f"recovery_{cond.target_metric}_{int(time.time())}",
            action=ControlAction.HEAL.value,
            target="auto",
            reason=f"Metric {cond.target_metric} improved",
            params={"metric": cond.target_metric},
            priority=1,
        )

    # ── Execution ──────────────────────────────────────────────────────────────

    async def execute_signal(self, signal: ControlSignal) -> bool:
        """执行控制信号"""
        try:
            if signal.action == ControlAction.SCALE_UP.value:
                await self._execute_scale_up(signal)
            elif signal.action == ControlAction.SCALE_DOWN.value:
                await self._execute_scale_down(signal)
            elif signal.action == ControlAction.ALERT.value:
                await self._execute_alert(signal)
            elif signal.action == ControlAction.STRATEGY_SWITCH.value:
                await self._execute_strategy_switch(signal)
            elif signal.action == ControlAction.HEAL.value:
                await self._execute_heal(signal)
            elif signal.action == ControlAction.THROTTLE.value:
                await self._execute_throttle(signal)
            else:
                logger.info(f"No-op signal: {signal.signal_id}")
                signal.executed = True
                signal.execution_result = "noop"
                return True

            signal.executed = True
            signal.execution_result = "ok"
            return True

        except Exception as e:
            logger.error(f"Failed to execute signal {signal.signal_id}: {e}")
            signal.executed = False
            signal.execution_result = str(e)
            return False

    async def _execute_scale_up(self, signal: ControlSignal):
        """执行扩容"""
        logger.warning(f"[FEEDBACK] Scale UP: {signal.reason}")
        await self.pubsub.publish(
            Topic.FEEDBACK_CONTROL,
            Event(
                topic=Topic.FEEDBACK_CONTROL,
                event_type="feedback.control_signal",
                data=signal.to_dict(),
            )
        )

    async def _execute_scale_down(self, signal: ControlSignal):
        """执行缩容"""
        logger.info(f"[FEEDBACK] Scale DOWN: {signal.reason}")
        await self.pubsub.publish(
            Topic.FEEDBACK_CONTROL,
            Event(
                topic=Topic.FEEDBACK_CONTROL,
                event_type="feedback.control_signal",
                data=signal.to_dict(),
            )
        )

    async def _execute_alert(self, signal: ControlSignal):
        """发送告警"""
        logger.warning(f"[FEEDBACK] ALERT: {signal.reason}")
        await self.pubsub.publish(
            Topic.FEEDBACK_CONTROL,
            Event(
                topic=Topic.FEEDBACK_CONTROL,
                event_type="feedback.threshold_breach",
                data=signal.to_dict(),
            )
        )

    async def _execute_strategy_switch(self, signal: ControlSignal):
        """切换策略"""
        strategy_name = signal.params.get("strategy", "default")
        logger.info(f"[FEEDBACK] Strategy switch to {strategy_name}: {signal.reason}")
        # 通过 adaptive domain 切换
        if hasattr(self.adaptive, "switcher"):
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.adaptive.switcher.switch_to(strategy_name)
            )

    async def _execute_heal(self, signal: ControlSignal):
        """触发自愈"""
        logger.info(f"[FEEDBACK] Self-heal triggered: {signal.reason}")
        if hasattr(self.adaptive, "self_healing"):
            asyncio.create_task(self.adaptive.self_healing.heal())

    async def _execute_throttle(self, signal: ControlSignal):
        """限流"""
        logger.info(f"[FEEDBACK] Throttle: {signal.reason}")
        await self.pubsub.publish(
            Topic.FEEDBACK_CONTROL,
            Event(
                topic=Topic.FEEDBACK_CONTROL,
                event_type="feedback.throttle",
                data=signal.to_dict(),
            )
        )

    # ── Main Loop ──────────────────────────────────────────────────────────────

    async def run_once(self):
        """执行一次闭环迭代"""
        # 1. 收集指标
        self.collect_from_swarm()
        await self.pubsub.publish(
            Topic.FEEDBACK_CONTROL,
            Event(
                topic=Topic.FEEDBACK_CONTROL,
                event_type="feedback.metric_collected",
                data={"metrics": {k: self.collector.get_current(k) for k in self.collector._windows.keys()}},
            )
        )

        # 2. 评估条件
        eval_results = self.evaluate_conditions()
        await self.pubsub.publish(
            Topic.FEEDBACK_CONTROL,
            Event(
                topic=Topic.FEEDBACK_CONTROL,
                event_type="feedback.evaluation_done",
                data={"results": [(c.to_dict(), s, sc) for c, s, sc in eval_results]},
            )
        )

        # 3. 决策
        signals = self.decide(eval_results)
        for sig in signals:
            self._control_signals[sig.signal_id] = sig

        # 4. 执行
        for sig in signals:
            if not sig.executed:
                await self.execute_signal(sig)
                await self.pubsub.publish(
                    Topic.FEEDBACK_CONTROL,
                    Event(
                        topic=Topic.FEEDBACK_CONTROL,
                        event_type="feedback.control_signal",
                        data=sig.to_dict(),
                    )
                )

    async def _loop(self):
        """后台循环"""
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error(f"Feedback loop error: {e}")
            await asyncio.sleep(self.check_interval)

    def start(self):
        """启动反馈控制环"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("FeedbackControlLoop started")

    async def stop(self):
        """停止反馈控制环"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("FeedbackControlLoop stopped")

    # ── Status ────────────────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        return {
            "running": self._running,
            "conditions_count": len(self._conditions),
            "signals_pending": sum(1 for s in self._control_signals.values() if not s.executed),
            "signals_executed": sum(1 for s in self._control_signals.values() if s.executed),
            "metrics": {k: self.collector.get_current(k) for k in self.collector._windows.keys()},
        }
