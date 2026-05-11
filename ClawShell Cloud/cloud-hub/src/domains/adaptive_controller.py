#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Adaptive Controller
=========================================
从 ClawShell-Windows lib/layer2/adaptive_controller.py 提取重构

核心能力：
- 实时指标反馈（CPU/内存/响应时间/错误率/吞吐）
- 阈值触发 + 自动调节
- PID 风格调节器
"""

import time
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import deque


@dataclass
class MetricSnapshot:
    timestamp: float; cpu_percent: float; memory_percent: float
    response_time: float; error_rate: float; throughput: float


@dataclass
class FeedbackSignal:
    metric: str; current: float; target: float; deviation: float
    adjustment: float; timestamp: float = field(default_factory=time.time)


class AdaptiveController:
    """自适应控制器（神经反馈机制）"""
    def __init__(self):
        self._snapshots: deque = deque(maxlen=200)
        self._thresholds: Dict[str, Dict] = {}
        self._callbacks: Dict[str, List[Callable]] = {}
        self._target_values: Dict[str, float] = {}

    def set_threshold(self, metric: str, warn: float, critical: float,
                     target: Optional[float] = None):
        self._thresholds[metric] = {"warn": warn, "critical": critical}
        if target is not None:
            self._target_values[metric] = target

    def record(self, cpu: float = 0, memory: float = 0,
               response_time: float = 0, error_rate: float = 0,
               throughput: float = 0) -> List[FeedbackSignal]:
        snapshot = MetricSnapshot(
            timestamp=time.time(), cpu_percent=cpu, memory_percent=memory,
            response_time=response_time, error_rate=error_rate,
            throughput=throughput)
        self._snapshots.append(snapshot)
        signals = self._evaluate_thresholds(snapshot)
        return signals

    def _evaluate_thresholds(self, snap: MetricSnapshot) -> List[FeedbackSignal]:
        signals = []
        metrics = {"cpu_percent": snap.cpu_percent, "memory_percent": snap.memory_percent,
                   "response_time": snap.response_time, "error_rate": snap.error_rate,
                   "throughput": snap.throughput}
        for metric, value in metrics.items():
            thresholds = self._thresholds.get(metric, {})
            if not thresholds: continue
            warn = thresholds.get("warn", float("inf"))
            critical = thresholds.get("critical", float("inf"))
            if value >= critical:
                signals.append(self._make_signal(metric, value, "critical"))
            elif value >= warn:
                signals.append(self._make_signal(metric, value, "warning"))
        for sig in signals:
            for cb in self._callbacks.get(sig.metric, []):
                try: cb(sig)
                except: pass
            for cb in self._callbacks.get("*", []):
                try: cb(sig)
                except: pass
        return signals

    def _make_signal(self, metric: str, current: float,
                     level: str) -> FeedbackSignal:
        target = self._target_values.get(metric, 0)
        dev = current - target
        adj = dev * 0.1  # 简单P调节
        return FeedbackSignal(metric=metric, current=current,
                            target=target, deviation=dev, adjustment=adj)

    def on_threshold_breach(self, metric: str, callback: Callable):
        self._callbacks.setdefault(metric, []).append(callback)

    def get_current_state(self) -> Optional[MetricSnapshot]:
        return self._snapshots[-1] if self._snapshots else None

    def get_history(self, limit: int = 50) -> List[MetricSnapshot]:
        return list(self._snapshots)[-limit:]

    def get_stats(self) -> Dict:
        return {"snapshots": len(self._snapshots),
                "metrics_monitored": len(self._thresholds)}
