#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Event Metrics
====================================
从 ClawShell-Windows lib/core/eventbus/event_metrics.py 提取重构

核心能力：
- 事件计数、吞吐、延迟统计
- 滑动窗口指标
- 异常检测
- Top N 事件排名
"""

import time, json, threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from collections import defaultdict


@dataclass
class EventMetric:
    event_type: str; count: int = 0; total_size: int = 0; avg_size: float = 0.0
    min_latency: float = float('inf'); max_latency: float = 0.0; avg_latency: float = 0.0
    error_count: int = 0; last_occurrence: float = 0; first_occurrence: float = 0


class EventMetrics:
    """事件指标收集器"""
    def __init__(self, window_size: int = 60):
        self.window_size = window_size
        self._metrics: Dict[str, EventMetric] = {}
        self._history: List[Dict] = []
        self._lock = threading.Lock()
        self._stats = {"total_events": 0, "total_errors": 0, "snapshots_taken": 0}

    def record(self, event_type: str, size: int = 0, latency: float = 0.0,
               is_error: bool = False):
        with self._lock:
            now = time.time()
            if event_type not in self._metrics:
                self._metrics[event_type] = EventMetric(event_type=event_type,
                                                        first_occurrence=now)
            m = self._metrics[event_type]
            m.count += 1; m.total_size += size
            m.avg_size = m.total_size / m.count
            m.last_occurrence = now
            if latency > 0:
                m.min_latency = min(m.min_latency, latency)
                m.max_latency = max(m.max_latency, latency)
                if m.avg_latency == 0: m.avg_latency = latency
                else: m.avg_latency = (m.avg_latency + latency) / 2
            if is_error:
                m.error_count += 1
                self._stats["total_errors"] += 1
            self._history.append({"timestamp": now, "event_type": event_type,
                                   "is_error": is_error, "latency": latency, "size": size})
            self._stats["total_events"] += 1
            self._cleanup(now)

    def _cleanup(self, now: float):
        cutoff = now - self.window_size
        self._history = [e for e in self._history if e["timestamp"] >= cutoff]

    def get_metric(self, event_type: str) -> Optional[EventMetric]:
        return self._metrics.get(event_type)

    def get_snapshot(self) -> Dict:
        with self._lock:
            now = time.time()
            recent = [e for e in self._history if e["timestamp"] >= now - self.window_size]
            tp = len(recent) / self.window_size if self.window_size > 0 else 0
            lats = [e["latency"] for e in recent if e["latency"] > 0]
            avg_lat = sum(lats) / len(lats) if lats else 0
            self._stats["snapshots_taken"] += 1
            return {"timestamp": now, "total_events": self._stats["total_events"],
                    "total_errors": self._stats["total_errors"],
                    "throughput": tp, "avg_latency": avg_lat,
                    "metrics": {k: asdict(v) for k, v in self._metrics.items()}}

    def get_top_events(self, limit: int = 10, by: str = "count") -> List[EventMetric]:
        with self._lock:
            ms = list(self._metrics.values())
            if by == "count": return sorted(ms, key=lambda x: x.count, reverse=True)[:limit]
            if by == "error": return sorted(ms, key=lambda x: x.error_count, reverse=True)[:limit]
            if by == "latency": return sorted(ms, key=lambda x: x.avg_latency, reverse=True)[:limit]
            return ms[:limit]

    def get_error_rate(self, event_type: Optional[str] = None) -> float:
        with self._lock:
            if event_type:
                m = self._metrics.get(event_type)
                return m.error_count / m.count if m and m.count > 0 else 0.0
            total = sum(m.count for m in self._metrics.values())
            errors = sum(m.error_count for m in self._metrics.values())
            return errors / total if total > 0 else 0.0

    def detect_anomalies(self, threshold: float = 2.0) -> List[Dict]:
        with self._lock:
            anomalies = []
            now = time.time()
            for et, m in self._metrics.items():
                if m.count == 0: continue
                recent = [e for e in self._history
                          if e["event_type"] == et and e["timestamp"] >= now - self.window_size]
                if len(recent) < 5: continue
                lats = [e["latency"] for e in recent if e["latency"] > 0]
                if not lats: continue
                mean = sum(lats) / len(lats)
                std = (sum((x - mean) ** 2 for x in lats) / len(lats)) ** 0.5
                if m.avg_latency > mean + (threshold * std):
                    anomalies.append({"event_type": et, "type": "high_latency",
                                      "current": m.avg_latency, "expected": mean,
                                      "deviation": (m.avg_latency - mean) / std if std > 0 else 0})
                recent_errs = sum(1 for e in recent if e["is_error"])
                if recent_errs / len(recent) > 0.1:
                    anomalies.append({"event_type": et, "type": "high_error_rate",
                                      "current": recent_errs / len(recent), "threshold": 0.1})
            return anomalies

    def export(self) -> str:
        return json.dumps(self.get_snapshot(), indent=2)

    def get_stats(self) -> Dict:
        with self._lock:
            return {**self._stats, "tracked_event_types": len(self._metrics),
                    "history_size": len(self._history)}