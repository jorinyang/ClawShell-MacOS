#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Swarm Metrics Collector
=============================================
从 ClawShell-Windows lib/layer4/metrics_collector.py 提取重构

核心能力：
- 节点性能指标（吞吐/延迟/成功率）
- 可用性追踪
- 协作质量评估
"""

import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict


@dataclass
class PerformanceMetrics:
    node_id: str; timestamp: float
    requests_total: int = 0; requests_success: int = 0; requests_failed: int = 0
    avg_response_time_ms: float = 0.0; min_response_time_ms: float = 0.0
    max_response_time_ms: float = 0.0; cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    def to_dict(self) -> Dict:
        return {"node_id": self.node_id, "timestamp": self.timestamp,
                "requests_total": self.requests_total,
                "requests_success": self.requests_success,
                "requests_failed": self.requests_failed,
                "avg_response_time_ms": self.avg_response_time_ms,
                "success_rate": (self.requests_success / self.requests_total
                                 if self.requests_total > 0 else 0)}


class MetricsCollector:
    """Swarm 指标收集器"""
    def __init__(self):
        self._node_metrics: Dict[str, List[PerformanceMetrics]] = defaultdict(list)
        self._window = 3600  # 1小时滑动窗口

    def record(self, node_id: str, metrics: PerformanceMetrics):
        self._node_metrics[node_id].append(metrics)
        cutoff = time.time() - self._window
        self._node_metrics[node_id] = [
            m for m in self._node_metrics[node_id] if m.timestamp >= cutoff]
        if len(self._node_metrics[node_id]) > 1000:
            self._node_metrics[node_id] = self._node_metrics[node_id][-500:]

    def get_current(self, node_id: str) -> Optional[PerformanceMetrics]:
        recs = self._node_metrics.get(node_id, [])
        return recs[-1] if recs else None

    def get_history(self, node_id: str, limit: int = 100) -> List[PerformanceMetrics]:
        return self._node_metrics.get(node_id, [])[-limit:]

    def get_aggregated(self, node_id: str) -> Dict:
        recs = self._node_metrics.get(node_id, [])
        if not recs: return {}
        total_req = sum(r.requests_total for r in recs)
        total_suc = sum(r.requests_success for r in recs)
        resp_times = [r.avg_response_time_ms for r in recs if r.avg_response_time_ms > 0]
        return {
            "node_id": node_id,
            "total_requests": total_req,
            "total_success": total_suc,
            "total_failed": sum(r.requests_failed for r in recs),
            "success_rate": total_suc / total_req if total_req > 0 else 0,
            "avg_response_time_ms": sum(resp_times) / len(resp_times) if resp_times else 0,
            "samples": len(recs)
        }

    def get_all_stats(self) -> Dict:
        return {nid: self.get_aggregated(nid) for nid in self._node_metrics}

    def get_top_by_success_rate(self, limit: int = 5) -> List[Dict]:
        stats = [(nid, self.get_aggregated(nid)) for nid in self._node_metrics]
        stats.sort(key=lambda x: x[1].get("success_rate", 0), reverse=True)
        return [{"node_id": s[0], **s[1]} for _, s in stats[:limit]]

    def get_least_available(self, limit: int = 5) -> List[Dict]:
        stats = [(nid, self.get_aggregated(nid)) for nid in self._node_metrics]
        stats.sort(key=lambda x: x[1].get("success_rate", 1))
        return [{"node_id": s[0], **s[1]} for _, s in stats[:limit]]
