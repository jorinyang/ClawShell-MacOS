#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Event Tracer
===================================
从 ClawShell-Windows lib/core/eventbus/event_tracer.py 提取重构

核心能力：
- 分布式追踪（Trace/Span）
- 因果链分析
- 性能分析
- LRU 内存淘汰
"""

import time
import json
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
import logging

logger = logging.getLogger("tracer")


@dataclass
class EventSpan:
    """事件跨度"""
    trace_id: str
    span_id: str
    event_id: str
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    parent_span_id: Optional[str] = None
    tags: Dict = field(default_factory=dict)
    logs: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class TraceResult:
    """追踪结果"""
    trace_id: str
    spans: List[EventSpan]
    total_duration: float
    event_count: int


class EventTracer:
    """
    事件追溯器
    """

    def __init__(self, max_traces: int = 1000):
        self.max_traces = max_traces
        self._traces: Dict[str, Dict[str, EventSpan]] = {}
        self._trace_index: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._stats = {"total_traces": 0, "total_spans": 0, "active_traces": 0}

    def start_trace(
        self,
        trace_id: str,
        event_id: str,
        operation: str,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict] = None,
    ) -> str:
        """开始追踪"""
        with self._lock:
            span_count = len(self._traces.get(trace_id, {}))
            span_id = f"span_{span_count}_{int(time.time() * 1000)}"
            span = EventSpan(
                trace_id=trace_id,
                span_id=span_id,
                event_id=event_id,
                operation_name=operation,
                start_time=time.time(),
                parent_span_id=parent_span_id,
                tags=tags or {},
            )
            if trace_id not in self._traces:
                self._traces[trace_id] = {}
                self._trace_index[trace_id] = time.time()
            self._traces[trace_id][span_id] = span
            self._stats["total_traces"] += 1
            self._stats["total_spans"] += 1
            self._stats["active_traces"] = len(self._traces)
            self._cleanup_old_traces()
            return span_id

    def end_span(self, trace_id: str, span_id: str, tags: Optional[Dict] = None) -> None:
        """结束追踪跨度"""
        with self._lock:
            if trace_id in self._traces and span_id in self._traces[trace_id]:
                span = self._traces[trace_id][span_id]
                span.end_time = time.time()
                if tags:
                    span.tags.update(tags)

    def add_log(
        self,
        trace_id: str,
        span_id: str,
        message: str,
        data: Any = None,
    ) -> None:
        """添加日志"""
        with self._lock:
            if trace_id in self._traces and span_id in self._traces[trace_id]:
                span = self._traces[trace_id][span_id]
                log = {"timestamp": time.time(), "message": message}
                if data is not None:
                    log["data"] = data
                span.logs.append(log)

    def get_trace(self, trace_id: str) -> Optional[TraceResult]:
        """获取追踪结果"""
        with self._lock:
            if trace_id not in self._traces:
                return None
            spans = list(self._traces[trace_id].values())
            if not spans:
                return None
            start_times = [s.start_time for s in spans]
            end_times = [s.end_time for s in spans if s.end_time]
            total_duration = max(end_times) - min(start_times) if end_times else 0
            return TraceResult(
                trace_id=trace_id,
                spans=spans,
                total_duration=total_duration,
                event_count=len(spans),
            )

    def find_causal_chain(self, trace_id: str, target_span_id: str) -> List[str]:
        """查找因果链"""
        with self._lock:
            if trace_id not in self._traces:
                return []
            chain = []
            current_id = target_span_id
            while current_id:
                if current_id in self._traces[trace_id]:
                    span = self._traces[trace_id][current_id]
                    chain.insert(0, span.span_id)
                    current_id = span.parent_span_id
                else:
                    break
            return chain

    def analyze_performance(self, trace_id: str) -> Dict:
        """分析性能"""
        with self._lock:
            if trace_id not in self._traces:
                return {}
            spans = list(self._traces[trace_id].values())
            durations = [s.end_time - s.start_time for s in spans if s.end_time]
            if not durations:
                return {"error": "No completed spans"}
            return {
                "total_spans": len(spans),
                "completed_spans": len(durations),
                "avg_duration": sum(durations) / len(durations),
                "max_duration": max(durations),
                "min_duration": min(durations),
                "total_time": max(durations) - min(durations),
            }

    def get_span_graph(self, trace_id: str) -> Dict[str, List[str]]:
        """获取跨度关系图"""
        with self._lock:
            if trace_id not in self._traces:
                return {}
            graph: Dict[str, List[str]] = {}
            for span_id, span in self._traces[trace_id].items():
                if span.parent_span_id:
                    graph.setdefault(span.parent_span_id, []).append(span_id)
            return graph

    def export_trace(self, trace_id: str) -> Optional[str]:
        """导出追踪为 JSON"""
        result = self.get_trace(trace_id)
        if result is None:
            return None
        data = {
            "trace_id": result.trace_id,
            "total_duration": result.total_duration,
            "event_count": result.event_count,
            "spans": [
                {
                    "span_id": s.span_id,
                    "event_id": s.event_id,
                    "operation_name": s.operation_name,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "parent_span_id": s.parent_span_id,
                    "duration": s.end_time - s.start_time if s.end_time else None,
                    "tags": s.tags,
                    "logs": s.logs,
                }
                for s in result.spans
            ],
        }
        return json.dumps(data, indent=2)

    def get_stats(self) -> Dict:
        with self._lock:
            return {**self._stats, "stored_traces": len(self._traces)}

    def _cleanup_old_traces(self) -> None:
        if len(self._traces) <= self.max_traces:
            return
        sorted_traces = sorted(self._trace_index.items(), key=lambda x: x[1])
        to_delete = len(self._traces) - self.max_traces
        for trace_id, _ in sorted_traces[:to_delete]:
            del self._traces[trace_id]
            del self._trace_index[trace_id]