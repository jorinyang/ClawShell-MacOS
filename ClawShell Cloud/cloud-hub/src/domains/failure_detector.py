#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Failure Detector
======================================
从 ClawShell-Windows lib/layer4/failure_detector.py 提取重构

核心能力：
- 故障类型: TIMEOUT/ERROR/OFFLINE/DEGRADED/MEMORY/CPU
- 连续失败检测 + 阈值触发
- 故障历史记录
"""

import time
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum


class FailureType(Enum):
    TIMEOUT = "timeout"; ERROR = "error"; OFFLINE = "offline"
    DEGRADED = "degraded"; MEMORY = "memory"; CPU = "cpu"


@dataclass
class FailureRecord:
    node_id: str; failure_type: FailureType; timestamp: float
    details: str = ""; resolved: bool = False; resolved_at: float = 0
    def to_dict(self) -> Dict:
        return {"node_id": self.node_id,
                "failure_type": self.failure_type.value,
                "timestamp": self.timestamp,
                "details": self.details,
                "resolved": self.resolved,
                "resolved_at": self.resolved_at}


@dataclass
class FailureAlert:
    node_id: str; failure_type: FailureType
    consecutive_failures: int; threshold: int
    message: str


class FailureDetector:
    """故障检测器"""
    def __init__(self):
        self._failure_counts: Dict[str, int] = defaultdict(int)
        self._records: List[FailureRecord] = []
        self._callbacks: List[Callable] = []
        self._thresholds: Dict[str, int] = defaultdict(lambda: 3)

    def record(self, node_id: str, failure_type: FailureType,
               details: str = "") -> Optional[FailureAlert]:
        self._failure_counts[node_id] += 1
        rec = FailureRecord(node_id=node_id, failure_type=failure_type,
                            timestamp=time.time(), details=details)
        self._records.append(rec)
        if len(self._records) > 1000:
            self._records = self._records[-500:]
        threshold = self._thresholds[node_id]
        if self._failure_counts[node_id] >= threshold:
            alert = FailureAlert(
                node_id=node_id, failure_type=failure_type,
                consecutive_failures=self._failure_counts[node_id],
                threshold=threshold,
                message=f"Node {node_id} has {self._failure_counts[node_id]} "
                        f"consecutive {failure_type.value} failures")
            for cb in self._callbacks:
                try: cb(alert)
                except: pass
            return alert
        return None

    def record_success(self, node_id: str):
        self._failure_counts[node_id] = 0

    def resolve(self, node_id: str) -> int:
        resolved = 0
        now = time.time()
        for rec in self._records:
            if rec.node_id == node_id and not rec.resolved:
                rec.resolved = True; rec.resolved_at = now; resolved += 1
        return resolved

    def get_records(self, node_id: Optional[str] = None,
                   unresolved_only: bool = False) -> List[FailureRecord]:
        recs = self._records
        if node_id: recs = [r for r in recs if r.node_id == node_id]
        if unresolved_only: recs = [r for r in recs if not r.resolved]
        return recs

    def on_alert(self, callback: Callable):
        self._callbacks.append(callback)

    def set_threshold(self, node_id: str, threshold: int):
        self._thresholds[node_id] = threshold

    def get_stats(self) -> Dict:
        return {"total_records": len(self._records),
                "unresolved": sum(1 for r in self._records if not r.resolved),
                "by_type": {ft.value: sum(1 for r in self._records
                                         if r.failure_type == ft)
                            for ft in FailureType}}


from collections import defaultdict
