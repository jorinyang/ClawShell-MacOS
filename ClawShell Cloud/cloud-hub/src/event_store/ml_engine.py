#!/usr/bin/env python3
"""
ClawShell Cloud Hub — ML Engine
================================
从 ClawShell-Windows lib/layer2/ml_engine.py 提取重构

核心能力：
- 异常检测（Z-score / IQR）
- 趋势预测（线性回归滑动窗口）
- 根因分析（因果图追溯）
"""

import time, json, math
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import deque, defaultdict


@dataclass
class AnomalyResult:
    timestamp: float; metric: str; value: float
    expected: float; deviation: float; severity: str  # low/medium/high/critical


@dataclass
class TrendResult:
    metric: str; slope: float; direction: str  # rising/falling/stable
    confidence: float; forecast: float


class MLEngine:
    """ML 引擎（无外部依赖，纯Python实现）"""
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self._anomalies: List[AnomalyResult] = []
        self._zscore_params: Dict[str, Dict] = defaultdict(lambda: {"mean": 0, "std": 1})

    def add_sample(self, metric: str, value: float, timestamp: Optional[float] = None):
        ts = timestamp or time.time()
        self._metrics[metric].append({"time": ts, "value": value})
        self._update_zscore_params(metric)

    def _update_zscore_params(self, metric: str):
        vals = [s["value"] for s in self._metrics[metric]]
        if len(vals) < 2: return
        mean = sum(vals) / len(vals)
        std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
        std = std if std > 0 else 1.0
        self._zscore_params[metric] = {"mean": mean, "std": std}

    def detect_anomaly(self, metric: str, value: float,
                      threshold: float = 3.0) -> Optional[AnomalyResult]:
        params = self._zscore_params[metric]
        if params["std"] == 0: return None
        deviation = abs(value - params["mean"]) / params["std"]
        if deviation > threshold:
            severity = ("critical" if deviation > threshold * 2
                       else "high" if deviation > threshold * 1.5 else "medium")
            result = AnomalyResult(
                timestamp=time.time(), metric=metric, value=value,
                expected=params["mean"], deviation=deviation, severity=severity)
            self._anomalies.append(result)
            if len(self._anomalies) > 1000:
                self._anomalies = self._anomalies[-500:]
            return result
        return None

    def predict_trend(self, metric: str, steps: int = 1) -> Optional[TrendResult]:
        vals = list(self._metrics[metric])
        if len(vals) < 5: return None
        n = len(vals)
        times = [v["time"] for v in vals]
        values = [v["value"] for v in vals]
        t_mean = sum(times) / n; v_mean = sum(values) / n
        num = sum((t - t_mean) * (v - v_mean) for t, v in zip(times, values))
        den = sum((t - t_mean) ** 2 for t in times)
        if den == 0: return None
        slope = num / den
        direction = ("rising" if slope > 0.001
                    else "falling" if slope < -0.001 else "stable")
        last_time = times[-1]
        interval = (times[-1] - times[0]) / max(n - 1, 1)
        forecast = values[-1] + slope * interval * steps
        ss_res = sum((v - (v_mean + slope * (t - t_mean))) ** 2
                     for t, v in zip(times, values))
        ss_tot = sum((v - v_mean) ** 2 for v in values)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        confidence = max(0.0, min(1.0, r2))
        return TrendResult(metric=metric, slope=slope, direction=direction,
                          confidence=confidence, forecast=forecast)

    def find_root_cause(self, symptom_metric: str,
                        candidate_metrics: List[str]) -> List[Dict]:
        results = []
        symptom_vals = [s["value"] for s in self._metrics.get(symptom_metric, [])]
        if len(symptom_vals) < 3: return results
        symptom_mean = sum(symptom_vals) / len(symptom_vals)
        for cand in candidate_metrics:
            cand_vals = [s["value"] for s in self._metrics.get(cand, [])]
            if len(cand_vals) < 3: continue
            corr = self._pearson_correlation(
                symptom_vals[-len(cand_vals):], cand_vals)
            if abs(corr) > 0.7:
                results.append({"metric": cand, "correlation": corr,
                              "likely_cause": abs(corr) > 0.85})
        results.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        return results

    def _pearson_correlation(self, xs: List[float], ys: List[float]) -> float:
        n = min(len(xs), len(ys))
        if n < 3: return 0
        xm, ym = sum(xs[-n:]) / n, sum(ys[-n:]) / n
        num = sum((x - xm) * (y - ym) for x, y in zip(xs[-n:], ys[-n:]))
        den = math.sqrt(sum((x - xm) ** 2 for x in xs[-n:])
                      * sum((y - ym) ** 2 for y in ys[-n:]))
        return num / den if den > 0 else 0

    def get_metrics(self, metric: str) -> List[Dict]:
        return list(self._metrics.get(metric, []))

    def get_anomalies(self, limit: int = 100) -> List[AnomalyResult]:
        return self._anomalies[-limit:]

    def clear_anomalies(self):
        self._anomalies = []
