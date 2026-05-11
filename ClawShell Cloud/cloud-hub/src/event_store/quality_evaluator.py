#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Knowledge Quality Evaluator
=================================================
从 ClawShell-Windows lib/core/genome/quality_evaluator.py 提取重构

核心能力：
- 知识质量打分（完整性/准确性/时效性/一致性）
- 置信度计算
- 质量阈值判定
"""

import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class QualityLevel(Enum):
    EXCELLENT = "excellent"; GOOD = "good"
    FAIR = "fair"; POOR = "poor"; UNKNOWN = "unknown"


@dataclass
class QualityScore:
    entry_id: str
    completeness: float = 0.0   # 0-1
    accuracy: float = 0.0       # 0-1
    recency: float = 0.0        # 0-1 (时效性)
    consistency: float = 0.0   # 0-1
    overall: float = 0.0        # 加权总分 0-1
    level: QualityLevel = QualityLevel.UNKNOWN
    issues: List[str] = field(default_factory=list)
    def to_dict(self) -> Dict:
        return {"entry_id": self.entry_id, "completeness": self.completeness,
                "accuracy": self.accuracy, "recency": self.recency,
                "consistency": self.consistency, "overall": self.overall,
                "level": self.level.value, "issues": self.issues}


class QualityEvaluator:
    """知识质量评估器"""
    def __init__(self):
        self._scores: Dict[str, QualityScore] = {}
        self._weights = {"completeness": 0.3, "accuracy": 0.35,
                         "recency": 0.15, "consistency": 0.2}
        self._thresholds = {"poor": 0.3, "fair": 0.5, "good": 0.7, "excellent": 0.85}

    def evaluate(self, entry: Dict) -> QualityScore:
        """评估知识条目质量"""
        entry_id = entry.get("id", "")
        issues: List[str] = []

        # 完整性
        required = ["content", "tags", "category"]
        missing = [f for f in required if not entry.get(f)]
        completeness = 1.0 - (len(missing) / len(required)) if required else 1.0
        if missing: issues.append(f"missing fields: {missing}")

        # 准确性（简单启发式）
        content = entry.get("content", "")
        accuracy = 0.8  # 默认
        if len(content) < 10: accuracy -= 0.3; issues.append("content too short")
        if "TODO" in content or "FIXME" in content: accuracy -= 0.2; issues.append("contains placeholders")

        # 时效性
        updated = entry.get("updated_at", time.time())
        age_days = (time.time() - updated) / 86400
        recency = max(0.0, 1.0 - (age_days / 365))  # 1年内新鲜

        # 一致性（简化版：标签格式规范）
        tags = entry.get("tags", [])
        consistency = 1.0
        if not isinstance(tags, list): consistency -= 0.3
        elif len(tags) == 0: consistency -= 0.2
        elif len(tags) > 20: consistency -= 0.2; issues.append("too many tags")

        # 综合评分
        overall = (completeness * self._weights["completeness"] +
                   accuracy * self._weights["accuracy"] +
                   recency * self._weights["recency"] +
                   consistency * self._weights["consistency"])

        level = self._calc_level(overall)
        score = QualityScore(entry_id=entry_id, completeness=completeness,
                             accuracy=accuracy, recency=recency,
                             consistency=consistency, overall=overall,
                             level=level, issues=issues)
        self._scores[entry_id] = score
        return score

    def _calc_level(self, overall: float) -> QualityLevel:
        if overall >= self._thresholds["excellent"]: return QualityLevel.EXCELLENT
        if overall >= self._thresholds["good"]: return QualityLevel.GOOD
        if overall >= self._thresholds["fair"]: return QualityLevel.FAIR
        if overall >= self._thresholds["poor"]: return QualityLevel.POOR
        return QualityLevel.UNKNOWN

    def get_score(self, entry_id: str) -> Optional[QualityScore]:
        return self._scores.get(entry_id)

    def get_all_scores(self) -> List[QualityScore]:
        return list(self._scores.values())

    def get_stats(self) -> Dict:
        if not self._scores:
            return {"total": 0, "avg_overall": 0.0, "by_level": {}}
        levels = {}
        for s in self._scores.values():
            l = s.level.value
            levels[l] = levels.get(l, 0) + 1
        return {"total": len(self._scores),
                "avg_overall": sum(s.overall for s in self._scores.values()) / len(self._scores),
                "by_level": levels}