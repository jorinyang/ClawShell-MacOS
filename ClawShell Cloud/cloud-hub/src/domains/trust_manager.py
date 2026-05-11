#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Trust Manager
===================================
从 ClawShell-Windows lib/layer4/trust_manager.py 提取重构

核心能力：
- 信任评分（0-100）
- 信任级别（BLOCKED/LOW/MEDIUM/HIGH/FULL）
- 成功/失败历史追踪
- 交互限制决策
"""

import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class TrustLevel(Enum):
    UNKNOWN = "unknown"; BLOCKED = "blocked"; LOW = "low"
    MEDIUM = "medium"; HIGH = "high"; FULL = "full"


@dataclass
class TrustScore:
    node_id: str; score: float = 0.0; level: TrustLevel = TrustLevel.UNKNOWN
    interactions: int = 0; successes: int = 0; failures: int = 0
    last_interaction: float = 0; history: List[Dict] = field(default_factory=list)
    @property
    def success_rate(self) -> float:
        return self.successes / self.interactions if self.interactions > 0 else 0.0
    def to_dict(self) -> Dict:
        return {"node_id": self.node_id, "score": self.score,
                "level": self.level.value, "interactions": self.interactions,
                "successes": self.successes, "failures": self.failures,
                "last_interaction": self.last_interaction,
                "history": self.history[-50:]}


class TrustManager:
    """信任管理器"""
    def __init__(self):
        self._config = {
            "initial_trust": 50.0, "success_bonus": 5.0,
            "failure_penalty": 10.0, "time_decay": 0.95,
            "min_score": 0.0, "max_score": 100.0,
            "thresholds": {"low": 30, "medium": 60, "high": 80, "full": 95},
            "known_nodes": {"openclaw": 100.0, "hermes": 100.0, "n8n": 80.0,
                           "memos": 70.0, "obsidian": 70.0}
        }
        self._state: Dict = {"trust_scores": {}}

    def _calc_level(self, score: float) -> TrustLevel:
        t = self._config["thresholds"]
        if score >= t["full"]: return TrustLevel.FULL
        if score >= t["high"]: return TrustLevel.HIGH
        if score >= t["medium"]: return TrustLevel.MEDIUM
        if score >= t["low"]: return TrustLevel.LOW
        return TrustLevel.BLOCKED

    def get_trust(self, node_id: str) -> TrustScore:
        if node_id in self._state["trust_scores"]:
            d = self._state["trust_scores"][node_id]
            return TrustScore(node_id=node_id, score=d["score"],
                             level=TrustLevel(d.get("level", "unknown")),
                             interactions=d.get("interactions", 0),
                             successes=d.get("successes", 0),
                             failures=d.get("failures", 0),
                             last_interaction=d.get("last_interaction", 0),
                             history=d.get("history", []))
        for prefix, score in self._config["known_nodes"].items():
            if node_id.startswith(prefix):
                return TrustScore(node_id=node_id, score=score, level=TrustLevel.HIGH,
                                 interactions=1, successes=1)
        return TrustScore(node_id=node_id, score=self._config["initial_trust"],
                          level=TrustLevel.MEDIUM)

    def evaluate(self, node_id: str) -> Dict:
        trust = self.get_trust(node_id)
        return {"node_id": node_id, "score": trust.score, "level": trust.level.value,
                "can_trust": trust.level not in [TrustLevel.UNKNOWN, TrustLevel.BLOCKED],
                "recommendation": self._recommendation(trust)}

    def _recommendation(self, trust: TrustScore) -> str:
        return {TrustLevel.BLOCKED: "拒绝交互",
                TrustLevel.LOW: "谨慎交互，限制权限",
                TrustLevel.MEDIUM: "允许基本交互",
                TrustLevel.HIGH: "允许大多数操作",
                TrustLevel.FULL: "完全信任，允许所有操作",
                TrustLevel.UNKNOWN: "需要先建立信任"}.get(trust.level, "")

    def record_success(self, node_id: str, details: Optional[Dict] = None):
        self._update_trust(node_id, True, details)

    def record_failure(self, node_id: str, details: Optional[Dict] = None):
        self._update_trust(node_id, False, details)

    def _update_trust(self, node_id: str, success: bool, details: Optional[Dict]):
        trust = self.get_trust(node_id)
        trust.last_interaction = time.time()
        trust.interactions += 1
        if success:
            trust.successes += 1
            trust.score = min(self._config["max_score"],
                             trust.score + self._config["success_bonus"])
        else:
            trust.failures += 1
            trust.score = max(self._config["min_score"],
                             trust.score - self._config["failure_penalty"])
        trust.level = self._calc_level(trust.score)
        trust.history.append({"timestamp": time.time(), "success": success,
                              "details": details or {}})
        self._state["trust_scores"][node_id] = trust.to_dict()

    def can_interact(self, node_id: str, required: TrustLevel = TrustLevel.MEDIUM) -> bool:
        trust = self.get_trust(node_id)
        order = [TrustLevel.UNKNOWN, TrustLevel.BLOCKED, TrustLevel.LOW,
                 TrustLevel.MEDIUM, TrustLevel.HIGH, TrustLevel.FULL]
        return order.index(trust.level) >= order.index(required)

    def get_interaction_limits(self, node_id: str) -> Dict:
        trust = self.get_trust(node_id)
        limits = {TrustLevel.BLOCKED: {"read": False, "write": False, "execute": False},
                   TrustLevel.LOW: {"read": True, "write": False, "execute": False},
                   TrustLevel.MEDIUM: {"read": True, "write": True, "execute": False},
                   TrustLevel.HIGH: {"read": True, "write": True, "execute": True},
                   TrustLevel.FULL: {"read": True, "write": True, "execute": True}}
        return limits.get(trust.level, limits[TrustLevel.LOW])

    def get_all_trusted(self) -> List[str]:
        return [nid for nid in self._state.get("trust_scores", {})
                 if self.can_interact(nid, TrustLevel.LOW)]

    def get_leaderboard(self, limit: int = 10) -> List[Dict]:
        scores = [{"node_id": nid, "score": d["score"], "level": d["level"],
                   "interactions": d.get("interactions", 0)}
                  for nid, d in self._state.get("trust_scores", {}).items()]
        scores.sort(key=lambda x: x["score"], reverse=True)
        return scores[:limit]