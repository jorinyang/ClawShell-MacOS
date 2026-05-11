#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Strategy Registry
========================================
从 ClawShell-Windows lib/core/strategy/registry.py 提取重构
"""

import yaml, logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


logger = logging.getLogger(__name__)


@dataclass
class Strategy:
    name: str; strategy_type: str
    config: Dict = field(default_factory=dict)
    enabled: bool = True; description: str = ""
    def to_dict(self) -> Dict:
        return {"name": self.name, "strategy_type": self.strategy_type,
                "config": self.config, "enabled": self.enabled,
                "description": self.description}


class StrategyRegistry:
    """策略注册器"""
    def __init__(self):
        self._strategies: Dict[str, Strategy] = {}

    def register(self, strategy: Strategy) -> str:
        self._strategies[strategy.name] = strategy
        return strategy.name

    def get(self, name: str) -> Optional[Strategy]:
        return self._strategies.get(name)

    def list_all(self) -> List[Strategy]:
        return list(self._strategies.values())

    def list_enabled(self) -> List[Strategy]:
        return [s for s in self._strategies.values() if s.enabled]

    def unregister(self, name: str) -> bool:
        return bool(self._strategies.pop(name, None))

    def enable(self, name: str):
        if name in self._strategies:
            self._strategies[name].enabled = True

    def disable(self, name: str):
        if name in self._strategies:
            self._strategies[name].enabled = False

    def export(self) -> List[Dict]:
        return [s.to_dict() for s in self._strategies.values()]

    def import_from(self, strategies: List[Dict]):
        for d in strategies:
            s = Strategy(**d)
            self._strategies[s.name] = s
