#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Pattern Miner
===================================
从 ClawShell-Windows lib/core/genome/pattern_miner.py 提取重构

核心能力：
- 频繁项集挖掘（Apriori 算法）
- 关联规则学习（支持度/置信度/lift）
- 序列模式发现
- 简单聚类
"""

import time
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from itertools import combinations
import logging

logger = logging.getLogger("pattern_miner")


@dataclass
class Pattern:
    """模式"""
    id: str
    pattern_type: str          # frequent_item, frequent_pair, association_rule, sequential_pattern, cluster
    items: List[str]
    support: float = 0.0
    confidence: float = 0.0
    lift: float = 0.0
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        d = asdict(self)
        d.pop("created_at")
        return d


@dataclass
class MiningResult:
    """挖掘结果"""
    patterns: List[Pattern]
    statistics: Dict
    execution_time: float


class PatternMiner:
    """
    知识模式挖掘器
    """

    def __init__(self, min_support: float = 0.1, min_confidence: float = 0.5):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self._transactions: List[Set[str]] = []
        self._patterns: Dict[str, Pattern] = {}
        self._stats = {
            "total_transactions": 0,
            "patterns_found": 0,
            "mining_count": 0,
        }

    def add_transaction(self, items: List[str]) -> None:
        """添加事务"""
        self._transactions.append(set(items))
        self._stats["total_transactions"] = len(self._transactions)

    def mine_frequent_itemsets(self) -> List[Pattern]:
        """挖掘频繁项集（Apriori）"""
        if not self._transactions:
            return []

        item_counts: Dict[str, int] = defaultdict(int)
        for txn in self._transactions:
            for item in txn:
                item_counts[item] += 1

        n = len(self._transactions)

        # 频繁 1-项集
        freq_1: List[Tuple[str, float]] = [
            (item, count / n)
            for item, count in item_counts.items()
            if count / n >= self.min_support
        ]

        # 频繁 2-项集
        freq_2: List[Tuple[Tuple[str, str], float]] = []
        for txn in self._transactions:
            items = [i for i in txn if any(i == f[0] for f in freq_1)]
            for pair in combinations(items, 2):
                count = sum(1 for t in self._transactions if set(pair).issubset(t))
                support = count / n
                if support >= self.min_support:
                    freq_2.append((pair, support))

        patterns = []
        for item, support in freq_1:
            pid = f"freq_1_{item}"
            p = Pattern(pid, "frequent_item", [item], support=support)
            patterns.append(p)
            self._patterns[pid] = p

        for pair, support in freq_2:
            pid = f"freq_2_{pair[0]}_{pair[1]}"
            p = Pattern(pid, "frequent_pair", list(pair), support=support)
            patterns.append(p)
            self._patterns[pid] = p

        self._stats["patterns_found"] = len(patterns)
        return patterns

    def mine_association_rules(
        self,
        frequent_itemsets: List[Pattern] = None,
    ) -> List[Pattern]:
        """挖掘关联规则"""
        if frequent_itemsets is None:
            frequent_itemsets = self.mine_frequent_itemsets()

        rules = []
        for pattern in frequent_itemsets:
            if len(pattern.items) < 2:
                continue
            items = pattern.items
            for r in range(1, len(items)):
                for antecedent in combinations(items, r):
                    consequent = [i for i in items if i not in antecedent]
                    antecedent_set = set(antecedent)
                    consequent_set = set(consequent)

                    ant_count = sum(1 for t in self._transactions if antecedent_set.issubset(t))
                    both_count = sum(
                        1 for t in self._transactions
                        if antecedent_set.issubset(t) and consequent_set.issubset(t)
                    )

                    if ant_count > 0:
                        confidence = both_count / ant_count
                        if confidence >= self.min_confidence:
                            con_count = sum(1 for t in self._transactions if consequent_set.issubset(t))
                            expected = (ant_count / n) * (con_count / n) if (n := len(self._transactions)) > 0 else 0
                            lift = both_count / n / expected if expected > 0 else 0

                            pid = f"rule_{'_'.join(antecedent)}->{'_'.join(consequent)}"
                            rule = Pattern(
                                pid, "association_rule",
                                list(antecedent) + ["|"] + list(consequent),
                                support=pattern.support,
                                confidence=confidence,
                                lift=lift,
                            )
                            rules.append(rule)
                            self._patterns[pid] = rule
        return rules

    def mine_sequential_patterns(self, sequences: List[List[str]]) -> List[Pattern]:
        """挖掘序列模式"""
        if not sequences:
            return []

        item_counts: Dict[str, int] = defaultdict(int)
        for seq in sequences:
            for item in set(seq):
                item_counts[item] += 1

        n = len(sequences)
        patterns = []
        for item, count in item_counts.items():
            support = count / n
            if support >= self.min_support:
                pid = f"seq_{item}"
                p = Pattern(pid, "sequential_pattern", [item], support=support)
                patterns.append(p)
                self._patterns[pid] = p

        return patterns

    def find_clusters(
        self,
        data: List[Dict],
        similarity_threshold: float = 0.7,
    ) -> List[List[int]]:
        """简单词袋相似度聚类"""
        n = len(data)
        if n == 0:
            return []

        def get_features(d: Dict) -> Set[str]:
            features = set()
            for v in d.values():
                if isinstance(v, str):
                    features.update(v.lower().split())
            return features

        def similarity(a: Dict, b: Dict) -> float:
            fa, fb = get_features(a), get_features(b)
            if not fa or not fb:
                return 0
            return len(fa & fb) / len(fa | fb)

        clusters = [[i] for i in range(n)]
        merged = True
        while merged:
            merged = False
            new_clusters = []
            used = set()
            for i, c1 in enumerate(clusters):
                if i in used:
                    continue
                for j, c2 in enumerate(clusters):
                    if j <= i or j in used:
                        continue
                    sim_sum = sum(similarity(data[a], data[b]) for a in c1 for b in c2)
                    avg_sim = sim_sum / (len(c1) * len(c2)) if c1 and c2 else 0
                    if avg_sim >= similarity_threshold:
                        new_clusters.append(c1 + c2)
                        used.add(i)
                        used.add(j)
                        merged = True
                        break
                if i not in used:
                    new_clusters.append(c1)
            if merged:
                clusters = new_clusters

        return clusters

    def get_pattern(self, pattern_id: str) -> Optional[Pattern]:
        return self._patterns.get(pattern_id)

    def get_patterns_by_type(self, pattern_type: str) -> List[Pattern]:
        return [p for p in self._patterns.values() if p.pattern_type == pattern_type]

    def get_stats(self) -> Dict:
        return {
            **self._stats,
            "patterns_by_type": {
                ptype: len([p for p in self._patterns.values() if p.pattern_type == ptype])
                for ptype in set(p.pattern_type for p in self._patterns.values())
            },
        }

    def clear(self) -> None:
        self._transactions.clear()
        self._patterns.clear()
        self._stats = {"total_transactions": 0, "patterns_found": 0, "mining_count": 0}