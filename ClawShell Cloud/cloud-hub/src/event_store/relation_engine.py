#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Relation Engine
======================================
从 ClawShell-Windows lib/core/genome/relation_engine.py 提取重构

核心能力：
- 关系识别: opposite/similar/part_whole/cause_effect/condition/temporal/spatial/reference
- 传递推理: A→B, B→C => A→C
- 演绎推理: 双重否定、原因链追溯
"""

import json, re
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime


RELATION_TYPES = ["opposite", "similar", "part_whole", "cause_effect",
                  "condition", "temporal", "spatial", "reference"]


@dataclass
class Relation:
    id: str; relation_type: str; source: str; target: str
    confidence: float = 1.0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    def to_dict(self) -> Dict:
        return {"id": self.id, "relation_type": self.relation_type,
                "source": self.source, "target": self.target,
                "confidence": self.confidence, "created_at": self.created_at}


class RelationEngine:
    """关系推理引擎"""
    def __init__(self):
        self.relations: Dict[str, List[Relation]] = {}
        self.relation_graph: Dict[str, Set[str]] = {}

    def add_relation(self, relation_type: str, source: str, target: str,
                    confidence: float = 1.0) -> Relation:
        rel_id = f"{source}|{relation_type}|{target}"
        rel = Relation(id=rel_id, relation_type=relation_type,
                      source=source, target=target, confidence=confidence)
        self.relations.setdefault(relation_type, []).append(rel)
        self.relation_graph.setdefault(source, set()).add(target)
        return rel

    def find_opposite(self, entity: str) -> List[str]:
        result = []
        for rel in self.relations.get("opposite", []):
            if rel.source == entity: result.append(rel.target)
            elif rel.target == entity: result.append(rel.source)
        return result

    def find_similar(self, entity: str) -> List[str]:
        result = []
        for rel in self.relations.get("similar", []):
            if rel.source == entity: result.append(rel.target)
            elif rel.target == entity: result.append(rel.source)
        return result

    def find_causes(self, entity: str) -> List[str]:
        return [rel.source for rel in self.relations.get("cause_effect", [])
                if rel.target == entity]

    def find_effects(self, entity: str) -> List[str]:
        return [rel.target for rel in self.relations.get("cause_effect", [])
                if rel.source == entity]

    def transitive_inference(self, entity: str, relation_type: str) -> Set[str]:
        result, visited, queue = set(), set(), [entity]
        while queue:
            current = queue.pop(0)
            if current in visited: continue
            visited.add(current)
            for rel in self.relations.get(relation_type, []):
                if rel.source == current and rel.target not in visited:
                    result.add(rel.target); queue.append(rel.target)
        return result

    def deduce_from_opposites(self, entity: str) -> Dict:
        opposites = self.find_opposite(entity)
        return {"entity": entity, "opposites": opposites,
                "double_negation": entity if opposites else None}

    def deduce_from_causes(self, entity: str) -> Dict:
        chain, current = [], entity
        while True:
            causes = self.find_causes(current)
            if not causes: break
            chain.append(causes[0]); current = causes[0]
        return {"entity": entity, "root_causes": chain,
                "effects": self.find_effects(entity)}

    def export_graph(self) -> Dict:
        return {
            "nodes": list(self.relation_graph.keys()),
            "edges": [{"source": k, "target": v}
                      for k, vs in self.relation_graph.items() for v in vs],
            "relation_counts": {rt: len(rs) for rt, rs in self.relations.items()}
        }

    def import_from_json(self, data: Dict):
        self.relations = {}; self.relation_graph = {}
        for rt, rels in data.get("relations", {}).items():
            for rd in rels:
                self.add_relation(rt, rd["source"], rd["target"],
                               rd.get("confidence", 1.0))

    def get_stats(self) -> Dict:
        return {"total_relations": sum(len(v) for v in self.relations.values()),
                "by_type": {rt: len(rs) for rt, rs in self.relations.items()}}
