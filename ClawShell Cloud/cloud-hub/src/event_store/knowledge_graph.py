#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Knowledge Graph
=====================================
从 ClawShell-Windows lib/core/genome/knowledge_graph.py 提取重构
适配云端事件驱动架构
P1a Evolution 集成：从 cloud/engines/evolution.py 的 InsightAggregator 扩展

核心能力：
|- 实体/关系管理
|- 知识图谱查询（DFS 深度遍历）
|- 传递性/对称性推理
|- 最短路径查找
|- 图谱统计
|- 演化引擎集成：聚合事件洞察到知识图谱
"""

import asyncio
import time
import uuid
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import logging

logger = logging.getLogger("knowledge_graph")


# ============ 数据结构 ============

@dataclass
class Entity:
    """实体"""
    id: str
    name: str
    entity_type: str           # concept, task, skill, memory, tool, agent
    properties: Dict = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "Entity":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Relation:
    """关系"""
    id: str
    source_id: str
    target_id: str
    relation_type: str        # is_a, part_of, depends_on, integrates_with, related_to, similar_to
    weight: float = 1.0
    properties: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "Relation":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class GraphQuery:
    """图谱查询结果"""
    entities: List[Entity]
    relations: List[Relation]
    paths: List[List[str]]
    depth: int


# ============ P1a: InsightAggregator (from EvolutionEngine) ====================

class InsightAggregator:
    """聚合事件和任务结果为可操作洞察（EvolutionEngine 核心组件）"""

    def __init__(self):
        self._lock = asyncio.Lock() if hasattr(asyncio, 'Lock') else None
        self._sync_lock = __import__('threading').RLock()
        self._insights: Dict[str, dict] = {}
        self._counter = 0

    def add_insight(self, title: str, content: str, category: str = "general",
                    source_edges: Optional[List[str]] = None,
                    confidence: float = 0.5,
                    action_suggestion: str = "") -> str:
        """添加新洞察。返回 insight_id。"""
        iid = str(uuid.uuid4())
        with self._sync_lock:
            self._insights[iid] = {
                "insight_id": iid,
                "title": title,
                "content": content,
                "category": category,
                "source_edges": source_edges or [],
                "confidence": confidence,
                "created_at": time.time(),
                "action_suggestion": action_suggestion,
            }
            self._counter += 1
            return iid

    async def add_insight_async(self, title: str, content: str, category: str = "general",
                                 source_edges: Optional[List[str]] = None,
                                 confidence: float = 0.5,
                                 action_suggestion: str = "") -> str:
        """异步添加洞察。返回 insight_id。"""
        iid = str(uuid.uuid4())
        if self._lock:
            async with self._lock:
                self._insights[iid] = {
                    "insight_id": iid,
                    "title": title,
                    "content": content,
                    "category": category,
                    "source_edges": source_edges or [],
                    "confidence": confidence,
                    "created_at": time.time(),
                    "action_suggestion": action_suggestion,
                }
                self._counter += 1
        else:
            return self.add_insight(title, content, category, source_edges, confidence, action_suggestion)
        return iid

    def get_insights(self, limit: int = 50, min_confidence: float = 0.0) -> List[dict]:
        """获取近期洞察。"""
        with self._sync_lock:
            insights = [
                i for i in self._insights.values()
                if i.get("confidence", 0) >= min_confidence
            ]
            insights.sort(key=lambda i: i.get("created_at", 0), reverse=True)
            return insights[:limit]

    def total(self) -> int:
        return self._counter


# ============ 知识图谱 ============

class KnowledgeGraph:
    """
    知识图谱（适配云端架构，内存 + EventStore 持久化）
    """

    def __init__(self, agent_type: str = "default"):
        self.agent_type = agent_type

        # 实体存储
        self._entities: Dict[str, Entity] = {}

        # 关系存储
        self._relations: Dict[str, Relation] = {}

        # 索引
        self._entity_index: Dict[str, Set[str]] = defaultdict(set)
        self._incoming: Dict[str, Set[str]] = defaultdict(set)   # target -> relation_ids
        self._outgoing: Dict[str, Set[str]] = defaultdict(set)    # source -> relation_ids

        # 统计
        self._stats = {
            "total_entities": 0,
            "total_relations": 0,
            "query_count": 0,
            "inference_count": 0,
        }

    # ── 实体管理 ──────────────────────────────────────────────────────────

    def add_entity(
        self,
        name: str,
        entity_type: str,
        properties: Optional[Dict] = None,
        entity_id: Optional[str] = None,
    ) -> Entity:
        entity_id = entity_id or self._uid("entity")
        entity = Entity(
            id=entity_id,
            name=name,
            entity_type=entity_type,
            properties=properties or {},
            metadata={},
        )
        self._entities[entity_id] = entity
        self._entity_index[entity_type].add(entity_id)
        self._stats["total_entities"] += 1
        return entity

    def get_entity(self, entity_id: str) -> Optional[Entity]:
        return self._entities.get(entity_id)

    def get_entities_by_type(self, entity_type: str) -> List[Entity]:
        return [self._entities[eid] for eid in self._entity_index.get(entity_type, set())
                if eid in self._entities]

    def delete_entity(self, entity_id: str) -> bool:
        if entity_id not in self._entities:
            return False
        entity = self._entities.pop(entity_id)
        self._entity_index[entity.entity_type].discard(entity_id)
        # 删除相关关系
        for rid in list(self._outgoing.get(entity_id, [])) + list(self._incoming.get(entity_id, [])):
            self.delete_relation(rid)
        self._stats["total_entities"] -= 1
        return True

    # ── 关系管理 ──────────────────────────────────────────────────────────

    def add_relation(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        weight: float = 1.0,
        properties: Optional[Dict] = None,
        relation_id: Optional[str] = None,
    ) -> Optional[Relation]:
        if source_id not in self._entities or target_id not in self._entities:
            return None

        relation_id = relation_id or self._uid("relation")
        relation = Relation(
            id=relation_id,
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
            weight=weight,
            properties=properties or {},
        )
        self._relations[relation_id] = relation
        self._outgoing[source_id].add(relation_id)
        self._incoming[target_id].add(relation_id)
        self._stats["total_relations"] += 1
        return relation

    def get_relation(self, relation_id: str) -> Optional[Relation]:
        return self._relations.get(relation_id)

    def delete_relation(self, relation_id: str) -> bool:
        if relation_id not in self._relations:
            return False
        rel = self._relations.pop(relation_id)
        self._outgoing[rel.source_id].discard(relation_id)
        self._incoming[rel.target_id].discard(relation_id)
        self._stats["total_relations"] -= 1
        return True

    # ── 图查询 ────────────────────────────────────────────────────────────

    def get_neighbors(
        self,
        entity_id: str,
        relation_type: Optional[str] = None,
        direction: str = "both",
    ) -> List[Tuple[Entity, Relation]]:
        """获取邻居节点：(邻居实体, 关系)"""
        results = []
        if direction in ("outgoing", "both"):
            for rid in self._outgoing.get(entity_id, set()):
                rel = self._relations.get(rid)
                if rel and (relation_type is None or rel.relation_type == relation_type):
                    target = self._entities.get(rel.target_id)
                    if target:
                        results.append((target, rel))
        if direction in ("incoming", "both"):
            for rid in self._incoming.get(entity_id, set()):
                rel = self._relations.get(rid)
                if rel and (relation_type is None or rel.relation_type == relation_type):
                    source = self._entities.get(rel.source_id)
                    if source:
                        results.append((source, rel))
        return results

    def query(
        self,
        start_id: str,
        depth: int = 2,
        relation_type: Optional[str] = None,
    ) -> GraphQuery:
        """深度优先遍历查询"""
        self._stats["query_count"] += 1
        visited: Set[str] = set()
        entities: List[Entity] = []
        relations: List[Relation] = []
        paths: List[List[str]] = []

        def dfs(current_id: str, path: List[str], current_depth: int):
            if current_depth > depth or current_id in visited:
                return
            visited.add(current_id)
            path = path + [current_id]
            entity = self._entities.get(current_id)
            if entity:
                entities.append(entity)
            for neighbor, rel in self.get_neighbors(current_id, relation_type, "outgoing"):
                if rel not in relations:
                    relations.append(rel)
                if current_depth < depth:
                    dfs(neighbor.id, path, current_depth + 1)
            if path:
                paths.append(path)

        dfs(start_id, [], 0)
        return GraphQuery(entities=entities, relations=relations, paths=paths, depth=depth)

    # ── 推理 ──────────────────────────────────────────────────────────────

    def infer(self, entity_id: str) -> List[Tuple[Entity, str, float]]:
        """
        知识推理：传递性 + 对称性
        Returns: List[(inferred_entity, inference_type, confidence)]
        """
        self._stats["inference_count"] += 1
        inferences = []
        entity = self._entities.get(entity_id)
        if not entity:
            return inferences

        # 传递性：A->B, B->C => A->C
        for neighbor, rel in self.get_neighbors(entity_id, direction="outgoing"):
            for sub_neighbor, sub_rel in self.get_neighbors(neighbor.id, direction="outgoing"):
                inferred_type = f"transitive:{rel.relation_type}->{sub_rel.relation_type}"
                confidence = rel.weight * sub_rel.weight
                inferences.append((sub_neighbor, inferred_type, confidence))

        # 对称性：A~B => B~A
        for neighbor, rel in self.get_neighbors(entity_id, direction="outgoing"):
            if rel.relation_type in ("related_to", "similar_to", "integrates_with"):
                inferred_type = f"symmetric:{rel.relation_type}"
                inferences.append((neighbor, inferred_type, rel.weight))

        return inferences

    def find_paths(self, source_id: str, target_id: str, max_depth: int = 3) -> List[List[str]]:
        """查找两点间所有路径"""
        paths = []

        def dfs(current_id: str, target: str, path: List[str], depth: int):
            if depth > max_depth:
                return
            if current_id == target:
                paths.append(path + [current_id])
                return
            for neighbor, _ in self.get_neighbors(current_id, direction="outgoing"):
                if neighbor.id not in path:
                    dfs(neighbor.id, target, path + [current_id], depth + 1)

        dfs(source_id, target_id, [], 0)
        return paths

    # ── 持久化 ────────────────────────────────────────────────────────────

    def to_dict(self) -> Dict:
        return {
            "agent_type": self.agent_type,
            "entities": {k: v.to_dict() for k, v in self._entities.items()},
            "relations": {k: v.to_dict() for k, v in self._relations.items()},
            "stats": self._stats,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "KnowledgeGraph":
        kg = cls(agent_type=data.get("agent_type", "default"))
        for eid, edata in data.get("entities", {}).items():
            entity = Entity.from_dict(edata)
            kg._entities[eid] = entity
            kg._entity_index[entity.entity_type].add(eid)
        for rid, rdata in data.get("relations", {}).items():
            rel = Relation.from_dict(rdata)
            kg._relations[rid] = rel
            kg._outgoing[rel.source_id].add(rid)
            kg._incoming[rel.target_id].add(rid)
        kg._stats = data.get("stats", kg._stats)
        return kg

    def get_stats(self) -> Dict:
        return {
            **self._stats,
            "entity_types": len(self._entity_index),
            "avg_relations_per_entity": (
                self._stats["total_relations"] / max(1, self._stats["total_entities"])
            ),
        }

    def _uid(self, prefix: str) -> str:
        return f"{prefix}_{int(time.time() * 1000)}"