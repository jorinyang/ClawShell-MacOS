#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Semantic Search
=====================================
从 ClawShell-Windows lib/core/genome/semantic_search.py 提取重构

核心能力：
- TF-IDF 向量化
- 余弦相似度搜索
- 关键词高亮
- 文档相似度检索
"""

import time, math, json
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class SemanticVector:
    entity_id: str; vector: List[float]; dimension: int
    created_at: float = field(default_factory=time.time)


@dataclass
class SearchResult:
    entity_id: str; score: float
    highlights: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)


class SemanticSearch:
    """语义搜索（TF-IDF + 余弦相似度）"""
    def __init__(self, dimension: int = 100, persistence_path: Optional[str] = None):
        self.dimension = dimension
        self.persistence_path = persistence_path
        self._documents: Dict[str, Dict] = {}
        self._vectors: Dict[str, SemanticVector] = {}
        self._vocabulary: Dict[str, int] = {}
        self._idf: Dict[str, float] = {}
        self._stats = {"total_documents": 0, "total_terms": 0,
                      "search_count": 0, "avg_search_time_ms": 0}
        self._load()

    def index_document(self, entity_id: str, text: str,
                      metadata: Optional[Dict] = None):
        self._documents[entity_id] = {"text": text, "metadata": metadata or {}}
        tf = self._compute_tf(text)
        for term in tf.keys():
            if term not in self._vocabulary:
                self._vocabulary[term] = len(self._vocabulary)
            self._idf[term] = self._idf.get(term, 0) + 1
        vector = self._compute_tfidf_vector(tf)
        self._vectors[entity_id] = SemanticVector(
            entity_id=entity_id, vector=vector, dimension=self.dimension)
        self._stats["total_documents"] += 1
        self._stats["total_terms"] = len(self._vocabulary)
        self._save()

    def search(self, query: str, top_k: int = 10,
               min_score: float = 0.0) -> List[SearchResult]:
        start = time.time()
        self._stats["search_count"] += 1
        query_tf = self._compute_tf(query)
        query_vector = self._compute_tfidf_vector(query_tf)
        scores = []
        for eid, doc_vec in self._vectors.items():
            sim = self._cosine_similarity(query_vector, doc_vec.vector)
            if sim >= min_score:
                scores.append(SearchResult(
                    entity_id=eid, score=sim,
                    highlights=self._get_highlights(query,
                                                   self._documents[eid]["text"]),
                    metadata=self._documents[eid].get("metadata", {})))
        scores.sort(key=lambda x: x.score, reverse=True)
        elapsed = (time.time() - start) * 1000
        self._stats["avg_search_time_ms"] = (
            (self._stats["avg_search_time_ms"] * (self._stats["search_count"] - 1) + elapsed)
            / self._stats["search_count"])
        return scores[:top_k]

    def get_similar(self, entity_id: str, top_k: int = 5) -> List[Tuple[str, float]]:
        if entity_id not in self._vectors: return []
        src = self._vectors[entity_id].vector
        sims = [(eid, self._cosine_similarity(src, vec.vector))
                for eid, vec in self._vectors.items() if eid != entity_id]
        sims.sort(key=lambda x: x[1], reverse=True)
        return sims[:top_k]

    def search_by_keyword(self, keyword: str,
                         top_k: int = 10) -> List[SearchResult]:
        kl = keyword.lower()
        results = []
        for eid, doc in self._documents.items():
            count = doc["text"].lower().count(kl)
            if count > 0:
                results.append(SearchResult(
                    entity_id=eid, score=float(count),
                    highlights=[keyword],
                    metadata=doc.get("metadata", {})))
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]

    def delete_document(self, entity_id: str) -> bool:
        if entity_id not in self._documents: return False
        del self._documents[entity_id]
        if entity_id in self._vectors: del self._vectors[entity_id]
        self._stats["total_documents"] -= 1
        self._save(); return True

    def get_document(self, entity_id: str) -> Optional[Dict]:
        return self._documents.get(entity_id)

    def get_stats(self) -> Dict:
        return {**self._stats, "vocabulary_size": len(self._vocabulary),
                "avg_vector_norm": self._compute_avg_norm()}

    def _compute_tf(self, text: str) -> Dict[str, float]:
        words = text.lower().split()
        wc = len(words)
        if wc == 0: return {}
        tf = defaultdict(int)
        for w in words:
            w = ''.join(c for c in w if c.isalnum())
            if w: tf[w] += 1
        for w in tf: tf[w] /= wc
        return dict(tf)

    def _compute_tfidf_vector(self, tf: Dict[str, float]) -> List[float]:
        N = max(self._stats["total_documents"], 1)
        vector = [0.0] * min(self.dimension, len(self._vocabulary))
        for term, tf_val in tf.items():
            if term not in self._vocabulary: continue
            idx = self._vocabulary[term]
            if idx >= self.dimension: continue
            df = self._idf.get(term, 1)
            vector[idx] = tf_val * math.log(N / df)
        return vector

    def _cosine_similarity(self, v1: List[float], v2: List[float]) -> float:
        max_len = max(len(v1), len(v2))
        v1 = v1 + [0.0] * (max_len - len(v1))
        v2 = v2 + [0.0] * (max_len - len(v2))
        dot = sum(a * b for a, b in zip(v1, v2))
        n1 = math.sqrt(sum(a * a for a in v1))
        n2 = math.sqrt(sum(b * b for b in v2))
        return dot / (n1 * n2) if n1 > 0 and n2 > 0 else 0.0

    def _get_highlights(self, query: str, text: str,
                       window: int = 50) -> List[str]:
        words = [''.join(c for c in w if c.isalnum())
                 for w in query.lower().split()]
        text_lower = text.lower()
        highlights = []
        for word in words:
            if not word: continue
            idx = text_lower.find(word)
            if idx >= 0:
                start = max(0, idx - window)
                end = min(len(text), idx + len(word) + window)
                snippet = ("..." + text[start:end] + "..." if start > 0 else text[start:end] + ("..." if end < len(text) else ""))
                highlights.append(snippet)
        return highlights[:3]

    def _compute_avg_norm(self) -> float:
        if not self._vectors: return 0.0
        return sum(math.sqrt(sum(v * v for v in vec.vector))
                   for vec in self._vectors.values()) / len(self._vectors)

    def _save(self):
        if not self.persistence_path: return
        try:
            data = {"documents": self._documents, "vectors":
                    {k: {"entity_id": v.entity_id, "vector": v.vector,
                         "dimension": v.dimension}
                     for k, v in self._vectors.items()},
                    "vocabulary": self._vocabulary, "idf": self._idf}
            with open(self.persistence_path, "w") as f: json.dump(data, f)
        except: pass

    def _load(self):
        if not self.persistence_path: return
        try:
            with open(self.persistence_path) as f: data = json.load(f)
            self._documents = data.get("documents", {})
            for eid, vdata in data.get("vectors", {}).items():
                self._vectors[eid] = SemanticVector(**vdata)
            self._vocabulary = data.get("vocabulary", {})
            self._idf = data.get("idf", {})
            self._stats["total_documents"] = len(self._documents)
            self._stats["total_terms"] = len(self._vocabulary)
        except: pass
