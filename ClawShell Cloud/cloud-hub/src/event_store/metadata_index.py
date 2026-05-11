#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Metadata Index
====================================
从 ClawShell-Windows lib/core/genome/metadata_index.py 提取重构

核心能力：
- 键值元数据索引（string/number/boolean/list/dict）
- 倒排索引快速查询
- 范围查询（min/max）
- 全文搜索
"""

import time, json
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from collections import defaultdict
from pathlib import Path


@dataclass
class MetadataEntry:
    entry_id: str; entity_id: str; entity_type: str; key: str
    value: Any; value_type: str  # string/number/boolean/list/dict
    indexed: bool = True
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    def to_dict(self) -> Dict:
        return {"entry_id": self.entry_id, "entity_id": self.entity_id,
                "entity_type": self.entity_type, "key": self.key,
                "value": self.value, "value_type": self.value_type,
                "indexed": self.indexed, "created_at": self.created_at,
                "updated_at": self.updated_at}


class MetadataIndex:
    """元数据索引"""
    def __init__(self, persistence_path: Optional[str] = None):
        self.persistence_path = persistence_path
        self._entries: Dict[str, MetadataEntry] = {}
        self._entity_index: Dict[str, List[str]] = defaultdict(list)
        self._inverted_index: Dict[str, Dict[str, List[str]]] = defaultdict(
            lambda: defaultdict(list))
        self._type_index: Dict[str, List[str]] = defaultdict(list)
        self._load()

    def add(self, entity_id: str, entity_type: str, key: str, value: Any) -> MetadataEntry:
        import uuid
        vt = self._detect_type(value)
        entry = MetadataEntry(
            entry_id=f"me_{int(time.time() * 1000)}",
            entity_id=entity_id, entity_type=entity_type,
            key=key, value=value, value_type=vt)
        self._entries[entry.entry_id] = entry
        self._entity_index[entity_id].append(entry.entry_id)
        if entry.indexed:
            self._inverted_index[key][str(value)[:100]].append(entry.entry_id)
        self._type_index[entity_type].append(entry.entry_id)
        self._save(); return entry

    def _detect_type(self, value: Any) -> str:
        return type(value).__name__

    def search(self, key: str, value: Any) -> List[MetadataEntry]:
        entry_ids = self._inverted_index.get(key, {}).get(str(value)[:100], [])
        return [self._entries[eid] for eid in entry_ids if eid in self._entries]

    def range_query(self, key: str, min_val: Any = None,
                   max_val: Any = None) -> List[MetadataEntry]:
        results = []
        for entry in self._entries.values():
            if entry.key != key: continue
            try:
                v = float(entry.value)
                if (min_val is None or v >= min_val) and \
                   (max_val is None or v <= max_val):
                    results.append(entry)
            except (TypeError, ValueError): pass
        return results

    def get_entity_metadata(self, entity_id: str) -> List[MetadataEntry]:
        return [self._entries[eid] for eid in self._entity_index.get(entity_id, [])
                if eid in self._entries]

    def get_type_metadata(self, entity_type: str) -> List[MetadataEntry]:
        return [self._entries[eid] for eid in self._type_index.get(entity_type, [])
                if eid in self._entries]

    def fulltext_search(self, query: str) -> List[MetadataEntry]:
        ql = query.lower()
        return [e for e in self._entries.values()
                if isinstance(e.value, str) and ql in e.value.lower()]

    def remove(self, entry_id: str) -> bool:
        if entry_id not in self._entries: return False
        entry = self._entries[entry_id]
        del self._entries[entry_id]
        self._entity_index[entry.entity_id] = [
            e for e in self._entity_index[entry.entity_id] if e != entry_id]
        self._type_index[entry.entity_type] = [
            e for e in self._type_index[entry.entity_type] if e != entry_id]
        self._save(); return True

    def get_stats(self) -> Dict:
        total = sum(len(ids) for ids in self._entity_index.values())
        return {"total_entries": total,
                "by_type": {t: len(ids) for t, ids in self._type_index.items()
                             if isinstance(ids, list)}}

    def _save(self):
        if not self.persistence_path: return
        try:
            with open(self.persistence_path, "w") as f:
                json.dump({k: v.to_dict() for k, v in self._entries.items()}, f)
        except: pass

    def _load(self):
        if not self.persistence_path: return
        try:
            if not Path(self.persistence_path).exists(): return
            with open(self.persistence_path) as f:
                data = json.load(f)
            if not isinstance(data, dict): return
            for entry_dict in data.values():
                if not isinstance(entry_dict, dict): continue
                entry = MetadataEntry(**entry_dict)
                self._entries[entry.entry_id] = entry
                self._entity_index[entry.entity_id].append(entry.entry_id)
                self._type_index[entry.entity_type].append(entry.entry_id)
        except Exception: pass
