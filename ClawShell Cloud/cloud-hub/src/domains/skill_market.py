#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Skill Market
===================================
从 ClawShell-Windows lib/core/skill_market.py 提取重构

核心能力：
- 技能发布/发现/安装
- 版本管理（semver）
- 分类标签检索
"""

import json, uuid, time, re
from typing import Dict, List, Optional
from dataclasses import dataclass, field, asdict
from pathlib import Path


MARKET_DIR = Path("~/.cloudshell/skill_market").expanduser()


@dataclass
class MarketSkill:
    skill_id: str; name: str; version: str; description: str
    content: str; author: str
    trigger_words: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    category: str = "general"
    dependencies: List[str] = field(default_factory=list)
    published_at: float = field(default_factory=time.time)
    downloads: int = 0; rating: float = 0.0
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["published_at"] = self.published_at; return d


class SkillMarket:
    """技能市场"""
    def __init__(self, market_dir: Optional[Path] = None):
        self.market_dir = market_dir or MARKET_DIR
        self.market_dir.mkdir(parents=True, exist_ok=True)
        self._skills: Dict[str, MarketSkill] = {}
        self._load_index()

    def _skill_path(self, skill_id: str) -> Path:
        return self.market_dir / f"{skill_id}.json"

    def _load_index(self):
        for p in self.market_dir.glob("*.json"):
            try:
                with open(p) as f: d = json.load(f)
                self._skills[d["skill_id"]] = MarketSkill(**d)
            except: pass

    def publish(self, skill: MarketSkill) -> str:
        skill.skill_id = skill.skill_id or f"skill_{uuid.uuid4().hex[:12]}"
        self._skills[skill.skill_id] = skill
        with open(self._skill_path(skill.skill_id), "w") as f:
            json.dump(skill.to_dict(), f, indent=2)
        return skill.skill_id

    def discover(self, query: str, category: Optional[str] = None,
                 limit: int = 10) -> List[MarketSkill]:
        ql = query.lower()
        results = []
        for skill in self._skills.values():
            if category and skill.category != category: continue
            score = (ql in skill.name.lower() or
                    ql in skill.description.lower() or
                    any(ql in tw.lower() for tw in skill.trigger_words) or
                    any(ql in tag.lower() for tag in skill.tags))
            if score: results.append(skill)
        results.sort(key=lambda s: s.downloads + s.rating * 10, reverse=True)
        return results[:limit]

    def get(self, skill_id: str) -> Optional[MarketSkill]:
        return self._skills.get(skill_id)

    def install(self, skill_id: str) -> Optional[str]:
        skill = self._skills.get(skill_id)
        if not skill: return None
        skill.downloads += 1
        with open(self._skill_path(skill_id), "w") as f:
            json.dump(skill.to_dict(), f, indent=2)
        return skill.content

    def list_by_category(self, category: str) -> List[MarketSkill]:
        return [s for s in self._skills.values() if s.category == category]

    def get_stats(self) -> Dict:
        cats: Dict[str, int] = {}
        for s in self._skills.values():
            cats[s.category] = cats.get(s.category, 0) + 1
        return {"total_skills": len(self._skills),
                "by_category": cats}
