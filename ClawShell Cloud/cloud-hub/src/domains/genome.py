"""
ClawShell Cloud Hub — GenomeDomain
=================================

知识传承体系（从 ClawShell-Windows lib/core/genome/ 提取重构）

核心能力：
- 知识条目管理（KnowledgeEntry）
- 技能状态追踪（SkillState）
- 错误模式库（ErrorPattern）
- 进化记录（EvolutionRecord）
- 传承协议（HeritageRecord）
- 语义搜索（KnowledgeGraph）

所有操作产生事件 → EventStore 持久化
"""

import asyncio
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..event_store.schema import Event, Topic
from ..event_store.store import OssEventStore
from ..pubsub.manager import PubSubManager

logger = logging.getLogger("genome_domain")


# ─── Schema ────────────────────────────────────────────────────────────────────

class AgentType:
    OPENCLAW = "openclaw"
    HERMES = "hermes"
    WUKONG = "wukong"
    SHARED = "shared"


class KnowledgeEntry:
    def __init__(
        self,
        key: str,
        value: Any,
        category: str = "general",
        source: str = None,
        confidence: float = 1.0,
        created_at: str = None,
    ):
        self.key = key
        self.value = value
        self.category = category
        self.source = source
        self.confidence = confidence
        self.created_at = created_at or datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
            "category": self.category,
            "source": self.source,
            "confidence": self.confidence,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "KnowledgeEntry":
        return cls(**{k: v for k, v in d.items() if k != "updated_at"})


class ErrorPattern:
    def __init__(
        self,
        error_type: str,
        description: str,
        solution: str,
        occurrences: int = 0,
        last_occurrence: str = None,
        tags: List[str] = None,
    ):
        self.error_type = error_type
        self.description = description
        self.solution = solution
        self.occurrences = occurrences
        self.last_occurrence = last_occurrence
        self.tags = tags or []

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ErrorPattern":
        return cls(**d)


class SkillState:
    def __init__(
        self,
        skill_name: str,
        status: str = "active",
        version: str = "1.0.0",
        config: Dict = None,
        performance: float = 1.0,
        last_used: str = None,
        tags: List[str] = None,
    ):
        self.skill_name = skill_name
        self.status = status
        self.version = version
        self.config = config or {}
        self.performance = performance
        self.last_used = last_used
        self.tags = tags or []

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SkillState":
        return cls(**d)


class EvolutionRecord:
    def __init__(
        self,
        version: str,
        timestamp: str = None,
        changes: List[str] = None,
        improvements: List[str] = None,
        from_version: str = None,
        notes: str = None,
    ):
        self.version = version
        self.timestamp = timestamp or datetime.now().isoformat()
        self.changes = changes or []
        self.improvements = improvements or []
        self.from_version = from_version
        self.notes = notes

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "EvolutionRecord":
        return cls(**d)


class HeritageRecord:
    def __init__(
        self,
        from_version: str,
        to_version: str,
        timestamp: str = None,
        heritage_type: str = "restart",
        knowledge_transferred: int = 0,
        errors_transferred: int = 0,
        skills_transferred: int = 0,
        notes: str = None,
    ):
        self.from_version = from_version
        self.to_version = to_version
        self.timestamp = timestamp or datetime.now().isoformat()
        self.heritage_type = heritage_type
        self.knowledge_transferred = knowledge_transferred
        self.errors_transferred = errors_transferred
        self.skills_transferred = skills_transferred
        self.notes = notes

    def to_dict(self) -> dict:
        return {
            "from_version": self.from_version,
            "to_version": self.to_version,
            "timestamp": self.timestamp,
            "heritage_type": self.heritage_type,
            "knowledge_transferred": self.knowledge_transferred,
            "errors_transferred": self.errors_transferred,
            "skills_transferred": self.skills_transferred,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "HeritageRecord":
        return cls(**d)


class Genome:
    """基因组主对象"""

    def __init__(
        self,
        agent_type: str = AgentType.SHARED,
        version: str = "1.0.0",
        created_at: str = None,
        updated_at: str = None,
    ):
        self.agent_type = agent_type
        self.version = version
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = updated_at or datetime.now().isoformat()
        self.knowledge: List[KnowledgeEntry] = []
        self.preferences: Dict[str, Any] = {}
        self.error_patterns: List[ErrorPattern] = []
        self.skills: List[SkillState] = []
        self.evolution: List[EvolutionRecord] = []
        self.current_task: Optional[str] = None
        self.context: Optional[str] = None
        self.pending_issues: List[str] = []
        self.metadata: Dict[str, Any] = {}

    def to_dict(self) -> dict:
        return {
            "agent_type": self.agent_type,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "knowledge": [k.to_dict() for k in self.knowledge],
            "preferences": self.preferences,
            "error_patterns": [e.to_dict() for e in self.error_patterns],
            "skills": [s.to_dict() for s in self.skills],
            "evolution": [e.to_dict() for e in self.evolution],
            "current_task": self.current_task,
            "context": self.context,
            "pending_issues": self.pending_issues,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Genome":
        g = cls(
            agent_type=d.get("agent_type", AgentType.SHARED),
            version=d.get("version", "1.0.0"),
            created_at=d.get("created_at"),
            updated_at=d.get("updated_at"),
        )
        g.knowledge = [KnowledgeEntry.from_dict(k) for k in d.get("knowledge", [])]
        g.preferences = d.get("preferences", {})
        g.error_patterns = [ErrorPattern.from_dict(e) for e in d.get("error_patterns", [])]
        g.skills = [SkillState.from_dict(s) for s in d.get("skills", [])]
        g.evolution = [EvolutionRecord.from_dict(e) for e in d.get("evolution", [])]
        g.current_task = d.get("current_task")
        g.context = d.get("context")
        g.pending_issues = d.get("pending_issues", [])
        g.metadata = d.get("metadata", {})
        return g

    def add_knowledge(self, key: str, value: Any, category: str = "general",
                      source: str = None, confidence: float = 1.0):
        self.knowledge.append(KnowledgeEntry(key, value, category, source, confidence))
        self.updated_at = datetime.now().isoformat()

    def add_error_pattern(self, error_type: str, description: str,
                          solution: str, tags: List[str] = None):
        self.error_patterns.append(ErrorPattern(error_type, description, solution, tags=tags))
        self.updated_at = datetime.now().isoformat()

    def record_evolution(self, version: str, changes: List[str],
                         improvements: List[str] = None, notes: str = None):
        self.evolution.append(EvolutionRecord(
            version, changes=changes, improvements=improvements,
            from_version=self.version, notes=notes,
        ))
        self.version = version
        self.updated_at = datetime.now().isoformat()

    def get_knowledge(self, key: str) -> Optional[Any]:
        for entry in self.knowledge:
            if entry.key == key:
                return entry.value
        return None

    def find_error_solution(self, error_type: str) -> Optional[str]:
        for p in self.error_patterns:
            if p.error_type == error_type:
                return p.solution
        return None


# ─── Domain ────────────────────────────────────────────────────────────────────

class GenomeDomain:
    """
    知识传承 Domain。

    通过 EventStore 持久化所有变更（append-only 事件溯源），
    Genome 是内存视图，底层以事件驱动更新。

    事件类型：
    - genome.knowledge_add      → knowledge +1
    - genome.error_pattern_add  → error_patterns +1
    - genome.skill_update       → skills 更新
    - genome.evolve             → version bump + evolution record
    - genome.heritage           → 传承记录
    """

    def __init__(self, store: OssEventStore, pubsub: PubSubManager):
        self.store = store
        self.pubsub = pubsub
        # 内存缓存：agent_type → Genome
        self._genomes: Dict[str, Genome] = {}
        # 传承日志
        self._heritage_log: List[HeritageRecord] = []

    # ─── 内部 ─────────────────────────────────────────────────────────────────

    def _genome_key(self, agent_type: str) -> str:
        return f"genome/{agent_type}"

    async def _get_genome(self, agent_type: str) -> Genome:
        """获取或创建 Genome（从 EventStore replay）"""
        if agent_type in self._genomes:
            return self._genomes[agent_type]

        # Replay 事件重建（只取最近 10000 条，过滤 genome.*）
        genome = Genome(agent_type=agent_type)
        try:
            events = await self.store.replay_by_seq(0, limit=10000)
            for ev in events:
                if ev.topic.startswith("genome."):
                    self._apply_event(genome, ev)
        except Exception:
            pass  # 新建 genome

        self._genomes[agent_type] = genome
        return genome

    def _apply_event(self, genome: Genome, event: Event):
        """将事件应用到 Genome 内存视图"""
        try:
            data = event.payload or {}
            if event.topic == "genome.knowledge_add":
                genome.add_knowledge(data["key"], data["value"],
                                     data.get("category"), data.get("source"),
                                     data.get("confidence", 1.0))
            elif event.topic == "genome.error_pattern_add":
                genome.add_error_pattern(data["error_type"], data["description"],
                                        data["solution"], data.get("tags"))
            elif event.topic == "genome.evolve":
                genome.record_evolution(data["version"], data["changes"],
                                        data.get("improvements"), data.get("notes"))
            elif event.topic == "genome.preference_set":
                genome.preferences[data["key"]] = data["value"]
            elif event.topic == "genome.skill_update":
                # 更新或添加 skill
                skill = SkillState(
                    skill_name=data["skill_name"],
                    status=data.get("status", "active"),
                    version=data.get("version", "1.0.0"),
                    config=data.get("config", {}),
                    performance=data.get("performance", 1.0),
                    last_used=data.get("last_used"),
                    tags=data.get("tags", []),
                )
                for i, s in enumerate(genome.skills):
                    if s.skill_name == skill.skill_name:
                        genome.skills[i] = skill
                        break
                else:
                    genome.skills.append(skill)
        except Exception as e:
            logger.warning(f"_apply_event failed for {event.topic}: {e}")

    async def _emit(self, topic: str, data: dict, source: str = "cloud-hub"):
        """发出事件并持久化"""
        ev = Event.make(topic, source, data)
        await self.store.append(ev)
        self.pubsub.publish(ev)
        # 同步更新内存缓存（确保缓存与 store 一致）
        for agent_type, genome in self._genomes.items():
            if ev.topic.startswith("genome."):
                self._apply_event(genome, ev)
        return ev

    # ─── API ──────────────────────────────────────────────────────────────────

    async def genome_get(self, params: dict) -> dict:
        """genome_get: 获取 Genome 当前状态"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        genome = await self._get_genome(agent_type)
        return {"success": True, "genome": genome.to_dict()}

    async def knowledge_add(self, params: dict) -> dict:
        """knowledge_add: 添加知识条目"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        key = params["key"]
        value = params["value"]
        category = params.get("category", "general")
        source = params.get("source")
        confidence = params.get("confidence", 1.0)

        await self._emit("genome.knowledge_add", {
            "key": key, "value": value, "category": category,
            "source": source, "confidence": confidence,
        }, source=agent_type)
        return {"success": True, "knowledge_count": len((await self._get_genome(agent_type)).knowledge)}

    async def knowledge_query(self, params: dict) -> dict:
        """knowledge_query: 查询知识（支持模糊匹配）"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        query = params.get("query", "")
        genome = await self._get_genome(agent_type)

        if not query:
            return {"success": True, "results": [k.to_dict() for k in genome.knowledge]}

        q = query.lower()
        results = [
            k.to_dict() for k in genome.knowledge
            if q in k.key.lower() or q in str(k.value).lower() or q in k.category.lower()
        ]
        return {"success": True, "results": results, "count": len(results)}

    async def error_pattern_add(self, params: dict) -> dict:
        """error_pattern_add: 添加错误模式"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        await self._emit("genome.error_pattern_add", {
            "error_type": params["error_type"],
            "description": params["description"],
            "solution": params["solution"],
            "tags": params.get("tags", []),
        }, source=agent_type)
        return {"success": True}

    async def error_resolve(self, params: dict) -> dict:
        """error_resolve: 查找错误解决方案"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        error_type = params.get("error_type")
        genome = await self._get_genome(agent_type)
        solution = genome.find_error_solution(error_type)
        return {"success": True, "solution": solution}

    async def skill_update(self, params: dict) -> dict:
        """skill_update: 更新技能状态"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        await self._emit("genome.skill_update", {
            "skill_name": params["skill_name"],
            "status": params.get("status", "active"),
            "version": params.get("version", "1.0.0"),
            "config": params.get("config", {}),
            "performance": params.get("performance", 1.0),
            "last_used": datetime.now().isoformat(),
            "tags": params.get("tags", []),
        }, source=agent_type)
        return {"success": True}

    async def evolve(self, params: dict) -> dict:
        """evolve: 记录进化版本"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        changes = params.get("changes", [])
        improvements = params.get("improvements", [])
        notes = params.get("notes")

        current = await self._get_genome(agent_type)
        new_version = self._bump_version(current.version)

        await self._emit("genome.evolve", {
            "version": new_version,
            "changes": changes,
            "improvements": improvements,
            "from_version": current.version,
            "notes": notes,
        }, source=agent_type)
        return {"success": True, "new_version": new_version}

    async def heritage(self, params: dict) -> dict:
        """heritage: 执行传承协议"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        heritage_type = params.get("heritage_type", "restart")
        notes = params.get("notes")

        current = await self._get_genome(agent_type)
        new_version = self._bump_version(current.version)

        record = HeritageRecord(
            from_version=current.version,
            to_version=new_version,
            heritage_type=heritage_type,
            knowledge_transferred=len(current.knowledge),
            errors_transferred=len(current.error_patterns),
            skills_transferred=len(current.skills),
            notes=notes,
        )
        self._heritage_log.append(record)

        # 触发 evolve 事件
        await self._emit("genome.evolve", {
            "version": new_version,
            "changes": [f"heritage:{heritage_type}"],
            "improvements": [],
            "from_version": current.version,
            "notes": notes,
        }, source=agent_type)

        await self._emit("genome.heritage", record.to_dict(), source=agent_type)

        return {"success": True, "record": record.to_dict()}

    async def genome_stats(self, params: dict) -> dict:
        """genome_stats: 获取 Genome 统计"""
        agent_type = params.get("agent_type", AgentType.SHARED)
        genome = await self._get_genome(agent_type)
        return {
            "success": True,
            "stats": {
                "agent_type": genome.agent_type,
                "version": genome.version,
                "knowledge_count": len(genome.knowledge),
                "error_patterns_count": len(genome.error_patterns),
                "skills_count": len(genome.skills),
                "evolution_count": len(genome.evolution),
                "last_updated": genome.updated_at,
            }
        }

    # ─── 辅助 ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _bump_version(version: str) -> str:
        parts = version.split(".")
        if len(parts) == 3:
            parts[2] = str(int(parts[2]) + 1)
        return ".".join(parts)
