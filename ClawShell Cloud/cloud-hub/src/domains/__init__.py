# Domains package — cloud-hub domain handlers

from .kanban import KanbanDomain
from .skill import SkillDomain
from .node import NodeDomain
from .memory import MemoryDomain
from .workflow import WorkflowDomain
from .genome import GenomeDomain, Genome, KnowledgeEntry, ErrorPattern, SkillState
from .adaptive import (
    AdaptiveDomain, ConditionEngine, StrategySwitcher,
    SelfHealing, Condition, HealthReport,
)
from .swarm import (
    SwarmDomain, NodeRegistry, TrustManager, EcologyMatcher,
    Node, NodeType, NodeStatus,
)

__all__ = [
    # Base
    "DomainHandler",
    # Core domains
    "MemoryDomain",
    "KanbanDomain",
    "SkillDomain",
    "NodeDomain",
    "WorkflowDomain",
    # New domains (from Windows synthesis)
    "GenomeDomain",
    "AdaptiveDomain",
    "SwarmDomain",
    # Schema classes
    "Genome",
    "KnowledgeEntry",
    "ErrorPattern",
    "SkillState",
    "Condition",
    "HealthReport",
    "Node",
    "NodeType",
    "NodeStatus",
    # Engines
    "ConditionEngine",
    "StrategySwitcher",
    "SelfHealing",
    "TrustManager",
    "EcologyMatcher",
    "NodeRegistry",
]
