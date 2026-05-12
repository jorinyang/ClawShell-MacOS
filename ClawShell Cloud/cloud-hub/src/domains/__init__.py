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
from .deep_think import DeepThinkEngine
from .review import ReviewDomain, ReviewResult
from .feedback_loop import (
    FeedbackControlLoop, MetricCollector, ControlSignal,
    ControlAction, MetricSample,
)
from .global_optimizer import (
    GlobalOptimizer, CostModel,
    OptimizationGoal, OptimizationResult, AllocationPlan,
    ResourceQuota, ResourceType,
)
from .n8n import N8NBridgeDomain

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
    "DeepThinkEngine",
    "ReviewDomain",
    # Schema classes
    "Genome",
    "KnowledgeEntry",
    "ErrorPattern",
    "SkillState",
    "Condition",
    "HealthReport",
    "ReviewResult",
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
    # Feedback & Optimizer
    "FeedbackControlLoop",
    "MetricCollector",
    "ControlSignal",
    "ControlAction",
    "MetricSample",
    "GlobalOptimizer",
    "CostModel",
    "OptimizationGoal",
    "OptimizationResult",
    "AllocationPlan",
    "ResourceQuota",
    "ResourceType",
    # P1b: N8N Bridge Domain
    "N8NBridgeDomain",
]
