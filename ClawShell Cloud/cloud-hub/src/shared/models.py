"""ClawShell 2.0 — Pydantic shared models (inspired by ClawShell-Deep)
Aligns with SPEC.md: NodeType, Task, EventMessage, Skill models.
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────────────────────────

class NodeType(str, Enum):
    HUB = "hub"
    EDGE = "edge"

class NodeStatus(str, Enum):
    OFFLINE = "offline"
    ONLINE = "online"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"

class Strategy(str, Enum):
    DEFAULT = "default"
    EMERGENCY = "emergency"
    ECONOMY = "economy"
    AGGRESSIVE = "aggressive"
    CONSERVATIVE = "conservative"

class EventCategory(str, Enum):
    TASK = "task"
    NODE = "node"
    INSIGHT = "insight"
    STRATEGY = "strategy"
    ERROR = "error"
    SYSTEM = "system"
    MEMORY = "memory"
    KNOWLEDGE = "knowledge"

class EventPriority(int, Enum):
    LOW = 0
    NORMAL = 50
    HIGH = 80
    CRITICAL = 100

class TaskStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    WARNING = "warning"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"

class OpenClawVariant(str, Enum):
    STANDARD = "standard"
    MINIMAL = "minimal"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    REMOTE = "remote"
    WINDOWS = "windows"
    MACOS = "macos"
    UNKNOWN = "unknown"

class CapabilityDomain(str, Enum):
    SKILL = "skill"
    TOOL = "tool"
    API = "api"
    MODEL = "model"
    SERVICE = "service"


# ── Node Models ───────────────────────────────────────────────────────────────

class NodeInfo(BaseModel):
    node_id: str
    node_type: NodeType = NodeType.EDGE
    variant: OpenClawVariant = OpenClawVariant.UNKNOWN
    hostname: str = ""
    os: str = ""
    status: NodeStatus = NodeStatus.OFFLINE
    capabilities: list[str] = Field(default_factory=list)
    plugins: list[str] = Field(default_factory=list)
    registered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_heartbeat: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)

    class Config:
        use_enum_values = True


class NodeHeartbeat(BaseModel):
    node_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: NodeStatus = NodeStatus.ONLINE
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    disk_percent: float = 0.0
    active_tasks: int = 0


class CortexInfo(BaseModel):
    node_id: str = "hub-01"
    node_type: NodeType = NodeType.HUB
    version: str = "2.1.0"
    status: NodeStatus = NodeStatus.ONLINE
    connected_edges: int = 0
    uptime_seconds: float = 0.0


# ── Task Models ────────────────────────────────────────────────────────────────

class Task(BaseModel):
    task_id: str
    title: str
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING
    priority: EventPriority = EventPriority.NORMAL
    assigned_to: Optional[str] = None
    created_by: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    result: Optional[dict[str, Any]] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    max_retries: int = 3
    retry_count: int = 0
    timeout_seconds: int = 300

    class Config:
        use_enum_values = True


class TaskResult(BaseModel):
    task_id: str
    success: bool
    output: dict[str, Any] = Field(default_factory=dict)
    error_message: str = ""
    duration_ms: float = 0.0
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Event Models ──────────────────────────────────────────────────────────────

class EventMessage(BaseModel):
    event_id: str = ""
    category: EventCategory
    event_type: str
    source: str
    target: Optional[str] = None
    priority: EventPriority = EventPriority.NORMAL
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[str] = None
    ttl_seconds: int = 60

    class Config:
        use_enum_values = True


# ── Insight / Knowledge / Memory ─────────────────────────────────────────────

class Insight(BaseModel):
    insight_id: str
    title: str
    content: str
    category: str = "general"
    severity: EventPriority = EventPriority.NORMAL
    source: str = "hub"
    actionable: bool = False
    action: Optional[dict[str, Any]] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        use_enum_values = True


class Knowledge(BaseModel):
    knowledge_id: str
    title: str
    content: str
    category: str = "general"
    tags: list[str] = Field(default_factory=list)
    source: str = "hub"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Memory(BaseModel):
    memory_id: str
    content: str
    importance: float = 0.5
    decay_factor: float = 0.95
    last_access: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    access_count: int = 0


# ── Plugin Models ─────────────────────────────────────────────────────────────

class Plugin(BaseModel):
    plugin_id: str
    name: str
    domain: CapabilityDomain
    provider: str
    endpoint: Optional[str] = None
    enabled: bool = True
    health_status: HealthStatus = HealthStatus.UNKNOWN

    class Config:
        use_enum_values = True


class PluginRegistry(BaseModel):
    node_id: str
    plugins: list[Plugin] = Field(default_factory=list)
    registered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Health / Repair ───────────────────────────────────────────────────────────

class RepairAction(BaseModel):
    action_type: str
    target: str
    status: str = "pending"
    message: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class HealthReport(BaseModel):
    overall: HealthStatus = HealthStatus.UNKNOWN
    components: dict[str, HealthStatus] = Field(default_factory=dict)
    issues: list[dict[str, Any]] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        use_enum_values = True


# ── Swarm / Domain Stats ─────────────────────────────────────────────────────

class SwarmNode(BaseModel):
    node_id: str
    version: str
    status: NodeStatus = NodeStatus.OFFLINE
    capabilities: list[str] = Field(default_factory=list)
    last_seen: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    trust_score: float = 1.0

    class Config:
        use_enum_values = True


class DomainStats(BaseModel):
    domain: str
    requests: int = 0
    errors: int = 0
    avg_latency_ms: float = 0.0
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MetricRecord(BaseModel):
    metric_id: str
    value: float
    unit: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tags: dict[str, str] = Field(default_factory=dict)
