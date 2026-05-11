"""
Event Schema - ClawShell Edge Gateway
=====================================

统一事件格式定义。
所有在EventBus中流通的事件都必须符合此格式。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from datetime import datetime
from enum import Enum
import uuid


class EventType(Enum):
    """
    事件类型枚举
    ==============
    
    分类：
    - task.*: 任务相关事件
    - error.*: 错误相关事件  
    - insight.*: 洞察相关事件
    - strategy.*: 策略相关事件
    - system.*: 系统相关事件
    - health.*: 健康检查事件
    - genome.*: 基因组相关事件
    """
    
    # ========== Task Events ==========
    TASK_SCHEDULED = "task.scheduled"
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_CANCELLED = "task.cancelled"
    TASK_TIMEOUT = "task.timeout"
    
    # ========== Error Events ==========
    ERROR_OCCURRED = "error.occurred"
    ERROR_RECOVERED = "error.recovered"
    ERROR_CRITICAL = "error.critical"
    ERROR_RETRY = "error.retry"
    
    # ========== Insight Events ==========
    INSIGHT_GENERATED = "insight.generated"
    INSIGHT_CONSUMED = "insight.consumed"
    INSIGHT_DISCARDED = "insight.discarded"
    
    # ========== Strategy Events ==========
    STRATEGY_SWITCHED = "strategy.switched"
    STRATEGY_LOADED = "strategy.loaded"
    STRATEGY_UPDATED = "strategy.updated"
    
    # ========== System Events ==========
    SYSTEM_STARTED = "system.started"
    SYSTEM_STOPPED = "system.stopped"
    SYSTEM_HEALTH_CHECK = "system.health_check"
    SYSTEM_CONFIG_CHANGED = "system.config_changed"
    
    # ========== Health Events ==========
    HEALTH_API_ERROR = "health.api_error"
    HEALTH_API_RECOVERED = "health.api_recovered"
    HEALTH_BALANCE_LOW = "health.balance_low"
    HEALTH_PERFORMANCE_DEGRADED = "health.performance_degraded"
    
    # ========== Genome Events ==========
    GENOME_LOADED = "genome.loaded"
    GENOME_SAVED = "genome.saved"
    GENOME_HERITAGE = "genome.heritance"
    GENOME_EVOLVED = "genome.evolved"
    
    # ========== Task Market Events ==========
    TASK_REGISTERED = "task.registered"
    TASK_MATCHED = "task.matched"
    TASK_EXECUTED = "task.executed"
    
    # ========== Custom Events ==========
    CUSTOM = "custom"

    # ========== Condition Events ==========
    CONDITION_TRIGGERED = "condition.triggered"
    CONDITION_EVALUATED = "condition.evaluated"
    METRIC_UPDATED = "metric.updated"


class EventSource(Enum):
    """事件来源枚举"""
    OPENCLAW = "openclaw"
    HERMES = "hermes"
    N8N = "n8n"
    CRONS = "crons"
    SKILLS = "skills"
    MCP = "mcp"
    SYSTEM = "system"
    USER = "user"
    EXTERNAL = "external"


@dataclass
class Event:
    """
    统一事件格式
    =============
    
    属性：
    - id: 事件唯一标识符 (UUID)
    - type: 事件类型 (EventType)
    - source: 事件来源 (EventSource 或字符串)
    - payload: 事件数据负载
    - timestamp: 事件发生时间 (ISO8601)
    - trace_id: 用于追踪整个处理链
    - correlation_id: 用于关联相关事件
    - tags: 事件标签，用于过滤
    """
    
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: EventType = EventType.CUSTOM
    source: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "id": self.id,
            "type": self.type.value if isinstance(self.type, EventType) else self.type,
            "source": self.source,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "trace_id": self.trace_id,
            "correlation_id": self.correlation_id,
            "tags": self.tags,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Event":
        """从字典创建"""
        event_type = data.get("type", "custom")
        if isinstance(event_type, str):
            try:
                event_type = EventType(event_type)
            except ValueError:
                event_type = EventType.CUSTOM
        
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            type=event_type,
            source=data.get("source", ""),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", datetime.now().isoformat()),
            trace_id=data.get("trace_id"),
            correlation_id=data.get("correlation_id"),
            tags=data.get("tags", []),
        )
    
    def add_tag(self, tag: str):
        """添加标签"""
        if tag not in self.tags:
            self.tags.append(tag)
    
    def has_tag(self, tag: str) -> bool:
        """检查是否包含标签"""
        return tag in self.tags


@dataclass
class EventFilter:
    """事件过滤器"""
    types: List[EventType] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    
    def matches(self, event: Event) -> bool:
        """检查事件是否匹配过滤器"""
        # 检查类型
        if self.types and event.type not in self.types:
            return False
        
        # 检查来源
        if self.sources and event.source not in self.sources:
            return False
        
        # 检查标签
        if self.tags and not any(tag in self.tags for tag in event.tags):
            return False
        
        # 检查时间范围
        event_time = datetime.fromisoformat(event.timestamp)
        if self.since and event_time < self.since:
            return False
        if self.until and event_time > self.until:
            return False
        
        return True
