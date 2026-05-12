"""InsightDomain — Event-driven insight engine (inspired by ClawShell-Deep insight.py)

核心能力：
- 错误风暴检测：同一来源 5+ 错误 → Insight
- 周期性摘要：每 5 分钟生成一次摘要
- 模式分析：1 小时内 3+ 节点离线 → Insight
- 洞察发布：发布到 PubSub 供其他 Domain 消费

这与 Deep InsightEngine 的核心逻辑完全一致，
但以 MacOS Domain 架构封装，而非散落在独立文件。
"""
import asyncio
import uuid
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from loguru import logger

try:
    from shared.models import Insight, EventMessage, EventCategory, EventPriority
    from shared.models import HealthStatus
except ImportError:
    from ..shared.models import Insight, EventMessage, EventCategory, EventPriority
    from ..shared.models import HealthStatus


class InsightEngine:
    """
    洞察引擎（借鉴 ClawShell-Deep insight.py）。

    订阅事件总线，监听 error.* 和 task.* 事件，
    自动生成有价值的洞察（Insight），发布到事件总线。
    """

    MAX_EVENT_HISTORY = 1000
    ERROR_STORM_THRESHOLD = 5       # 5+ 错误触发警告
    OFFLINE_STORM_THRESHOLD = 3      # 3+ 节点离线触发模式
    SUMMARY_INTERVAL_SECONDS = 300   # 每 5 分钟生成摘要

    def __init__(
        self,
        node_id: str = "hub-01",
        event_bus=None,  # PubSubManager 或 EventBus
        pubsub=None,
        summary_interval: int = 300,
    ):
        self.node_id = node_id
        self.event_bus = event_bus
        self.pubsub = pubsub
        self.summary_interval = summary_interval

        # 事件历史（滑动窗口）
        self.event_history: deque[EventMessage] = deque(maxlen=self.MAX_EVENT_HISTORY)

        # 洞察存储
        self.insights: list[Insight] = []

        # 错误计数（source → count）
        self._error_count: dict[str, int] = defaultdict(int)

        # 注册事件订阅
        self._register_subscriptions()

        logger.info(f"InsightDomain initialized (node={self.node_id})")

    def _register_subscriptions(self):
        """向事件总线订阅相关事件"""
        if self.pubsub:
            self.pubsub.subscribe("error.*", self._on_error)
            self.pubsub.subscribe("task.*", self._on_task)
            self.pubsub.subscribe("node.offline", self._on_node_offline)
            self.pubsub.subscribe("*", self._on_any)
            logger.debug("InsightDomain subscriptions registered")

    # ── Event Handlers ────────────────────────────────────────────────────────

    async def _on_error(self, event: EventMessage):
        """处理错误事件：更新计数，检测错误风暴"""
        self.event_history.append(event)
        source = event.source or "unknown"
        self._error_count[source] += 1

        # 错误风暴检测
        if self._error_count[source] >= self.ERROR_STORM_THRESHOLD:
            insight = self._create_insight(
                title=f"Error storm on {source}",
                content=f"Node {source} had {self._error_count[source]} errors in recent history",
                category="alert",
                severity=EventPriority.CRITICAL,
                actionable=True,
                action={"type": "investigate", "target": source},
            )
            await self._publish_insight(insight)
            self._error_count[source] = 0  # 重置计数

    async def _on_task(self, event: EventMessage):
        """处理任务事件：记录历史"""
        self.event_history.append(event)

    async def _on_node_offline(self, event: EventMessage):
        """处理节点离线事件：检测多节点离线模式"""
        self.event_history.append(event)

        # 模式：1 小时内 3+ 节点离线
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=1)
        recent_offline = [
            e for e in self.event_history
            if e.category == EventCategory.NODE
            and e.event_type == "node.offline"
            and e.timestamp > cutoff
        ]

        if len(recent_offline) >= self.OFFLINE_STORM_THRESHOLD:
            nodes = list(set(e.source for e in recent_offline))
            insight = self._create_insight(
                title=f"Multiple nodes offline: {len(nodes)}",
                content=f"{len(nodes)} ganglions offline in the last hour: {nodes}",
                category="pattern",
                severity=EventPriority.HIGH,
            )
            await self._publish_insight(insight)

    async def _on_any(self, event: EventMessage):
        """处理任意事件：记录历史"""
        self.event_history.append(event)

    # ── Periodic Insight Generation ───────────────────────────────────────────

    async def generate_periodic_insight(self) -> Optional[Insight]:
        """每 SUMMARY_INTERVAL_SECONDS 生成一次摘要洞察"""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=5)
        recent = [e for e in self.event_history if e.timestamp > cutoff]

        if not recent:
            return None

        errors = [e for e in recent if e.category == EventCategory.ERROR]
        tasks = [e for e in recent if e.category == EventCategory.TASK]
        nodes = set(e.source for e in recent)

        content = (
            f"5-min summary: {len(nodes)} nodes, "
            f"{len(tasks)} tasks, {len(errors)} errors"
        )

        insight = self._create_insight(
            title=f"Summary {now.strftime('%H:%M')}",
            content=content,
            category="summary",
            severity=EventPriority.LOW,
        )
        await self._publish_insight(insight)
        return insight

    async def analyze_patterns(self) -> list[Insight]:
        """分析事件历史，返回模式洞察"""
        insights = []
        now = datetime.now(timezone.utc)

        # 模式1：多节点离线（已在 _on_node_offline 检测）
        cutoff = now - timedelta(hours=1)
        offline = [
            e for e in self.event_history
            if e.category == EventCategory.NODE
            and e.event_type == "node.offline"
            and e.timestamp > cutoff
        ]
        if len(offline) >= self.OFFLINE_STORM_THRESHOLD:
            nodes = list(set(e.source for e in offline))
            insights.append(self._create_insight(
                title=f"Multiple offline: {len(nodes)}",
                content=f"{len(nodes)} ganglions offline: {nodes}",
                category="pattern",
                severity=EventPriority.HIGH,
            ))

        # 模式2：高频错误源
        for source, count in list(self._error_count.items()):
            if count >= self.ERROR_STORM_THRESHOLD:
                insights.append(self._create_insight(
                    title=f"Frequent errors: {source}",
                    content=f"{source} generated {count} errors",
                    category="pattern",
                    severity=EventPriority.HIGH,
                ))

        return insights

    # ── Insight Publishing ────────────────────────────────────────────────────

    async def _publish_insight(self, insight: Insight):
        """将洞察发布到事件总线"""
        self.insights.append(insight)
        logger.info(f"Insight generated: {insight.title}")

        if self.pubsub:
            ev = EventMessage(
                event_id=str(uuid.uuid4()),
                category=EventCategory.INSIGHT,
                event_type="insight.generated",
                source=self.node_id,
                priority=insight.severity,
                payload=insight.model_dump(mode="json"),
            )
            await self.pubsub.publish(ev)

    def _create_insight(
        self,
        title: str,
        content: str,
        category: str = "general",
        severity: EventPriority = EventPriority.NORMAL,
        actionable: bool = False,
        action: Optional[dict[str, Any]] = None,
    ) -> Insight:
        return Insight(
            insight_id=str(uuid.uuid4()),
            title=title,
            content=content,
            category=category,
            severity=severity,
            source=self.node_id,
            actionable=actionable,
            action=action,
            created_at=datetime.now(timezone.utc),
        )

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self):
        """启动周期洞察生成任务"""
        asyncio.create_task(self._periodic_loop())
        logger.info("InsightDomain started")

    async def _periodic_loop(self):
        """后台周期循环：定期生成摘要和模式洞察"""
        while True:
            await asyncio.sleep(self.summary_interval)
            try:
                await self.generate_periodic_insight()
                for insight in await self.analyze_patterns():
                    await self._publish_insight(insight)
            except Exception:
                logger.exception("Periodic insight loop error")

    # ── Sync Wrapper ─────────────────────────────────────────────────────────

    def sync_generate_insight(self, title: str, content: str,
                               category: str = "general",
                               severity: EventPriority = EventPriority.NORMAL) -> Insight:
        """同步接口：手动生成一条洞察"""
        return self._create_insight(title, content, category, severity)
