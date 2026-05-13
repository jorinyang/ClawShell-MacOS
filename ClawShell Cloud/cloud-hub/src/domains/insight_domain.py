"""InsightDomain — Event-driven insight engine (inspired by ClawShell-Deep insight.py)
Core capabilities:
- Error storm detection: 5+ errors from same source → Insight
- Periodic summary: every 5 minutes
- Pattern analysis: 3+ nodes offline in 1h → Insight
"""
import asyncio, uuid
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from loguru import logger

try:
    from shared.models import Insight, EventMessage, EventCategory, EventPriority
except (ImportError, ModuleNotFoundError):
    import sys as _sys
    _sys.path.insert(0, str(__file__).rsplit('/domains/', 1)[0])
    from shared.models import Insight, EventMessage, EventCategory, EventPriority


class InsightEngine:
    MAX_EVENT_HISTORY = 1000
    ERROR_STORM_THRESHOLD = 5
    OFFLINE_STORM_THRESHOLD = 3
    SUMMARY_INTERVAL_SECONDS = 300

    def __init__(self, node_id: str = "hub-01", event_bus=None, pubsub=None, summary_interval: int = 300):
        self.node_id = node_id
        self.event_bus = event_bus
        self.pubsub = pubsub
        self.summary_interval = summary_interval
        self.event_history: deque = deque(maxlen=self.MAX_EVENT_HISTORY)
        self.insights: list = []
        self._error_count: dict = defaultdict(int)
        self._running = False
        logger.info(f"InsightDomain initialized (node={self.node_id})")

    # ── Event Handlers (sync) ───────────────────────────────────────────────
    def _on_error(self, event: EventMessage):
        self.event_history.append(event)
        source = event.source or "unknown"
        self._error_count[source] += 1
        if self._error_count[source] >= self.ERROR_STORM_THRESHOLD:
            insight = self._create_insight(
                title=f"Error storm on {source}",
                content=f"Node {source} had {self._error_count[source]} errors",
                category="alert", severity=EventPriority.CRITICAL,
                actionable=True, action={"type": "investigate", "target": source},
            )
            self._publish_insight(insight)
            self._error_count[source] = 0

    def _on_task(self, event: EventMessage):
        self.event_history.append(event)

    def _on_node_offline(self, event: EventMessage):
        self.event_history.append(event)
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=1)
        recent = [e for e in self.event_history
                  if e.category == EventCategory.NODE and e.event_type == "node.offline"
                  and e.timestamp > cutoff]
        if len(recent) >= self.OFFLINE_STORM_THRESHOLD:
            nodes = list(set(e.source for e in recent))
            insight = self._create_insight(
                title=f"Multiple nodes offline: {len(nodes)}",
                content=f"{len(nodes)} ganglions offline: {nodes}",
                category="pattern", severity=EventPriority.HIGH,
            )
            self._publish_insight(insight)

    def _on_any(self, event: EventMessage):
        self.event_history.append(event)

    # ── Insight Publishing (sync) ───────────────────────────────────────────
    def _publish_insight(self, insight: Insight):
        self.insights.append(insight)
        logger.info(f"Insight: {insight.title}")

    def _create_insight(self, title: str, content: str,
                        category: str = "general",
                        severity: EventPriority = EventPriority.NORMAL,
                        actionable: bool = False,
                        action: Optional[dict] = None) -> Insight:
        return Insight(
            insight_id=str(uuid.uuid4()), title=title, content=content,
            category=category, severity=severity, source=self.node_id,
            actionable=actionable, action=action,
            created_at=datetime.now(timezone.utc),
        )

    # ── Lifecycle ───────────────────────────────────────────────────────────
    async def start(self):
        if self.pubsub:
            await self.pubsub.subscribe("error.*", self._on_error)
            await self.pubsub.subscribe("task.*", self._on_task)
            await self.pubsub.subscribe("node.offline", self._on_node_offline)
            await self.pubsub.subscribe("*", self._on_any)
        self._running = True
        asyncio.create_task(self._periodic_loop())
        logger.info("InsightDomain started")

    async def stop(self):
        self._running = False
        logger.info("InsightDomain stopped")

    async def _periodic_loop(self):
        while self._running:
            await asyncio.sleep(self.summary_interval)
            try:
                await self.generate_periodic_insight()
            except Exception:
                logger.exception("Periodic insight loop error")

    async def generate_periodic_insight(self) -> Optional[Insight]:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=5)
        recent = [e for e in self.event_history if e.timestamp > cutoff]
        if not recent:
            return None
        errors = [e for e in recent if e.category == EventCategory.ERROR]
        tasks = [e for e in recent if e.category == EventCategory.TASK]
        nodes = set(e.source for e in recent)
        insight = self._create_insight(
            title=f"Summary {now.strftime('%H:%M')}",
            content=f"5-min summary: {len(nodes)} nodes, {len(tasks)} tasks, {len(errors)} errors",
            category="summary", severity=EventPriority.LOW,
        )
        self._publish_insight(insight)
        return insight

    def sync_generate_insight(self, title: str, content: str,
                              category: str = "general",
                              severity: EventPriority = EventPriority.NORMAL) -> Insight:
        return self._create_insight(title, content, category, severity)
