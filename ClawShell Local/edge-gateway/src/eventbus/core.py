"""
EventBus Core - ClawShell Edge Gateway
======================================

事件总线核心实现。
提供事件的发布、订阅、路由和持久化功能。
"""

import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Optional

from .schema import Event, EventType, EventFilter

logger = logging.getLogger(__name__)


class EventBus:
    """
    统一事件总线
    ============
    
    功能：
    - 发布/订阅模式
    - 事件持久化
    - 事件过滤
    - 异步处理
    - 死信队列
    - 云端同步（通过 SyncEngine）
    """
    
    def __init__(
        self,
        persistence_path: str = "~/.clawshell-local/eventbus/events",
        max_history: int = 10000,
        enable_dead_letter: bool = True,
        dead_letter_path: str = "~/.clawshell-local/eventbus/dead_letter",
        sync_engine=None,
    ):
        self.persistence_path = Path(persistence_path).expanduser()
        self.persistence_path.mkdir(parents=True, exist_ok=True)
        self.max_history = max_history
        self.enable_dead_letter = enable_dead_letter
        self._sync_engine = sync_engine
        
        if enable_dead_letter:
            self.dead_letter_path = Path(dead_letter_path).expanduser()
            self.dead_letter_path.mkdir(parents=True, exist_ok=True)
        
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._async_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_history: List[Event] = []
        self._lock = Lock()
        self._running = False
        self._async_thread: Optional[Thread] = None
        self._async_queue: List[Event] = []
        
        logger.info("EventBus initialized (sync_engine=%s)", "set" if sync_engine else "None")

    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """订阅事件（同步）"""
        with self._lock:
            self._subscribers[event_type.value].append(handler)
        logger.debug(f"Subscribed to {event_type.value}")

    def subscribe_async(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """订阅事件（异步）"""
        with self._lock:
            self._async_subscribers[event_type.value].append(handler)
        logger.debug(f"Subscribed async to {event_type.value}")

    def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """取消订阅"""
        with self._lock:
            for subs in [self._subscribers, self._async_subscribers]:
                if event_type.value in subs:
                    try:
                        subs[event_type.value].remove(handler)
                    except ValueError:
                        pass
        logger.debug(f"Unsubscribed from {event_type.value}")

    def publish(self, event: Event) -> None:
        """发布事件"""
        if not event.timestamp:
            event.timestamp = datetime.now().isoformat()
        if not event.id:
            event.id = str(uuid.uuid4())
        
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self.max_history:
                self._event_history = self._event_history[-self.max_history:]
            self._persist_event(event)
            
            event_type_value = event.type.value if isinstance(event.type, EventType) else str(event.type)
            
            if event_type_value in self._subscribers:
                for handler in self._subscribers[event_type_value]:
                    try:
                        handler(event)
                    except Exception as e:
                        logger.error(f"Sync handler error: {e}")
                        self._handle_dead_letter(event, str(e))
            
            if event_type_value in self._async_subscribers:
                self._async_queue.append(event)
        
        # 同步到云端（如果配置了 SyncEngine）
        if self._sync_engine is not None:
            try:
                self._sync_engine.queue_event(event.to_dict())
            except Exception as e:
                logger.warning(f"Failed to queue event to sync_engine: {e}")
        
        logger.info(f"Published: {event_type_value} from {event.source}")

    def publish_batch(self, events: List[Event]) -> None:
        """批量发布事件"""
        for event in events:
            self.publish(event)

    def start_async_processing(self) -> None:
        """启动异步处理线程"""
        if self._running:
            return
        self._running = True
        self._async_thread = Thread(target=self._async_worker, daemon=True)
        self._async_thread.start()
        logger.info("Async event processing started")

    def stop_async_processing(self) -> None:
        """停止异步处理线程"""
        self._running = False
        if self._async_thread:
            self._async_thread.join(timeout=5)
        logger.info("Async event processing stopped")

    def _async_worker(self) -> None:
        """异步处理工作线程"""
        while self._running:
            try:
                if self._async_queue:
                    with self._lock:
                        if self._async_queue:
                            event = self._async_queue.pop(0)
                    
                    event_type_value = event.type.value if isinstance(event.type, EventType) else str(event.type)
                    
                    if event_type_value in self._async_subscribers:
                        for handler in self._async_subscribers[event_type_value]:
                            try:
                                handler(event)
                            except Exception as e:
                                logger.error(f"Async handler error: {e}")
                                self._handle_dead_letter(event, str(e))
                else:
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Async worker error: {e}")

    def _persist_event(self, event: Event) -> None:
        """持久化事件到磁盘"""
        try:
            date_str = event.timestamp[:10]
            date_dir = self.persistence_path / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"{event.timestamp[:19].replace(':', '-')}_{event.id[:8]}.json"
            filepath = date_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(event.to_dict(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to persist event: {e}")

    def _handle_dead_letter(self, event: Event, error: str) -> None:
        """处理死信事件"""
        if not self.enable_dead_letter:
            return
        
        try:
            dead_letter = {
                "event": event.to_dict(),
                "error": error,
                "timestamp": datetime.now().isoformat(),
            }
            
            filename = f"dl_{event.id[:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
            filepath = self.dead_letter_path / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(dead_letter, f, ensure_ascii=False, indent=2)
            
            logger.warning(f"Dead letter stored: {filename}")
        except Exception as e:
            logger.error(f"Failed to store dead letter: {e}")

    def get_history(
        self,
        event_type: Optional[EventType] = None,
        source: Optional[str] = None,
        limit: int = 100,
    ) -> List[Event]:
        """获取事件历史"""
        with self._lock:
            history = self._event_history[-limit:]
        
        if event_type:
            history = [e for e in history if e.type == event_type]
        if source:
            history = [e for e in history if e.source == source]
        
        return history

    def query_events(self, filter: EventFilter) -> List[Event]:
        """查询事件"""
        with self._lock:
            return [e for e in self._event_history if filter.matches(e)]

    def get_stats(self) -> Dict[str, Any]:
        """获取事件统计"""
        with self._lock:
            type_counts = defaultdict(int)
            source_counts = defaultdict(int)
            
            for event in self._event_history:
                event_type_value = event.type.value if isinstance(event.type, EventType) else str(event.type)
                type_counts[event_type_value] += 1
                source_counts[event.source] += 1
            
            return {
                "total_events": len(self._event_history),
                "subscribers_count": {
                    "sync": sum(len(v) for v in self._subscribers.values()),
                    "async": sum(len(v) for v in self._async_subscribers.values()),
                },
                "event_types": dict(type_counts),
                "sources": dict(source_counts),
                "async_queue_size": len(self._async_queue),
            }

    def clear_history(self) -> None:
        """清空历史事件"""
        with self._lock:
            self._event_history.clear()
        logger.warning("Event history cleared")


# 全局单例
_global_eventbus: Optional[EventBus] = None
_global_sync_engine = None


def get_eventbus(sync_engine=None) -> EventBus:
    """获取全局事件总线实例（sync_engine 仅第一次生效）"""
    global _global_eventbus, _global_sync_engine
    if _global_eventbus is None:
        _global_sync_engine = sync_engine
        _global_eventbus = EventBus(sync_engine=sync_engine)
    return _global_eventbus


def configure_eventbus(sync_engine) -> EventBus:
    """配置全局事件总线的 SyncEngine（在初始化后调用）"""
    global _global_eventbus, _global_sync_engine
    if _global_eventbus is None:
        return get_eventbus(sync_engine)
    _global_eventbus._sync_engine = sync_engine
    _global_sync_engine = sync_engine
    return _global_eventbus


def publish(event: Event) -> None:
    """快捷发布函数"""
    get_eventbus().publish(event)


def subscribe(event_type: EventType, handler: Callable[[Event], None]) -> None:
    """快捷订阅函数"""
    get_eventbus().subscribe(event_type, handler)
