"""
ClawShell Cloud Hub — Pub/Sub Manager
支持：
- 客户端按 topic 订阅（支持通配符 node.state.*）
- 服务端向指定 topic 发布事件
- 支持 wildcard 订阅（如 node.state.* 匹配 node.state.A）
- 所有在线客户端自动收到广播
"""
import asyncio
import json
import logging
import fnmatch
import re
from typing import Callable, Dict, List, Set

from ..event_store.schema import Event, Topic

logger = logging.getLogger("cloud-hub.pubsub")

# WS 连接包装
Connection = any


class Subscription:
    """
    单个订阅。
    conn: WS 连接
    topics: 该连接订阅的 topic 列表（支持通配符）
    last_seq: 该连接已收到的最新事件序列号（用于 replay 时定位）
    """
    def __init__(self, conn: Connection, topics: List[str], last_seq: int = 0):
        self.conn = conn
        self.topics = topics      # e.g. ["skill.registered", "node.state.*"]
        self.last_seq = last_seq  # 该订阅者已确认的 seq


class PubSubManager:
    """
    云端 Pub/Sub 管理器。
    - 管理所有 WS 连接和订阅
    - 维护全局 topic → [Subscription] 倒排索引
    - publish() 时向所有匹配的订阅者推送
    """

    def __init__(self):
        # topic 订阅索引: topic → Set[Subscription]
        self._topic_subs: Dict[str, Set[Subscription]] = {}
        # 所有活跃订阅
        self._subscriptions: Set[Subscription] = set()
        # 锁
        self._lock = asyncio.Lock()
        # 全局锁，防止 publish 并发
        self._pub_lock = asyncio.Lock()

    # ─── 订阅管理 ────────────────────────────────────────────────────────────────

    async def subscribe(
        self, conn: Connection, topics: List[str], last_seq: int = 0
    ) -> Subscription:
        """客户端订阅一组 topics，返回 Subscription 对象"""
        sub = Subscription(conn, topics, last_seq)
        async with self._lock:
            self._subscriptions.add(sub)
            for t in topics:
                # 规范化 topic（替换 * 为正则）
                norm = self._norm_topic(t)
                if norm not in self._topic_subs:
                    self._topic_subs[norm] = set()
                self._topic_subs[norm].add(sub)
        logger.info(f"Subscribe: conn={id(conn)} topics={topics} last_seq={last_seq}")
        return sub

    async def unsubscribe(self, conn: Connection) -> None:
        """断开连接时移除所有相关订阅"""
        async with self._lock:
            to_remove = [s for s in self._subscriptions if s.conn is conn]
            for s in to_remove:
                self._subscriptions.discard(s)
                for topic_set in self._topic_subs.values():
                    topic_set.discard(s)
            logger.info(f"Unsubscribe: conn={id(conn)} removed={len(to_remove)}")

    async def update_seq(self, conn: Connection, seq: int) -> None:
        """更新订阅者的 last_seq（客户端确认已处理到 seq）"""
        async with self._lock:
            for s in self._subscriptions:
                if s.conn is conn:
                    s.last_seq = max(s.last_seq, seq)

    # ─── 发布 ──────────────────────────────────────────────────────────────────

    async def publish(self, event: Event) -> int:
        """
        向所有订阅了 event.topic 的在线客户端推送事件。
        返回推送数量。
        """
        if not event.topic:
            return 0

        # 匹配订阅者
        matched: List[Subscription] = []
        async with self._pub_lock:
            # 精确匹配
            if event.topic in self._topic_subs:
                matched.extend(self._topic_subs[event.topic])
            # 通配符匹配 (subscription="node.state.*" 匹配 event.topic="node.state.A")
            actual_topic = self._norm_topic(event.topic)
            for norm_topic, subs in self._topic_subs.items():
                if self._wildcard_match(norm_topic, actual_topic):
                    for s in subs:
                        if s not in matched:
                            matched.append(s)

        # 推送
        sent = 0
        msg = json.dumps(event.to_dict())
        for sub in matched:
            try:
                await sub.conn.send(msg)
                sent += 1
            except Exception as e:
                logger.warning(f"Failed to send to conn={id(sub.conn)}: {e}")

        logger.debug(f"Published {event.topic} to {sent} subscribers (seq={event.seq})")
        return sent

    async def broadcast_all(self, event: Event) -> int:
        """广播给所有已连接的客户端（不论订阅了什么）"""
        sent = 0
        msg = json.dumps(event.to_dict())
        async with self._lock:
            subscribers = list(self._subscriptions)
        for sub in subscribers:
            try:
                await sub.conn.send(msg)
                sent += 1
            except Exception as e:
                logger.warning(f"Failed to broadcast to conn={id(sub.conn)}: {e}")
        return sent

    # ─── 查询 ──────────────────────────────────────────────────────────────────

    def get_subscription_count(self) -> int:
        return len(self._subscriptions)

    def get_active_topics(self) -> List[str]:
        return list(self._topic_subs.keys())

    # ─── 工具 ──────────────────────────────────────────────────────────────────

    def _norm_topic(self, topic: str) -> str:
        """规范化 topic（用于索引）：替换 * 为通配符标记"""
        # 保持 * 不变，储存原始形式
        return topic

    def _wildcard_match(self, pattern_topic: str, actual_topic: str) -> bool:
        """检查 actual_topic 是否匹配 pattern_topic（支持末尾 *）"""
        # node.state.* → 匹配 node.state.A / node.state.B
        if pattern_topic.endswith(".*"):
            prefix = pattern_topic[:-2]
            return actual_topic == prefix or actual_topic.startswith(prefix + ".")
        if pattern_topic == "*":
            return True
        return pattern_topic == actual_topic
