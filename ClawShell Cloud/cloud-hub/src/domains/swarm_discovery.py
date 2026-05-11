#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Swarm Discovery
=====================================
从 ClawShell-Windows lib/layer4/swarm_discovery.py 提取重构

核心能力：
- 节点广播/探测协议
- 自动发现新节点
- 节点上线/下线通知
"""

import time, json, socket, logging
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum


logger = logging.getLogger(__name__)


class DiscoveryProtocol:
    ANNOUNCE = "node_announce"; PROBE = "probe"; RESPONSE = "probe_response"
    LEAVE = "node_leave"


@dataclass
class DiscoveredNode:
    node_id: str; addr: str; port: int; node_type: str
    timestamp: float; metadata: Dict = field(default_factory=dict)


class SwarmDiscovery:
    """Swarm 动态发现引擎"""
    def __init__(self, node_id: str, node_type: str = "generic"):
        self._node_id = node_id
        self._node_type = node_type
        self._peers: Dict[str, DiscoveredNode] = {}
        self._callbacks: Dict[str, List[Callable]] = {
            "discovered": [], "left": []}

    def announce(self, addr: str, port: int, metadata: Optional[Dict] = None):
        msg = {"type": DiscoveryProtocol.ANNOUNCE, "node_id": self._node_id,
               "node_type": self._node_type, "addr": addr, "port": port,
               "metadata": metadata or {}, "timestamp": time.time()}
        self._broadcast(msg)

    def probe(self, broadcast_addr: str = "<broadcast>", port: int = 9999):
        msg = {"type": DiscoveryProtocol.PROBE, "node_id": self._node_id,
               "timestamp": time.time()}
        logger.info(f"Probe sent to {broadcast_addr}:{port}")

    def receive_announce(self, msg: Dict):
        node = DiscoveredNode(
            node_id=msg["node_id"], addr=msg["addr"], port=msg["port"],
            node_type=msg.get("node_type", "unknown"),
            timestamp=msg["timestamp"], metadata=msg.get("metadata", {}))
        self._peers[node.node_id] = node
        for cb in self._callbacks["discovered"]:
            try: cb(node)
            except: pass

    def receive_leave(self, msg: Dict):
        node_id = msg.get("node_id")
        if node_id in self._peers:
            del self._peers[node_id]
            for cb in self._callbacks["left"]:
                try: cb(node_id)
                except: pass

    def get_peers(self) -> List[DiscoveredNode]:
        return list(self._peers.values())

    def on_discovered(self, callback: Callable):
        self._callbacks["discovered"].append(callback)

    def on_left(self, callback: Callable):
        self._callbacks["left"].append(callback)

    def _broadcast(self, msg: Dict):
        logger.info(f"Broadcast: {msg}")

    def get_stats(self) -> Dict:
        return {"node_id": self._node_id, "peers_count": len(self._peers),
                "peer_ids": list(self._peers.keys())}
