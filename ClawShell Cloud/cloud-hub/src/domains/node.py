"""
ClawShell Cloud Hub — 节点域 (Node Domain)
node_* — 端侧节点注册、能力发现、协同调度

端侧启动时向 cloud-hub 注册，云端维护节点注册表（OSS 持久化 + 内存缓存）。
云端调度器（KanbanDomain / SkillDomain）依赖 NodeDomain 做节点选择。
"""
import json
import logging
import time
import uuid
from typing import Any, Optional, List

from ..storage import OssStore
from ..protocol import (
    NODE_STATUS_ONLINE, NODE_STATUS_OFFLINE,
    NODE_STATUS_BUSY, NODE_STATUS_IDLE,
)

logger = logging.getLogger("cloud-hub.domain.node")

NODE_PREFIX = "node/registry/"


class NodeDomain:
    """
    节点域：端侧节点注册与发现中心。
    节点注册信息存 OSS (node/registry/{node_id}.json)，内存缓存加速热路径。
    """

    def __init__(self, store: OssStore):
        self.store = store
        # 内存缓存：node_id → NodeInfo
        self._online_nodes: dict[str, dict] = {}

    def _node_key(self, node_id: str) -> str:
        return f"{NODE_PREFIX}{node_id}.json"

    async def node_register(self, params: dict) -> dict:
        """
        node_register — 端侧注册自己到云端。
        node_info: {name, platform, capabilities, tags, version, metadata}
        """
        node_id = params.get("node_id")
        node_info = params.get("node_info", {})
        if not node_id:
            raise ValueError("node_id required")

        existing = await self._load_node(node_id)

        node = {
            "node_id": node_id,
            "name": node_info.get("name", f"node-{node_id[:8]}"),
            "platform": node_info.get("platform", "unknown"),
            "capabilities": node_info.get("capabilities", []),
            "tags": node_info.get("tags", {}),
            "version": node_info.get("version", ""),
            "status": NODE_STATUS_ONLINE,
            "registered_at": existing.get("registered_at") if existing else self._now(),
            "last_seen_at": self._now(),
            "active_tasks": [],
            "metadata": node_info.get("metadata", {}),
        }

        await self.store.save(self._node_key(node_id), json.dumps(node))
        self._online_nodes[node_id] = node
        logger.info(f"Node registered: {node_id} [{node.get('platform')}]")
        return {"node": node, "registered": True}

    async def node_heartbeat(self, params: dict) -> dict:
        """node_heartbeat — 端侧定期心跳，更新在线状态"""
        node_id = params.get("node_id")
        if not node_id:
            raise ValueError("node_id required")
        node = await self._load_node(node_id)
        if node is None:
            return await self.node_register(params)
        node["last_seen_at"] = self._now()
        node["status"] = NODE_STATUS_ONLINE
        await self.store.save(self._node_key(node_id), json.dumps(node))
        self._online_nodes[node_id] = node
        return {"node_id": node_id, "status": NODE_STATUS_ONLINE}

    async def node_unregister(self, params: dict) -> dict:
        """node_unregister — 端侧关闭时注销"""
        node_id = params.get("node_id")
        if not node_id:
            raise ValueError("node_id required")
        node = await self._load_node(node_id)
        if node:
            node["status"] = NODE_STATUS_OFFLINE
            node["unregistered_at"] = self._now()
            await self.store.save(self._node_key(node_id), json.dumps(node))
        self._online_nodes.pop(node_id, None)
        logger.info(f"Node unregistered: {node_id}")
        return {"node_id": node_id, "unregistered": True}

    async def node_update_status(self, params: dict) -> dict:
        """node_update_status — 端侧更新自己的状态（idle/busy）"""
        node_id = params.get("node_id")
        status = params.get("status")
        if not node_id or not status:
            raise ValueError("node_id and status required")
        node = await self._load_node(node_id)
        if node is None:
            return {"error": f"Node '{node_id}' not found"}
        node["status"] = status
        node["last_seen_at"] = self._now()
        await self.store.save(self._node_key(node_id), json.dumps(node))
        self._online_nodes[node_id] = node
        return {"node_id": node_id, "status": status}

    async def node_get(self, params: dict) -> dict:
        """node_get — 获取指定节点信息"""
        node_id = params.get("node_id")
        if not node_id:
            raise ValueError("node_id required")
        node = await self._load_node(node_id)
        if node is None:
            return {"error": f"Node '{node_id}' not found"}
        return {"node": node}

    async def node_list(self, params: dict) -> dict:
        """node_list — 列出所有已注册节点"""
        status_filter = params.get("status")
        platform_filter = params.get("platform")
        keys = await self.store.list_all(NODE_PREFIX)
        all_nodes = []
        for k in keys:
            raw = await self.store.load(k)
            if raw:
                all_nodes.append(json.loads(raw))
        filtered = all_nodes
        if status_filter:
            filtered = [n for n in filtered if n.get("status") == status_filter]
        if platform_filter:
            filtered = [n for n in filtered if n.get("platform") == platform_filter]
        return {"nodes": filtered, "total": len(filtered)}

    async def node_discover(self, params: dict) -> dict:
        """
        node_discover — 云端查询符合条件的节点（用于调度器选择最优节点）。
        按 capability / status / platform / tags 筛选。
        """
        capability = params.get("capability")
        status = params.get("status", NODE_STATUS_ONLINE)
        platform = params.get("platform")
        tags = params.get("tags", {})

        keys = await self.store.list_all(NODE_PREFIX)
        candidates = []
        for k in keys:
            raw = await self.store.load(k)
            if not raw:
                continue
            n = json.loads(raw)
            if n.get("status") != status:
                continue
            if platform and n.get("platform") != platform:
                continue
            if capability and capability not in n.get("capabilities", []):
                continue
            if tags:
                ntags = n.get("tags", {})
                if not all(ntags.get(tk) == tv for tk, tv in tags.items()):
                    continue
            candidates.append(n)

        # 按最后可见时间排序（最近在线优先）
        candidates.sort(key=lambda n: n.get("last_seen_at", ""), reverse=True)
        return {"nodes": candidates, "total": len(candidates)}

    async def node_report_task(self, params: dict) -> dict:
        """node_report_task — 端侧报告正在执行的任务（用于负载感知调度）"""
        node_id = params.get("node_id")
        active_tasks = params.get("active_tasks", [])
        if not node_id:
            raise ValueError("node_id required")
        node = await self._load_node(node_id)
        if node is None:
            return {"error": f"Node '{node_id}' not found"}
        node["active_tasks"] = active_tasks
        node["status"] = NODE_STATUS_BUSY if active_tasks else NODE_STATUS_IDLE
        node["last_seen_at"] = self._now()
        await self.store.save(self._node_key(node_id), json.dumps(node))
        self._online_nodes[node_id] = node
        return {"node_id": node_id, "active_tasks": len(active_tasks)}

    # ─── 内部方法 ─────────────────────────────────────────────────────────────

    async def _load_node(self, node_id: str) -> Optional[dict]:
        if node_id in self._online_nodes:
            return self._online_nodes[node_id]
        raw = await self.store.load(self._node_key(node_id))
        if raw:
            node = json.loads(raw)
            self._online_nodes[node_id] = node
            return node
        return None

    def get_online_nodes(self) -> List[dict]:
        """获取当前在线节点列表（内存缓存）"""
        return list(self._online_nodes.values())

    def _now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
