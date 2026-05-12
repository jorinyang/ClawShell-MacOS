"""
ClawShell Cloud Hub — MemOS Cloud Client
跨设备记忆同步客户端。

memos_store      → 存储记忆条目
memos_search     → 语义相似度搜索记忆
memos_get        → 获取单条记忆
memos_delete     → 删除记忆
memos_list       → 列出最近记忆
memos_sync       → 从云端增量同步
memos_batch      → 批量存储记忆
memos_stats      → 获取记忆统计

凭证通过环境变量 (CLAWSHELL_MEMOS_API_KEY) 配置。
"""
import json
import logging
import time
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional

logger = logging.getLogger("memos-client")


class MemOSCloudClient:
    """
    MemOS Cloud API 客户端 (memos.memtensor.cn)。
    用于存储和检索跨设备 agent 记忆。
    """

    DEFAULT_BASE_URL = "https://memos.memtensor.cn/api/openmem/v1"

    def __init__(
        self,
        api_key: str = "",
        user_id: str = "",
        base_url: str = "",
    ):
        self._api_key = api_key or ""
        self._user_id = user_id or ""
        self._base_url = base_url or self.DEFAULT_BASE_URL
        self._last_call_time = 0.0
        self._call_count = 0

    # ─── Memory CRUD ─────────────────────────────────────────────────────────

    def store_memory(
        self,
        content: str,
        tags: Optional[List[str]] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        memos_store: 存储记忆条目。
        params: {content: str, tags?: list, metadata?: dict}
        """
        payload = {
            "user_id": self._user_id,
            "content": content,
            "tags": tags or [],
            "metadata": metadata or {},
        }
        return self._post("/memories", payload)

    def search_memories(self, query: str, limit: int = 10) -> List[dict]:
        """
        memos_search: 按语义相似度搜索记忆。
        params: {query: str, limit?: int}
        """
        result = self._post("/memories/search", {
            "user_id": self._user_id,
            "query": query,
            "limit": limit,
        })
        return result.get("memories", []) if isinstance(result, dict) else []

    def get_memory(self, memory_id: str) -> Optional[dict]:
        """
        memos_get: 按 ID 获取单条记忆。
        params: {memory_id: str}
        """
        return self._get(f"/memories/{memory_id}")

    def delete_memory(self, memory_id: str) -> bool:
        """
        memos_delete: 删除记忆条目。
        params: {memory_id: str}
        """
        result = self._delete(f"/memories/{memory_id}")
        return isinstance(result, dict) and result.get("success", False)

    def list_memories(self, limit: int = 50, offset: int = 0) -> List[dict]:
        """
        memos_list: 列出最近记忆。
        params: {limit?: int, offset?: int}
        """
        result = self._get(
            f"/memories?user_id={self._user_id}&limit={limit}&offset={offset}"
        )
        return result.get("memories", []) if isinstance(result, dict) else []

    # ─── Sync ───────────────────────────────────────────────────────────────

    def sync_from_cloud(self, since_timestamp: float = 0) -> List[dict]:
        """
        memos_sync: 拉取指定时间戳后更新的记忆。
        params: {since_timestamp?: float}
        """
        result = self._get(
            f"/memories/sync?user_id={self._user_id}&since={since_timestamp}"
        )
        return result.get("memories", []) if isinstance(result, dict) else []

    def batch_store(self, memories: List[dict]) -> dict:
        """
        memos_batch: 批量存储多条记忆。
        params: {memories: list}
        """
        return self._post("/memories/batch", {
            "user_id": self._user_id,
            "memories": memories,
        })

    # ─── Stats ──────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """
        memos_stats: 获取记忆统计信息。
        """
        return {
            "api_base": self._base_url,
            "user_id": self._user_id[:20] + "..." if self._user_id else "not set",
            "api_key_configured": bool(self._api_key),
            "total_calls": self._call_count,
        }

    # ─── Internal HTTP ───────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        timeout: int = 30,
    ) -> Any:
        """向 MemOS Cloud 发送 HTTP 请求。"""
        if not self._api_key:
            return {"error": "API key not configured", "memories": []}

        url = f"{self._base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        data = None
        if body:
            data = json.dumps(body).encode()

        try:
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            resp = urllib.request.urlopen(req, timeout=timeout)
            self._call_count += 1
            self._last_call_time = time.time()
            return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return {"error": f"HTTP {e.code}", "detail": str(e)}
        except Exception as e:
            return {"error": str(e)}

    def _get(self, path: str) -> Any:
        return self._request("GET", path)

    def _post(self, path: str, body: dict) -> Any:
        return self._request("POST", path, body)

    def _delete(self, path: str) -> Any:
        return self._request("DELETE", path)
