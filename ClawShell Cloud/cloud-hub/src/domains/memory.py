"""
ClawShell Cloud Hub — 记忆域 + 知识域 (Memory + Knowledge Base Domain)
memory_*  → memos-cloud API 代理
knowledge_* → OSS 知识库

记忆域：笔记同步、搜索、标签管理
知识域：文档、剪藏、网页内容（直接存 OSS，作为记忆域的扩展存储）
"""
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger("memory-domain")


class MemoryDomain:
    """
    记忆域 + 知识域 Handler。
    memory_* → memos-cloud REST API（只读代理，写入走端侧）
    knowledge_* → OSS 知识库（cloud-hub 自管）
    """

    def __init__(self, store, memos_api_base: str = "", memos_api_key: str = ""):
        self.store = store
        self.memos_api_base = memos_api_base.rstrip("/")
        self.memos_api_key = memos_api_key

    # ─── 记忆域: memos-cloud ───────────────────────────────────────────────

    async def memory_sync(self, params: dict) -> dict:
        """
        memory_sync: 增量同步记忆（从 memos-cloud 拉取）
        params: {since: unix_timestamp}
        """
        if not self.memos_api_base:
            return {"items": [], "message": "memos not configured"}

        import aiohttp
        headers = {}
        if self.memos_api_key:
            headers["Authorization"] = f"Bearer {self.memos_api_key}"

        since = params.get("since", 0)
        url = f"{self.memos_api_base}/api/memo?since={since}&limit=50"

        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        items = data.get("data", [])
                        # 写入本地 OSS 快照
                        for item in items:
                            await self.store.save(
                                f"memory/snapshots/{item['uid']}.json",
                                json.dumps(item)
                            )
                        return {"items": items, "count": len(items)}
                    else:
                        return {"items": [], "error": f"memos status {resp.status}"}
        except Exception as e:
            logger.error(f"memory_sync error: {e}")
            return {"items": [], "error": str(e)}

    async def memory_snapshot(self, params: dict) -> dict:
        """
        memory_snapshot: 全量快照（OSS 存储，key: memory/snapshots/）
        """
        if not self.memos_api_base:
            return {"message": "memos not configured"}

        sync_result = await self.memory_sync({"since": 0})
        count = sync_result.get("count", 0)
        return {
            "snapshotted": count,
            "message": f"Snapshot complete: {count} items"
        }

    async def memory_query(self, params: dict) -> dict:
        """
        memory_query: 语义搜索记忆（OSS + 关键词）
        params: {query: str, limit: int}
        """
        query = params.get("query", "")
        limit = params.get("limit", 10)

        if not query:
            return {"items": [], "query": query}

        # 先搜索 OSS 快照
        try:
            all_items = await self.store.list_all("memory/snapshots/")
            matched = [
                json.loads(item) for item in all_items
                if query.lower() in json.loads(item).get("content", "").lower()
            ][:limit]
            return {"items": matched, "source": "oss", "query": query}
        except Exception as e:
            logger.error(f"memory_query error: {e}")
            return {"items": [], "error": str(e)}

    async def memory_tags(self, params: dict) -> dict:
        """memory_tags: 列出所有记忆标签"""
        try:
            all_items = await self.store.list_all("memory/snapshots/")
            tag_count: Dict[str, int] = {}
            for item_raw in all_items:
                item = json.loads(item_raw)
                for tag in item.get("tags", []):
                    tag_count[tag] = tag_count.get(tag, 0) + 1
            tags = [{"tag": t, "count": c} for t, c in sorted(tag_count.items(), key=lambda x: -x[1])]
            return {"tags": tags}
        except Exception as e:
            return {"tags": [], "error": str(e)}

    # ─── 知识域: OSS 知识库 ────────────────────────────────────────────────

    async def knowledge_index(self, params: dict) -> dict:
        """
        knowledge_index: 将内容索引到 OSS 知识库
        params: {content: str, title: str, source: str, tags: list}
        """
        content = params.get("content", "")
        title = params.get("title", "Untitled")
        source = params.get("source", "manual")
        tags = params.get("tags", [])

        if not content:
            return {"error": "content required"}

        import hashlib
        doc_id = hashlib.sha256(content.encode()).hexdigest()[:16]
        doc = {
            "doc_id": doc_id,
            "title": title,
            "content": content,
            "source": source,
            "tags": tags,
            "indexed_at": time.time(),
            "indexed_by": "cloud-hub",
        }
        await self.store.save(f"knowledge/docs/{doc_id}.json", json.dumps(doc))
        logger.info(f"Indexed doc {doc_id}: {title}")
        return {"doc_id": doc_id, "title": title, "indexed": True}

    async def knowledge_search(self, params: dict) -> dict:
        """
        knowledge_search: 搜索知识库（关键词匹配）
        params: {query: str, limit: int}
        """
        query = params.get("query", "").lower()
        limit = params.get("limit", 10)

        if not query:
            return {"results": [], "query": query}

        try:
            all_docs = await self.store.list_all("knowledge/docs/")
            results = []
            for doc_raw in all_docs:
                doc = json.loads(doc_raw)
                if query in doc.get("content", "").lower() or query in doc.get("title", "").lower():
                    results.append({
                        "doc_id": doc["doc_id"],
                        "title": doc["title"],
                        "source": doc.get("source"),
                        "tags": doc.get("tags", []),
                    })
            return {"results": results[:limit], "query": query, "count": len(results)}
        except Exception as e:
            return {"results": [], "error": str(e)}

    async def knowledge_list(self, params: dict) -> dict:
        """knowledge_list: 列出知识库所有文档"""
        try:
            all_docs_keys = await self.store.list_all("knowledge/docs/")
            docs = []
            for key in all_docs_keys:
                raw = await self.store.load(key)
                if not raw:
                    continue
                try:
                    doc = json.loads(raw)
                    docs.append({
                        "doc_id": doc.get("doc_id"),
                        "title": doc.get("title"),
                        "source": doc.get("source"),
                        "tags": doc.get("tags", []),
                        "indexed_at": doc.get("indexed_at"),
                    })
                except (json.JSONDecodeError, TypeError):
                    continue
            return {"docs": docs, "total": len(docs)}
        except Exception as e:
            return {"docs": [], "error": str(e)}

    async def knowledge_delete(self, params: dict) -> dict:
        """knowledge_delete: 从知识库删除文档"""
        doc_id = params.get("doc_id")
        if not doc_id:
            return {"error": "doc_id required"}
        try:
            await self.store.delete(f"knowledge/docs/{doc_id}.json")
            return {"deleted": doc_id}
        except Exception as e:
            return {"error": str(e)}
