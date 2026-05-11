#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Dead Letter Queue
========================================
从 ClawShell-Windows lib/core/eventbus/dead_letter_queue.py 提取重构

核心能力：
- 死信存储与重试
- DLQReason 枚举（重试超限/无效消息/处理错误/超时）
- 统计监控
- 磁盘持久化
"""

import time
import json
import os
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging

logger = logging.getLogger("dlq")


class DLQReason(Enum):
    """死信原因"""
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    INVALID_MESSAGE = "invalid_message"
    PROCESSING_ERROR = "processing_error"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass
class DeadLetter:
    """死信"""
    id: str
    original_event: Dict
    reason: DLQReason
    error_message: str
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    last_retry_at: Optional[float] = None
    processed_at: Optional[float] = None
    metadata: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "original_event": self.original_event,
            "reason": self.reason.value,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "last_retry_at": self.last_retry_at,
            "processed_at": self.processed_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "DeadLetter":
        return cls(
            id=d["id"],
            original_event=d["original_event"],
            reason=DLQReason(d["reason"]),
            error_message=d["error_message"],
            retry_count=d["retry_count"],
            max_retries=d["max_retries"],
            created_at=d["created_at"],
            last_retry_at=d.get("last_retry_at"),
            processed_at=d.get("processed_at"),
            metadata=d.get("metadata", {}),
        )


@dataclass
class DLQStats:
    """死信队列统计"""
    total_dead_letters: int = 0
    pending_count: int = 0
    processed_count: int = 0
    failed_count: int = 0
    by_reason: Dict = field(default_factory=dict)


class DeadLetterQueue:
    """
    死信队列管理器
    """

    def __init__(self, storage_path: Optional[str] = None, max_retries: int = 3):
        self.storage_path = storage_path or "/tmp/cloudshell/dlq"
        self.max_retries = max_retries
        self._dead_letters: Dict[str, DeadLetter] = {}
        self._stats = DLQStats()
        os.makedirs(self.storage_path, exist_ok=True)
        self._load_from_disk()

    def add(
        self,
        event: Dict,
        reason: DLQReason,
        error_message: str,
        metadata: Optional[Dict] = None,
    ) -> str:
        """添加死信"""
        dl_id = f"dlq_{int(time.time() * 1000)}_{len(self._dead_letters)}"
        dl = DeadLetter(
            id=dl_id,
            original_event=event,
            reason=reason,
            error_message=error_message,
            metadata=metadata or {},
        )
        self._dead_letters[dl_id] = dl
        self._update_stats()
        self._save_to_disk(dl_id)
        return dl_id

    def retry(self, dead_letter_id: str, processor: Callable[[Dict], bool]) -> bool:
        """重试处理死信"""
        if dead_letter_id not in self._dead_letters:
            return False
        dlq = self._dead_letters[dead_letter_id]
        if dlq.processed_at is not None:
            return False

        dlq.retry_count += 1
        dlq.last_retry_at = time.time()

        try:
            success = processor(dlq.original_event)
            if success:
                dlq.processed_at = time.time()
                self._update_stats()
                self._save_to_disk(dead_letter_id)
                return True
        except Exception as e:
            dlq.error_message = str(e)

        if dlq.retry_count >= dlq.max_retries:
            dlq.processed_at = time.time()

        self._save_to_disk(dead_letter_id)
        return False

    def get(self, dead_letter_id: str) -> Optional[DeadLetter]:
        return self._dead_letters.get(dead_letter_id)

    def get_pending(self, limit: Optional[int] = None) -> List[DeadLetter]:
        pending = [d for d in self._dead_letters.values() if d.processed_at is None]
        pending.sort(key=lambda x: x.created_at)
        return pending[:limit] if limit else pending

    def get_by_reason(self, reason: DLQReason) -> List[DeadLetter]:
        return [d for d in self._dead_letters.values() if d.reason == reason]

    def delete(self, dead_letter_id: str) -> bool:
        if dead_letter_id in self._dead_letters:
            del self._dead_letters[dead_letter_id]
            self._update_stats()
            self._delete_from_disk(dead_letter_id)
            return True
        return False

    def purge(self, before_timestamp: Optional[float] = None) -> int:
        """清理已处理的死信"""
        if before_timestamp is None:
            before_timestamp = time.time() - (7 * 24 * 3600)
        to_delete = [
            did for did, d in self._dead_letters.items()
            if d.created_at < before_timestamp and d.processed_at is not None
        ]
        for did in to_delete:
            del self._dead_letters[did]
            self._delete_from_disk(did)
        self._update_stats()
        return len(to_delete)

    def reprocess_all(
        self,
        processor: Callable[[Dict], bool],
        max_per_batch: int = 100,
    ) -> Dict:
        results = {"success": 0, "failed": 0, "remaining": 0}
        for dlq in self.get_pending(limit=max_per_batch):
            if self.retry(dlq.id, processor):
                results["success"] += 1
            else:
                results["failed"] += 1
        results["remaining"] = len(self.get_pending())
        return results

    def get_stats(self) -> DLQStats:
        return self._stats

    def _update_stats(self) -> None:
        pending = [d for d in self._dead_letters.values() if d.processed_at is None]
        processed = [d for d in self._dead_letters.values() if d.processed_at is not None]
        self._stats.total_dead_letters = len(self._dead_letters)
        self._stats.pending_count = len(pending)
        self._stats.processed_count = len(processed)
        self._stats.failed_count = len([d for d in processed if d.retry_count >= d.max_retries])
        self._stats.by_reason = {}
        for d in self._dead_letters.values():
            reason = d.reason.value
            self._stats.by_reason[reason] = self._stats.by_reason.get(reason, 0) + 1

    def _file_path(self, dead_letter_id: str) -> str:
        return os.path.join(self.storage_path, f"{dead_letter_id}.json")

    def _save_to_disk(self, dead_letter_id: str) -> None:
        dlq = self._dead_letters.get(dead_letter_id)
        if dlq is None:
            return
        try:
            with open(self._file_path(dead_letter_id), "w") as f:
                json.dump(dlq.to_dict(), f)
        except Exception as e:
            logger.warning(f"Failed to save DLQ {dead_letter_id}: {e}")

    def _load_from_disk(self) -> None:
        try:
            for fn in os.listdir(self.storage_path):
                if fn.endswith(".json"):
                    try:
                        with open(os.path.join(self.storage_path, fn)) as f:
                            data = json.load(f)
                        dlq = DeadLetter.from_dict(data)
                        self._dead_letters[dlq.id] = dlq
                    except Exception:
                        pass
            self._update_stats()
        except Exception:
            pass

    def _delete_from_disk(self, dead_letter_id: str) -> None:
        try:
            p = self._file_path(dead_letter_id)
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass