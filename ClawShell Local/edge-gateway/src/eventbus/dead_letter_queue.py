#!/usr/bin/env python3
"""
ClawShell Edge EventBus Dead Letter Queue
死信队列管理模块 - 边缘计算版本
功能: 管理处理失败的事件，支持重试和死信处理
"""

import time
import json
import os
import threading
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum


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
    死信队列管理器 - 边缘计算版本
    
    功能：
    - 死信存储（本地文件系统）
    - 重试机制
    - 死信处理
    - 统计监控
    """

    def __init__(self, storage_path: Optional[str] = None, max_retries: int = 3):
        self.storage_path = storage_path or "/tmp/clawshell/edge/eventbus/dead_letter"
        self.max_retries = max_retries
        self._dead_letters: Dict[str, DeadLetter] = {}
        self._lock = threading.Lock()
        os.makedirs(self.storage_path, exist_ok=True)
        self._stats = DLQStats()
        self._load_from_disk()

    def add(self, event: Dict, reason: DLQReason, error_message: str,
            metadata: Optional[Dict] = None) -> str:
        """添加死信"""
        with self._lock:
            dead_letter_id = f"dlq_{int(time.time() * 1000)}_{len(self._dead_letters)}"
            dead_letter = DeadLetter(
                id=dead_letter_id,
                original_event=event,
                reason=reason,
                error_message=error_message,
                metadata=metadata or {}
            )
            self._dead_letters[dead_letter_id] = dead_letter
            self._update_stats()
            self._save_to_disk(dead_letter_id)
            return dead_letter_id

    def retry(self, dead_letter_id: str, processor: Callable[[Dict], bool]) -> bool:
        """重试处理死信"""
        with self._lock:
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
        """获取死信"""
        return self._dead_letters.get(dead_letter_id)

    def get_pending(self, limit: Optional[int] = None) -> List[DeadLetter]:
        """获取待处理死信"""
        pending = [dlq for dlq in self._dead_letters.values() if dlq.processed_at is None]
        pending.sort(key=lambda x: x.created_at)
        return pending[:limit] if limit else pending

    def get_by_reason(self, reason: DLQReason) -> List[DeadLetter]:
        """按原因获取死信"""
        return [dlq for dlq in self._dead_letters.values() if dlq.reason == reason]

    def delete(self, dead_letter_id: str) -> bool:
        """删除死信"""
        with self._lock:
            if dead_letter_id in self._dead_letters:
                del self._dead_letters[dead_letter_id]
                self._update_stats()
                self._delete_from_disk(dead_letter_id)
                return True
            return False

    def purge(self, before_timestamp: Optional[float] = None) -> int:
        """清理死信（默认7天前已处理的）"""
        with self._lock:
            if before_timestamp is None:
                before_timestamp = time.time() - (7 * 24 * 3600)
            to_delete = [
                dlq_id for dlq_id, dlq in self._dead_letters.items()
                if dlq.created_at < before_timestamp and dlq.processed_at is not None
            ]
            for dlq_id in to_delete:
                del self._dead_letters[dlq_id]
                self._delete_from_disk(dlq_id)
            self._update_stats()
            return len(to_delete)

    def get_stats(self) -> DLQStats:
        """获取统计信息"""
        return self._stats

    def _update_stats(self) -> None:
        """更新统计"""
        pending = [d for d in self._dead_letters.values() if d.processed_at is None]
        processed = [d for d in self._dead_letters.values() if d.processed_at is not None]
        self._stats.total_dead_letters = len(self._dead_letters)
        self._stats.pending_count = len(pending)
        self._stats.processed_count = len(processed)
        self._stats.failed_count = len([d for d in processed if d.retry_count >= d.max_retries])
        self._stats.by_reason = {}
        for dlq in self._dead_letters.values():
            reason = dlq.reason.value
            self._stats.by_reason[reason] = self._stats.by_reason.get(reason, 0) + 1

    def _get_file_path(self, dead_letter_id: str) -> str:
        """获取文件路径"""
        return os.path.join(self.storage_path, f"{dead_letter_id}.json")

    def _save_to_disk(self, dead_letter_id: str) -> None:
        """保存到磁盘"""
        dlq = self._dead_letters.get(dead_letter_id)
        if dlq is None:
            return
        data = {
            "id": dlq.id,
            "original_event": dlq.original_event,
            "reason": dlq.reason.value,
            "error_message": dlq.error_message,
            "retry_count": dlq.retry_count,
            "max_retries": dlq.max_retries,
            "created_at": dlq.created_at,
            "last_retry_at": dlq.last_retry_at,
            "processed_at": dlq.processed_at,
            "metadata": dlq.metadata,
        }
        with open(self._get_file_path(dead_letter_id), "w") as f:
            json.dump(data, f)

    def _load_from_disk(self) -> None:
        """从磁盘加载"""
        try:
            for filename in os.listdir(self.storage_path):
                if filename.endswith(".json"):
                    file_path = os.path.join(self.storage_path, filename)
                    with open(file_path, "r") as f:
                        data = json.load(f)
                    dlq = DeadLetter(
                        id=data["id"],
                        original_event=data["original_event"],
                        reason=DLQReason(data["reason"]),
                        error_message=data["error_message"],
                        retry_count=data["retry_count"],
                        max_retries=data["max_retries"],
                        created_at=data["created_at"],
                        last_retry_at=data.get("last_retry_at"),
                        processed_at=data.get("processed_at"),
                        metadata=data.get("metadata", {}),
                    )
                    self._dead_letters[dlq.id] = dlq
            self._update_stats()
        except Exception:
            pass

    def _delete_from_disk(self, dead_letter_id: str) -> None:
        """从磁盘删除"""
        try:
            file_path = self._get_file_path(dead_letter_id)
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass
