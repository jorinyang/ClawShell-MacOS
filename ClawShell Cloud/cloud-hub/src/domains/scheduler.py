"""
ClawShell Cloud Hub — Scheduler Domain
定时任务调度域：基于 Crontab 表达式的后台任务调度器。

scheduler_register    → 注册定时任务
scheduler_unregister  → 注销定时任务
scheduler_list        → 列出所有任务
scheduler_log         → 获取执行日志
scheduler_trigger     → 手动触发任务
scheduler_start       → 启动调度器守护进程
scheduler_shutdown    → 停止调度器
"""
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("scheduler-domain")


class CronExpression:
    """解析并评估标准5字段 cron 表达式。"""

    FIELD_NAMES = ["minute", "hour", "day_of_month", "month", "day_of_week"]
    FIELD_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
    MONTH_NAMES = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    DOW_NAMES = {
        "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
    }

    def __init__(self, expression: str):
        self.expression = expression.strip()
        self._fields = self._parse(self.expression)

    def _parse(self, expr: str) -> List[set]:
        """将 cron 表达式解析为每字段有效值集合。"""
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: '{expr}' — need 5 fields")

        fields = []
        for i, (part, (lo, hi)) in enumerate(zip(parts, self.FIELD_RANGES)):
            values = set()
            for segment in part.split(","):
                segment = segment.strip()

                # 名称替换（月份、星期）
                if i == 3:  # 月份
                    for name, num in self.MONTH_NAMES.items():
                        segment = segment.replace(name, str(num))
                elif i == 4:  # 星期
                    for name, num in self.DOW_NAMES.items():
                        segment = segment.replace(name, str(num))

                if segment == "*":
                    values.update(range(lo, hi + 1))
                elif segment.startswith("*/"):
                    step = int(segment[2:])
                    values.update(range(lo, hi + 1, step))
                elif "-" in segment:
                    start, end = map(int, segment.split("-"))
                    values.update(range(max(lo, start), min(hi, end) + 1))
                else:
                    try:
                        v = int(segment)
                        if lo <= v <= hi:
                            values.add(v)
                    except ValueError:
                        raise ValueError(f"Invalid cron field: '{segment}' in '{expr}'")
            fields.append(values)
        return fields

    def matches(self, dt: Optional[datetime] = None) -> bool:
        """检查当前 cron 表达式是否匹配给定时间（默认：现在）。"""
        if dt is None:
            dt = datetime.now()
        check = [dt.minute, dt.hour, dt.day, dt.month, dt.weekday()]
        # Python 中 Sunday 是 6，cron 中是 0 或 7
        if check[4] == 6:
            check[4] = 0  # 标准化 Python Sunday (6) 为 cron Sunday (0)
        return all(check[i] in self._fields[i] for i in range(5))

    def next_run(self, from_dt: Optional[datetime] = None) -> datetime:
        """查找下一个匹配的时间。"""
        dt = from_dt or datetime.now()
        dt = dt.replace(second=0, microsecond=0)

        # 简单方法：每分钟推进，最多 366 天
        max_iterations = 366 * 24 * 60
        for _ in range(max_iterations):
            dt = datetime.fromtimestamp(dt.timestamp() + 60)
            if self.matches(dt):
                return dt

        raise ValueError(f"Cannot find next match for: {self.expression}")


class SchedulerDomain:
    """
    定时任务调度域。
    scheduler_* → CloudScheduler cron-based task scheduler
    """

    CHECK_INTERVAL = 60  # 秒

    def __init__(self, store, data_dir: str = "scheduler_data"):
        self.store = store
        self._data_dir = data_dir
        self._tasks_file = f"{data_dir}/cron_tasks.json"

        self._tasks: Dict[str, dict] = {}
        self._handlers: Dict[str, callable] = {}
        self._execution_log: List[dict] = []

        # 守护进程
        self._running = False
        self._thread = None

        # 异步锁
        self._lock = asyncio.Lock()

    # ─── Public API ──────────────────────────────────────────────────────────────

    async def scheduler_register(self, params: dict) -> dict:
        """
        scheduler_register: 注册定时任务
        params: {
            task_id: str,       # 任务ID（不提供则自动生成）
            cron: str,          # cron 表达式（必填）
            description: str,  # 任务描述
            handler_name: str,  # 处理器名称
            enabled: bool,      # 是否启用
        }
        """
        task_id = params.get("task_id", str(uuid.uuid4())[:8])
        cron_expr = params.get("cron", "")
        description = params.get("description", "")
        handler_name = params.get("handler_name", "")
        enabled = params.get("enabled", True)

        async with self._lock:
            try:
                CronExpression(cron_expr)  # 验证
            except ValueError as e:
                return {"success": False, "error": f"Invalid cron expression '{cron_expr}': {e}"}

            self._tasks[task_id] = {
                "task_id": task_id,
                "cron": cron_expr,
                "description": description,
                "handler_name": handler_name,
                "enabled": enabled,
                "last_run": None,
                "run_count": 0,
                "fail_count": 0,
            }
            await self._save()
            return {"success": True, "task_id": task_id}

    async def scheduler_unregister(self, params: dict) -> dict:
        """
        scheduler_unregister: 注销定时任务
        params: {task_id: str}
        """
        task_id = params.get("task_id", "")
        async with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                await self._save()
                return {"success": True}
            return {"success": False, "error": "Task not found"}

    async def scheduler_list(self, params: dict) -> dict:
        """
        scheduler_list: 列出所有任务
        """
        async with self._lock:
            return {"tasks": list(self._tasks.values())}

    async def scheduler_log(self, params: dict) -> dict:
        """
        scheduler_log: 获取执行日志
        params: {limit: int}
        """
        limit = params.get("limit", 50)
        async with self._lock:
            return {"logs": self._execution_log[-limit:]}

    async def scheduler_trigger(self, params: dict) -> dict:
        """
        scheduler_trigger: 手动触发任务
        params: {task_id: str}
        """
        task_id = params.get("task_id", "")
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return {"success": False, "error": "Task not found"}
            result = await self._execute_task(task)
            return result

    async def scheduler_set_handler(self, params: dict) -> dict:
        """
        scheduler_set_handler: 注册任务处理器
        params: {name: str, handler: callable}
        """
        name = params.get("name", "")
        handler = params.get("handler")
        if not name or not handler:
            return {"success": False, "error": "name and handler required"}
        self._handlers[name] = handler
        return {"success": True}

    # ─── Daemon ────────────────────────────────────────────────────────────────

    async def scheduler_start(self, params: dict) -> dict:
        """
        scheduler_start: 启动调度器守护进程
        """
        if self._running:
            return {"success": True, "message": "Already running"}

        self._running = True
        import threading

        def _run_loop():
            asyncio.run(self._scheduler_loop())

        self._thread = threading.Thread(target=_run_loop, daemon=True, name="scheduler-daemon")
        self._thread.start()
        return {"success": True, "message": "Scheduler started"}

    async def scheduler_shutdown(self, params: dict) -> dict:
        """
        scheduler_shutdown: 停止调度器
        """
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        return {"success": True, "message": "Scheduler stopped"}

    async def _scheduler_loop(self):
        """主调度循环 — 5秒块以实现快速关闭。"""
        while self._running:
            await self._check_and_execute()
            await asyncio.sleep(5)

    async def _check_and_execute(self):
        """检查所有启用任务并执行到期的任务。"""
        now = datetime.now()
        due_tasks = []

        async with self._lock:
            for task in self._tasks.values():
                if not task.get("enabled", True):
                    continue

                try:
                    expr = CronExpression(task["cron"])
                    if expr.matches(now):
                        due_tasks.append(dict(task))
                except ValueError:
                    continue

        for task in due_tasks:
            await self._execute_task(task)

    async def _execute_task(self, task: dict) -> dict:
        """执行单个任务并记录结果。"""
        task_id = task["task_id"]
        start_time = time.time()
        result = {
            "task_id": task_id,
            "started_at": start_time,
            "status": "executed",
            "error": None,
        }

        handler_name = task.get("handler_name", "")
        handler = self._handlers.get(handler_name)

        if handler:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(task)
                else:
                    handler(task)
            except Exception as e:
                result["status"] = "failed"
                result["error"] = str(e)
        else:
            result["status"] = "skipped"
            result["error"] = f"No handler: {handler_name}"

        result["duration_ms"] = (time.time() - start_time) * 1000

        async with self._lock:
            t = self._tasks.get(task_id)
            if t:
                t["last_run"] = start_time
                t["run_count"] = t.get("run_count", 0) + 1
                if result["status"] == "failed":
                    t["fail_count"] = t.get("fail_count", 0) + 1
            self._execution_log.append(result)
            if len(self._execution_log) > 1000:
                self._execution_log = self._execution_log[-500:]
            await self._save()

        return result

    # ─── Persistence ──────────────────────────────────────────────────────────

    async def _save(self):
        """持久化任务定义到 OSS。"""
        try:
            content = json.dumps(list(self._tasks.values()), ensure_ascii=False, default=str)
            await self.store.save(self._tasks_file, content)
        except Exception as e:
            logger.error(f"Scheduler save error: {e}")

    async def _load(self):
        """从 OSS 加载任务定义。"""
        try:
            content = await self.store.load(self._tasks_file)
            if content:
                tasks = json.loads(content)
                for task in tasks:
                    tid = task.get("task_id")
                    if tid:
                        self._tasks[tid] = task
        except Exception as e:
            logger.warning(f"Scheduler load error: {e}")

    async def on_hub_ready(self):
        """Hub 就绪后加载持久化数据。"""
        await self._load()
