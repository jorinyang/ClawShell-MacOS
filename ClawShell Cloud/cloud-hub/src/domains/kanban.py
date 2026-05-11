"""
ClawShell Cloud Hub — 任务域 (Kanban Domain)
kanban_* — 看板任务管理 + 混合调度

调度模式：
- open_claim: 端侧主动认领，云端仲裁避免重复认领
- cloud_assign: 云端直接指派给指定端侧
- broadcast: 任务广播给所有端侧

云端维护任务全局视图，端侧执行后回调云端同步状态。
数据全部存在 OSS，不依赖 Docker Volume。
"""
import json
import logging
import time
import uuid
from typing import Any, Optional

from ..storage import OssStore

logger = logging.getLogger("cloud-hub.domain.kanban")

BOARDS_PREFIX = "kanban/boards/"
TASKS_PREFIX = "kanban/tasks/"


class KanbanDomain:
    """
    任务域：云端任务协同中枢。
    所有数据存 OSS: kanban/boards/{board_id}.json, kanban/tasks/{task_id}.json
    """

    def __init__(self, store: OssStore):
        self.store = store

    # ─── Board ─────────────────────────────────────────────────────────────

    def _board_key(self, board_id: str) -> str:
        return f"{BOARDS_PREFIX}{board_id}.json"

    def _task_key(self, task_id: str) -> str:
        return f"{TASKS_PREFIX}{task_id}.json"

    async def kanban_board_create(self, params: dict) -> dict:
        """kanban_board_create — 创建看板"""
        board_id = params.get("board_id", str(uuid.uuid4()))
        title = params.get("title", "Default Board")
        description = params.get("description", "")

        board = {
            "board_id": board_id,
            "title": title,
            "description": description,
            "wip_limit": params.get("wip_limit", 5),
            "created_at": self._now(),
            "updated_at": self._now(),
        }
        await self.store.save(self._board_key(board_id), json.dumps(board))
        return {"board": board, "created": True}

    async def kanban_board_list(self, params: dict) -> dict:
        """kanban_board_list — 列出所有看板"""
        keys = await self.store.list_all(BOARDS_PREFIX)
        boards = []
        for k in keys:
            raw = await self.store.load(k)
            if raw:
                boards.append(json.loads(raw))
        return {"boards": boards, "total": len(boards)}

    async def kanban_board_get(self, params: dict) -> dict:
        """kanban_board_get — 获取看板详情（含所有任务）"""
        board_id = params.get("board_id")
        if not board_id:
            return {"error": "board_id required"}
        raw = await self.store.load(self._board_key(board_id))
        if raw is None:
            return {"error": f"Board '{board_id}' not found"}
        board = json.loads(raw)
        # 收集该看板所有任务
        task_keys = await self.store.list_all(TASKS_PREFIX)
        tasks = []
        for tk in task_keys:
            tr = await self.store.load(tk)
            if not tr:
                continue
            t = json.loads(tr)
            if t.get("board_id") == board_id:
                tasks.append(t)
        return {"board": board, "tasks": tasks}

    # ─── Task ─────────────────────────────────────────────────────────────

    async def kanban_task_create(self, params: dict) -> dict:
        """kanban_task_create — 创建任务"""
        task_id = str(uuid.uuid4())
        board_id = params.get("board_id", "default")
        title = params.get("title", "Untitled")
        description = params.get("description", "")
        priority = params.get("priority", "medium")
        tags = params.get("tags", [])
        dispatch_mode = params.get("dispatch_mode", "open_claim")

        task = {
            "task_id": task_id,
            "board_id": board_id,
            "title": title,
            "description": description,
            "priority": priority,
            "tags": tags,
            "status": "pending",
            "dispatch_mode": dispatch_mode,
            "assigned_node": None,
            "created_at": self._now(),
            "updated_at": self._now(),
            "claimed_by": None,
            "claimed_at": None,
            "work_started_at": None,
            "work_done_at": None,
        }
        await self.store.save(self._task_key(task_id), json.dumps(task))
        logger.info(f"Task created: {task_id} [{dispatch_mode}]")
        return {"task": task, "created": True}

    async def kanban_task_move(self, params: dict) -> dict:
        """kanban_task_move — 移动任务列（如 todo→doing→done）"""
        task_id = params.get("task_id")
        new_status = params.get("status")
        node_id = params.get("node_id")

        if not task_id or not new_status:
            return {"error": "task_id and status required"}

        raw = await self.store.load(self._task_key(task_id))
        if raw is None:
            return {"error": f"Task '{task_id}' not found"}
        task = json.loads(raw)

        # 强制状态机
        valid = ["pending", "claimed", "working", "done", "failed"]
        if new_status not in valid:
            return {"error": f"Invalid status: {new_status}"}

        # 工作流验证
        current = task["status"]
        if current == "done":
            return {"error": "Cannot move a completed task"}

        task["status"] = new_status
        task["updated_at"] = self._now()

        if new_status == "working":
            task["work_started_at"] = self._now()
        elif new_status == "done":
            task["work_done_at"] = self._now()
        elif new_status == "failed":
            task["work_done_at"] = self._now()

        await self.store.save(self._task_key(task_id), json.dumps(task))
        return {"task": task, "moved": True}

    async def kanban_task_assign(self, params: dict) -> dict:
        """kanban_task_assign — 云端指派任务给指定端侧（cloud_assign 模式）"""
        task_id = params.get("task_id")
        node_id = params.get("node_id")
        if not task_id or not node_id:
            return {"error": "task_id and node_id required"}

        raw = await self.store.load(self._task_key(task_id))
        if raw is None:
            return {"error": f"Task '{task_id}' not found"}
        task = json.loads(raw)

        if task.get("dispatch_mode") != "cloud_assign":
            return {"error": "Task is not in cloud_assign mode"}

        task["assigned_node"] = node_id
        task["status"] = "assigned"
        task["updated_at"] = self._now()

        await self.store.save(self._task_key(task_id), json.dumps(task))
        logger.info(f"Task {task_id} assigned to node {node_id}")
        return {"task": task, "assigned": True}

    async def kanban_task_claim(self, params: dict) -> dict:
        """
        kanban_task_claim — 端侧认领任务（open_claim 模式）。
        云端做仲裁：同一任务被多端同时认领时，只有第一个成功。
        """
        task_id = params.get("task_id")
        node_id = params.get("node_id")
        if not task_id or not node_id:
            return {"error": "task_id and node_id required"}

        raw = await self.store.load(self._task_key(task_id))
        if raw is None:
            return {"error": f"Task '{task_id}' not found"}
        task = json.loads(raw)

        if task.get("dispatch_mode") == "cloud_assign":
            return {"error": "Task is not open for claim (cloud_assign mode)"}

        if task["status"] not in ("pending", "claimed"):
            return {"error": f"Cannot claim task in status: {task['status']}"}

        # 仲裁：claimed_by 为空则认领成功，否则拒绝
        if task.get("claimed_by") and task["claimed_by"] != node_id:
            return {
                "error": f"Task already claimed by {task['claimed_by']}",
                "claimed_by": task["claimed_by"],
            }

        task["claimed_by"] = node_id
        task["status"] = "claimed"
        task["claimed_at"] = self._now()
        task["updated_at"] = self._now()

        await self.store.save(self._task_key(task_id), json.dumps(task))
        logger.info(f"Task {task_id} claimed by node {node_id}")
        return {"task": task, "claimed": True, "node_id": node_id}

    async def kanban_task_work_done(self, params: dict) -> dict:
        """kanban_task_work_done — 端侧报告任务完成（回调）"""
        task_id = params.get("task_id")
        node_id = params.get("node_id")
        result = params.get("result", "")

        raw = await self.store.load(self._task_key(task_id))
        if raw is None:
            return {"error": f"Task '{task_id}' not found"}
        task = json.loads(raw)

        if task.get("assigned_node") and task["assigned_node"] != node_id:
            return {"error": "Not your assigned task"}

        task["status"] = "done"
        task["work_done_at"] = self._now()
        task["updated_at"] = self._now()
        if result:
            task["result"] = result

        await self.store.save(self._task_key(task_id), json.dumps(task))
        return {"task": task, "done": True}

    async def kanban_task_work_fail(self, params: dict) -> dict:
        """kanban_task_work_fail — 端侧报告任务失败（回调）"""
        task_id = params.get("task_id")
        node_id = params.get("node_id")
        error = params.get("error", "Unknown error")

        raw = await self.store.load(self._task_key(task_id))
        if raw is None:
            return {"error": f"Task '{task_id}' not found"}
        task = json.loads(raw)

        if task.get("assigned_node") and task["assigned_node"] != node_id:
            return {"error": "Not your assigned task"}

        task["status"] = "failed"
        task["work_done_at"] = self._now()
        task["updated_at"] = self._now()
        task["error"] = error

        await self.store.save(self._task_key(task_id), json.dumps(task))
        return {"task": task, "failed": True}

    async def kanban_task_update(self, params: dict) -> dict:
        """kanban_task_update — 更新任务字段"""
        task_id = params.get("task_id")
        updates = params.get("updates", {})
        if not task_id:
            return {"error": "task_id required"}

        raw = await self.store.load(self._task_key(task_id))
        if raw is None:
            return {"error": f"Task '{task_id}' not found"}
        task = json.loads(raw)

        for key in ("title", "description", "priority", "tags"):
            if key in updates:
                task[key] = updates[key]
        task["updated_at"] = self._now()

        await self.store.save(self._task_key(task_id), json.dumps(task))
        return {"task": task, "updated": True}

    async def kanban_task_list(self, params: dict) -> dict:
        """kanban_task_list — 列出任务"""
        board_id = params.get("board_id")
        status = params.get("status")
        node_id = params.get("node_id")

        keys = await self.store.list_all(TASKS_PREFIX)
        tasks = []
        for tk in keys:
            raw = await self.store.load(tk)
            if not raw:
                continue
            t = json.loads(raw)
            if board_id and t.get("board_id") != board_id:
                continue
            if status and t.get("status") != status:
                continue
            if node_id and t.get("assigned_node") != node_id and t.get("claimed_by") != node_id:
                continue
            tasks.append(t)
        return {"tasks": tasks, "total": len(tasks)}

    def _now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
