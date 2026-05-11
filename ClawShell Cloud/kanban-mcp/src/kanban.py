"""
ClawShell Kanban MCP Server
WIP-limit 任务看板的云端存储
"""
import json
import os
import threading
from pathlib import Path

DATA_DIR = os.environ.get("KANBAN_DATA_PATH", "/data")
BOARD_FILE = os.path.join(DATA_DIR, "board.json")
LOCK_FILE = os.path.join(DATA_DIR, "board.lock")

DEFAULT_BOARD = {
    "board_id": "default",
    "columns": [
        {"id": "todo", "name": "待办", "wip_limit": 10},
        {"id": "doing", "name": "进行中", "wip_limit": 3},
        {"id": "done", "name": "已完成", "wip_limit": None}
    ],
    "tasks": [],
    "version": 0
}

def _read_board():
    if not os.path.exists(BOARD_FILE):
        return json.loads(json.dumps(DEFAULT_BOARD))
    with open(BOARD_FILE) as f:
        return json.load(f)

def _write_board(board):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(BOARD_FILE + ".tmp", "w") as f:
        json.dump(board, f, indent=2, ensure_ascii=False)
    os.replace(BOARD_FILE + ".tmp", BOARD_FILE)

def kanban_get_board():
    return _read_board()

def kanban_create_task(title: str, column: str = "todo", assignee: str = ""):
    board = _read_board()
    import uuid
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    import datetime
    task = {
        "id": task_id,
        "title": title,
        "column": column,
        "assignee": assignee,
        "created_at": datetime.datetime.utcnow().isoformat(),
        "updated_at": datetime.datetime.utcnow().isoformat(),
        "version": 1
    }
    board["tasks"].append(task)
    board["version"] += 1
    _write_board(board)
    return task

def kanban_move_task(task_id: str, target_column: str, expected_version: int = None):
    board = _read_board()
    for task in board["tasks"]:
        if task["id"] == task_id:
            if expected_version is not None and task["version"] != expected_version:
                return {"error": f"Version conflict: expected {expected_version}, got {task['version']}"}
            task["column"] = target_column
            task["version"] += 1
            import datetime
            task["updated_at"] = datetime.datetime.utcnow().isoformat()
            board["version"] += 1
            _write_board(board)
            return task
    return {"error": f"Task not found: {task_id}"}

def kanban_delete_task(task_id: str):
    board = _read_board()
    original_len = len(board["tasks"])
    board["tasks"] = [t for t in board["tasks"] if t["id"] != task_id]
    if len(board["tasks"]) == original_len:
        return {"error": f"Task not found: {task_id}"}
    board["version"] += 1
    _write_board(board)
    return {"success": True, "task_id": task_id}
