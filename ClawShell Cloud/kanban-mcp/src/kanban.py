"""
ClawShell Kanban MCP — MCP WebSocket Server
WIP-limit 看板，支持多端同步
"""
import asyncio
import json
import logging
import os
import threading
import uuid
import datetime
import websockets
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("kanban-mcp")

DATA_DIR = os.environ.get("KANBAN_DATA_PATH", "/data")
BOARD_FILE = os.path.join(DATA_DIR, "board.json")
LOCK_FILE = os.path.join(DATA_DIR, "board.lock")
PORT = int(os.environ.get("KANBAN_PORT", "8446"))

DEFAULT_BOARD = {
    "board_id": "default",
    "columns": [
        {"id": "todo",   "name": "待办",     "wip_limit": 10},
        {"id": "doing",  "name": "进行中",   "wip_limit": 3},
        {"id": "done",   "name": "已完成",   "wip_limit": None}
    ],
    "tasks": [],
    "version": 0
}

# ─── Persistence ─────────────────────────────────────────────────────────────

def _ensure_data_dir():
    """Create DATA_DIR, fall back to /tmp if not writable."""
    import tempfile
    global DATA_DIR, BOARD_FILE
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError:
        DATA_DIR = tempfile.mkdtemp(prefix="kanban-")
        BOARD_FILE = os.path.join(DATA_DIR, "board.json")
        os.makedirs(DATA_DIR, exist_ok=True)

def _read_board() -> dict:
    _ensure_data_dir()
    if not os.path.exists(BOARD_FILE):
        board = json.loads(json.dumps(DEFAULT_BOARD))
        _write_board(board)
        return board
    with open(BOARD_FILE) as f:
        return json.load(f)

def _write_board(board: dict):
    with open(BOARD_FILE + ".tmp", "w") as f:
        json.dump(board, f, indent=2, ensure_ascii=False)
    os.replace(BOARD_FILE + ".tmp", BOARD_FILE)

# ─── Board Operations ────────────────────────────────────────────────────────

def kanban_get_board() -> dict:
    return _read_board()

def kanban_list_boards() -> list:
    return [{"board_id": "default", "name": "默认看板"}]

def kanban_create_task(title: str, column: str = "todo", assignee: str = "") -> dict:
    board = _read_board()
    task_id = f"task-{uuid.uuid4().hex[:8]}"
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

def kanban_move_task(task_id: str, target_column: str, expected_version: int = None) -> dict:
    board = _read_board()

    # WIP limit check
    if target_column != "done":
        target_col = next((c for c in board["columns"] if c["id"] == target_column), None)
        if target_col and target_col.get("wip_limit"):
            current_count = sum(
                1 for t in board["tasks"]
                if t["column"] == target_column and t["id"] != task_id
            )
            if current_count >= target_col["wip_limit"]:
                return {"error": f"WIP limit reached for '{target_col['name']}' (limit={target_col['wip_limit']})"}

    for task in board["tasks"]:
        if task["id"] == task_id:
            if expected_version is not None and task["version"] != expected_version:
                return {"error": f"Version conflict: expected {expected_version}, got {task['version']}"}
            task["column"] = target_column
            task["version"] += 1
            task["updated_at"] = datetime.datetime.utcnow().isoformat()
            board["version"] += 1
            _write_board(board)
            return task
    return {"error": f"Task not found: {task_id}"}

def kanban_update_task(task_id: str, title: str = None, assignee: str = None) -> dict:
    board = _read_board()
    for task in board["tasks"]:
        if task["id"] == task_id:
            if title is not None:
                task["title"] = title
            if assignee is not None:
                task["assignee"] = assignee
            task["version"] += 1
            task["updated_at"] = datetime.datetime.utcnow().isoformat()
            board["version"] += 1
            _write_board(board)
            return task
    return {"error": f"Task not found: {task_id}"}

def kanban_delete_task(task_id: str) -> dict:
    board = _read_board()
    original_len = len(board["tasks"])
    board["tasks"] = [t for t in board["tasks"] if t["id"] != task_id]
    if len(board["tasks"]) == original_len:
        return {"error": f"Task not found: {task_id}"}
    board["version"] += 1
    _write_board(board)
    return {"ok": True, "task_id": task_id}

# ─── MCP Request Handler ─────────────────────────────────────────────────────

def handle_request(method: str, params: dict = None) -> dict:
    params = params or {}

    if method == "kanban_list":
        return {"ok": True, "boards": kanban_list_boards()}

    elif method == "kanban_get":
        return {"ok": True, "board": kanban_get_board()}

    elif method == "kanban_task_create":
        return {"ok": True, "task": kanban_create_task(
            params.get("title"),
            params.get("column", "todo"),
            params.get("assignee", "")
        )}

    elif method == "kanban_task_move":
        result = kanban_move_task(
            params.get("task_id"),
            params.get("target_column"),
            params.get("expected_version")
        )
        if "error" in result:
            return {"ok": False, "error": result["error"]}
        return {"ok": True, "task": result}

    elif method == "kanban_task_update":
        result = kanban_update_task(
            params.get("task_id"),
            params.get("title"),
            params.get("assignee")
        )
        if "error" in result:
            return {"ok": False, "error": result["error"]}
        return {"ok": True, "task": result}

    elif method == "kanban_task_delete":
        result = kanban_delete_task(params.get("task_id"))
        if "error" in result:
            return {"ok": False, "error": result["error"]}
        return {"ok": True, **result}

    else:
        return {"ok": False, "error": f"Unknown method: {method}"}

# ─── WebSocket Server ────────────────────────────────────────────────────────

async def ws_handler(ws):
    logger.info(f"Client connected: {ws.remote_address}")
    try:
        async for msg in ws:
            data = json.loads(msg)
            msg_id = data.get("id")
            method = data.get("method")
            params = data.get("params", {})

            if method == "tools/list":
                response = {
                    "id": msg_id,
                    "result": {
                        "tools": [
                            {"Name": "kanban_list", "description": "List all boards"},
                            {"Name": "kanban_get", "description": "Get full board state"},
                            {"Name": "kanban_task_create", "params": ["title", "column"]},
                            {"Name": "kanban_task_move", "params": ["task_id", "target_column", "expected_version"]},
                            {"Name": "kanban_task_update", "params": ["task_id", "title", "assignee"]},
                            {"Name": "kanban_task_delete", "params": ["task_id"]},
                        ]
                    }
                }
            else:
                result = handle_request(method, params)
                response = {"id": msg_id, "result": result}

            await ws.send(json.dumps(response))
    except Exception as e:
        logger.error(f"Handler error: {e}")
    finally:
        logger.info(f"Client disconnected: {ws.remote_address}")

async def main():
    logger.info(f"Kanban MCP WS server starting on port {PORT}")
    async with websockets.serve(ws_handler, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
