"""
ClawShell Skill Registry — MCP WebSocket Server
提供技能注册、发布、查询的 MCP 工具接口
"""
import asyncio
import json
import logging
import os
import sqlite3
import uuid
import websockets
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("skill-registry")

DB_PATH = os.environ.get("REGISTRY_DB_PATH", "/data/registry.db")
PORT = int(os.environ.get("SKILL_PORT", "8445"))

# ─── Database ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS skills (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            version TEXT NOT NULL,
            cloud_only INTEGER DEFAULT 0,
            adapter_required TEXT,
            local_template TEXT,
            parameters TEXT,
            published_by TEXT,
            published_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_name_version ON skills(name, version)")
    conn.commit()
    conn.close()

# ─── Skill CRUD ─────────────────────────────────────────────────────────────

def skill_list() -> list:
    conn = get_db()
    rows = conn.execute("SELECT * FROM skills ORDER BY published_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def skill_get(skill_id: str) -> dict:
    conn = get_db()
    row = conn.execute("SELECT * FROM skills WHERE id = ?", (skill_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def skill_create(data: dict) -> dict:
    import datetime
    skill_id = data.get("id", str(uuid.uuid4()))
    now = datetime.datetime.utcnow().isoformat()
    conn = get_db()
    conn.execute("""
        INSERT INTO skills (id, name, description, version, cloud_only,
                           adapter_required, local_template, parameters,
                           published_by, published_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        skill_id,
        data["name"],
        data.get("description", ""),
        data.get("version", "1.0.0"),
        int(data.get("cloud_only", False)),
        json.dumps(data.get("adapter_required", [])),
        data.get("local_template", ""),
        json.dumps(data.get("parameters", [])),
        data.get("published_by", "anonymous"),
        now, now
    ))
    conn.commit()
    conn.close()
    return skill_get(skill_id)

def skill_delete(skill_id: str) -> bool:
    conn = get_db()
    cur = conn.execute("DELETE FROM skills WHERE id = ?", (skill_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0

# ─── MCP Request Handler ────────────────────────────────────────────────────

def handle_request(method: str, params: dict = None) -> dict:
    params = params or {}

    if method == "skill_list":
        return {"ok": True, "skills": skill_list()}

    elif method == "skill_get":
        skill = skill_get(params.get("skill_id", ""))
        if skill:
            return {"ok": True, "skill": skill}
        return {"ok": False, "error": "Skill not found"}

    elif method == "skill_publish":
        result = skill_create(params)
        return {"ok": True, "skill": result}

    elif method == "skill_delete":
        ok = skill_delete(params.get("skill_id", ""))
        return {"ok": ok, "error": None if ok else "Not found"}

    else:
        return {"ok": False, "error": f"Unknown method: {method}"}

# ─── WebSocket Server ───────────────────────────────────────────────────────

async def ws_handler(ws):
    logger.info(f"Client connected: {ws.remote_address}")
    try:
        async for msg in ws:
            data = json.loads(msg)

            # MCP request/response envelope
            msg_id = data.get("id")
            method = data.get("method")
            params = data.get("params", {})

            if method == "tools/list":
                # Return available tools
                response = {
                    "id": msg_id,
                    "result": {
                        "tools": [
                            {"name": "skill_list", "description": "List all published skills"},
                            {"name": "skill_get", "description": "Get skill details", "params": ["skill_id"]},
                            {"name": "skill_publish", "description": "Publish a new skill", "params": ["name", "version", "description"]},
                            {"name": "skill_delete", "description": "Delete a skill", "params": ["skill_id"]},
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
    init_db()
    logger.info(f"Skill Registry MCP WS server starting on port {PORT}")
    async with websockets.serve(ws_handler, "0.0.0.0", PORT):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
