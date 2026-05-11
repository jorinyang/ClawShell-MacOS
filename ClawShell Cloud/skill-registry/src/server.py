"""
ClawShell Skill Registry — MCP Server
存储技能骨架的云端注册表
"""
import json
import sqlite3
import uuid
import os
from pathlib import Path

DB_PATH = os.environ.get("REGISTRY_DB_PATH", "/data/registry.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
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
    conn.commit()
    conn.close()

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
    skill_id = data.get("id", str(uuid.uuid4()))
    import datetime
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

init_db()
