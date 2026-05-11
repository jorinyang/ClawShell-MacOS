#!/usr/bin/env python3
"""
ClawShell Cloud Hub — Self-Healing Engine
==========================================
从 ClawShell-Windows lib/layer2/self_healing.py 提取重构

核心能力：
- 自动备份 / 恢复
- 检查点管理
- 服务切换（主/备）
- 自动回滚
"""

import os, json, time, shutil
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime


HEALING_STATE_PATH = Path("~/.cloudshell/.healing_state.json").expanduser()
BACKUP_DIR = Path("~/.cloudshell/backups").expanduser()
CHECKPOINT_DIR = Path("~/.cloudshell/checkpoints").expanduser()


@dataclass
class Backup:
    id: str; timestamp: float; type: str; path: str; size: int
    checksum: str; description: str; status: str
    def to_dict(self) -> Dict:
        return {"id": self.id, "timestamp": self.timestamp, "type": self.type,
                "path": self.path, "size": self.size, "checksum": self.checksum,
                "description": self.description, "status": self.status}


@dataclass
class Checkpoint:
    id: str; timestamp: float; name: str; description: str
    components: List[str]; status: str; metadata: Dict = field(default_factory=dict)
    def to_dict(self) -> Dict:
        return {"id": self.id, "timestamp": self.timestamp, "name": self.name,
                "description": self.description, "components": self.components,
                "status": self.status, "metadata": self.metadata}


@dataclass
class HealingAction:
    action: str; target: str; source: Optional[str] = None
    status: str = "pending"; result: Optional[str] = None
    error: Optional[str] = None; timestamp: float = field(default_factory=time.time)
    def to_dict(self) -> Dict:
        return {"action": self.action, "target": self.target, "source": self.source,
                "status": self.status, "result": self.result, "error": self.error,
                "timestamp": self.timestamp}


def _calc_checksum(path: Path) -> str:
    import hashlib
    if path.is_file():
        with open(path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    elif path.is_dir():
        h = hashlib.md5()
        for item in sorted(path.rglob("*")):
            if item.is_file():
                h.update(item.name.encode())
                h.update(open(item, 'rb').read())
        return h.hexdigest()
    return ""


def _dir_size(path: Path) -> int:
    total = 0
    if path.is_file():
        total = path.stat().st_size
    elif path.is_dir():
        for item in path.rglob("*"):
            if item.is_file():
                total += item.stat().st_size
    return total


class BackupManager:
    def __init__(self):
        self.backup_dir = BACKUP_DIR; self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        if HEALING_STATE_PATH.exists():
            try:
                with open(HEALING_STATE_PATH) as f: return json.load(f)
            except: pass
        return {"backups": [], "checkpoints": []}

    def _save_state(self):
        HEALING_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(HEALING_STATE_PATH, 'w') as f: json.dump(self.state, f, indent=2)

    def create_backup(self, name: str, paths: List[str], backup_type: str = "full") -> Optional[Backup]:
        backup_id = f"backup_{int(time.time())}"
        backup_path = self.backup_dir / backup_id
        backup_path.mkdir(exist_ok=True)
        try:
            total_size = 0
            for p in paths:
                path = Path(p).expanduser()
                if not path.exists(): continue
                dest = backup_path / path.name
                if path.is_file():
                    shutil.copy2(path, dest); total_size += path.stat().st_size
                elif path.is_dir():
                    shutil.copytree(path, dest, dirs_exist_ok=True)
                    total_size += _dir_size(path)
            checksum = _calc_checksum(backup_path)
            b = Backup(id=backup_id, timestamp=time.time(), type=backup_type,
                       path=str(backup_path), size=total_size, checksum=checksum,
                       description=f"{backup_type} backup: {name}", status="completed")
            self.state["backups"].append(b.to_dict()); self._save_state()
            return b
        except Exception as e:
            return Backup(id=backup_id, timestamp=time.time(), type=backup_type,
                         path=str(backup_path), size=0, checksum="",
                         description=f"{backup_type} backup: {name}", status="failed")

    def restore_backup(self, backup_id: str, target_paths: Optional[List[str]] = None) -> bool:
        info = self._get_backup_info(backup_id)
        if not info: return False
        bp = Path(info["path"])
        if not bp.exists(): return False
        try:
            if target_paths:
                for i, tp in enumerate(target_paths):
                    target = Path(tp).expanduser()
                    source = bp / target.name
                    if source.exists():
                        if target.is_dir(): shutil.rmtree(target)
                        shutil.copytree(source, target)
            else:
                for item in bp.iterdir():
                    dest = Path.home() / item.name
                    if dest.exists() and dest.is_dir(): shutil.rmtree(dest)
                    shutil.copytree(item, dest)
            return True
        except: return False

    def _get_backup_info(self, backup_id: str) -> Optional[Dict]:
        for b in self.state.get("backups", []):
            if b["id"] == backup_id: return b
        return None

    def list_backups(self, limit: int = 10) -> List[Backup]:
        bs = [Backup(**b) for b in self.state.get("backups", [])]
        bs.sort(key=lambda x: x.timestamp, reverse=True)
        return bs[:limit]

    def delete_backup(self, backup_id: str) -> bool:
        info = self._get_backup_info(backup_id)
        if not info: return False
        try:
            bp = Path(info["path"])
            if bp.exists(): shutil.rmtree(bp)
            self.state["backups"] = [b for b in self.state["backups"] if b["id"] != backup_id]
            self._save_state(); return True
        except: return False


class CheckpointManager:
    def __init__(self):
        self.checkpoint_dir = CHECKPOINT_DIR
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        if HEALING_STATE_PATH.exists():
            try:
                with open(HEALING_STATE_PATH) as f: return json.load(f)
            except: pass
        return {"backups": [], "checkpoints": []}

    def _save_state(self):
        HEALING_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(HEALING_STATE_PATH, 'w') as f: json.dump(self.state, f, indent=2)

    def create_checkpoint(self, name: str, description: str, components: List[str]) -> Checkpoint:
        cp_id = f"cp_{int(time.time())}"
        bm = BackupManager()
        backup = bm.create_backup(f"checkpoint_{cp_id}", components, "full")
        cp = Checkpoint(id=cp_id, timestamp=time.time(), name=name, description=description,
                        components=components, status="created",
                        metadata={"backup_id": backup.id if backup else None})
        self.state["checkpoints"].append(cp.to_dict()); self._save_state()
        return cp

    def apply_checkpoint(self, checkpoint_id: str) -> bool:
        info = self._get_checkpoint_info(checkpoint_id)
        if not info: return False
        backup_id = info.get("metadata", {}).get("backup_id")
        if not backup_id: return False
        bm = BackupManager()
        return bm.restore_backup(backup_id, info["components"])

    def _get_checkpoint_info(self, checkpoint_id: str) -> Optional[Dict]:
        for c in self.state.get("checkpoints", []):
            if c["id"] == checkpoint_id: return c
        return None

    def list_checkpoints(self, limit: int = 10) -> List[Checkpoint]:
        cps = [Checkpoint(**c) for c in self.state.get("checkpoints", [])]
        cps.sort(key=lambda x: x.timestamp, reverse=True)
        return cps[:limit]


class SelfHealingEngine:
    """自修复引擎"""
    def __init__(self):
        self.backup_manager = BackupManager()
        self.checkpoint_manager = CheckpointManager()
        self.actions: List[HealingAction] = []

    def auto_backup(self, components: Optional[List[str]] = None) -> Optional[Backup]:
        if components is None:
            components = [str(Path.home() / ".cloudshell" / "config")]
        name = f"auto_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return self.backup_manager.create_backup(name, components, "auto")

    def auto_migrate(self, source: str, target: str) -> bool:
        action = HealingAction(action="migrate", target=target, source=source, status="running")
        self.actions.append(action)
        try:
            sp = Path(source).expanduser(); tp = Path(target).expanduser()
            if not sp.exists(): raise FileNotFoundError(f"Source not found: {source}")
            tp.parent.mkdir(parents=True, exist_ok=True)
            if sp.is_dir(): shutil.copytree(sp, tp, dirs_exist_ok=True)
            else: shutil.copy2(sp, tp)
            action.status = "completed"
            action.result = f"Migrated {source} to {target}"
            return True
        except Exception as e:
            action.status = "failed"; action.error = str(e)
            return False
        finally:
            self._save_actions()

    def auto_rollback(self, checkpoint_id: str) -> bool:
        action = HealingAction(action="rollback", target=checkpoint_id, status="running")
        self.actions.append(action)
        try:
            ok = self.checkpoint_manager.apply_checkpoint(checkpoint_id)
            action.status = "completed" if ok else "failed"
            action.result = f"Rolled back to {checkpoint_id}" if ok else "failed"
            return ok
        except Exception as e:
            action.status = "failed"; action.error = str(e)
            return False
        finally:
            self._save_actions()

    def create_recovery_checkpoint(self, name: str, description: str) -> Checkpoint:
        return self.checkpoint_manager.create_checkpoint(name, description, [
            str(Path.home() / ".cloudshell" / "config"),
            str(Path.home() / ".cloudshell" / "workspace"),
        ])

    def get_health_report(self) -> Dict:
        backups = self.backup_manager.list_backups(5)
        checkpoints = self.checkpoint_manager.list_checkpoints(5)
        return {
            "timestamp": time.time(),
            "recent_backups": [b.to_dict() for b in backups],
            "recent_checkpoints": [c.to_dict() for c in checkpoints],
            "pending_actions": len([a for a in self.actions if a.status == "pending"]),
        }

    def _save_actions(self):
        state = {"last_update": time.time(),
                 "actions": [a.to_dict() for a in self.actions[-100:]]}
        HEALING_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(HEALING_STATE_PATH, 'w') as f: json.dump(state, f, indent=2)