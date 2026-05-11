"""
ClawShell Cloud Hub — 技能域 (Skill Domain)
skill_* — 云端技能注册、发现、调度

技能注册：端侧向云端注册技能元数据（存 OSS）
技能发现：云端维护技能目录，端侧查询
技能调度：云端向指定端侧发起技能执行请求
"""
import json
import logging
import time
import uuid
from typing import Any, Optional

from ..storage import OssStore

logger = logging.getLogger("cloud-hub.domain.skill")

SKILL_PREFIX = "skill/definitions/"
INVOKE_PREFIX = "skill/invocations/"


class SkillDomain:
    """
    技能域：云端技能协调中心。
    技能定义存在 OSS (skill/definitions/{skill_id}.json)。
    技能执行由云端调度到指定端侧，结果回调写入 skill/invocations/{invoke_id}.json。
    """

    def __init__(self, store: OssStore):
        self.store = store

    # ─── 技能注册 ─────────────────────────────────────────────────────────────

    async def skill_register(self, params: dict) -> dict:
        """
        skill_register — 端侧向云端注册技能。
        skill_def: {name, version, description, trigger_words, input_schema, output_schema}
        node_id: 注册端侧节点ID
        """
        skill_id = params.get("skill_id") or str(uuid.uuid4())
        skill_def = params.get("skill_def", {})
        node_id = params.get("node_id")
        if not skill_def.get("name"):
            raise ValueError("skill_def.name required")
        if not node_id:
            raise ValueError("node_id required")

        skill = {
            "skill_id": skill_id,
            "name": skill_def.get("name"),
            "version": skill_def.get("version", "1.0.0"),
            "description": skill_def.get("description", ""),
            "trigger_words": skill_def.get("trigger_words", []),
            "input_schema": skill_def.get("input_schema", {}),
            "output_schema": skill_def.get("output_schema", {}),
            "registered_node": node_id,
            "status": "active",
            "created_at": self._now(),
            "updated_at": self._now(),
        }

        key = f"{SKILL_PREFIX}{skill_id}.json"
        await self.store.save(key, json.dumps(skill))
        logger.info(f"Skill registered: {skill_id} by node {node_id}")
        return {"skill": skill, "registered": True}

    async def skill_unregister(self, params: dict) -> dict:
        """skill_unregister — 端侧注销技能"""
        skill_id = params.get("skill_id")
        node_id = params.get("node_id")
        if not skill_id or not node_id:
            raise ValueError("skill_id and node_id required")
        key = f"{SKILL_PREFIX}{skill_id}.json"
        raw = await self.store.load(key)
        if raw is None:
            return {"error": f"Skill '{skill_id}' not found"}
        skill = json.loads(raw)
        if skill.get("registered_node") != node_id:
            return {"error": "Not your skill to unregister"}
        skill["status"] = "unregistered"
        skill["updated_at"] = self._now()
        await self.store.save(key, json.dumps(skill))
        return {"skill_id": skill_id, "unregistered": True}

    # ─── 技能发现 ─────────────────────────────────────────────────────────────

    def _skill_key(self, skill_id: str) -> str:
        return f"{SKILL_PREFIX}{skill_id}.json"

    def _invoke_key(self, invoke_id: str) -> str:
        return f"{INVOKE_PREFIX}{invoke_id}.json"

    async def skill_list(self, params: dict) -> dict:
        """skill_list — 列出所有可用技能"""
        keys = await self.store.list_all(SKILL_PREFIX)
        active = []
        for k in keys:
            raw = await self.store.load(k)
            if raw:
                s = json.loads(raw)
                if s.get("status") == "active":
                    active.append(s)
        return {"skills": active, "total": len(active)}

    async def skill_get(self, params: dict) -> dict:
        """skill_get — 获取技能详情"""
        skill_id = params.get("skill_id")
        if not skill_id:
            raise ValueError("skill_id required")
        raw = await self.store.load(self._skill_key(skill_id))
        if raw is None:
            return {"error": f"Skill '{skill_id}' not found"}
        return {"skill": json.loads(raw)}

    async def skill_search(self, params: dict) -> dict:
        """skill_search — 按触发词/名称搜索技能"""
        query = params.get("query", "").lower()
        keys = await self.store.list_all(SKILL_PREFIX)
        matched = []
        for k in keys:
            raw = await self.store.load(k)
            if not raw:
                continue
            s = json.loads(raw)
            if s.get("status") != "active":
                continue
            if (query in s.get("name", "").lower()
                or query in s.get("description", "").lower()
                or any(query in w.lower() for w in s.get("trigger_words", []))):
                matched.append(s)
        return {"skills": matched, "total": len(matched)}

    async def skill_by_node(self, params: dict) -> dict:
        """skill_by_node — 查询某端侧节点注册的所有技能"""
        node_id = params.get("node_id")
        if not node_id:
            raise ValueError("node_id required")
        keys = await self.store.list_all(SKILL_PREFIX)
        node_skills = []
        for k in keys:
            raw = await self.store.load(k)
            if not raw:
                continue
            s = json.loads(raw)
            if s.get("registered_node") == node_id and s.get("status") == "active":
                node_skills.append(s)
        return {"skills": node_skills, "total": len(node_skills)}

    async def skill_update(self, params: dict) -> dict:
        """skill_update — 更新技能定义"""
        skill_id = params.get("skill_id")
        node_id = params.get("node_id")
        updates = params.get("updates", {})
        if not skill_id or not node_id:
            raise ValueError("skill_id and node_id required")
        raw = await self.store.load(self._skill_key(skill_id))
        if raw is None:
            return {"error": f"Skill '{skill_id}' not found"}
        skill = json.loads(raw)
        if skill.get("registered_node") != node_id:
            return {"error": "Not your skill to update"}
        for key in ("description", "version", "trigger_words", "input_schema", "output_schema"):
            if key in updates:
                skill[key] = updates[key]
        skill["updated_at"] = self._now()
        await self.store.save(self._skill_key(skill_id), json.dumps(skill))
        return {"skill": skill, "updated": True}

    # ─── 技能调度 ─────────────────────────────────────────────────────────────

    async def skill_invoke(self, params: dict) -> dict:
        """
        skill_invoke — 云端向指定端侧发起技能执行请求。
        调度信息由 hub 推送给端侧，本方法只写入 invocation 记录。
        """
        skill_id = params.get("skill_id")
        target_node = params.get("target_node")
        invoke_id = params.get("invoke_id") or str(uuid.uuid4())
        input_params = params.get("input", {})

        if not skill_id or not target_node:
            raise ValueError("skill_id and target_node required")

        skill_raw = await self.store.load(self._skill_key(skill_id))
        if skill_raw is None:
            return {"error": f"Skill '{skill_id}' not found"}
        skill = json.loads(skill_raw)
        if skill.get("status") != "active":
            return {"error": f"Skill '{skill_id}' not active"}

        invoke_record = {
            "invoke_id": invoke_id,
            "skill_id": skill_id,
            "skill_name": skill.get("name"),
            "target_node": target_node,
            "status": "dispatched",
            "input": input_params,
            "dispatched_at": self._now(),
            "result": None,
            "completed_at": None,
        }
        await self.store.save(self._invoke_key(invoke_id), json.dumps(invoke_record))
        logger.info(f"Skill invoked: {skill_id} → node {target_node}, invoke_id={invoke_id}")

        return {
            "invoke_id": invoke_id,
            "skill_id": skill_id,
            "skill_name": skill.get("name"),
            "target_node": target_node,
            "input": input_params,
            "dispatched": True,
        }

    async def skill_result(self, params: dict) -> dict:
        """skill_result — 端侧执行完成后回调云端"""
        invoke_id = params.get("invoke_id")
        node_id = params.get("node_id")
        success = params.get("success", True)
        output = params.get("output", {})
        error = params.get("error", "")

        if not invoke_id:
            raise ValueError("invoke_id required")

        raw = await self.store.load(self._invoke_key(invoke_id))
        if raw is None:
            return {"error": f"Invoke '{invoke_id}' not found"}
        invoke = json.loads(raw)
        if invoke.get("target_node") != node_id:
            return {"error": "Not your invoke"}

        invoke["status"] = "done" if success else "failed"
        invoke["result"] = output if success else {"error": error}
        invoke["completed_at"] = self._now()
        await self.store.save(self._invoke_key(invoke_id), json.dumps(invoke))
        return {"invoke_id": invoke_id, "recorded": True}

    async def skill_invoke_status(self, params: dict) -> dict:
        """skill_invoke_status — 查询技能调用状态"""
        invoke_id = params.get("invoke_id")
        if not invoke_id:
            raise ValueError("invoke_id required")
        raw = await self.store.load(self._invoke_key(invoke_id))
        if raw is None:
            return {"error": f"Invoke '{invoke_id}' not found"}
        return {"invoke": json.loads(raw)}

    def _now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
