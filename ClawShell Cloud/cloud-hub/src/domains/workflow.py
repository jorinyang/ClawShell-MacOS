"""
ClawShell Cloud Hub — Workflow Domain
多步任务编排引擎：支持顺序执行、并行执行、条件分支、结果传递。
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable

from ..event_store.schema import Event, Topic
from ..pubsub.manager import PubSubManager

logger = logging.getLogger("workflow")

# ─── Workflow Definition ────────────────────────────────────────────────────

class StepType:
    TASK = "task"           # 创建 kanban 任务
    SKILL = "skill"         # 调用 skill
    MEMORY = "memory"       # 记忆查询
    WAIT = "wait"           # 等待人工确认
    PARALLEL = "parallel"   # 并行执行多个子步骤
    CONDITION = "condition" # 条件分支
    NOTIFY = "notify"       # 通知
    SAGA = "saga"          # SAGA 事务步骤（带补偿的原子事务）


class Step:
    """工作流单步定义"""
    def __init__(
        self,
        step_id: str,
        step_type: str,
        params: Dict[str, Any],
        next_on_success: Optional[str] = None,
        next_on_failure: Optional[str] = None,
        retry: int = 0,
        timeout: int = 300,
        compensation: Optional[Dict[str, Any]] = None,
    ):
        self.step_id = step_id
        self.step_type = step_type
        self.params = params
        self.next_on_success = next_on_success  # 成功时跳转 step_id
        self.next_on_failure = next_on_failure  # 失败时跳转 step_id
        self.retry = retry
        self.timeout = timeout
        self.compensation = compensation  # 补偿动作：{step_type, params}


class Workflow:
    """工作流定义"""
    def __init__(
        self,
        workflow_id: str,
        title: str,
        description: str = "",
        steps: Optional[List[Step]] = None,
        created_by: str = "cloud-hub",
    ):
        self.workflow_id = workflow_id
        self.title = title
        self.description = description
        self.steps: Dict[str, Step] = {s.step_id: s for s in (steps or [])}
        self.created_by = created_by

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "title": self.title,
            "description": self.description,
            "steps": [
                {
                    "step_id": s.step_id,
                    "step_type": s.step_type,
                    "params": s.params,
                    "next_on_success": s.next_on_success,
                    "next_on_failure": s.next_on_failure,
                    "retry": s.retry,
                    "timeout": s.timeout,
                    "compensation": s.compensation,
                }
                for s in self.steps.values()
            ],
            "created_by": self.created_by,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Workflow":
        steps = [
            Step(
                step_id=sd["step_id"],
                step_type=sd["step_type"],
                params=sd["params"],
                next_on_success=sd.get("next_on_success"),
                next_on_failure=sd.get("next_on_failure"),
                retry=sd.get("retry", 0),
                timeout=sd.get("timeout", 300),
                compensation=sd.get("compensation"),
            )
            for sd in data.get("steps", [])
        ]
        return cls(
            workflow_id=data["workflow_id"],
            title=data["title"],
            description=data.get("description", ""),
            steps=steps,
            created_by=data.get("created_by", "cloud-hub"),
        )


# ─── Workflow Execution ──────────────────────────────────────────────────────

class ExecutionStatus:
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    WAITING = "waiting"    # 等待人工确认
    CANCELLED = "cancelled"


class Execution:
    """工作流执行实例"""
    def __init__(
        self,
        execution_id: str,
        workflow_id: str,
        trigger_params: Dict[str, Any],
        current_step_id: Optional[str] = None,
        status: str = ExecutionStatus.PENDING,
        step_results: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        created_by: str = "cloud-hub",
    ):
        self.execution_id = execution_id
        self.workflow_id = workflow_id
        self.trigger_params = trigger_params
        self.current_step_id = current_step_id
        self.status = status
        self.step_results: Dict[str, Any] = step_results or {}
        self.error = error
        self.created_by = created_by

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "workflow_id": self.workflow_id,
            "trigger_params": self.trigger_params,
            "current_step_id": self.current_step_id,
            "status": self.status,
            "step_results": self.step_results,
            "error": self.error,
            "created_by": self.created_by,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Execution":
        return cls(
            execution_id=data["execution_id"],
            workflow_id=data["workflow_id"],
            trigger_params=data.get("trigger_params", {}),
            current_step_id=data.get("current_step_id"),
            status=data.get("status", ExecutionStatus.PENDING),
            step_results=data.get("step_results", {}),
            error=data.get("error"),
            created_by=data.get("created_by", "cloud-hub"),
        )


# ─── Workflow Domain ─────────────────────────────────────────────────────────

class WorkflowDomain:
    """
    工作流域：管理工作流定义和执行实例。
    
    支持的工作流类型：
    - task:    创建 kanban 任务
    - skill:   调用 skill
    - memory:  查询记忆/知识库
    - wait:    等待人工确认
    - parallel: 并行执行多个子步骤
    - condition: 条件分支
    - notify:  通知
    """

    def __init__(self, store, pubsub: Optional[PubSubManager] = None):
        self.store = store
        self.pubsub = pubsub
        self._running_executions: Dict[str, asyncio.Task] = {}

    async def _load_workflow(self, workflow_id: str) -> Optional[Workflow]:
        raw = await self.store.load(f"workflows/{workflow_id}/def.json")
        if not raw:
            return None
        import json
        return Workflow.from_dict(json.loads(raw))

    async def _save_workflow(self, wf: Workflow) -> None:
        import json
        await self.store.save(
            f"workflows/{wf.workflow_id}/def.json",
            json.dumps(wf.to_dict())
        )

    async def _load_execution(self, execution_id: str) -> Optional[Execution]:
        raw = await self.store.load(f"workflows/executions/{execution_id}.json")
        if not raw:
            return None
        import json
        return Execution.from_dict(json.loads(raw))

    async def _save_execution(self, exec: Execution) -> None:
        import json
        await self.store.save(
            f"workflows/executions/{exec.execution_id}.json",
            json.dumps(exec.to_dict())
        )

    # ─── Public API ────────────────────────────────────────────────────────

    async def workflow_define(self, params: dict) -> dict:
        """workflow_define: 注册新的工作流定义"""
        wf_data = params.get("workflow")
        # 支持平铺格式（测试用）
        if not wf_data:
            wf_data = {
                "workflow_id": params.get("workflow_id"),
                "title": params.get("title", "Untitled"),
                "version": params.get("version", "1.0.0"),
                "description": params.get("description", ""),
                "steps": params.get("steps", []),
            }
        if not wf_data or not wf_data.get("workflow_id"):
            return {"success": False, "error": "workflow.workflow_id is required"}
        wf = Workflow.from_dict(wf_data)
        await self._save_workflow(wf)
        logger.info(f"Workflow defined: {wf.workflow_id} [{wf.title}]")
        return {"success": True, "workflow": wf.to_dict()}

    async def workflow_get(self, params: dict) -> dict:
        """workflow_get: 获取工作流定义"""
        wf_id = params.get("workflow_id")
        wf = await self._load_workflow(wf_id)
        if not wf:
            return {"success": False, "error": f"workflow {wf_id} not found"}
        return {"success": True, "workflow": wf.to_dict()}

    async def workflow_execute(self, params: dict) -> dict:
        """workflow_execute: 触发工作流执行"""
        wf_id = params.get("workflow_id")
        trigger_params = params.get("params", {})
        execution_id = params.get("execution_id") or f"exec-{wf_id}-{self._uid8()}"
        
        wf = await self._load_workflow(wf_id)
        if not wf:
            return {"success": False, "error": f"workflow {wf_id} not found"}
        
        exec_instance = Execution(
            execution_id=execution_id,
            workflow_id=wf_id,
            trigger_params=trigger_params,
            current_step_id=None,
            status=ExecutionStatus.PENDING,
        )
        await self._save_execution(exec_instance)
        
        # 异步执行（不阻塞调用方）
        task = asyncio.create_task(self._run_execution(exec_instance, wf))
        self._running_executions[execution_id] = task
        
        logger.info(f"Workflow execution started: {execution_id}")
        return {
            "success": True,
            "execution_id": execution_id,
            "status": ExecutionStatus.PENDING,
        }

    async def workflow_status(self, params: dict) -> dict:
        """workflow_status: 查询执行状态"""
        exec_id = params.get("execution_id")
        running = self._running_executions.get(exec_id)
        if running:
            # asyncio.Task — 任务还在运行中，未保存到 store
            return {
                "success": True,
                "execution": {
                    "execution_id": exec_id,
                    "status": "running",
                }
            }
        exec_instance = await self._load_execution(exec_id)
        if not exec_instance:
            return {"success": False, "error": f"execution {exec_id} not found"}
        return {"success": True, "execution": exec_instance.to_dict()}

    async def workflow_cancel(self, params: dict) -> dict:
        """workflow_cancel: 取消执行"""
        exec_id = params.get("execution_id")
        # 先查内存（新建的 execution 尚未写入 store）
        running = self._running_executions.get(exec_id)
        if running:
            running.cancel()
            return {"success": True, "status": ExecutionStatus.CANCELLED}
        exec_instance = await self._load_execution(exec_id)
        if not exec_instance:
            return {"success": False, "error": f"execution {exec_id} not found"}
        if exec_instance.status in (ExecutionStatus.SUCCESS, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED):
            return {"success": False, "error": f"execution already {exec_instance.status}"}
        exec_instance.status = ExecutionStatus.CANCELLED
        await self._save_execution(exec_instance)
        if exec_id in self._running_executions:
            self._running_executions[exec_id].cancel()
        return {"success": True, "status": ExecutionStatus.CANCELLED}

    async def workflow_list(self, params: dict) -> dict:
        """workflow_list: 列出所有工作流定义"""
        try:
            keys = await self.store.list_all("workflows/")
            wfs = []
            for k in keys:
                if k.endswith("/def.json"):
                    raw = await self.store.load(k)
                    if raw:
                        import json
                        wfs.append(json.loads(raw))
            return {"success": True, "workflows": wfs}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def workflow_wait(self, params: dict) -> dict:
        """workflow_wait: 等待人工确认（阻塞直到 confirmed/rejected）"""
        exec_id = params.get("execution_id")
        exec_instance = await self._load_execution(exec_id)
        if not exec_instance:
            return {"success": False, "error": f"execution {exec_id} not found"}
        
        # 状态改为 waiting，保存
        exec_instance.status = ExecutionStatus.WAITING
        await self._save_execution(exec_instance)
        
        # 广播等待事件
        await self._emit("workflow.waiting", exec_instance.workflow_id, {
            "execution_id": exec_id,
            "workflow_id": exec_instance.workflow_id,
        })
        
        return {"success": True, "status": ExecutionStatus.WAITING}

    async def workflow_confirm(self, params: dict) -> dict:
        """workflow_confirm: 确认继续执行（由人工或外部调用）"""
        exec_id = params.get("execution_id")
        confirmed = params.get("confirmed", True)
        exec_instance = await self._load_execution(exec_id)
        if not exec_instance:
            return {"success": False, "error": f"execution {exec_id} not found"}
        if exec_instance.status != ExecutionStatus.WAITING:
            return {"success": False, "error": f"execution not waiting, status={exec_instance.status}"}
        
        # 恢复执行
        exec_instance.status = ExecutionStatus.RUNNING
        await self._save_execution(exec_instance)
        
        # 触发后续执行（resume）
        task = asyncio.create_task(self._resume_execution(exec_instance))
        self._running_executions[exec_id] = task
        
        return {"success": True, "status": ExecutionStatus.RUNNING}

    # ─── Execution Engine ─────────────────────────────────────────────────

    async def _run_execution(self, exec_instance: Execution, wf: Workflow) -> None:
        """执行工作流主循环"""
        try:
            exec_instance.status = ExecutionStatus.RUNNING
            await self._save_execution(exec_instance)

            # 找到第一个步骤
            if not wf.steps:
                exec_instance.status = ExecutionStatus.FAILED
                exec_instance.error = "workflow has no steps"
                await self._save_execution(exec_instance)
                return

            # 从第一个步骤开始
            step_ids = list(wf.steps.keys())
            current_idx = 0
            while current_idx < len(step_ids):
                step_id = step_ids[current_idx]
                step = wf.steps[step_id]
                
                exec_instance.current_step_id = step_id
                await self._save_execution(exec_instance)

                # 执行步骤
                success, result = await self._execute_step(step, exec_instance)
                
                # 记录结果
                exec_instance.step_results[step_id] = result

                if success:
                    if step.next_on_success:
                        # 跳转到指定步骤
                        if step.next_on_success not in wf.steps:
                            exec_instance.status = ExecutionStatus.FAILED
                            exec_instance.error = f"next_on_success step {step.next_on_success} not found"
                            break
                        # 找到目标步骤索引
                        try:
                            current_idx = step_ids.index(step.next_on_success)
                        except ValueError:
                            exec_instance.status = ExecutionStatus.FAILED
                            exec_instance.error = f"step {step.next_on_success} not found"
                            break
                    else:
                        current_idx += 1
                else:
                    if step.next_on_failure:
                        # 跳转到失败处理步骤
                        if step.next_on_failure not in wf.steps:
                            exec_instance.status = ExecutionStatus.FAILED
                            exec_instance.error = f"next_on_failure step {step.next_on_failure} not found"
                            break
                        try:
                            current_idx = step_ids.index(step.next_on_failure)
                        except ValueError:
                            exec_instance.status = ExecutionStatus.FAILED
                            exec_instance.error = f"step {step.next_on_failure} not found"
                            break
                    else:
                        # 无失败处理，默认失败 → 先执行 SAGA 补偿
                        await self._compensate_saga(exec_instance)
                        exec_instance.status = ExecutionStatus.FAILED
                        exec_instance.error = str(result)
                        break

                # 检查是否被取消
                updated = await self._load_execution(exec_instance.execution_id)
                if updated and updated.status == ExecutionStatus.CANCELLED:
                    exec_instance.status = ExecutionStatus.CANCELLED
                    break

            else:
                # 所有步骤执行完毕
                if exec_instance.status == ExecutionStatus.RUNNING:
                    exec_instance.status = ExecutionStatus.SUCCESS

            await self._save_execution(exec_instance)
            await self._emit_workflow_done(exec_instance)

        except asyncio.CancelledError:
            exec_instance.status = ExecutionStatus.CANCELLED
            await self._save_execution(exec_instance)
            await self._emit_workflow_done(exec_instance)
        except Exception as e:
            logger.exception(f"Workflow execution {exec_instance.execution_id} error")
            exec_instance.status = ExecutionStatus.FAILED
            exec_instance.error = str(e)
            await self._save_execution(exec_instance)
            await self._emit_workflow_done(exec_instance)
        finally:
            self._running_executions.pop(exec_instance.execution_id, None)

    async def _resume_execution(self, exec_instance: Execution) -> None:
        """从等待状态恢复执行"""
        wf = await self._load_workflow(exec_instance.workflow_id)
        if not wf:
            exec_instance.status = ExecutionStatus.FAILED
            exec_instance.error = "workflow not found"
            await self._save_execution(exec_instance)
            return
        await self._run_execution(exec_instance, wf)

    async def _execute_step(self, step: Step, exec_instance: Execution) -> tuple[bool, Any]:
        """执行单个步骤"""
        ctx = {**exec_instance.trigger_params, **exec_instance.step_results}
        step_type = step.step_type

        for attempt in range(step.retry + 1):
            try:
                if step_type == StepType.TASK:
                    return await self._step_task(step, ctx)
                elif step_type == StepType.SKILL:
                    return await self._step_skill(step, ctx)
                elif step_type == StepType.MEMORY:
                    return await self._step_memory(step, ctx)
                elif step_type == StepType.WAIT:
                    return await self._step_wait(step, exec_instance)
                elif step_type == StepType.PARALLEL:
                    return await self._step_parallel(step, ctx)
                elif step_type == StepType.CONDITION:
                    return await self._step_condition(step, ctx)
                elif step_type == StepType.NOTIFY:
                    return await self._step_notify(step, ctx)
                elif step_type == StepType.SAGA:
                    return await self._step_saga(step, exec_instance)
                else:
                    return False, f"unknown step type: {step_type}"
            except Exception as e:
                if attempt == step.retry:
                    return False, str(e)
                await asyncio.sleep(1 * (attempt + 1))

        return False, f"failed after {step.retry + 1} attempts"

    async def _step_task(self, step: Step, ctx: Dict) -> tuple[bool, Any]:
        """创建 kanban 任务"""
        params = self._resolve_params(step.params, ctx)
        title = params.get("title", "")
        board_id = params.get("board_id", "default")
        dispatch_mode = params.get("dispatch_mode", "open_claim")
        
        # 调用 kanban domain（通过 store 或 RPC，这里简化直接写 OSS）
        import json
        task_id = f"task-{self._uid8()}"
        task = {
            "task_id": task_id,
            "title": title,
            "board_id": board_id,
            "status": "pending",
            "dispatch_mode": dispatch_mode,
            "workflow_execution_id": ctx.get("_execution_id"),
        }
        await self.store.save(f"kanban/tasks/{task_id}.json", json.dumps(task))
        
        # 广播任务创建
        await self._emit("task.created", "cloud-hub", {
            "task_id": task_id,
            "title": title,
            "dispatch_mode": dispatch_mode,
            "workflow_execution_id": ctx.get("_execution_id"),
        })
        
        return True, {"task_id": task_id, "title": title}

    async def _step_skill(self, step: Step, ctx: Dict) -> tuple[bool, Any]:
        """调用 skill"""
        params = self._resolve_params(step.params, ctx)
        skill_id = params.get("skill_id")
        skill_params = params.get("params", {})
        
        # Skill 结果通过 skill_invoke API 获取，这里简化
        # 实际执行由 edge 端处理
        invoke_id = f"invoke-{self._uid8()}"
        
        await self._emit("skill.invoked", "cloud-hub", {
            "invoke_id": invoke_id,
            "skill_id": skill_id,
            "params": skill_params,
            "workflow_execution_id": ctx.get("_execution_id"),
        })
        
        return True, {"invoke_id": invoke_id, "skill_id": skill_id}

    async def _step_memory(self, step: Step, ctx: Dict) -> tuple[bool, Any]:
        """查询记忆/知识库"""
        params = self._resolve_params(step.params, ctx)
        query = params.get("query", "")
        limit = params.get("limit", 5)
        
        # 简化为知识库搜索
        # 实际由 memory domain 处理
        await self._emit("knowledge.searched", "cloud-hub", {
            "query": query,
            "limit": limit,
            "workflow_execution_id": ctx.get("_execution_id"),
        })
        
        return True, {"query": query, "results": []}

    async def _step_wait(self, step: Step, exec_instance: Execution) -> tuple[bool, Any]:
        """等待人工确认（暂停执行）"""
        # 将执行状态改为 waiting
        exec_instance.status = ExecutionStatus.WAITING
        await self._save_execution(exec_instance)
        
        await self._emit("workflow.waiting", exec_instance.workflow_id, {
            "execution_id": exec_instance.execution_id,
            "workflow_id": exec_instance.workflow_id,
            "current_step_id": step.step_id,
        })
        
        # 这里需要阻塞等待外部唤醒
        # 使用 asyncio.Event 实现
        evt = asyncio.Event()
        # 将事件存入全局 registry，等 workflow_confirm 唤醒
        _wait_registry[exec_instance.execution_id] = evt
        
        try:
            await asyncio.wait_for(evt.wait(), timeout=step.timeout)
            # 被唤醒，状态已是 RUNNING
            return True, {"confirmed": True}
        except asyncio.TimeoutError:
            return False, "wait timeout"

    async def _step_parallel(self, step: Step, ctx: Dict) -> tuple[bool, Any]:
        """并行执行多个子步骤"""
        sub_steps = step.params.get("steps", [])
        tasks = []
        for sub in sub_steps:
            s = Step(
                step_id=sub.get("step_id", f"sub-{self._uid8()}"),
                step_type=sub.get("step_type"),
                params=sub.get("params", {}),
            )
            tasks.append(self._execute_step(s, Execution(
                execution_id=ctx.get("_execution_id", "parallel"),
                workflow_id=ctx.get("_workflow_id", "parallel"),
                trigger_params=ctx,
            )))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_success = all(not isinstance(r, Exception) and r[0] for r in results)
        return all_success, results

    async def _step_condition(self, step: Step, ctx: Dict) -> tuple[bool, Any]:
        """条件分支"""
        params = self._resolve_params(step.params, ctx)
        condition_expr = params.get("if")
        # 简单表达式求值（支持 ctx 变量）
        try:
            result = eval(condition_expr, {"ctx": ctx})
            return True, {"condition": condition_expr, "result": result}
        except Exception as e:
            return False, f"condition eval error: {e}"

    async def _step_notify(self, step: Step, ctx: Dict) -> tuple[bool, Any]:
        """发送通知"""
        params = self._resolve_params(step.params, ctx)
        message = params.get("message", "")
        to = params.get("to", "all")
        
        await self._emit("workflow.notification", exec_id := ctx.get("_execution_id"), {
            "message": message,
            "to": to,
            "execution_id": exec_id,
        })
        
        return True, {"notified": True, "message": message}

    async def _step_saga(self, step: Step, exec_instance: Execution) -> tuple[bool, Any]:
        """
        SAGA 事务步骤：执行正向动作，失败时自动补偿已完成的 saga 步骤。
        compensation 字段格式：{step_type, params}
        """
        # 从 params 中提取正向动作
        params = self._resolve_params(step.params, exec_instance.step_results)
        forward_type = params.get("forward_type", StepType.TASK)
        forward_params = params.get("forward_params", {})

        # 构建临时 Step 执行正向动作
        forward_step = Step(
            step_id=f"{step.step_id}-forward",
            step_type=forward_type,
            params=forward_params,
        )

        success, result = await self._execute_step(forward_step, exec_instance)

        if success:
            # 记录完成的 saga 步骤，供后续补偿使用
            saga_log = exec_instance.step_results.get("_saga_log", [])
            saga_log.append({
                "step_id": step.step_id,
                "forward_type": forward_type,
                "forward_params": forward_params,
                "result": result,
                "compensation": step.compensation,
            })
            exec_instance.step_results["_saga_log"] = saga_log
            return True, result
        else:
            # 正向失败，执行补偿（反向执行已完成的 saga 步骤）
            if step.compensation:
                comp = step.compensation
                comp_type = comp.get("step_type", StepType.TASK)
                comp_params = comp.get("params", {})
                comp_step = Step(
                    step_id=f"{step.step_id}-compensate",
                    step_type=comp_type,
                    params=comp_params,
                )
                comp_success, comp_result = await self._execute_step(
                    comp_step, exec_instance
                )
                return False, {
                    "forward_failed": result,
                    "compensation_attempted": True,
                    "compensation_success": comp_success,
                    "compensation_result": comp_result,
                }
            return False, {
                "forward_failed": result,
                "compensation_attempted": False,
            }

    async def _compensate_saga(self, exec_instance: Execution) -> None:
        """
        当 saga 事务中的非 saga 步骤失败时，补偿所有已完成的 saga 步骤（反向顺序）。
        """
        saga_log = exec_instance.step_results.get("_saga_log", [])
        if not saga_log:
            return

        logger.info(f"SAGA compensation for execution {exec_instance.execution_id}, "
                    f"rolling back {len(saga_log)} steps")

        for saga_entry in reversed(saga_log):
            comp = saga_entry.get("compensation")
            if not comp:
                continue

            comp_type = comp.get("step_type", StepType.TASK)
            comp_params = comp.get("params", {})

            comp_step = Step(
                step_id=f"compensate-{saga_entry['step_id']}",
                step_type=comp_type,
                params=comp_params,
            )

            try:
                _, comp_result = await self._execute_step(comp_step, exec_instance)
                logger.info(f"Compensated {saga_entry['step_id']}: {comp_result}")
            except Exception as e:
                logger.error(f"Compensation failed for {saga_entry['step_id']}: {e}")
                # 补偿失败记入执行结果
                compensations = exec_instance.step_results.get("_compensation_results", [])
                compensations.append({
                    "step_id": saga_entry["step_id"],
                    "status": "failed",
                    "error": str(e),
                })
                exec_instance.step_results["_compensation_results"] = compensations

    def _resolve_params(self, params: Dict, ctx: Dict) -> Dict:
        """将参数中的 {{variable}} 替换为 ctx 中的值"""
        import re
        def replace(m):
            key = m.group(1)
            return str(ctx.get(key, m.group(0)))
        resolved = {}
        for k, v in params.items():
            if isinstance(v, str):
                resolved[k] = re.sub(r"\{\{(\w+)\}\}", replace, v)
            else:
                resolved[k] = v
        return resolved

    async def _emit(self, topic: str, source: str, payload: Dict) -> None:
        """发布事件"""
        ev = Event.make(topic, source, payload)
        if self.pubsub:
            await self.pubsub.publish(ev)

    async def _emit_workflow_done(self, exec_instance: Execution) -> None:
        """工作流执行结束，发布事件"""
        # 保存最终状态到 store（cancel 时可查到）
        await self._save_execution(exec_instance)
        topic = "workflow.success" if exec_instance.status == ExecutionStatus.SUCCESS else "workflow.failed"
        await self._emit(topic, exec_instance.workflow_id, {
            "execution_id": exec_instance.execution_id,
            "workflow_id": exec_instance.workflow_id,
            "status": exec_instance.status,
            "step_results": exec_instance.step_results,
            "error": exec_instance.error,
        })

    @staticmethod
    def _uid8() -> str:
        import uuid
        return uuid.uuid4().hex[:8]


# 全局等待注册表（workflow_confirm 唤醒用）
_wait_registry: Dict[str, asyncio.Event] = {}
