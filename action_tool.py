"""SOLF-guarded action execution for project workflows.

Python is the execution layer. SOLF is the semantic guard and policy layer.
This module performs DB writes only after guard predicates are evaluated.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ActionResult:
    success: bool
    message: str
    data: dict[str, Any] | None = None
    warnings: list[str] | None = None
    requires_confirmation: bool = False
    confirmation_prompt: str | None = None


class ActionTool:
    """CRUD operations with SOLF guards and optional confirmation."""

    _ALLOWED_UPDATE_TABLES: set[str] = {
        "projects",
        "project_tasks",
        "project_milestones",
        "project_members",
        "invoices",
        "crm_tasks",
    }

    def __init__(self, db_connection_fn, solf_interpreter=None, require_confirmation: bool = False):
        self._db_connection_fn = db_connection_fn
        self.solf_interpreter = solf_interpreter
        self.require_confirmation = require_confirmation

    def _invoke_guard(self, predicate_name: str, payload: dict[str, Any], default_allow: bool = True) -> bool:
        if self.solf_interpreter is None:
            return default_allow
        try:
            result = self.solf_interpreter._invoke_clause(predicate_name, [payload])
        except Exception:
            return default_allow

        if isinstance(result, dict):
            mode = str(result.get("mode") or "allow").strip().lower()
            if mode == "deny":
                return False
            if mode in {"allow", "escalate"}:
                return True
            allowed = result.get("allowed")
            if isinstance(allowed, bool):
                return allowed
        if isinstance(result, bool):
            return result
        return default_allow

    def _confirm_if_needed(self, action: str, payload: dict[str, Any]) -> ActionResult | None:
        if not self.require_confirmation:
            return None
        return ActionResult(
            success=False,
            message=f"Confirmation required before {action}.",
            requires_confirmation=True,
            confirmation_prompt=f"Proceed with {action}? payload={payload}",
            data={"payload": payload},
        )

    def create_task(
        self,
        project_id: int,
        title: str,
        hours_estimate: float,
        assigned_to: int | None = None,
        description: str = "",
        priority: str = "medium",
        due_date: str | None = None,
    ) -> ActionResult:
        payload = {
            "project_id": project_id,
            "title": title,
            "hours_estimate": hours_estimate,
            "assigned_to": assigned_to,
            "priority": priority,
            "due_date": due_date,
        }
        confirm = self._confirm_if_needed("create_task", payload)
        if confirm is not None:
            return confirm

        allowed = self._invoke_guard("can_create_task", payload, default_allow=True)
        if not allowed:
            return ActionResult(success=False, message="SOLF guard rejected create_task.")

        sql = (
            "INSERT INTO project_tasks "
            "(project_id, assigned_to, name, description, status, priority, due_date, estimated_hours) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
        )
        try:
            with self._db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            int(project_id),
                            int(assigned_to) if assigned_to is not None else None,
                            str(title).strip(),
                            str(description or "").strip(),
                            "open",
                            str(priority or "medium").strip(),
                            due_date,
                            float(hours_estimate),
                        ),
                    )
                    task_id = cur.fetchone()[0]
            return ActionResult(
                success=True,
                message="Task created.",
                data={"task_id": task_id, "project_id": project_id},
            )
        except Exception as exc:
            return ActionResult(success=False, message=f"Failed to create task: {exc}")

    def assign_task(self, task_id: int, assigned_to: int) -> ActionResult:
        payload = {"task_id": task_id, "assigned_to": assigned_to}
        confirm = self._confirm_if_needed("assign_task", payload)
        if confirm is not None:
            return confirm

        allowed = self._invoke_guard("can_assign_task", payload, default_allow=True)
        if not allowed:
            return ActionResult(success=False, message="SOLF guard rejected assign_task.")

        try:
            with self._db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE project_tasks SET assigned_to=%s, updated_at=NOW() WHERE id=%s RETURNING id",
                        (int(assigned_to), int(task_id)),
                    )
                    row = cur.fetchone()
            if not row:
                return ActionResult(success=False, message="Task not found.")
            return ActionResult(success=True, message="Task assigned.", data={"task_id": int(task_id), "assigned_to": int(assigned_to)})
        except Exception as exc:
            return ActionResult(success=False, message=f"Failed to assign task: {exc}")

    def close_task(self, task_id: int) -> ActionResult:
        payload = {"task_id": task_id}
        confirm = self._confirm_if_needed("close_task", payload)
        if confirm is not None:
            return confirm

        allowed = self._invoke_guard("can_close_task", payload, default_allow=True)
        if not allowed:
            return ActionResult(success=False, message="SOLF guard rejected close_task.")

        try:
            with self._db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE project_tasks SET status=%s, updated_at=NOW() WHERE id=%s RETURNING id",
                        ("done", int(task_id)),
                    )
                    row = cur.fetchone()
            if not row:
                return ActionResult(success=False, message="Task not found.")
            return ActionResult(success=True, message="Task closed.", data={"task_id": int(task_id), "status": "done"})
        except Exception as exc:
            return ActionResult(success=False, message=f"Failed to close task: {exc}")

    def update_entity(self, entity_type: str, entity_id: int, field: str, new_value: Any) -> ActionResult:
        table = str(entity_type or "").strip().lower()
        column = str(field or "").strip().lower()

        if table not in self._ALLOWED_UPDATE_TABLES:
            return ActionResult(success=False, message=f"Unsupported entity_type: {table}")
        if not column.replace("_", "").isalnum() or column in {"id", "created_at"}:
            return ActionResult(success=False, message=f"Unsafe field: {column}")

        payload = {
            "entity_type": table,
            "entity_id": int(entity_id),
            "field": column,
            "new_value": new_value,
        }
        confirm = self._confirm_if_needed("update_entity", payload)
        if confirm is not None:
            return confirm

        allowed = self._invoke_guard("can_update_entity", payload, default_allow=True)
        if not allowed:
            return ActionResult(success=False, message="SOLF guard rejected update_entity.")

        select_sql = f"SELECT {column} FROM {table} WHERE id=%s"
        update_sql = f"UPDATE {table} SET {column}=%s, updated_at=NOW() WHERE id=%s RETURNING id"

        try:
            with self._db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (int(entity_id),))
                    old = cur.fetchone()
                    if old is None:
                        return ActionResult(success=False, message="Entity not found.")
                    old_value = old[0]
                    cur.execute(update_sql, (new_value, int(entity_id)))
                    row = cur.fetchone()
            if not row:
                return ActionResult(success=False, message="Update failed.")
            return ActionResult(
                success=True,
                message="Entity updated.",
                data={
                    "entity_type": table,
                    "entity_id": int(entity_id),
                    "field": column,
                    "old_value": old_value,
                    "new_value": new_value,
                },
            )
        except Exception as exc:
            return ActionResult(success=False, message=f"Failed to update entity: {exc}")

    def validate_plan(self, plan_id: int) -> ActionResult:
        payload = {"plan_id": int(plan_id)}
        allowed = self._invoke_guard("can_validate_plan", payload, default_allow=True)
        if not allowed:
            return ActionResult(success=False, message="SOLF guard rejected validate_plan.")

        warnings: list[str] = []
        data: dict[str, Any] = {"plan_id": int(plan_id)}

        try:
            with self._db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(budget_amount, 0) FROM projects WHERE id=%s",
                        (int(plan_id),),
                    )
                    row = cur.fetchone()
                    if row is None:
                        return ActionResult(success=False, message="Project not found.")
                    budget_amount = float(row[0] or 0)
                    data["budget_amount"] = budget_amount

                    cur.execute(
                        "SELECT COALESCE(SUM(COALESCE(cost_amount,0)),0) FROM project_time_entries WHERE project_id=%s",
                        (int(plan_id),),
                    )
                    spent = float((cur.fetchone() or [0])[0] or 0)
                    data["spent_amount"] = spent
                    if budget_amount > 0 and spent > budget_amount:
                        warnings.append("Project is over budget based on recorded time entries.")

                    cur.execute(
                        "SELECT id, name FROM project_tasks WHERE project_id=%s AND status NOT IN ('done','cancelled') AND due_date < CURRENT_DATE",
                        (int(plan_id),),
                    )
                    overdue_tasks = [{"task_id": int(r[0]), "name": r[1]} for r in (cur.fetchall() or [])]
                    data["overdue_tasks"] = overdue_tasks
                    if overdue_tasks:
                        warnings.append(f"Found {len(overdue_tasks)} overdue task(s).")

                    cur.execute(
                        "SELECT assigned_to, COALESCE(SUM(estimated_hours),0) FROM project_tasks WHERE project_id=%s AND assigned_to IS NOT NULL AND status NOT IN ('done','cancelled') GROUP BY assigned_to HAVING COALESCE(SUM(estimated_hours),0) > 40",
                        (int(plan_id),),
                    )
                    conflicts = [{"assigned_to": int(r[0]), "estimated_hours": float(r[1])} for r in (cur.fetchall() or [])]
                    data["resource_conflicts"] = conflicts
                    if conflicts:
                        warnings.append("Resource conflict: one or more assignees exceed 40 estimated hours.")

            return ActionResult(
                success=len(warnings) == 0,
                message="Plan validation completed." if not warnings else "Plan validation completed with warnings.",
                warnings=warnings,
                data=data,
            )
        except Exception as exc:
            return ActionResult(success=False, message=f"Failed to validate plan: {exc}")
