"""What-If Analysis & Scenario Simulation for Project Planning (Interaction Type 6)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import json


@dataclass
class ScenarioModification:
    """Represents a parameter modification for what-if analysis."""
    
    param_type: str  # budget_percent, timeline_days, resource_count, task_hours
    param_name: str  # name of parameter being modified
    original_value: float
    modified_value: float
    reason: str


@dataclass
class SchedulingResult:
    """Result of scheduling calculation for a scenario."""
    
    total_duration_days: int
    critical_path: list[str]  # Task IDs on critical path
    critical_path_duration: int
    resource_utilization_percent: float
    slack_per_task: dict[str, int]  # task_id -> slack days
    conflicts: list[str]  # Resource conflict descriptions
    warnings: list[str]


class ProjectSimulator:
    """Scenario simulation and what-if analysis engine for projects."""

    def __init__(self, db_connection_fn):
        self.db_connection_fn = db_connection_fn
        self.ensure_tables()

    def ensure_tables(self) -> None:
        """Create scenario and analysis tracking tables."""
        ddl = """
        CREATE TABLE IF NOT EXISTS project_scenarios (
            id BIGSERIAL PRIMARY KEY,
            base_project_id BIGINT NOT NULL,
            scenario_name TEXT NOT NULL,
            scenario_type TEXT NOT NULL,
            description TEXT,
            modifications JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NOT NULL DEFAULT 'draft',
            created_by_ref TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(base_project_id, scenario_name)
        );

        CREATE TABLE IF NOT EXISTS scenario_analysis (
            id BIGSERIAL PRIMARY KEY,
            scenario_id BIGINT NOT NULL REFERENCES project_scenarios(id),
            analysis_type TEXT NOT NULL,
            scheduling_result JSONB,
            budget_impact JSONB,
            resource_impact JSONB,
            risk_assessment JSONB,
            recommendations TEXT,
            analysis_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            analyzed_by_ref TEXT
        );

        CREATE TABLE IF NOT EXISTS scenario_comparison (
            id BIGSERIAL PRIMARY KEY,
            base_scenario_id BIGINT NOT NULL REFERENCES project_scenarios(id),
            comparison_scenario_id BIGINT NOT NULL REFERENCES project_scenarios(id),
            duration_delta_days INT,
            budget_delta_percent DECIMAL(5,2),
            resource_delta_count INT,
            risk_delta_level TEXT,
            recommendation TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(base_scenario_id, comparison_scenario_id)
        );

        CREATE INDEX IF NOT EXISTS idx_project_scenarios_base ON project_scenarios(base_project_id);
        CREATE INDEX IF NOT EXISTS idx_project_scenarios_status ON project_scenarios(status);
        CREATE INDEX IF NOT EXISTS idx_scenario_analysis_scenario ON scenario_analysis(scenario_id);
        CREATE INDEX IF NOT EXISTS idx_scenario_comparison_base ON scenario_comparison(base_scenario_id);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def create_scenario(
        self,
        base_project_id: int,
        scenario_name: str,
        scenario_type: str,
        description: str | None = None,
        modifications: dict[str, Any] | None = None,
        actor_ref: str = "system",
    ) -> dict[str, Any]:
        """Create a new what-if scenario based on existing project."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO project_scenarios (
                        base_project_id, scenario_name, scenario_type, description,
                        modifications, created_by_ref, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'draft')
                    RETURNING id, created_at
                    """,
                    (
                        base_project_id,
                        scenario_name,
                        scenario_type,
                        description,
                        json.dumps(modifications or {}),
                        actor_ref,
                    ),
                )
                row = cur.fetchone()

        return {
            "scenario_id": int(row[0]) if row else 0,
            "scenario_name": scenario_name,
            "base_project_id": base_project_id,
            "created_at": row[1].isoformat() if row else None,
        }

    def clone_project_scenario(
        self,
        base_project_id: int,
        scenario_name: str,
        modifications: list[ScenarioModification] | None = None,
    ) -> dict[str, Any]:
        """Clone a project into a named scenario with optional modifications."""
        # Retrieve base project data
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, project_code, project_type, budget_amount, 
                           start_date, end_date, status
                    FROM projects
                    WHERE id = %s
                    """,
                    (base_project_id,),
                )
                project_row = cur.fetchone()
                if not project_row:
                    return {"success": False, "error": f"Project {base_project_id} not found"}

                # Get all tasks for the project
                cur.execute(
                    """
                    SELECT id, task_name, planned_effort_hours, planned_start, planned_end,
                           parent_task_id
                    FROM project_tasks
                    WHERE project_ref = %s
                    ORDER BY id ASC
                    """,
                    (base_project_id,),
                )
                tasks = cur.fetchall() or []

        # Create scenario record
        modifications_dict = {}
        if modifications:
            for mod in modifications:
                modifications_dict[mod.param_name] = {
                    "original": mod.original_value,
                    "modified": mod.modified_value,
                    "reason": mod.reason,
                }

        scenario = self.create_scenario(
            base_project_id=base_project_id,
            scenario_name=scenario_name,
            scenario_type="what_if",
            description=f"What-if scenario: {scenario_name}",
            modifications=modifications_dict,
        )

        if not scenario.get("scenario_id"):
            return {"success": False, "error": "Failed to create scenario"}

        return {
            "success": True,
            "scenario_id": scenario["scenario_id"],
            "scenario_name": scenario_name,
            "base_project_id": base_project_id,
            "task_count": len(tasks),
            "modifications": modifications_dict,
        }

    def simulate_project_schedule(
        self, scenario_id: int, include_resource_check: bool = True
    ) -> dict[str, Any]:
        """Simulate project schedule for a scenario (critical path analysis)."""
        # Load scenario
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT base_project_id, modifications
                    FROM project_scenarios
                    WHERE id = %s
                    """,
                    (scenario_id,),
                )
                scenario_row = cur.fetchone()
                if not scenario_row:
                    return {"success": False, "error": f"Scenario {scenario_id} not found"}

                base_project_id = scenario_row[0]
                modifications = json.loads(scenario_row[1]) if scenario_row[1] else {}

                # Get project tasks
                cur.execute(
                    """
                    SELECT id, task_name, planned_effort_hours, planned_start, planned_end,
                           parent_task_id, assignee_ref
                    FROM project_tasks
                    WHERE project_ref = %s
                    ORDER BY id ASC
                    """,
                    (base_project_id,),
                )
                tasks = cur.fetchall() or []

        # Build task graph
        task_map: dict[int, dict[str, Any]] = {}
        for task in tasks:
            task_id, name, hours, start, end, parent_id, assignee = task
            adjusted_hours = hours
            
            # Apply hour modifications if specified
            if "task_hours" in modifications:
                factor = float(modifications["task_hours"].get("modified", 1.0)) / float(
                    modifications["task_hours"].get("original", 1.0)
                )
                adjusted_hours = int(hours * factor)

            task_map[int(task_id)] = {
                "id": int(task_id),
                "name": str(name),
                "hours": adjusted_hours,
                "days": max(1, adjusted_hours // 8),  # Assume 8-hour day
                "parent_id": parent_id,
                "assignee": assignee,
                "slack": 0,
            }

        # Simple critical path analysis: sequential task sum + parallel adjustments
        total_days = 0
        critical_path = []
        for task_id, task_info in task_map.items():
            if not task_info["parent_id"]:  # Top-level task
                total_days += task_info["days"]
                critical_path.append(str(task_id))

        # Check for conflicts
        conflicts = []
        if include_resource_check:
            resource_count = {}
            for task_id, task_info in task_map.items():
                assignee = task_info.get("assignee")
                if assignee:
                    resource_count[assignee] = resource_count.get(assignee, 0) + 1
                    if resource_count[assignee] > 1:
                        conflicts.append(f"{assignee} assigned to {resource_count[assignee]} tasks (potential overload)")

        result = SchedulingResult(
            total_duration_days=total_days,
            critical_path=critical_path,
            critical_path_duration=total_days,
            resource_utilization_percent=min(100, len([t for t in task_map.values() if t.get("assignee")]) / max(1, len(task_map)) * 100),
            slack_per_task={str(tid): 0 for tid in task_map.keys()},
            conflicts=conflicts,
            warnings=["Simple scheduling model used; actual CPM analysis recommended for complex projects"],
        )

        # Store analysis result
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scenario_analysis (
                        scenario_id, analysis_type, scheduling_result
                    )
                    VALUES (%s, 'schedule_simulation', %s)
                    """,
                    (
                        scenario_id,
                        json.dumps({
                            "total_duration_days": result.total_duration_days,
                            "critical_path": result.critical_path,
                            "conflicts_count": len(result.conflicts),
                            "resource_utilization": result.resource_utilization_percent,
                        }),
                    ),
                )

        return {
            "success": True,
            "scenario_id": scenario_id,
            "total_duration_days": result.total_duration_days,
            "critical_path": result.critical_path,
            "resource_utilization_percent": result.resource_utilization_percent,
            "conflicts": result.conflicts,
            "warnings": result.warnings,
        }

    def calculate_budget_impact(self, scenario_id: int) -> dict[str, Any]:
        """Calculate budget impact for a scenario."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ps.base_project_id, ps.modifications, p.budget_amount
                    FROM project_scenarios ps
                    JOIN projects p ON p.id = ps.base_project_id
                    WHERE ps.id = %s
                    """,
                    (scenario_id,),
                )
                row = cur.fetchone()
                if not row:
                    return {"success": False, "error": f"Scenario {scenario_id} not found"}

                base_project_id, modifications_json, base_budget = row
                modifications = json.loads(modifications_json) if modifications_json else {}

        # Calculate budget adjustments
        budget_multiplier = 1.0
        adjustments = []

        # Budget percent modification
        if "budget_percent" in modifications:
            factor = float(modifications["budget_percent"].get("modified", 100)) / 100
            budget_multiplier *= factor
            adjustments.append(f"Budget adjusted to {int(factor * 100)}%")

        # Timeline modification impact (longer timeline = higher costs due to resource allocation)
        if "timeline_days" in modifications:
            original_days = float(modifications["timeline_days"].get("original", 1))
            modified_days = float(modifications["timeline_days"].get("modified", 1))
            if modified_days > original_days:
                time_impact = 1.0 + (0.1 * (modified_days - original_days) / original_days)
                budget_multiplier *= time_impact
                adjustments.append(f"Extended timeline increases costs by {int((time_impact - 1) * 100)}%")

        # Resource count modification
        if "resource_count" in modifications:
            original_count = int(modifications["resource_count"].get("original", 1))
            modified_count = int(modifications["resource_count"].get("modified", 1))
            if modified_count > original_count:
                resource_impact = modified_count / original_count
                budget_multiplier *= resource_impact
                adjustments.append(f"Added {modified_count - original_count} resources, {int((resource_impact - 1) * 100)}% budget increase")

        new_budget = int(base_budget * budget_multiplier)
        budget_delta = int(new_budget - base_budget)
        budget_delta_percent = round(((new_budget / base_budget) - 1) * 100, 2) if base_budget else 0

        return {
            "success": True,
            "scenario_id": scenario_id,
            "base_budget": int(base_budget),
            "new_budget": new_budget,
            "budget_delta": budget_delta,
            "budget_delta_percent": budget_delta_percent,
            "adjustments": adjustments,
        }

    def compare_scenarios(
        self, base_scenario_id: int, comparison_scenario_id: int
    ) -> dict[str, Any]:
        """Compare two scenarios to show impact of modifications."""
        # Get base scenario analysis
        base_schedule = self.simulate_project_schedule(base_scenario_id, include_resource_check=True)
        base_budget = self.calculate_budget_impact(base_scenario_id)

        # Get comparison scenario analysis
        comp_schedule = self.simulate_project_schedule(comparison_scenario_id, include_resource_check=True)
        comp_budget = self.calculate_budget_impact(comparison_scenario_id)

        # Calculate deltas
        duration_delta = (
            comp_schedule.get("total_duration_days", 0)
            - base_schedule.get("total_duration_days", 0)
        )
        budget_delta_percent = (
            comp_budget.get("budget_delta_percent", 0)
            - (base_budget.get("budget_delta_percent", 0) or 0)
        )

        # Generate recommendation
        recommendation = ""
        if duration_delta < 0:
            recommendation += f"Scenario reduces timeline by {-duration_delta} days. "
        elif duration_delta > 0:
            recommendation += f"Scenario extends timeline by {duration_delta} days. "

        if budget_delta_percent < 0:
            recommendation += f"Budget savings of {-budget_delta_percent}%. "
        elif budget_delta_percent > 0:
            recommendation += f"Budget increase of {budget_delta_percent}%. "

        if not comp_schedule.get("conflicts"):
            recommendation += "No resource conflicts detected in comparison scenario."

        # Store comparison
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scenario_comparison (
                        base_scenario_id, comparison_scenario_id,
                        duration_delta_days, budget_delta_percent, recommendation
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (base_scenario_id, comparison_scenario_id)
                    DO UPDATE SET recommendation = EXCLUDED.recommendation
                    """,
                    (
                        base_scenario_id,
                        comparison_scenario_id,
                        duration_delta,
                        budget_delta_percent,
                        recommendation,
                    ),
                )

        return {
            "success": True,
            "base_scenario_id": base_scenario_id,
            "comparison_scenario_id": comparison_scenario_id,
            "base_duration_days": base_schedule.get("total_duration_days", 0),
            "comparison_duration_days": comp_schedule.get("total_duration_days", 0),
            "duration_delta_days": duration_delta,
            "base_budget": base_budget.get("new_budget", 0),
            "comparison_budget": comp_budget.get("new_budget", 0),
            "budget_delta_percent": round(budget_delta_percent, 2),
            "base_conflicts": len(base_schedule.get("conflicts", [])),
            "comparison_conflicts": len(comp_schedule.get("conflicts", [])),
            "recommendation": recommendation,
        }

    def list_scenarios(self, base_project_id: int | None = None, status: str = "draft") -> dict[str, Any]:
        """List scenarios for a project."""
        filters = ["status = %s"]
        params: list[Any] = [status]

        if base_project_id:
            filters.append("base_project_id = %s")
            params.append(base_project_id)

        where_clause = " AND ".join(filters)
        query = f"""
            SELECT id, base_project_id, scenario_name, scenario_type,
                   description, modifications, created_at, created_by_ref
            FROM project_scenarios
            WHERE {where_clause}
            ORDER BY created_at DESC
        """

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall() or []

        scenarios = [
            {
                "scenario_id": int(row[0]),
                "base_project_id": int(row[1]),
                "scenario_name": str(row[2]),
                "scenario_type": str(row[3]),
                "description": row[4],
                "modifications": json.loads(row[5]) if row[5] else {},
                "created_at": row[6].isoformat() if row[6] else None,
                "created_by": str(row[7]),
            }
            for row in rows
        ]

        return {
            "success": True,
            "scenarios": scenarios,
            "count": len(scenarios),
        }

    def get_scenario_details(self, scenario_id: int) -> dict[str, Any]:
        """Get full details and analysis for a scenario."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, base_project_id, scenario_name, description, modifications,
                           created_at, created_by_ref
                    FROM project_scenarios
                    WHERE id = %s
                    """,
                    (scenario_id,),
                )
                scenario_row = cur.fetchone()
                if not scenario_row:
                    return {"success": False, "error": f"Scenario {scenario_id} not found"}

                # Get latest analysis
                cur.execute(
                    """
                    SELECT analysis_type, scheduling_result, budget_impact, risk_assessment
                    FROM scenario_analysis
                    WHERE scenario_id = %s
                    ORDER BY analysis_date DESC
                    LIMIT 1
                    """,
                    (scenario_id,),
                )
                analysis_row = cur.fetchone()

        return {
            "success": True,
            "scenario_id": int(scenario_row[0]),
            "base_project_id": int(scenario_row[1]),
            "scenario_name": str(scenario_row[2]),
            "description": scenario_row[3],
            "modifications": json.loads(scenario_row[4]) if scenario_row[4] else {},
            "created_at": scenario_row[5].isoformat() if scenario_row[5] else None,
            "created_by": str(scenario_row[6]),
            "latest_analysis": {
                "type": analysis_row[0],
                "schedule": json.loads(analysis_row[1]) if analysis_row[1] else None,
                "budget": json.loads(analysis_row[2]) if analysis_row[2] else None,
                "risk": json.loads(analysis_row[3]) if analysis_row[3] else None,
            }
            if analysis_row
            else None,
        }
