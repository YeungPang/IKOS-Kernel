"""Task scheduler for recurring autonomous queries.

Uses APScheduler and persists job metadata/results in PostgreSQL.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


@dataclass
class ScheduledJob:
    job_id: str
    query: str
    cron_expression: str
    tags: list[str] | None = None
    enabled: bool = True
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None


class TaskScheduler:
    def __init__(self, autonomous_query_fn: Callable[[str], dict[str, Any]], db_connection_fn: Callable, event_bus=None):
        self.scheduler = BackgroundScheduler()
        self.autonomous_query_fn = autonomous_query_fn
        self.db_connection_fn = db_connection_fn
        self.event_bus = event_bus
        self.jobs: dict[str, ScheduledJob] = {}

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS scheduled_action (
            id BIGSERIAL PRIMARY KEY,
            query TEXT NOT NULL,
            cron_expression VARCHAR(64) NOT NULL,
            tags TEXT[],
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_run_at TIMESTAMPTZ,
            last_result JSONB
        );
        CREATE TABLE IF NOT EXISTS job_execution_log (
            id BIGSERIAL PRIMARY KEY,
            scheduled_action_id BIGINT NOT NULL REFERENCES scheduled_action(id) ON DELETE CASCADE,
            query TEXT,
            result JSONB,
            status VARCHAR(32),
            error_message TEXT,
            executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_scheduled_action_enabled ON scheduled_action(enabled);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def _parse_cron(self, cron_expression: str) -> CronTrigger:
        parts = [p for p in str(cron_expression or "").split() if p]
        if len(parts) != 5:
            raise ValueError("cron_expression must have 5 fields: minute hour day month weekday")
        minute, hour, day, month, weekday = parts
        return CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=weekday)

    def _register_runtime_job(self, job_id: str, query: str, cron_expression: str) -> None:
        trigger = self._parse_cron(cron_expression)
        runtime_id = f"scheduled_action:{job_id}"
        self.scheduler.add_job(
            self._execute_job,
            trigger=trigger,
            id=runtime_id,
            replace_existing=True,
            kwargs={"job_id": job_id, "query": query},
        )

    def start(self) -> None:
        self.ensure_tables()
        self.load_from_db()
        if not self.scheduler.running:
            self.scheduler.start()

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    def add_job(self, query: str, cron_expression: str, tags: list[str] | None = None) -> ScheduledJob:
        trigger = self._parse_cron(cron_expression)
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scheduled_action (query, cron_expression, tags, enabled) VALUES (%s, %s, %s, TRUE) RETURNING id",
                    (str(query).strip(), str(cron_expression).strip(), list(tags or [])),
                )
                job_id = str(cur.fetchone()[0])

        runtime_id = f"scheduled_action:{job_id}"
        self.scheduler.add_job(
            self._execute_job,
            trigger=trigger,
            id=runtime_id,
            replace_existing=True,
            kwargs={"job_id": job_id, "query": str(query).strip()},
        )

        runtime_job = self.scheduler.get_job(runtime_id)
        next_run = getattr(runtime_job, "next_run_time", None) if runtime_job is not None else None
        job = ScheduledJob(
            job_id=job_id,
            query=str(query).strip(),
            cron_expression=str(cron_expression).strip(),
            tags=list(tags or []),
            enabled=True,
            next_run_at=next_run,
        )
        self.jobs[job_id] = job
        return job

    def remove_job(self, job_id: str) -> bool:
        runtime_id = f"scheduled_action:{job_id}"
        try:
            self.scheduler.remove_job(runtime_id)
        except Exception:
            pass

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM scheduled_action WHERE id=%s", (int(job_id),))
                deleted = cur.rowcount > 0

        self.jobs.pop(str(job_id), None)
        return deleted

    def pause_job(self, job_id: str) -> bool:
        runtime_id = f"scheduled_action:{job_id}"
        try:
            self.scheduler.pause_job(runtime_id)
        except Exception:
            pass

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE scheduled_action SET enabled=FALSE WHERE id=%s", (int(job_id),))
                updated = cur.rowcount > 0

        if updated and str(job_id) in self.jobs:
            self.jobs[str(job_id)].enabled = False
        return updated

    def resume_job(self, job_id: str) -> bool:
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT query, cron_expression FROM scheduled_action WHERE id=%s", (int(job_id),))
                row = cur.fetchone()
                if not row:
                    return False
                query, cron_expression = row
                cur.execute("UPDATE scheduled_action SET enabled=TRUE WHERE id=%s", (int(job_id),))

        runtime_id = f"scheduled_action:{job_id}"
        if self.scheduler.get_job(runtime_id) is None:
            self._register_runtime_job(str(job_id), str(query), str(cron_expression))
        else:
            self.scheduler.resume_job(runtime_id)

        if str(job_id) in self.jobs:
            self.jobs[str(job_id)].enabled = True
        return True

    def list_jobs(self) -> list[ScheduledJob]:
        jobs: list[ScheduledJob] = []
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, query, cron_expression, tags, enabled, last_run_at FROM scheduled_action ORDER BY id DESC"
                )
                rows = cur.fetchall() or []

        for row in rows:
            jid = str(row[0])
            runtime_id = f"scheduled_action:{jid}"
            runtime_job = self.scheduler.get_job(runtime_id)
            next_run = runtime_job.next_run_time if runtime_job else None
            item = ScheduledJob(
                job_id=jid,
                query=row[1],
                cron_expression=row[2],
                tags=list(row[3] or []),
                enabled=bool(row[4]),
                last_run_at=row[5],
                next_run_at=next_run,
            )
            self.jobs[jid] = item
            jobs.append(item)
        return jobs

    def load_from_db(self) -> None:
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, query, cron_expression FROM scheduled_action WHERE enabled=TRUE")
                rows = cur.fetchall() or []

        for row in rows:
            jid = str(row[0])
            query = str(row[1])
            cron = str(row[2])
            self._register_runtime_job(jid, query, cron)
            self.jobs[jid] = ScheduledJob(job_id=jid, query=query, cron_expression=cron, enabled=True)

    def _execute_job(self, job_id: str, query: str) -> None:
        result: dict[str, Any] | None = None
        status = "error"
        error_message = None
        try:
            result = self.autonomous_query_fn(query)
            source = str((result or {}).get("answer_source") or "")
            status = "success" if source and source != "not_found" else "no_result"
        except Exception as exc:
            error_message = str(exc)
            result = {"error": error_message}

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE scheduled_action SET last_run_at=NOW(), last_result=%s WHERE id=%s",
                    (json.dumps(result, ensure_ascii=False), int(job_id)),
                )
                cur.execute(
                    "INSERT INTO job_execution_log (scheduled_action_id, query, result, status, error_message) VALUES (%s, %s, %s, %s, %s)",
                    (int(job_id), query, json.dumps(result, ensure_ascii=False), status, error_message),
                )

        if self.event_bus is not None:
            payload = {
                "job_id": int(job_id),
                "query": query,
                "status": status,
                "result": result,
            }
            self.event_bus.emit(
                event_type="scheduler.job_executed",
                source_entity="scheduled_action",
                entity_id=int(job_id),
                payload=payload,
                tags=["scheduler"],
            )
