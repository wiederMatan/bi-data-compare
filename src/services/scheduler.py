"""Scheduled comparison service using APScheduler."""

import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.core.logging import get_logger
from src.data.database import get_cached_connection
from src.data.models import AuthType, ComparisonMode, ConnectionInfo
from src.services.comparison import ComparisonService
from src.services.persistence import get_persistence_service

logger = get_logger(__name__)


class ScheduledJob:
    """Represents a scheduled comparison job."""

    def __init__(
        self,
        job_id: str,
        name: str,
        source_config: dict,
        target_config: dict,
        schema_name: str,
        tables: list[str],
        schedule_type: str,
        schedule_config: dict,
        enabled: bool = True,
        on_complete: Optional[Callable[[str, dict], None]] = None,
    ):
        self.job_id = job_id
        self.name = name
        self.source_config = source_config
        self.target_config = target_config
        self.schema_name = schema_name
        self.tables = tables
        self.schedule_type = schedule_type
        self.schedule_config = schedule_config
        self.enabled = enabled
        self.on_complete = on_complete
        self.last_run: Optional[datetime] = None
        self.last_result: Optional[dict] = None
        self.run_count = 0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "job_id": self.job_id,
            "name": self.name,
            "schema_name": self.schema_name,
            "tables": self.tables,
            "schedule_type": self.schedule_type,
            "schedule_config": self.schedule_config,
            "enabled": self.enabled,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "run_count": self.run_count,
            "source": {
                "server": self.source_config.get("server"),
                "database": self.source_config.get("database"),
            },
            "target": {
                "server": self.target_config.get("server"),
                "database": self.target_config.get("database"),
            },
        }


class SchedulerService:
    """Service for scheduling recurring database comparisons."""

    def __init__(self):
        """Initialize scheduler service."""
        self._scheduler: Optional[BackgroundScheduler] = None
        self._jobs: dict[str, ScheduledJob] = {}
        self._lock = threading.Lock()
        self._started = False

    def start(self) -> None:
        """Start the scheduler."""
        if self._started:
            return

        jobstores = {
            "default": MemoryJobStore()
        }
        executors = {
            "default": ThreadPoolExecutor(max_workers=4)
        }
        job_defaults = {
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 60,
        }

        self._scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
        )
        self._scheduler.start()
        self._started = True
        logger.info("Scheduler service started")

    def stop(self) -> None:
        """Stop the scheduler."""
        if self._scheduler and self._started:
            self._scheduler.shutdown(wait=False)
            self._started = False
            logger.info("Scheduler service stopped")

    def add_job(
        self,
        name: str,
        source_config: dict,
        target_config: dict,
        schema_name: str,
        tables: list[str],
        schedule_type: str = "interval",
        schedule_config: Optional[dict] = None,
        on_complete: Optional[Callable[[str, dict], None]] = None,
    ) -> ScheduledJob:
        """
        Add a scheduled comparison job.

        Args:
            name: Job name
            source_config: Source database config (server, database, username, password, use_windows_auth)
            target_config: Target database config
            schema_name: Schema to compare
            tables: Tables to compare
            schedule_type: 'interval' or 'cron'
            schedule_config: Schedule configuration
                - For interval: {"hours": 1} or {"minutes": 30} or {"days": 1}
                - For cron: {"hour": 2, "minute": 0} (daily at 2 AM)
            on_complete: Callback function when job completes

        Returns:
            ScheduledJob instance
        """
        if not self._started:
            self.start()

        job_id = str(uuid.uuid4())[:8]

        if schedule_config is None:
            schedule_config = {"hours": 1}  # Default: hourly

        job = ScheduledJob(
            job_id=job_id,
            name=name,
            source_config=source_config,
            target_config=target_config,
            schema_name=schema_name,
            tables=tables,
            schedule_type=schedule_type,
            schedule_config=schedule_config,
            on_complete=on_complete,
        )

        # Create trigger
        if schedule_type == "cron":
            trigger = CronTrigger(**schedule_config)
        else:
            trigger = IntervalTrigger(**schedule_config)

        # Add to scheduler
        self._scheduler.add_job(
            func=self._execute_job,
            trigger=trigger,
            id=job_id,
            name=name,
            args=[job],
            replace_existing=True,
        )

        with self._lock:
            self._jobs[job_id] = job

        logger.info(f"Added scheduled job: {name} (ID: {job_id})")
        return job

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a scheduled job.

        Args:
            job_id: Job ID to remove

        Returns:
            True if removed, False if not found
        """
        with self._lock:
            if job_id not in self._jobs:
                return False

            self._scheduler.remove_job(job_id)
            del self._jobs[job_id]

        logger.info(f"Removed scheduled job: {job_id}")
        return True

    def pause_job(self, job_id: str) -> bool:
        """Pause a scheduled job."""
        with self._lock:
            if job_id not in self._jobs:
                return False

            self._scheduler.pause_job(job_id)
            self._jobs[job_id].enabled = False

        logger.info(f"Paused scheduled job: {job_id}")
        return True

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        with self._lock:
            if job_id not in self._jobs:
                return False

            self._scheduler.resume_job(job_id)
            self._jobs[job_id].enabled = True

        logger.info(f"Resumed scheduled job: {job_id}")
        return True

    def run_job_now(self, job_id: str) -> bool:
        """
        Trigger immediate execution of a job.

        Args:
            job_id: Job ID to run

        Returns:
            True if triggered, False if not found
        """
        with self._lock:
            if job_id not in self._jobs:
                return False

            job = self._jobs[job_id]

        # Execute in background
        threading.Thread(
            target=self._execute_job,
            args=[job],
            daemon=True,
        ).start()

        return True

    def get_jobs(self) -> list[dict]:
        """Get all scheduled jobs."""
        with self._lock:
            jobs = []
            for job in self._jobs.values():
                job_dict = job.to_dict()

                # Get next run time from scheduler
                scheduler_job = self._scheduler.get_job(job.job_id)
                if scheduler_job and scheduler_job.next_run_time:
                    job_dict["next_run"] = scheduler_job.next_run_time.isoformat()
                else:
                    job_dict["next_run"] = None

                jobs.append(job_dict)

            return jobs

    def get_job(self, job_id: str) -> Optional[dict]:
        """Get a specific job by ID."""
        with self._lock:
            if job_id not in self._jobs:
                return None

            job = self._jobs[job_id]
            job_dict = job.to_dict()

            scheduler_job = self._scheduler.get_job(job_id)
            if scheduler_job and scheduler_job.next_run_time:
                job_dict["next_run"] = scheduler_job.next_run_time.isoformat()

            return job_dict

    def _execute_job(self, job: ScheduledJob) -> None:
        """Execute a scheduled comparison job."""
        logger.info(f"Executing scheduled job: {job.name} (ID: {job.job_id})")

        try:
            run_id = str(uuid.uuid4())[:8]

            # Create connection info
            source_conn_info = ConnectionInfo(
                server=job.source_config["server"],
                database=job.source_config["database"],
                username=job.source_config.get("username"),
                password=job.source_config.get("password"),
                auth_type=(
                    AuthType.WINDOWS
                    if job.source_config.get("use_windows_auth")
                    else AuthType.SQL
                ),
            )

            target_conn_info = ConnectionInfo(
                server=job.target_config["server"],
                database=job.target_config["database"],
                username=job.target_config.get("username"),
                password=job.target_config.get("password"),
                auth_type=(
                    AuthType.WINDOWS
                    if job.target_config.get("use_windows_auth")
                    else AuthType.SQL
                ),
            )

            # Get connections
            source_conn = get_cached_connection(source_conn_info)
            target_conn = get_cached_connection(target_conn_info)

            # Create comparison service
            service = ComparisonService(source_conn, target_conn)

            # Create persistence run
            persistence = get_persistence_service()
            persistence.create_run(
                run_id=run_id,
                source_server=job.source_config["server"],
                source_database=job.source_config["database"],
                target_server=job.target_config["server"],
                target_database=job.target_config["database"],
                schema_name=job.schema_name,
            )

            # Run comparisons
            matching = 0
            different = 0
            failed = 0

            for result in service.compare_multiple_tables(
                job.schema_name,
                job.schema_name,
                job.tables,
                ComparisonMode.QUICK,
            ):
                persistence.save_result(run_id, result)

                if result.status == "failed":
                    failed += 1
                elif result.is_match():
                    matching += 1
                else:
                    different += 1

            # Complete run
            persistence.complete_run(
                run_id=run_id,
                total_tables=len(job.tables),
                matching_tables=matching,
                different_tables=different,
                failed_tables=failed,
            )

            # Update job status
            job.last_run = datetime.now()
            job.run_count += 1
            job.last_result = {
                "run_id": run_id,
                "total": len(job.tables),
                "matching": matching,
                "different": different,
                "failed": failed,
            }

            logger.info(
                f"Scheduled job completed: {job.name} - "
                f"{matching}/{len(job.tables)} matching"
            )

            # Call completion callback
            if job.on_complete:
                job.on_complete(job.job_id, job.last_result)

        except Exception as e:
            logger.error(f"Scheduled job failed: {job.name} - {str(e)}")
            job.last_result = {"error": str(e)}


# Global singleton
_scheduler_service: Optional[SchedulerService] = None
_scheduler_lock = threading.Lock()


def get_scheduler_service() -> SchedulerService:
    """Get global scheduler service instance."""
    global _scheduler_service
    with _scheduler_lock:
        if _scheduler_service is None:
            _scheduler_service = SchedulerService()
        return _scheduler_service
