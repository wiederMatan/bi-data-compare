"""Scheduler API routes."""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.core.logging import get_logger
from src.services.scheduler import get_scheduler_service

logger = get_logger(__name__)
router = APIRouter()


class ConnectionConfig(BaseModel):
    """Connection configuration for scheduled job."""

    server: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    use_windows_auth: bool = False


class ScheduleConfig(BaseModel):
    """Schedule configuration."""

    type: str = Field("interval", description="'interval' or 'cron'")
    hours: Optional[int] = Field(None, description="Interval hours")
    minutes: Optional[int] = Field(None, description="Interval minutes")
    days: Optional[int] = Field(None, description="Interval days")
    hour: Optional[int] = Field(None, description="Cron hour (0-23)")
    minute: Optional[int] = Field(None, description="Cron minute (0-59)")
    day_of_week: Optional[str] = Field(None, description="Cron day of week (mon,tue,...)")


class CreateJobRequest(BaseModel):
    """Request to create scheduled job."""

    name: str = Field(..., description="Job name")
    source: ConnectionConfig = Field(..., description="Source database")
    target: ConnectionConfig = Field(..., description="Target database")
    schema_name: str = Field("dbo", description="Schema to compare")
    tables: list[str] = Field(..., description="Tables to compare")
    schedule: ScheduleConfig = Field(..., description="Schedule configuration")


class JobResponse(BaseModel):
    """Response model for job info."""

    job_id: str
    name: str
    enabled: bool
    schedule_type: str
    next_run: Optional[str]
    last_run: Optional[str]
    run_count: int


@router.post("/jobs", response_model=JobResponse)
async def create_job(request: CreateJobRequest):
    """
    Create a scheduled comparison job.

    Sets up recurring database comparison on a schedule.
    """
    try:
        scheduler = get_scheduler_service()

        # Build schedule config
        schedule_config = {}
        if request.schedule.type == "cron":
            if request.schedule.hour is not None:
                schedule_config["hour"] = request.schedule.hour
            if request.schedule.minute is not None:
                schedule_config["minute"] = request.schedule.minute
            if request.schedule.day_of_week:
                schedule_config["day_of_week"] = request.schedule.day_of_week
        else:
            if request.schedule.hours:
                schedule_config["hours"] = request.schedule.hours
            elif request.schedule.minutes:
                schedule_config["minutes"] = request.schedule.minutes
            elif request.schedule.days:
                schedule_config["days"] = request.schedule.days
            else:
                schedule_config["hours"] = 1  # Default hourly

        job = scheduler.add_job(
            name=request.name,
            source_config=request.source.model_dump(),
            target_config=request.target.model_dump(),
            schema_name=request.schema_name,
            tables=request.tables,
            schedule_type=request.schedule.type,
            schedule_config=schedule_config,
        )

        job_info = scheduler.get_job(job.job_id)

        return JobResponse(
            job_id=job.job_id,
            name=job.name,
            enabled=job.enabled,
            schedule_type=job.schedule_type,
            next_run=job_info.get("next_run") if job_info else None,
            last_run=None,
            run_count=0,
        )

    except Exception as e:
        logger.error(f"Failed to create job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_jobs():
    """
    List all scheduled jobs.

    Returns all configured scheduled comparison jobs.
    """
    try:
        scheduler = get_scheduler_service()
        jobs = scheduler.get_jobs()

        return {
            "jobs": jobs,
            "count": len(jobs),
        }

    except Exception as e:
        logger.error(f"Failed to list jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """
    Get details of a scheduled job.

    Returns job configuration and status.
    """
    try:
        scheduler = get_scheduler_service()
        job = scheduler.get_job(job_id)

        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return job

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """
    Delete a scheduled job.

    Removes job from scheduler.
    """
    try:
        scheduler = get_scheduler_service()
        removed = scheduler.remove_job(job_id)

        if not removed:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return {"message": f"Job {job_id} deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    """
    Pause a scheduled job.

    Job will not run until resumed.
    """
    try:
        scheduler = get_scheduler_service()
        paused = scheduler.pause_job(job_id)

        if not paused:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return {"message": f"Job {job_id} paused"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to pause job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    """
    Resume a paused job.

    Resumes scheduled execution.
    """
    try:
        scheduler = get_scheduler_service()
        resumed = scheduler.resume_job(job_id)

        if not resumed:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return {"message": f"Job {job_id} resumed"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resume job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/run")
async def run_job_now(job_id: str):
    """
    Trigger immediate job execution.

    Runs the job now regardless of schedule.
    """
    try:
        scheduler = get_scheduler_service()
        triggered = scheduler.run_job_now(job_id)

        if not triggered:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return {"message": f"Job {job_id} triggered"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/start")
async def start_scheduler():
    """Start the scheduler service."""
    try:
        scheduler = get_scheduler_service()
        scheduler.start()
        return {"message": "Scheduler started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop")
async def stop_scheduler():
    """Stop the scheduler service."""
    try:
        scheduler = get_scheduler_service()
        scheduler.stop()
        return {"message": "Scheduler stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
