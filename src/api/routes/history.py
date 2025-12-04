"""History API routes for comparison runs."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.core.logging import get_logger
from src.services.persistence import get_persistence_service

logger = get_logger(__name__)
router = APIRouter()


class RunInfo(BaseModel):
    """Comparison run information."""

    run_id: str
    started_at: str
    completed_at: Optional[str]
    source_server: str
    source_database: str
    target_server: str
    target_database: str
    schema_name: str
    total_tables: int
    matching_tables: int
    different_tables: int
    failed_tables: int
    status: str


class HistoryResponse(BaseModel):
    """Response model for history list."""

    runs: list[dict]
    total: int
    limit: int
    offset: int


class StatisticsResponse(BaseModel):
    """Response model for statistics."""

    total_runs: int
    total_tables_compared: int
    matching_tables: int
    different_tables: int
    failed_tables: int
    recent_runs: list[dict]


@router.get("/runs", response_model=HistoryResponse)
async def get_runs(
    limit: int = Query(50, ge=1, le=100, description="Number of runs to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """
    Get list of comparison runs.

    Returns paginated list of all comparison runs.
    """
    try:
        persistence = get_persistence_service()
        runs = persistence.get_runs(limit=limit, offset=offset, status=status)

        return HistoryResponse(
            runs=runs,
            total=len(runs),
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        logger.error(f"Failed to get runs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    """
    Get details of a specific run.

    Returns run metadata and all comparison results.
    """
    try:
        persistence = get_persistence_service()
        run = persistence.get_run(run_id)

        if not run:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

        results = persistence.get_run_results(run_id)

        return {
            "run": run,
            "results": results,
            "results_count": len(results),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get run {run_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/runs/{run_id}")
async def delete_run(run_id: str):
    """
    Delete a comparison run.

    Removes run and all associated results from history.
    """
    try:
        persistence = get_persistence_service()
        deleted = persistence.delete_run(run_id)

        if not deleted:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

        return {"message": f"Run {run_id} deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete run {run_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics", response_model=StatisticsResponse)
async def get_statistics():
    """
    Get overall statistics.

    Returns aggregate statistics across all comparison runs.
    """
    try:
        persistence = get_persistence_service()
        stats = persistence.get_statistics()

        return StatisticsResponse(**stats)

    except Exception as e:
        logger.error(f"Failed to get statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cleanup")
async def cleanup_old_runs(days: int = Query(30, ge=1, le=365, description="Days to keep")):
    """
    Clean up old comparison runs.

    Deletes runs older than specified number of days.
    """
    try:
        persistence = get_persistence_service()
        deleted_count = persistence.cleanup_old_runs(days=days)

        return {
            "message": f"Cleaned up {deleted_count} runs older than {days} days",
            "deleted_count": deleted_count,
        }

    except Exception as e:
        logger.error(f"Failed to cleanup runs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
