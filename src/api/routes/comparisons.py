"""Comparison API routes."""

import uuid
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from src.core.logging import get_logger
from src.data.database import get_cached_connection
from src.data.models import AuthType, ComparisonMode, ConnectionInfo
from src.services.comparison import ComparisonService
from src.services.persistence import get_persistence_service

logger = get_logger(__name__)
router = APIRouter()


class ConnectionConfig(BaseModel):
    """Connection configuration."""

    server: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    use_windows_auth: bool = False


class ComparisonRequest(BaseModel):
    """Request model for comparison."""

    source: ConnectionConfig = Field(..., description="Source database connection")
    target: ConnectionConfig = Field(..., description="Target database connection")
    schema_name: str = Field("dbo", description="Schema to compare")
    tables: list[str] = Field(..., description="List of tables to compare")
    parallel: bool = Field(True, description="Run comparisons in parallel")
    max_workers: int = Field(4, description="Maximum parallel workers")


class ComparisonResultItem(BaseModel):
    """Single comparison result."""

    source_table: str
    target_table: str
    status: str
    source_row_count: int
    target_row_count: int
    matching_rows: int
    different_rows: int
    source_only_rows: int
    target_only_rows: int
    match_percentage: float
    duration_seconds: float
    error_message: Optional[str] = None


class ComparisonResponse(BaseModel):
    """Response model for comparison results."""

    run_id: str
    status: str
    total_tables: int
    matching_tables: int
    different_tables: int
    failed_tables: int
    results: list[ComparisonResultItem]


class AsyncComparisonResponse(BaseModel):
    """Response model for async comparison start."""

    run_id: str
    status: str
    message: str
    tables_count: int


# In-memory storage for async job status
_async_jobs: dict[str, dict] = {}


def _run_comparison_background(
    run_id: str,
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    schema_name: str,
    tables: list[str],
    parallel: bool,
    max_workers: int,
):
    """Background task to run comparison."""
    try:
        _async_jobs[run_id] = {"status": "running", "progress": 0, "results": []}

        # Create connection info
        source_conn_info = ConnectionInfo(
            server=source_config.server,
            database=source_config.database,
            username=source_config.username,
            password=source_config.password,
            auth_type=AuthType.WINDOWS if source_config.use_windows_auth else AuthType.SQL,
        )

        target_conn_info = ConnectionInfo(
            server=target_config.server,
            database=target_config.database,
            username=target_config.username,
            password=target_config.password,
            auth_type=AuthType.WINDOWS if target_config.use_windows_auth else AuthType.SQL,
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
            source_server=source_config.server,
            source_database=source_config.database,
            target_server=target_config.server,
            target_database=target_config.database,
            schema_name=schema_name,
        )

        # Run comparisons
        results = []
        matching = 0
        different = 0
        failed = 0

        for i, result in enumerate(service.compare_multiple_tables(
            schema_name, schema_name, tables,
            ComparisonMode.QUICK, max_workers, parallel
        )):
            # Save to persistence
            persistence.save_result(run_id, result)

            # Track statistics
            if result.status == "failed":
                failed += 1
            elif result.is_match():
                matching += 1
            else:
                different += 1

            results.append({
                "source_table": result.source_table,
                "target_table": result.target_table,
                "status": result.status,
                "source_row_count": result.source_row_count,
                "target_row_count": result.target_row_count,
                "matching_rows": result.matching_rows,
                "different_rows": result.different_rows,
                "source_only_rows": result.source_only_rows,
                "target_only_rows": result.target_only_rows,
                "match_percentage": result.get_match_percentage(),
                "duration_seconds": result.duration_seconds,
                "error_message": result.error_message,
            })

            _async_jobs[run_id]["progress"] = (i + 1) / len(tables) * 100
            _async_jobs[run_id]["results"] = results

        # Complete run
        persistence.complete_run(
            run_id=run_id,
            total_tables=len(tables),
            matching_tables=matching,
            different_tables=different,
            failed_tables=failed,
        )

        _async_jobs[run_id]["status"] = "completed"
        _async_jobs[run_id]["matching"] = matching
        _async_jobs[run_id]["different"] = different
        _async_jobs[run_id]["failed"] = failed

    except Exception as e:
        logger.error(f"Background comparison failed: {str(e)}")
        _async_jobs[run_id]["status"] = "failed"
        _async_jobs[run_id]["error"] = str(e)


@router.post("/run", response_model=ComparisonResponse)
async def run_comparison(request: ComparisonRequest):
    """
    Run synchronous comparison.

    Compares specified tables between source and target databases.
    Returns when all comparisons are complete.
    """
    try:
        run_id = str(uuid.uuid4())[:8]

        # Create connection info
        source_conn_info = ConnectionInfo(
            server=request.source.server,
            database=request.source.database,
            username=request.source.username,
            password=request.source.password,
            auth_type=AuthType.WINDOWS if request.source.use_windows_auth else AuthType.SQL,
        )

        target_conn_info = ConnectionInfo(
            server=request.target.server,
            database=request.target.database,
            username=request.target.username,
            password=request.target.password,
            auth_type=AuthType.WINDOWS if request.target.use_windows_auth else AuthType.SQL,
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
            source_server=request.source.server,
            source_database=request.source.database,
            target_server=request.target.server,
            target_database=request.target.database,
            schema_name=request.schema_name,
        )

        # Run comparisons
        results = []
        matching = 0
        different = 0
        failed = 0

        for result in service.compare_multiple_tables(
            request.schema_name,
            request.schema_name,
            request.tables,
            ComparisonMode.QUICK,
            request.max_workers,
            request.parallel,
        ):
            # Save to persistence
            persistence.save_result(run_id, result)

            # Track statistics
            if result.status == "failed":
                failed += 1
            elif result.is_match():
                matching += 1
            else:
                different += 1

            results.append(ComparisonResultItem(
                source_table=result.source_table,
                target_table=result.target_table,
                status=result.status,
                source_row_count=result.source_row_count,
                target_row_count=result.target_row_count,
                matching_rows=result.matching_rows,
                different_rows=result.different_rows,
                source_only_rows=result.source_only_rows,
                target_only_rows=result.target_only_rows,
                match_percentage=result.get_match_percentage(),
                duration_seconds=result.duration_seconds,
                error_message=result.error_message,
            ))

        # Complete run
        persistence.complete_run(
            run_id=run_id,
            total_tables=len(request.tables),
            matching_tables=matching,
            different_tables=different,
            failed_tables=failed,
        )

        return ComparisonResponse(
            run_id=run_id,
            status="completed",
            total_tables=len(request.tables),
            matching_tables=matching,
            different_tables=different,
            failed_tables=failed,
            results=results,
        )

    except Exception as e:
        logger.error(f"Comparison failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run/async", response_model=AsyncComparisonResponse)
async def run_comparison_async(
    request: ComparisonRequest,
    background_tasks: BackgroundTasks,
):
    """
    Start asynchronous comparison.

    Starts comparison in background and returns immediately with run ID.
    Use /status/{run_id} to check progress.
    """
    run_id = str(uuid.uuid4())[:8]

    background_tasks.add_task(
        _run_comparison_background,
        run_id,
        request.source,
        request.target,
        request.schema_name,
        request.tables,
        request.parallel,
        request.max_workers,
    )

    return AsyncComparisonResponse(
        run_id=run_id,
        status="started",
        message="Comparison started in background",
        tables_count=len(request.tables),
    )


@router.get("/status/{run_id}")
async def get_comparison_status(run_id: str):
    """
    Get status of async comparison.

    Returns current progress and results for background comparison.
    """
    if run_id in _async_jobs:
        job = _async_jobs[run_id]
        return {
            "run_id": run_id,
            "status": job.get("status"),
            "progress": job.get("progress", 0),
            "results_count": len(job.get("results", [])),
            "matching": job.get("matching", 0),
            "different": job.get("different", 0),
            "failed": job.get("failed", 0),
            "error": job.get("error"),
        }

    # Check persistence
    persistence = get_persistence_service()
    run = persistence.get_run(run_id)

    if run:
        return {
            "run_id": run_id,
            "status": run.get("status"),
            "progress": 100 if run.get("status") == "completed" else 0,
            "matching": run.get("matching_tables", 0),
            "different": run.get("different_tables", 0),
            "failed": run.get("failed_tables", 0),
        }

    raise HTTPException(status_code=404, detail=f"Run {run_id} not found")


@router.get("/results/{run_id}")
async def get_comparison_results(run_id: str):
    """
    Get results of completed comparison.

    Returns detailed results from persistence storage.
    """
    persistence = get_persistence_service()
    run = persistence.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    results = persistence.get_run_results(run_id)

    return {
        "run_id": run_id,
        "run_info": run,
        "results": results,
    }
