"""Tests for persistence service."""

import os
import tempfile
from datetime import datetime

import pytest

from src.data.models import ComparisonMode, ComparisonResult, DifferenceType, SchemaDifference
from src.services.persistence import ResultPersistenceService


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def persistence_service(temp_db):
    """Create persistence service with temp database."""
    return ResultPersistenceService(db_path=temp_db)


class TestResultPersistenceService:
    """Tests for ResultPersistenceService."""

    def test_create_run(self, persistence_service):
        """Test creating a comparison run."""
        run_id = persistence_service.create_run(
            run_id="test123",
            source_server="source-server",
            source_database="source-db",
            target_server="target-server",
            target_database="target-db",
            schema_name="dbo",
        )

        assert run_id == "test123"

        # Verify run was created
        run = persistence_service.get_run("test123")
        assert run is not None
        assert run["source_server"] == "source-server"
        assert run["target_database"] == "target-db"

    def test_save_and_get_results(self, persistence_service):
        """Test saving and retrieving results."""
        # Create run
        persistence_service.create_run(
            run_id="test456",
            source_server="src",
            source_database="srcdb",
            target_server="tgt",
            target_database="tgtdb",
            schema_name="dbo",
        )

        # Create a comparison result
        result = ComparisonResult(
            source_table="dbo.test_table",
            target_table="dbo.test_table",
            mode=ComparisonMode.QUICK,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="completed",
            source_row_count=100,
            target_row_count=100,
            matching_rows=100,
        )

        # Save result
        result_id = persistence_service.save_result("test456", result)
        assert result_id > 0

        # Retrieve results
        results = persistence_service.get_run_results("test456")
        assert len(results) == 1
        assert results[0]["source_table"] == "dbo.test_table"
        assert results[0]["source_row_count"] == 100

    def test_complete_run(self, persistence_service):
        """Test completing a run."""
        persistence_service.create_run(
            run_id="test789",
            source_server="src",
            source_database="srcdb",
            target_server="tgt",
            target_database="tgtdb",
            schema_name="dbo",
        )

        persistence_service.complete_run(
            run_id="test789",
            total_tables=10,
            matching_tables=8,
            different_tables=2,
            failed_tables=0,
        )

        run = persistence_service.get_run("test789")
        assert run["status"] == "completed"
        assert run["total_tables"] == 10
        assert run["matching_tables"] == 8

    def test_get_runs_pagination(self, persistence_service):
        """Test getting runs with pagination."""
        # Create multiple runs
        for i in range(5):
            persistence_service.create_run(
                run_id=f"run{i}",
                source_server="src",
                source_database="srcdb",
                target_server="tgt",
                target_database="tgtdb",
                schema_name="dbo",
            )

        # Get first 3 runs
        runs = persistence_service.get_runs(limit=3, offset=0)
        assert len(runs) == 3

        # Get next 2 runs
        runs = persistence_service.get_runs(limit=3, offset=3)
        assert len(runs) == 2

    def test_delete_run(self, persistence_service):
        """Test deleting a run."""
        persistence_service.create_run(
            run_id="to_delete",
            source_server="src",
            source_database="srcdb",
            target_server="tgt",
            target_database="tgtdb",
            schema_name="dbo",
        )

        # Verify it exists
        assert persistence_service.get_run("to_delete") is not None

        # Delete
        deleted = persistence_service.delete_run("to_delete")
        assert deleted is True

        # Verify it's gone
        assert persistence_service.get_run("to_delete") is None

    def test_get_statistics(self, persistence_service):
        """Test getting statistics."""
        # Create run with results
        persistence_service.create_run(
            run_id="stats_test",
            source_server="src",
            source_database="srcdb",
            target_server="tgt",
            target_database="tgtdb",
            schema_name="dbo",
        )

        result = ComparisonResult(
            source_table="dbo.test",
            target_table="dbo.test",
            mode=ComparisonMode.QUICK,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="completed",
            source_row_count=50,
            target_row_count=50,
            matching_rows=50,
        )
        persistence_service.save_result("stats_test", result)
        persistence_service.complete_run("stats_test", 1, 1, 0, 0)

        stats = persistence_service.get_statistics()
        assert stats["total_runs"] >= 1
        assert "recent_runs" in stats

    def test_result_with_schema_differences(self, persistence_service):
        """Test saving results with schema differences."""
        persistence_service.create_run(
            run_id="schema_diff",
            source_server="src",
            source_database="srcdb",
            target_server="tgt",
            target_database="tgtdb",
            schema_name="dbo",
        )

        result = ComparisonResult(
            source_table="dbo.test",
            target_table="dbo.test",
            mode=ComparisonMode.QUICK,
            started_at=datetime.now(),
            status="completed",
        )
        result.schema_differences = [
            SchemaDifference(
                table_name="test",
                difference_type=DifferenceType.SCHEMA_ONLY_SOURCE,
                column_name="extra_col",
                description="Column exists only in source",
            )
        ]

        persistence_service.save_result("schema_diff", result)

        results = persistence_service.get_run_results("schema_diff")
        assert len(results) == 1
        assert len(results[0]["schema_differences"]) == 1
        assert results[0]["schema_differences"][0]["column_name"] == "extra_col"
