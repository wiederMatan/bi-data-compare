"""Tests for scheduler service."""

import time

import pytest

from src.services.scheduler import ScheduledJob, SchedulerService


@pytest.fixture
def scheduler_service():
    """Create scheduler service."""
    service = SchedulerService()
    yield service
    service.stop()


class TestScheduledJob:
    """Tests for ScheduledJob model."""

    def test_to_dict(self):
        """Test converting job to dictionary."""
        job = ScheduledJob(
            job_id="test123",
            name="Test Job",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1", "table2"],
            schedule_type="interval",
            schedule_config={"hours": 1},
        )

        job_dict = job.to_dict()

        assert job_dict["job_id"] == "test123"
        assert job_dict["name"] == "Test Job"
        assert job_dict["schema_name"] == "dbo"
        assert len(job_dict["tables"]) == 2
        assert job_dict["enabled"] is True


class TestSchedulerService:
    """Tests for SchedulerService."""

    def test_start_stop(self, scheduler_service):
        """Test starting and stopping scheduler."""
        scheduler_service.start()
        assert scheduler_service._started is True

        scheduler_service.stop()
        assert scheduler_service._started is False

    def test_add_job(self, scheduler_service):
        """Test adding a scheduled job."""
        job = scheduler_service.add_job(
            name="Test Job",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1"],
            schedule_type="interval",
            schedule_config={"hours": 24},
        )

        assert job.job_id is not None
        assert job.name == "Test Job"
        assert job.enabled is True

    def test_get_jobs(self, scheduler_service):
        """Test getting list of jobs."""
        # Add multiple jobs
        scheduler_service.add_job(
            name="Job 1",
            source_config={"server": "src1", "database": "db1"},
            target_config={"server": "tgt1", "database": "db1"},
            schema_name="dbo",
            tables=["t1"],
            schedule_config={"hours": 1},
        )
        scheduler_service.add_job(
            name="Job 2",
            source_config={"server": "src2", "database": "db2"},
            target_config={"server": "tgt2", "database": "db2"},
            schema_name="dbo",
            tables=["t2"],
            schedule_config={"hours": 2},
        )

        jobs = scheduler_service.get_jobs()
        assert len(jobs) == 2

    def test_get_job(self, scheduler_service):
        """Test getting a specific job."""
        job = scheduler_service.add_job(
            name="Get Test",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1"],
            schedule_config={"hours": 1},
        )

        retrieved = scheduler_service.get_job(job.job_id)
        assert retrieved is not None
        assert retrieved["name"] == "Get Test"

    def test_remove_job(self, scheduler_service):
        """Test removing a job."""
        job = scheduler_service.add_job(
            name="Remove Test",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1"],
            schedule_config={"hours": 1},
        )

        removed = scheduler_service.remove_job(job.job_id)
        assert removed is True

        # Verify job is gone
        assert scheduler_service.get_job(job.job_id) is None

    def test_pause_resume_job(self, scheduler_service):
        """Test pausing and resuming a job."""
        job = scheduler_service.add_job(
            name="Pause Test",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1"],
            schedule_config={"hours": 1},
        )

        # Pause
        paused = scheduler_service.pause_job(job.job_id)
        assert paused is True

        job_info = scheduler_service.get_job(job.job_id)
        assert job_info["enabled"] is False

        # Resume
        resumed = scheduler_service.resume_job(job.job_id)
        assert resumed is True

        job_info = scheduler_service.get_job(job.job_id)
        assert job_info["enabled"] is True

    def test_cron_schedule(self, scheduler_service):
        """Test creating job with cron schedule."""
        job = scheduler_service.add_job(
            name="Cron Test",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1"],
            schedule_type="cron",
            schedule_config={"hour": 2, "minute": 0},
        )

        assert job.schedule_type == "cron"

    def test_remove_nonexistent_job(self, scheduler_service):
        """Test removing a non-existent job."""
        scheduler_service.start()
        removed = scheduler_service.remove_job("nonexistent")
        assert removed is False

    def test_job_callback(self, scheduler_service):
        """Test job completion callback."""
        callback_called = []

        def on_complete(job_id, result):
            callback_called.append((job_id, result))

        job = scheduler_service.add_job(
            name="Callback Test",
            source_config={"server": "src", "database": "srcdb"},
            target_config={"server": "tgt", "database": "tgtdb"},
            schema_name="dbo",
            tables=["table1"],
            schedule_config={"hours": 1},
            on_complete=on_complete,
        )

        assert job.on_complete is not None
