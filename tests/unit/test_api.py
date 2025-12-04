"""Tests for API routes."""

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app


@pytest.fixture
def client():
    """Create test client."""
    app = create_app()
    return TestClient(app)


class TestHealthEndpoint:
    """Tests for health endpoint."""

    def test_health_check(self, client):
        """Test health check returns healthy status."""
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


class TestAPIInfo:
    """Tests for API info endpoint."""

    def test_api_info(self, client):
        """Test API info endpoint."""
        response = client.get("/api/v1")
        assert response.status_code == 200

        data = response.json()
        assert data["name"] == "BI Data Compare API"
        assert "endpoints" in data


class TestConnectionsAPI:
    """Tests for connections API."""

    def test_test_connection_missing_server(self, client):
        """Test connection with missing server."""
        response = client.post(
            "/api/v1/connections/test",
            json={
                "database": "testdb",
            },
        )
        assert response.status_code == 422  # Validation error


class TestHistoryAPI:
    """Tests for history API."""

    def test_get_runs_empty(self, client):
        """Test getting runs when empty."""
        response = client.get("/api/v1/history/runs")
        assert response.status_code == 200

        data = response.json()
        assert "runs" in data
        assert "total" in data

    def test_get_statistics(self, client):
        """Test getting statistics."""
        response = client.get("/api/v1/history/statistics")
        assert response.status_code == 200

        data = response.json()
        assert "total_runs" in data
        assert "total_tables_compared" in data


class TestSchedulerAPI:
    """Tests for scheduler API."""

    def test_list_jobs_empty(self, client):
        """Test listing jobs when empty."""
        response = client.get("/api/v1/scheduler/jobs")
        assert response.status_code == 200

        data = response.json()
        assert "jobs" in data
        assert "count" in data

    def test_create_job_validation(self, client):
        """Test job creation validation."""
        # Missing required fields
        response = client.post(
            "/api/v1/scheduler/jobs",
            json={
                "name": "Test",
            },
        )
        assert response.status_code == 422


class TestNotificationsAPI:
    """Tests for notifications API."""

    def test_notification_status_unconfigured(self, client):
        """Test notification status when not configured."""
        response = client.get("/api/v1/notifications/status")
        assert response.status_code == 200

        data = response.json()
        assert data["enabled"] is False

    def test_send_email_unconfigured(self, client):
        """Test sending email when not configured."""
        response = client.post(
            "/api/v1/notifications/send",
            json={
                "to": ["test@example.com"],
                "subject": "Test",
                "body": "Test body",
            },
        )
        assert response.status_code == 400  # Not configured


class TestComparisonsAPI:
    """Tests for comparisons API."""

    def test_run_comparison_validation(self, client):
        """Test comparison validation."""
        # Missing required fields
        response = client.post(
            "/api/v1/comparisons/run",
            json={
                "source": {"server": "src"},
            },
        )
        assert response.status_code == 422

    def test_get_status_not_found(self, client):
        """Test getting status for non-existent run."""
        response = client.get("/api/v1/comparisons/status/nonexistent")
        assert response.status_code == 404


class TestOpenAPISpec:
    """Tests for OpenAPI specification."""

    def test_openapi_available(self, client):
        """Test OpenAPI spec is available."""
        response = client.get("/api/openapi.json")
        assert response.status_code == 200

        data = response.json()
        assert "openapi" in data
        assert "paths" in data

    def test_docs_available(self, client):
        """Test docs UI is available."""
        response = client.get("/api/docs")
        assert response.status_code == 200
