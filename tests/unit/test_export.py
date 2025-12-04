"""Tests for export service."""

import json
import os
import tempfile
from datetime import datetime

import pytest

from src.data.models import ComparisonMode, ComparisonResult
from src.services.export import ExportService


@pytest.fixture
def export_service():
    """Create export service."""
    return ExportService()


@pytest.fixture
def sample_results():
    """Create sample comparison results."""
    return [
        ComparisonResult(
            source_table="dbo.table1",
            target_table="dbo.table1",
            mode=ComparisonMode.QUICK,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="completed",
            source_row_count=100,
            target_row_count=100,
            matching_rows=100,
            different_rows=0,
        ),
        ComparisonResult(
            source_table="dbo.table2",
            target_table="dbo.table2",
            mode=ComparisonMode.QUICK,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="completed",
            source_row_count=50,
            target_row_count=55,
            matching_rows=45,
            different_rows=5,
        ),
    ]


class TestExportService:
    """Tests for ExportService."""

    def test_export_to_json(self, export_service, sample_results):
        """Test exporting to JSON."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            export_service.export_comparison_to_json(sample_results, output_path)

            # Verify file was created
            assert os.path.exists(output_path)

            # Verify content
            with open(output_path, "r") as f:
                data = json.load(f)

            assert "export_date" in data
            assert data["total_comparisons"] == 2
            assert len(data["results"]) == 2
            assert data["results"][0]["source_table"] == "dbo.table1"

        finally:
            os.unlink(output_path)

    def test_export_to_csv(self, export_service, sample_results):
        """Test exporting to CSV."""
        with tempfile.TemporaryDirectory() as output_dir:
            files = export_service.export_comparison_to_csv(sample_results, output_dir)

            # Verify files were created
            assert len(files) >= 1
            assert os.path.exists(os.path.join(output_dir, "summary.csv"))

    def test_export_to_excel(self, export_service, sample_results):
        """Test exporting to Excel."""
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            output_path = f.name

        try:
            export_service.export_comparison_to_excel(sample_results, output_path)

            # Verify file was created and has content
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0

        finally:
            os.unlink(output_path)

    def test_generate_html_report(self, export_service, sample_results):
        """Test generating HTML report."""
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_path = f.name

        try:
            export_service.generate_html_report(sample_results, output_path)

            # Verify file was created
            assert os.path.exists(output_path)

            # Verify content
            with open(output_path, "r") as f:
                html = f.read()

            assert "<html" in html
            assert "Database Comparison Report" in html
            assert "dbo.table1" in html

        finally:
            os.unlink(output_path)

    def test_export_to_pdf(self, export_service, sample_results):
        """Test exporting to PDF."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            output_path = f.name

        try:
            export_service.export_comparison_to_pdf(sample_results, output_path)

            # Verify file was created
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0

            # Verify it's a PDF (starts with %PDF)
            with open(output_path, "rb") as f:
                header = f.read(4)
            assert header == b"%PDF"

        finally:
            os.unlink(output_path)

    def test_json_export_includes_all_fields(self, export_service, sample_results):
        """Test that JSON export includes all expected fields."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            export_service.export_comparison_to_json(sample_results, output_path)

            with open(output_path, "r") as f:
                data = json.load(f)

            result = data["results"][0]
            expected_fields = [
                "source_table",
                "target_table",
                "mode",
                "status",
                "source_row_count",
                "target_row_count",
                "matching_rows",
                "different_rows",
                "match_percentage",
            ]
            for field in expected_fields:
                assert field in result, f"Missing field: {field}"

        finally:
            os.unlink(output_path)

    def test_export_empty_results(self, export_service):
        """Test exporting empty results."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            export_service.export_comparison_to_json([], output_path)

            with open(output_path, "r") as f:
                data = json.load(f)

            assert data["total_comparisons"] == 0
            assert len(data["results"]) == 0

        finally:
            os.unlink(output_path)
