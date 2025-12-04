"""Unit tests for data models."""

import pytest
from datetime import datetime

from src.data.models import (
    ColumnInfo,
    ComparisonMode,
    ComparisonResult,
    CompressionAnalysis,
    CompressionRecommendation,
    CompressionType,
    ConnectionInfo,
    DifferenceType,
    SchemaDifference,
    TableInfo,
)


class TestConnectionInfo:
    """Tests for ConnectionInfo model."""

    def test_get_display_name(self, source_connection_info):
        """Test display name generation."""
        assert source_connection_info.get_display_name() == "test_server/source_db"

    def test_mask_password(self, source_connection_info):
        """Test password masking."""
        masked = source_connection_info.mask_password()
        assert masked.password == "****"
        assert masked.server == source_connection_info.server


class TestColumnInfo:
    """Tests for ColumnInfo model."""

    def test_get_full_type_with_length(self):
        """Test full type with length."""
        col = ColumnInfo(
            column_name="Name",
            data_type="varchar",
            max_length=50,
        )
        assert col.get_full_type() == "varchar(50)"

    def test_get_full_type_with_precision(self):
        """Test full type with precision and scale."""
        col = ColumnInfo(
            column_name="Price",
            data_type="decimal",
            precision=10,
            scale=2,
        )
        assert col.get_full_type() == "decimal(10,2)"

    def test_get_full_type_max(self):
        """Test full type with MAX."""
        col = ColumnInfo(
            column_name="Description",
            data_type="nvarchar",
            max_length=-1,
        )
        assert col.get_full_type() == "nvarchar(MAX)"

    def test_column_equality(self):
        """Test column comparison."""
        col1 = ColumnInfo(
            column_name="Id",
            data_type="int",
            is_nullable=False,
        )
        col2 = ColumnInfo(
            column_name="ID",  # Different case
            data_type="INT",  # Different case
            is_nullable=False,
        )
        assert col1 == col2


class TestTableInfo:
    """Tests for TableInfo model."""

    def test_get_full_name(self, sample_table_info):
        """Test full table name."""
        assert sample_table_info.get_full_name() == "dbo.Users"

    def test_get_size_mb(self, sample_table_info):
        """Test size conversion to MB."""
        assert sample_table_info.get_size_mb() == pytest.approx(0.586, rel=0.01)

    def test_has_primary_key(self, sample_table_info):
        """Test primary key detection."""
        assert not sample_table_info.has_primary_key()

        sample_table_info.primary_key_columns = ["Id"]
        assert sample_table_info.has_primary_key()


class TestComparisonResult:
    """Tests for ComparisonResult model."""

    def test_get_match_percentage(self):
        """Test match percentage calculation."""
        result = ComparisonResult(
            source_table="dbo.Users",
            target_table="dbo.Users",
            mode=ComparisonMode.STANDARD,
            started_at=datetime.now(),
            source_row_count=100,
            target_row_count=100,
            matching_rows=75,
        )
        assert result.get_match_percentage() == 75.0

    def test_is_match_true(self):
        """Test complete match detection."""
        result = ComparisonResult(
            source_table="dbo.Users",
            target_table="dbo.Users",
            mode=ComparisonMode.STANDARD,
            started_at=datetime.now(),
            schema_match=True,
            source_row_count=100,
            target_row_count=100,
            matching_rows=100,
            different_rows=0,
            source_only_rows=0,
            target_only_rows=0,
        )
        assert result.is_match() is True

    def test_is_match_false(self):
        """Test mismatch detection."""
        result = ComparisonResult(
            source_table="dbo.Users",
            target_table="dbo.Users",
            mode=ComparisonMode.STANDARD,
            started_at=datetime.now(),
            schema_match=True,
            source_row_count=100,
            target_row_count=100,
            matching_rows=90,
            different_rows=10,
        )
        assert result.is_match() is False

    def test_get_summary(self):
        """Test summary generation."""
        result = ComparisonResult(
            source_table="dbo.Users",
            target_table="dbo.Users",
            mode=ComparisonMode.STANDARD,
            started_at=datetime.now(),
            schema_match=False,
            source_row_count=100,
            target_row_count=100,
            different_rows=10,
            schema_differences=[
                SchemaDifference(
                    table_name="Users",
                    difference_type=DifferenceType.SCHEMA_DIFFERENT,
                )
            ],
        )
        summary = result.get_summary()
        assert "schema diff" in summary.lower()
        assert "data diff" in summary.lower()


class TestCompressionAnalysis:
    """Tests for CompressionAnalysis model."""

    def test_get_savings_percent(self):
        """Test savings calculation."""
        analysis = CompressionAnalysis(
            table_name="dbo.Users",
            current_compression=CompressionType.NONE,
            current_size_kb=1000.0,
            row_count=10000,
            none_size_kb=1000.0,
            page_size_kb=600.0,
        )

        savings = analysis.get_savings_percent(CompressionType.PAGE)
        assert savings == pytest.approx(40.0, rel=0.01)

    def test_get_savings_percent_none(self):
        """Test savings when size is None."""
        analysis = CompressionAnalysis(
            table_name="dbo.Users",
            current_compression=CompressionType.NONE,
            current_size_kb=1000.0,
            row_count=10000,
        )

        savings = analysis.get_savings_percent(CompressionType.PAGE)
        assert savings is None


class TestCompressionRecommendation:
    """Tests for CompressionRecommendation model."""

    def test_should_apply_true(self):
        """Test recommendation should be applied."""
        rec = CompressionRecommendation(
            table_name="dbo.Users",
            current_compression=CompressionType.NONE,
            recommended_compression=CompressionType.PAGE,
            current_size_mb=100.0,
            estimated_size_mb=60.0,
            estimated_savings_mb=40.0,
            estimated_savings_percent=40.0,
            reason="Test",
        )
        assert rec.should_apply() is True

    def test_should_apply_false_same_compression(self):
        """Test recommendation should not be applied when same."""
        rec = CompressionRecommendation(
            table_name="dbo.Users",
            current_compression=CompressionType.PAGE,
            recommended_compression=CompressionType.PAGE,
            current_size_mb=100.0,
            estimated_size_mb=95.0,
            estimated_savings_mb=5.0,
            estimated_savings_percent=5.0,
            reason="Test",
        )
        assert rec.should_apply() is False

    def test_should_apply_false_low_savings(self):
        """Test recommendation should not be applied when savings too low."""
        rec = CompressionRecommendation(
            table_name="dbo.Users",
            current_compression=CompressionType.NONE,
            recommended_compression=CompressionType.PAGE,
            current_size_mb=100.0,
            estimated_size_mb=95.0,
            estimated_savings_mb=5.0,
            estimated_savings_percent=5.0,
            reason="Test",
        )
        assert rec.should_apply() is False
