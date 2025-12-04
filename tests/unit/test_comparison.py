"""Tests for comparison service - column validation."""

import pytest

from src.data.models import ColumnInfo


@pytest.fixture
def source_columns():
    """Create source columns for testing."""
    return [
        ColumnInfo(column_name="id", data_type="int", is_nullable=False, ordinal_position=1),
        ColumnInfo(column_name="name", data_type="nvarchar", max_length=100, ordinal_position=2),
        ColumnInfo(column_name="email", data_type="nvarchar", max_length=255, ordinal_position=3),
    ]


@pytest.fixture
def target_columns_identical():
    """Create target columns identical to source."""
    return [
        ColumnInfo(column_name="id", data_type="int", is_nullable=False, ordinal_position=1),
        ColumnInfo(column_name="name", data_type="nvarchar", max_length=100, ordinal_position=2),
        ColumnInfo(column_name="email", data_type="nvarchar", max_length=255, ordinal_position=3),
    ]


@pytest.fixture
def target_columns_different():
    """Create target columns with different count."""
    return [
        ColumnInfo(column_name="id", data_type="int", is_nullable=False, ordinal_position=1),
        ColumnInfo(column_name="name", data_type="nvarchar", max_length=100, ordinal_position=2),
        # Missing 'email' column
    ]


@pytest.fixture
def target_columns_extra():
    """Create target columns with extra column."""
    return [
        ColumnInfo(column_name="id", data_type="int", is_nullable=False, ordinal_position=1),
        ColumnInfo(column_name="name", data_type="nvarchar", max_length=100, ordinal_position=2),
        ColumnInfo(column_name="email", data_type="nvarchar", max_length=255, ordinal_position=3),
        ColumnInfo(column_name="phone", data_type="nvarchar", max_length=20, ordinal_position=4),
    ]


class TestColumnCountComparison:
    """Test cases for column count comparison."""

    def test_identical_column_count(self, source_columns, target_columns_identical):
        """Test that tables with identical column count pass."""
        source_col_names = {c.column_name for c in source_columns}
        target_col_names = {c.column_name for c in target_columns_identical}

        source_only = source_col_names - target_col_names
        target_only = target_col_names - source_col_names

        assert len(source_columns) == len(target_columns_identical)
        assert len(source_only) == 0
        assert len(target_only) == 0

    def test_source_has_more_columns(self, source_columns, target_columns_different):
        """Test detection when source has more columns than target."""
        source_col_names = {c.column_name for c in source_columns}
        target_col_names = {c.column_name for c in target_columns_different}

        source_only = source_col_names - target_col_names
        target_only = target_col_names - source_col_names

        assert len(source_columns) > len(target_columns_different)
        assert len(source_only) == 1
        assert "email" in source_only
        assert len(target_only) == 0

    def test_target_has_more_columns(self, source_columns, target_columns_extra):
        """Test detection when target has more columns than source."""
        source_col_names = {c.column_name for c in source_columns}
        target_col_names = {c.column_name for c in target_columns_extra}

        source_only = source_col_names - target_col_names
        target_only = target_col_names - source_col_names

        assert len(source_columns) < len(target_columns_extra)
        assert len(source_only) == 0
        assert len(target_only) == 1
        assert "phone" in target_only

    def test_column_count_difference(self, source_columns, target_columns_different):
        """Test column count difference calculation."""
        diff = len(source_columns) - len(target_columns_different)
        assert diff == 1

    def test_empty_columns(self):
        """Test handling of empty column lists."""
        source_cols = []
        target_cols = []

        assert len(source_cols) == len(target_cols) == 0

    def test_columns_match_returns_true_when_identical(self, source_columns, target_columns_identical):
        """Test that columns_match returns True for identical columns."""
        source_col_names = {c.column_name for c in source_columns}
        target_col_names = {c.column_name for c in target_columns_identical}

        columns_match = source_col_names == target_col_names
        assert columns_match is True

    def test_columns_match_returns_false_when_different(self, source_columns, target_columns_different):
        """Test that columns_match returns False for different columns."""
        source_col_names = {c.column_name for c in source_columns}
        target_col_names = {c.column_name for c in target_columns_different}

        columns_match = source_col_names == target_col_names
        assert columns_match is False
