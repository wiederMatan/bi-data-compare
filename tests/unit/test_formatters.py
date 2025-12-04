"""Unit tests for formatters."""

import pytest

from src.utils.formatters import (
    format_bytes,
    format_duration,
    format_number,
    format_percentage,
    format_table_name,
    truncate_string,
)


class TestFormatBytes:
    """Tests for byte formatting."""

    def test_format_bytes_b(self):
        """Test bytes formatting."""
        assert format_bytes(512) == "512.00 B"

    def test_format_kb(self):
        """Test kilobytes formatting."""
        assert format_bytes(1536) == "1.50 KB"

    def test_format_mb(self):
        """Test megabytes formatting."""
        assert format_bytes(1572864) == "1.50 MB"

    def test_format_gb(self):
        """Test gigabytes formatting."""
        assert format_bytes(1610612736) == "1.50 GB"


class TestFormatNumber:
    """Tests for number formatting."""

    def test_format_small_number(self):
        """Test small number formatting."""
        assert format_number(999) == "999"

    def test_format_thousands(self):
        """Test thousands formatting."""
        assert format_number(1234) == "1,234"

    def test_format_millions(self):
        """Test millions formatting."""
        assert format_number(1234567) == "1,234,567"


class TestFormatPercentage:
    """Tests for percentage formatting."""

    def test_format_percentage_normal(self):
        """Test normal percentage."""
        assert format_percentage(75, 100) == "75.0%"

    def test_format_percentage_decimal(self):
        """Test percentage with decimals."""
        assert format_percentage(33, 100, precision=2) == "33.00%"

    def test_format_percentage_zero_total(self):
        """Test percentage with zero total."""
        assert format_percentage(0, 0) == "0.0%"


class TestFormatDuration:
    """Tests for duration formatting."""

    def test_format_seconds(self):
        """Test seconds formatting."""
        assert format_duration(30.5) == "30.5s"

    def test_format_minutes(self):
        """Test minutes formatting."""
        assert format_duration(125) == "2m 5s"

    def test_format_hours(self):
        """Test hours formatting."""
        assert format_duration(7325) == "2h 2m"

    def test_format_days(self):
        """Test days formatting."""
        assert format_duration(90000) == "1d 1h"


class TestFormatTableName:
    """Tests for table name formatting."""

    def test_format_table_name(self):
        """Test table name formatting."""
        assert format_table_name("dbo", "Users") == "[dbo].[Users]"


class TestTruncateString:
    """Tests for string truncation."""

    def test_no_truncation_needed(self):
        """Test string shorter than max length."""
        assert truncate_string("Short", 10) == "Short"

    def test_truncation(self):
        """Test string truncation."""
        result = truncate_string("This is a very long string", 15)
        assert result == "This is a ve..."
        assert len(result) == 15

    def test_custom_suffix(self):
        """Test truncation with custom suffix."""
        result = truncate_string("Long string", 8, suffix="--")
        assert result == "Long s--"
