"""Advanced filtering utilities for comparison operations."""

import re
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Any, Callable, Optional, Union

from src.core.logging import get_logger

logger = get_logger(__name__)


class FilterOperator(str, Enum):
    """Filter comparison operators."""

    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUALS = "gte"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUALS = "lte"
    IN = "in"
    NOT_IN = "not_in"
    LIKE = "like"
    NOT_LIKE = "not_like"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"
    REGEX = "regex"


@dataclass
class ColumnFilter:
    """Filter for a single column."""

    column_name: str
    operator: FilterOperator
    value: Any = None
    value2: Any = None  # For BETWEEN operator

    def to_sql(self, param_prefix: str = "p") -> tuple[str, dict]:
        """
        Convert filter to SQL WHERE clause.

        Args:
            param_prefix: Prefix for parameter names

        Returns:
            Tuple of (SQL clause, parameters dict)
        """
        params = {}
        col = f"[{self.column_name}]"

        if self.operator == FilterOperator.EQUALS:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} = :{param_prefix}_val", params

        elif self.operator == FilterOperator.NOT_EQUALS:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} != :{param_prefix}_val", params

        elif self.operator == FilterOperator.GREATER_THAN:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} > :{param_prefix}_val", params

        elif self.operator == FilterOperator.GREATER_THAN_OR_EQUALS:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} >= :{param_prefix}_val", params

        elif self.operator == FilterOperator.LESS_THAN:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} < :{param_prefix}_val", params

        elif self.operator == FilterOperator.LESS_THAN_OR_EQUALS:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} <= :{param_prefix}_val", params

        elif self.operator == FilterOperator.IN:
            if isinstance(self.value, (list, tuple)):
                placeholders = ", ".join(
                    f":{param_prefix}_{i}" for i in range(len(self.value))
                )
                for i, v in enumerate(self.value):
                    params[f"{param_prefix}_{i}"] = v
                return f"{col} IN ({placeholders})", params
            return f"{col} IN (:{param_prefix}_val)", {f"{param_prefix}_val": self.value}

        elif self.operator == FilterOperator.NOT_IN:
            if isinstance(self.value, (list, tuple)):
                placeholders = ", ".join(
                    f":{param_prefix}_{i}" for i in range(len(self.value))
                )
                for i, v in enumerate(self.value):
                    params[f"{param_prefix}_{i}"] = v
                return f"{col} NOT IN ({placeholders})", params
            return f"{col} NOT IN (:{param_prefix}_val)", {f"{param_prefix}_val": self.value}

        elif self.operator == FilterOperator.LIKE:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} LIKE :{param_prefix}_val", params

        elif self.operator == FilterOperator.NOT_LIKE:
            params[f"{param_prefix}_val"] = self.value
            return f"{col} NOT LIKE :{param_prefix}_val", params

        elif self.operator == FilterOperator.IS_NULL:
            return f"{col} IS NULL", {}

        elif self.operator == FilterOperator.IS_NOT_NULL:
            return f"{col} IS NOT NULL", {}

        elif self.operator == FilterOperator.BETWEEN:
            params[f"{param_prefix}_val1"] = self.value
            params[f"{param_prefix}_val2"] = self.value2
            return f"{col} BETWEEN :{param_prefix}_val1 AND :{param_prefix}_val2", params

        else:
            raise ValueError(f"Unsupported operator: {self.operator}")

    def evaluate(self, row_value: Any) -> bool:
        """
        Evaluate filter against a value (for in-memory filtering).

        Args:
            row_value: Value to evaluate

        Returns:
            True if filter matches
        """
        if self.operator == FilterOperator.EQUALS:
            return row_value == self.value

        elif self.operator == FilterOperator.NOT_EQUALS:
            return row_value != self.value

        elif self.operator == FilterOperator.GREATER_THAN:
            return row_value > self.value

        elif self.operator == FilterOperator.GREATER_THAN_OR_EQUALS:
            return row_value >= self.value

        elif self.operator == FilterOperator.LESS_THAN:
            return row_value < self.value

        elif self.operator == FilterOperator.LESS_THAN_OR_EQUALS:
            return row_value <= self.value

        elif self.operator == FilterOperator.IN:
            return row_value in self.value

        elif self.operator == FilterOperator.NOT_IN:
            return row_value not in self.value

        elif self.operator == FilterOperator.LIKE:
            pattern = self.value.replace("%", ".*").replace("_", ".")
            return bool(re.match(pattern, str(row_value), re.IGNORECASE))

        elif self.operator == FilterOperator.NOT_LIKE:
            pattern = self.value.replace("%", ".*").replace("_", ".")
            return not bool(re.match(pattern, str(row_value), re.IGNORECASE))

        elif self.operator == FilterOperator.IS_NULL:
            return row_value is None

        elif self.operator == FilterOperator.IS_NOT_NULL:
            return row_value is not None

        elif self.operator == FilterOperator.BETWEEN:
            return self.value <= row_value <= self.value2

        elif self.operator == FilterOperator.REGEX:
            return bool(re.match(self.value, str(row_value)))

        return False


@dataclass
class TableFilter:
    """Filter configuration for a table."""

    table_name: str
    column_filters: list[ColumnFilter] = field(default_factory=list)
    exclude_columns: list[str] = field(default_factory=list)
    include_columns: Optional[list[str]] = None
    row_limit: Optional[int] = None
    order_by: Optional[list[str]] = None

    def add_filter(
        self,
        column: str,
        operator: FilterOperator,
        value: Any = None,
        value2: Any = None,
    ) -> "TableFilter":
        """Add a column filter."""
        self.column_filters.append(
            ColumnFilter(column, operator, value, value2)
        )
        return self

    def exclude_column(self, column: str) -> "TableFilter":
        """Exclude a column from comparison."""
        self.exclude_columns.append(column)
        return self

    def to_where_clause(self) -> tuple[str, dict]:
        """
        Generate SQL WHERE clause from filters.

        Returns:
            Tuple of (WHERE clause without 'WHERE', parameters dict)
        """
        if not self.column_filters:
            return "", {}

        clauses = []
        params = {}

        for i, filter in enumerate(self.column_filters):
            clause, filter_params = filter.to_sql(f"p{i}")
            clauses.append(clause)
            params.update(filter_params)

        return " AND ".join(clauses), params

    def filter_columns(self, columns: list[str]) -> list[str]:
        """
        Apply column inclusion/exclusion rules.

        Args:
            columns: List of column names

        Returns:
            Filtered list of column names
        """
        if self.include_columns:
            columns = [c for c in columns if c in self.include_columns]

        if self.exclude_columns:
            columns = [c for c in columns if c not in self.exclude_columns]

        return columns

    def filter_row(self, row: dict) -> bool:
        """
        Check if row passes all filters.

        Args:
            row: Row data as dictionary

        Returns:
            True if row passes all filters
        """
        for filter in self.column_filters:
            if filter.column_name in row:
                if not filter.evaluate(row[filter.column_name]):
                    return False
        return True


@dataclass
class ComparisonFilter:
    """Global filter configuration for comparison."""

    table_filters: dict[str, TableFilter] = field(default_factory=dict)
    global_exclude_columns: list[str] = field(default_factory=list)
    table_name_pattern: Optional[str] = None
    exclude_empty_tables: bool = False
    min_row_count: int = 0
    max_row_count: Optional[int] = None

    def add_table_filter(self, table_filter: TableFilter) -> "ComparisonFilter":
        """Add a table-specific filter."""
        self.table_filters[table_filter.table_name] = table_filter
        return self

    def get_table_filter(self, table_name: str) -> Optional[TableFilter]:
        """Get filter for a specific table."""
        return self.table_filters.get(table_name)

    def should_compare_table(self, table_name: str, row_count: int = 0) -> bool:
        """
        Check if a table should be compared based on filters.

        Args:
            table_name: Table name
            row_count: Number of rows in table

        Returns:
            True if table should be compared
        """
        # Check name pattern
        if self.table_name_pattern:
            if not re.match(self.table_name_pattern, table_name, re.IGNORECASE):
                return False

        # Check empty tables
        if self.exclude_empty_tables and row_count == 0:
            return False

        # Check row count limits
        if row_count < self.min_row_count:
            return False

        if self.max_row_count and row_count > self.max_row_count:
            return False

        return True

    def filter_columns(self, table_name: str, columns: list[str]) -> list[str]:
        """
        Filter columns for a table.

        Args:
            table_name: Table name
            columns: List of column names

        Returns:
            Filtered list of column names
        """
        # Apply global exclusions first
        columns = [c for c in columns if c not in self.global_exclude_columns]

        # Apply table-specific filter if exists
        table_filter = self.get_table_filter(table_name)
        if table_filter:
            columns = table_filter.filter_columns(columns)

        return columns


# Pre-built filter factories
def date_range_filter(
    column: str,
    start_date: Union[datetime, date, str],
    end_date: Union[datetime, date, str],
) -> ColumnFilter:
    """Create a date range filter."""
    return ColumnFilter(column, FilterOperator.BETWEEN, start_date, end_date)


def exclude_nulls_filter(column: str) -> ColumnFilter:
    """Create a filter to exclude NULL values."""
    return ColumnFilter(column, FilterOperator.IS_NOT_NULL)


def exact_match_filter(column: str, value: Any) -> ColumnFilter:
    """Create an exact match filter."""
    return ColumnFilter(column, FilterOperator.EQUALS, value)


def pattern_filter(column: str, pattern: str) -> ColumnFilter:
    """Create a LIKE pattern filter."""
    return ColumnFilter(column, FilterOperator.LIKE, pattern)


def in_list_filter(column: str, values: list) -> ColumnFilter:
    """Create an IN list filter."""
    return ColumnFilter(column, FilterOperator.IN, values)
