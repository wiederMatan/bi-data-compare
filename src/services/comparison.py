"""Table comparison service."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Generator, Optional

import pandas as pd
from tqdm import tqdm

from src.core.config import get_settings
from src.core.exceptions import ComparisonError
from src.core.logging import get_logger
from src.data.database import DatabaseConnection
from src.data.models import (
    ColumnInfo,
    ComparisonMode,
    ComparisonResult,
    DataDifference,
    DifferenceType,
    SchemaDifference,
    TableInfo,
)
from src.data.repositories import MetadataRepository, TableDataRepository

logger = get_logger(__name__)


class ComparisonService:
    """Service for comparing tables between databases."""

    def __init__(
        self,
        source_connection: DatabaseConnection,
        target_connection: DatabaseConnection,
    ) -> None:
        """
        Initialize comparison service.

        Args:
            source_connection: Source database connection
            target_connection: Target database connection
        """
        self.source_connection = source_connection
        self.target_connection = target_connection
        self.source_metadata = MetadataRepository(source_connection)
        self.target_metadata = MetadataRepository(target_connection)
        self.source_data = TableDataRepository(source_connection)
        self.target_data = TableDataRepository(target_connection)
        self.settings = get_settings()
        # Cache for column metadata to avoid duplicate queries
        self._column_cache: dict[str, list[ColumnInfo]] = {}
        # Thread lock for cache access
        self._cache_lock = threading.Lock()

    def _get_cached_columns(
        self, repo: MetadataRepository, schema: str, table: str, prefix: str
    ) -> list[ColumnInfo]:
        """
        Get columns with caching to avoid duplicate queries (thread-safe).

        Args:
            repo: Metadata repository (source or target)
            schema: Schema name
            table: Table name
            prefix: Cache key prefix ('source' or 'target')

        Returns:
            List of column information
        """
        cache_key = f"{prefix}:{schema}.{table}"
        with self._cache_lock:
            if cache_key not in self._column_cache:
                logger.debug(f"Cache MISS: fetching columns for {cache_key}")
                self._column_cache[cache_key] = repo.get_table_columns(schema, table)
            else:
                logger.debug(f"Cache HIT: using cached columns for {cache_key}")
            return self._column_cache[cache_key]

    def clear_cache(self) -> None:
        """Clear the column metadata cache (thread-safe)."""
        with self._cache_lock:
            self._column_cache.clear()

    def compare_schemas(
        self,
        source_schema: str,
        target_schema: str,
        table_filter: Optional[list[str]] = None,
    ) -> list[SchemaDifference]:
        """
        Compare schemas between source and target databases.

        Args:
            source_schema: Source schema name
            target_schema: Target schema name
            table_filter: Optional list of table names to compare

        Returns:
            List of schema differences
        """
        differences: list[SchemaDifference] = []

        # Get tables from both databases
        source_tables = {
            t.table_name.upper(): t
            for t in self.source_metadata.get_tables(source_schema)
        }
        target_tables = {
            t.table_name.upper(): t
            for t in self.target_metadata.get_tables(target_schema)
        }

        # Filter tables if specified
        if table_filter:
            source_tables = {
                k: v for k, v in source_tables.items() if k in table_filter
            }
            target_tables = {
                k: v for k, v in target_tables.items() if k in table_filter
            }

        # Find tables only in source
        source_only = set(source_tables.keys()) - set(target_tables.keys())
        for table_name in source_only:
            differences.append(
                SchemaDifference(
                    table_name=table_name,
                    difference_type=DifferenceType.SCHEMA_ONLY_SOURCE,
                    description=f"Table exists only in source database",
                )
            )

        # Find tables only in target
        target_only = set(target_tables.keys()) - set(source_tables.keys())
        for table_name in target_only:
            differences.append(
                SchemaDifference(
                    table_name=table_name,
                    difference_type=DifferenceType.SCHEMA_ONLY_TARGET,
                    description=f"Table exists only in target database",
                )
            )

        # Compare common tables
        common_tables = set(source_tables.keys()) & set(target_tables.keys())
        for table_name in common_tables:
            table_diffs = self._compare_table_schema(
                source_schema,
                target_schema,
                table_name,
            )
            differences.extend(table_diffs)

        logger.info(f"Found {len(differences)} schema differences")
        return differences

    def _compare_table_schema(
        self,
        source_schema: str,
        target_schema: str,
        table_name: str,
    ) -> list[SchemaDifference]:
        """
        Compare schema of a single table.

        Args:
            source_schema: Source schema name
            target_schema: Target schema name
            table_name: Table name to compare

        Returns:
            List of schema differences for this table
        """
        differences: list[SchemaDifference] = []

        # Get columns (cached to avoid duplicate queries)
        source_cols = {
            c.column_name.upper(): c
            for c in self._get_cached_columns(
                self.source_metadata, source_schema, table_name, "source"
            )
        }
        target_cols = {
            c.column_name.upper(): c
            for c in self._get_cached_columns(
                self.target_metadata, target_schema, table_name, "target"
            )
        }

        # Columns only in source
        for col_name in set(source_cols.keys()) - set(target_cols.keys()):
            differences.append(
                SchemaDifference(
                    table_name=table_name,
                    difference_type=DifferenceType.SCHEMA_ONLY_SOURCE,
                    column_name=col_name,
                    source_value=source_cols[col_name].get_full_type(),
                    description=f"Column exists only in source",
                )
            )

        # Columns only in target
        for col_name in set(target_cols.keys()) - set(source_cols.keys()):
            differences.append(
                SchemaDifference(
                    table_name=table_name,
                    difference_type=DifferenceType.SCHEMA_ONLY_TARGET,
                    column_name=col_name,
                    target_value=target_cols[col_name].get_full_type(),
                    description=f"Column exists only in target",
                )
            )

        # Compare common columns
        for col_name in set(source_cols.keys()) & set(target_cols.keys()):
            source_col = source_cols[col_name]
            target_col = target_cols[col_name]

            if source_col != target_col:
                differences.append(
                    SchemaDifference(
                        table_name=table_name,
                        difference_type=DifferenceType.SCHEMA_DIFFERENT,
                        column_name=col_name,
                        source_value=source_col.get_full_type(),
                        target_value=target_col.get_full_type(),
                        description=f"Column definition differs",
                    )
                )

        return differences

    def compare_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        mode: ComparisonMode = ComparisonMode.QUICK,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> ComparisonResult:
        """
        Compare a single table between source and target.

        Args:
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
            mode: Comparison mode
            progress_callback: Optional callback for progress updates (current, total)

        Returns:
            Comparison results
        """
        start_time = datetime.now()
        result = ComparisonResult(
            source_table=f"{source_schema}.{source_table}",
            target_table=f"{target_schema}.{target_table}",
            mode=mode,
            started_at=start_time,
            status="running",
        )

        try:
            logger.info(
                f"Comparing {result.source_table} <-> {result.target_table} (mode: {mode.value})"
            )

            # Compare schema
            schema_diffs = self._compare_table_schema(
                source_schema, target_schema, source_table
            )
            result.schema_differences = schema_diffs
            result.schema_match = len(schema_diffs) == 0

            # Get row counts
            result.source_row_count = self.source_data.get_row_count(
                source_schema, source_table
            )
            result.target_row_count = self.target_data.get_row_count(
                target_schema, target_table
            )

            # Compare data using quick checksum mode
            self._compare_quick(result, source_schema, source_table, target_schema, target_table)

            # Calculate metrics
            result.completed_at = datetime.now()
            result.duration_seconds = (
                result.completed_at - start_time
            ).total_seconds()
            if result.duration_seconds > 0:
                total_rows = max(result.source_row_count, result.target_row_count)
                result.rows_per_second = total_rows / result.duration_seconds

            result.status = "completed"
            logger.info(f"Comparison completed: {result.get_summary()}")

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            result.completed_at = datetime.now()
            logger.error(f"Comparison failed: {str(e)}")
            raise ComparisonError(
                f"Failed to compare tables: {str(e)}",
                source_table=result.source_table,
                target_table=result.target_table,
            ) from e

        return result

    def _compare_quick(
        self,
        result: ComparisonResult,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
    ) -> None:
        """
        Quick comparison using checksums.

        Args:
            result: Comparison result to update
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
        """
        # Get common columns for checksum (using cached columns) - case insensitive
        source_cols = {
            c.column_name.upper(): c.column_name
            for c in self._get_cached_columns(
                self.source_metadata, source_schema, source_table, "source"
            )
        }
        target_cols = {
            c.column_name.upper(): c.column_name
            for c in self._get_cached_columns(
                self.target_metadata, target_schema, target_table, "target"
            )
        }
        common_cols = [source_cols[k] for k in source_cols.keys() & target_cols.keys()]

        if not common_cols:
            logger.warning("No common columns for checksum comparison")
            return

        # Calculate checksums
        source_checksum = self.source_data.get_checksum(
            source_schema, source_table, common_cols
        )
        target_checksum = self.target_data.get_checksum(
            target_schema, target_table, common_cols
        )

        if source_checksum == target_checksum:
            result.matching_rows = min(
                result.source_row_count, result.target_row_count
            )
        else:
            result.different_rows = max(
                result.source_row_count, result.target_row_count
            )

    def _compare_full(
        self,
        result: ComparisonResult,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        """
        Full row-by-row comparison.

        Args:
            result: Comparison result to update
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
            progress_callback: Optional progress callback
        """
        # Get primary key columns
        source_pk = self.source_metadata.get_primary_key_columns(
            source_schema, source_table
        )
        target_pk = self.target_metadata.get_primary_key_columns(
            target_schema, target_table
        )

        if not source_pk or not target_pk:
            logger.warning(
                "Table has no primary key - comparison may be inaccurate"
            )
            # Use all columns as key (cached)
            source_pk = [
                c.column_name
                for c in self._get_cached_columns(
                    self.source_metadata, source_schema, source_table, "source"
                )
            ]
            target_pk = source_pk

        # Read data in chunks
        chunk_size = self.settings.chunk_size
        total_rows = max(result.source_row_count, result.target_row_count)
        processed_rows = 0

        # Get common columns (using cached columns) - case insensitive
        source_cols = {
            c.column_name.upper(): c.column_name
            for c in self._get_cached_columns(
                self.source_metadata, source_schema, source_table, "source"
            )
        }
        target_cols = {
            c.column_name.upper(): c.column_name
            for c in self._get_cached_columns(
                self.target_metadata, target_schema, target_table, "target"
            )
        }
        common_cols = [source_cols[k] for k in source_cols.keys() & target_cols.keys()]

        # Compare in chunks
        for source_chunk in self.source_data.get_data_chunked(
            source_schema, source_table, chunk_size, order_by=source_pk
        ):
            # Get corresponding target data
            if source_chunk.empty:
                continue

            # Build where clause for target based on source PK values
            pk_values = source_chunk[source_pk].drop_duplicates()

            target_chunk = self.target_data.get_data(
                target_schema,
                target_table,
                columns=common_cols,
                order_by=target_pk,
            )

            # Compare chunks
            self._compare_chunks(
                result,
                source_chunk,
                target_chunk,
                source_pk,
                common_cols,
                f"{source_schema}.{source_table}",
            )

            processed_rows += len(source_chunk)
            if progress_callback:
                progress_callback(processed_rows, total_rows)

    def _compare_chunks(
        self,
        result: ComparisonResult,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        pk_columns: list[str],
        compare_columns: list[str],
        table_name: str,
    ) -> None:
        """
        Compare two DataFrame chunks.

        Args:
            result: Comparison result to update
            source_df: Source DataFrame
            target_df: Target DataFrame
            pk_columns: Primary key columns
            compare_columns: Columns to compare
            table_name: Table name for differences
        """
        if source_df.empty and target_df.empty:
            return

        # Set index to PK for easier comparison
        if not source_df.empty:
            source_df = source_df.set_index(pk_columns)
        if not target_df.empty:
            target_df = target_df.set_index(pk_columns)

        # Find rows only in source
        if not source_df.empty and not target_df.empty:
            source_only_idx = source_df.index.difference(target_df.index)
            result.source_only_rows += len(source_only_idx)

            # Find rows only in target
            target_only_idx = target_df.index.difference(source_df.index)
            result.target_only_rows += len(target_only_idx)

            # Find common rows
            common_idx = source_df.index.intersection(target_df.index)

            for idx in common_idx:
                source_row = source_df.loc[idx]
                target_row = target_df.loc[idx]

                # Compare each column
                row_matches = True
                for col in compare_columns:
                    if col not in source_row or col not in target_row:
                        continue

                    source_val = source_row[col]
                    target_val = target_row[col]

                    # Handle NaN comparison
                    if pd.isna(source_val) and pd.isna(target_val):
                        continue

                    if source_val != target_val:
                        row_matches = False
                        # Store difference (limit to avoid memory issues)
                        if len(result.data_differences) < 10000:
                            pk_dict = (
                                {pk_columns[0]: idx}
                                if len(pk_columns) == 1
                                else dict(zip(pk_columns, idx))
                            )
                            result.data_differences.append(
                                DataDifference(
                                    table_name=table_name,
                                    primary_key_values=pk_dict,
                                    difference_type=DifferenceType.DATA_DIFFERENT,
                                    column_name=col,
                                    source_value=source_val,
                                    target_value=target_val,
                                )
                            )

                if row_matches:
                    result.matching_rows += 1
                else:
                    result.different_rows += 1
        elif not source_df.empty:
            result.source_only_rows += len(source_df)
        else:
            result.target_only_rows += len(target_df)

    def compare_multiple_tables(
        self,
        source_schema: str,
        target_schema: str,
        table_names: list[str],
        mode: ComparisonMode = ComparisonMode.QUICK,
        max_workers: Optional[int] = None,
        parallel: bool = True,
    ) -> Generator[ComparisonResult, None, None]:
        """
        Compare multiple tables with optional parallel execution.

        Args:
            source_schema: Source schema name
            target_schema: Target schema name
            table_names: List of table names to compare
            mode: Comparison mode
            max_workers: Maximum number of parallel workers (default from settings)
            parallel: Whether to run comparisons in parallel (default True)

        Yields:
            Comparison results for each table
        """
        if max_workers is None:
            max_workers = self.settings.comparison.max_parallel_tables

        # Use parallel execution if enabled and more than one table
        if parallel and len(table_names) > 1 and max_workers > 1:
            yield from self._compare_tables_parallel(
                source_schema, target_schema, table_names, mode, max_workers
            )
        else:
            yield from self._compare_tables_sequential(
                source_schema, target_schema, table_names, mode
            )

    def _compare_tables_sequential(
        self,
        source_schema: str,
        target_schema: str,
        table_names: list[str],
        mode: ComparisonMode,
    ) -> Generator[ComparisonResult, None, None]:
        """
        Compare tables sequentially (one at a time).

        Args:
            source_schema: Source schema name
            target_schema: Target schema name
            table_names: List of table names to compare
            mode: Comparison mode

        Yields:
            Comparison results for each table
        """
        for table_name in table_names:
            try:
                result = self.compare_table(
                    source_schema,
                    table_name,
                    target_schema,
                    table_name,
                    mode,
                )
                yield result
            except Exception as e:
                logger.error(
                    f"Failed to compare table {table_name}: {str(e)}"
                )
                yield ComparisonResult(
                    source_table=f"{source_schema}.{table_name}",
                    target_table=f"{target_schema}.{table_name}",
                    mode=mode,
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                    status="failed",
                    error_message=str(e),
                )

    def _compare_tables_parallel(
        self,
        source_schema: str,
        target_schema: str,
        table_names: list[str],
        mode: ComparisonMode,
        max_workers: int,
    ) -> Generator[ComparisonResult, None, None]:
        """
        Compare tables in parallel using ThreadPoolExecutor.

        Args:
            source_schema: Source schema name
            target_schema: Target schema name
            table_names: List of table names to compare
            mode: Comparison mode
            max_workers: Maximum number of parallel workers

        Yields:
            Comparison results for each table (in completion order)
        """
        logger.info(f"Starting parallel comparison of {len(table_names)} tables with {max_workers} workers")

        def compare_single_table(table_name: str) -> ComparisonResult:
            """Worker function to compare a single table."""
            try:
                return self.compare_table(
                    source_schema,
                    table_name,
                    target_schema,
                    table_name,
                    mode,
                )
            except Exception as e:
                logger.error(f"Failed to compare table {table_name}: {str(e)}")
                return ComparisonResult(
                    source_table=f"{source_schema}.{table_name}",
                    target_table=f"{target_schema}.{table_name}",
                    mode=mode,
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                    status="failed",
                    error_message=str(e),
                )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(compare_single_table, table_name): table_name
                for table_name in table_names
            }

            # Yield results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    yield result
                except Exception as e:
                    logger.error(f"Unexpected error comparing {table_name}: {str(e)}")
                    yield ComparisonResult(
                        source_table=f"{source_schema}.{table_name}",
                        target_table=f"{target_schema}.{table_name}",
                        mode=mode,
                        started_at=datetime.now(),
                        completed_at=datetime.now(),
                        status="failed",
                        error_message=str(e),
                    )
