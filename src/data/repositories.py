"""Repository classes for data access."""

from typing import Any, Generator, Optional

import pandas as pd

from src.core.exceptions import DatabaseError
from src.core.logging import get_logger
from src.data.database import DatabaseConnection
from src.data.models import (
    ColumnInfo,
    CompressionAnalysis,
    CompressionType,
    IndexInfo,
    TableInfo,
)

logger = get_logger(__name__)


class MetadataRepository:
    """Repository for database metadata operations."""

    def __init__(self, connection: DatabaseConnection) -> None:
        """
        Initialize metadata repository.

        Args:
            connection: Database connection
        """
        self.connection = connection

    def get_tables(self, schema_filter: Optional[str] = None) -> list[TableInfo]:
        """
        Get list of tables from the database.

        Args:
            schema_filter: Optional schema name filter

        Returns:
            List of table information
        """
        query = """
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                SUM(p.rows) AS row_count,
                SUM(a.total_pages) * 8 AS total_size_kb,
                SUM(a.used_pages) * 8 AS data_size_kb,
                (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS index_size_kb,
                ISNULL(p.data_compression_desc, 'NONE') AS compression_type
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.indexes i ON t.object_id = i.object_id
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.is_ms_shipped = 0
                AND i.index_id <= 1
        """

        if schema_filter:
            query += " AND s.name = ?"
            params = [schema_filter]
        else:
            params = []

        query += """
            GROUP BY s.name, t.name, p.data_compression_desc
            ORDER BY s.name, t.name
        """

        try:
            results = self.connection.execute_query(
                query, params if params else None
            )

            tables = []
            for row in results:
                table_info = TableInfo(
                    schema_name=row["schema_name"],
                    table_name=row["table_name"],
                    row_count=row["row_count"] or 0,
                    data_size_kb=float(row["data_size_kb"] or 0),
                    index_size_kb=float(row["index_size_kb"] or 0),
                    total_size_kb=float(row["total_size_kb"] or 0),
                    compression_type=row["compression_type"] or "NONE",
                )
                tables.append(table_info)

            logger.info(f"Retrieved {len(tables)} tables")
            return tables

        except Exception as e:
            logger.error(f"Failed to retrieve tables: {str(e)}")
            raise DatabaseError(f"Failed to retrieve tables: {str(e)}") from e

    def get_table_columns(
        self, schema_name: str, table_name: str
    ) -> list[ColumnInfo]:
        """
        Get column information for a table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of column information
        """
        query = """
            SELECT
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                c.is_computed,
                dc.definition AS default_value,
                c.column_id AS ordinal_position
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tb ON c.object_id = tb.object_id
            INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            WHERE s.name = ?
                AND tb.name = ?
            ORDER BY c.column_id
        """

        try:
            results = self.connection.execute_query(
                query, [schema_name, table_name]
            )

            columns = []
            for row in results:
                column_info = ColumnInfo(
                    column_name=row["column_name"],
                    data_type=row["data_type"],
                    max_length=row["max_length"],
                    precision=row["precision"],
                    scale=row["scale"],
                    is_nullable=bool(row["is_nullable"]),
                    is_identity=bool(row["is_identity"]),
                    is_computed=bool(row["is_computed"]),
                    default_value=row["default_value"],
                    ordinal_position=row["ordinal_position"],
                )
                columns.append(column_info)

            return columns

        except Exception as e:
            logger.error(
                f"Failed to retrieve columns for {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to retrieve columns: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e

    def get_table_indexes(
        self, schema_name: str, table_name: str
    ) -> list[IndexInfo]:
        """
        Get index information for a table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of index information
        """
        query = """
            SELECT
                i.name AS index_name,
                i.type_desc AS index_type,
                i.is_unique,
                i.is_primary_key,
                i.filter_definition,
                STRING_AGG(
                    CASE WHEN ic.is_included_column = 0 THEN c.name END,
                    ', '
                ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns,
                STRING_AGG(
                    CASE WHEN ic.is_included_column = 1 THEN c.name END,
                    ', '
                ) AS included_columns
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id
                AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id
                AND ic.column_id = c.column_id
            WHERE s.name = ?
                AND t.name = ?
                AND i.type > 0
            GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key, i.filter_definition
            ORDER BY i.is_primary_key DESC, i.name
        """

        try:
            results = self.connection.execute_query(
                query, [schema_name, table_name]
            )

            indexes = []
            for row in results:
                key_cols = (
                    row["key_columns"].split(", ") if row["key_columns"] else []
                )
                inc_cols = (
                    row["included_columns"].split(", ")
                    if row["included_columns"]
                    else []
                )

                index_info = IndexInfo(
                    index_name=row["index_name"],
                    index_type=row["index_type"],
                    is_unique=bool(row["is_unique"]),
                    is_primary_key=bool(row["is_primary_key"]),
                    columns=key_cols,
                    included_columns=inc_cols,
                    filter_definition=row["filter_definition"],
                )
                indexes.append(index_info)

            return indexes

        except Exception as e:
            logger.error(
                f"Failed to retrieve indexes for {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to retrieve indexes: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e

    def get_primary_key_columns(
        self, schema_name: str, table_name: str
    ) -> list[str]:
        """
        Get primary key columns for a table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of primary key column names
        """
        query = """
            SELECT c.name AS column_name
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id
                AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id
                AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE i.is_primary_key = 1
                AND s.name = ?
                AND t.name = ?
            ORDER BY ic.key_ordinal
        """

        try:
            results = self.connection.execute_query(
                query, [schema_name, table_name]
            )
            return [row["column_name"] for row in results]

        except Exception as e:
            logger.error(
                f"Failed to retrieve primary key for {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to retrieve primary key: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e

    def get_table_info(
        self, schema_name: str, table_name: str, include_metadata: bool = True
    ) -> TableInfo:
        """
        Get complete table information.

        Args:
            schema_name: Schema name
            table_name: Table name
            include_metadata: Whether to include columns and indexes

        Returns:
            Complete table information
        """
        tables = self.get_tables(schema_filter=schema_name)
        table_info = next(
            (t for t in tables if t.table_name == table_name), None
        )

        if not table_info:
            raise DatabaseError(
                f"Table {schema_name}.{table_name} not found",
                table=f"{schema_name}.{table_name}",
            )

        if include_metadata:
            table_info.columns = self.get_table_columns(schema_name, table_name)
            table_info.indexes = self.get_table_indexes(schema_name, table_name)
            table_info.primary_key_columns = self.get_primary_key_columns(
                schema_name, table_name
            )

        return table_info


class TableDataRepository:
    """Repository for table data operations."""

    def __init__(self, connection: DatabaseConnection) -> None:
        """
        Initialize table data repository.

        Args:
            connection: Database connection
        """
        self.connection = connection

    def get_row_count(self, schema_name: str, table_name: str) -> int:
        """
        Get row count for a table.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"
        try:
            return int(self.connection.execute_scalar(query) or 0)
        except Exception as e:
            logger.error(
                f"Failed to get row count for {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to get row count: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e

    def get_data_chunked(
        self,
        schema_name: str,
        table_name: str,
        chunk_size: int = 10000,
        order_by: Optional[list[str]] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Get table data in chunks.

        Args:
            schema_name: Schema name
            table_name: Table name
            chunk_size: Number of rows per chunk
            order_by: Optional list of columns to order by

        Yields:
            DataFrame chunks
        """
        query = f"SELECT * FROM [{schema_name}].[{table_name}]"

        if order_by:
            order_clause = ", ".join([f"[{col}]" for col in order_by])
            query += f" ORDER BY {order_clause}"

        try:
            with self.connection.get_connection() as conn:
                for chunk in pd.read_sql_query(
                    query, conn, chunksize=chunk_size
                ):
                    yield chunk

        except Exception as e:
            logger.error(
                f"Failed to read data from {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to read data: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e

    def get_data(
        self,
        schema_name: str,
        table_name: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        order_by: Optional[list[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get table data with optional filtering.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: Optional list of columns to select
            where: Optional WHERE clause
            order_by: Optional list of columns to order by
            limit: Optional row limit

        Returns:
            DataFrame with table data
        """
        # Build query
        col_clause = (
            ", ".join([f"[{col}]" for col in columns])
            if columns
            else "*"
        )
        query = f"SELECT {col_clause} FROM [{schema_name}].[{table_name}]"

        if where:
            query += f" WHERE {where}"

        if order_by:
            order_clause = ", ".join([f"[{col}]" for col in order_by])
            query += f" ORDER BY {order_clause}"

        if limit:
            if not order_by:
                # Need ORDER BY for TOP to be deterministic
                query = query.replace("SELECT", f"SELECT TOP {limit}")
            else:
                query += f" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"

        try:
            with self.connection.get_connection() as conn:
                return pd.read_sql_query(query, conn)

        except Exception as e:
            logger.error(
                f"Failed to read data from {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to read data: {str(e)}",
                query=query,
                table=f"{schema_name}.{table_name}",
            ) from e

    def get_checksum(
        self, schema_name: str, table_name: str, columns: list[str]
    ) -> int:
        """
        Calculate checksum for table data.

        Args:
            schema_name: Schema name
            table_name: Table name
            columns: Columns to include in checksum

        Returns:
            Checksum value
        """
        col_clause = ", ".join([f"[{col}]" for col in columns])
        query = f"""
            SELECT CHECKSUM_AGG(BINARY_CHECKSUM({col_clause}))
            FROM [{schema_name}].[{table_name}]
        """

        try:
            return int(self.connection.execute_scalar(query) or 0)

        except Exception as e:
            logger.error(
                f"Failed to calculate checksum for {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to calculate checksum: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e


class CompressionRepository:
    """Repository for compression analysis operations."""

    def __init__(self, connection: DatabaseConnection) -> None:
        """
        Initialize compression repository.

        Args:
            connection: Database connection
        """
        self.connection = connection

    def estimate_compression(
        self,
        schema_name: str,
        table_name: str,
        compression_types: Optional[list[str]] = None,
    ) -> CompressionAnalysis:
        """
        Estimate compression for a table.

        Args:
            schema_name: Schema name
            table_name: Table name
            compression_types: Optional list of compression types to estimate

        Returns:
            Compression analysis results
        """
        if compression_types is None:
            compression_types = ["NONE", "ROW", "PAGE"]

        # Get current state
        current_query = """
            SELECT
                p.data_compression_desc,
                SUM(a.total_pages) * 8 AS size_kb,
                SUM(p.rows) AS row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE s.name = ?
                AND t.name = ?
                AND p.index_id <= 1
            GROUP BY p.data_compression_desc
        """

        try:
            result = self.connection.execute_query(
                current_query, [schema_name, table_name]
            )

            if not result:
                raise DatabaseError(
                    f"Table {schema_name}.{table_name} not found",
                    table=f"{schema_name}.{table_name}",
                )

            current_row = result[0]
            current_compression = CompressionType[
                current_row["data_compression_desc"]
            ]
            current_size = float(current_row["size_kb"])
            row_count = int(current_row["row_count"])

            analysis = CompressionAnalysis(
                table_name=f"{schema_name}.{table_name}",
                current_compression=current_compression,
                current_size_kb=current_size,
                row_count=row_count,
            )

            # Estimate for each compression type
            for comp_type in compression_types:
                estimate_query = f"""
                    EXEC sp_estimate_data_compression_savings
                        @schema_name = ?,
                        @object_name = ?,
                        @index_id = NULL,
                        @partition_number = NULL,
                        @data_compression = ?
                """

                estimate_result = self.connection.execute_query(
                    estimate_query, [schema_name, table_name, comp_type]
                )

                if estimate_result:
                    size_kb = float(
                        estimate_result[0].get("size_with_requested_compression_setting(KB)", 0)
                    )

                    if comp_type == "NONE":
                        analysis.none_size_kb = size_kb
                    elif comp_type == "ROW":
                        analysis.row_size_kb = size_kb
                    elif comp_type == "PAGE":
                        analysis.page_size_kb = size_kb
                    elif comp_type == "COLUMNSTORE":
                        analysis.columnstore_size_kb = size_kb

            return analysis

        except Exception as e:
            logger.error(
                f"Failed to estimate compression for {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to estimate compression: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e

    def apply_compression(
        self,
        schema_name: str,
        table_name: str,
        compression_type: CompressionType,
        rebuild_index: bool = True,
    ) -> None:
        """
        Apply compression to a table.

        Args:
            schema_name: Schema name
            table_name: Table name
            compression_type: Compression type to apply
            rebuild_index: Whether to rebuild indexes

        Raises:
            DatabaseError: If compression application fails
        """
        try:
            compression_value = compression_type.value

            if rebuild_index:
                query = f"""
                    ALTER TABLE [{schema_name}].[{table_name}]
                    REBUILD WITH (DATA_COMPRESSION = {compression_value})
                """
            else:
                query = f"""
                    ALTER TABLE [{schema_name}].[{table_name}]
                    WITH (DATA_COMPRESSION = {compression_value})
                """

            self.connection.execute_query(query)

            logger.info(
                f"Applied {compression_value} compression to {schema_name}.{table_name}"
            )

        except Exception as e:
            logger.error(
                f"Failed to apply compression to {schema_name}.{table_name}: {str(e)}"
            )
            raise DatabaseError(
                f"Failed to apply compression: {str(e)}",
                table=f"{schema_name}.{table_name}",
            ) from e
