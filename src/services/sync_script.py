"""Sync script generator for data synchronization."""

from typing import Optional

import pandas as pd

from src.core.logging import get_logger
from src.data.models import ComparisonResult, DifferenceType

logger = get_logger(__name__)


class SyncScriptGenerator:
    """Generate SQL sync scripts from comparison results."""

    def __init__(self) -> None:
        """Initialize sync script generator."""
        pass

    def generate_sync_script(
        self,
        result: ComparisonResult,
        source_data: Optional[pd.DataFrame] = None,
        target_data: Optional[pd.DataFrame] = None,
        use_merge: bool = True,
    ) -> str:
        """
        Generate SQL script to synchronize target with source.

        Args:
            result: Comparison result
            source_data: Optional source data for generating INSERT/UPDATE values
            target_data: Optional target data
            use_merge: Use MERGE statement instead of separate INSERT/UPDATE/DELETE

        Returns:
            SQL sync script
        """
        logger.info(f"Generating sync script for {result.source_table}")

        script_parts = []

        # Header
        script_parts.append(
            f"-- Sync Script for {result.source_table} -> {result.target_table}"
        )
        script_parts.append(
            f"-- Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        script_parts.append(
            f"-- Source Rows: {result.source_row_count}, Target Rows: {result.target_row_count}"
        )
        script_parts.append(
            f"-- Differences: {result.different_rows} data diffs, {result.source_only_rows} source-only, {result.target_only_rows} target-only"
        )
        script_parts.append("")

        # Schema differences warning
        if not result.schema_match:
            script_parts.append("-- WARNING: Schema differences detected!")
            script_parts.append(
                "-- Please resolve schema differences before running this script:"
            )
            for diff in result.schema_differences:
                script_parts.append(f"--   {diff.description}")
            script_parts.append("")

        # Generate sync statements
        if use_merge and source_data is not None:
            merge_script = self._generate_merge_statement(
                result, source_data
            )
            script_parts.append(merge_script)
        else:
            # Generate separate statements
            if result.target_only_rows > 0:
                delete_script = self._generate_delete_statements(result)
                script_parts.append(delete_script)
                script_parts.append("")

            if result.source_only_rows > 0 and source_data is not None:
                insert_script = self._generate_insert_statements(
                    result, source_data
                )
                script_parts.append(insert_script)
                script_parts.append("")

            if result.different_rows > 0 and source_data is not None:
                update_script = self._generate_update_statements(
                    result, source_data
                )
                script_parts.append(update_script)

        # Footer
        script_parts.append("")
        script_parts.append("-- End of sync script")

        return "\n".join(script_parts)

    def _generate_merge_statement(
        self,
        result: ComparisonResult,
        source_data: pd.DataFrame,
    ) -> str:
        """Generate MERGE statement."""
        target_parts = result.target_table.split(".")
        if len(target_parts) == 2:
            schema, table = target_parts
        else:
            schema, table = "dbo", target_parts[0]

        # Get columns
        columns = list(source_data.columns)

        # Build MERGE statement
        script = f"""
BEGIN TRANSACTION;

MERGE INTO [{schema}].[{table}] AS Target
USING (
    -- Source data would be inserted here from {result.source_table}
    SELECT * FROM {result.source_table}
) AS Source
ON (
    -- Add primary key join conditions here
    1=1  -- Replace with actual PK comparison
)
WHEN MATCHED THEN
    UPDATE SET
        {', '.join([f'[{col}] = Source.[{col}]' for col in columns])}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({', '.join([f'[{col}]' for col in columns])})
    VALUES ({', '.join([f'Source.[{col}]' for col in columns])})
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;

COMMIT TRANSACTION;
"""
        return script

    def _generate_delete_statements(
        self, result: ComparisonResult
    ) -> str:
        """Generate DELETE statements for target-only rows."""
        target_parts = result.target_table.split(".")
        if len(target_parts) == 2:
            schema, table = target_parts
        else:
            schema, table = "dbo", target_parts[0]

        script = f"""
-- Delete {result.target_only_rows} rows that exist only in target
-- DELETE FROM [{schema}].[{table}]
-- WHERE <primary_key_conditions>;
"""
        return script

    def _generate_insert_statements(
        self,
        result: ComparisonResult,
        source_data: pd.DataFrame,
    ) -> str:
        """Generate INSERT statements for source-only rows."""
        target_parts = result.target_table.split(".")
        if len(target_parts) == 2:
            schema, table = target_parts
        else:
            schema, table = "dbo", target_parts[0]

        columns = list(source_data.columns)

        script = f"""
-- Insert {result.source_only_rows} rows that exist only in source
-- INSERT INTO [{schema}].[{table}] ({', '.join([f'[{col}]' for col in columns])})
-- SELECT {', '.join([f'[{col}]' for col in columns])}
-- FROM {result.source_table}
-- WHERE <conditions_for_source_only_rows>;
"""
        return script

    def _generate_update_statements(
        self,
        result: ComparisonResult,
        source_data: pd.DataFrame,
    ) -> str:
        """Generate UPDATE statements for different rows."""
        target_parts = result.target_table.split(".")
        if len(target_parts) == 2:
            schema, table = target_parts
        else:
            schema, table = "dbo", target_parts[0]

        script = f"""
-- Update {result.different_rows} rows with differences
-- UPDATE Target
-- SET <column_assignments>
-- FROM [{schema}].[{table}] Target
-- INNER JOIN {result.source_table} Source
-- ON <primary_key_join>
-- WHERE <difference_conditions>;
"""
        return script

    def generate_schema_sync_script(
        self, result: ComparisonResult
    ) -> Optional[str]:
        """
        Generate SQL script to synchronize schema differences.

        Args:
            result: Comparison result with schema differences

        Returns:
            SQL script or None if no schema differences
        """
        if result.schema_match:
            return None

        logger.info(f"Generating schema sync script for {result.source_table}")

        script_parts = []
        script_parts.append(
            f"-- Schema Sync Script for {result.target_table}"
        )
        script_parts.append(
            f"-- Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        script_parts.append("")

        target_parts = result.target_table.split(".")
        if len(target_parts) == 2:
            schema, table = target_parts
        else:
            schema, table = "dbo", target_parts[0]

        for diff in result.schema_differences:
            if diff.difference_type == DifferenceType.SCHEMA_ONLY_SOURCE:
                # Add missing column
                if diff.column_name:
                    script_parts.append(
                        f"-- Add missing column: {diff.column_name}"
                    )
                    script_parts.append(
                        f"ALTER TABLE [{schema}].[{table}] "
                        f"ADD [{diff.column_name}] {diff.source_value};"
                    )
                else:
                    # Missing table
                    script_parts.append(
                        f"-- Table missing in target: {diff.table_name}"
                    )
                    script_parts.append(
                        f"-- Create table script needed"
                    )

            elif diff.difference_type == DifferenceType.SCHEMA_ONLY_TARGET:
                # Remove extra column
                if diff.column_name:
                    script_parts.append(
                        f"-- Remove extra column: {diff.column_name}"
                    )
                    script_parts.append(
                        f"-- ALTER TABLE [{schema}].[{table}] "
                        f"DROP COLUMN [{diff.column_name}];"
                    )

            elif diff.difference_type == DifferenceType.SCHEMA_DIFFERENT:
                # Modify column
                if diff.column_name:
                    script_parts.append(
                        f"-- Modify column: {diff.column_name}"
                    )
                    script_parts.append(
                        f"-- Source: {diff.source_value}, Target: {diff.target_value}"
                    )
                    script_parts.append(
                        f"ALTER TABLE [{schema}].[{table}] "
                        f"ALTER COLUMN [{diff.column_name}] {diff.source_value};"
                    )

            script_parts.append("")

        return "\n".join(script_parts)
