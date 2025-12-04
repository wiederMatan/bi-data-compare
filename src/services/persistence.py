"""Result persistence service using SQLite."""

import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Optional

from src.core.config import get_settings
from src.core.logging import get_logger
from src.data.models import ComparisonMode, ComparisonResult

logger = get_logger(__name__)


class ResultPersistenceService:
    """Service for persisting comparison results to SQLite database."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        """
        Initialize persistence service.

        Args:
            db_path: Path to SQLite database file (default: config/results.db)
        """
        if db_path is None:
            settings = get_settings()
            config_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "config"
            )
            os.makedirs(config_dir, exist_ok=True)
            db_path = os.path.join(config_dir, "results.db")

        self.db_path = db_path
        self._local = threading.local()
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, "connection"):
            self._local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection

    def _init_database(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Create comparison_runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comparison_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT UNIQUE NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                source_server TEXT,
                source_database TEXT,
                target_server TEXT,
                target_database TEXT,
                schema_name TEXT,
                total_tables INTEGER DEFAULT 0,
                matching_tables INTEGER DEFAULT 0,
                different_tables INTEGER DEFAULT 0,
                failed_tables INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create comparison_results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comparison_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source_table TEXT NOT NULL,
                target_table TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                duration_seconds REAL,
                status TEXT NOT NULL,
                source_row_count INTEGER DEFAULT 0,
                target_row_count INTEGER DEFAULT 0,
                matching_rows INTEGER DEFAULT 0,
                different_rows INTEGER DEFAULT 0,
                source_only_rows INTEGER DEFAULT 0,
                target_only_rows INTEGER DEFAULT 0,
                schema_match INTEGER DEFAULT 1,
                schema_differences TEXT,
                data_differences TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES comparison_runs(run_id)
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_results_run_id
            ON comparison_results(run_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_started_at
            ON comparison_runs(started_at)
        """)

        conn.commit()
        logger.info(f"Database initialized at {self.db_path}")

    def create_run(
        self,
        run_id: str,
        source_server: str,
        source_database: str,
        target_server: str,
        target_database: str,
        schema_name: str,
    ) -> str:
        """
        Create a new comparison run.

        Args:
            run_id: Unique run identifier
            source_server: Source server name
            source_database: Source database name
            target_server: Target server name
            target_database: Target database name
            schema_name: Schema being compared

        Returns:
            Run ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO comparison_runs
            (run_id, started_at, source_server, source_database,
             target_server, target_database, schema_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                datetime.now().isoformat(),
                source_server,
                source_database,
                target_server,
                target_database,
                schema_name,
            ),
        )
        conn.commit()
        logger.info(f"Created comparison run: {run_id}")
        return run_id

    def save_result(self, run_id: str, result: ComparisonResult) -> int:
        """
        Save a comparison result.

        Args:
            run_id: Run ID to associate result with
            result: Comparison result to save

        Returns:
            Result ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Serialize differences to JSON
        schema_diffs_json = json.dumps(
            [
                {
                    "table_name": d.table_name,
                    "difference_type": d.difference_type.value,
                    "column_name": d.column_name,
                    "source_value": d.source_value,
                    "target_value": d.target_value,
                    "description": d.description,
                }
                for d in result.schema_differences
            ]
        ) if result.schema_differences else "[]"

        data_diffs_json = json.dumps(
            [
                {
                    "table_name": d.table_name,
                    "primary_key_values": d.primary_key_values,
                    "difference_type": d.difference_type.value,
                    "column_name": d.column_name,
                    "source_value": str(d.source_value) if d.source_value is not None else None,
                    "target_value": str(d.target_value) if d.target_value is not None else None,
                }
                for d in result.data_differences[:1000]  # Limit to 1000 differences
            ]
        ) if result.data_differences else "[]"

        cursor.execute(
            """
            INSERT INTO comparison_results
            (run_id, source_table, target_table, mode, started_at, completed_at,
             duration_seconds, status, source_row_count, target_row_count,
             matching_rows, different_rows, source_only_rows, target_only_rows,
             schema_match, schema_differences, data_differences, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                result.source_table,
                result.target_table,
                result.mode.value,
                result.started_at.isoformat() if result.started_at else None,
                result.completed_at.isoformat() if result.completed_at else None,
                result.duration_seconds,
                result.status,
                result.source_row_count,
                result.target_row_count,
                result.matching_rows,
                result.different_rows,
                result.source_only_rows,
                result.target_only_rows,
                1 if result.schema_match else 0,
                schema_diffs_json,
                data_diffs_json,
                result.error_message,
            ),
        )
        conn.commit()

        result_id = cursor.lastrowid
        logger.debug(f"Saved result for {result.source_table}: ID={result_id}")
        return result_id

    def complete_run(
        self,
        run_id: str,
        total_tables: int,
        matching_tables: int,
        different_tables: int,
        failed_tables: int,
    ) -> None:
        """
        Mark a comparison run as completed.

        Args:
            run_id: Run ID
            total_tables: Total number of tables compared
            matching_tables: Number of matching tables
            different_tables: Number of different tables
            failed_tables: Number of failed comparisons
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE comparison_runs
            SET completed_at = ?, total_tables = ?, matching_tables = ?,
                different_tables = ?, failed_tables = ?, status = 'completed'
            WHERE run_id = ?
            """,
            (
                datetime.now().isoformat(),
                total_tables,
                matching_tables,
                different_tables,
                failed_tables,
                run_id,
            ),
        )
        conn.commit()
        logger.info(f"Completed run {run_id}: {matching_tables}/{total_tables} matching")

    def get_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get list of comparison runs.

        Args:
            limit: Maximum number of runs to return
            offset: Offset for pagination
            status: Filter by status (optional)

        Returns:
            List of run dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if status:
            cursor.execute(
                """
                SELECT * FROM comparison_runs
                WHERE status = ?
                ORDER BY started_at DESC
                LIMIT ? OFFSET ?
                """,
                (status, limit, offset),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM comparison_runs
                ORDER BY started_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )

        return [dict(row) for row in cursor.fetchall()]

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        """
        Get a specific comparison run.

        Args:
            run_id: Run ID

        Returns:
            Run dictionary or None
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM comparison_runs WHERE run_id = ?",
            (run_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_run_results(self, run_id: str) -> list[dict[str, Any]]:
        """
        Get all results for a comparison run.

        Args:
            run_id: Run ID

        Returns:
            List of result dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM comparison_results
            WHERE run_id = ?
            ORDER BY source_table
            """,
            (run_id,),
        )

        results = []
        for row in cursor.fetchall():
            result = dict(row)
            # Parse JSON fields
            result["schema_differences"] = json.loads(
                result["schema_differences"] or "[]"
            )
            result["data_differences"] = json.loads(
                result["data_differences"] or "[]"
            )
            results.append(result)

        return results

    def delete_run(self, run_id: str) -> bool:
        """
        Delete a comparison run and its results.

        Args:
            run_id: Run ID

        Returns:
            True if deleted, False if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Delete results first
        cursor.execute(
            "DELETE FROM comparison_results WHERE run_id = ?",
            (run_id,),
        )

        # Delete run
        cursor.execute(
            "DELETE FROM comparison_runs WHERE run_id = ?",
            (run_id,),
        )

        deleted = cursor.rowcount > 0
        conn.commit()

        if deleted:
            logger.info(f"Deleted run {run_id}")
        return deleted

    def get_statistics(self) -> dict[str, Any]:
        """
        Get overall statistics.

        Returns:
            Dictionary with statistics
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Total runs
        cursor.execute("SELECT COUNT(*) FROM comparison_runs")
        total_runs = cursor.fetchone()[0]

        # Total tables compared
        cursor.execute("SELECT COUNT(*) FROM comparison_results")
        total_tables = cursor.fetchone()[0]

        # Matching vs different
        cursor.execute("""
            SELECT
                SUM(CASE WHEN matching_rows > 0 AND different_rows = 0
                    AND source_only_rows = 0 AND target_only_rows = 0
                    THEN 1 ELSE 0 END) as matching,
                SUM(CASE WHEN different_rows > 0 OR source_only_rows > 0
                    OR target_only_rows > 0 THEN 1 ELSE 0 END) as different,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM comparison_results
        """)
        row = cursor.fetchone()
        matching = row[0] or 0
        different = row[1] or 0
        failed = row[2] or 0

        # Recent runs
        cursor.execute("""
            SELECT * FROM comparison_runs
            ORDER BY started_at DESC
            LIMIT 5
        """)
        recent_runs = [dict(r) for r in cursor.fetchall()]

        return {
            "total_runs": total_runs,
            "total_tables_compared": total_tables,
            "matching_tables": matching,
            "different_tables": different,
            "failed_tables": failed,
            "recent_runs": recent_runs,
        }

    def cleanup_old_runs(self, days: int = 30) -> int:
        """
        Delete runs older than specified days.

        Args:
            days: Number of days to keep

        Returns:
            Number of runs deleted
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cutoff = datetime.now().isoformat()

        # Get old run IDs
        cursor.execute(
            """
            SELECT run_id FROM comparison_runs
            WHERE datetime(started_at) < datetime(?, '-' || ? || ' days')
            """,
            (cutoff, days),
        )
        old_run_ids = [row[0] for row in cursor.fetchall()]

        if not old_run_ids:
            return 0

        # Delete results
        placeholders = ",".join("?" * len(old_run_ids))
        cursor.execute(
            f"DELETE FROM comparison_results WHERE run_id IN ({placeholders})",
            old_run_ids,
        )

        # Delete runs
        cursor.execute(
            f"DELETE FROM comparison_runs WHERE run_id IN ({placeholders})",
            old_run_ids,
        )

        conn.commit()
        logger.info(f"Cleaned up {len(old_run_ids)} old runs")
        return len(old_run_ids)


# Global instance
_persistence_service: Optional[ResultPersistenceService] = None
_persistence_lock = threading.Lock()


def get_persistence_service() -> ResultPersistenceService:
    """Get global persistence service instance."""
    global _persistence_service
    with _persistence_lock:
        if _persistence_service is None:
            _persistence_service = ResultPersistenceService()
        return _persistence_service
