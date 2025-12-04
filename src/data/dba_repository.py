"""DBA Analysis repository for querying SQL Server DMVs."""

from typing import Any, Optional
from datetime import datetime

from src.core.logging import get_logger
from src.data.database import DatabaseConnection
from src.data.models import (
    ConnectionSource,
    QueryPattern,
    BlockingInfo,
    LockInfo,
)

logger = get_logger(__name__)


class DBARepository:
    """Repository for DBA analysis queries using SQL Server DMVs."""

    def __init__(self, connection: DatabaseConnection) -> None:
        """Initialize repository with database connection."""
        self.connection = connection

    def get_connection_sources(self) -> list[ConnectionSource]:
        """
        Get all unique connection sources with their resource usage.

        Uses sys.dm_exec_sessions, sys.dm_exec_requests, and sys.dm_exec_connections.
        """
        query = """
        SELECT
            COALESCE(s.program_name, 'Unknown') AS program_name,
            COALESCE(s.host_name, 'Unknown') AS host_name,
            COALESCE(s.login_name, 'Unknown') AS login_name,
            COUNT(DISTINCT s.session_id) AS session_count,
            SUM(CASE WHEN r.request_id IS NOT NULL THEN 1 ELSE 0 END) AS active_requests,
            SUM(CASE WHEN r.request_id IS NULL AND s.status = 'sleeping' THEN 1 ELSE 0 END) AS idle_connections,
            SUM(COALESCE(s.cpu_time, 0)) AS total_cpu_ms,
            SUM(COALESCE(s.reads, 0)) AS total_reads,
            SUM(COALESCE(s.writes, 0)) AS total_writes,
            SUM(COALESCE(s.memory_usage, 0) * 8) AS total_memory_kb,
            SUM(CASE WHEN s.open_transaction_count > 0 THEN 1 ELSE 0 END) AS open_transactions,
            MAX(COALESCE(DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0)) AS longest_transaction_seconds,
            SUM(CASE WHEN r.blocking_session_id > 0 THEN 1 ELSE 0 END) AS blocked_count,
            (
                SELECT COUNT(DISTINCT r2.session_id)
                FROM sys.dm_exec_requests r2
                WHERE r2.blocking_session_id IN (
                    SELECT s2.session_id FROM sys.dm_exec_sessions s2
                    WHERE s2.program_name = s.program_name
                    AND COALESCE(s2.host_name, '') = COALESCE(s.host_name, '')
                )
            ) AS blocking_count
        FROM sys.dm_exec_sessions s
        LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
        WHERE s.is_user_process = 1
        GROUP BY s.program_name, s.host_name, s.login_name
        ORDER BY SUM(COALESCE(s.cpu_time, 0)) DESC
        """

        try:
            results = self.connection.execute_query(query)
            sources = []
            for row in results:
                sources.append(ConnectionSource(
                    program_name=row.get('program_name', 'Unknown'),
                    host_name=row.get('host_name', 'Unknown'),
                    login_name=row.get('login_name', 'Unknown'),
                    session_count=row.get('session_count', 0),
                    active_requests=row.get('active_requests', 0),
                    idle_connections=row.get('idle_connections', 0),
                    total_cpu_ms=row.get('total_cpu_ms', 0),
                    total_reads=row.get('total_reads', 0),
                    total_writes=row.get('total_writes', 0),
                    total_memory_kb=row.get('total_memory_kb', 0),
                    open_transactions=row.get('open_transactions', 0),
                    longest_transaction_seconds=row.get('longest_transaction_seconds', 0),
                    blocked_count=row.get('blocked_count', 0),
                    blocking_count=row.get('blocking_count', 0),
                ))
            logger.info(f"Retrieved {len(sources)} connection sources")
            return sources
        except Exception as e:
            logger.error(f"Failed to get connection sources: {e}")
            return []

    def get_query_patterns(self, top_n: int = 50) -> list[QueryPattern]:
        """
        Get top query patterns by resource usage.

        Uses sys.dm_exec_query_stats and sys.dm_exec_sql_text.
        """
        query = f"""
        SELECT TOP {top_n}
            CONVERT(VARCHAR(64), qs.query_hash, 2) AS query_hash,
            SUBSTRING(st.text, 1, 4000) AS query_text,
            COALESCE(
                (SELECT TOP 1 s.program_name
                 FROM sys.dm_exec_sessions s
                 WHERE s.session_id = qs.plan_handle), 'Cached'
            ) AS source_program,
            '' AS source_host,
            qs.execution_count,
            qs.total_worker_time / 1000 AS total_worker_time_ms,
            qs.total_elapsed_time / 1000 AS total_elapsed_time_ms,
            qs.total_logical_reads,
            qs.total_logical_writes,
            qs.total_physical_reads,
            CASE WHEN qs.execution_count > 0
                THEN qs.total_worker_time / 1000.0 / qs.execution_count
                ELSE 0 END AS avg_worker_time_ms,
            CASE WHEN qs.execution_count > 0
                THEN qs.total_elapsed_time / 1000.0 / qs.execution_count
                ELSE 0 END AS avg_elapsed_time_ms,
            CASE WHEN qs.execution_count > 0
                THEN qs.total_logical_reads * 1.0 / qs.execution_count
                ELSE 0 END AS avg_logical_reads,
            qs.last_execution_time,
            COUNT(*) OVER (PARTITION BY qs.query_hash) AS plan_count
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        WHERE st.text IS NOT NULL
            AND st.text NOT LIKE '%sys.dm_%'
            AND st.text NOT LIKE '%INFORMATION_SCHEMA%'
        ORDER BY qs.total_worker_time DESC
        """

        try:
            results = self.connection.execute_query(query)
            patterns = []
            for row in results:
                patterns.append(QueryPattern(
                    query_hash=row.get('query_hash', ''),
                    query_text=row.get('query_text', ''),
                    source_program=row.get('source_program', 'Unknown'),
                    source_host=row.get('source_host', ''),
                    execution_count=row.get('execution_count', 0),
                    total_worker_time_ms=row.get('total_worker_time_ms', 0),
                    total_elapsed_time_ms=row.get('total_elapsed_time_ms', 0),
                    total_logical_reads=row.get('total_logical_reads', 0),
                    total_logical_writes=row.get('total_logical_writes', 0),
                    total_physical_reads=row.get('total_physical_reads', 0),
                    avg_worker_time_ms=row.get('avg_worker_time_ms', 0),
                    avg_elapsed_time_ms=row.get('avg_elapsed_time_ms', 0),
                    avg_logical_reads=row.get('avg_logical_reads', 0),
                    last_execution_time=row.get('last_execution_time'),
                    plan_count=row.get('plan_count', 1),
                ))
            logger.info(f"Retrieved {len(patterns)} query patterns")
            return patterns
        except Exception as e:
            logger.error(f"Failed to get query patterns: {e}")
            return []

    def get_current_blocking(self) -> list[BlockingInfo]:
        """
        Get current blocking chains.

        Uses sys.dm_exec_requests and sys.dm_exec_sessions.
        """
        query = """
        SELECT
            r.blocking_session_id,
            r.session_id AS blocked_session_id,
            COALESCE(bs.program_name, 'Unknown') AS blocking_program,
            COALESCE(bs.host_name, 'Unknown') AS blocking_host,
            COALESCE(s.program_name, 'Unknown') AS blocked_program,
            COALESCE(s.host_name, 'Unknown') AS blocked_host,
            r.wait_type,
            r.wait_time AS wait_time_ms,
            r.wait_resource,
            (SELECT TOP 1 SUBSTRING(text, 1, 500)
             FROM sys.dm_exec_sql_text(br.sql_handle)) AS blocking_query,
            (SELECT TOP 1 SUBSTRING(text, 1, 500)
             FROM sys.dm_exec_sql_text(r.sql_handle)) AS blocked_query
        FROM sys.dm_exec_requests r
        JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        LEFT JOIN sys.dm_exec_sessions bs ON r.blocking_session_id = bs.session_id
        LEFT JOIN sys.dm_exec_requests br ON r.blocking_session_id = br.session_id
        WHERE r.blocking_session_id > 0
        ORDER BY r.wait_time DESC
        """

        try:
            results = self.connection.execute_query(query)
            blocking = []
            for row in results:
                blocking.append(BlockingInfo(
                    blocking_session_id=row.get('blocking_session_id', 0),
                    blocked_session_id=row.get('blocked_session_id', 0),
                    blocking_program=row.get('blocking_program', 'Unknown'),
                    blocking_host=row.get('blocking_host', 'Unknown'),
                    blocked_program=row.get('blocked_program', 'Unknown'),
                    blocked_host=row.get('blocked_host', 'Unknown'),
                    wait_type=row.get('wait_type', ''),
                    wait_time_ms=row.get('wait_time_ms', 0),
                    wait_resource=row.get('wait_resource', ''),
                    blocking_query=row.get('blocking_query'),
                    blocked_query=row.get('blocked_query'),
                ))
            logger.info(f"Found {len(blocking)} blocking chains")
            return blocking
        except Exception as e:
            logger.error(f"Failed to get blocking info: {e}")
            return []

    def get_lock_info(self) -> list[LockInfo]:
        """
        Get current lock information by session.

        Uses sys.dm_tran_locks.
        """
        query = """
        SELECT
            l.request_session_id AS session_id,
            COALESCE(s.program_name, 'Unknown') AS program_name,
            COALESCE(s.host_name, 'Unknown') AS host_name,
            l.resource_type,
            l.request_mode,
            l.request_status,
            l.resource_description,
            COUNT(*) AS lock_count
        FROM sys.dm_tran_locks l
        JOIN sys.dm_exec_sessions s ON l.request_session_id = s.session_id
        WHERE s.is_user_process = 1
            AND l.resource_type != 'DATABASE'
        GROUP BY
            l.request_session_id,
            s.program_name,
            s.host_name,
            l.resource_type,
            l.request_mode,
            l.request_status,
            l.resource_description
        ORDER BY COUNT(*) DESC
        """

        try:
            results = self.connection.execute_query(query)
            locks = []
            for row in results:
                locks.append(LockInfo(
                    session_id=row.get('session_id', 0),
                    program_name=row.get('program_name', 'Unknown'),
                    host_name=row.get('host_name', 'Unknown'),
                    resource_type=row.get('resource_type', ''),
                    request_mode=row.get('request_mode', ''),
                    request_status=row.get('request_status', ''),
                    resource_description=row.get('resource_description', ''),
                    lock_count=row.get('lock_count', 1),
                ))
            logger.info(f"Retrieved {len(locks)} lock records")
            return locks
        except Exception as e:
            logger.error(f"Failed to get lock info: {e}")
            return []

    def get_wait_stats_by_session(self) -> list[dict[str, Any]]:
        """
        Get wait statistics aggregated by program/host.

        Uses sys.dm_exec_session_wait_stats.
        """
        query = """
        SELECT
            COALESCE(s.program_name, 'Unknown') AS program_name,
            COALESCE(s.host_name, 'Unknown') AS host_name,
            ws.wait_type,
            SUM(ws.waiting_tasks_count) AS waiting_tasks_count,
            SUM(ws.wait_time_ms) AS total_wait_time_ms,
            SUM(ws.signal_wait_time_ms) AS total_signal_wait_time_ms
        FROM sys.dm_exec_session_wait_stats ws
        JOIN sys.dm_exec_sessions s ON ws.session_id = s.session_id
        WHERE s.is_user_process = 1
        GROUP BY s.program_name, s.host_name, ws.wait_type
        HAVING SUM(ws.wait_time_ms) > 100
        ORDER BY SUM(ws.wait_time_ms) DESC
        """

        try:
            results = self.connection.execute_query(query)
            logger.info(f"Retrieved {len(results)} wait stat records")
            return results
        except Exception as e:
            logger.error(f"Failed to get wait stats: {e}")
            return []

    def get_session_details(self) -> list[dict[str, Any]]:
        """
        Get detailed session information.

        Uses sys.dm_exec_sessions and sys.dm_exec_requests.
        """
        query = """
        SELECT
            s.session_id,
            s.program_name,
            s.host_name,
            s.login_name,
            s.status,
            s.cpu_time,
            s.memory_usage * 8 AS memory_kb,
            s.reads,
            s.writes,
            s.logical_reads,
            s.open_transaction_count,
            s.last_request_start_time,
            s.last_request_end_time,
            r.status AS request_status,
            r.command,
            r.wait_type,
            r.wait_time,
            r.blocking_session_id,
            SUBSTRING(st.text, 1, 500) AS current_query
        FROM sys.dm_exec_sessions s
        LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
        WHERE s.is_user_process = 1
        ORDER BY s.cpu_time DESC
        """

        try:
            results = self.connection.execute_query(query)
            logger.info(f"Retrieved {len(results)} session details")
            return results
        except Exception as e:
            logger.error(f"Failed to get session details: {e}")
            return []

    def get_expensive_queries_by_source(self, top_n: int = 20) -> list[dict[str, Any]]:
        """
        Get expensive queries grouped by source program.
        """
        query = f"""
        WITH QueryStats AS (
            SELECT
                COALESCE(
                    (SELECT TOP 1 program_name FROM sys.dm_exec_sessions
                     WHERE session_id = @@SPID), 'Cached'
                ) AS program_name,
                CONVERT(VARCHAR(64), qs.query_hash, 2) AS query_hash,
                SUBSTRING(st.text, 1, 2000) AS query_text,
                qs.execution_count,
                qs.total_worker_time / 1000 AS total_cpu_ms,
                qs.total_logical_reads,
                qs.total_elapsed_time / 1000 AS total_elapsed_ms,
                ROW_NUMBER() OVER (
                    ORDER BY qs.total_worker_time DESC
                ) AS cost_rank
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            WHERE st.text IS NOT NULL
        )
        SELECT TOP {top_n} *
        FROM QueryStats
        WHERE cost_rank <= {top_n}
        ORDER BY total_cpu_ms DESC
        """

        try:
            results = self.connection.execute_query(query)
            logger.info(f"Retrieved {len(results)} expensive queries")
            return results
        except Exception as e:
            logger.error(f"Failed to get expensive queries: {e}")
            return []

    def get_connection_pool_stats(self) -> dict[str, Any]:
        """
        Get connection pooling statistics.
        """
        query = """
        SELECT
            COALESCE(program_name, 'Unknown') AS program_name,
            COALESCE(host_name, 'Unknown') AS host_name,
            COUNT(*) AS total_connections,
            SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS idle_connections,
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS active_connections,
            SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS waiting_connections,
            AVG(DATEDIFF(SECOND, login_time, GETDATE())) AS avg_connection_age_seconds,
            MAX(DATEDIFF(SECOND, last_request_end_time, GETDATE())) AS max_idle_time_seconds
        FROM sys.dm_exec_sessions
        WHERE is_user_process = 1
        GROUP BY program_name, host_name
        ORDER BY COUNT(*) DESC
        """

        try:
            results = self.connection.execute_query(query)
            return {
                'pool_stats': results,
                'total_pools': len(results),
            }
        except Exception as e:
            logger.error(f"Failed to get connection pool stats: {e}")
            return {'pool_stats': [], 'total_pools': 0}
