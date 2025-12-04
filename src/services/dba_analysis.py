"""DBA Analysis service for workload analysis and connection optimization."""

from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from src.core.logging import get_logger
from src.data.database import DatabaseConnection
from src.data.dba_repository import DBARepository
from src.data.models import (
    ConnectionSource,
    QueryPattern,
    BlockingInfo,
    SystemScorecard,
    RedundancyFinding,
    DBAAnalysisResult,
)

logger = get_logger(__name__)


class DBAAnalysisService:
    """Service for comprehensive DBA workload analysis."""

    def __init__(self, connection: DatabaseConnection) -> None:
        """Initialize service with database connection."""
        self.connection = connection
        self.repository = DBARepository(connection)

    def analyze(self) -> DBAAnalysisResult:
        """
        Perform comprehensive DBA analysis.

        Returns:
            DBAAnalysisResult with all findings
        """
        logger.info("Starting DBA analysis...")
        result = DBAAnalysisResult(
            analyzed_at=datetime.now(),
            server_name=self.connection.connection_info.server,
            database_name=self.connection.connection_info.database,
        )

        # 1. Connection Analysis
        logger.info("Analyzing connections...")
        result.connection_sources = self.repository.get_connection_sources()
        self._analyze_connections(result)

        # 2. Query Pattern Analysis
        logger.info("Analyzing query patterns...")
        result.query_patterns = self.repository.get_query_patterns(top_n=100)
        result.top_expensive_queries = self._get_top_expensive_queries(result.query_patterns)

        # 3. Blocking Analysis
        logger.info("Analyzing blocking...")
        result.current_blocking = self.repository.get_current_blocking()
        result.blocking_hotspots = self._identify_blocking_hotspots(result.current_blocking)

        # 4. Build System Scorecards
        logger.info("Building system scorecards...")
        result.system_scorecards = self._build_scorecards(result)

        # 5. Find Redundancies
        logger.info("Finding redundancies...")
        result.redundancy_findings = self._find_redundancies(result.query_patterns)

        # 6. Generate Recommendations
        logger.info("Generating recommendations...")
        result.recommendations = self._generate_recommendations(result)

        logger.info(f"DBA analysis complete: {result.get_summary()}")
        return result

    def _analyze_connections(self, result: DBAAnalysisResult) -> None:
        """Analyze connection patterns and identify issues."""
        total_connections = 0
        total_active = 0
        total_idle = 0
        issues = []

        for source in result.connection_sources:
            total_connections += source.session_count
            total_active += source.active_requests
            total_idle += source.idle_connections

            # Check for connection pooling issues
            if source.session_count > 50:
                issues.append(
                    f"High connection count ({source.session_count}) from "
                    f"{source.get_display_name()}"
                )

            # Check for idle connections holding resources
            if source.idle_connections > 20 and source.open_transactions > 0:
                issues.append(
                    f"Idle connections with open transactions from "
                    f"{source.get_display_name()}: {source.open_transactions} open txns"
                )

            # Check for long-running transactions
            if source.longest_transaction_seconds > 300:  # 5 minutes
                issues.append(
                    f"Long-running transaction ({source.longest_transaction_seconds}s) from "
                    f"{source.get_display_name()}"
                )

            # Check for blocking issues
            if source.blocking_count > 5:
                issues.append(
                    f"Frequent blocker: {source.get_display_name()} "
                    f"blocking {source.blocking_count} sessions"
                )

        result.total_connections = total_connections
        result.total_active = total_active
        result.total_idle = total_idle
        result.connection_issues = issues

    def _get_top_expensive_queries(
        self, patterns: list[QueryPattern], top_n: int = 20
    ) -> list[QueryPattern]:
        """Get top N most expensive queries."""
        expensive = [p for p in patterns if p.is_expensive()]
        sorted_patterns = sorted(expensive, key=lambda x: x.get_cost_score(), reverse=True)
        return sorted_patterns[:top_n]

    def _identify_blocking_hotspots(self, blocking: list[BlockingInfo]) -> list[str]:
        """Identify blocking hotspots from blocking chains."""
        hotspots = []
        blocker_counts = defaultdict(int)

        for b in blocking:
            key = f"{b.blocking_program} ({b.blocking_host})"
            blocker_counts[key] += 1

        for blocker, count in sorted(blocker_counts.items(), key=lambda x: -x[1]):
            if count >= 2:
                hotspots.append(f"{blocker}: blocking {count} sessions")

        # Check for long waits
        for b in blocking:
            if b.wait_time_ms > 30000:  # 30 seconds
                hotspots.append(
                    f"Long wait: {b.blocked_program} waiting {b.wait_time_ms/1000:.1f}s "
                    f"for {b.wait_type} on {b.wait_resource}"
                )

        return hotspots

    def _build_scorecards(self, result: DBAAnalysisResult) -> list[SystemScorecard]:
        """Build scorecards for each connecting system."""
        scorecards = []

        for source in result.connection_sources:
            scorecard = SystemScorecard(
                system_name=source.program_name,
                host_name=source.host_name,
                login_name=source.login_name,
                total_connections=source.session_count,
                active_connections=source.active_requests,
                idle_connections=source.idle_connections,
                cpu_cost_seconds=source.total_cpu_ms / 1000.0,
                io_reads=source.total_reads,
                io_writes=source.total_writes,
                memory_mb=source.total_memory_kb / 1024.0,
                times_blocked=source.blocked_count,
                times_blocking=source.blocking_count,
            )

            # Calculate connection pool efficiency
            if scorecard.total_connections > 0:
                scorecard.connection_pool_efficiency = (
                    scorecard.active_connections / scorecard.total_connections * 100
                )

            # Count queries from this source
            source_queries = [
                p for p in result.query_patterns
                if p.source_program == source.program_name
            ]
            scorecard.total_queries = sum(p.execution_count for p in source_queries)
            scorecard.distinct_query_patterns = len(source_queries)
            scorecard.expensive_queries = len([p for p in source_queries if p.is_expensive()])

            # Calculate overall score
            scorecard.calculate_score()
            scorecards.append(scorecard)

        # Sort by resource score and assign ranks
        scorecards.sort(key=lambda x: x.resource_score, reverse=True)
        for i, sc in enumerate(scorecards):
            sc.rank = i + 1

        return scorecards

    def _find_redundancies(self, patterns: list[QueryPattern]) -> list[RedundancyFinding]:
        """Find redundant query patterns across systems."""
        findings = []

        # Group queries by hash
        query_by_hash = defaultdict(list)
        for p in patterns:
            if p.query_hash:
                query_by_hash[p.query_hash].append(p)

        # Find queries executed by multiple systems
        for query_hash, query_list in query_by_hash.items():
            systems = list(set(p.source_program for p in query_list))
            if len(systems) > 1:
                total_exec = sum(p.execution_count for p in query_list)
                findings.append(RedundancyFinding(
                    query_pattern=query_list[0].get_truncated_query(100),
                    systems_involved=systems,
                    total_executions=total_exec,
                    potential_savings_percent=min(50.0, (len(systems) - 1) * 20.0),
                    recommendation="Consider consolidating this query to a single service or caching layer",
                    severity="medium" if total_exec > 1000 else "low",
                ))

        # Find N+1 patterns (same query executed many times in short window)
        for p in patterns:
            if p.execution_count > 1000 and p.avg_elapsed_time_ms < 10:
                findings.append(RedundancyFinding(
                    query_pattern=p.get_truncated_query(100),
                    systems_involved=[p.source_program],
                    total_executions=p.execution_count,
                    potential_savings_percent=80.0,
                    recommendation="Potential N+1 pattern - consider batching or caching",
                    severity="high",
                ))

        return findings

    def _generate_recommendations(self, result: DBAAnalysisResult) -> list[str]:
        """Generate prioritized recommendations."""
        recommendations = []

        # Connection recommendations
        if result.total_idle > result.total_active * 2:
            recommendations.append(
                f"HIGH: {result.total_idle} idle connections vs {result.total_active} active. "
                "Consider reducing connection pool sizes."
            )

        for issue in result.connection_issues:
            recommendations.append(f"MEDIUM: {issue}")

        # Query recommendations
        for query in result.top_expensive_queries[:5]:
            if query.avg_worker_time_ms > 5000:
                recommendations.append(
                    f"HIGH: Query consuming {query.avg_worker_time_ms/1000:.1f}s CPU avg "
                    f"({query.execution_count} executions). Review: {query.get_truncated_query(80)}"
                )

        # Blocking recommendations
        for hotspot in result.blocking_hotspots[:3]:
            recommendations.append(f"HIGH: Blocking hotspot - {hotspot}")

        # Redundancy recommendations
        high_severity = [f for f in result.redundancy_findings if f.severity == "high"]
        for finding in high_severity[:3]:
            recommendations.append(
                f"MEDIUM: {finding.recommendation} - "
                f"Query: {finding.query_pattern[:50]}..."
            )

        # Scorecard-based recommendations
        for sc in result.system_scorecards[:3]:
            if sc.connection_pool_efficiency < 10 and sc.total_connections > 10:
                recommendations.append(
                    f"LOW: {sc.system_name} has low connection utilization "
                    f"({sc.connection_pool_efficiency:.1f}%). Consider reducing pool size."
                )

        return recommendations

    def get_system_report(self, system_name: str, result: DBAAnalysisResult) -> dict[str, Any]:
        """Get detailed report for a specific system."""
        scorecard = next(
            (sc for sc in result.system_scorecards if sc.system_name == system_name),
            None
        )
        if not scorecard:
            return {"error": f"System '{system_name}' not found"}

        queries = [
            p for p in result.query_patterns
            if p.source_program == system_name
        ]

        blocking_as_blocker = [
            b for b in result.current_blocking
            if b.blocking_program == system_name
        ]

        blocking_as_blocked = [
            b for b in result.current_blocking
            if b.blocked_program == system_name
        ]

        redundancies = [
            r for r in result.redundancy_findings
            if system_name in r.systems_involved
        ]

        return {
            "scorecard": scorecard,
            "queries": sorted(queries, key=lambda x: x.get_cost_score(), reverse=True)[:20],
            "blocking_others": blocking_as_blocker,
            "blocked_by_others": blocking_as_blocked,
            "redundancies": redundancies,
        }
