"""Compression analysis and recommendation service."""

from typing import Optional

from src.core.config import get_settings
from src.core.logging import get_logger
from src.data.database import DatabaseConnection
from src.data.models import (
    CompressionAnalysis,
    CompressionRecommendation,
    CompressionType,
    TableInfo,
)
from src.data.repositories import CompressionRepository, MetadataRepository

logger = get_logger(__name__)


class CompressionService:
    """Service for compression analysis and recommendations."""

    def __init__(self, connection: DatabaseConnection) -> None:
        """
        Initialize compression service.

        Args:
            connection: Database connection
        """
        self.connection = connection
        self.compression_repo = CompressionRepository(connection)
        self.metadata_repo = MetadataRepository(connection)
        self.settings = get_settings()

    def analyze_table(
        self,
        schema_name: str,
        table_name: str,
        compression_types: Optional[list[str]] = None,
    ) -> CompressionAnalysis:
        """
        Analyze compression options for a table.

        Args:
            schema_name: Schema name
            table_name: Table name
            compression_types: Optional list of compression types to analyze

        Returns:
            Compression analysis results
        """
        logger.info(f"Analyzing compression for {schema_name}.{table_name}")

        # Get table info
        table_info = self.metadata_repo.get_table_info(
            schema_name, table_name, include_metadata=False
        )

        # Skip if table is too small
        if table_info.row_count < self.settings.compression.analyze_threshold:
            logger.info(
                f"Skipping {schema_name}.{table_name} - "
                f"row count ({table_info.row_count}) below threshold "
                f"({self.settings.compression.analyze_threshold})"
            )

        # Analyze compression
        analysis = self.compression_repo.estimate_compression(
            schema_name, table_name, compression_types
        )

        logger.info(
            f"Compression analysis complete for {schema_name}.{table_name}"
        )
        return analysis

    def get_recommendations(
        self,
        schema_name: str,
        table_name: Optional[str] = None,
    ) -> list[CompressionRecommendation]:
        """
        Get compression recommendations for tables.

        Args:
            schema_name: Schema name
            table_name: Optional specific table name

        Returns:
            List of compression recommendations
        """
        recommendations: list[CompressionRecommendation] = []

        # Get tables to analyze
        if table_name:
            tables = [
                self.metadata_repo.get_table_info(
                    schema_name, table_name, include_metadata=False
                )
            ]
        else:
            tables = self.metadata_repo.get_tables(schema_filter=schema_name)

        # Analyze each table
        for table in tables:
            try:
                # Skip small tables
                if (
                    table.row_count
                    < self.settings.compression.analyze_threshold
                ):
                    continue

                analysis = self.analyze_table(
                    table.schema_name,
                    table.table_name,
                    self.settings.compression.supported_types,
                )

                recommendation = self._generate_recommendation(table, analysis)
                if recommendation:
                    recommendations.append(recommendation)

            except Exception as e:
                logger.error(
                    f"Failed to analyze {table.get_full_name()}: {str(e)}"
                )
                continue

        # Sort by estimated savings
        recommendations.sort(
            key=lambda x: x.estimated_savings_mb, reverse=True
        )

        logger.info(f"Generated {len(recommendations)} recommendations")
        return recommendations

    def _generate_recommendation(
        self,
        table_info: TableInfo,
        analysis: CompressionAnalysis,
    ) -> Optional[CompressionRecommendation]:
        """
        Generate compression recommendation based on analysis.

        Args:
            table_info: Table information
            analysis: Compression analysis results

        Returns:
            Compression recommendation or None
        """
        current_type = analysis.current_compression
        current_size_mb = analysis.current_size_kb / 1024.0

        # Find best compression type
        best_type = current_type
        best_size_kb = analysis.current_size_kb
        best_savings_percent = 0.0

        # Check each compression type
        compression_options = [
            (CompressionType.ROW, analysis.row_size_kb),
            (CompressionType.PAGE, analysis.page_size_kb),
            (CompressionType.COLUMNSTORE, analysis.columnstore_size_kb),
        ]

        for comp_type, size_kb in compression_options:
            if size_kb is None:
                continue

            savings_percent = analysis.get_savings_percent(comp_type)
            if savings_percent and savings_percent > best_savings_percent:
                best_type = comp_type
                best_size_kb = size_kb
                best_savings_percent = savings_percent

        # No better option found
        if best_type == current_type or best_savings_percent < 5.0:
            return None

        # Generate recommendation
        best_size_mb = best_size_kb / 1024.0
        savings_mb = current_size_mb - best_size_mb

        # Determine priority
        if best_savings_percent > 50 or savings_mb > 1000:
            priority = "high"
        elif best_savings_percent > 25 or savings_mb > 100:
            priority = "medium"
        else:
            priority = "low"

        # Generate reason
        reason = self._generate_reason(
            table_info, current_type, best_type, best_savings_percent
        )

        return CompressionRecommendation(
            table_name=table_info.get_full_name(),
            current_compression=current_type,
            recommended_compression=best_type,
            current_size_mb=current_size_mb,
            estimated_size_mb=best_size_mb,
            estimated_savings_mb=savings_mb,
            estimated_savings_percent=best_savings_percent,
            reason=reason,
            priority=priority,
        )

    def _generate_reason(
        self,
        table_info: TableInfo,
        current_type: CompressionType,
        recommended_type: CompressionType,
        savings_percent: float,
    ) -> str:
        """
        Generate human-readable reason for recommendation.

        Args:
            table_info: Table information
            current_type: Current compression type
            recommended_type: Recommended compression type
            savings_percent: Estimated savings percentage

        Returns:
            Reason string
        """
        reasons = []

        # Base recommendation
        reasons.append(
            f"Switching from {current_type.value} to {recommended_type.value} "
            f"compression could save ~{savings_percent:.1f}% space"
        )

        # Type-specific reasons
        if recommended_type == CompressionType.COLUMNSTORE:
            if table_info.row_count > 1000000:
                reasons.append(
                    "Large table with many rows - ideal for columnstore"
                )
            reasons.append("Good for analytical workloads and data warehousing")

        elif recommended_type == CompressionType.PAGE:
            reasons.append(
                "PAGE compression provides good balance of space savings and performance"
            )

        elif recommended_type == CompressionType.ROW:
            reasons.append(
                "ROW compression has minimal performance impact with decent savings"
            )

        return ". ".join(reasons)

    def apply_recommendations(
        self,
        recommendations: list[CompressionRecommendation],
        min_priority: str = "medium",
        dry_run: bool = True,
    ) -> list[str]:
        """
        Apply compression recommendations.

        Args:
            recommendations: List of recommendations to apply
            min_priority: Minimum priority to apply (low, medium, high)
            dry_run: If True, only generate scripts without applying

        Returns:
            List of SQL scripts (if dry_run=True) or results
        """
        priority_order = {"low": 0, "medium": 1, "high": 2}
        min_priority_value = priority_order.get(min_priority, 1)

        scripts = []
        applied = []

        for rec in recommendations:
            if not rec.should_apply():
                continue

            if priority_order.get(rec.priority, 0) < min_priority_value:
                continue

            # Parse schema and table name
            parts = rec.table_name.split(".")
            if len(parts) != 2:
                logger.warning(f"Invalid table name format: {rec.table_name}")
                continue

            schema_name, table_name = parts

            # Generate script
            script = (
                f"-- {rec.reason}\n"
                f"ALTER TABLE [{schema_name}].[{table_name}] "
                f"REBUILD WITH (DATA_COMPRESSION = {rec.recommended_compression.value});"
            )
            scripts.append(script)

            if not dry_run:
                try:
                    self.compression_repo.apply_compression(
                        schema_name,
                        table_name,
                        rec.recommended_compression,
                    )
                    applied.append(rec.table_name)
                    logger.info(f"Applied compression to {rec.table_name}")
                except Exception as e:
                    logger.error(
                        f"Failed to apply compression to {rec.table_name}: {str(e)}"
                    )

        if dry_run:
            logger.info(f"Generated {len(scripts)} compression scripts")
        else:
            logger.info(f"Applied compression to {len(applied)} tables")

        return scripts if dry_run else applied
