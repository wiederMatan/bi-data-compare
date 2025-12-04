"""Export service for comparison results."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fpdf import FPDF

from src.core.exceptions import ExportError
from src.core.logging import get_logger
from src.data.models import ComparisonResult, CompressionRecommendation

logger = get_logger(__name__)


class ExportService:
    """Service for exporting comparison results and reports."""

    def __init__(self) -> None:
        """Initialize export service."""
        pass

    def export_comparison_to_excel(
        self,
        results: list[ComparisonResult],
        output_path: str,
    ) -> None:
        """
        Export comparison results to Excel.

        Args:
            results: List of comparison results
            output_path: Output file path

        Raises:
            ExportError: If export fails
        """
        try:
            logger.info(f"Exporting comparison results to Excel: {output_path}")

            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                # Summary sheet
                summary_data = []
                for result in results:
                    summary_data.append(
                        {
                            "Source Table": result.source_table,
                            "Target Table": result.target_table,
                            "Status": result.status,
                            "Schema Match": result.schema_match,
                            "Source Rows": result.source_row_count,
                            "Target Rows": result.target_row_count,
                            "Matching Rows": result.matching_rows,
                            "Different Rows": result.different_rows,
                            "Source Only": result.source_only_rows,
                            "Target Only": result.target_only_rows,
                            "Match %": f"{result.get_match_percentage():.2f}%",
                            "Duration (s)": f"{result.duration_seconds:.2f}",
                            "Summary": result.get_summary(),
                        }
                    )

                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(
                    writer, sheet_name="Summary", index=False
                )

                # Schema differences sheet
                schema_diffs = []
                for result in results:
                    for diff in result.schema_differences:
                        schema_diffs.append(
                            {
                                "Table": diff.table_name,
                                "Type": diff.difference_type.value,
                                "Column": diff.column_name or "",
                                "Source": diff.source_value or "",
                                "Target": diff.target_value or "",
                                "Description": diff.description,
                            }
                        )

                if schema_diffs:
                    schema_df = pd.DataFrame(schema_diffs)
                    schema_df.to_excel(
                        writer, sheet_name="Schema Differences", index=False
                    )

                # Data differences sheet (limited to prevent huge files)
                data_diffs = []
                for result in results:
                    for diff in result.data_differences[:1000]:  # Limit rows
                        data_diffs.append(
                            {
                                "Table": diff.table_name,
                                "Primary Key": diff.get_pk_display(),
                                "Column": diff.column_name or "",
                                "Source Value": str(diff.source_value or ""),
                                "Target Value": str(diff.target_value or ""),
                            }
                        )

                if data_diffs:
                    data_df = pd.DataFrame(data_diffs)
                    data_df.to_excel(
                        writer, sheet_name="Data Differences", index=False
                    )

            logger.info("Excel export completed successfully")

        except Exception as e:
            logger.error(f"Failed to export to Excel: {str(e)}")
            raise ExportError(
                f"Failed to export to Excel: {str(e)}",
                export_format="excel",
                file_path=output_path,
            ) from e

    def export_comparison_to_csv(
        self,
        results: list[ComparisonResult],
        output_dir: str,
    ) -> list[str]:
        """
        Export comparison results to CSV files.

        Args:
            results: List of comparison results
            output_dir: Output directory path

        Returns:
            List of created file paths

        Raises:
            ExportError: If export fails
        """
        try:
            logger.info(f"Exporting comparison results to CSV: {output_dir}")
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            created_files = []

            # Summary CSV
            summary_data = []
            for result in results:
                summary_data.append(
                    {
                        "source_table": result.source_table,
                        "target_table": result.target_table,
                        "status": result.status,
                        "schema_match": result.schema_match,
                        "source_rows": result.source_row_count,
                        "target_rows": result.target_row_count,
                        "matching_rows": result.matching_rows,
                        "different_rows": result.different_rows,
                        "source_only": result.source_only_rows,
                        "target_only": result.target_only_rows,
                        "match_percentage": result.get_match_percentage(),
                        "duration_seconds": result.duration_seconds,
                    }
                )

            summary_file = output_path / "summary.csv"
            pd.DataFrame(summary_data).to_csv(summary_file, index=False)
            created_files.append(str(summary_file))

            # Schema differences CSV
            schema_diffs = []
            for result in results:
                for diff in result.schema_differences:
                    schema_diffs.append(
                        {
                            "table": diff.table_name,
                            "type": diff.difference_type.value,
                            "column": diff.column_name or "",
                            "source": diff.source_value or "",
                            "target": diff.target_value or "",
                            "description": diff.description,
                        }
                    )

            if schema_diffs:
                schema_file = output_path / "schema_differences.csv"
                pd.DataFrame(schema_diffs).to_csv(schema_file, index=False)
                created_files.append(str(schema_file))

            logger.info(f"CSV export completed: {len(created_files)} files created")
            return created_files

        except Exception as e:
            logger.error(f"Failed to export to CSV: {str(e)}")
            raise ExportError(
                f"Failed to export to CSV: {str(e)}",
                export_format="csv",
                file_path=output_dir,
            ) from e

    def export_comparison_to_json(
        self,
        results: list[ComparisonResult],
        output_path: str,
    ) -> None:
        """
        Export comparison results to JSON.

        Args:
            results: List of comparison results
            output_path: Output file path

        Raises:
            ExportError: If export fails
        """
        try:
            logger.info(f"Exporting comparison results to JSON: {output_path}")

            data = {
                "export_date": datetime.now().isoformat(),
                "total_comparisons": len(results),
                "results": [],
            }

            for result in results:
                data["results"].append(
                    {
                        "source_table": result.source_table,
                        "target_table": result.target_table,
                        "mode": result.mode.value,
                        "status": result.status,
                        "started_at": result.started_at.isoformat(),
                        "completed_at": (
                            result.completed_at.isoformat()
                            if result.completed_at
                            else None
                        ),
                        "schema_match": result.schema_match,
                        "source_row_count": result.source_row_count,
                        "target_row_count": result.target_row_count,
                        "matching_rows": result.matching_rows,
                        "different_rows": result.different_rows,
                        "source_only_rows": result.source_only_rows,
                        "target_only_rows": result.target_only_rows,
                        "match_percentage": result.get_match_percentage(),
                        "duration_seconds": result.duration_seconds,
                        "schema_differences": [
                            {
                                "table": d.table_name,
                                "type": d.difference_type.value,
                                "column": d.column_name,
                                "source": d.source_value,
                                "target": d.target_value,
                                "description": d.description,
                            }
                            for d in result.schema_differences
                        ],
                        "data_differences_count": len(result.data_differences),
                    }
                )

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info("JSON export completed successfully")

        except Exception as e:
            logger.error(f"Failed to export to JSON: {str(e)}")
            raise ExportError(
                f"Failed to export to JSON: {str(e)}",
                export_format="json",
                file_path=output_path,
            ) from e

    def export_compression_recommendations(
        self,
        recommendations: list[CompressionRecommendation],
        output_path: str,
        format: str = "excel",
    ) -> None:
        """
        Export compression recommendations.

        Args:
            recommendations: List of recommendations
            output_path: Output file path
            format: Output format (excel, csv, json)

        Raises:
            ExportError: If export fails
        """
        try:
            logger.info(
                f"Exporting compression recommendations to {format}: {output_path}"
            )

            data = []
            for rec in recommendations:
                data.append(
                    {
                        "Table": rec.table_name,
                        "Current Compression": rec.current_compression.value,
                        "Recommended": rec.recommended_compression.value,
                        "Current Size (MB)": f"{rec.current_size_mb:.2f}",
                        "Estimated Size (MB)": f"{rec.estimated_size_mb:.2f}",
                        "Savings (MB)": f"{rec.estimated_savings_mb:.2f}",
                        "Savings %": f"{rec.estimated_savings_percent:.1f}%",
                        "Priority": rec.priority,
                        "Reason": rec.reason,
                    }
                )

            df = pd.DataFrame(data)

            if format == "excel":
                df.to_excel(output_path, index=False, engine="openpyxl")
            elif format == "csv":
                df.to_csv(output_path, index=False)
            elif format == "json":
                df.to_json(output_path, orient="records", indent=2)
            else:
                raise ExportError(
                    f"Unsupported export format: {format}",
                    export_format=format,
                )

            logger.info("Compression recommendations export completed")

        except Exception as e:
            logger.error(f"Failed to export recommendations: {str(e)}")
            raise ExportError(
                f"Failed to export recommendations: {str(e)}",
                export_format=format,
                file_path=output_path,
            ) from e

    def generate_html_report(
        self,
        results: list[ComparisonResult],
        output_path: str,
    ) -> None:
        """
        Generate HTML report of comparison results.

        Args:
            results: List of comparison results
            output_path: Output file path

        Raises:
            ExportError: If export fails
        """
        try:
            logger.info(f"Generating HTML report: {output_path}")

            html = self._build_html_report(results)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html)

            logger.info("HTML report generated successfully")

        except Exception as e:
            logger.error(f"Failed to generate HTML report: {str(e)}")
            raise ExportError(
                f"Failed to generate HTML report: {str(e)}",
                export_format="html",
                file_path=output_path,
            ) from e

    def _build_html_report(self, results: list[ComparisonResult]) -> str:
        """Build HTML content for report."""
        total_tables = len(results)
        matching_tables = sum(1 for r in results if r.is_match())
        failed_tables = sum(1 for r in results if r.status == "failed")

        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Comparison Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #0066CC; padding-bottom: 10px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .summary-card {{ background: #f8f9fa; padding: 15px; border-radius: 5px; border-left: 4px solid #0066CC; }}
        .summary-card h3 {{ margin: 0 0 5px 0; color: #666; font-size: 14px; }}
        .summary-card .value {{ font-size: 32px; font-weight: bold; color: #333; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #0066CC; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        .match {{ color: #28A745; }}
        .diff {{ color: #DC3545; }}
        .warning {{ color: #FFC107; }}
        .badge {{ padding: 4px 8px; border-radius: 3px; font-size: 12px; font-weight: bold; }}
        .badge-success {{ background: #28A745; color: white; }}
        .badge-danger {{ background: #DC3545; color: white; }}
        .badge-warning {{ background: #FFC107; color: black; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Database Comparison Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <div class="summary">
            <div class="summary-card">
                <h3>Total Tables</h3>
                <div class="value">{total_tables}</div>
            </div>
            <div class="summary-card">
                <h3>Matching</h3>
                <div class="value match">{matching_tables}</div>
            </div>
            <div class="summary-card">
                <h3>Differences</h3>
                <div class="value diff">{total_tables - matching_tables - failed_tables}</div>
            </div>
            <div class="summary-card">
                <h3>Failed</h3>
                <div class="value warning">{failed_tables}</div>
            </div>
        </div>

        <h2>Comparison Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Table</th>
                    <th>Status</th>
                    <th>Source Rows</th>
                    <th>Target Rows</th>
                    <th>Match %</th>
                    <th>Summary</th>
                </tr>
            </thead>
            <tbody>
        """

        for result in results:
            status_class = (
                "badge-success"
                if result.is_match()
                else "badge-danger"
                if result.status == "failed"
                else "badge-warning"
            )
            html += f"""
                <tr>
                    <td>{result.source_table}</td>
                    <td><span class="badge {status_class}">{result.status}</span></td>
                    <td>{result.source_row_count:,}</td>
                    <td>{result.target_row_count:,}</td>
                    <td>{result.get_match_percentage():.1f}%</td>
                    <td>{result.get_summary()}</td>
                </tr>
            """

        html += """
            </tbody>
        </table>
    </div>
</body>
</html>
        """

        return html
