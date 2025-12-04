"""Results visualization and export page."""
import sys
import os
import tempfile
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load cache first
from src.ui.cache_loader import load_all_cache
load_all_cache()

from src.core.config import get_settings
from src.core.logging import get_logger
from src.services.export import ExportService
from src.services.sync_script import SyncScriptGenerator
from src.utils.formatters import format_number, format_percentage
from src.ui.styles import apply_professional_style, render_empty_state, render_status_badge

logger = get_logger(__name__)
settings = get_settings()

# Apply professional styling
apply_professional_style()


def render() -> None:
    """Render the results page."""
    st.title("Comparison Results")

    results = st.session_state.get("comparison_results", [])

    if not results:
        st.markdown(render_empty_state(
            "📊",
            "No Results Available",
            "Run a comparison from the Comparison page to see results here"
        ), unsafe_allow_html=True)
        return

    # Summary section
    render_summary(results)

    st.markdown("---")

    # Detailed results
    st.subheader("Detailed Results")

    # Filter options
    col1, col2, col3 = st.columns(3)

    with col1:
        filter_status = st.selectbox(
            "Filter by Status",
            options=["All", "Matching", "Different", "Failed"],
        )

    with col2:
        sort_by = st.selectbox(
            "Sort by",
            options=["Table Name", "Source Rows", "Differences", "Duration"],
        )

    with col3:
        sort_order = st.selectbox(
            "Order",
            options=["Ascending", "Descending"],
        )

    # Apply filters
    filtered_results = filter_results(results, filter_status)
    sorted_results = sort_results(filtered_results, sort_by, sort_order)

    # Display results table
    render_results_table(sorted_results)

    # Export options
    st.markdown("---")
    render_export_options(results)

    # Sync script generation
    st.markdown("---")
    render_sync_script_options(sorted_results)


def render_summary(results: list) -> None:
    """Render summary statistics and charts."""
    st.subheader("📊 Summary Dashboard")

    # Calculate statistics
    total_tables = len(results)
    matching = sum(1 for r in results if r.is_match())
    different = sum(1 for r in results if not r.is_match() and r.status == "completed")
    failed = sum(1 for r in results if r.status == "failed")

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Tables", total_tables)

    with col2:
        st.metric("✅ Matching", matching, delta=format_percentage(matching, total_tables))

    with col3:
        st.metric("⚠️ Different", different, delta=f"-{format_percentage(different, total_tables)}")

    with col4:
        st.metric("❌ Failed", failed)

    # Charts row
    col1, col2 = st.columns(2)

    with col1:
        # Pie chart of match status
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=["Matching", "Different", "Failed"],
                    values=[matching, different, failed],
                    marker=dict(
                        colors=[
                            settings.ui.match_color,
                            settings.ui.schema_diff_color,
                            settings.ui.data_diff_color,
                        ]
                    ),
                )
            ]
        )
        fig.update_layout(title="Comparison Status Distribution")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Bar chart of row counts
        table_names = [r.source_table.split(".")[-1] for r in results[:10]]  # Top 10
        source_rows = [r.source_row_count for r in results[:10]]
        target_rows = [r.target_row_count for r in results[:10]]

        fig = go.Figure(
            data=[
                go.Bar(name="Source", x=table_names, y=source_rows),
                go.Bar(name="Target", x=table_names, y=target_rows),
            ]
        )
        fig.update_layout(
            title="Row Count Comparison (Top 10 Tables)",
            barmode="group",
            xaxis_title="Table",
            yaxis_title="Row Count",
        )
        st.plotly_chart(fig, use_container_width=True)


def filter_results(results: list, filter_status: str) -> list:
    """Filter results based on status."""
    if filter_status == "All":
        return results
    elif filter_status == "Matching":
        return [r for r in results if r.is_match()]
    elif filter_status == "Different":
        return [r for r in results if not r.is_match() and r.status == "completed"]
    elif filter_status == "Failed":
        return [r for r in results if r.status == "failed"]
    return results


def sort_results(results: list, sort_by: str, order: str) -> list:
    """Sort results based on criteria."""
    reverse = order == "Descending"

    if sort_by == "Table Name":
        return sorted(results, key=lambda x: x.source_table, reverse=reverse)
    elif sort_by == "Source Rows":
        return sorted(results, key=lambda x: x.source_row_count, reverse=reverse)
    elif sort_by == "Differences":
        return sorted(
            results,
            key=lambda x: x.different_rows + x.source_only_rows + x.target_only_rows,
            reverse=reverse,
        )
    elif sort_by == "Duration":
        return sorted(results, key=lambda x: x.duration_seconds, reverse=reverse)

    return results


def render_results_table(results: list) -> None:
    """Render detailed results table."""
    for result in results:
        # Status indicator
        if result.is_match():
            status_color = settings.ui.match_color
            status_text = "✅ Match"
        elif result.status == "failed":
            status_color = settings.ui.data_diff_color
            status_text = "❌ Failed"
        else:
            status_color = settings.ui.schema_diff_color
            status_text = "⚠️ Different"

        with st.expander(f"{status_text} - {result.source_table}"):
            # Metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Source Rows", format_number(result.source_row_count))

            with col2:
                st.metric("Target Rows", format_number(result.target_row_count))

            with col3:
                st.metric("Match %", f"{result.get_match_percentage():.1f}%")

            # Drill-down button for different tables
            if not result.is_match():
                parts = result.source_table.split(".")
                schema_name = parts[0] if len(parts) > 1 else "dbo"
                table_name = parts[-1]

                # Get connection info from session state
                source_conn_info = st.session_state.get("source_connection")
                target_conn_info = st.session_state.get("target_connection")

                if source_conn_info and target_conn_info:
                    if st.button(f"🔎 View Data Differences", key=f"drill_result_{result.source_table}", type="primary"):
                        st.session_state.drill_down_data = {
                            "table_name": table_name,
                            "schema_name": schema_name,
                            "source_conn_info": source_conn_info,
                            "target_conn_info": target_conn_info,
                            "source_row_count": result.source_row_count,
                            "target_row_count": result.target_row_count,
                        }
                        st.info("✅ Data loaded! Click **Drill_Down** in the sidebar to view details.")

            # Row differences
            if result.source_only_rows > 0 or result.target_only_rows > 0:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Source Only", format_number(result.source_only_rows))
                with col2:
                    st.metric("Target Only", format_number(result.target_only_rows))

            # Schema differences
            if result.schema_differences:
                st.markdown("**Schema Differences:**")
                schema_df = pd.DataFrame(
                    [
                        {
                            "Column": d.column_name or "-",
                            "Type": d.difference_type.value,
                            "Source": d.source_value or "-",
                            "Target": d.target_value or "-",
                            "Description": d.description,
                        }
                        for d in result.schema_differences[:100]  # Limit
                    ]
                )
                st.dataframe(schema_df, use_container_width=True)

            # Data differences
            if result.data_differences:
                st.markdown(f"**Data Differences:** (showing first 100 of {len(result.data_differences)})")
                data_df = pd.DataFrame(
                    [
                        {
                            "Primary Key": d.get_pk_display(),
                            "Column": d.column_name or "-",
                            "Source Value": str(d.source_value),
                            "Target Value": str(d.target_value),
                        }
                        for d in result.data_differences[:100]
                    ]
                )
                st.dataframe(data_df, use_container_width=True)


def render_export_options(results: list) -> None:
    """Render export options."""
    st.subheader("💾 Export Results")

    col1, col2, col3 = st.columns(3)

    export_service = ExportService()

    with col1:
        if st.button("📊 Export to Excel", use_container_width=True):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"comparison_results_{timestamp}.xlsx"

                with st.spinner("Exporting to Excel..."):
                    with tempfile.TemporaryDirectory() as temp_dir:
                        output_path = os.path.join(temp_dir, filename)
                        export_service.export_comparison_to_excel(results, output_path)

                        # Read file and provide download
                        with open(output_path, "rb") as f:
                            file_data = f.read()

                st.success("✅ Excel file ready for download")
                st.download_button(
                    label="📥 Download Excel File",
                    data=file_data,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            except Exception as e:
                st.error(f"Export failed: {str(e)}")
                logger.error(f"Excel export failed: {str(e)}", exc_info=True)

    with col2:
        if st.button("📄 Export to CSV", use_container_width=True, disabled=True):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_dir = f"comparison_results_{timestamp}"

                with st.spinner("Exporting to CSV..."):
                    files = export_service.export_comparison_to_csv(results, output_dir)

                st.success(f"✅ Exported {len(files)} CSV files to {output_dir}/")

            except Exception as e:
                st.error(f"Export failed: {str(e)}")
                logger.error(f"CSV export failed: {str(e)}", exc_info=True)

    with col3:
        if st.button("🌐 Export to HTML", use_container_width=True):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"comparison_report_{timestamp}.html"

                with st.spinner("Generating HTML report..."):
                    with tempfile.TemporaryDirectory() as temp_dir:
                        output_path = os.path.join(temp_dir, filename)
                        export_service.generate_html_report(results, output_path)

                        # Read file and provide download
                        with open(output_path, "rb") as f:
                            file_data = f.read()

                st.success("✅ HTML report ready for download")
                st.download_button(
                    label="📥 Download HTML Report",
                    data=file_data,
                    file_name=filename,
                    mime="text/html",
                )

            except Exception as e:
                st.error(f"Export failed: {str(e)}")
                logger.error(f"HTML export failed: {str(e)}", exc_info=True)


def render_sync_script_options(results: list) -> None:
    """Render sync script generation options."""
    st.subheader("🔄 Generate Sync Scripts")

    st.markdown("Generate SQL scripts to synchronize target database with source.")

    # Select tables for sync script
    table_names = [r.source_table for r in results if not r.is_match()]

    if not table_names:
        st.info("All tables match - no sync scripts needed!")
        return

    selected_tables = st.multiselect(
        "Select Tables for Sync Script",
        options=table_names,
        help="Select tables to generate synchronization scripts for",
    )

    if selected_tables:
        script_generator = SyncScriptGenerator()

        if st.button("⚙️ Generate Sync Scripts", type="primary"):
            try:
                scripts = []

                with st.spinner("Generating sync scripts..."):
                    for table_name in selected_tables:
                        result = next(r for r in results if r.source_table == table_name)

                        # Generate data sync script
                        data_script = script_generator.generate_sync_script(result)
                        scripts.append(data_script)

                        # Generate schema sync script if needed
                        if not result.schema_match:
                            schema_script = script_generator.generate_schema_sync_script(result)
                            if schema_script:
                                scripts.append(schema_script)

                # Combine scripts
                full_script = "\n\n".join(scripts)

                # Display script
                st.code(full_script, language="sql")

                # Download button
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"sync_script_{timestamp}.sql"

                st.download_button(
                    label="📥 Download Sync Script",
                    data=full_script,
                    file_name=filename,
                    mime="text/plain",
                )

                st.success("✅ Sync scripts generated successfully!")

            except Exception as e:
                st.error(f"Failed to generate sync scripts: {str(e)}")
                logger.error(f"Sync script generation failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    render()