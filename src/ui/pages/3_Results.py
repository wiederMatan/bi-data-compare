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
from src.data.database import get_cached_connection
from src.utils.validators import validate_sql_identifier, validate_date_value

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

            # Inline Drill-Down for non-failed tables
            if result.status != "failed":
                render_inline_drill_down(result)
            else:
                # Show error message for failed tables
                if result.error_message:
                    st.error(f"Comparison failed: {result.error_message}")


def render_inline_drill_down(result) -> None:
    """Render inline drill-down analysis for a table."""
    parts = result.source_table.split(".")
    schema_name = parts[0] if len(parts) > 1 else "dbo"
    table_name = parts[-1]

    # Get connection info from session state
    source_conn_info = st.session_state.get("source_connection")
    target_conn_info = st.session_state.get("target_connection")

    if not source_conn_info or not target_conn_info:
        st.warning("Connection information not available. Please re-run the comparison.")
        return

    # Create a unique key for this table's drill-down state
    drill_key = f"drill_expanded_{result.source_table}"

    st.markdown("---")

    # Button to load/toggle drill-down data
    button_label = "🔍 View Data Differences" if result.is_match() else "🔍 View Data Differences"
    if result.is_match():
        button_label = "🔍 Verify Data (Sample)"

    if st.button(button_label, key=f"drill_btn_{result.source_table}", type="primary"):
        # Toggle the drill-down state
        st.session_state[drill_key] = not st.session_state.get(drill_key, False)
        st.rerun()

    # Show drill-down content if expanded
    if st.session_state.get(drill_key, False):
        try:
            # Use cached connections
            source_conn = get_cached_connection(source_conn_info)
            target_conn = get_cached_connection(target_conn_info)

            # Validate schema and table names to prevent SQL injection
            try:
                validate_sql_identifier(schema_name, "schema_name")
                validate_sql_identifier(table_name, "table_name")
            except Exception as e:
                st.error(f"Invalid identifier: {e}")
                return

            # Check if incremental comparison filter should be applied
            date_filter = ""
            inc_config = st.session_state.get("incremental_config")
            if inc_config and inc_config.get("table") == table_name:
                date_col = inc_config.get("date_column")
                min_max_date = inc_config.get("min_max_date")

                if date_col and min_max_date:
                    try:
                        validate_sql_identifier(date_col, "date_column")
                        validate_date_value(str(min_max_date), "min_max_date")
                        date_filter = f" WHERE [{date_col}] <= '{min_max_date}'"
                        st.info(f"📅 **Incremental filter active:** Comparing only rows where `{date_col} <= '{min_max_date}'`")
                    except Exception as e:
                        st.error(f"Invalid identifier or date value: {e}")
                        date_filter = ""

            # Fetch data with filter applied to both sides
            query = f"SELECT TOP 1000 * FROM [{schema_name}].[{table_name}]{date_filter}"

            with st.spinner("Loading data from databases..."):
                source_rows = source_conn.execute_query(query)
                target_rows = target_conn.execute_query(query)

            df_source = pd.DataFrame(source_rows) if source_rows else pd.DataFrame()
            df_target = pd.DataFrame(target_rows) if target_rows else pd.DataFrame()

            if df_source.empty and df_target.empty:
                st.info("Both source and target tables are empty.")
                return

            # Get comparable columns
            if not df_source.empty and not df_target.empty:
                common_cols = [c for c in df_source.columns if c in df_target.columns]
                all_compare_cols = [c for c in common_cols if df_source[c].dtype != 'datetime64[ns]'
                               and 'date' not in c.lower() and 'time' not in c.lower() and 'created' not in c.lower()]

                if all_compare_cols:
                    # Column selection
                    st.subheader("⚙️ Column Selection")
                    st.caption("Select columns to include in comparison. Datetime columns are excluded by default.")

                    selected_cols = st.multiselect(
                        "Columns to Compare",
                        options=all_compare_cols,
                        default=all_compare_cols,
                        key=f"drill_cols_{result.source_table}",
                        help="Deselect columns to exclude them from the EXCEPT and row-by-row comparisons"
                    )

                    if not selected_cols:
                        st.warning("Please select at least one column to compare.")
                        return

                    compare_cols = selected_cols

                    # EXCEPT comparison
                    st.subheader("📊 EXCEPT Comparison")

                    source_set = set(df_source[compare_cols].apply(tuple, axis=1))
                    target_set = set(df_target[compare_cols].apply(tuple, axis=1))

                    source_only = source_set - target_set
                    target_only = target_set - source_set

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown(f"**Source EXCEPT Target ({len(source_only)} rows)**")
                        if source_only:
                            source_only_data = [dict(zip(compare_cols, row)) for row in source_only]
                            df_source_only = pd.DataFrame(source_only_data)
                            st.dataframe(df_source_only, use_container_width=True, height=300)
                        else:
                            st.success("✅ All source rows exist in target")

                    with col2:
                        st.markdown(f"**Target EXCEPT Source ({len(target_only)} rows)**")
                        if target_only:
                            target_only_data = [dict(zip(compare_cols, row)) for row in target_only]
                            df_target_only = pd.DataFrame(target_only_data)
                            st.dataframe(df_target_only, use_container_width=True, height=300)
                        else:
                            st.success("✅ All target rows exist in source")

                    # Row-by-row comparison
                    st.subheader("🔍 Row-by-Row Value Differences")

                    key_col = compare_cols[0] if compare_cols else None

                    if len(compare_cols) < 2:
                        st.info("Select at least 2 columns to enable row-by-row comparison. The first column is used as the key to match rows.")
                    elif key_col:
                        st.caption(f"Using **{key_col}** as the key column to match rows between source and target.")

                        df_merged = pd.merge(
                            df_source[compare_cols],
                            df_target[compare_cols],
                            on=key_col,
                            how='inner',
                            suffixes=('_source', '_target')
                        )

                        if not df_merged.empty:
                            diff_rows_list = []
                            for _, row in df_merged.iterrows():
                                row_diff = {'key': row[key_col], 'differences': []}
                                for col in compare_cols[1:]:
                                    src_val = row.get(f'{col}_source')
                                    tgt_val = row.get(f'{col}_target')
                                    if src_val != tgt_val:
                                        row_diff['differences'].append({
                                            'column': col,
                                            'source': src_val,
                                            'target': tgt_val
                                        })
                                if row_diff['differences']:
                                    diff_rows_list.append(row_diff)

                            if diff_rows_list:
                                st.warning(f"Found {len(diff_rows_list)} rows with value differences")

                                # Create a summary table
                                all_diffs = []
                                for diff_row in diff_rows_list:
                                    for d in diff_row['differences']:
                                        all_diffs.append({
                                            'Key': diff_row['key'],
                                            'Column': d['column'],
                                            'Source Value': str(d['source']),
                                            'Target Value': str(d['target']),
                                        })

                                if all_diffs:
                                    df_diffs = pd.DataFrame(all_diffs)

                                    # Highlight function
                                    def highlight_diff(row):
                                        return ['background-color: #ffcccc' if row['Source Value'] != row['Target Value'] else '' for _ in row]

                                    st.dataframe(
                                        df_diffs.style.apply(highlight_diff, axis=1),
                                        use_container_width=True,
                                        height=300
                                    )
                            else:
                                st.success("✅ No value differences in matching rows")
                        else:
                            st.markdown(render_empty_state(
                                "🔗",
                                "No Matching Keys",
                                f"No rows with matching '{key_col}' values found between source and target."
                            ), unsafe_allow_html=True)
                else:
                    st.info("No comparable columns found (all columns are datetime-related).")
            elif df_source.empty:
                st.warning("Source table is empty.")
                if not df_target.empty:
                    st.markdown(f"**Target has {len(df_target)} rows (sample):**")
                    st.dataframe(df_target.head(100), use_container_width=True)
            else:
                st.warning("Target table is empty.")
                st.markdown(f"**Source has {len(df_source)} rows (sample):**")
                st.dataframe(df_source.head(100), use_container_width=True)

        except Exception as e:
            st.error(f"Error fetching data: {e}")
            logger.error(f"Inline drill-down error for {result.source_table}: {e}", exc_info=True)


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