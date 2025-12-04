"""Comparison execution page."""
import sys
import os
import re
import json
import pickle
import streamlit as st
import importlib
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load cache first
from src.ui.cache_loader import load_all_cache
load_all_cache()

from src.core.logging import get_logger
from src.core.config import get_settings
import src.data.database
from src.data.database import get_cached_connection
importlib.reload(src.data.database)
from src.data.models import ComparisonMode
from src.data.repositories import MetadataRepository
from src.services.comparison import ComparisonService
from src.utils.formatters import format_duration, format_number
from src.utils.validators import validate_sql_identifier

logger = get_logger(__name__)

CACHE_DIR = os.path.join(project_root, "config")
TABLES_CACHE = os.path.join(CACHE_DIR, "tables_cache.json")
RESULTS_CACHE = os.path.join(CACHE_DIR, "results_cache.pkl")


def load_cached_state():
    """Load cached state from files."""
    # Load tables cache
    try:
        if os.path.exists(TABLES_CACHE):
            with open(TABLES_CACHE, "r") as f:
                data = json.load(f)
                if "available_tables" not in st.session_state:
                    st.session_state.available_tables = data.get("available_tables", [])
    except Exception:
        pass

    # Load results cache
    try:
        if os.path.exists(RESULTS_CACHE):
            with open(RESULTS_CACHE, "rb") as f:
                results = pickle.load(f)
                if "comparison_results" not in st.session_state:
                    st.session_state.comparison_results = results
    except Exception:
        pass


def save_tables_cache(tables: list):
    """Save tables list to cache."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(TABLES_CACHE, "w") as f:
            json.dump({"available_tables": tables}, f)
    except Exception:
        pass


def save_results_cache(results: list):
    """Save comparison results to cache."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(RESULTS_CACHE, "wb") as f:
            pickle.dump(results, f)
    except Exception:
        pass


def render() -> None:
    """Render the comparison page."""
    st.title("⚖️ Table Comparison")
    st.markdown("Select tables and comparison options to analyze differences.")

    # Load cached state on first run
    if "comparison_cache_loaded" not in st.session_state:
        load_cached_state()
        st.session_state.comparison_cache_loaded = True

    # Get connections from session state
    source_conn_info = st.session_state.get("source_connection")
    target_conn_info = st.session_state.get("target_connection")

    if not source_conn_info or not target_conn_info:
        st.warning("⚠️ Please configure and test connections first on the Connection page.")
        return

    # Comparison options
    st.subheader("Comparison Options")

    schema_name = st.text_input(
        "Schema Name",
        value="dbo",
        help="Schema to compare (e.g., dbo)",
    )

    # Fixed comparison mode
    comparison_mode = "Quick"

    # Get available tables
    if st.button("🔍 Load Tables"):
        load_tables(source_conn_info, target_conn_info, schema_name)

    # Table selection
    st.subheader("Select Tables to Compare")

    available_tables = st.session_state.get("available_tables", [])

    if available_tables:
        # Filter options
        col1, col2 = st.columns([2, 2])
        with col1:
            table_filter = st.text_input(
                "🔍 Filter tables (regex)",
                value="",
                help="Filter tables by regex pattern. Examples: ^mrr_, ^stg_, ^dwh_, .*_fact$",
                placeholder="e.g., ^mrr_ or ^stg_"
            )
        with col2:
            preset_filters = st.selectbox(
                "Quick filters",
                options=["All", "mrr_*", "stg_*", "dwh_*", "dim_*", "fact_*", "link_*", "lnk_*"],
                help="Select a preset filter"
            )

        # Apply filter
        if preset_filters != "All":
            pattern = f"^{preset_filters.replace('*', '.*')}"
        elif table_filter:
            pattern = table_filter
        else:
            pattern = None

        if pattern:
            try:
                regex = re.compile(pattern, re.IGNORECASE)
                filtered_tables = [t for t in available_tables if regex.search(t)]
            except re.error:
                st.warning("Invalid regex pattern")
                filtered_tables = available_tables
        else:
            filtered_tables = available_tables

        st.caption(f"Showing {len(filtered_tables)} of {len(available_tables)} tables")

        # Separate restricted tables (fact/link) from unrestricted tables
        def is_restricted(table_name):
            name_lower = table_name.lower()
            return 'fact' in name_lower or 'link' in name_lower

        restricted_tables = [t for t in filtered_tables if is_restricted(t)]
        unrestricted_tables = [t for t in filtered_tables if not is_restricted(t)]

        # Select all checkbox (only for unrestricted tables)
        select_all = st.checkbox("Select All Filtered (excludes fact/link tables)")

        # Table list with multiselect
        if select_all:
            # Only auto-select unrestricted tables
            selected_tables = st.multiselect(
                "Tables",
                options=filtered_tables,
                default=unrestricted_tables,
                help="Select tables to compare. Fact/Link tables: select ONE at a time",
            )
        else:
            selected_tables = st.multiselect(
                "Tables",
                options=filtered_tables,
                help="Select tables to compare. Fact/Link tables: select ONE at a time",
            )

        # Validate selection - if fact/link table is selected, only ONE table allowed total
        selected_restricted = [t for t in selected_tables if is_restricted(t)]
        if len(selected_restricted) > 0 and len(selected_tables) > 1:
            st.error(f"⚠️ When selecting a Fact/Link table, you can only select ONE table. You selected: {', '.join(selected_tables)}")
            st.info("Please select only one fact or link table mother fucker")
            selected_tables = []  # Clear selection to prevent comparison

        st.caption(f"Selected {len(selected_tables)} tables ({len(selected_restricted)} fact/link, {len(selected_tables) - len(selected_restricted)} other)")

        # Incremental comparison option for fact tables
        incremental_config = None
        if len(selected_restricted) == 1 and 'fact' in selected_restricted[0].lower():
            fact_table = selected_restricted[0]
            st.markdown("---")
            st.subheader("📅 Incremental Comparison")

            is_incremental = st.checkbox("Is Incremental", key="is_incremental")

            if is_incremental:
                try:
                    # Get date columns from the fact table
                    source_conn = get_cached_connection(source_conn_info)
                    date_columns_query = f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{schema_name}'
                        AND TABLE_NAME = '{fact_table}'
                        AND DATA_TYPE IN ('date', 'datetime', 'datetime2', 'smalldatetime')
                        ORDER BY COLUMN_NAME
                    """
                    date_cols_result = source_conn.execute_query(date_columns_query)
                    date_columns = [row['COLUMN_NAME'] for row in date_cols_result]

                    if date_columns:
                        selected_date_col = st.selectbox(
                            "Select Date Column",
                            options=date_columns,
                            key="incremental_date_column"
                        )

                        if selected_date_col:
                            # Get max dates from both databases to show preview
                            target_conn = get_cached_connection(target_conn_info)

                            source_max_q = f"SELECT MAX([{selected_date_col}]) as max_val FROM [{schema_name}].[{fact_table}]"
                            target_max_q = f"SELECT MAX([{selected_date_col}]) as max_val FROM [{schema_name}].[{fact_table}]"

                            source_max_result = source_conn.execute_query(source_max_q)
                            target_max_result = target_conn.execute_query(target_max_q)

                            source_max = source_max_result[0]['max_val'] if source_max_result and source_max_result[0]['max_val'] else None
                            target_max = target_max_result[0]['max_val'] if target_max_result and target_max_result[0]['max_val'] else None

                            # Show max dates
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric(f"Source Max ({selected_date_col})", str(source_max) if source_max else "No data")
                            with col2:
                                st.metric(f"Target Max ({selected_date_col})", str(target_max) if target_max else "No data")

                            # Calculate and show filter that will be applied
                            min_max_date = None
                            if source_max and target_max:
                                if str(source_max) != str(target_max):
                                    min_max_date = min(source_max, target_max)
                                    st.warning(f"⚠️ Max dates differ! Comparison will filter BOTH source and target to: `{selected_date_col} <= '{min_max_date}'`")
                                else:
                                    st.success("✅ Max dates match - no filtering needed")

                            # Store config for comparison
                            incremental_config = {
                                "table": fact_table,
                                "schema": schema_name,
                                "date_column": selected_date_col,
                                "source_conn_info": source_conn_info,
                                "target_conn_info": target_conn_info,
                                "source_max": str(source_max) if source_max else None,
                                "target_max": str(target_max) if target_max else None,
                                "min_max_date": str(min_max_date) if min_max_date else None
                            }
                            st.session_state["incremental_config"] = incremental_config
                    else:
                        st.warning("No date columns found in this fact table")
                except Exception as e:
                    st.error(f"Error fetching date columns: {e}")
            else:
                # Clear incremental config if unchecked
                if "incremental_config" in st.session_state:
                    del st.session_state["incremental_config"]

        # Column difference checking
        if selected_tables:
            st.markdown("---")
            st.subheader("Column Validation")
            if st.button("🔍 Check Column Differences", use_container_width=True):
                check_column_differences(
                    source_conn_info,
                    target_conn_info,
                    schema_name,
                    selected_tables,
                )

        # Default values
        chunk_size = 10000
        max_workers = 4

        # Start comparison button
        st.markdown("---")
        if st.button("🚀 Start Comparison", type="primary", use_container_width=True):
            if not selected_tables:
                st.warning("Please select at least one table to compare.")
            else:
                run_comparison(
                    source_conn_info,
                    target_conn_info,
                    schema_name,
                    selected_tables,
                    comparison_mode,
                    chunk_size,
                    max_workers,
                )
    else:
        st.info("👆 Click 'Load Tables' to retrieve available tables.")

    # Log viewer section
    with st.expander("📋 View Application Logs", expanded=False):
        render_log_viewer()


def check_column_differences(source_conn_info, target_conn_info, schema_name: str, tables: list[str]) -> None:
    """Check column differences between source and target for selected tables."""
    try:
        with st.spinner("Checking column differences..."):
            # Get cached connections
            source_conn = get_cached_connection(source_conn_info)
            source_repo = MetadataRepository(source_conn)

            target_conn = get_cached_connection(target_conn_info)
            target_repo = MetadataRepository(target_conn)

            has_differences = False

            for table_name in tables:
                # Get columns from both databases
                source_cols = {c.column_name: c for c in source_repo.get_table_columns(schema_name, table_name)}
                target_cols = {c.column_name: c for c in target_repo.get_table_columns(schema_name, table_name)}

                # Find differences
                source_only = set(source_cols.keys()) - set(target_cols.keys())
                target_only = set(target_cols.keys()) - set(source_cols.keys())

                if source_only or target_only:
                    has_differences = True
                    with st.expander(f"⚠️ {table_name} - Column Differences", expanded=True):
                        col1, col2 = st.columns(2)

                        with col1:
                            if source_only:
                                st.markdown("**Source EXCEPT Target:**")
                                for col in sorted(source_only):
                                    st.write(f"• {col} ({source_cols[col].get_full_type()})")

                                # Show 10 sample rows for source-only columns
                                st.markdown("**Sample data (10 rows):**")
                                try:
                                    cols_list = list(source_only)[:5]  # Limit columns shown
                                    query = f"SELECT TOP 10 [{'], ['.join(cols_list)}] FROM [{schema_name}].[{table_name}]"
                                    result = source_conn.execute(query)
                                    if result:
                                        import pandas as pd
                                        df = pd.DataFrame(result.fetchall(), columns=cols_list)
                                        st.dataframe(df, use_container_width=True)
                                except Exception as e:
                                    st.caption(f"Could not fetch sample: {e}")
                            else:
                                st.success("No columns only in source")

                        with col2:
                            if target_only:
                                st.markdown("**Target EXCEPT Source:**")
                                for col in sorted(target_only):
                                    st.write(f"• {col} ({target_cols[col].get_full_type()})")

                                # Show 10 sample rows for target-only columns
                                st.markdown("**Sample data (10 rows):**")
                                try:
                                    cols_list = list(target_only)[:5]  # Limit columns shown
                                    query = f"SELECT TOP 10 [{'], ['.join(cols_list)}] FROM [{schema_name}].[{table_name}]"
                                    result = target_conn.execute(query)
                                    if result:
                                        import pandas as pd
                                        df = pd.DataFrame(result.fetchall(), columns=cols_list)
                                        st.dataframe(df, use_container_width=True)
                                except Exception as e:
                                    st.caption(f"Could not fetch sample: {e}")
                            else:
                                st.success("No columns only in target")

            # Don't disconnect - keep connections cached

            if not has_differences:
                st.success("✅ All tables have matching columns!")
            else:
                st.warning(f"⚠️ Found column differences in some tables")

    except Exception as e:
        st.error(f"Failed to check columns: {str(e)}")
        logger.error(f"Failed to check columns: {str(e)}", exc_info=True)


def load_tables(source_conn_info, target_conn_info, schema_name: str) -> None:
    """Load available tables from both databases."""
    try:
        with st.spinner("Loading tables..."):
            # Get cached connections
            source_conn = get_cached_connection(source_conn_info)
            source_repo = MetadataRepository(source_conn)

            target_conn = get_cached_connection(target_conn_info)
            target_repo = MetadataRepository(target_conn)

            # Get tables
            source_tables = {t.table_name for t in source_repo.get_tables(schema_name)}
            target_tables = {t.table_name for t in target_repo.get_tables(schema_name)}

            # Get common tables
            common_tables = sorted(list(source_tables & target_tables))

            # Don't disconnect - keep connections cached

            # Show tables only in source or target
            source_only = sorted(list(source_tables - target_tables))
            target_only = sorted(list(target_tables - source_tables))

            if common_tables:
                st.session_state.available_tables = common_tables
                save_tables_cache(common_tables)
                with st.expander(f"✅ {len(common_tables)} common tables", expanded=False):
                    for table in common_tables:
                        st.write(f"• {table}")

                if source_only:
                    with st.expander(f"⚠️ {len(source_only)} tables exist only in source database", expanded=True):
                        for table in source_only:
                            st.write(f"• {table}")
                if target_only:
                    with st.expander(f"⚠️ {len(target_only)} tables exist only in target database", expanded=True):
                        for table in target_only:
                            st.write(f"• {table}")
            else:
                st.error("❌ No common tables found in the specified schema")
                st.session_state.available_tables = []
                if source_only:
                    with st.expander(f"⚠️ {len(source_only)} tables exist only in source database", expanded=True):
                        for table in source_only:
                            st.write(f"• {table}")
                if target_only:
                    with st.expander(f"⚠️ {len(target_only)} tables exist only in target database", expanded=True):
                        for table in target_only:
                            st.write(f"• {table}")

    except Exception as e:
        st.error(f"Failed to load tables: {str(e)}")
        logger.error(f"Failed to load tables: {str(e)}", exc_info=True)


def run_comparison(
    source_conn_info,
    target_conn_info,
    schema_name: str,
    selected_tables: list[str],
    comparison_mode: str,
    chunk_size: int,
    max_workers: int,
) -> None:
    """Run the comparison process."""
    try:
        # Use Quick comparison mode
        mode = ComparisonMode.QUICK

        # Get cached connections
        source_conn = get_cached_connection(source_conn_info)
        target_conn = get_cached_connection(target_conn_info)

        # Create comparison service
        comparison_service = ComparisonService(source_conn, target_conn)

        # Progress tracking
        st.subheader("Comparison Progress")
        progress_bar = st.progress(0)
        status_text = st.empty()
        results_container = st.container()

        results = []
        completed = 0
        total = len(selected_tables)

        # Run comparisons
        for result in comparison_service.compare_multiple_tables(
            schema_name,
            schema_name,
            selected_tables,
            mode,
            max_workers,
        ):
            completed += 1
            progress = completed / total

            # Update progress
            progress_bar.progress(progress)
            status_text.text(
                f"Comparing table {completed}/{total}: {result.source_table}"
            )

            # Store result
            results.append(result)

            # Show live results
            with results_container:
                display_result_summary(result, source_conn, target_conn)

        # Don't disconnect - keep connections cached

        # Store results in session state and cache
        st.session_state.comparison_results = results
        save_results_cache(results)

        # Show completion
        st.success(f"✅ Comparison completed! Analyzed {total} tables.")
        st.balloons()

        # Show incremental max values comparison if configured
        if "incremental_config" in st.session_state:
            inc_config = st.session_state["incremental_config"]
            st.markdown("---")
            st.subheader("📅 Incremental Date Column Max Values")
            try:
                table = inc_config["table"]
                schema = inc_config["schema"]
                date_col = inc_config["date_column"]

                # Get max from source
                source_max_query = f"SELECT MAX([{date_col}]) as max_val FROM [{schema}].[{table}]"
                source_max_result = source_conn.execute_query(source_max_query)
                source_max = source_max_result[0]['max_val'] if source_max_result and source_max_result[0]['max_val'] else "No data"

                # Get max from target
                target_max_query = f"SELECT MAX([{date_col}]) as max_val FROM [{schema}].[{table}]"
                target_max_result = target_conn.execute_query(target_max_query)
                target_max = target_max_result[0]['max_val'] if target_max_result and target_max_result[0]['max_val'] else "No data"

                col1, col2 = st.columns(2)
                with col1:
                    st.metric(f"Source Max ({date_col})", str(source_max))
                with col2:
                    st.metric(f"Target Max ({date_col})", str(target_max))

                # Show if they match
                if str(source_max) == str(target_max):
                    st.success("✅ Max dates match!")
                else:
                    st.warning("⚠️ Max dates differ between source and target")
            except Exception as e:
                st.error(f"Error fetching max values: {e}")

        # Summary statistics
        st.markdown("---")
        show_comparison_summary(results)

    except Exception as e:
        st.error(f"Comparison failed: {str(e)}")
        logger.error(f"Comparison failed: {str(e)}", exc_info=True)


def display_result_summary(result, source_conn=None, target_conn=None) -> None:
    """Display a single comparison result summary."""
    status_emoji = "✅" if result.is_match() else "⚠️" if result.status == "completed" else "❌"

    with st.expander(f"{status_emoji} {result.source_table}"):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Source Rows", format_number(result.source_row_count))

        with col2:
            st.metric("Target Rows", format_number(result.target_row_count))

        with col3:
            st.metric("Match %", f"{result.get_match_percentage():.1f}%")

        if not result.is_match():
            st.caption(f"Summary: {result.get_summary()}")

        # Drill-down for differences
        if not result.is_match() and source_conn and target_conn:
            import pandas as pd
            st.markdown("---")

            # Button to open drill-down page
            parts = result.source_table.split(".")
            schema_name = parts[0] if len(parts) > 1 else "dbo"
            table_name = parts[-1]

            if st.button(f"🔎 Open Drill-Down Details", key=f"drill_{result.source_table}"):
                # Store drill-down data in session state
                st.session_state.drill_down_data = {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "source_conn_info": source_conn.connection_info,
                    "target_conn_info": target_conn.connection_info,
                    "source_row_count": result.source_row_count,
                    "target_row_count": result.target_row_count,
                }
                st.info("✅ Data loaded! Click **Drill_Down** in the sidebar to view details.")

            st.markdown("**🔍 Data Comparison (EXCEPT):**")

            # Parse schema and table from result
            parts = result.source_table.split(".")
            schema_name = parts[0] if len(parts) > 1 else "dbo"
            table_name = parts[-1]

            try:
                # Check if incremental comparison is enabled and get date filter
                date_filter = ""
                inc_config = st.session_state.get("incremental_config")
                if inc_config and inc_config.get("table") == table_name:
                    date_col = inc_config.get("date_column")
                    min_max_date = inc_config.get("min_max_date")

                    if date_col and min_max_date:
                        # Validate column name to prevent SQL injection
                        try:
                            validate_sql_identifier(date_col, "date_column")
                            # Build safe query with validated identifiers
                            date_filter = f" WHERE [{date_col}] <= '{min_max_date}'"
                            st.info(f"📅 Filtering BOTH source and target: `{date_col} <= '{min_max_date}'`")
                        except Exception as e:
                            st.error(f"Invalid date column name: {e}")
                            date_filter = ""

                # Validate schema and table names
                validate_sql_identifier(schema_name, "schema_name")
                validate_sql_identifier(table_name, "table_name")

                # Fetch data from both tables with same filter (limit to reasonable size)
                query = f"SELECT TOP 1000 * FROM [{schema_name}].[{table_name}]{date_filter}"
                source_rows = source_conn.execute_query(query)
                target_rows = target_conn.execute_query(query)

                df_source = pd.DataFrame(source_rows) if source_rows else pd.DataFrame()
                df_target = pd.DataFrame(target_rows) if target_rows else pd.DataFrame()

                # Get common columns (exclude datetime for comparison)
                if not df_source.empty and not df_target.empty:
                    common_cols = [c for c in df_source.columns if c in df_target.columns]
                    # Exclude datetime columns for comparison
                    compare_cols = [c for c in common_cols if df_source[c].dtype != 'datetime64[ns]' and 'date' not in c.lower() and 'time' not in c.lower() and 'created' not in c.lower()]

                    if compare_cols:
                        # Convert to tuples for set comparison
                        source_set = set(df_source[compare_cols].apply(tuple, axis=1))
                        target_set = set(df_target[compare_cols].apply(tuple, axis=1))

                        # Source EXCEPT Target
                        source_only = source_set - target_set
                        # Target EXCEPT Source
                        target_only = target_set - source_set

                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown(f"**Source EXCEPT Target** ({len(source_only)} rows)")
                            if source_only:
                                df_source_only = pd.DataFrame(list(source_only)[:10], columns=compare_cols)
                                st.dataframe(df_source_only, use_container_width=True)
                            else:
                                st.success("0 - All source rows exist in target")

                        with col2:
                            st.markdown(f"**Target EXCEPT Source** ({len(target_only)} rows)")
                            if target_only:
                                df_target_only = pd.DataFrame(list(target_only)[:10], columns=compare_cols)
                                st.dataframe(df_target_only, use_container_width=True)
                            else:
                                st.success("0 - All target rows exist in source")

                        # Find rows with same key but different values (highlight differences)
                        key_col = compare_cols[0] if compare_cols else None
                        if key_col and len(compare_cols) > 1:
                            st.markdown("---")
                            st.markdown("**🔎 Row-by-Row Comparison (Differences Highlighted):**")

                            # Merge on key column
                            df_merged = pd.merge(
                                df_source[compare_cols],
                                df_target[compare_cols],
                                on=key_col,
                                how='inner',
                                suffixes=('_source', '_target')
                            )

                            if not df_merged.empty:
                                # Find rows with differences
                                diff_rows_list = []
                                for _, row in df_merged.head(10).iterrows():
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
                                    for diff_row in diff_rows_list[:5]:
                                        with st.expander(f"🔴 Key: {diff_row['key']}"):
                                            diff_data = []
                                            for d in diff_row['differences']:
                                                diff_data.append({
                                                    'Column': d['column'],
                                                    'Source Value': str(d['source']),
                                                    'Target Value': str(d['target']),
                                                    'Status': '⚠️ DIFFERENT'
                                                })
                                            if diff_data:
                                                st.dataframe(
                                                    pd.DataFrame(diff_data),
                                                    use_container_width=True,
                                                    hide_index=True
                                                )
                                else:
                                    st.success("No value differences in matching rows")
                            else:
                                st.info("No matching keys between source and target")
                    else:
                        st.warning("No comparable columns found")
                else:
                    st.info("One or both tables are empty")

            except Exception as e:
                st.warning(f"Could not compare: {e}")


def show_comparison_summary(results: list) -> None:
    """Show overall comparison summary."""
    st.subheader("Summary Statistics")

    total_tables = len(results)
    matching_tables = sum(1 for r in results if r.is_match())
    failed_tables = sum(1 for r in results if r.status == "failed")
    different_tables = total_tables - matching_tables - failed_tables

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Tables", total_tables)

    with col2:
        st.metric("✅ Matching", matching_tables)

    with col3:
        st.metric("⚠️ Different", different_tables)

    with col4:
        st.metric("❌ Failed", failed_tables)

    # Total rows
    total_source_rows = sum(r.source_row_count for r in results)
    total_target_rows = sum(r.target_row_count for r in results)
    total_duration = sum(r.duration_seconds for r in results)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Source Rows", format_number(total_source_rows))

    with col2:
        st.metric("Total Target Rows", format_number(total_target_rows))

    with col3:
        st.metric("Total Duration", format_duration(total_duration))

def render_log_viewer() -> None:
    """Render the log viewer section."""
    settings = get_settings()
    log_file = settings.logging.file_path

    # Controls row
    col1, col2, col3 = st.columns([2, 1, 2])
    with col1:
        num_lines = st.slider("Lines to show", min_value=10, max_value=200, value=50, step=10)
    with col2:
        st.button("🔄 Refresh")

    try:
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                lines = f.readlines()

            # Get last N lines
            recent_lines = lines[-num_lines:] if len(lines) > num_lines else lines

            # Display in a code block with scrolling
            log_content = "".join(recent_lines)
            st.code(log_content, language="log")

            st.caption(f"Showing last {len(recent_lines)} of {len(lines)} lines from {log_file}")
        else:
            st.info(f"Log file not found: {log_file}")

    except Exception as e:
        st.error(f"Error reading log file: {e}")


if __name__ == "__main__":
    render()
