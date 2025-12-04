"""Drill-down detail page for table comparison."""
import sys
import os
import pandas as pd
import streamlit as st

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.core.logging import get_logger
from src.data.database import get_cached_connection
from src.utils.validators import validate_sql_identifier, validate_date_value
from src.ui.styles import apply_professional_style, render_empty_state

logger = get_logger(__name__)

# Apply professional styling
apply_professional_style()


def render() -> None:
    """Render the drill-down detail page."""
    st.title("Drill-Down Analysis")

    # Get drill-down data from session state
    drill_data = st.session_state.get("drill_down_data")

    if not drill_data:
        st.markdown(render_empty_state(
            "🔍",
            "No Table Selected",
            "Run a comparison and click 'View Data Differences' on a table to analyze it here"
        ), unsafe_allow_html=True)
        return

    table_name = drill_data.get("table_name", "Unknown")
    schema_name = drill_data.get("schema_name", "dbo")
    source_conn_info = drill_data.get("source_conn_info")
    target_conn_info = drill_data.get("target_conn_info")
    source_row_count = drill_data.get("source_row_count", 0)
    target_row_count = drill_data.get("target_row_count", 0)

    st.subheader(f"Table: {schema_name}.{table_name}")

    # Summary metrics
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Source Rows", source_row_count)
    with col2:
        st.metric("Target Rows", target_row_count)

    st.markdown("---")

    # Connect and fetch data
    if source_conn_info and target_conn_info:
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
                    # Validate column name and date value to prevent SQL injection
                    try:
                        validate_sql_identifier(date_col, "date_column")
                        validate_date_value(str(min_max_date), "min_max_date")
                        date_filter = f" WHERE [{date_col}] <= '{min_max_date}'"
                        st.info(f"📅 **Incremental filter active:** Comparing only rows where `{date_col} <= '{min_max_date}'` (applied to BOTH source and target)")
                    except Exception as e:
                        st.error(f"Invalid identifier or date value: {e}")
                        date_filter = ""

            # Fetch data with filter applied to both sides
            query = f"SELECT TOP 1000 * FROM [{schema_name}].[{table_name}]{date_filter}"
            source_rows = source_conn.execute_query(query)
            target_rows = target_conn.execute_query(query)

            df_source = pd.DataFrame(source_rows) if source_rows else pd.DataFrame()
            df_target = pd.DataFrame(target_rows) if target_rows else pd.DataFrame()

            # Get comparable columns
            if not df_source.empty and not df_target.empty:
                common_cols = [c for c in df_source.columns if c in df_target.columns]
                all_compare_cols = [c for c in common_cols if df_source[c].dtype != 'datetime64[ns]'
                               and 'date' not in c.lower() and 'time' not in c.lower() and 'created' not in c.lower()]

                if all_compare_cols:
                    # Column selection
                    st.subheader("⚙️ Column Selection")
                    st.markdown("Select columns to include in comparison. Deselect columns to exclude them.")

                    selected_cols = st.multiselect(
                        "Columns to Compare",
                        options=all_compare_cols,
                        default=all_compare_cols,
                        key="drill_down_columns",
                        help="Deselect columns to exclude them from the EXCEPT and row-by-row comparisons"
                    )

                    if not selected_cols:
                        st.warning("Please select at least one column to compare.")
                        source_conn.disconnect()
                        target_conn.disconnect()
                        return

                    compare_cols = selected_cols

                    st.markdown("---")

                    # EXCEPT comparison
                    st.subheader("📊 EXCEPT Comparison")

                    source_set = set(df_source[compare_cols].apply(tuple, axis=1))
                    target_set = set(df_target[compare_cols].apply(tuple, axis=1))

                    source_only = source_set - target_set
                    target_only = target_set - source_set

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown(f"### Source EXCEPT Target ({len(source_only)} rows)")
                        if source_only:
                            # Convert tuples to list of dicts to ensure column alignment
                            source_only_data = [dict(zip(compare_cols, row)) for row in source_only]
                            df_source_only = pd.DataFrame(source_only_data)
                            st.dataframe(df_source_only, use_container_width=True, height=400)
                        else:
                            st.success("✅ All source rows exist in target")

                    with col2:
                        st.markdown(f"### Target EXCEPT Source ({len(target_only)} rows)")
                        if target_only:
                            # Convert tuples to list of dicts to ensure column alignment
                            target_only_data = [dict(zip(compare_cols, row)) for row in target_only]
                            df_target_only = pd.DataFrame(target_only_data)
                            st.dataframe(df_target_only, use_container_width=True, height=400)
                        else:
                            st.success("✅ All target rows exist in source")

                    # Row-by-row comparison
                    st.markdown("---")
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
                                        height=400
                                    )
                            else:
                                st.success("✅ No value differences in matching rows")
                        else:
                            st.markdown(render_empty_state(
                                "🔗",
                                "No Matching Keys",
                                f"No rows with matching '{key_col}' values found between source and target. The rows exist only on one side."
                            ), unsafe_allow_html=True)

            # Don't disconnect - keep connections cached

        except Exception as e:
            st.error(f"Error fetching data: {e}")
            logger.error(f"Drill-down error: {e}", exc_info=True)

    st.markdown("---")
    st.info("Use the sidebar to navigate back to **Comparison** or **Results** pages.")


if __name__ == "__main__":
    render()
