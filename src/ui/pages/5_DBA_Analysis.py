"""DBA Analysis page for workload analysis and connection optimization."""
import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.core.logging import get_logger
from src.data.database import get_cached_connection
from src.services.dba_analysis import DBAAnalysisService
from src.ui.styles import apply_professional_style, render_empty_state

logger = get_logger(__name__)

# Apply professional styling
apply_professional_style()


def render() -> None:
    """Render the DBA Analysis page."""
    st.title("DBA Workload Analysis")
    st.markdown("Analyze database connections, query patterns, and resource consumption.")

    # Check for connection
    source_conn_info = st.session_state.get("source_connection")
    target_conn_info = st.session_state.get("target_connection")

    if not source_conn_info and not target_conn_info:
        st.markdown(render_empty_state(
            "🔌",
            "No Database Connection",
            "Connect to a database from the Connection page first"
        ), unsafe_allow_html=True)
        return

    # Database selection
    st.markdown("### Select Database to Analyze")

    options = []
    if source_conn_info:
        options.append(f"Source: {source_conn_info.get_display_name()}")
    if target_conn_info:
        options.append(f"Target: {target_conn_info.get_display_name()}")

    selected = st.selectbox("Database", options)

    conn_info = source_conn_info if "Source" in selected else target_conn_info

    # Analysis button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        analyze_clicked = st.button("Run Analysis", type="primary", use_container_width=True)

    if analyze_clicked:
        with st.spinner("Analyzing database workload..."):
            try:
                conn = get_cached_connection(conn_info)
                service = DBAAnalysisService(conn)
                result = service.analyze()
                st.session_state["dba_analysis_result"] = result
                st.success("Analysis complete!")
            except Exception as e:
                st.error(f"Analysis failed: {e}")
                logger.error(f"DBA Analysis failed: {e}", exc_info=True)
                return

    # Display results
    result = st.session_state.get("dba_analysis_result")
    if not result:
        st.info("Click 'Run Analysis' to analyze the database workload.")
        return

    st.markdown("---")

    # Summary metrics
    st.markdown("### Summary")
    summary = result.get_summary()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Systems", summary["total_systems"])
    with col2:
        st.metric("Connections", summary["total_connections"])
    with col3:
        st.metric("Blocking Chains", summary["blocking_chains"])
    with col4:
        st.metric("Recommendations", summary["recommendations"])

    # Tabs for different sections
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "System Scorecards",
        "Expensive Queries",
        "Blocking Analysis",
        "Redundancies",
        "Recommendations"
    ])

    with tab1:
        render_scorecards(result)

    with tab2:
        render_expensive_queries(result)

    with tab3:
        render_blocking_analysis(result)

    with tab4:
        render_redundancies(result)

    with tab5:
        render_recommendations(result)


def render_scorecards(result) -> None:
    """Render system scorecards section."""
    st.markdown("### System-by-System Scorecard")
    st.caption("Systems ranked by resource cost (higher score = more resource consumption)")

    if not result.system_scorecards:
        st.info("No system data available")
        return

    # Create scorecard dataframe
    data = []
    for sc in result.system_scorecards:
        data.append({
            "Rank": sc.rank,
            "System": sc.system_name,
            "Host": sc.host_name,
            "Connections": sc.total_connections,
            "Active": sc.active_connections,
            "Idle": sc.idle_connections,
            "CPU (sec)": f"{sc.cpu_cost_seconds:.1f}",
            "I/O Reads": f"{sc.io_reads:,}",
            "I/O Writes": f"{sc.io_writes:,}",
            "Memory (MB)": f"{sc.memory_mb:.1f}",
            "Blocking": sc.times_blocking,
            "Blocked": sc.times_blocked,
            "Score": f"{sc.resource_score:.1f}",
        })

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, height=400)

    # Resource distribution chart
    if len(result.system_scorecards) > 1:
        st.markdown("#### Resource Distribution by System")

        fig = go.Figure()

        systems = [sc.system_name[:20] for sc in result.system_scorecards[:10]]
        cpu_values = [sc.cpu_cost_seconds for sc in result.system_scorecards[:10]]
        io_values = [(sc.io_reads + sc.io_writes) / 10000 for sc in result.system_scorecards[:10]]

        fig.add_trace(go.Bar(name='CPU (seconds)', x=systems, y=cpu_values))
        fig.add_trace(go.Bar(name='I/O (x10K)', x=systems, y=io_values))

        fig.update_layout(
            barmode='group',
            title='Top 10 Systems by Resource Usage',
            xaxis_title='System',
            yaxis_title='Resource Units',
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)


def render_expensive_queries(result) -> None:
    """Render expensive queries section."""
    st.markdown("### Top Expensive Queries")
    st.caption("Queries with highest CPU and I/O cost")

    if not result.top_expensive_queries:
        st.success("No expensive queries detected")
        return

    for i, query in enumerate(result.top_expensive_queries[:10], 1):
        with st.expander(f"#{i} - {query.source_program} - Cost Score: {query.get_cost_score():.1f}"):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Executions", f"{query.execution_count:,}")
            with col2:
                st.metric("Avg CPU (ms)", f"{query.avg_worker_time_ms:.1f}")
            with col3:
                st.metric("Avg Elapsed (ms)", f"{query.avg_elapsed_time_ms:.1f}")
            with col4:
                st.metric("Avg Reads", f"{query.avg_logical_reads:,.0f}")

            st.markdown("**Query:**")
            st.code(query.query_text[:2000], language="sql")

            if query.plan_count > 1:
                st.warning(f"Multiple execution plans detected: {query.plan_count} plans")


def render_blocking_analysis(result) -> None:
    """Render blocking analysis section."""
    st.markdown("### Current Blocking Chains")

    if not result.current_blocking:
        st.success("No active blocking detected")
    else:
        st.warning(f"Found {len(result.current_blocking)} blocking chains")

        for b in result.current_blocking:
            with st.expander(
                f"Session {b.blocking_session_id} blocking {b.blocked_session_id} "
                f"({b.get_wait_time_seconds():.1f}s)"
            ):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Blocker**")
                    st.write(f"Program: {b.blocking_program}")
                    st.write(f"Host: {b.blocking_host}")
                    if b.blocking_query:
                        st.code(b.blocking_query[:500], language="sql")

                with col2:
                    st.markdown("**Blocked**")
                    st.write(f"Program: {b.blocked_program}")
                    st.write(f"Host: {b.blocked_host}")
                    st.write(f"Wait Type: {b.wait_type}")
                    st.write(f"Resource: {b.wait_resource}")
                    if b.blocked_query:
                        st.code(b.blocked_query[:500], language="sql")

    # Blocking hotspots
    if result.blocking_hotspots:
        st.markdown("### Blocking Hotspots")
        for hotspot in result.blocking_hotspots:
            st.warning(hotspot)


def render_redundancies(result) -> None:
    """Render redundancy findings section."""
    st.markdown("### Cross-System Redundancy Findings")
    st.caption("Queries executed by multiple systems that could be consolidated")

    if not result.redundancy_findings:
        st.success("No redundancy issues detected")
        return

    # Group by severity
    high = [f for f in result.redundancy_findings if f.severity == "high"]
    medium = [f for f in result.redundancy_findings if f.severity == "medium"]
    low = [f for f in result.redundancy_findings if f.severity == "low"]

    if high:
        st.markdown("#### High Severity")
        for finding in high:
            with st.expander(f"Executions: {finding.total_executions:,} - {finding.query_pattern[:60]}..."):
                st.markdown(f"**Systems involved:** {', '.join(finding.systems_involved)}")
                st.markdown(f"**Potential savings:** {finding.potential_savings_percent:.0f}%")
                st.markdown(f"**Recommendation:** {finding.recommendation}")
                st.code(finding.query_pattern, language="sql")

    if medium:
        st.markdown("#### Medium Severity")
        for finding in medium[:5]:
            with st.expander(f"Executions: {finding.total_executions:,} - {finding.query_pattern[:60]}..."):
                st.markdown(f"**Systems involved:** {', '.join(finding.systems_involved)}")
                st.markdown(f"**Potential savings:** {finding.potential_savings_percent:.0f}%")
                st.markdown(f"**Recommendation:** {finding.recommendation}")

    if low and st.checkbox("Show low severity findings"):
        st.markdown("#### Low Severity")
        for finding in low[:5]:
            st.write(f"- {finding.query_pattern[:80]}... ({finding.total_executions:,} executions)")


def render_recommendations(result) -> None:
    """Render recommendations section."""
    st.markdown("### Prioritized Recommendations")

    if not result.recommendations:
        st.success("No recommendations - database workload looks healthy!")
        return

    # Group by priority
    high = [r for r in result.recommendations if r.startswith("HIGH:")]
    medium = [r for r in result.recommendations if r.startswith("MEDIUM:")]
    low = [r for r in result.recommendations if r.startswith("LOW:")]

    if high:
        st.markdown("#### High Priority")
        for rec in high:
            st.markdown(f'''
            <div style="background: #fef2f2; border-left: 4px solid #ef4444; padding: 1rem;
                        border-radius: 4px; margin-bottom: 0.5rem;">
                {rec.replace("HIGH:", "").strip()}
            </div>
            ''', unsafe_allow_html=True)

    if medium:
        st.markdown("#### Medium Priority")
        for rec in medium:
            st.markdown(f'''
            <div style="background: #fefce8; border-left: 4px solid #eab308; padding: 1rem;
                        border-radius: 4px; margin-bottom: 0.5rem;">
                {rec.replace("MEDIUM:", "").strip()}
            </div>
            ''', unsafe_allow_html=True)

    if low:
        st.markdown("#### Low Priority")
        for rec in low:
            st.markdown(f'''
            <div style="background: #f0fdf4; border-left: 4px solid #22c55e; padding: 1rem;
                        border-radius: 4px; margin-bottom: 0.5rem;">
                {rec.replace("LOW:", "").strip()}
            </div>
            ''', unsafe_allow_html=True)

    # Export recommendations
    st.markdown("---")
    if st.button("Export Recommendations"):
        report = f"""# DBA Analysis Report
Generated: {result.analyzed_at.strftime('%Y-%m-%d %H:%M:%S')}
Server: {result.server_name}
Database: {result.database_name}

## Summary
- Total Systems: {len(result.connection_sources)}
- Total Connections: {result.total_connections}
- Active: {result.total_active}
- Idle: {result.total_idle}
- Blocking Chains: {len(result.current_blocking)}

## Recommendations

### High Priority
{chr(10).join(['- ' + r.replace('HIGH:', '').strip() for r in high])}

### Medium Priority
{chr(10).join(['- ' + r.replace('MEDIUM:', '').strip() for r in medium])}

### Low Priority
{chr(10).join(['- ' + r.replace('LOW:', '').strip() for r in low])}
"""
        st.download_button(
            "Download Report",
            report,
            file_name=f"dba_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
            mime="text/markdown"
        )


if __name__ == "__main__":
    render()
