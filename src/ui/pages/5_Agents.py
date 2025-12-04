"""
Multi-System Agents Management Page.

This page provides a UI for managing intelligent agents that operate across
multiple database systems for comparison, monitoring, and synchronization.
"""

import streamlit as st
import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.services.agents import (
    AgentOrchestrator,
    AgentPriority,
    AgentStatus,
    AgentTask,
    ComparisonAgent,
    MonitoringAgent,
    SyncAgent,
    SystemConnection,
    SystemType,
    get_orchestrator,
)

st.set_page_config(
    page_title="Multi-System Agents",
    page_icon="🤖",
    layout="wide",
)

st.title("🤖 Multi-System Agents")
st.markdown("Manage intelligent agents for cross-database operations")

# Initialize session state
if "orchestrator" not in st.session_state:
    st.session_state.orchestrator = get_orchestrator()

orchestrator = st.session_state.orchestrator

# Tabs for different sections
tab_overview, tab_create, tab_systems, tab_tasks = st.tabs([
    "📊 Overview",
    "➕ Create Agent",
    "🖥️ Register Systems",
    "📋 Submit Tasks",
])

with tab_overview:
    st.header("Agent Overview")

    agents = orchestrator.list_agents()

    if not agents:
        st.info("No agents created yet. Go to 'Create Agent' tab to create one.")
    else:
        # Display agents in columns
        cols = st.columns(3)

        for i, agent in enumerate(agents):
            with cols[i % 3]:
                status_emoji = {
                    "idle": "⚪",
                    "running": "🟢",
                    "paused": "🟡",
                    "completed": "✅",
                    "failed": "🔴",
                    "cancelled": "⛔",
                }.get(agent["status"], "⚪")

                with st.container():
                    st.markdown(f"### {status_emoji} {agent['name']}")
                    st.caption(f"ID: `{agent['agent_id'][:8]}...`")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Status", agent["status"].upper())
                    with col2:
                        st.metric("Queue", agent["queue_size"])

                    # Action buttons
                    btn_col1, btn_col2, btn_col3 = st.columns(3)

                    with btn_col1:
                        if agent["status"] == "idle":
                            if st.button("▶️ Start", key=f"start_{agent['agent_id']}"):
                                orchestrator.start_agent(agent["agent_id"])
                                st.rerun()
                        elif agent["status"] == "running":
                            if st.button("⏹️ Stop", key=f"stop_{agent['agent_id']}"):
                                orchestrator.stop_agent(agent["agent_id"])
                                st.rerun()

                    with btn_col2:
                        if st.button("🔄 Refresh", key=f"refresh_{agent['agent_id']}"):
                            st.rerun()

                    with btn_col3:
                        if st.button("🗑️ Delete", key=f"delete_{agent['agent_id']}"):
                            orchestrator.remove_agent(agent["agent_id"])
                            st.rerun()

                    st.divider()

with tab_create:
    st.header("Create New Agent")

    agent_type = st.selectbox(
        "Agent Type",
        options=["comparison", "monitoring", "sync"],
        format_func=lambda x: {
            "comparison": "🔍 Comparison Agent - Compare data across systems",
            "monitoring": "📡 Monitoring Agent - Monitor system health",
            "sync": "🔄 Sync Agent - Synchronize data between systems",
        }.get(x, x),
    )

    custom_id = st.text_input(
        "Custom Agent ID (optional)",
        placeholder="Leave blank for auto-generated ID",
    )

    st.markdown("---")

    # Agent type descriptions
    descriptions = {
        "comparison": """
        **Comparison Agent** performs intelligent data comparisons across multiple database systems.

        Features:
        - Schema comparison between source and target
        - Data difference detection
        - Row-by-row analysis
        - Cross-database type support (SQL Server, PostgreSQL, MySQL)
        """,
        "monitoring": """
        **Monitoring Agent** tracks health and performance metrics across database systems.

        Features:
        - Real-time health checks
        - Performance metrics collection
        - Alert generation on thresholds
        - Historical metrics tracking
        """,
        "sync": """
        **Sync Agent** synchronizes data between database systems.

        Features:
        - Full and incremental sync modes
        - Sync script generation
        - Validation after sync
        - Sync history tracking
        """,
    }

    st.markdown(descriptions.get(agent_type, ""))

    if st.button("Create Agent", type="primary"):
        try:
            agent = orchestrator.create_agent(
                agent_type,
                custom_id if custom_id else None,
            )
            st.success(f"✅ Created {agent.name} with ID: `{agent.agent_id}`")
            st.balloons()
        except Exception as e:
            st.error(f"❌ Failed to create agent: {e}")

with tab_systems:
    st.header("Register Database Systems")

    agents = orchestrator.list_agents()

    if not agents:
        st.warning("Create an agent first before registering systems.")
    else:
        agent_options = {a["agent_id"]: f"{a['name']} ({a['agent_id'][:8]}...)" for a in agents}
        selected_agent_id = st.selectbox(
            "Select Agent",
            options=list(agent_options.keys()),
            format_func=lambda x: agent_options[x],
        )

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            system_name = st.text_input("System Name", placeholder="e.g., Production DB")
            system_type = st.selectbox(
                "Database Type",
                options=["sqlserver", "postgresql", "mysql", "oracle", "sqlite"],
                format_func=lambda x: {
                    "sqlserver": "SQL Server",
                    "postgresql": "PostgreSQL",
                    "mysql": "MySQL",
                    "oracle": "Oracle",
                    "sqlite": "SQLite",
                }.get(x, x),
            )
            host = st.text_input("Host", placeholder="localhost or IP address")
            port = st.number_input(
                "Port",
                min_value=1,
                max_value=65535,
                value={"sqlserver": 1433, "postgresql": 5432, "mysql": 3306, "oracle": 1521, "sqlite": 0}.get(system_type, 1433),
            )

        with col2:
            database = st.text_input("Database Name", placeholder="e.g., my_database")
            username = st.text_input("Username", placeholder="e.g., sa")
            password = st.text_input("Password", type="password")

        if st.button("Register System", type="primary"):
            if not all([system_name, host, database, username]):
                st.error("Please fill in all required fields")
            else:
                try:
                    agent = orchestrator.get_agent(selected_agent_id)
                    if agent and hasattr(agent, "register_system"):
                        system = SystemConnection(
                            name=system_name,
                            system_type=SystemType(system_type),
                            host=host,
                            port=port,
                            database=database,
                            username=username,
                            password=password,
                        )
                        agent.register_system(system)
                        st.success(f"✅ System '{system_name}' registered with agent")
                    else:
                        st.error("Agent not found or doesn't support system registration")
                except Exception as e:
                    st.error(f"❌ Failed to register system: {e}")

        # Show registered systems
        st.markdown("---")
        st.subheader("Registered Systems")

        agent = orchestrator.get_agent(selected_agent_id)
        if agent and hasattr(agent, "systems") and agent.systems:
            for system_id, system in agent.systems.items():
                with st.expander(f"🖥️ {system.name} ({system.system_type.value})"):
                    st.write(f"**Host:** {system.host}:{system.port}")
                    st.write(f"**Database:** {system.database}")
                    st.write(f"**Username:** {system.username}")
                    st.write(f"**System ID:** `{system_id}`")
        else:
            st.info("No systems registered yet.")

with tab_tasks:
    st.header("Submit Tasks")

    agents = orchestrator.list_agents()

    if not agents:
        st.warning("Create an agent first before submitting tasks.")
    else:
        agent_options = {a["agent_id"]: f"{a['name']} ({a['agent_id'][:8]}...)" for a in agents}
        selected_agent_id = st.selectbox(
            "Select Agent for Task",
            options=list(agent_options.keys()),
            format_func=lambda x: agent_options[x],
            key="task_agent_select",
        )

        agent = orchestrator.get_agent(selected_agent_id)

        st.markdown("---")

        # Task form based on agent type
        if agent:
            if agent.name == "ComparisonAgent":
                st.subheader("Comparison Task")

                if hasattr(agent, "systems") and len(agent.systems) >= 2:
                    system_options = {s.system_id: s.name for s in agent.systems.values()}

                    col1, col2 = st.columns(2)
                    with col1:
                        source_id = st.selectbox(
                            "Source System",
                            options=list(system_options.keys()),
                            format_func=lambda x: system_options[x],
                        )
                    with col2:
                        target_id = st.selectbox(
                            "Target System",
                            options=list(system_options.keys()),
                            format_func=lambda x: system_options[x],
                            key="target_system",
                        )

                    tables = st.text_area(
                        "Tables to Compare (one per line)",
                        placeholder="dim_customer\ndim_product\nfact_sales",
                    )

                    if st.button("Start Comparison", type="primary"):
                        table_list = [t.strip() for t in tables.split("\n") if t.strip()]
                        if table_list:
                            task = AgentTask(
                                name=f"Compare {len(table_list)} tables",
                                priority=AgentPriority.HIGH,
                                payload={
                                    "type": "compare",
                                    "source_system_id": source_id,
                                    "target_system_id": target_id,
                                    "tables": table_list,
                                },
                            )
                            task_id = agent.add_task(task)
                            st.success(f"✅ Comparison task submitted: `{task_id}`")
                        else:
                            st.error("Please enter at least one table name")
                else:
                    st.warning("Register at least 2 systems to run comparisons")

            elif agent.name == "MonitoringAgent":
                st.subheader("Monitoring Task")

                task_type = st.radio(
                    "Task Type",
                    options=["health_check", "collect_metrics", "check_alerts"],
                    format_func=lambda x: {
                        "health_check": "🏥 Health Check",
                        "collect_metrics": "📊 Collect Metrics",
                        "check_alerts": "🚨 Check Alerts",
                    }.get(x, x),
                )

                if st.button("Run Task", type="primary"):
                    task = AgentTask(
                        name=task_type.replace("_", " ").title(),
                        priority=AgentPriority.HIGH,
                        payload={"type": task_type},
                    )
                    task_id = agent.add_task(task)
                    st.success(f"✅ Task submitted: `{task_id}`")

                # Show alerts
                if hasattr(agent, "alerts") and agent.alerts:
                    st.markdown("---")
                    st.subheader("🚨 Active Alerts")
                    for alert in agent.alerts[-10:]:  # Last 10 alerts
                        st.warning(f"**{alert['metric']}**: {alert['value']} (threshold: {alert['threshold']})")

            elif agent.name == "SyncAgent":
                st.subheader("Sync Task")

                if hasattr(agent, "systems") and len(agent.systems) >= 2:
                    system_options = {s.system_id: s.name for s in agent.systems.values()}

                    col1, col2 = st.columns(2)
                    with col1:
                        source_id = st.selectbox(
                            "Source System",
                            options=list(system_options.keys()),
                            format_func=lambda x: system_options[x],
                            key="sync_source",
                        )
                    with col2:
                        target_id = st.selectbox(
                            "Target System",
                            options=list(system_options.keys()),
                            format_func=lambda x: system_options[x],
                            key="sync_target",
                        )

                    sync_mode = st.radio(
                        "Sync Mode",
                        options=["incremental", "full"],
                        format_func=lambda x: {
                            "incremental": "⚡ Incremental (faster, changes only)",
                            "full": "🔄 Full (complete resync)",
                        }.get(x, x),
                    )

                    tables = st.text_area(
                        "Tables to Sync (one per line)",
                        placeholder="dim_customer\ndim_product",
                        key="sync_tables",
                    )

                    if st.button("Start Sync", type="primary"):
                        table_list = [t.strip() for t in tables.split("\n") if t.strip()]
                        if table_list:
                            task = AgentTask(
                                name=f"Sync {len(table_list)} tables ({sync_mode})",
                                priority=AgentPriority.HIGH,
                                payload={
                                    "type": "sync",
                                    "source_system_id": source_id,
                                    "target_system_id": target_id,
                                    "tables": table_list,
                                    "mode": sync_mode,
                                },
                            )
                            task_id = agent.add_task(task)
                            st.success(f"✅ Sync task submitted: `{task_id}`")
                        else:
                            st.error("Please enter at least one table name")
                else:
                    st.warning("Register at least 2 systems to run synchronization")

                # Show sync history
                if hasattr(agent, "sync_history") and agent.sync_history:
                    st.markdown("---")
                    st.subheader("📜 Sync History")
                    for sync in agent.sync_history[-5:]:  # Last 5 syncs
                        st.write(f"**{sync['source_system']} → {sync['target_system']}**")
                        st.write(f"Mode: {sync['mode']} | Tables: {sync['tables_synced']}")
                        st.write(f"Inserted: {sync['rows_inserted']} | Updated: {sync['rows_updated']}")
                        st.divider()

# Footer
st.markdown("---")
st.caption("Multi-System Agents v1.0 | Powered by BI Data Compare")
