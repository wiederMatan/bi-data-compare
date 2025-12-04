"""
This is the main entry point for the Streamlit app.
Streamlit automatically discovers and loads pages from the 'pages' directory.
"""

import streamlit as st
from PIL import Image
import os
import sys
import json
import pickle

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.data.models import AuthType, ConnectionInfo

st.set_page_config(
    page_title="SQL Data Compare",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply professional styling
from src.ui.styles import apply_professional_style, render_status_badge, render_empty_state
apply_professional_style()

# Load all cached state on app startup
if "app_cache_loaded" not in st.session_state:
    config_dir = os.path.join(project_root, "config")

    # Load connection cache
    conn_cache = os.path.join(config_dir, "connection_cache.json")
    if os.path.exists(conn_cache):
        try:
            with open(conn_cache, "r") as f:
                cached = json.load(f)
                for key, value in cached.items():
                    if key not in st.session_state:
                        st.session_state[key] = value

                # Restore ConnectionInfo objects
                for prefix in ["source", "target"]:
                    server = cached.get(f"{prefix}_server")
                    database = cached.get(f"{prefix}_database")
                    username = cached.get(f"{prefix}_username")
                    password = cached.get(f"{prefix}_password")
                    if server and database and username and password:
                        conn_info = ConnectionInfo(
                            server=server,
                            database=database,
                            username=username,
                            password=password,
                            auth_type=AuthType.SQL,
                        )
                        st.session_state[f"{prefix}_connection"] = conn_info
                        st.session_state[f"{prefix}_connected"] = True
        except Exception:
            pass

    # Load tables cache
    tables_cache = os.path.join(config_dir, "tables_cache.json")
    if os.path.exists(tables_cache):
        try:
            with open(tables_cache, "r") as f:
                data = json.load(f)
                if "available_tables" not in st.session_state:
                    st.session_state.available_tables = data.get("available_tables", [])
        except Exception:
            pass

    # Load results cache
    results_cache = os.path.join(config_dir, "results_cache.pkl")
    if os.path.exists(results_cache):
        try:
            with open(results_cache, "rb") as f:
                results = pickle.load(f)
                if "comparison_results" not in st.session_state:
                    st.session_state.comparison_results = results
        except Exception:
            pass

    st.session_state.app_cache_loaded = True

# Hero section - simplified
st.markdown("""
<div style="text-align: center; padding: 1.5rem 0 2rem 0;">
    <h1 style="border: none; font-size: 2.25rem; margin-bottom: 0.5rem; color: #1e3a5f;">
        SQL Data Compare
    </h1>
    <p style="font-size: 1.1rem; color: #64748b; margin-bottom: 0;">
        Compare tables between SQL Server databases
    </p>
</div>
""", unsafe_allow_html=True)

# Quick actions - streamlined
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="background: white; padding: 1.5rem; border-radius: 12px; border: 1px solid #e2e8f0;
                box-shadow: 0 2px 8px rgba(0,0,0,0.04); height: 140px;">
        <div style="display: flex; align-items: center; margin-bottom: 0.75rem;">
            <span style="font-size: 1.5rem; margin-right: 0.5rem;">🔗</span>
            <h3 style="margin: 0; color: #1e3a5f; font-size: 1.1rem;">Connect</h3>
        </div>
        <p style="font-size: 0.9rem; color: #64748b; margin: 0; line-height: 1.5;">
            Configure source and target database connections
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style="background: white; padding: 1.5rem; border-radius: 12px; border: 1px solid #e2e8f0;
                box-shadow: 0 2px 8px rgba(0,0,0,0.04); height: 140px;">
        <div style="display: flex; align-items: center; margin-bottom: 0.75rem;">
            <span style="font-size: 1.5rem; margin-right: 0.5rem;">📊</span>
            <h3 style="margin: 0; color: #1e3a5f; font-size: 1.1rem;">Compare</h3>
        </div>
        <p style="font-size: 0.9rem; color: #64748b; margin: 0; line-height: 1.5;">
            Select tables and run schema & data comparisons
        </p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="background: white; padding: 1.5rem; border-radius: 12px; border: 1px solid #e2e8f0;
                box-shadow: 0 2px 8px rgba(0,0,0,0.04); height: 140px;">
        <div style="display: flex; align-items: center; margin-bottom: 0.75rem;">
            <span style="font-size: 1.5rem; margin-right: 0.5rem;">🔍</span>
            <h3 style="margin: 0; color: #1e3a5f; font-size: 1.1rem;">Analyze</h3>
        </div>
        <p style="font-size: 0.9rem; color: #64748b; margin: 0; line-height: 1.5;">
            Drill down into differences and export reports
        </p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# Status dashboard - cleaner layout
st.markdown("### Dashboard")

source_connected = st.session_state.get("source_connected", False)
target_connected = st.session_state.get("target_connected", False)
results = st.session_state.get("comparison_results", [])

# Connection status with badges
col1, col2 = st.columns(2)

with col1:
    st.markdown("**Connection Status**")

    source_conn = st.session_state.get("source_connection")
    target_conn = st.session_state.get("target_connection")

    if source_connected and source_conn:
        st.markdown(f'''
        <div style="display: flex; align-items: center; padding: 0.75rem; background: #f0fdf4;
                    border-radius: 8px; margin-bottom: 0.5rem; border: 1px solid #bbf7d0;">
            <span style="background: #22c55e; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.75rem;"></span>
            <span style="color: #166534; font-weight: 500;">Source:</span>
            <span style="color: #166534; margin-left: 0.5rem;">{source_conn.server}/{source_conn.database}</span>
        </div>
        ''', unsafe_allow_html=True)
    else:
        st.markdown('''
        <div style="display: flex; align-items: center; padding: 0.75rem; background: #fefce8;
                    border-radius: 8px; margin-bottom: 0.5rem; border: 1px solid #fef08a;">
            <span style="background: #eab308; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.75rem;"></span>
            <span style="color: #854d0e; font-weight: 500;">Source:</span>
            <span style="color: #854d0e; margin-left: 0.5rem;">Not connected</span>
        </div>
        ''', unsafe_allow_html=True)

    if target_connected and target_conn:
        st.markdown(f'''
        <div style="display: flex; align-items: center; padding: 0.75rem; background: #f0fdf4;
                    border-radius: 8px; border: 1px solid #bbf7d0;">
            <span style="background: #22c55e; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.75rem;"></span>
            <span style="color: #166534; font-weight: 500;">Target:</span>
            <span style="color: #166534; margin-left: 0.5rem;">{target_conn.server}/{target_conn.database}</span>
        </div>
        ''', unsafe_allow_html=True)
    else:
        st.markdown('''
        <div style="display: flex; align-items: center; padding: 0.75rem; background: #fefce8;
                    border-radius: 8px; border: 1px solid #fef08a;">
            <span style="background: #eab308; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.75rem;"></span>
            <span style="color: #854d0e; font-weight: 500;">Target:</span>
            <span style="color: #854d0e; margin-left: 0.5rem;">Not connected</span>
        </div>
        ''', unsafe_allow_html=True)

with col2:
    st.markdown("**Last Comparison**")

    if results:
        matching = sum(1 for r in results if r.is_match())
        different = sum(1 for r in results if r.status == "completed" and not r.is_match())
        failed = sum(1 for r in results if r.status == "failed")

        st.markdown(f'''
        <div style="display: flex; gap: 1rem;">
            <div style="flex: 1; background: white; padding: 1rem; border-radius: 8px;
                        border: 1px solid #e2e8f0; text-align: center;">
                <div style="font-size: 1.75rem; font-weight: 700; color: #1e3a5f;">{len(results)}</div>
                <div style="font-size: 0.75rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em;">Tables</div>
            </div>
            <div style="flex: 1; background: #f0fdf4; padding: 1rem; border-radius: 8px;
                        border: 1px solid #bbf7d0; text-align: center;">
                <div style="font-size: 1.75rem; font-weight: 700; color: #166534;">{matching}</div>
                <div style="font-size: 0.75rem; color: #166534; text-transform: uppercase; letter-spacing: 0.05em;">Match</div>
            </div>
            <div style="flex: 1; background: #fefce8; padding: 1rem; border-radius: 8px;
                        border: 1px solid #fef08a; text-align: center;">
                <div style="font-size: 1.75rem; font-weight: 700; color: #854d0e;">{different}</div>
                <div style="font-size: 0.75rem; color: #854d0e; text-transform: uppercase; letter-spacing: 0.05em;">Different</div>
            </div>
            <div style="flex: 1; background: #fef2f2; padding: 1rem; border-radius: 8px;
                        border: 1px solid #fecaca; text-align: center;">
                <div style="font-size: 1.75rem; font-weight: 700; color: #991b1b;">{failed}</div>
                <div style="font-size: 0.75rem; color: #991b1b; text-transform: uppercase; letter-spacing: 0.05em;">Failed</div>
            </div>
        </div>
        ''', unsafe_allow_html=True)
    else:
        st.markdown(render_empty_state(
            "📋",
            "No comparisons yet",
            "Run a comparison to see results here"
        ), unsafe_allow_html=True)

# Footer
st.markdown("""
<div style="text-align: center; padding: 2rem 0 1rem 0; color: #94a3b8; font-size: 0.8rem;">
    SQL Data Compare v1.0
</div>
""", unsafe_allow_html=True)

