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

# Professional styling
st.markdown("""
<style>
    /* Main container styling */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }

    /* Header styling */
    h1 {
        color: #1e3a5f;
        font-weight: 700;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid #3498db;
        margin-bottom: 1.5rem;
    }

    h2, h3 {
        color: #2c3e50;
        font-weight: 600;
    }

    /* Card-like containers */
    .stExpander {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }

    /* Metric styling */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1e3a5f;
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        font-weight: 500;
        color: #5a6c7d;
    }

    /* Button styling */
    .stButton > button {
        border-radius: 6px;
        font-weight: 600;
        transition: all 0.2s ease;
    }

    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }

    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e3a5f 0%, #2c5282 100%);
    }

    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {
        color: #ffffff !important;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #ffffff !important;
    }

    /* Sidebar navigation links */
    [data-testid="stSidebarNav"] a span {
        color: #ffffff !important;
    }

    [data-testid="stSidebarNav"] a:hover span {
        color: #3498db !important;
    }

    /* Sidebar close button */
    [data-testid="stSidebar"] button svg {
        color: #ffffff !important;
    }

    /* Success/Error message styling */
    .stSuccess {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        border-radius: 4px;
    }

    .stError {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        border-radius: 4px;
    }

    .stWarning {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        border-radius: 4px;
    }

    .stInfo {
        background-color: #d1ecf1;
        border-left: 4px solid #17a2b8;
        border-radius: 4px;
    }

    /* DataFrame styling */
    .stDataFrame {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
    }

    /* Input field styling */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div {
        border-radius: 6px;
    }

    /* Divider styling */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, #e0e0e0, transparent);
        margin: 2rem 0;
    }

    /* Footer */
    .footer {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background-color: #f8f9fa;
        padding: 0.5rem;
        text-align: center;
        font-size: 0.8rem;
        color: #6c757d;
        border-top: 1px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

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

# Hero section
st.markdown("""
<div style="text-align: center; padding: 2rem 0;">
    <h1 style="border: none; font-size: 2.5rem; margin-bottom: 0.5rem;">SQL Data Compare</h1>
    <p style="font-size: 1.2rem; color: #5a6c7d; margin-bottom: 2rem;">
        Enterprise-grade table comparison for SQL Server databases
    </p>
</div>
""", unsafe_allow_html=True)

# Feature cards
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1.5rem; border-radius: 12px; color: white; height: 180px;">
        <h3 style="color: white; border: none; margin-bottom: 0.5rem;">Schema Comparison</h3>
        <p style="font-size: 0.95rem; opacity: 0.9;">
            Detect column differences, type mismatches, and structural changes between databases
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); padding: 1.5rem; border-radius: 12px; color: white; height: 180px;">
        <h3 style="color: white; border: none; margin-bottom: 0.5rem;">Data Validation</h3>
        <p style="font-size: 0.95rem; opacity: 0.9;">
            Compare row counts, checksums, and identify data discrepancies with precision
        </p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); padding: 1.5rem; border-radius: 12px; color: white; height: 180px;">
        <h3 style="color: white; border: none; margin-bottom: 0.5rem;">Drill-Down Analysis</h3>
        <p style="font-size: 0.95rem; opacity: 0.9;">
            Investigate differences with EXCEPT queries and row-by-row comparisons
        </p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Quick start section
st.markdown("### Quick Start")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div style="text-align: center; padding: 1rem;">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">1</div>
        <strong>Connect</strong>
        <p style="font-size: 0.85rem; color: #6c757d;">Configure source & target databases</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style="text-align: center; padding: 1rem;">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">2</div>
        <strong>Select</strong>
        <p style="font-size: 0.85rem; color: #6c757d;">Choose tables to compare</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="text-align: center; padding: 1rem;">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">3</div>
        <strong>Compare</strong>
        <p style="font-size: 0.85rem; color: #6c757d;">Run comparison analysis</p>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div style="text-align: center; padding: 1rem;">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">4</div>
        <strong>Analyze</strong>
        <p style="font-size: 0.85rem; color: #6c757d;">Review results & drill down</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# Status section
col1, col2 = st.columns(2)

with col1:
    st.markdown("### Connection Status")
    source_connected = st.session_state.get("source_connected", False)
    target_connected = st.session_state.get("target_connected", False)

    if source_connected:
        source_conn = st.session_state.get("source_connection")
        st.success(f"Source: {source_conn.server}/{source_conn.database}" if source_conn else "Source: Connected")
    else:
        st.warning("Source: Not connected")

    if target_connected:
        target_conn = st.session_state.get("target_connection")
        st.success(f"Target: {target_conn.server}/{target_conn.database}" if target_conn else "Target: Connected")
    else:
        st.warning("Target: Not connected")

with col2:
    st.markdown("### Recent Activity")
    results = st.session_state.get("comparison_results", [])
    if results:
        matching = sum(1 for r in results if r.is_match())
        different = sum(1 for r in results if r.status == "completed" and not r.is_match())
        failed = sum(1 for r in results if r.status == "failed")
        st.metric("Last Run", f"{len(results)} tables")
        cols = st.columns(3)
        cols[0].metric("Matching", matching)
        cols[1].metric("Different", different)
        cols[2].metric("Failed", failed)
    else:
        st.info("No comparison results yet. Run a comparison to see results here.")

# Footer
st.markdown("""
<div style="text-align: center; padding: 2rem 0; color: #6c757d; font-size: 0.85rem;">
    <p>SQL Data Compare v1.0 | Built with Streamlit</p>
</div>
""", unsafe_allow_html=True)

