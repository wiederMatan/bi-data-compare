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
)

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

st.title("SQL Server Data Comparison")
st.write("Please select a page from the sidebar to get started.")

# Display the image
image_path = os.path.join(os.path.dirname(__file__), 'assets', 'sql_differ.jpg')
try:
    image = Image.open(image_path)
    st.image(image, caption='SQL Differ', width=700)
except FileNotFoundError:
    st.error(f"Image not found at {image_path}")

