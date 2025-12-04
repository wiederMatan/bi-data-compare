"""Shared cache loader for session state persistence."""
import os
import json
import pickle
import streamlit as st

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
config_dir = os.path.join(project_root, "config")


def load_all_cache():
    """Load all cached state. Call this at the top of every page."""
    # Always try to restore ConnectionInfo if missing (even if cache was "loaded")
    _restore_connections_if_needed()

    if st.session_state.get("_all_cache_loaded"):
        return

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

    st.session_state._all_cache_loaded = True


def _restore_connections_if_needed():
    """Restore ConnectionInfo objects from cache if they're missing."""
    # Import here to avoid circular imports
    from src.data.models import AuthType, ConnectionInfo

    # Load connection cache
    conn_cache = os.path.join(config_dir, "connection_cache.json")
    if not os.path.exists(conn_cache):
        return

    try:
        with open(conn_cache, "r") as f:
            cached = json.load(f)
    except Exception:
        return

    # Restore basic session state values
    for key, value in cached.items():
        if key not in st.session_state:
            st.session_state[key] = value

    # Restore ConnectionInfo objects for each prefix
    for prefix in ["source", "target"]:
        if f"{prefix}_connection" not in st.session_state:
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
