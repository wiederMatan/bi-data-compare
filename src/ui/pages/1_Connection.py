"""Connection configuration page."""
import sys
import os
import json
import streamlit as st
import importlib

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load cache first
from src.ui.cache_loader import load_all_cache
load_all_cache()

from src.core.exceptions import ConnectionError as AppConnectionError
from src.core.logging import get_logger
import src.data.database
importlib.reload(src.data.database)
from src.data.models import AuthType, ConnectionInfo
from src.utils.validators import validate_credentials, validate_database_name, validate_server_name
from src.ui.styles import apply_professional_style

logger = get_logger(__name__)

# Apply professional styling
apply_professional_style()


@st.cache_data(ttl=300, show_spinner=False)
def get_databases_cached(server: str, username: str, password: str) -> list[str]:
    """
    Fetch database list with caching (5 min TTL).

    Args:
        server: Server name
        username: SQL username
        password: SQL password

    Returns:
        List of database names
    """
    try:
        master_conn_info = ConnectionInfo(
            server=server,
            database='master',
            username=username,
            password=password,
            auth_type=AuthType.SQL
        )
        db_conn = src.data.database.DatabaseConnection(master_conn_info)
        with db_conn:
            return db_conn.get_databases()
    except Exception as e:
        logger.warning(f"Failed to fetch databases for {server}: {e}")
        return []

CACHE_FILE = os.path.join(project_root, "config", "connection_cache.json")


def load_cached_settings() -> dict:
    """Load cached connection settings from file."""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_cached_settings(settings: dict) -> None:
    """Save connection settings to cache file."""
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(settings, f)
    except Exception:
        pass


def restore_connection_from_cache(prefix: str, cached: dict) -> None:
    """Restore ConnectionInfo object from cached settings."""
    server = cached.get(f"{prefix}_server")
    database = cached.get(f"{prefix}_database")
    username = cached.get(f"{prefix}_username")
    password = cached.get(f"{prefix}_password")

    if server and database and username and password:
        try:
            connection_info = ConnectionInfo(
                server=server,
                database=database,
                username=username,
                password=password,
                auth_type=AuthType.SQL,
            )
            st.session_state[f"{prefix}_connection"] = connection_info
            st.session_state[f"{prefix}_connected"] = True
        except Exception:
            pass


def render() -> None:
    """Render the connection page."""
    st.title("🔌 Database Connection")
    st.markdown("Configure connections to source and target SQL Server databases.")

    # Load cached settings on first run
    if "cache_loaded" not in st.session_state:
        cached = load_cached_settings()
        if cached:
            for key, value in cached.items():
                if key not in st.session_state:
                    st.session_state[key] = value
            # Auto-restore ConnectionInfo objects
            restore_connection_from_cache("source", cached)
            restore_connection_from_cache("target", cached)
        st.session_state.cache_loaded = True

    # Create two columns for source and target
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📤 Source Database")
        render_connection_form("source")

    with col2:
        st.subheader("📥 Target Database")
        render_connection_form("target")

    # Test connections button
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])

    with col2:
        if st.button("🔍 Test Connections", type="primary", use_container_width=True):
            test_connections()


def render_connection_form(prefix: str) -> None:
    """
    Render connection form for source or target.

    Args:
        prefix: Either 'source' or 'target'
    """
    # Server
    servers = ["qa", "dev"]
    # Get cached server index
    cached_server = st.session_state.get(f"{prefix}_server", servers[0])
    server_index = servers.index(cached_server) if cached_server in servers else 0

    server = st.selectbox(
        "Server",
        options=servers,
        index=server_index,
        key=f"{prefix}_server",
        help="Select the SQL Server instance.",
    )

    # SQL Authentication credentials
    username = st.text_input(
        "Username",
        value=st.session_state.get(f"{prefix}_username", "sa"),
        key=f"{prefix}_username",
        help="SQL Server username",
    )
    password = st.text_input(
        "Password",
        value=st.session_state.get(f"{prefix}_password", "YourStrong@Passw0rd"),
        type="password",
        key=f"{prefix}_password",
        help="SQL Server password",
    )

    # Database - use cached function to avoid duplicate connections
    database_options = []
    if server and username and password:
        database_options = get_databases_cached(server, username, password)

    # Get cached database value
    cached_db = st.session_state.get(f"{prefix}_database", "")

    if database_options:
        # Use selectbox if we have database options
        db_index = database_options.index(cached_db) if cached_db in database_options else 0
        database = st.selectbox(
            "Database",
            options=database_options,
            index=db_index,
            key=f"{prefix}_database",
            help="Database name to connect to",
        )
    else:
        # Use text input if no database options available
        database = st.text_input(
            "Database",
            value=cached_db or "",
            key=f"{prefix}_database",
            help="Enter database name manually",
        )

    # Use SQL Authentication
    use_windows_auth = False

    # Store in session state
    st.session_state[f"{prefix}_connection_info"] = {
        "server": server,
        "database": database,
        "username": username,
        "password": password,
        "use_windows_auth": use_windows_auth,
    }


def test_connections() -> None:
    """Test both source and target connections."""
    success = True

    # Test source
    with st.spinner("Testing source connection..."):
        source_result = test_single_connection("source")
        if source_result:
            st.success("✅ Source connection successful!")
            st.session_state.source_connected = True
        else:
            st.error("❌ Source connection failed!")
            st.session_state.source_connected = False
            success = False

    # Test target
    with st.spinner("Testing target connection..."):
        target_result = test_single_connection("target")
        if target_result:
            st.success("✅ Target connection successful!")
            st.session_state.target_connected = True
        else:
            st.error("❌ Target connection failed!")
            st.session_state.target_connected = False
            success = False

    if success:
        # Save settings to cache for persistence across refreshes
        cache_data = {
            "source_server": st.session_state.get("source_server"),
            "source_username": st.session_state.get("source_username"),
            "source_password": st.session_state.get("source_password"),
            "source_database": st.session_state.get("source_database"),
            "target_server": st.session_state.get("target_server"),
            "target_username": st.session_state.get("target_username"),
            "target_password": st.session_state.get("target_password"),
            "target_database": st.session_state.get("target_database"),
        }
        save_cached_settings(cache_data)
        st.balloons()
        st.info("✨ Both connections are ready! You can proceed to the Comparison page.")


def test_single_connection(prefix: str) -> bool:
    """
    Test a single database connection.

    Args:
        prefix: Either 'source' or 'target'

    Returns:
        True if connection successful, False otherwise
    """
    try:
        # Get connection info from session state
        conn_info = st.session_state.get(f"{prefix}_connection_info")
        if not conn_info:
            st.error(f"No {prefix} connection information found")
            return False

        # Validate inputs
        try:
            validate_server_name(conn_info["server"])
            validate_database_name(conn_info["database"])
            validate_credentials(
                conn_info["username"],
                conn_info["password"],
                conn_info["use_windows_auth"],
            )
        except Exception as e:
            st.error(f"Validation error: {str(e)}")
            return False

        # Create connection info
        connection_info = ConnectionInfo(
            server=conn_info["server"],
            database=conn_info["database"],
            username=conn_info["username"],
            password=conn_info["password"],
            auth_type=AuthType.WINDOWS if conn_info["use_windows_auth"] else AuthType.SQL,
        )

        # Test connection
        connection = src.data.database.DatabaseConnection(connection_info)
        connection.connect()

        # Test query
        result = connection.test_connection()

        # Disconnect
        connection.disconnect()

        # Store connection in session state for later use
        st.session_state[f"{prefix}_connection"] = connection_info

        logger.info(f"{prefix.capitalize()} connection test successful")
        return result

    except AppConnectionError as e:
        st.error(f"Connection error: {str(e)}")
        logger.error(f"{prefix.capitalize()} connection failed: {str(e)}")
        return False
    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        logger.error(f"{prefix.capitalize()} connection failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    render()
