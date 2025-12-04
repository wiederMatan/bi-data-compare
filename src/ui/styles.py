"""Shared styling for all Streamlit pages."""

import streamlit as st


def apply_professional_style():
    """Apply professional styling to the current page."""
    st.markdown("""
<style>
    /* ===== TYPOGRAPHY SCALE ===== */
    html {
        font-size: 16px;
        line-height: 1.6;
    }

    body {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        -webkit-font-smoothing: antialiased;
    }

    /* ===== MAIN CONTAINER ===== */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1200px;
    }

    /* Subtle background pattern */
    .main {
        background:
            linear-gradient(90deg, rgba(52, 152, 219, 0.02) 1px, transparent 1px),
            linear-gradient(rgba(52, 152, 219, 0.02) 1px, transparent 1px),
            #fafbfc;
        background-size: 20px 20px;
    }

    /* ===== HEADER STYLING ===== */
    h1 {
        color: #1e3a5f;
        font-weight: 700;
        font-size: 2rem;
        padding-bottom: 0.75rem;
        border-bottom: 3px solid #3498db;
        margin-bottom: 1.5rem;
        letter-spacing: -0.02em;
    }

    h2 {
        color: #2c3e50;
        font-weight: 600;
        font-size: 1.5rem;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }

    h3 {
        color: #34495e;
        font-weight: 600;
        font-size: 1.2rem;
        margin-top: 1rem;
        margin-bottom: 0.75rem;
    }

    p, li {
        line-height: 1.7;
        color: #4a5568;
    }

    /* ===== CARD CONTAINERS ===== */
    .stExpander {
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        transition: all 0.3s ease;
        overflow: hidden;
    }

    .stExpander:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.08);
        border-color: #cbd5e0;
    }

    /* ===== METRIC STYLING ===== */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #1e3a5f;
        letter-spacing: -0.02em;
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.875rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    [data-testid="stMetricDelta"] {
        font-weight: 600;
    }

    /* ===== BUTTON STYLING ===== */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        padding: 0.5rem 1.25rem;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        border: none;
        position: relative;
        overflow: hidden;
    }

    .stButton > button::after {
        content: '';
        position: absolute;
        top: 50%;
        left: 50%;
        width: 0;
        height: 0;
        background: rgba(255,255,255,0.2);
        border-radius: 50%;
        transform: translate(-50%, -50%);
        transition: width 0.3s, height 0.3s;
    }

    .stButton > button:hover::after {
        width: 200%;
        height: 200%;
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(52, 152, 219, 0.3);
    }

    .stButton > button:active {
        transform: translateY(0);
    }

    .stButton > button:focus {
        outline: 3px solid rgba(52, 152, 219, 0.4);
        outline-offset: 2px;
    }

    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
        color: white;
    }

    .stButton > button[kind="secondary"] {
        background: #f1f5f9;
        color: #475569;
        border: 1px solid #e2e8f0;
    }

    .stButton > button[kind="secondary"]:hover {
        background: #e2e8f0;
    }

    /* ===== SIDEBAR STYLING ===== */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a365d 0%, #2d4a6f 50%, #1e3a5f 100%);
        box-shadow: 4px 0 20px rgba(0,0,0,0.1);
    }

    [data-testid="stSidebar"]::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background:
            radial-gradient(circle at 20% 80%, rgba(52, 152, 219, 0.1) 0%, transparent 50%),
            radial-gradient(circle at 80% 20%, rgba(52, 152, 219, 0.08) 0%, transparent 40%);
        pointer-events: none;
    }

    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #ffffff !important;
        text-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }

    /* Sidebar navigation links */
    [data-testid="stSidebarNav"] a {
        border-radius: 8px;
        margin: 4px 8px;
        transition: all 0.2s ease;
    }

    [data-testid="stSidebarNav"] a span {
        color: #e2e8f0 !important;
        font-weight: 500;
    }

    [data-testid="stSidebarNav"] a:hover {
        background: rgba(255,255,255,0.1) !important;
    }

    [data-testid="stSidebarNav"] a:hover span {
        color: #ffffff !important;
    }

    [data-testid="stSidebarNav"] a[aria-current="page"] {
        background: rgba(52, 152, 219, 0.3) !important;
        border-left: 3px solid #3498db;
    }

    [data-testid="stSidebarNav"] a[aria-current="page"] span {
        color: #ffffff !important;
        font-weight: 600;
    }

    /* Sidebar close button */
    [data-testid="stSidebar"] button svg {
        color: #e2e8f0 !important;
        transition: transform 0.2s ease;
    }

    [data-testid="stSidebar"] button:hover svg {
        transform: scale(1.1);
    }

    /* ===== ALERT STYLING ===== */
    .stSuccess, .stError, .stWarning, .stInfo {
        border-radius: 8px;
        padding: 1rem 1.25rem;
        margin: 0.75rem 0;
        animation: slideIn 0.3s ease-out;
    }

    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(-10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    .stSuccess {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        border-left: 4px solid #28a745;
    }

    .stError {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        border-left: 4px solid #dc3545;
    }

    .stWarning {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeeba 100%);
        border-left: 4px solid #ffc107;
    }

    .stInfo {
        background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
        border-left: 4px solid #17a2b8;
    }

    /* ===== DATAFRAME STYLING ===== */
    .stDataFrame {
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

    .stDataFrame [data-testid="stDataFrameResizable"] {
        border-radius: 12px;
    }

    /* ===== INPUT STYLING ===== */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stMultiSelect > div > div > div {
        border-radius: 8px;
        border: 2px solid #e2e8f0;
        transition: all 0.2s ease;
    }

    .stTextInput > div > div > input:focus,
    .stSelectbox > div > div > div:focus-within,
    .stMultiSelect > div > div > div:focus-within {
        border-color: #3498db;
        box-shadow: 0 0 0 3px rgba(52, 152, 219, 0.15);
    }

    /* ===== DIVIDER ===== */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, #e2e8f0 20%, #e2e8f0 80%, transparent);
        margin: 2.5rem 0;
    }

    /* ===== PROGRESS BAR ===== */
    .stProgress > div > div {
        background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
        border-radius: 999px;
        transition: width 0.3s ease;
    }

    .stProgress > div {
        background: #e2e8f0;
        border-radius: 999px;
    }

    /* ===== TABS ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #f8fafc;
        padding: 4px;
        border-radius: 12px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 500;
        transition: all 0.2s ease;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background: #e2e8f0;
    }

    .stTabs [aria-selected="true"] {
        background: white !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }

    /* ===== SPINNER/LOADING ===== */
    .stSpinner > div {
        border-color: #3498db transparent transparent transparent;
    }

    /* ===== RESPONSIVE DESIGN ===== */
    @media (max-width: 768px) {
        .main .block-container {
            padding: 1rem;
        }

        h1 {
            font-size: 1.5rem;
        }

        h2 {
            font-size: 1.25rem;
        }

        [data-testid="stMetricValue"] {
            font-size: 1.5rem;
        }

        .stButton > button {
            width: 100%;
            margin-bottom: 0.5rem;
        }
    }

    @media (max-width: 480px) {
        h1 {
            font-size: 1.25rem;
        }

        [data-testid="stMetricValue"] {
            font-size: 1.25rem;
        }
    }

    /* ===== FOCUS STATES FOR ACCESSIBILITY ===== */
    *:focus-visible {
        outline: 3px solid rgba(52, 152, 219, 0.5);
        outline-offset: 2px;
    }

    a:focus-visible {
        outline: 3px solid rgba(52, 152, 219, 0.5);
        outline-offset: 2px;
        border-radius: 4px;
    }

    /* ===== SKELETON LOADER ===== */
    @keyframes shimmer {
        0% {
            background-position: -200% 0;
        }
        100% {
            background-position: 200% 0;
        }
    }

    .skeleton {
        background: linear-gradient(90deg, #f0f0f0 25%, #e0e0e0 50%, #f0f0f0 75%);
        background-size: 200% 100%;
        animation: shimmer 1.5s infinite;
        border-radius: 4px;
    }

    /* ===== STATUS BADGES ===== */
    .status-badge {
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    .status-badge-success {
        background: #d4edda;
        color: #155724;
    }

    .status-badge-warning {
        background: #fff3cd;
        color: #856404;
    }

    .status-badge-error {
        background: #f8d7da;
        color: #721c24;
    }

    .status-badge-info {
        background: #d1ecf1;
        color: #0c5460;
    }

    /* ===== EMPTY STATE ===== */
    .empty-state {
        text-align: center;
        padding: 3rem 2rem;
        color: #64748b;
    }

    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
        opacity: 0.5;
    }

    .empty-state-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: #475569;
        margin-bottom: 0.5rem;
    }

    .empty-state-description {
        font-size: 0.95rem;
        max-width: 400px;
        margin: 0 auto;
    }

    /* ===== TOOLTIPS ===== */
    [data-tooltip] {
        position: relative;
    }

    [data-tooltip]::after {
        content: attr(data-tooltip);
        position: absolute;
        bottom: 100%;
        left: 50%;
        transform: translateX(-50%);
        padding: 0.5rem 0.75rem;
        background: #1e3a5f;
        color: white;
        font-size: 0.75rem;
        border-radius: 6px;
        white-space: nowrap;
        opacity: 0;
        visibility: hidden;
        transition: all 0.2s ease;
    }

    [data-tooltip]:hover::after {
        opacity: 1;
        visibility: visible;
        bottom: calc(100% + 8px);
    }
</style>
""", unsafe_allow_html=True)


def render_status_badge(status: str, text: str) -> str:
    """Render a colored status badge."""
    badge_class = {
        "success": "status-badge-success",
        "warning": "status-badge-warning",
        "error": "status-badge-error",
        "info": "status-badge-info"
    }.get(status, "status-badge-info")

    return f'<span class="status-badge {badge_class}">{text}</span>'


def render_empty_state(icon: str, title: str, description: str) -> str:
    """Render an empty state component."""
    return f'''
    <div class="empty-state">
        <div class="empty-state-icon">{icon}</div>
        <div class="empty-state-title">{title}</div>
        <div class="empty-state-description">{description}</div>
    </div>
    '''


def render_skeleton_loader(height: str = "20px", width: str = "100%") -> str:
    """Render a skeleton loader placeholder."""
    return f'<div class="skeleton" style="height: {height}; width: {width};"></div>'
