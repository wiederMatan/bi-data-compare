"""Shared styling for all Streamlit pages."""

import streamlit as st


def apply_professional_style():
    """Apply professional styling to the current page."""
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

    /* Multiselect styling */
    .stMultiSelect {
        border-radius: 6px;
    }

    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 6px 6px 0 0;
        padding: 8px 16px;
    }
</style>
""", unsafe_allow_html=True)
