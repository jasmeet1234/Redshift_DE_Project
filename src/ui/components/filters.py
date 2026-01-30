from __future__ import annotations

import streamlit as st


def deployment_filter(
    default: str = "all",
    key: str = "deployment_filter",
) -> str:
    """
    Returns: 'all' | 'provisioned' | 'serverless'
    """
    return st.selectbox(
        "Deployment type",
        options=["all", "provisioned", "serverless"],
        index=["all", "provisioned", "serverless"].index(default),
        key=key,
    )


def time_window_filter(
    default_minutes: int = 30,
    key: str = "time_window_filter",
) -> int:
    """
    Returns a window size in minutes.
    """
    options = [15, 30, 60, 120, 360, 720, 1440]
    if default_minutes not in options:
        options = sorted(set(options + [default_minutes]))

    return st.selectbox(
        "Window (minutes)",
        options=options,
        index=options.index(default_minutes),
        key=key,
    )