# from __future__ import annotations

# from dataclasses import dataclass
# from datetime import datetime, timedelta, timezone
# from typing import Optional

# import pandas as pd
# import streamlit as st

# from src.common.settings import Settings
# from src.storage.duckdb_client import DuckDBClient


# def _utc_now() -> datetime:
#     return datetime.now(tz=timezone.utc)


# def _since_minutes(minutes: int) -> datetime:
#     return _utc_now() - timedelta(minutes=int(minutes))


# @dataclass(frozen=True)
# class DashboardData:
#     deployment_history: pd.DataFrame
#     latest_bucket: pd.DataFrame
#     instance_history: pd.DataFrame
#     recent_processed: pd.DataFrame
#     top_problem_queries: pd.DataFrame


# @st.cache_data(ttl=1, show_spinner=False)
# def get_deployment_history(settings: Settings, deployment_type: str) -> pd.DataFrame:
#     client = DuckDBClient.from_settings(settings)
#     since_ts = _since_minutes(settings.ui.lookback_minutes)
#     return client.get_deployment_history(deployment_type=deployment_type, since_ts=since_ts)


# @st.cache_data(ttl=1, show_spinner=False)
# def get_latest_bucket(settings: Settings, deployment_type: str) -> pd.DataFrame:
#     client = DuckDBClient.from_settings(settings)
#     return client.get_latest_deployment_bucket(deployment_type=deployment_type)


# @st.cache_data(ttl=1, show_spinner=False)
# def get_instance_history(settings: Settings, deployment_type: str) -> pd.DataFrame:
#     client = DuckDBClient.from_settings(settings)
#     since_ts = _since_minutes(settings.ui.lookback_minutes)
#     return client.get_instance_history(deployment_type=deployment_type, since_ts=since_ts)


# @st.cache_data(ttl=1, show_spinner=False)
# def get_recent_processed(settings: Settings, deployment_type: Optional[str], limit: int = 50) -> pd.DataFrame:
#     client = DuckDBClient.from_settings(settings)
#     return client.get_recent_processed(deployment_type=deployment_type, limit=limit)


# @st.cache_data(ttl=1, show_spinner=False)
# def get_top_problem_queries(settings: Settings, deployment_type: Optional[str], limit: int = 50) -> pd.DataFrame:
#     client = DuckDBClient.from_settings(settings)
#     since_ts = _since_minutes(settings.ui.lookback_minutes)
#     return client.get_top_problem_queries(deployment_type=deployment_type, since_ts=since_ts, limit=limit)


# def load_dashboard_data(settings: Settings, deployment_type: str) -> DashboardData:
#     return DashboardData(
#         deployment_history=get_deployment_history(settings, deployment_type),
#         latest_bucket=get_latest_bucket(settings, deployment_type),
#         instance_history=get_instance_history(settings, deployment_type),
#         recent_processed=get_recent_processed(settings, deployment_type, limit=50),
#         top_problem_queries=get_top_problem_queries(settings, deployment_type, limit=50),
#     )