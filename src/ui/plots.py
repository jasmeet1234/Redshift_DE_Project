# from __future__ import annotations

# from dataclasses import dataclass
# from typing import List, Optional, Sequence

# import pandas as pd
# import streamlit as st

# try:
#     import plotly.express as px

#     _HAS_PLOTLY = True
# except Exception:
#     _HAS_PLOTLY = False


# @dataclass(frozen=True)
# class PressureIndicators:
#     queue_pressure: float
#     execution_load: float
#     io_pressure: float
#     memory_pressure: float


# def _empty(msg: str) -> None:
#     st.info(msg)


# def _ensure_time_sorted(df: pd.DataFrame, time_col: str = "window_start") -> pd.DataFrame:
#     if df is None or df.empty or time_col not in df.columns:
#         return df
#     return df.sort_values(time_col)


# def compute_pressure_indicators(latest_bucket: pd.DataFrame) -> Optional[PressureIndicators]:
#     if latest_bucket is None or latest_bucket.empty:
#         return None

#     row = latest_bucket.iloc[0]
#     running = float(row.get("running_count", 0.0) or 0.0)
#     queued = float(row.get("queued_count", 0.0) or 0.0)
#     scanned = float(row.get("scanned_mb", 0.0) or 0.0)
#     spill_pressure = float(row.get("spill_pressure", 0.0) or 0.0)

#     denom = running + queued
#     queue_pressure = (queued / denom) if denom > 0 else 0.0
#     execution_load = running
#     io_pressure = scanned
#     memory_pressure = max(0.0, min(1.0, spill_pressure))

#     return PressureIndicators(
#         queue_pressure=float(max(0.0, min(1.0, queue_pressure))),
#         execution_load=float(max(0.0, execution_load)),
#         io_pressure=float(max(0.0, io_pressure)),
#         memory_pressure=float(memory_pressure),
#     )


# def render_pressure_reader(ind: Optional[PressureIndicators]) -> None:
#     st.subheader("Pressure Reader")
#     if ind is None:
#         _empty("No latest bucket available yet.")
#         return

#     c1, c2, c3, c4 = st.columns(4)

#     # show normalized as % where it makes sense
#     c1.metric("Queue Pressure", f"{ind.queue_pressure*100:.0f}%")
#     c2.metric("Execution Load", f"{ind.execution_load:.2f}")
#     c3.metric("IO Pressure (MB/5m)", f"{ind.io_pressure:.2f}")
#     c4.metric("Memory Pressure", f"{ind.memory_pressure*100:.0f}%")


# def plot_running_queries(history: pd.DataFrame) -> None:
#     if history is None or history.empty:
#         _empty("No bucket data yet for running queries.")
#         return
#     df = _ensure_time_sorted(history)
#     if _HAS_PLOTLY:
#         fig = px.line(df, x="window_start", y="running_count", title="Running Queries Over Time")
#         st.plotly_chart(fig, use_container_width=True)
#     else:
#         st.line_chart(df.set_index("window_start")["running_count"])


# def plot_queued_queries(history: pd.DataFrame) -> None:
#     if history is None or history.empty:
#         _empty("No bucket data yet for queued queries.")
#         return
#     df = _ensure_time_sorted(history)
#     if _HAS_PLOTLY:
#         fig = px.line(df, x="window_start", y="queued_count", title="Queued Queries Over Time")
#         st.plotly_chart(fig, use_container_width=True)
#     else:
#         st.line_chart(df.set_index("window_start")["queued_count"])


# def plot_scan_throughput(history: pd.DataFrame) -> None:
#     if history is None or history.empty:
#         _empty("No bucket data yet for scan throughput.")
#         return
#     df = _ensure_time_sorted(history)
#     if _HAS_PLOTLY:
#         fig = px.bar(df, x="window_start", y="scanned_mb", title="Scan Throughput Over Time (MB / 5 min)")
#         st.plotly_chart(fig, use_container_width=True)
#     else:
#         st.bar_chart(df.set_index("window_start")["scanned_mb"])


# def plot_spill_pressure(history: pd.DataFrame) -> None:
#     if history is None or history.empty:
#         _empty("No bucket data yet for spill pressure.")
#         return
#     df = _ensure_time_sorted(history)
#     if _HAS_PLOTLY:
#         fig = px.line(df, x="window_start", y="spill_pressure", title="Spill Pressure Over Time (0–1)")
#         fig.update_yaxes(range=[0, 1])
#         st.plotly_chart(fig, use_container_width=True)
#     else:
#         st.line_chart(df.set_index("window_start")["spill_pressure"])


# def plot_instance_utilization(
#     instance_history: pd.DataFrame,
#     *,
#     max_instances: int = 12,
#     instance_filter: Optional[Sequence[str]] = None,
# ) -> None:
#     if instance_history is None or instance_history.empty:
#         _empty("No instance bucket data yet.")
#         return

#     df = _ensure_time_sorted(instance_history)

#     # pick top instances by total running_count if not filtered
#     if instance_filter:
#         df = df[df["instance_id"].isin(list(instance_filter))]
#     else:
#         top = (
#             df.groupby("instance_id", as_index=False)["running_count"]
#             .sum()
#             .sort_values("running_count", ascending=False)
#             .head(int(max_instances))
#         )
#         df = df[df["instance_id"].isin(top["instance_id"].tolist())]

#     st.markdown("**Instance Utilization – Provisioned**")

#     if _HAS_PLOTLY:
#         fig1 = px.line(df, x="window_start", y="running_count", color="instance_id", title="Running Count by Instance")
#         st.plotly_chart(fig1, use_container_width=True)

#         fig2 = px.line(df, x="window_start", y="scanned_mb", color="instance_id", title="Scanned MB by Instance")
#         st.plotly_chart(fig2, use_container_width=True)

#         fig3 = px.line(df, x="window_start", y="spilled_mb", color="instance_id", title="Spilled MB by Instance")
#         st.plotly_chart(fig3, use_container_width=True)
#     else:
#         st.line_chart(df.pivot_table(index="window_start", columns="instance_id", values="running_count"))
#         st.line_chart(df.pivot_table(index="window_start", columns="instance_id", values="scanned_mb"))
#         st.line_chart(df.pivot_table(index="window_start", columns="instance_id", values="spilled_mb"))