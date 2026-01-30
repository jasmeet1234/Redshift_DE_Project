from __future__ import annotations

from typing import List

import streamlit as st
import pandas as pd


def line_chart_mb(df: pd.DataFrame, x: str, y: List[str]) -> None:
    """
    Simple multi-series line chart using Streamlit.
    Expects df[x] to be datetime-like and y columns numeric.
    """
    if df is None or df.empty:
        st.info("No data to chart.")
        return

    plot_df = df[[x] + y].copy()
    plot_df = plot_df.sort_values(x).set_index(x)

    st.line_chart(plot_df[y])


def gauge_spill_pressure(value: float) -> None:
    """
    Simple spill pressure gauge using a progress bar + label.
    value expected in [0, 1].
    """
    v = 0.0 if value is None else float(value)
    v = max(0.0, min(1.0, v))

    st.write(f"Spill pressure: **{v:.2%}**")
    st.progress(v)