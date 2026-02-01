from __future__ import annotations

import streamlit as st
from datetime import timedelta

from src.common.settings import Settings
from src.storage.duckdb_client import DuckDBClient
from src.ui.components.filters import deployment_filter, time_window_filter

st.set_page_config(page_title="Top Queries", layout="wide")


def main() -> None:
    settings = Settings.load()
    db = DuckDBClient.from_settings(settings).as_read_only(busy_timeout_ms=30_000)

    st.header("🔎 Top Queries")
    st.caption("Recent heavy queries (scan/spill/queue/compile) from DuckDB processed events")

    with st.sidebar:
        st.subheader("Filters")

        deployment = deployment_filter()
        window_minutes = time_window_filter(default_minutes=60)

        metric = st.selectbox(
            "Rank by",
            options=["scanned_mb", "spilled_mb", "queue_duration_ms", "execution_duration_ms", "compile_duration_ms"],
            index=0,
        )
        limit = st.slider("Top N", 10, 200, 50)

    # Use processed table (consumer writes there)
    processed_table = settings.storage.duckdb.tables["processed"]

    # Anchor window using latest processed arrival_timestamp
    latest = db.fetchall(f"SELECT max(arrival_timestamp) FROM {processed_table}")[0][0]
    if latest is None:
        st.info("No data yet. Run the producer + consumer first.")
        return

    start_ts = latest - timedelta(minutes=window_minutes)

    df = db.fetchdf(
        f"""
        SELECT
          query_id,
          deployment_type,
          COUNT(*) AS occurrences,
          AVG(duration_seconds) AS avg_duration_seconds,
          AVG(spill_pressure) AS avg_spill_pressure,
          MAX(arrival_timestamp) AS last_seen,
          AVG({metric}) AS metric_value,
          SUM(scanned_mb) AS scanned_mb,
          SUM(spilled_mb) AS spilled_mb
        FROM {processed_table}
        WHERE arrival_timestamp BETWEEN ? AND ?
          AND (? = 'all' OR deployment_type = ?)
        GROUP BY query_id, deployment_type
        ORDER BY metric_value DESC, last_seen DESC
        LIMIT ?
        """,
        params=[start_ts, latest, deployment, deployment, limit],
    )

    if df.empty:
        st.info("No queries in selected window.")
        return

    st.subheader(f"Top {limit} queries by `{metric}`")
    st.dataframe(df, width="stretch", hide_index=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Count", f"{len(df):,}")
    c2.metric("Total scanned (MB)", f"{df['scanned_mb'].sum():,.1f}")
    c3.metric("Total spilled (MB)", f"{df['spilled_mb'].sum():,.1f}")


if __name__ == "__main__":
    main()
