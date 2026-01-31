from __future__ import annotations

import streamlit as st
from datetime import timedelta

from src.common.settings import Settings
from src.storage.clickhouse_client import ClickHouseClient
from src.ui.components.filters import deployment_filter, time_window_filter

st.set_page_config(page_title="Top Queries", layout="wide")


def main() -> None:
    settings = Settings.load()
    db = ClickHouseClient.from_settings(settings)

    st.header("🔎 Top Queries")
    st.caption("Recent heavy queries (scan/spill/queue/compile) from ClickHouse processed events")

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
    processed_table = settings.clickhouse.tables["processed"]

    # Anchor window using latest processed arrival_timestamp
    latest_df = db.query_df(f"SELECT max(arrival_timestamp) AS latest FROM {processed_table}")
    latest = latest_df["latest"].iloc[0] if not latest_df.empty else None
    if latest is None:
        st.info("No data yet. Run the producer + consumer first.")
        return

    start_ts = latest - timedelta(minutes=window_minutes)

    df = db.query_df(
        f"""
        SELECT
          query_id,
          dataset_type AS deployment_type,
          COUNT(*) AS occurrences,
          AVG(duration_seconds) AS avg_duration_seconds,
          AVG(spill_pressure) AS avg_spill_pressure,
          MAX(arrival_timestamp) AS last_seen,
          AVG({metric}) AS metric_value,
          SUM(scanned_mb) AS scanned_mb,
          SUM(spilled_mb) AS spilled_mb
        FROM {processed_table}
        WHERE arrival_timestamp BETWEEN %(start_ts)s AND %(end_ts)s
          AND (%(deployment)s = 'all' OR dataset_type = %(deployment)s)
        GROUP BY query_id, dataset_type
        ORDER BY metric_value DESC, last_seen DESC
        LIMIT %(limit)s
        """,
        params={
            "start_ts": start_ts,
            "end_ts": latest,
            "deployment": deployment,
            "limit": limit,
        },
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
