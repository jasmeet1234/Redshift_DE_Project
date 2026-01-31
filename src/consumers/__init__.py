from __future__ import annotations

from src.consumers.duckdb_consumer import run_duckdb_consumer
from src.consumers.clickhouse_consumer import run_clickhouse_consumer
from src.consumers.redshift_loader import run_redshift_consumer

__all__ = [
    "run_clickhouse_consumer",
    "run_duckdb_consumer",
    "run_redshift_consumer",
]
