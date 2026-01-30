from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from src.dataset.parquet_reader import ParquetReader as _DatasetParquetReader
from src.dataset.parquet_reader import read_parquet_as_dicts


@dataclass(frozen=True)
class ParquetReader:
    """
    Backward-compatible wrapper for producer code.

    The project historically had two parquet readers:
      - src/producer/parquet_reader.py (local-file oriented)
      - src/dataset/parquet_reader.py (DuckDB + httpfs)

    To support pulling Parquet directly from an S3 HTTP URL and to prevent
    inconsistent behavior across modules, the producer now delegates to the
    dataset reader implementation.
    """
    parquet_url: str
    event_time_col: str = "arrival_timestamp"
    batch_size: int = 1000
    columns: Optional[List[str]] = None
    enforce_event_time_order: bool = True

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return read_parquet_as_dicts(
            self.parquet_url,
            event_time_col=self.event_time_col,
            batch_size=self.batch_size,
            columns=self.columns,
            enforce_event_time_order=self.enforce_event_time_order,
        )


def iter_parquet_rows(
    parquet_url: str,
    *,
    event_time_col: str = "arrival_timestamp",
    batch_size: int = 1000,
    columns: Optional[List[str]] = None,
    enforce_event_time_order: bool = True,
) -> Iterator[Dict[str, Any]]:
    """
    Convenience generator for producer callers that previously imported a function.

    Supports:
      - HTTP/S URLs (including S3-hosted objects via HTTPS)
      - Local file paths
    """
    reader = _DatasetParquetReader(
        parquet_url=parquet_url,
        event_time_col=event_time_col,
        batch_size=batch_size,
        columns=columns,
        enforce_event_time_order=enforce_event_time_order,
    )
    yield from reader