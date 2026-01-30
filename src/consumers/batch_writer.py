from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class BatchWriterConfig:
    out_dir: Path
    max_rows: int
    max_seconds: int
    format: str = "parquet"  # parquet | csv


class BatchWriter:
    """
    Buffers rows and flushes them to disk as partitioned batch files.
    """

    def __init__(self, cfg: BatchWriterConfig):
        self.cfg = cfg
        self.cfg.out_dir.mkdir(parents=True, exist_ok=True)

        self._rows: List[dict] = []
        self._last_flush = time.time()

    def add(self, row: dict) -> List[Path]:
        """
        Add a single row.
        Returns list of flushed file paths (usually empty).
        """
        self._rows.append(row)
        if self._should_flush():
            return self.flush()
        return []

    def _should_flush(self) -> bool:
        if len(self._rows) >= self.cfg.max_rows:
            return True
        if (time.time() - self._last_flush) >= self.cfg.max_seconds:
            return True
        return False

    def flush(self) -> List[Path]:
        if not self._rows:
            return []

        df = pd.DataFrame(self._rows)
        self._rows.clear()
        self._last_flush = time.time()

        fname = f"batch_{int(self._last_flush)}_{uuid.uuid4().hex}"
        path = self.cfg.out_dir / f"{fname}.{self.cfg.format}"

        if self.cfg.format == "parquet":
            table = pa.Table.from_pandas(df)
            pq.write_table(table, path)
        elif self.cfg.format == "csv":
            df.to_csv(path, index=False)
        else:
            raise ValueError(f"Unsupported format: {self.cfg.format}")

        return [path]

    def close(self) -> List[Path]:
        """
        Flush remaining rows.
        """
        return self.flush()