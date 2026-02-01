from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional

import clickhouse_connect

from src.common.settings import Settings


@dataclass
class ClickHouseClient:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_settings(cls, settings: Settings) -> "ClickHouseClient":
        cfg = settings.clickhouse
        return cls(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            user=cfg.user,
            password=cfg.password,
        )

    def connect(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
        )

    def _connect_admin(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database="default",
        )

    def ensure_database(self) -> None:
        client = self._connect_admin()
        client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def command(self, sql: str) -> None:
        client = self.connect()
        client.command(sql)

    def insert_rows(self, table: str, rows: Iterable[Iterable[Any]], columns: list[str]) -> None:
        client = self.connect()
        client.insert(table, rows, column_names=columns)

    def query_df(self, sql: str, params: Optional[dict[str, Any]] = None):
        client = self.connect()
        return client.query_df(sql, parameters=params or {})
