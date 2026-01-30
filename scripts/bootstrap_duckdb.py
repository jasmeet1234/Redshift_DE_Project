from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import duckdb

from src.common.settings import Settings


ROOT = Path(__file__).resolve().parents[1]
DUCKDB_SQL_DIR = ROOT / "configs" / "sql" / "duckdb"


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _sorted_sql_files(sql_dir: Path) -> List[Path]:
    # Expect files like 001_*.sql, 002_*.sql ...
    files = sorted([p for p in sql_dir.glob("*.sql") if p.is_file()], key=lambda p: p.name)
    if not files:
        raise FileNotFoundError(f"No .sql files found in {sql_dir}")
    return files


def bootstrap(db_path: Path, *, skip_rollups: bool = False, skip_views: bool = False) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    sql_files = _sorted_sql_files(DUCKDB_SQL_DIR)

    con = duckdb.connect(str(db_path))
    try:
        for sql_file in sql_files:
            name = sql_file.name.lower()

            if skip_rollups and "rollup" in name:
                continue
            if skip_views and "view" in name:
                continue

            con.execute(_read_sql(sql_file))
    finally:
        con.close()


def main() -> None:
    settings = Settings.load()
    default_db_path = Path(settings.storage.duckdb.path)

    parser = argparse.ArgumentParser(description="Bootstrap DuckDB schema for local analytics.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=default_db_path,
        help=f"Path to DuckDB file (default from configs/app.yaml: {default_db_path})",
    )
    parser.add_argument(
        "--skip-rollups",
        action="store_true",
        help="Skip executing rollup SQL files during bootstrap (recommended when rollups are computed continuously).",
    )
    parser.add_argument(
        "--skip-views",
        action="store_true",
        help="Skip executing view SQL files during bootstrap.",
    )
    args = parser.parse_args()

    bootstrap(args.db_path, skip_rollups=args.skip_rollups, skip_views=args.skip_views)
    print(f"✅ DuckDB bootstrapped at: {args.db_path}")


if __name__ == "__main__":
    main()