from __future__ import annotations

import argparse
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
import os


ROOT = Path(__file__).resolve().parents[1]
REDSHIFT_SQL_DIR = ROOT / "configs" / "sql" / "redshift"


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _conn_params() -> dict:
    return {
        "host": os.environ["REDSHIFT_HOST"],
        "port": int(os.getenv("REDSHIFT_PORT", "5439")),
        "dbname": os.getenv("REDSHIFT_DATABASE", "dev"),
        "user": os.environ["REDSHIFT_USER"],
        "password": os.environ["REDSHIFT_PASSWORD"],
    }


def _exec_sql_file(cur, path: Path) -> None:
    sql = _read_sql(path).strip()
    if not sql:
        return
    cur.execute(sql)


def bootstrap(run_rollups: bool = False) -> None:
    params = _conn_params()
    with psycopg2.connect(**params) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            _exec_sql_file(cur, REDSHIFT_SQL_DIR / "001_create_schema.sql")
            _exec_sql_file(cur, REDSHIFT_SQL_DIR / "002_create_tables.sql")
            if run_rollups:
                _exec_sql_file(cur, REDSHIFT_SQL_DIR / "003_rollups.sql")


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Bootstrap Redshift schema/tables for analytics.")
    parser.add_argument(
        "--run-rollups",
        action="store_true",
        help="Also run rollups once (usually run separately or on schedule).",
    )
    args = parser.parse_args()

    # Ensure required env vars exist (fail fast with good errors)
    required = ["REDSHIFT_HOST", "REDSHIFT_USER", "REDSHIFT_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise SystemExit(f"Missing required env vars: {missing} (set them in .env)")

    bootstrap(run_rollups=args.run_rollups)
    print("✅ Redshift bootstrap complete")


if __name__ == "__main__":
    main()