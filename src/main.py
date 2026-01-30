from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

from src.common.logging import setup_logging
from src.common.settings import Settings
from src.consumers.duckdb_consumer import run_duckdb_consumer
from src.consumers.redshift_loader import run_redshift_consumer
from src.producer.replay_producer import run_replay_producer

logger = logging.getLogger(__name__)


def _resolve_input_path(input_path: Optional[str], source_url: Optional[str], settings: Settings) -> str:
    if source_url:
        return source_url
    if input_path:
        return input_path
    return settings.dataset.source_url


def _run_streamlit_ui() -> None:
    app_path = Path("src/ui/app.py")
    if not app_path.exists():
        raise FileNotFoundError(f"Streamlit app not found at {app_path}")

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.headless=true",
    ]
    logger.info("Launching Streamlit UI: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def cli(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Redshift Streaming Analytics CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    producer_parser = subparsers.add_parser("producer", help="Run the replay producer")
    producer_parser.add_argument(
        "--input",
        dest="input_path",
        help="Local parquet file/dir (or URL). Overrides dataset.source_url if provided.",
    )
    producer_parser.add_argument(
        "--source-url",
        dest="source_url",
        help="HTTP(S) parquet URL. Overrides --input if provided.",
    )

    subparsers.add_parser("consumer-duckdb", help="Run the DuckDB consumer")
    subparsers.add_parser("consumer-redshift", help="Run the Redshift consumer")
    subparsers.add_parser("ui", help="Run the Streamlit UI")

    args = parser.parse_args(argv)

    settings = Settings.load()
    setup_logging(settings.logging)

    logger.info("Command: %s", args.command)
    logger.info("Working directory: %s", os.getcwd())

    if args.command == "producer":
        input_path = _resolve_input_path(args.input_path, args.source_url, settings)
        run_replay_producer(input_path, settings)
        return 0

    if args.command == "consumer-duckdb":
        run_duckdb_consumer(settings)
        return 0

    if args.command == "consumer-redshift":
        run_redshift_consumer(settings)
        return 0

    if args.command == "ui":
        _run_streamlit_ui()
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(cli())
