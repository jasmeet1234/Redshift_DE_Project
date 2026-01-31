from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from kafka import KafkaConsumer, KafkaProducer

from src.common.schema import QueryMetricsEvent
from src.common.settings import Settings
from src.producer.metrics_calc import map_clean_enrich_row
from src.storage.clickhouse_client import ClickHouseClient

logger = logging.getLogger(__name__)


@dataclass
class StreamingBaselines:
    alpha: float
    values: Dict[str, float]

    def update(self, key: str, value: float) -> float:
        if key not in self.values:
            self.values[key] = value
        else:
            self.values[key] = self.alpha * value + (1 - self.alpha) * self.values[key]
        return self.values[key]


def _ensure_tables(client: ClickHouseClient, settings: Settings) -> None:
    tables = settings.clickhouse.tables
    processed = tables["processed"]
    rollups = tables["rollups_minute"]

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {processed} (
            dataset_type String,
            query_id String,
            instance_id String,
            arrival_timestamp DateTime64(6),
            execution_start_time DateTime64(6),
            execution_end_time DateTime64(6),
            queue_duration_ms Int64,
            compile_duration_ms Int64,
            execution_duration_ms Int64,
            scanned_mb Float64,
            spilled_mb Float64,
            duration_seconds Float64,
            spill_pressure Float64,
            queued UInt8,
            queue_score Float64,
            compile_score Float64,
            scan_score Float64,
            spill_score Float64,
            utilization_score Float64
        )
        ENGINE = MergeTree
        ORDER BY (dataset_type, arrival_timestamp)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {rollups} (
            window_start DateTime64(6),
            dataset_type String,
            query_count UInt64,
            avg_duration_seconds Float64,
            avg_spill_pressure Float64,
            queued_ratio Float64
        )
        ENGINE = MergeTree
        ORDER BY (dataset_type, window_start)
        """
    )


def _score(value: float, baseline: float) -> float:
    denom = baseline if baseline > 0 else 1.0
    return value / denom


def _compute_utilization(
    evt: QueryMetricsEvent,
    *,
    baseline: float,
) -> float:
    raw = float(evt.execution_duration_ms) + float(evt.scanned_mb) + float(evt.spilled_mb)
    return _score(raw, baseline)


def _parse_raw(msg: bytes) -> Dict[str, Any]:
    payload = json.loads(msg.decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def run_clickhouse_consumer(settings: Settings) -> None:
    logger.info("Starting ClickHouse consumer")

    topics = settings.kafka.topics
    consumer = KafkaConsumer(
        topics.raw_query_metrics_provisioned,
        topics.raw_query_metrics_serverless,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_groups.get("clickhouse_writer"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,
        max_poll_records=2000,
    )

    client = ClickHouseClient.from_settings(settings)
    client.ensure_database()
    _ensure_tables(client, settings)

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_serializer=lambda d: json.dumps(d, default=str).encode("utf-8"),
        linger_ms=25,
    )

    baselines = {
        "queue": StreamingBaselines(alpha=0.05, values=defaultdict(float)),
        "compile": StreamingBaselines(alpha=0.05, values=defaultdict(float)),
        "scan": StreamingBaselines(alpha=0.05, values=defaultdict(float)),
        "spill": StreamingBaselines(alpha=0.05, values=defaultdict(float)),
        "util": StreamingBaselines(alpha=0.05, values=defaultdict(float)),
    }

    batch: List[Dict[str, Any]] = []
    batch_size = settings.replay.producer_batch_size

    def flush(rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        columns = [
            "dataset_type",
            "query_id",
            "instance_id",
            "arrival_timestamp",
            "execution_start_time",
            "execution_end_time",
            "queue_duration_ms",
            "compile_duration_ms",
            "execution_duration_ms",
            "scanned_mb",
            "spilled_mb",
            "duration_seconds",
            "spill_pressure",
            "queued",
            "queue_score",
            "compile_score",
            "scan_score",
            "spill_score",
            "utilization_score",
        ]
        data_rows = [[row.get(c) for c in columns] for row in rows]
        client.insert_rows(settings.clickhouse.tables["processed"], data_rows, columns)

        rollups_rows: Dict[tuple, Dict[str, float]] = {}
        for row in rows:
            arrival = row["arrival_timestamp"]
            if isinstance(arrival, datetime):
                window_start = arrival.replace(second=0, microsecond=0)
            else:
                window_start = arrival
            key = (window_start, row["dataset_type"])
            bucket = rollups_rows.setdefault(
                key,
                {
                    "count": 0,
                    "duration_sum": 0.0,
                    "spill_sum": 0.0,
                    "queued_sum": 0.0,
                },
            )
            bucket["count"] += 1
            bucket["duration_sum"] += float(row["duration_seconds"])
            bucket["spill_sum"] += float(row["spill_pressure"])
            bucket["queued_sum"] += float(row["queued"])

        rollup_columns = [
            "window_start",
            "dataset_type",
            "query_count",
            "avg_duration_seconds",
            "avg_spill_pressure",
            "queued_ratio",
        ]
        rollup_data = []
        for (window_start, dataset_type), agg in rollups_rows.items():
            count = agg["count"]
            rollup_data.append(
                [
                    window_start,
                    dataset_type,
                    count,
                    agg["duration_sum"] / count if count else 0.0,
                    agg["spill_sum"] / count if count else 0.0,
                    agg["queued_sum"] / count if count else 0.0,
                ]
            )
        if rollup_data:
            client.insert_rows(settings.clickhouse.tables["rollups_minute"], rollup_data, rollup_columns)

    while True:
        batch_records = consumer.poll(timeout_ms=1000)
        if not batch_records:
            continue

        for _tp, records in batch_records.items():
            for record in records:
                raw = _parse_raw(record.value)
                if not raw:
                    continue

                dataset_type = str(raw.get("dataset_type") or raw.get("deployment_type") or "unknown")
                raw.setdefault("deployment_type", dataset_type)

                try:
                    evt = map_clean_enrich_row(raw, settings.processing)
                except Exception as e:
                    logger.debug("Dropping row due to cleaning error: %s", e)
                    continue

                queue_base = baselines["queue"].update(dataset_type, float(evt.queue_duration_ms))
                compile_base = baselines["compile"].update(dataset_type, float(evt.compile_duration_ms))
                scan_base = baselines["scan"].update(dataset_type, float(evt.scanned_mb))
                spill_base = baselines["spill"].update(dataset_type, float(evt.spilled_mb))

                util_raw = float(evt.execution_duration_ms) + float(evt.scanned_mb) + float(evt.spilled_mb)
                util_base = baselines["util"].update(dataset_type, util_raw)

                row = {
                    "dataset_type": dataset_type,
                    "query_id": str(evt.query_id),
                    "instance_id": str(evt.instance_id) if evt.instance_id is not None else "",
                    "arrival_timestamp": evt.arrival_timestamp,
                    "execution_start_time": evt.execution_start_time,
                    "execution_end_time": evt.execution_end_time,
                    "queue_duration_ms": int(evt.queue_duration_ms),
                    "compile_duration_ms": int(evt.compile_duration_ms),
                    "execution_duration_ms": int(evt.execution_duration_ms),
                    "scanned_mb": float(evt.scanned_mb),
                    "spilled_mb": float(evt.spilled_mb),
                    "duration_seconds": float(evt.duration_seconds),
                    "spill_pressure": float(evt.spill_pressure),
                    "queued": 1 if evt.queued else 0,
                    "queue_score": _score(float(evt.queue_duration_ms), queue_base),
                    "compile_score": _score(float(evt.compile_duration_ms), compile_base),
                    "scan_score": _score(float(evt.scanned_mb), scan_base),
                    "spill_score": _score(float(evt.spilled_mb), spill_base),
                    "utilization_score": _compute_utilization(evt, baseline=util_base),
                }
                batch.append(row)

                processed_topic = (
                    topics.processed_query_metrics_provisioned
                    if dataset_type == "provisioned"
                    else topics.processed_query_metrics_serverless
                )
                producer.send(
                    processed_topic,
                    {
                        **evt.model_dump(mode="json"),
                        "dataset_type": dataset_type,
                        "queue_score": row["queue_score"],
                        "compile_score": row["compile_score"],
                        "scan_score": row["scan_score"],
                        "spill_score": row["spill_score"],
                        "utilization_score": row["utilization_score"],
                    },
                )

                if len(batch) >= batch_size:
                    flush(batch)
                    batch.clear()

        if batch:
            flush(batch)
            batch.clear()
        consumer.commit()
