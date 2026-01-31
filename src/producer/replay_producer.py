from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

from kafka import KafkaProducer

from src.common.settings import Settings
from src.producer.parquet_reader import ParquetReader

logger = logging.getLogger(__name__)


def _json_default(o: Any) -> Any:
    """Robust JSON serialization for datetime/date and common scalar types."""
    if isinstance(o, (datetime, date)):
        return o.isoformat()

    try:
        import numpy as np  # type: ignore

        if isinstance(o, (np.integer, np.floating, np.bool_)):
            return o.item()
    except Exception:
        pass

    return str(o)


def _make_producer(settings: Settings) -> KafkaProducer:
    # Prefer env override: KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or settings.kafka.bootstrap_servers

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda d: json.dumps(d, default=_json_default).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=25,
        request_timeout_ms=60_000,
        max_in_flight_requests_per_connection=5,
    )


class TTLKeyDeduper:
    """
    Bounded, TTL-based deduper for streaming.

    - key: tuple of configured columns (e.g. query_id + arrival_timestamp)
    - keeps at most max_size keys
    - evicts keys older than ttl_seconds
    """

    def __init__(self, *, ttl_seconds: int, max_size: int = 200_000) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        if max_size <= 0:
            raise ValueError("max_size must be > 0")

        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._store: "OrderedDict[tuple, float]" = OrderedDict()

    def _evict_expired(self, now_ts: float) -> None:
        cutoff = now_ts - float(self.ttl_seconds)
        while self._store:
            (_, ts) = next(iter(self._store.items()))
            if ts >= cutoff:
                break
            self._store.popitem(last=False)

    def seen_or_add(self, key: tuple, now_ts: float) -> bool:
        """
        Returns True if key already seen (within TTL), else records it and returns False.
        """
        self._evict_expired(now_ts)

        if key in self._store:
            # refresh recency
            self._store.move_to_end(key, last=True)
            self._store[key] = now_ts
            return True

        self._store[key] = now_ts
        self._store.move_to_end(key, last=True)

        # enforce max size
        while len(self._store) > self.max_size:
            self._store.popitem(last=False)

        return False


def _iter_raw_rows(
    source: Union[str, Path],
    *,
    event_time_col: str,
    batch_size: int,
    enforce_event_time_order: bool,
) -> Iterable[Dict[str, Any]]:
    src = str(source)
    reader = ParquetReader(
        parquet_url=src,
        event_time_col=event_time_col,
        batch_size=batch_size,
        enforce_event_time_order=enforce_event_time_order,
    )
    return reader


def _buffered_rows(rows: Iterable[Dict[str, Any]], max_size: int) -> Iterable[Dict[str, Any]]:
    """
    Buffer rows in a background thread to smooth over slow IO/bandwidth.
    """
    q: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=max_size)
    sentinel = object()

    def _worker() -> None:
        try:
            for row in rows:
                q.put(row)
        finally:
            q.put(sentinel)  # type: ignore[arg-type]

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()

    while True:
        item = q.get()
        if item is sentinel:
            break
        yield item  # type: ignore[misc]


def _parse_arrival_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        from dateutil import parser as dt_parser

        return dt_parser.parse(str(value))
    except Exception:
        return None


def run_replay_producer(
    input_path: Union[str, Path],
    settings: Settings,
    *,
    dataset_type: str,
    topic_override: Optional[str] = None,
) -> None:
    """
    Read Parquet rows (URL or local path) and publish raw events to Kafka.

    Replay timing:
      - Uses event-time deltas (arrival_timestamp) scaled by settings.replay.time_scale_factor.
      - Uses a target_duration_seconds to maintain a 1-hour replay wall-clock budget.
    """
    topics = settings.kafka.topics
    processed_topic = topic_override or topics.processed_query_metrics

    replay_factor = int(settings.replay.time_scale_factor)
    producer_batch_size = int(settings.replay.producer_batch_size)
    max_events = settings.replay.max_events
    target_duration_s = int(settings.replay.target_duration_seconds)

    dataset_cfg = settings.datasets or settings.dataset

    logger.info(
        "Replay producer starting. source=%s topic=%s dataset=%s replay_factor=%sx batch_read=%d batch_send=%d max_events=%s target_duration_s=%d",
        str(input_path),
        processed_topic,
        dataset_type,
        replay_factor,
        dataset_cfg.batch_read_size,
        producer_batch_size,
        str(max_events),
        target_duration_s,
    )

    producer = _make_producer(settings)

    sent = 0
    last_arrival: Optional[datetime] = None
    first_arrival: Optional[datetime] = None
    wall_start: Optional[float] = None
    pending_futures = []

    try:
        rows = _buffered_rows(
            _iter_raw_rows(
                input_path,
                event_time_col=dataset_cfg.event_time_column,
                batch_size=dataset_cfg.batch_read_size,
                enforce_event_time_order=dataset_cfg.enforce_event_time_order,
            ),
            settings.replay.prefetch_rows,
        )
        for raw in rows:
            arrival = _parse_arrival_ts(raw.get(dataset_cfg.event_time_column))
            if arrival is None:
                continue

            # Replay timing (event-time based with a target wall-clock budget)
            if first_arrival is None:
                first_arrival = arrival
                last_arrival = arrival
                wall_start = time.time()

            if last_arrival is not None and wall_start is not None and first_arrival is not None:
                delta = (arrival - last_arrival).total_seconds()
                if delta > 0 and replay_factor > 0:
                    time.sleep(delta / float(replay_factor))

                dataset_elapsed = (arrival - first_arrival).total_seconds()
                target_wall_elapsed = dataset_elapsed / float(replay_factor)
                now_wall_elapsed = time.time() - wall_start
                if target_wall_elapsed > now_wall_elapsed:
                    time.sleep(min(target_wall_elapsed - now_wall_elapsed, 1.0))

                if dataset_elapsed >= target_duration_s:
                    break

            last_arrival = arrival

            payload = dict(raw)
            payload["dataset_type"] = dataset_type
            payload.setdefault("deployment_type", dataset_type)

            fut = producer.send(processed_topic, payload)
            pending_futures.append(fut)
            sent += 1

            # Backpressure + batching
            if len(pending_futures) >= producer_batch_size:
                for f in pending_futures:
                    # raises on error
                    f.get(timeout=60)
                pending_futures.clear()

            if max_events is not None and sent >= max_events:
                break

        # flush remaining
        for f in pending_futures:
            f.get(timeout=60)
        pending_futures.clear()

        producer.flush()
        logger.info("Replay producer finished. sent=%d topic=%s dataset=%s", sent, processed_topic, dataset_type)

    finally:
        try:
            producer.flush()
        except Exception:
            pass
        producer.close()
