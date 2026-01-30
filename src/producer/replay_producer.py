from __future__ import annotations

import json
import logging
import os
import time
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

from kafka import KafkaProducer

from src.common.schema import QueryMetricsEvent
from src.common.settings import Settings
from src.producer.metrics_calc import map_clean_enrich_row
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


def _iter_raw_rows(source: Union[str, Path], settings: Settings) -> Iterable[Dict[str, Any]]:
    src = str(source)
    reader = ParquetReader(
        parquet_url=src,
        event_time_col=settings.dataset.event_time_column,
        batch_size=settings.dataset.batch_read_size,
        enforce_event_time_order=settings.dataset.enforce_event_time_order,
    )
    return reader


def run_replay_producer(input_path: Union[str, Path], settings: Settings) -> None:
    """
    Read Parquet rows (URL or local path), clean/enrich, dedupe, and publish to Kafka.

    Produces cleaned canonical events to:
      - settings.kafka.topics.processed_query_metrics

    Replay timing:
      - Uses event-time deltas (arrival_timestamp) scaled by settings.replay.time_scale_factor.
        Example: factor=50 -> 50x speed-up (sleep = delta / 50).
    """
    topics = settings.kafka.topics
    processed_topic = topics.processed_query_metrics

    replay_factor = int(settings.replay.time_scale_factor)
    producer_batch_size = int(settings.replay.producer_batch_size)
    max_events = settings.replay.max_events

    proc_cfg = settings.processing

    deduper: Optional[TTLKeyDeduper] = None
    if proc_cfg.duplicates.enabled:
        deduper = TTLKeyDeduper(
            ttl_seconds=proc_cfg.duplicates.ttl_seconds,
            max_size=max(50_000, producer_batch_size * 50),
        )

    logger.info(
        "Replay producer starting. source=%s processed_topic=%s replay_factor=%sx batch_read=%d batch_send=%d max_events=%s",
        str(input_path),
        processed_topic,
        replay_factor,
        settings.dataset.batch_read_size,
        producer_batch_size,
        str(max_events),
    )

    producer = _make_producer(settings)

    sent = 0
    last_arrival: Optional[datetime] = None
    pending_futures = []

    try:
        for raw in _iter_raw_rows(input_path, settings):
            # 1) Clean + enrich into canonical event
            try:
                evt: QueryMetricsEvent = map_clean_enrich_row(raw, proc_cfg)
            except Exception as e:
                logger.debug("Dropping row due to mapping/cleaning error: %s | row_keys=%s", str(e), list(raw.keys()))
                continue

            # 2) Deduplicate if enabled
            if deduper is not None:
                key_cols = proc_cfg.duplicates.key_columns
                key = tuple(getattr(evt, c) for c in key_cols if hasattr(evt, c))
                now_ts = time.time()
                if deduper.seen_or_add(key, now_ts):
                    continue

            # 3) Replay timing (event-time based)
            if replay_factor > 0:
                if last_arrival is not None:
                    delta = (evt.arrival_timestamp - last_arrival).total_seconds()
                    if delta > 0:
                        time.sleep(delta / float(replay_factor))
                last_arrival = evt.arrival_timestamp

            # 4) Publish to Kafka (processed stream)
            fut = producer.send(processed_topic, evt.model_dump())
            pending_futures.append(fut)
            sent += 1

            # 5) Backpressure + batching
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
        logger.info("✅ Replay producer finished. sent=%d processed_topic=%s", sent, processed_topic)

    finally:
        try:
            producer.flush()
        except Exception:
            pass
        producer.close()