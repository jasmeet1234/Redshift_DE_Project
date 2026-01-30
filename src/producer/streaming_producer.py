# from __future__ import annotations

# import json
# import logging
# import time
# from datetime import datetime, timezone
# from typing import Any, Dict, Iterator, Optional

# import duckdb
# from confluent_kafka import Producer

# from src.common.logging import configure_logging
# from src.common.schema import UiQueryMetricsEvent
# from src.common.settings import Settings
# from src.producer.metrics_calc import build_event

# logger = logging.getLogger(__name__)


# def _utc_ts(dt: datetime) -> float:
#     if dt.tzinfo is None:
#         dt = dt.replace(tzinfo=timezone.utc)
#     return dt.astimezone(timezone.utc).timestamp()


# def _iter_parquet_rows_duckdb(url: str, batch_size: int = 10_000) -> Iterator[Dict[str, Any]]:
#     """
#     Stream parquet rows directly from an HTTPS URL using DuckDB (httpfs).
#     """
#     con = duckdb.connect()
#     try:
#         con.execute("INSTALL httpfs;")
#         con.execute("LOAD httpfs;")

#         rel = con.execute(f"SELECT * FROM read_parquet('{url}');")
#         cols = [d[0] for d in rel.description]

#         while True:
#             rows = rel.fetchmany(batch_size)
#             if not rows:
#                 break
#             for r in rows:
#                 yield {cols[i]: r[i] for i in range(len(cols))}
#     finally:
#         try:
#             con.close()
#         except Exception:
#             pass


# def run_streaming_producer(settings: Settings) -> None:
#     """
#     Reads parquet rows from the configured S3 HTTPS URL, cleans/enriches them,
#     then publishes UiQueryMetricsEvent JSON into Kafka.
#     """
#     configure_logging()

#     kafka_cfg = settings.kafka
#     topic = kafka_cfg.topics.processed_query_metrics
#     producer = Producer({"bootstrap.servers": kafka_cfg.bootstrap_servers})

#     url = settings.dataset.source_url
#     batch_size = int(settings.dataset.batch_read_size)

#     time_scale = float(settings.replay.time_scale_factor) if settings.replay.time_scale_factor > 0 else 1.0
#     max_events = settings.replay.max_events

#     rows = _iter_parquet_rows_duckdb(url, batch_size=batch_size)

#     first_event_ts: Optional[float] = None
#     wall_start: Optional[float] = None
#     sent = 0

#     logger.info("Producer starting. source_url=%s topic=%s time_scale=%s", url, topic, time_scale)

#     for row in rows:
#         if max_events is not None and sent >= int(max_events):
#             logger.info("Reached max_events=%s; stopping producer.", max_events)
#             break

#         evt = build_event(row)
#         if evt is None:
#             continue

#         ui_evt = UiQueryMetricsEvent.from_canonical(evt)
#         evt_ts = _utc_ts(ui_evt.arrival_timestamp)

#         if first_event_ts is None:
#             first_event_ts = evt_ts
#             wall_start = time.time()

#         assert wall_start is not None and first_event_ts is not None

#         dataset_elapsed = evt_ts - first_event_ts
#         target_wall_elapsed = dataset_elapsed / time_scale
#         now_wall_elapsed = time.time() - wall_start

#         sleep_s = target_wall_elapsed - now_wall_elapsed
#         if sleep_s > 0:
#             time.sleep(min(sleep_s, 2.0))

#         payload = ui_evt.model_dump(mode="json")
#         producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
#         sent += 1

#         if sent % 500 == 0:
#             producer.flush(5)
#             logger.info("Published %s events...", sent)

#     producer.flush(10)
#     logger.info("Producer finished. total_published=%s", sent)


# if __name__ == "__main__":
#     run_streaming_producer(Settings.load())