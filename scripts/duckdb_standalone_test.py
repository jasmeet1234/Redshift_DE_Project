from src.storage.duckdb_store import DuckDBStore

def main():
    store = DuckDBStore("data/duckdb/analytics.duckdb")
    store.init_schema()

    dummy = [
        {
            "query_id": 1,
            "user_id": 1,
            "instance_id": 1,
            "query_type": "select",
            "arrival_timestamp": "2024-01-01T00:00:00Z",
            "execution_duration_ms": 10,
            "queue_duration_ms": 1,
            "compile_duration_ms": 0.5,
            "mbytes_scanned": 5.0,
            "mbytes_spilled": 0.0,
            "queue_score": 0.1,
            "scan_score": 0.5,
            "spill_score": 0.0,
            "compile_score": 0.05,
        }
    ]

    store.insert_events(dummy)
    print("rows:", store.count_events())
    store.close()

if __name__ == "__main__":
    main()