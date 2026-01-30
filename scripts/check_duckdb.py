import duckdb

DB_PATH = "data/analytics.duckdb"

def main():
    # Works on Windows even if the consumer is running, as long as consumer isn't using an exclusive lock.
    # If you still get "file is being used", just stop the consumer briefly, run this, then restart consumer.
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        print("Tables:", tables)

        n = con.execute("SELECT COUNT(*) FROM query_events").fetchone()[0]
        print(f"✅ query_events row_count = {n}")

        rows = con.execute("""
            SELECT query_id, arrival_timestamp, execution_duration_ms, queue_duration_ms,
                   mbytes_scanned, mbytes_spilled, was_cached, was_aborted, ingested_at
            FROM query_events
            ORDER BY ingested_at DESC
            LIMIT 5
        """).fetchall()

        print("Last 5 rows:")
        for r in rows:
            print(r)
    finally:
        con.close()

if __name__ == "__main__":
    main()