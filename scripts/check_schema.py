import duckdb

con = duckdb.connect("data/analytics.duckdb")
try:
    print(con.execute("DESCRIBE query_events").fetchdf())
finally:
    con.close()