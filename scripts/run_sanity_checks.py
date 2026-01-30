from __future__ import annotations

import subprocess
import time
from pathlib import Path

import duckdb


def run(cmd: str) -> None:
    print(f"\n$ {cmd}")
    subprocess.check_call(cmd, shell=True)


def main() -> None:
    # 1) generate parquet (requires your generator file or use the one-liner)
    if not Path("dummy_query_metrics.parquet").exists():
        run('python -c "from datetime import datetime,timedelta,timezone; import random; import pandas as pd; import pyarrow as pa, pyarrow.parquet as pq; from pathlib import Path; rows=20; now=datetime.now(timezone.utc)-timedelta(minutes=rows); data=[]; '
            'for i in range(rows): '
            ' a=now+timedelta(minutes=i); q=random.choice([0,50,100,200]); c=random.choice([10,20,30]); e=random.randint(500,3000); s=random.uniform(50,500); sp=s*random.choice([0.0,0.05,0.1,0.2]); es=a+timedelta(milliseconds=q); ee=es+timedelta(milliseconds=e); '
            ' data.append({\\"query_id\\":f\\"query_{i+1:03d}\\",\\"deployment_type\\":random.choice([\\"serverless\\",\\"provisioned\\"]),\\"instance_id\\":f\\"i-{random.randint(1000,9999)}\\",\\"arrival_timestamp\\":a,'
            '\\"queue_duration_ms\\":q,\\"compile_duration_ms\\":c,\\"execution_duration_ms\\":e,\\"scanned_mb\\":round(s,2),\\"spilled_mb\\":round(sp,2),\\"execution_start_time\\":es,\\"execution_end_time\\":ee}); '
            'df=pd.DataFrame(data); pq.write_table(pa.Table.from_pandas(df), \\"dummy_query_metrics.parquet\\"); '
            'print(\\"✅ wrote\\", Path(\\"dummy_query_metrics.parquet\\").resolve())"')

    # 2) run producer once (assumes consumer-duckdb already running in another terminal)
    run('python -m src.main producer --input "dummy_query_metrics.parquet"')

    # 3) wait a moment for consumer to write
    time.sleep(2)

    # 4) verify duckdb rows
    con = duckdb.connect("data/duckdb/analytics.duckdb")
    raw = con.execute("select count(*) from query_metrics_raw").fetchone()[0]
    roll = con.execute("select count(*) from metrics_5m_deployment").fetchone()[0]
    print("\n✅ DuckDB counts:", {"raw": raw, "rollup": roll})
    print("✅ Health:", con.execute("select * from v_pipeline_health").fetchall())
    con.close()


if __name__ == "__main__":
    main()