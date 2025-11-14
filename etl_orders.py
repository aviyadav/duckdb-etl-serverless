import duckdb as dd
from pathlib import Path
from time import perf_counter
from contextlib import contextmanager
from datetime import datetime

RAW = Path("data/raw")         # drop *.parquet or *.csv here
STAGE = Path("data/stage")
OUT = Path("data/out")
STAGE.mkdir(parents=True, exist_ok=True); OUT.mkdir(parents=True, exist_ok=True)

con = dd.connect(database=":memory:", read_only=False)  # pure in-memory engine

@contextmanager
def timer(name):
    t0 = perf_counter()
    yield
    dt = perf_counter() - t0
    print(f"{name}: {dt:.2f}s")

# 1) Ingest: lazily scan columns you actually need
con.execute(f"""
    CREATE OR REPLACE VIEW orders AS
    SELECT order_id, customer_id, order_status, total, ds
    FROM read_parquet('{RAW / "orders_*.parquet"}')
""")

# 2) Transform: tidy enums, filter junk, standardize dates
con.execute("""
    CREATE OR REPLACE TABLE orders_clean AS
    SELECT
        order_id::BIGINT,
        customer_id::BIGINT,
        CASE
            WHEN order_status ILIKE 'shipp%' THEN 'shipped'
            WHEN order_status ILIKE 'pend%'  THEN 'pending'
            WHEN order_status ILIKE 'canc%'  THEN 'cancelled'
            ELSE 'other'
        END AS order_status,
        CAST(total AS DOUBLE) AS total,
        CAST(ds AS DATE)       AS ds
    FROM orders
    WHERE total IS NOT NULL AND total >= 0
""")

# 3) Validate: cheap data tests that actually catch pain
result = con.execute("""
    SELECT
      COUNT(*)                       AS rows_total,
      SUM(total < 0)                 AS neg_totals,
      SUM(order_status NOT IN ('shipped','pending','cancelled','other')) AS bad_status
    FROM orders_clean
""")
tests = result.fetchone()

assert tests is not None, "No test results returned"
assert tests[1] == 0, "Negative totals found"
assert tests[2] == 0, "Unexpected order_status values"

# 4) Aggregate & publish - generate monthly files with date suffix
with timer("aggregate & write"):
    # Get the distinct months from the data
    months = con.execute("""
        SELECT DISTINCT strftime(ds, '%Y-%m') AS month
        FROM orders_clean
        ORDER BY month
    """).fetchall()
    
    for (month,) in months:
        output_file = OUT / f"orders_{month}.parquet"
        con.execute(f"""
            COPY (
              SELECT ds, order_status, COUNT(*) AS orders, SUM(total) AS gross
              FROM orders_clean
              WHERE strftime(ds, '%Y-%m') = '{month}'
              GROUP BY ALL
              ORDER BY ds, order_status
            ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"OK → {output_file}")