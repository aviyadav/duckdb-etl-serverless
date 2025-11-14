import duckdb as dd, json
from pathlib import Path

META = Path("data/meta.json")
state = json.loads(META.read_text()) if META.exists() else {"last_ds": "1970-01-01"}

con = dd.connect(":memory:")
con.execute("CREATE VIEW orders AS SELECT * FROM read_parquet('data/raw/orders_*.parquet')")

# Find new partition window
last = state["last_ds"]
result = con.execute("SELECT max(ds)::VARCHAR FROM orders").fetchone()
new_max = result[0] if result else None

# Nothing to do
if not new_max or new_max <= last:
    print("No new partitions"); raise SystemExit(0)

# Process only new days
con.execute("""
COPY (
  SELECT ds, order_status, COUNT(*) AS orders, SUM(total) AS gross
  FROM orders
  WHERE ds > ?
  GROUP BY ALL
) TO 'data/out/orders_daily_incr.parquet' (FORMAT PARQUET)
""", [last])

state["last_ds"] = new_max
META.write_text(json.dumps(state))
print("Incremental publish →", new_max)

