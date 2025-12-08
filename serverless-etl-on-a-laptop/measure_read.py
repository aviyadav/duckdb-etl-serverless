import duckdb as dd, time

def run(sql):
    con = dd.connect(":memory:")
    t0 = time.time()
    res = con.execute(sql).fetchall()
    return time.time() - t0, res

def csv_to_parquet():
    con = dd.connect(":memory:")
    con.execute("""
        COPY (
            SELECT *, YEAR(ds) AS Year 
            FROM read_csv_auto('data/csv_raw/orders_*.csv', AUTO_DETECT=TRUE)
        )
        TO 'data/raw_parquet' (FORMAT PARQUET, PARTITION_BY (Year), COMPRESSION ZSTD)
    """)

def compute_daily_aggregates():
    con = dd.connect(":memory:")
    con.execute("""
        COPY (
        SELECT customer_id, ds,
                COUNT(*) AS orders, SUM(total) AS gross,
                AVG(total) AS avg_ticket
        FROM read_parquet('data/raw/orders_*.parquet')
        GROUP BY ALL
        ) TO 'data/features/customer_daily' (FORMAT PARQUET, PARTITION_BY (ds))
    """)

def big_to_smal_join():
    con = dd.connect(":memory:")
    con.execute("CREATE VIEW dim AS SELECT * FROM read_parquet('data/dim_customer.parquet')")
    con.execute("""
        COPY (
        SELECT f.ds, f.customer_id, d.segment, SUM(f.total) AS gross
        FROM read_parquet('data/raw/orders_*.parquet') f
        JOIN dim d USING (customer_id)
        WHERE f.ds BETWEEN DATE '2025-10-01' AND DATE '2025-10-15'
        GROUP BY ALL
        ) TO 'data/out/gross_by_segment.parquet' (FORMAT PARQUET)
    """)

def parquet_to_csv():
    import os
    from pathlib import Path
    
    con = dd.connect(":memory:")
    
    # Define input and output directories
    input_dir = Path("data/raw")
    output_dir = Path("data/csv_raw")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all parquet files in the input directory
    parquet_files = list(input_dir.glob("*.parquet"))
    
    if not parquet_files:
        print(f"No parquet files found in {input_dir}")
        return
    
    # Convert each parquet file to CSV
    for parquet_file in parquet_files:
        csv_filename = parquet_file.stem + ".csv"
        csv_path = output_dir / csv_filename
        
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{parquet_file}')
            ) TO '{csv_path}' (FORMAT CSV, HEADER TRUE)
        """)
        print(f"Converted {parquet_file.name} -> {csv_filename}")

if __name__ == "__main__":
    
    # total_secs, _ = run("""
    #     SELECT count(*) FROM read_parquet('data/raw/orders_*.parquet')
    # """)

    # print(f"Total DuckDB read: {total_secs:.2f} seconds for full count")

    # local_secs, _ = run("""
    #     SELECT count(*) FROM read_parquet('data/raw/orders_*.parquet') WHERE ds >= '2023-10-03'
    # """)

    # print(f"Local DuckDB read: {local_secs:.2f} seconds for filtered count")
    
    csv_to_parquet()

    # parquet_to_csv()

    # compute_daily_aggregates()
    # print("ETL complete.")

    # big_to_smal_join()
    # print("Join complete.")