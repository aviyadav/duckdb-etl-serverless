import polars as pl
from utils import measure_performance
import duckdb


@measure_performance
def main():
    # Lazy scan of a partitioned dataset
    df_orders = pl.scan_parquet("data/warehouse/orders/orders.parquet")
    df_customers = pl.scan_parquet("data/warehouse/customers/customers.parquet")

    # Build a lazy pipeline
    pipeline = (
        df_orders
        .filter(pl.col("status") == "delivered")
        .with_columns(
            pl.col("order_date").dt.date().alias("order_ts"),
            pl.col("amount").cast(pl.Float64).alias("amount"),
        )
        .join(
            df_customers.select(
                "customer_id",
                "country",
                "segment",
            ),
            on="customer_id",
            how="inner",
        )
        .group_by("country", "segment")
        .agg([
            pl.col("amount").sum().alias("gmv"),
            pl.col("order_id").count().alias("orders"),
        ])
    )

    # Execute the pipeline
    result = pipeline.collect()
    print(result)

    # using duckdb
    conn = duckdb.connect()
    conn.register("metrics", result.to_arrow())
    
    top_segment = conn.execute("""
        SELECT
            country,
            segment,
            gmv,
            orders,
            ROW_NUMBER() OVER (
                PARTITION BY country
                ORDER BY gmv DESC
            ) as rank_in_country
        FROM metrics
        QUALIFY rank_in_country <= 3
    """).pl()

    print(top_segment)


if __name__ == "__main__":
    main()
