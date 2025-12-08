import duckdb
import os
import json

# Define constants for file paths
DB_PATH = 'urbancycle_warehouse.duckdb'
ORDERS_CSV = 'orders_data.csv'
PRODUCTS_JSON = 'product_inventory.json'

def run_et_phase():
    """Connects to DuckDB and loads raw data from CSV and JSON."""

    print("\n--- Starting EL Phase (Extract & Load) ---")

    # Use 'with' for safe connection management
    try:
        with duckdb.connect(DB_PATH) as conn:
            # Create a dedicated schema for raw, untransformed data
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")

            # 1. Load CSV Orders Data (Native Ingest)
            print(f"Loading {ORDERS_CSV} into raw_layer.orders...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw_layer.orders AS
                SELECT * FROM read_csv_auto('{ORDERS_CSV}')
            """)

            # 2. Load JSON Products Data (Native JSON Read)
            # We treat the JSON file as a source table. 
            # The 'UNNEST' function is key to flattening complex arrays in JSON.

            print(f"Loading {PRODUCTS_JSON} into raw_layer.products...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE raw_layer.products AS
                SELECT * FROM read_json_auto('{PRODUCTS_JSON}')
            """)

            # Now, flatten the complex JSON structure into a usable table
            conn.execute("""
                CREATE OR REPLACE TABLE raw_layer.products_flat AS
                SELECT
                    sku as product_sku,
                    name as product_name,
                    category as product_category,
                    unit_price,
                    is_active
                FROM raw_layer.products
            """)

            # Verify the load count
            order_count = conn.sql("SELECT count(order_id) FROM raw_layer.orders").fetchone()[0]
            product_count = conn.sql("SELECT count(product_sku) FROM raw_layer.products_flat").fetchone()[0]
            
            print(f"✅ Loaded {order_count} orders and {product_count} products.")
            return True

    except duckdb.Error as e:
        print(f"❌ Error during EL phase: {str(e)}")
        return False
    except IOError as e:
        print(f"❌ File I/O Error: Ensure {ORDERS_CSV} and {PRODUCTS_JSON} exist. {e}")
        return False

def run_t_phase():
    """Performs transformation and aggregation for the unified report."""

    print("\n--- Starting T Phase (Transform) ---")

    try:
        with duckdb.connect(DB_PATH) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytical_layer")

            # The transformation query for the report sales by product and category
            transformation_query = """
                -- Create the final report table for the business users
                CREATE OR REPLACE TABLE analytical_layer.revenue_by_category AS
                SELECT
                    p.product_category,
                    p.product_name,
                    CAST(STRFTIME(o.sale_date, '%Y-%m') AS VARCHAR) AS sales_month,
                    sum(o.quantity) as total_units_sold,
                    SUM(o.quantity * p.unit_price) as total_revenue_usd,
                    COUNT(DISTINCT o.customer_id) as distinct_customers
                FROM raw_layer.orders o
                JOIN raw_layer.products_flat p ON o.product_sku = p.product_sku
                WHERE p.is_active = True -- only consider active products
                GROUP BY p.product_category, p.product_name, sales_month
                ORDER BY total_revenue_usd DESC;
            """

            conn.execute(transformation_query)
            print("✅ Transformation completed successfully.")

            # Display the final report
            print("\n--- Final UrbanCycle Revenue Report ---")
            result_df = conn.sql("SELECT * FROM analytical_layer.revenue_by_category").df()
            print(result_df.to_markdown(index=False))

            return True

    except duckdb.Error as e:
        print(f"❌ Error during T phase: {str(e)}")
        return False


if __name__ == "__main__":
    if run_et_phase():
        if run_t_phase():
            print("\n--- ETL Process Completed Successfully ---")
        else:
            print("\n--- ETL Process Failed ---")
    else:
        print("\n--- ETL Process Failed ---")
