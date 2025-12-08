import duckdb as dd
import random
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker

# Setup
fake = Faker()
RAW = Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

# Configuration
START_DATE = datetime(2025, 10, 1)
END_DATE = datetime(2025, 10, 31)
ROWS_PER_DAY_MIN = 200
ROWS_PER_DAY_MAX = 500

ORDER_STATUSES = ['shipped', 'pending', 'cancelled', 'new', 'other']
SEGMENTS = ['Premium', 'Standard', 'Budget', 'Enterprise']
ITEMS = [
    ('ITM001', 'Laptop'),
    ('ITM002', 'Mouse'),
    ('ITM003', 'Keyboard'),
    ('ITM004', 'Monitor'),
    ('ITM005', 'Headphones'),
    ('ITM006', 'Webcam'),
    ('ITM007', 'USB Cable'),
    ('ITM008', 'Desk Chair'),
    ('ITM009', 'Desk Lamp'),
    ('ITM010', 'Phone Case'),
    ('ITM011', 'Charger'),
    ('ITM012', 'Speaker'),
    ('ITM013', 'Router'),
    ('ITM014', 'Hard Drive'),
    ('ITM015', 'SSD'),
]

def generate_orders_for_date(date, num_orders, customer_segments):
    """Generate random orders data for a specific date"""
    orders = []
    
    for _ in range(num_orders):
        order_id = fake.unique.random_int(min=100000, max=999999)
        customer_id = random.randint(1000, 9999)
        customer_name = fake.name()
        status = random.choice(ORDER_STATUSES)
        
        # Get customer segment from mapping, or assign random if not found
        segment = customer_segments.get(customer_id, random.choice(SEGMENTS))
        
        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        items_in_order = random.sample(ITEMS, num_items)
        
        total = 0
        for item_id, item_name in items_in_order:
            item_price = round(random.uniform(10.0, 500.0), 2)
            total += item_price
            
            orders.append({
                'order_id': order_id,
                'customer_id': customer_id,
                'customer_name': customer_name,
                'customer_segment': segment,
                'order_status': status,
                'item_id': item_id,
                'item_name': item_name,
                'item_price': item_price,
                'total': round(total, 2),
                'ds': date.strftime('%Y-%m-%d')
            })
    
    return orders

def load_customer_segments():
    """Load customer segments from dimension file if it exists"""
    dim_file = Path("data/dim_customer.parquet")
    if dim_file.exists():
        con = dd.connect(database=":memory:")
        result = con.execute(f"""
            SELECT customer_id, segment 
            FROM read_parquet('{dim_file}')
        """).fetchall()
        con.close()
        return {customer_id: segment for customer_id, segment in result}
    else:
        print("⚠ Customer dimension file not found. Using random segments.")
        return {}

def main():
    con = dd.connect(database=":memory:")
    
    # Load customer segments from dimension file
    customer_segments = load_customer_segments()
    
    current_date = START_DATE
    files_created = []
    
    while current_date <= END_DATE:
        num_orders = random.randint(ROWS_PER_DAY_MIN, ROWS_PER_DAY_MAX)
        orders = generate_orders_for_date(current_date, num_orders, customer_segments)
        
        # Create a temporary table with the data
        con.execute("DROP TABLE IF EXISTS temp_orders")
        con.execute("""
            CREATE TABLE temp_orders (
                order_id BIGINT,
                customer_id BIGINT,
                customer_name VARCHAR,
                customer_segment VARCHAR,
                order_status VARCHAR,
                item_id VARCHAR,
                item_name VARCHAR,
                item_price DOUBLE,
                total DOUBLE,
                ds DATE
            )
        """)
        
        # Insert the generated data
        for order in orders:
            con.execute("""
                INSERT INTO temp_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                order['order_id'],
                order['customer_id'],
                order['customer_name'],
                order['customer_segment'],
                order['order_status'],
                order['item_id'],
                order['item_name'],
                order['item_price'],
                order['total'],
                order['ds']
            ])
        
        # Write to parquet file
        output_file = RAW / f"orders_{current_date.strftime('%Y-%m-%d')}.parquet"
        con.execute(f"""
            COPY temp_orders TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        
        files_created.append(output_file)
        print(f"Created: {output_file} ({len(orders)} rows)")
        
        current_date += timedelta(days=1)
    
    con.close()
    
    print(f"\n✓ Generated {len(files_created)} files in {RAW}")
    print(f"✓ Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    main()
