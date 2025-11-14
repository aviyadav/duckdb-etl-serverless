import duckdb as dd
import random
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker

# Setup
fake = Faker()
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configuration
NUM_CUSTOMERS = 5000  # Generate 5000 unique customers

SEGMENTS = ['Premium', 'Standard', 'Budget', 'Enterprise']
CUSTOMER_TYPES = ['B2B', 'B2C', 'Wholesale']
LOYALTY_TIERS = ['Gold', 'Silver', 'Bronze', 'New']
REGIONS = ['North', 'South', 'East', 'West', 'Central']
ACQUISITION_CHANNELS = ['Online', 'Referral', 'Direct', 'Partner', 'Social Media', 'Email Campaign']

def generate_customers():
    """Generate customer dimension data"""
    customers = []
    
    # Use consistent customer IDs that match the orders (1000-9999)
    customer_ids = list(range(1000, 10000))
    random.shuffle(customer_ids)
    
    for i in range(NUM_CUSTOMERS):
        customer_id = customer_ids[i]
        
        # Generate acquisition date (last 3 years)
        acquisition_date = fake.date_between(start_date='-3y', end_date='-1d')
        last_order_date = fake.date_between(start_date=acquisition_date, end_date='today')
        days_since_last = (datetime.now().date() - last_order_date).days
        
        # Generate business metrics
        total_orders = random.randint(1, 500)
        lifetime_value = round(random.uniform(100, 50000), 2)
        average_order_value = round(lifetime_value / total_orders, 2)
        
        # Determine activity status
        is_active = days_since_last <= 180  # Active if ordered in last 6 months
        
        customer = {
            'customer_id': customer_id,
            'customer_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'segment': random.choice(SEGMENTS),
            'customer_type': random.choice(CUSTOMER_TYPES),
            'loyalty_tier': random.choice(LOYALTY_TIERS),
            'city': fake.city(),
            'state': fake.state(),
            'country': 'USA',
            'zip_code': fake.zipcode(),
            'region': random.choice(REGIONS),
            'acquisition_date': acquisition_date.strftime('%Y-%m-%d'),
            'acquisition_channel': random.choice(ACQUISITION_CHANNELS),
            'account_manager': fake.name(),
            'is_active': is_active,
            'credit_limit': round(random.uniform(1000, 100000), 2),
            'lifetime_value': lifetime_value,
            'average_order_value': average_order_value,
            'total_orders': total_orders,
            'last_order_date': last_order_date.strftime('%Y-%m-%d'),
            'days_since_last_order': days_since_last
        }
        
        customers.append(customer)
    
    return customers

def main():
    con = dd.connect(database=":memory:")
    
    print("Generating customer dimension data...")
    customers = generate_customers()
    
    # Create table with proper schema
    con.execute("""
        CREATE TABLE dim_customer (
            customer_id BIGINT PRIMARY KEY,
            customer_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            segment VARCHAR,
            customer_type VARCHAR,
            loyalty_tier VARCHAR,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            zip_code VARCHAR,
            region VARCHAR,
            acquisition_date DATE,
            acquisition_channel VARCHAR,
            account_manager VARCHAR,
            is_active BOOLEAN,
            credit_limit DOUBLE,
            lifetime_value DOUBLE,
            average_order_value DOUBLE,
            total_orders BIGINT,
            last_order_date DATE,
            days_since_last_order INTEGER
        )
    """)
    
    # Insert data
    for customer in customers:
        con.execute("""
            INSERT INTO dim_customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            customer['customer_id'],
            customer['customer_name'],
            customer['email'],
            customer['phone'],
            customer['segment'],
            customer['customer_type'],
            customer['loyalty_tier'],
            customer['city'],
            customer['state'],
            customer['country'],
            customer['zip_code'],
            customer['region'],
            customer['acquisition_date'],
            customer['acquisition_channel'],
            customer['account_manager'],
            customer['is_active'],
            customer['credit_limit'],
            customer['lifetime_value'],
            customer['average_order_value'],
            customer['total_orders'],
            customer['last_order_date'],
            customer['days_since_last_order']
        ])
    
    # Write to parquet file
    output_file = DATA_DIR / "dim_customer.parquet"
    con.execute(f"""
        COPY dim_customer TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    
    # Print summary statistics
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_customers,
            COUNT(CASE WHEN is_active THEN 1 END) as active_customers,
            segment,
            COUNT(*) as count_by_segment
        FROM dim_customer
        GROUP BY segment
        ORDER BY segment
    """).fetchall()
    
    con.close()
    
    print(f"\n✓ Created: {output_file}")
    print(f"✓ Total customers: {len(customers)}")
    print(f"\n✓ Breakdown by segment:")
    
    # Get segment breakdown
    con2 = dd.connect(database=":memory:")
    segment_stats = con2.execute(f"""
        SELECT 
            segment,
            COUNT(*) as count,
            AVG(lifetime_value) as avg_ltv,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_count
        FROM read_parquet('{output_file}')
        GROUP BY segment
        ORDER BY segment
    """).fetchall()
    
    for segment, count, avg_ltv, active in segment_stats:
        print(f"  {segment}: {count} customers (avg LTV: ${avg_ltv:.2f}, active: {active})")
    
    con2.close()

if __name__ == "__main__":
    main()
