import polars as pl
from faker import Faker
import random
import os
from utils import measure_performance

@measure_performance
def generate_data():
    fake = Faker()

    # Generate customers
    customers = []

    segments = ['high_value', 'medium_value', 'low_value', 'undefined']
    countries = ['USA', 'Canada', 'Mexico', 'Brazil', 'Argentina']

    for i in range(500):
        customer = {
            'customer_id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address_line_1': fake.street_address(),
            'address_line_2': fake.secondary_address() if random.choice([True, False]) else '',
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'country': random.choice(countries),
            'segment': random.choice(segments),
            'registration_date': fake.date_this_decade()
        }
        customers.append(customer)

    df_customers = pl.DataFrame(customers)
    os.makedirs('data/warehouse/customers', exist_ok=True)
    df_customers.write_parquet('data/warehouse/customers/customers.parquet')

    # Generate orders
    orders = []
    statuses = ['pending', 'shipped', 'delivered', 'cancelled']
    payment_methods = ['credit_card', 'paypal', 'bank_transfer']

    for i in range(10000):
        customer_id = random.randint(1, 500)
        order = {
            'order_id': i + 1,
            'customer_id': customer_id,
            'order_date': fake.date_this_year(),
            'amount': round(random.uniform(10, 1000), 2),
            'status': random.choice(statuses),
            'payment_method': random.choice(payment_methods),
            'shipping_address': f"{fake.street_address()}, {fake.city()}, {fake.state()} {fake.zipcode()}",
            'billing_address': f"{fake.street_address()}, {fake.city()}, {fake.state()} {fake.zipcode()}",
            'item_details': f"Item {random.randint(1, 100)} x{random.randint(1, 5)}"
        }
        orders.append(order)

    df_orders = pl.DataFrame(orders)
    os.makedirs('data/warehouse/orders', exist_ok=True)
    df_orders.write_parquet('data/warehouse/orders/orders.parquet')

    print("Data generation complete. Files: customers.parquet and orders.parquet")

if __name__ == "__main__":
    generate_data()