import json

# Assume the two data sources are ORDERS_CSV and PRODUCTS_JSON
# Below are what they look like

# 1. Orders Data (CSV)
csv_content = (
    "order_id,product_sku,quantity,sale_date,customer_id\n"
    "1001,TIRE_R_001,2,2023-10-01,C123\n"
    "1002,HELMET_A_005,1,2023-10-01,C124\n"
    "1003,TIRE_R_001,5,2023-10-02,C125\n"
    "1004,BRAKE_L_002,3,2023-10-02,C126\n"
    "1005,TIRE_R_001,2,2023-10-03,C127\n"
    "1006,HELMET_A_005,1,2023-10-03,C128\n"
    "1007,TIRE_R_001,5,2023-10-04,C129\n"
    "1008,BRAKE_L_002,3,2023-10-04,C130\n"
    "1009,TIRE_R_001,2,2023-10-05,C131\n"
    "1010,HELMET_A_005,1,2023-10-05,C132\n"
    "1011,TIRE_R_001,5,2023-10-06,C133\n"
    "1012,BRAKE_L_002,3,2023-10-06,C134\n"
    "1013,TIRE_R_001,2,2023-10-07,C135\n"
    "1014,HELMET_A_005,1,2023-10-07,C136\n"
    "1015,TIRE_R_001,5,2023-10-08,C137\n"
    "1016,BRAKE_L_002,3,2023-10-08,C138\n"
    "1017,TIRE_R_001,2,2023-10-09,C139\n"
    "1018,HELMET_A_005,1,2023-10-09,C140\n"
    "1019,TIRE_R_001,5,2023-10-10,C141\n"
    "1020,BRAKE_L_002,3,2023-10-10,C142\n"
    "1021,TIRE_R_001,2,2023-10-11,C143\n"
    "1022,HELMET_A_005,1,2023-10-11,C144\n"
    "1023,TIRE_R_001,5,2023-10-12,C145\n"
    "1024,BRAKE_L_002,3,2023-10-12,C146\n"
    "1025,TIRE_R_001,2,2023-10-13,C147\n"
    "1026,HELMET_A_005,1,2023-10-13,C148\n"
    "1027,TIRE_R_001,5,2023-10-14,C149\n"
    "1028,BRAKE_L_002,3,2023-10-14,C150\n"
    "1029,TIRE_R_001,2,2023-10-15,C151\n"
    "1030,HELMET_A_005,1,2023-10-15,C152\n"
    "1031,TIRE_R_001,5,2023-10-16,C153\n"
    "1032,BRAKE_L_002,3,2023-10-16,C154\n"
    "1033,TIRE_R_001,2,2023-10-17,C155\n"
    "1034,HELMET_A_005,1,2023-10-17,C156\n"
    "1035,TIRE_R_001,5,2023-10-18,C157\n"
    "1036,BRAKE_L_002,3,2023-10-18,C158\n"
    "1037,TIRE_R_001,2,2023-10-19,C159\n"
    "1038,HELMET_A_005,1,2023-10-19,C160\n"
    "1039,TIRE_R_001,5,2023-10-20,C161\n"
    "1040,BRAKE_L_002,3,2023-10-20,C162\n"
    "1041,TIRE_R_001,2,2023-10-21,C163\n"
    "1042,HELMET_A_005,1,2023-10-21,C164\n"
    "1043,TIRE_R_001,5,2023-10-22,C165\n"
    "1044,BRAKE_L_002,3,2023-10-22,C166\n"
    "1045,TIRE_R_001,2,2023-10-23,C167\n"
    "1046,HELMET_A_005,1,2023-10-23,C168\n"
    "1047,TIRE_R_001,5,2023-10-24,C169\n"
    "1048,BRAKE_L_002,3,2023-10-24,C170\n"
    "1049,TIRE_R_001,2,2023-10-25,C171\n"
    "1050,HELMET_A_005,1,2023-10-25,C172\n"
    "1051,TIRE_R_001,5,2023-10-26,C173\n"
    "1052,BRAKE_L_002,3,2023-10-26,C174\n"
    "1053,TIRE_R_001,2,2023-10-27,C175\n"
    "1054,HELMET_A_005,1,2023-10-27,C176\n"
    "1055,TIRE_R_001,5,2023-10-28,C177\n"
    "1056,BRAKE_L_002,3,2023-10-28,C178\n"
    "1057,TIRE_R_001,2,2023-10-29,C179\n"
    "1058,HELMET_A_005,1,2023-10-29,C180\n"
    "1059,TIRE_R_001,5,2023-10-30,C181\n"
    "1060,BRAKE_L_002,3,2023-10-30,C182\n"
    "1061,TIRE_R_001,2,2023-10-31,C183\n"
)


# 2. Products Data (JSON - Simulating API dump)
product_data = [
    {'sku': 'TIRE_R_001', 'name': 'Road Tire Pro', 'category': 'Wheels', 'unit_price': 50.00, 'is_active': True},
    {'sku': 'HELMET_A_005', 'name': 'Aero Helmet 5', 'category': 'Accessories', 'unit_price': 120.00, 'is_active': True},
    {'sku': 'BRAKE_L_002', 'name': 'Light Brake', 'category': 'Brakes', 'unit_price': 80.00, 'is_active': True},
    {'sku': 'TIRE_R_002', 'name': 'Mountain Tire', 'category': 'Wheels', 'unit_price': 60.00, 'is_active': True},
    {'sku': 'HELMET_A_006', 'name': 'Street Helmet', 'category': 'Accessories', 'unit_price': 150.00, 'is_active': True},
    {'sku': 'BRAKE_L_003', 'name': 'Heavy Duty Brake', 'category': 'Brakes', 'unit_price': 100.00, 'is_active': True},
    {'sku': 'TIRE_R_003', 'name': 'Hybrid Tire', 'category': 'Wheels', 'unit_price': 70.00, 'is_active': True},
    {'sku': 'HELMET_A_007', 'name': 'Sport Helmet', 'category': 'Accessories', 'unit_price': 180.00, 'is_active': True},
    {'sku': 'BRAKE_L_004', 'name': 'Super Brake', 'category': 'Brakes', 'unit_price': 120.00, 'is_active': True},
    {'sku': 'TIRE_R_004', 'name': 'Performance Tire', 'category': 'Wheels', 'unit_price': 80.00, 'is_active': True},
    {'sku': 'HELMET_A_008', 'name': 'Race Helmet', 'category': 'Accessories', 'unit_price': 200.00, 'is_active': True},
    {'sku': 'BRAKE_L_005', 'name': 'Ultimate Brake', 'category': 'Brakes', 'unit_price': 150.00, 'is_active': True},
    {'sku': 'TIRE_R_005', 'name': 'Premium Tire', 'category': 'Wheels', 'unit_price': 90.00, 'is_active': True},
    {'sku': 'HELMET_A_009', 'name': 'Pro Helmet', 'category': 'Accessories', 'unit_price': 220.00, 'is_active': True},
    {'sku': 'BRAKE_L_006', 'name': 'Advanced Brake', 'category': 'Brakes', 'unit_price': 160.00, 'is_active': True},
    {'sku': 'TIRE_R_006', 'name': 'Ultimate Tire', 'category': 'Wheels', 'unit_price': 100.00, 'is_active': True},
    {'sku': 'HELMET_A_010', 'name': 'Ultimate Helmet', 'category': 'Accessories', 'unit_price': 250.00, 'is_active': True},
    {'sku': 'BRAKE_L_007', 'name': 'Ultimate Brake', 'category': 'Brakes', 'unit_price': 180.00, 'is_active': True}
]


if __name__ == "__main__":

    ORDERS_CSV = 'orders_data.csv'
    PRODUCT_JSON = 'product_inventory.json'

    # Write the CSV file
    with open(ORDERS_CSV, 'w') as f:
        f.write(csv_content)

    # Write the JSON file
    with open(PRODUCT_JSON, 'w') as f:
        json.dump(product_data, f, indent=4)


