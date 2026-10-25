#!/usr/bin/env python3
"""
Synthetic POS Transaction Data Generator for RetailCo.
Generates realistic multi-store retail checkout transactions with:
- Multi-region stores across India (Bengaluru, Mumbai, Delhi, Hyderabad, Chennai)
- Skewed transaction distributions (e.g. flagship store STORE_999) to benchmark PySpark salting
- Optional seeded anomalies (null keys, negative prices, date format drifts) to test DQ gates
"""
import argparse
import csv
import random
from datetime import datetime, timedelta

STORES = [
    ("STORE_001", "Indiranagar Metro", "South", "Bengaluru"),
    ("STORE_002", "Bandra West Flagship", "West", "Mumbai"),
    ("STORE_003", "Connaught Place", "North", "Delhi"),
    ("STORE_004", "Hitec City Retail", "South", "Hyderabad"),
    ("STORE_005", "Anna Nagar Central", "South", "Chennai"),
    ("STORE_999", "Koramangala Superstore (Hotspot)", "South", "Bengaluru") # Hot key for skew
]

PRODUCTS = [
    ("SKU_GROC_001", "Basmati Rice 5kg", "Grocery", 450.00),
    ("SKU_GROC_002", "Organic Sunflower Oil 1L", "Grocery", 180.00),
    ("SKU_BEV_001", "Artisan Filter Coffee 250g", "Beverages", 240.00),
    ("SKU_BEV_002", "Green Tea Box 50s", "Beverages", 190.00),
    ("SKU_ELEC_001", "Fast Charging Cable USB-C", "Electronics", 399.00),
    ("SKU_ELEC_002", "Wireless Ergonomic Mouse", "Electronics", 899.00),
    ("SKU_APP_001", "Cotton Crew Neck T-Shirt", "Apparel", 599.00),
    ("SKU_APP_002", "Denim Slim Fit Jeans", "Apparel", 1499.00),
    ("SKU_HOME_001", "Microfiber Cleaning Cloth 3pk", "Home Goods", 149.00),
    ("SKU_HOME_002", "Stainless Steel Water Bottle 1L", "Home Goods", 349.00)
]

PAYMENTS = ["Cash", "UPI", "Credit Card", "Debit Card", "Net Banking"]

def generate_transactions(num_records: int, output_file: str, seed_corrupt: bool = False, skew: bool = True):
    print(f"Generating {num_records} POS transaction records to: {output_file}")
    start_date = datetime(2024, 1, 15)

    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "transaction_id", "store_id", "store_name", "transaction_date",
            "customer_id", "product_code", "product_name", "quantity",
            "unit_price_inr", "total_amount_inr", "discount_pct",
            "payment_method", "cashier_id", "region", "city", "category"
        ])

        for i in range(1, num_records + 1):
            txn_id = f"TXN_{202401150000 + i}"
            
            # Skew distribution: 60% of volume sent to STORE_999 if skew=True
            if skew and random.random() < 0.60:
                store = STORES[5] # STORE_999
            else:
                store = random.choice(STORES[:5])

            prod = random.choice(PRODUCTS)
            qty = random.randint(1, 5)
            unit_price = prod[3]
            discount_pct = random.choice([0.0, 5.0, 10.0, 15.0, 20.0])
            gross = qty * unit_price
            discount_val = gross * (discount_pct / 100.0)
            net_total = round(gross - discount_val, 2)

            cust_id = f"CUST_{random.randint(1000, 9999)}"
            cashier_id = f"CASH_{random.randint(10, 50)}"
            txn_date = (start_date + timedelta(days=random.randint(0, 3))).strftime("%Y-%m-%d")
            payment = random.choice(PAYMENTS)

            # Seed anomalous rows if requested
            if seed_corrupt and (i % 25 == 0):
                anomaly_type = i % 4
                if anomaly_type == 0:
                    txn_id = "" # Null primary key
                elif anomaly_type == 1:
                    qty = -2 # Negative quantity
                elif anomaly_type == 2:
                    unit_price = -50.0 # Negative price
                elif anomaly_type == 3:
                    discount_pct = 150.0 # Impossible discount

            writer.writerow([
                txn_id, store[0], store[1], txn_date,
                cust_id, prod[0], prod[1], qty,
                unit_price, net_total, discount_pct,
                payment, cashier_id, store[2], store[3], prod[2]
            ])

    print(f"✅ Generated {num_records} POS records saved in {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic POS transactions")
    parser.add_argument("--count", type=int, default=5000, help="Number of records to generate")
    parser.add_argument("--output", default="data/raw/synthetic_pos_batch.csv", help="Output CSV path")
    parser.add_argument("--corrupt", action="store_true", help="Seed corrupted records to test DQ gates")
    parser.add_argument("--no-skew", action="store_true", help="Disable hotspot store key skew")
    args = parser.parse_args()

    generate_transactions(args.count, args.output, seed_corrupt=args.corrupt, skew=not args.no_skew)
