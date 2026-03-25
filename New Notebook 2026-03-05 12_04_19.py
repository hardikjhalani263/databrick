# Databricks notebook source
# =============================================================================
# E-Commerce Analytics Pipeline — Databricks Free Edition
# =============================================================================
# Description : End-to-end Spark data engineering pipeline using two source
#               files (orders.json + customers.csv) with full transformations:
#               Joins, GroupBy, Case/When, Window Functions, Pivot, Delta write
#
# Architecture: Bronze → Silver → Gold (Medallion)
# Author      : Data Engineering Template
# Compatibility: Databricks Community Edition | Apache Spark 3.x | Python 3.8+
# =============================================================================

import json
import random
from datetime import datetime, timedelta

# from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, LongType
)
from pyspark.sql.window import Window


# =============================================================================
# INITIALISE SPARK SESSION
# =============================================================================


# =============================================================================
# STEP 1 — CREATE SAMPLE SOURCE FILES
# =============================================================================
# Generates:  /dbfs/FileStore/ecommerce/orders.json    (200 orders, nested items)
#             /dbfs/FileStore/ecommerce/customers.csv   (50 customers)

def generate_source_files():
    """Write synthetic orders.json and customers.csv to DBFS."""

    import os
    os.makedirs("/Volumes/bigdata2/session_learning/use-case2/ecommerce", exist_ok=True)

    random.seed(42)  # reproducible data

    # ── orders.json (newline-delimited JSON) ─────────────────────────────────
    statuses        = ["delivered", "shipped", "cancelled", "returned", "pending"]
    categories      = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    payment_methods = ["credit_card", "debit_card", "UPI", "netbanking", "wallet"]
    cities          = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"]

    orders = []
    for i in range(1, 201):
        num_items  = random.randint(1, 4)
        items      = []
        for j in range(num_items):
            qty   = random.randint(1, 5)
            price = round(random.uniform(50, 5000), 2)
            items.append({
                "item_id":      f"ITEM{random.randint(100, 999)}",
                "product_name": f"Product_{random.choice(categories)}_{j + 1}",
                "category":     random.choice(categories),
                "quantity":     qty,
                "unit_price":   price,
                "line_total":   round(qty * price, 2),
            })

        order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
        orders.append({
            "order_id":       f"ORD{i:05d}",
            "customer_id":    f"CUST{random.randint(1, 50):04d}",
            "order_date":     order_date.strftime("%Y-%m-%d"),
            "order_status":   random.choice(statuses),
            "payment_method": random.choice(payment_methods),
            "shipping_city":  random.choice(cities),
            "discount_pct":   random.choice([0, 5, 10, 15, 20]),
            "items":          items,
        })

    with open("/Volumes/bigdata2/session_learning/use-case2/ecommerce/orders.json", "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

    # ── customers.csv ─────────────────────────────────────────────────────────
    tiers  = ["Bronze", "Silver", "Gold", "Platinum"]
    states = ["Maharashtra", "Delhi", "Karnataka", "Tamil Nadu", "Telangana"]

    csv_lines = ["customer_id,name,email,age,gender,city,state,tier,signup_date,is_active"]
    for i in range(1, 51):
        signup = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 800))
        csv_lines.append(
            f"CUST{i:04d},"
            f"Customer_{i},"
            f"cust{i}@email.com,"
            f"{random.randint(22, 60)},"
            f"{random.choice(['M', 'F', 'Other'])},"
            f"{random.choice(cities)},"
            f"{random.choice(states)},"
            f"{random.choice(tiers)},"
            f"{signup.strftime('%Y-%m-%d')},"
            f"{random.choice(['true', 'true', 'true', 'false'])}"
        )

    with open("/Volumes/bigdata2/session_learning/use-case2/ecommerce/customers.csv", "w") as f:
        f.write("\n".join(csv_lines))

    print(f"orders.json written   → {len(orders)} records")
    print(f" customers.csv written → {len(csv_lines) - 1} records")


generate_source_files()
