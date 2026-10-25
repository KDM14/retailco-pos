"""
Unit and integration tests for PySpark Transformations.
Validates:
- Date normalization under mixed date string representations
- Financial reconciliation logic
- Broadcast dimension join semantics
- Two-Phase Salting Skew Mitigation equivalence
"""
import pytest
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

from spark.transforms import (
    clean_sales_dataframe,
    apply_broadcast_dimension_join,
    apply_salted_skew_mitigation
)

def test_clean_sales_dataframe_date_and_financials(spark):
    data = [
        Row(
            transaction_id="TXN_01", store_id="store_01", customer_id="C1",
            product_code="sku_01", quantity=3, unit_price_inr=100.0,
            total_amount_inr=None, discount_pct=10.0, transaction_date="2024-01-15"
        ),
        Row(
            transaction_id="TXN_02", store_id="store_02", customer_id="C2",
            product_code="sku_02", quantity=2, unit_price_inr=200.0,
            total_amount_inr=400.0, discount_pct=0.0, transaction_date="15-01-2024"
        ),
        Row(
            transaction_id="TXN_03", store_id="store_03", customer_id="C3",
            product_code="sku_03", quantity=1, unit_price_inr=50.0,
            total_amount_inr=50.0, discount_pct=0.0, transaction_date="invalid-date"
        )
    ]
    df = spark.createDataFrame(data)
    cleaned = clean_sales_dataframe(df)
    rows = cleaned.collect()

    assert len(rows) == 3

    # Date parsing assertions
    assert rows[0]["parsed_date"] == date(2024, 1, 15)
    assert rows[1]["parsed_date"] == date(2024, 1, 15) # dd-MM-yyyy format
    assert rows[2]["parsed_date"] is None

    # Financial metric calculations
    # Row 0: gross = 300.0, discount = 30.0, net = 270.0
    assert rows[0]["gross_amount_inr"] == 300.0
    assert rows[0]["discount_amount_inr"] == 30.0
    assert rows[0]["net_amount_inr"] == 270.0
    assert rows[0]["total_amount"] == 270.0

    # Upper case normalization
    assert rows[0]["store_id"] == "STORE_01"
    assert rows[0]["product_code"] == "SKU_01"

def test_apply_broadcast_dimension_join(spark):
    fact_data = [
        Row(store_id="STORE_001", total_amount=500.0),
        Row(store_id="STORE_002", total_amount=250.0)
    ]
    dim_data = [
        Row(store_id="STORE_001", region="South", city="Bengaluru"),
        Row(store_id="STORE_002", region="West", city="Mumbai")
    ]
    fact_df = spark.createDataFrame(fact_data)
    dim_df = spark.createDataFrame(dim_data)

    joined = apply_broadcast_dimension_join(fact_df, dim_df)
    results = joined.collect()

    assert len(results) == 2
    bengaluru_row = [r for r in results if r["store_id"] == "STORE_001"][0]
    assert bengaluru_row["city"] == "Bengaluru"
    assert bengaluru_row["region"] == "South"

def test_salted_skew_mitigation_mathematical_equivalence(spark):
    """
    Proves that 2-Phase Salting produces 100% mathematically equivalent aggregates
    to standard groupBy while breaking up single-partition skew.
    """
    data = [
        Row(parsed_date=date(2024, 1, 15), store_id="STORE_999", total_amount=100.0, quantity=2),
        Row(parsed_date=date(2024, 1, 15), store_id="STORE_999", total_amount=200.0, quantity=4),
        Row(parsed_date=date(2024, 1, 15), store_id="STORE_999", total_amount=300.0, quantity=6),
        Row(parsed_date=date(2024, 1, 15), store_id="STORE_999", total_amount=400.0, quantity=8),
        Row(parsed_date=date(2024, 1, 15), store_id="STORE_001", total_amount=150.0, quantity=1),
    ]
    df = spark.createDataFrame(data)

    # 1. Unsalted standard aggregation (Baseline ground truth)
    unsalted_df = (
        df.groupBy("parsed_date", "store_id")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_sales_amount"),
            F.count("*").alias("total_transaction_count"),
            F.sum("quantity").alias("total_quantity_sold")
        )
    )
    unsalted_results = {
        (r["parsed_date"], r["store_id"]): (r["total_sales_amount"], r["total_transaction_count"], r["total_quantity_sold"])
        for r in unsalted_df.collect()
    }

    # 2. Salted 2-Phase Aggregation
    salted_df = apply_salted_skew_mitigation(df, num_salts=3)
    salted_results = {
        (r["parsed_date"], r["store_id"]): (r["total_sales_amount"], r["total_transaction_count"], r["total_quantity_sold"])
        for r in salted_df.collect()
    }

    # Verification: Every key must match exactly
    assert salted_results == unsalted_results
    assert salted_results[(date(2024, 1, 15), "STORE_999")] == (1000.0, 4, 20)
    assert salted_results[(date(2024, 1, 15), "STORE_001")] == (150.0, 1, 1)
