"""
Tests for Data Quality Verification Gates.
"""
import pytest
from datetime import date
from pyspark.sql import Row

from spark.dq_gates import run_pyspark_dq_suite

def test_dq_gate_pass_on_clean_data(spark):
    clean_data = [
        Row(
            transaction_id="TXN_001", store_id="STORE_001", store_name="Indiranagar",
            transaction_date="2024-01-15", customer_id="CUST_1001", product_code="SKU_001",
            product_name="Basmati Rice", quantity=2, unit_price_inr=100.0,
            total_amount_inr=200.0, net_amount_inr=200.0, discount_pct=0.0,
            payment_method="UPI", cashier_id="CASH_01", region="South",
            city="Bengaluru", category="Grocery", parsed_date=date(2024, 1, 15)
        )
    ]
    df = spark.createDataFrame(clean_data)
    results = run_pyspark_dq_suite(df)

    assert results["overall_status"] == "PASS"
    assert results["failed_check_count"] == 0
    assert results["checks"]["null_transaction_id"]["status"] == "PASS"
    assert results["checks"]["invalid_quantity"]["status"] == "PASS"
    assert results["checks"]["future_date_validation"]["status"] == "PASS"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def test_dq_gate_fail_on_corrupt_data(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price_inr", DoubleType(), True),
        StructField("total_amount_inr", DoubleType(), True),
        StructField("net_amount_inr", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("cashier_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("category", StringType(), True),
        StructField("parsed_date", DateType(), True),
    ])
    corrupt_data = [
        (
            None, "STORE_001", "Indiranagar",
            "2099-01-01", "CUST_1001", "SKU_001",
            "Basmati Rice", -3, -50.0,
            150.0, 150.0, 250.0,
            "UnauthorizedMethod", "CASH_01", "South",
            "Bengaluru", "Grocery", date(2099, 1, 1)
        )
    ]
    df = spark.createDataFrame(corrupt_data, schema=schema)
    results = run_pyspark_dq_suite(df)

    assert results["overall_status"] == "FAIL"
    assert results["failed_check_count"] > 0
    assert "null_transaction_id" in results["failed_checks"]
    assert "invalid_quantity" in results["failed_checks"]
    assert "invalid_unit_price" in results["failed_checks"]
    assert "future_date_validation" in results["failed_checks"]
    assert "invalid_discount_pct" in results["failed_checks"]
    assert "invalid_payment_method" in results["failed_checks"]
