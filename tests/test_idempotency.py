"""
Tests verifying pipeline idempotency (zero duplicated rows upon identical re-executions).
"""
import pytest
from datetime import date
from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def test_idempotent_deduplication(spark):
    """
    Simulates reprocessing an identical batch of transactions.
    Proves that deduplicating by composite key (transaction_id, product_code)
    guarantees idempotent warehouse state.
    """
    batch_run_1 = [
        Row(transaction_id="TXN_100", product_code="SKU_A", quantity=2, net_amount=200.0, parsed_date=date(2024, 1, 15)),
        Row(transaction_id="TXN_101", product_code="SKU_B", quantity=1, net_amount=150.0, parsed_date=date(2024, 1, 15))
    ]
    # Re-run arrives with identical keys but potentially updated values or retransmitted exact duplicate
    batch_run_2 = [
        Row(transaction_id="TXN_100", product_code="SKU_A", quantity=2, net_amount=200.0, parsed_date=date(2024, 1, 15)),
        Row(transaction_id="TXN_101", product_code="SKU_B", quantity=1, net_amount=150.0, parsed_date=date(2024, 1, 15)),
        Row(transaction_id="TXN_102", product_code="SKU_C", quantity=3, net_amount=300.0, parsed_date=date(2024, 1, 15))
    ]

    combined_df = spark.createDataFrame(batch_run_1 + batch_run_2)
    assert combined_df.count() == 5 # Raw combined contains duplicates

    # Idempotent deduplication logic matching dbt/Spark UPSERT grain
    deduped_df = (
        combined_df
        .withColumn("row_num", F.row_number().over(
            Window.partitionBy("transaction_id", "product_code").orderBy(F.col("parsed_date").desc())
        ))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    assert deduped_df.count() == 3 # Exactly 3 unique line items
    keys = [(r["transaction_id"], r["product_code"]) for r in deduped_df.collect()]
    assert set(keys) == {("TXN_100", "SKU_A"), ("TXN_101", "SKU_B"), ("TXN_102", "SKU_C")}
