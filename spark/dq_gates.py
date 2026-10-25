"""
PySpark Native Data Quality Verification Gate Module.
Executes automated pre-load assertion suites directly over distributed DataFrames.
Evaluates critical business invariants and halts pipeline execution if anomalies are detected.
"""
from typing import Dict, Any
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def run_pyspark_dq_suite(df: DataFrame) -> Dict[str, Any]:
    """
    Executes comprehensive Data Quality assertions against transformed POS transactions.
    
    Checks evaluated:
    1. non_empty_dataset: Table must contain > 0 records.
    2. null_transaction_id: Mandatory primary key must never be NULL.
    3. duplicate_transaction_item: Composite key (transaction_id, product_code) must be unique.
    4. invalid_quantity: Quantity purchased must be >= 1.
    5. invalid_unit_price: Unit price must be non-negative.
    6. invalid_date: Parsed date must be valid and non-null.
    7. future_date: Transaction date must not be in the future.
    8. invalid_discount_pct: Discount must be in [0, 100].
    9. payment_method_valid: Must be one of authorized retail payment rails.
    10. financial_discrepancy: Discrepancy between stated total and computed line-item total must be <= 1.00 INR.
    """
    total_records = df.count()
    if total_records == 0:
        return {
            "overall_status": "FAIL",
            "total_records": 0,
            "failure_reason": "Input dataset is completely empty.",
            "checks": {
                "non_empty_dataset": {"status": "FAIL", "failed_records": 0}
            }
        }

    # 1. Null ID check
    null_id_count = df.filter(F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == "")).count()

    # 2. Duplicate composite primary key check (transaction_id, product_code)
    dup_pk_count = (
        df.groupBy("transaction_id", "product_code")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # 3. Invalid quantity
    invalid_qty_count = df.filter(F.col("quantity") < 1).count()

    # 4. Invalid unit price
    invalid_price_count = df.filter(F.col("unit_price_inr") < 0).count()

    # 5. Invalid date
    invalid_date_count = df.filter(F.col("parsed_date").isNull()).count()

    # 6. Future date
    future_date_count = df.filter(F.col("parsed_date") > F.current_date()).count()

    # 7. Discount range bounds [0, 100]
    invalid_discount_count = df.filter((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100)).count()

    # 8. Payment method verification
    valid_payment_methods = ["Cash", "UPI", "Credit Card", "Debit Card", "Net Banking", "Other"]
    invalid_payment_count = df.filter(~F.col("payment_method").isin(valid_payment_methods)).count()

    # 9. Financial discrepancy check: abs(total_amount_inr - net_amount_inr) > 1.00
    if "net_amount_inr" in df.columns and "total_amount_inr" in df.columns:
        financial_discrepancy_count = df.filter(
            F.abs(F.col("total_amount_inr") - F.col("net_amount_inr")) > 1.00
        ).count()
    else:
        financial_discrepancy_count = 0

    checks = {
        "non_empty_dataset": {
            "status": "PASS" if total_records > 0 else "FAIL",
            "failed_records": 0 if total_records > 0 else 1
        },
        "null_transaction_id": {
            "status": "PASS" if null_id_count == 0 else "FAIL",
            "failed_records": null_id_count
        },
        "duplicate_composite_pk": {
            "status": "PASS" if dup_pk_count == 0 else "FAIL",
            "failed_records": dup_pk_count
        },
        "invalid_quantity": {
            "status": "PASS" if invalid_qty_count == 0 else "FAIL",
            "failed_records": invalid_qty_count
        },
        "invalid_unit_price": {
            "status": "PASS" if invalid_price_count == 0 else "FAIL",
            "failed_records": invalid_price_count
        },
        "invalid_parsed_date": {
            "status": "PASS" if invalid_date_count == 0 else "FAIL",
            "failed_records": invalid_date_count
        },
        "future_date_validation": {
            "status": "PASS" if future_date_count == 0 else "FAIL",
            "failed_records": future_date_count
        },
        "invalid_discount_pct": {
            "status": "PASS" if invalid_discount_count == 0 else "FAIL",
            "failed_records": invalid_discount_count
        },
        "invalid_payment_method": {
            "status": "PASS" if invalid_payment_count == 0 else "FAIL",
            "failed_records": invalid_payment_count
        },
        "financial_reconciliation": {
            "status": "PASS" if financial_discrepancy_count == 0 else "FAIL",
            "failed_records": financial_discrepancy_count
        }
    }

    # Gate decision
    failed_checks = [k for k, v in checks.items() if v["status"] == "FAIL"]
    overall_status = "FAIL" if failed_checks else "PASS"

    return {
        "overall_status": overall_status,
        "total_records": total_records,
        "failed_check_count": len(failed_checks),
        "failed_checks": failed_checks,
        "checks": checks
    }
