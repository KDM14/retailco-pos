"""
Data Contract Enforcement Module for RetailCo POS Pipeline.
Validates raw POS ingestion payloads against producer-consumer JSON schema specifications.
Protects downstream distributed compute (Dataproc/PySpark) from malformed payloads.
"""
import os
import json
import pandas as pd
from typing import Tuple, List, Dict, Any

CONTRACT_PATH = os.path.join(os.path.dirname(__file__), "retailco_pos_transaction_v1.0.0.json")

def get_contract_definition() -> Dict[str, Any]:
    """Loads JSON schema contract definition."""
    if os.path.exists(CONTRACT_PATH):
        with open(CONTRACT_PATH, "r") as f:
            return json.load(f)
    return {
        "required": [
            "transaction_id", "store_id", "store_name", "transaction_date",
            "customer_id", "product_code", "product_name", "quantity",
            "unit_price_inr", "total_amount_inr", "discount_pct",
            "payment_method", "cashier_id", "region", "city", "category"
        ]
    }

def get_required_columns() -> List[str]:
    """Returns list of required columns defined in the schema contract."""
    contract = get_contract_definition()
    return contract.get("required", [])

def validate_preflight_schema(csv_path: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Executes lightweight pre-flight schema contract validation.
    
    Args:
        csv_path: Local file path or gs:// URI to raw CSV.
        
    Returns:
        Tuple of (valid_dataframe, list_of_rejected_record_dicts)
        
    Raises:
        ValueError: If mandatory schema columns are missing entirely from the CSV header.
    """
    if str(csv_path).startswith("gs://"):
        df = pd.read_csv(csv_path, storage_options={"token": "cloud"})
    else:
        df = pd.read_csv(csv_path)

    required_cols = get_required_columns()
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Schema Contract Violation: Missing mandatory columns {missing_cols}")

    # Numeric conversions with graceful coercion
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price_inr"] = pd.to_numeric(df["unit_price_inr"], errors="coerce")
    df["total_amount_inr"] = pd.to_numeric(df["total_amount_inr"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    valid_payment_methods = ["Cash", "UPI", "Credit Card", "Debit Card", "Net Banking", "Other"]

    # Invariant assertions:
    # 1. quantity >= 1
    # 2. unit_price_inr >= 0
    # 3. total_amount_inr >= 0
    # 4. discount_pct between 0 and 100
    # 5. non-null, non-empty transaction_id
    # 6. valid payment method enum
    valid_mask = (
        (df["quantity"] >= 1) &
        (df["unit_price_inr"] >= 0) &
        (df["total_amount_inr"] >= 0) &
        (df["discount_pct"] >= 0) & (df["discount_pct"] <= 100) &
        (df["transaction_id"].notnull()) &
        (df["transaction_id"].astype(str).str.strip().str.len() > 0) &
        (df["payment_method"].isin(valid_payment_methods))
    )

    valid_df = df[valid_mask].copy()
    invalid_records = df[~valid_mask].to_dict(orient="records")

    return valid_df, invalid_records
