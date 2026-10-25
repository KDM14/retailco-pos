"""
Tests for Data Contract Enforcement.
"""
import pytest
import pandas as pd
from contracts.schema_contract import validate_preflight_schema, get_required_columns

def test_contract_required_columns():
    cols = get_required_columns()
    assert isinstance(cols, list)
    assert len(cols) == 16
    assert "transaction_id" in cols
    assert "store_id" in cols
    assert "unit_price_inr" in cols

def test_validate_preflight_valid_data(tmp_path):
    data = {
        "transaction_id": ["TXN_01", "TXN_02"],
        "store_id": ["S1", "S1"],
        "store_name": ["Indiranagar", "Indiranagar"],
        "transaction_date": ["2024-01-15", "2024-01-15"],
        "customer_id": ["C1", "C2"],
        "product_code": ["P1", "P2"],
        "product_name": ["Rice", "Oil"],
        "quantity": [1, 2],
        "unit_price_inr": [100.0, 200.0],
        "total_amount_inr": [100.0, 400.0],
        "discount_pct": [0.0, 0.0],
        "payment_method": ["Cash", "UPI"],
        "cashier_id": ["CA1", "CA1"],
        "region": ["South", "South"],
        "city": ["Bengaluru", "Bengaluru"],
        "category": ["Grocery", "Grocery"]
    }
    df = pd.DataFrame(data)
    csv_file = tmp_path / "valid.csv"
    df.to_csv(csv_file, index=False)

    valid_df, bad_records = validate_preflight_schema(str(csv_file))
    assert len(valid_df) == 2
    assert len(bad_records) == 0

def test_validate_preflight_missing_columns_raises_error(tmp_path):
    # Missing required columns
    data = {"transaction_id": ["TXN_01"], "quantity": [1]}
    df = pd.DataFrame(data)
    csv_file = tmp_path / "missing_cols.csv"
    df.to_csv(csv_file, index=False)

    with pytest.raises(ValueError, match="Schema Contract Violation: Missing mandatory columns"):
        validate_preflight_schema(str(csv_file))

def test_validate_preflight_detects_bad_rows(tmp_path):
    data = {
        "transaction_id": ["TXN_01", "TXN_02", ""], # Row 2 has empty txn id
        "store_id": ["S1", "S1", "S1"],
        "store_name": ["Indiranagar", "Indiranagar", "Indiranagar"],
        "transaction_date": ["2024-01-15", "2024-01-15", "2024-01-15"],
        "customer_id": ["C1", "C2", "C3"],
        "product_code": ["P1", "P2", "P3"],
        "product_name": ["Rice", "Oil", "Tea"],
        "quantity": [1, -5, 2], # Row 1 has negative quantity
        "unit_price_inr": [100.0, 200.0, 50.0],
        "total_amount_inr": [100.0, 400.0, 100.0],
        "discount_pct": [0.0, 150.0, 0.0], # Row 1 has invalid discount
        "payment_method": ["Cash", "UPI", "GoldBar"], # Row 2 has invalid payment
        "cashier_id": ["CA1", "CA1", "CA1"],
        "region": ["South", "South", "South"],
        "city": ["Bengaluru", "Bengaluru", "Bengaluru"],
        "category": ["Grocery", "Grocery", "Beverages"]
    }
    df = pd.DataFrame(data)
    csv_file = tmp_path / "mixed.csv"
    df.to_csv(csv_file, index=False)

    valid_df, bad_records = validate_preflight_schema(str(csv_file))
    assert len(valid_df) == 1
    assert len(bad_records) == 2
