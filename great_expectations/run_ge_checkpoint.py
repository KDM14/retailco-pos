#!/usr/bin/env python3
"""
Great Expectations Automated Data Quality Verification Gate & Quarantine Engine.
Loads POS transaction batches, evaluates the Expectation Suite, and halts execution on failure.
"""
import os
import sys
import json
import argparse
import pandas as pd
from datetime import datetime

# Set path
GE_DIR = os.path.dirname(os.path.abspath(__file__))
SUITE_PATH = os.path.join(GE_DIR, "expectations", "pos_transaction_suite.json")

def run_expectations_gate(input_csv: str, quarantine_csv: str = None) -> dict:
    """
    Executes automated expectations verification gate against input CSV dataset.
    Returns structured results dictionary.
    """
    print(f"[{datetime.now().isoformat()}] Loading dataset for Great Expectations gate: {input_csv}")
    df = pd.read_csv(input_csv)
    total_records = len(df)

    with open(SUITE_PATH, "r") as f:
        suite = json.load(f)

    results = []
    failed_indices = set()
    all_passed = True

    # Convert numeric columns
    df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price_num"] = pd.to_numeric(df["unit_price_inr"], errors="coerce")
    df["discount_num"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    for exp in suite["expectations"]:
        exp_type = exp["expectation_type"]
        kwargs = exp["kwargs"]
        meta = exp.get("meta", {})
        severity = meta.get("severity", "MEDIUM")

        success = True
        failing_count = 0

        if exp_type == "expect_table_column_count_to_equal":
            expected_count = kwargs["value"]
            actual_count = len(df.columns) - 3 # excluding helper cols
            success = (actual_count == expected_count)
            failing_count = 0 if success else abs(actual_count - expected_count)

        elif exp_type == "expect_column_values_to_not_be_null":
            col = kwargs["column"]
            bad_mask = df[col].isnull() | (df[col].astype(str).str.strip() == "")
            failing_count = int(bad_mask.sum())
            success = (failing_count == 0)
            if not success:
                failed_indices.update(df[bad_mask].index.tolist())

        elif exp_type == "expect_compound_columns_to_be_unique":
            cols = kwargs["column_list"]
            dup_mask = df.duplicated(subset=cols, keep=False)
            failing_count = int(dup_mask.sum())
            success = (failing_count == 0)
            if not success:
                failed_indices.update(df[dup_mask].index.tolist())

        elif exp_type == "expect_column_values_to_be_between":
            col = kwargs["column"]
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            target_col = f"{col}_num" if f"{col}_num" in df.columns else col
            
            bad_mask = (df[target_col] < min_val) | (df[target_col] > max_val) | df[target_col].isnull()
            failing_count = int(bad_mask.sum())
            success = (failing_count == 0)
            if not success:
                failed_indices.update(df[bad_mask].index.tolist())

        elif exp_type == "expect_column_values_to_be_in_set":
            col = kwargs["column"]
            allowed = kwargs["value_set"]
            bad_mask = ~df[col].isin(allowed)
            failing_count = int(bad_mask.sum())
            success = (failing_count == 0)
            if not success:
                failed_indices.update(df[bad_mask].index.tolist())

        if not success and severity in ["CRITICAL", "HIGH"]:
            all_passed = False

        results.append({
            "expectation_type": exp_type,
            "success": success,
            "severity": severity,
            "failing_record_count": failing_count,
            "description": meta.get("description", "")
        })

    # Summary
    print("\n" + "=" * 70)
    print(" GREAT EXPECTATIONS AUTOMATED VERIFICATION RESULTS")
    print("=" * 70)
    print(f"Total Records Evaluated: {total_records}")
    print(f"Total Expectations Run:  {len(results)}")

    for r in results:
        status_icon = "✅ PASS" if r["success"] else "🚨 FAIL"
        print(f"[{status_icon}] ({r['severity']}) {r['expectation_type']}: {r['description']} (Failing: {r['failing_record_count']})")

    # Quarantine handling
    if failed_indices and quarantine_csv:
        quarantine_df = df.loc[list(failed_indices)].drop(columns=["quantity_num", "unit_price_num", "discount_num"], errors="ignore")
        os.makedirs(os.path.dirname(os.path.abspath(quarantine_csv)), exist_ok=True)
        quarantine_df.to_csv(quarantine_csv, index=False)
        print(f"\n⚠️ Quarantined {len(quarantine_df)} corrupted rows to: {quarantine_csv}")

    overall_result = {
        "overall_status": "PASS" if all_passed else "FAIL",
        "total_records": total_records,
        "failing_records_count": len(failed_indices),
        "results": results
    }

    print("=" * 70)
    if all_passed:
        print("✅ QUALITY GATE PASSED: All critical expectations met. Safe to load.")
    else:
        print("🚨 QUALITY GATE FAILED: Pipeline halted to prevent corrupted metrics!")
    print("=" * 70 + "\n")

    return overall_result

def main():
    parser = argparse.ArgumentParser(description="Run Great Expectations Checkpoint Gate")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--quarantine", default="data/output/quarantine.csv", help="Quarantine output path")
    parser.add_argument("--halt-on-failure", action="store_true", default=True, help="Exit with non-zero status on critical failure")
    args = parser.parse_args()

    report = run_expectations_gate(args.input, args.quarantine)

    if report["overall_status"] != "PASS" and args.halt_on_failure:
        sys.exit(1)

if __name__ == "__main__":
    main()
