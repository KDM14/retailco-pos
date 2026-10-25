#!/usr/bin/env python3
"""
RetailCo Local End-to-End Pipeline Runner.
Runs the entire batch processing pipeline locally for testing and demonstration:
1. Contract Pre-Flight Assertion
2. Great Expectations Quality Verification Gate
3. PySpark Distributed Batch Transform (with 2-Phase Salting Skew Mitigation)
4. Partitioned Snappy Parquet Generation
"""
import os
import sys
import subprocess

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

def run_step(title: str, command: list):
    print("\n" + "=" * 70)
    print(f" STEP: {title}")
    print("=" * 70)
    res = subprocess.run(command, cwd=REPO_ROOT)
    if res.returncode != 0:
        print(f"❌ Step failed with exit code {res.returncode}")
        sys.exit(res.returncode)

def main():
    print("""
    ========================================================================
     RETAILCO ENTERPRISE POS DATA PLATFORM - LOCAL PIPELINE EXECUTION
    ========================================================================
    """)

    sample_csv = os.path.join(REPO_ROOT, "data", "raw", "retailco_raw_sample.csv")
    output_dir = os.path.join(REPO_ROOT, "data", "output", "local_run")

    # Step 1: Pre-flight Contract Validation
    run_step(
        "1. Schema Contract Pre-Flight Validation",
        [
            sys.executable, "-c",
            f"from contracts.schema_contract import validate_preflight_schema; "
            f"v, bad = validate_preflight_schema('{sample_csv}'); "
            f"print(f'✅ Schema Contract Passed! Valid: {{len(v)}}, Malformed: {{len(bad)}}')"
        ]
    )

    # Step 2: Great Expectations Automated Verification Gate
    run_step(
        "2. Great Expectations Data Quality Gate",
        [
            sys.executable,
            os.path.join(REPO_ROOT, "great_expectations", "run_ge_checkpoint.py"),
            "--input", sample_csv,
            "--quarantine", os.path.join(output_dir, "quarantined_corrupt_records.csv")
        ]
    )

    # Step 3: PySpark Batch Pipeline Execution with Salting
    run_step(
        "3. PySpark Distributed Transform & Salting Skew Mitigation",
        [
            sys.executable,
            os.path.join(REPO_ROOT, "spark", "retailco_spark_job.py"),
            "--input", sample_csv,
            "--output", output_dir,
            "--enable-salting",
            "--salts", "4"
        ]
    )

    print("\n" + "=" * 70)
    print(" 🎯 PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"Transformed Parquet outputs available in: {output_dir}")
    print("  -> Fact Transactions:  sales_transactions_parquet/parsed_date=YYYY-MM-DD/")
    print("  -> Store Performance:  store_performance/parsed_date=YYYY-MM-DD/")
    print("========================================================================\n")

if __name__ == "__main__":
    main()
