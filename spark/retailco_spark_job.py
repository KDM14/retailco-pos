#!/usr/bin/env python3
"""
RetailCo POS Enterprise Batch Processing Engine.
Designed for execution on Google Cloud Dataproc Serverless or local Spark clusters.

Pipeline Workflow:
1. Dynamic Ingestion from Cloud Storage (GCS) or Local Filesystem
2. ANSI-Safe Cleaning & Financial Reconciliation
3. Broadcast Join with Store & Product Dimensions
4. Optional 2-Phase Salting Skew Mitigation for High-Volume Stores
5. Automated PySpark Data Quality Halting Gate
6. Partitioned Snappy Parquet Output Writing
7. Dead-Letter / Quarantine Routing upon Assertion Failure
"""
import argparse
import sys
import json
import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

# Path handling for local imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.transforms import (
    clean_sales_dataframe,
    apply_broadcast_dimension_join,
    apply_salted_skew_mitigation,
    write_partitioned_parquet
)
from spark.dq_gates import run_pyspark_dq_suite

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RetailCoSparkJob")

def build_spark_session(app_name: str = "RetailCo_POS_Batch_Pipeline") -> SparkSession:
    """Instantiates optimized SparkSession with Dataproc / Parquet configurations."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024)) # 10MB threshold
        .getOrCreate()
    )

def main():
    parser = argparse.ArgumentParser(description="RetailCo Dataproc Serverless Batch Job")
    parser.add_argument("--input", required=True, help="Input CSV path (GCS gs:// or local file path)")
    parser.add_argument("--output", required=True, help="Output Parquet path (GCS gs:// or local directory)")
    parser.add_argument("--enable-salting", action="store_true", help="Enable 2-phase salting skew mitigation")
    parser.add_argument("--salts", type=int, default=4, help="Number of salt buckets for skew mitigation")
    parser.add_argument("--quarantine-path", default=None, help="Path to write quarantined records on DQ failure")
    parser.add_argument("--dq-report-path", default="/tmp/dq_report.json", help="Path to persist DQ JSON report")

    args = parser.parse_args()
    logger.info("Initializing RetailCo PySpark Job...")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output Target: {args.output}")
    logger.info(f"Salting Enabled: {args.enable_salting} (buckets={args.salts})")

    spark = build_spark_session()

    try:
        # Step 1: Ingest raw CSV batch
        logger.info(f"Ingesting raw batch dataset from: {args.input}")
        raw_df = spark.read.csv(args.input, header=True, inferSchema=True)
        raw_count = raw_df.count()
        logger.info(f"Successfully loaded {raw_count} raw POS transactions.")

        # Step 2: Clean and reconcile financials
        logger.info("Executing normalization and financial calculation rules...")
        cleaned_df = clean_sales_dataframe(raw_df)

        # Step 3: Broadcast Join with Dimension Metadata
        # In production on GCP Dataproc, this reads from BigQuery / dim tables
        dim_store_schema = StructType([
            StructField("store_id", StringType(), True),
            StructField("store_tier", StringType(), True),
            StructField("tax_jurisdiction", StringType(), True)
        ])
        dim_store_seed = [
            ("STORE_001", "Tier-1 Metro", "KA-BLR-01"),
            ("STORE_002", "Tier-2 Suburban", "MH-MUM-02"),
            ("STORE_003", "Tier-1 Metro", "DL-DEL-01"),
            ("STORE_999", "Flagship Megastore", "KA-BLR-01")
        ]
        dim_store_df = spark.createDataFrame(dim_store_seed, schema=dim_store_schema)
        enriched_df = apply_broadcast_dimension_join(cleaned_df, dim_store_df)

        # Step 4: Data Quality Gate Execution
        logger.info("Evaluating Data Quality Verification Suite...")
        dq_results = run_pyspark_dq_suite(enriched_df)
        logger.info(f"DQ Suite Completed. Overall Status: {dq_results['overall_status']}")

        # Persist DQ summary
        try:
            with open(args.dq_report_path, "w") as f:
                json.dump(dq_results, f, indent=2)
            logger.info(f"DQ Report written to: {args.dq_report_path}")
        except Exception as e:
            logger.warning(f"Could not write DQ report locally: {e}")

        # Gate Evaluation & Quarantine Handling
        if dq_results["overall_status"] != "PASS":
            logger.error("🚨 CRITICAL: Data Quality Gate FAILED!")
            logger.error(f"Violations detected: {dq_results['failed_checks']}")
            
            quarantine_target = args.quarantine_path or os.path.join(args.output, "quarantine")
            logger.warning(f"Quarantining corrupted batch to: {quarantine_target}")
            enriched_df.write.mode("overwrite").parquet(quarantine_target)
            logger.error("Pipeline aborted to prevent corrupted data from skewing reporting marts.")
            sys.exit(1)

        logger.info("✅ All Data Quality assertions PASSED successfully!")

        # Step 5: High-Performance Parquet Storage Generation
        transactions_out = os.path.join(args.output, "sales_transactions_parquet")
        logger.info(f"Writing Snappy-compressed date-partitioned fact Parquet to: {transactions_out}")
        write_partitioned_parquet(enriched_df, transactions_out, partition_col="parsed_date")

        # Step 6: Analytical Mart Aggregation (with Salting Skew Mitigation)
        if args.enable_salting:
            logger.info(f"Applying 2-Phase Salting Skew Mitigation with {args.salts} salt partitions...")
            aggregated_store_df = apply_salted_skew_mitigation(enriched_df, num_salts=args.salts)
        else:
            logger.info("Standard grouping aggregation...")
            aggregated_store_df = (
                enriched_df.groupBy("parsed_date", "store_id")
                .agg(
                    F.round(F.sum("total_amount"), 2).alias("total_sales_amount"),
                    F.count("*").alias("total_transaction_count"),
                    F.sum("quantity").alias("total_quantity_sold")
                )
            )

        store_perf_out = os.path.join(args.output, "store_performance")
        logger.info(f"Writing Store Performance mart to: {store_perf_out}")
        write_partitioned_parquet(aggregated_store_df, store_perf_out, partition_col="parsed_date")

        logger.info("🎯 RetailCo POS Batch Pipeline executed successfully to completion.")

    except Exception as e:
        logger.error(f"Fatal error during batch execution: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
