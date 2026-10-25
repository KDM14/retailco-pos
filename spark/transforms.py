"""
Distributed PySpark Transformation Engine for RetailCo POS Pipeline.
Pure DataFrame-in, DataFrame-out functions implementing enterprise optimization patterns:
- Multi-format resilient date parsing under Spark ANSI mode
- Broadcast Hash Join optimization for low-cardinality dimension lookup
- Two-Phase Salting Key mitigation for skewed retail store transaction volumes
- Snappy-compressed column-partitioned Parquet output generation
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

def clean_sales_dataframe(df: DataFrame) -> DataFrame:
    """
    Normalizes raw POS sales records into structured analytical format.
    
    Transforms:
    - Parses dates conforming to either 'yyyy-MM-dd' or 'dd-MM-yyyy'
    - Calculates standardized total amounts
    - Computes gross, discount, and net financial metrics
    - Standardizes categorical casing
    """
    return (
        df.withColumn(
            "parsed_date",
            F.when(F.col("transaction_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
             .when(F.col("transaction_date").rlike(r"^\d{2}-\d{2}-\d{4}$"), F.to_date(F.col("transaction_date"), "dd-MM-yyyy"))
             .otherwise(F.lit(None).cast("date"))
        )
        .withColumn(
            "quantity",
            F.coalesce(F.col("quantity").cast(IntegerType()), F.lit(0))
        )
        .withColumn(
            "unit_price_inr",
            F.coalesce(F.col("unit_price_inr").cast(DoubleType()), F.lit(0.0))
        )
        .withColumn(
            "discount_pct",
            F.coalesce(F.col("discount_pct").cast(DoubleType()), F.lit(0.0))
        )
        .withColumn(
            "gross_amount_inr",
            F.round(F.col("quantity") * F.col("unit_price_inr"), 2)
        )
        .withColumn(
            "discount_amount_inr",
            F.round(F.col("gross_amount_inr") * (F.col("discount_pct") / 100.0), 2)
        )
        .withColumn(
            "net_amount_inr",
            F.round(F.col("gross_amount_inr") - F.col("discount_amount_inr"), 2)
        )
        .withColumn(
            "total_amount",
            F.coalesce(F.col("total_amount_inr").cast(DoubleType()), F.col("net_amount_inr"))
        )
        .withColumn("store_id", F.trim(F.upper(F.col("store_id"))))
        .withColumn("product_code", F.trim(F.upper(F.col("product_code"))))
    )

def apply_broadcast_dimension_join(fact_df: DataFrame, dim_store_df: DataFrame) -> DataFrame:
    """
    Joins large sales fact dataset with small store dimension using Broadcast Hash Join.
    
    FinOps / Performance Benefit:
    Avoids expensive full-cluster shuffle exchanges (SortMergeJoin) across cluster nodes
    by broadcasting the small dimension table directly to executor working memory.
    """
    return fact_df.join(F.broadcast(dim_store_df), on="store_id", how="left")

def apply_salted_skew_mitigation(fact_df: DataFrame, num_salts: int = 4) -> DataFrame:
    """
    Mitigates severe data skew (e.g. flagship stores generating >50% of transaction volume)
    using a Two-Phase Salted Aggregation pattern.
    
    Technical Mechanics:
    - Phase 1 (Scatter): Appends pseudo-random salt integer [0, num_salts-1] to distribute
      hot partition keys uniformly across all available Spark worker tasks.
      Computes partial aggregates per (parsed_date, store_id, salt).
    - Phase 2 (Gather): Strips salt and computes final global aggregates per (parsed_date, store_id).
    
    Eliminates executor task stragglers and prevents disk spill during shuffle stages.
    """
    # Phase 1: Append pseudo-random salt key
    salted_df = fact_df.withColumn("salt_key", (F.rand(seed=42) * num_salts).cast(IntegerType()))

    partial_agg = (
        salted_df.groupBy("parsed_date", "store_id", "salt_key")
        .agg(
            F.sum("total_amount").alias("partial_sales_amount"),
            F.count("*").alias("partial_transaction_count"),
            F.sum("quantity").alias("partial_quantity_count")
        )
    )

    # Phase 2: Roll up partial results to true key grain
    final_agg = (
        partial_agg.groupBy("parsed_date", "store_id")
        .agg(
            F.round(F.sum("partial_sales_amount"), 2).alias("total_sales_amount"),
            F.sum("partial_transaction_count").alias("total_transaction_count"),
            F.sum("partial_quantity_count").alias("total_quantity_sold")
        )
    )

    return final_agg

def write_partitioned_parquet(df: DataFrame, output_path: str, partition_col: str = "parsed_date") -> None:
    """
    Writes DataFrame to Cloud Storage (GCS) or local path as Snappy-compressed Parquet.
    Partitioned by date column to enable downstream query engine partition pruning.
    """
    df.write \
        .mode("overwrite") \
        .partitionBy(partition_col) \
        .option("compression", "snappy") \
        .parquet(output_path)
