# RetailCo Enterprise POS Data Platform — Architecture Specification

## 1. High-Level Architectural Flow

```
                      RETAIL STORE CHECKOUT TERMINALS (POS)
                                        |
       +--------------------------------+--------------------------------+
       | (Batch: Daily Store EOD CSVs)                                   | (Streaming: Live Line-Item Events)
       v                                                                 v
+-----------------------------+                           +-----------------------------+
| Google Cloud Storage (GCS)  |                           | Google Cloud Pub/Sub        |
| gs://.../raw/pos_*.csv      |                           | topic: retailco-transactions|
+-----------------------------+                           +-----------------------------+
       |                                                                 |
       v                                                                 v
+-----------------------------+                           +-----------------------------+
| Cloud Composer (Airflow)    |                           | Cloud Dataflow (Beam)       |
| - Pre-flight JSON Contract  |                           | - PII Tokenization (SHA-256)|
| - SLA & Orchestration Gates |                           | - Live Financial Net Calc   |
+-----------------------------+                           +-----------------------------+
       |                                                                 |
       v                                                                 |
+-----------------------------+                                          |
| Dataproc Serverless PySpark |                                          |
| - ANSI-safe Date Parsing    |                                          |
| - Broadcast Dim Join        |                                          |
| - 2-Phase Salting Skew Mit. |                                          |
+-----------------------------+                                          |
       |                                                                 |
       +------------+------------+                                       |
       |                         |                                       |
  [DQ Passed]               [DQ Failed]                                  |
       |                         |                                       |
       v                         v                                       |
+-----------------------+ +-----------------------+                      |
| GCS Silver Parquet    | | GCS Quarantine Sink   |                      |
| (Snappy, Partitioned) | | (Corrupted Payloads)  |                      |
+-----------------------+ +-----------------------+                      |
       |                                                                 |
       v                                                                 |
+------------------------------------------------------------------------+
| Google BigQuery Analytical Lakehouse                                   |
| - Raw Ingest / Staging Landing Tables                                  |
| - dbt Medallion Dimensional Marts (Star Schema)                         |
|     * dim_store, dim_product, dim_customer, dim_date                   |
|     * fct_sales (Partitioned by date, Clustered by store_id, SKU)       |
|     * mart_daily_store_sales, mart_category_performance                |
+------------------------------------------------------------------------+
                               |
                               v
               EXECUTIVE BI & ANALYTICAL DASHBOARDS
               (Looker, PowerBI, Tableau, FinOps)
```

---

## 2. Medallion & Star Schema Design

### Grain Definition
**One record per product line-item within an individual customer checkout transaction.**

### Dimensional Model (Star Schema):
- **Fact Table: `fct_sales`**
  - **Surrogate Primary Key:** `sales_key = MD5(CONCAT(transaction_id, '-', product_code))`
  - **Foreign Keys:** `store_id`, `product_code`, `customer_id`, `parsed_date`
  - **Degenerate Dimensions:** `transaction_id`, `payment_method`, `cashier_id`
  - **Measures:** `quantity`, `unit_price_inr`, `discount_pct`, `gross_amount_inr`, `discount_amount_inr`, `net_amount_inr`
  - **Physical Layout:** Partitioned by `parsed_date` (DAY), Clustered by `[store_id, product_code]`.
  - **Materialization:** Incremental merge with 7-day lookback watermark to ensure zero duplicate rows.

- **Dimension Tables:**
  - `dim_store`: Store code, human-readable name, geographical region, city, operating tier.
  - `dim_product`: SKU code, product description, category hierarchy, MSRP.
  - `dim_customer`: Masked customer token (`SHA-256`), lifetime order counts, privacy compliant.
  - `dim_date`: Calendar date spine, fiscal quarter, day-of-week, weekend flags.

- **Analytical Marts:**
  - `mart_daily_store_sales`: Aggregated daily store performance, total gross/net sales, transaction count, average basket size.
  - `mart_category_performance`: Product category revenue, unit volume, discount leakage percentages.

---

## 3. Data Governance & Privacy Architecture (GDPR / DPDP)
- **Role-Based Tokenization:** Apache Beam and dbt enforce deterministic SHA-256 hashing on customer identities (`customer_id`, `customer_email`).
- **PCI-DSS Compliance:** Credit card numbers are masked preserving only the last 4 digits (`************1234`).
- **Access Isolation:** Restricted analytical service accounts and BI users are granted access exclusively to transformed marts in BigQuery; access to raw unmasked staging tables is strictly denied.
