-- ============================================================================
-- RetailCo POS FinOps Query Optimization & Cost Analysis
-- Benchmarking Columnar Pruning, Partitioning & Clustering in BigQuery
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. BASELINE UNOPTIMIZED QUERY (Full Table Scan / Anti-pattern)
-- ----------------------------------------------------------------------------
-- Scans all 16 columns across entire table.
-- Billed Bytes Processed: 11,759 bytes (Baseline test sample) -> Extrapolates to TBs at scale.
SELECT *
FROM `dhanush-week6-2026.retailco_analytical_marts.fct_sales`
WHERE city = 'Delhi';


-- ----------------------------------------------------------------------------
-- 2. FINOPS OPTIMIZED QUERY (Strict Columnar Projection Pruning)
-- ----------------------------------------------------------------------------
-- Scans only the 3 analytical fields required by the dashboard.
-- Billed Bytes Processed: 1,639 bytes
-- Impact: 86.06% reduction in scanned bytes billed!
SELECT 
    transaction_id, 
    store_id, 
    net_amount_inr
FROM `dhanush-week6-2026.retailco_analytical_marts.fct_sales`
WHERE city = 'Delhi';


-- ----------------------------------------------------------------------------
-- 3. PARTITION & CLUSTER PRUNED QUERY (Production Pattern)
-- ----------------------------------------------------------------------------
-- Prunes date partitions (parsed_date) and cluster blocks (store_id)
-- Evaluates only relevant date blocks, bypassing 95%+ of stored data.
SELECT
    store_id,
    parsed_date,
    SUM(quantity) AS total_units,
    SUM(net_amount_inr) AS total_revenue_inr
FROM `dhanush-week6-2026.retailco_analytical_marts.fct_sales`
WHERE parsed_date BETWEEN DATE('2024-01-15') AND DATE('2024-01-20')
  AND store_id IN ('STORE_001', 'STORE_002')
GROUP BY 1, 2;
