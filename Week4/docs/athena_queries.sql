-- Validation queries for Gold tables
-- Use Athena workgroup: jodi-oil-wg

-- 1) Validate monthly global production for a specific partition
SELECT year, month, metric_name, total_production
FROM jodi_oil_db.gold_monthly_global_production
WHERE year = 2023 AND month = 6
ORDER BY year, month;

-- 2) Validate country production trend for a specific partition
SELECT year, month, country, production_value
FROM jodi_oil_db.gold_country_production_trend
WHERE year = 2023 AND month = 6
ORDER BY production_value DESC
LIMIT 50;

-- 3) Validate top producers by month for a specific partition
SELECT year, month, country, production_value, producer_rank
FROM jodi_oil_db.gold_top_producers_by_month
WHERE year = 2023 AND month = 6
ORDER BY producer_rank ASC;

-- 4) Validate trade balance by country for a specific partition
SELECT year, month, country, imports_value, exports_value, trade_balance_value
FROM jodi_oil_db.gold_trade_balance_by_country
WHERE year = 2023 AND month = 6
ORDER BY ABS(trade_balance_value) DESC
LIMIT 50;

-- 5) Partition pruning proof (EXPLAIN with filter)
EXPLAIN
SELECT SUM(total_production)
FROM jodi_oil_db.gold_monthly_global_production
WHERE year = 2023 AND month = 6;

-- 6) Partition pruning comparison (no filter)
EXPLAIN
SELECT SUM(total_production)
FROM jodi_oil_db.gold_monthly_global_production;

-- 7) Count checks per table
SELECT 'gold_monthly_global_production' AS table_name, COUNT(*) AS row_count
FROM jodi_oil_db.gold_monthly_global_production
UNION ALL
SELECT 'gold_country_production_trend' AS table_name, COUNT(*) AS row_count
FROM jodi_oil_db.gold_country_production_trend
UNION ALL
SELECT 'gold_top_producers_by_month' AS table_name, COUNT(*) AS row_count
FROM jodi_oil_db.gold_top_producers_by_month
UNION ALL
SELECT 'gold_trade_balance_by_country' AS table_name, COUNT(*) AS row_count
FROM jodi_oil_db.gold_trade_balance_by_country;

-- Note:
-- In Athena UI, compare "Data scanned" and runtime between filtered and unfiltered queries
-- to demonstrate partition pruning.
