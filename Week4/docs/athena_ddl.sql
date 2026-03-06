-- Athena DDL for Silver + Gold tables
-- Replace bucket/path values if your environment differs.

CREATE DATABASE IF NOT EXISTS jodi_oil_db;

-- Silver table (subset schema for analytics use)
CREATE EXTERNAL TABLE IF NOT EXISTS jodi_oil_db.silver_jodi_oil (
  country string,
  area string,
  product string,
  flow string,
  unit string,
  value double,
  source_file_name string,
  ingested_at_utc timestamp
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/jodi-oil/silver/';

-- Gold 1: Monthly global production
CREATE EXTERNAL TABLE IF NOT EXISTS jodi_oil_db.gold_monthly_global_production (
  metric_name string,
  total_production double
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/jodi-oil/gold/gold_monthly_global_production/';

-- Gold 2: Country production trend
CREATE EXTERNAL TABLE IF NOT EXISTS jodi_oil_db.gold_country_production_trend (
  country string,
  production_value double
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/jodi-oil/gold/gold_country_production_trend/';

-- Gold 3: Top producers by month
CREATE EXTERNAL TABLE IF NOT EXISTS jodi_oil_db.gold_top_producers_by_month (
  country string,
  production_value double,
  producer_rank int
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/jodi-oil/gold/gold_top_producers_by_month/';

-- Gold 4: Trade balance by country
CREATE EXTERNAL TABLE IF NOT EXISTS jodi_oil_db.gold_trade_balance_by_country (
  country string,
  imports_value double,
  exports_value double,
  trade_balance_value double
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/jodi-oil/gold/gold_trade_balance_by_country/';

-- Partition discovery
MSCK REPAIR TABLE jodi_oil_db.silver_jodi_oil;
MSCK REPAIR TABLE jodi_oil_db.gold_monthly_global_production;
MSCK REPAIR TABLE jodi_oil_db.gold_country_production_trend;
MSCK REPAIR TABLE jodi_oil_db.gold_top_producers_by_month;
MSCK REPAIR TABLE jodi_oil_db.gold_trade_balance_by_country;
