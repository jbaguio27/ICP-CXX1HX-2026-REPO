# Week 4 - Gold Layer + Athena Validation

This week builds curated Gold tables from Silver data and validates them with Athena.

## Deliverables
- `code/spark_to_gold.py`
- `docs/emr_serverless_submit.md` (CLI runbook)
- `docs/ui_runbook/README.md` (AWS Console UI runbook + screenshot guide)
- `docs/athena_ddl.sql`
- `docs/athena_queries.sql`

## Definition of Done
1. EMR Serverless Gold job status is `SUCCESS`.
2. Gold outputs exist in S3:
   - `s3://<bucket>/jodi-oil/gold/gold_monthly_global_production/`
   - `s3://<bucket>/jodi-oil/gold/gold_country_production_trend/`
   - `s3://<bucket>/jodi-oil/gold/gold_top_producers_by_month/`
   - `s3://<bucket>/jodi-oil/gold/gold_trade_balance_by_country/`
3. Partition folders `year=.../month=...` exist in each Gold table path.
4. Athena DDL executed successfully for Silver + all Gold tables.
5. Athena validation queries return rows for at least one partition (for example `year=2023` and `month=6`).
6. Partition pruning evidence captured:
   - `EXPLAIN` query with `WHERE year = ... AND month = ...`
   - Compare Athena "Data scanned" against unfiltered query.

## Where to check
1. EMR Serverless console:
   - Application: `jodi-oil-emr-serverless`
   - Job runs tab: check final state and logs.
2. S3 console:
   - Prefix: `jodi-oil/gold/`
3. Athena console:
   - Workgroup: `jodi-oil-wg`
   - Database: `jodi_oil_db`
   - Run DDL and validation SQL.

## What to run
Use either:
1. CLI runbook: `docs/emr_serverless_submit.md`
2. UI runbook: `docs/ui_runbook/README.md`

Then run:
1. `docs/athena_ddl.sql`
2. `docs/athena_queries.sql`

## Evidence to capture
1. EMR job run success screenshot.
2. S3 Gold partitions screenshot.
3. Athena query result screenshot.
4. Athena partition-pruning comparison screenshot (filtered vs unfiltered data scanned).
