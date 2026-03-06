# Week 5 Data Quality Checks

This document defines the Week 5 quality gate for the AWS-only pipeline.

## Objective
- Validate Silver and Gold datasets in S3 after transformation.
- Publish a JSON report to S3 reports prefix.
- Fail the job when one or more quality checks fail.

## Script
- `Week5/code/data_quality_spark.py`
- UI execution guide: `Week5/docs/ui_runbook/README.md`

## Inputs
- `silver_uri` from `config.yaml`
- `gold_uri` from `config.yaml`

## Report Output
- Preferred: `REPORTS_URI` from `.env` or `--report-uri`
- If not provided, script derives from:
  - `raw_uri` by replacing `/raw/` with `/reports/`, or
  - `gold_uri` by replacing `/gold/` with `/reports/`

Output location pattern:
- `s3a://<bucket>/jodi-oil/reports/data_quality_report/run_date=YYYY-MM-DD/run_ts=YYYYMMDDTHHMMSSZ/`

## Implemented Checks
1. Silver parquet path is readable.
2. Silver dataset is non-empty.
3. Silver has partition columns `year`, `month`.
4. Silver `year`/`month` values are valid (`month` in `1..12` and non-null).
5. Silver has at least one distinct year-month partition.
6. Each required Gold table parquet path is readable:
   - `gold_monthly_global_production`
   - `gold_country_production_trend`
   - `gold_top_producers_by_month`
   - `gold_trade_balance_by_country`
7. Each Gold table is non-empty.
8. Each Gold table has partition columns `year`, `month`.
9. `gold_monthly_global_production` has unique `(metric_name, year, month)`.
10. `gold_top_producers_by_month` rank values are in `1..10`.
11. `gold_top_producers_by_month` has unique rank per `(year, month)`.
12. `gold_trade_balance_by_country` formula is consistent:
    - `trade_balance_value = exports_value - imports_value` (with nulls treated as zero).

## Execution
Run from repo root:

```powershell
python Week5\code\data_quality_spark.py --config config.yaml
```

Optional explicit report URI:

```powershell
python Week5\code\data_quality_spark.py --config config.yaml --report-uri s3a://<bucket>/jodi-oil/reports/
```

## EMR Serverless Notes
- Submit this script as a Spark job like Week 3/4.
- Include `--files s3://<bucket>/.../config.yaml` in Spark submit parameters.
- Pass `spark.emr-serverless.driverEnv.REPORTS_URI` when you want an explicit report path.
- Use UI runbook for screenshot sequence and pass/fail checks:
  - `Week5/docs/ui_runbook/README.md`

## Expected Behavior
- If all checks pass:
  - job succeeds
  - report is written to S3 reports path
- If any check fails:
  - report is still written
  - job ends in failed state with summary in error message
