# Week 5 - Data Quality and Reporting

Week 5 adds a data quality gate for Silver/Gold outputs and writes a JSON report to S3.

## Deliverables
- `code/data_quality_spark.py`
- `docs/data_quality_checks.md`
- `docs/ui_runbook/README.md`

## Definition of Done
1. EMR Serverless Week5 job state is `SUCCESS`.
2. Report JSON is written under:
   - `s3://<bucket>/jodi-oil/reports/data_quality_report/run_date=.../run_ts=.../`
3. Report summary shows `failed_checks = 0`.
4. Evidence screenshots are captured (job submit, job success, S3 reports output, report content).

## Where to check
1. EMR Serverless:
   - Application: `jodi-oil-emr-serverless`
   - Job runs tab -> Week5 run status and logs
2. S3:
   - Prefix: `jodi-oil/reports/data_quality_report/`
3. Report content:
   - Open generated `part-*.txt` and verify summary fields

## What to run
Use either:
1. CLI flow using `docs/data_quality_checks.md`
2. UI flow using `docs/ui_runbook/README.md`
