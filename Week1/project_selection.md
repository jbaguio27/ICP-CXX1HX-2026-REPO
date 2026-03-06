# Project Selection (AWS-Only)

## Objective
Build Project 7 and Project 8 in one repository using an AWS-only data path:
- Raw JODI-Oil CSV files (2021-2025) land in Amazon S3.
- Spark transformations run on EMR Serverless.
- Metadata is managed in AWS Glue Data Catalog.
- Query and validation run in Amazon Athena.
- Orchestration is handled by AWS Step Functions.

## Why This Design
- Keeps compute close to data in AWS.
- Uses managed services to reduce infrastructure overhead.
- Supports repeatable, auditable runs via Step Functions.
- Aligns with internship requirement: at least two completed pipelines with working code.

## Services In Scope
- Amazon S3: raw/silver/gold/report storage
- Amazon EMR Serverless (Spark): Silver and Gold jobs, data quality job
- AWS Glue Data Catalog: table metadata for Silver and Gold
- Amazon Athena: DDL, partition refresh/management, validation SQL
- AWS Step Functions: end-to-end orchestration
- AWS IAM: least-privilege runtime roles
- Amazon CloudWatch Logs: execution and troubleshooting logs
- Terraform: infrastructure as code from VS Code (`infra/terraform`)

## Pipelines To Deliver
1. Pipeline A: Raw -> Silver -> Gold
2. Pipeline B: Data quality checks -> JSON report to S3

## Success Criteria
- Raw CSV files are read from S3 and transformed to Silver Parquet partitioned by `year`, `month`.
- Gold tables are produced in S3 and queryable in Athena.
- Required Glue tables exist:
  - `silver_jodi_oil`
  - `gold_monthly_global_production`
  - `gold_country_production_trend`
  - `gold_top_producers_by_month`
  - `gold_trade_balance_by_country`
- Step Functions can run the full workflow from Silver to validation and DQ report.
- Athena validation queries prove outputs are correct and show partition pruning via `year` and `month` filters.
- Repository contains architecture documentation assets (Week6 architecture placeholder already prepared).

## Constraints
- AWS-only primary execution path (no local data_lake as main processing path).
- No long-lived AWS access keys in code or repo.
- Local development uses AWS SSO + `AWS_PROFILE`.
