# Architecture (Text Diagram)

## AWS-Only Flow
```text
JODI CSV Files (2021-2025)
        |
        v
S3 Raw (jodi-oil/raw)
        |
        v
Step Functions State Machine
  1) Start EMR Serverless Job: spark_to_silver
  2) Wait/Poll job status
  3) Start EMR Serverless Job: spark_to_gold
  4) Wait/Poll job status
  5) Run Athena DDL for Glue tables
  6) Run partition repair/management
  7) Run Athena validation queries
  8) Run EMR Serverless data quality job
        |
        +--> S3 Silver Parquet (partitioned by year, month)
        |
        +--> S3 Gold Parquet (partitioned where relevant)
        |
        +--> Glue Data Catalog tables
        |
        +--> Athena query results to S3 athena-results/
        |
        +--> S3 DQ report JSON (jodi-oil/reports)
```

## Core Components
- Storage: Amazon S3 (`raw`, `silver`, `gold`, `athena-results`, `reports`)
- Compute: Amazon EMR Serverless (Spark)
- Catalog: AWS Glue Data Catalog (`jodi_oil_db`)
- SQL/Validation: Amazon Athena (`jodi-oil-wg`)
- Orchestration: AWS Step Functions (deployed via Terraform)
- IaC: Terraform (`infra/terraform`)
- Access/Security: IAM roles + AWS SSO (`AWS_PROFILE=<your_aws_profile>`)
- Logging: CloudWatch log groups for EMR Serverless and Step Functions

## Data Contracts (Planned)
- Silver table: `silver_jodi_oil`
- Gold tables:
  - `gold_monthly_global_production`
  - `gold_country_production_trend`
  - `gold_top_producers_by_month`
  - `gold_trade_balance_by_country`

## Performance Defaults (Planned)
- `spark.sql.adaptive.enabled=true`
- `spark.sql.shuffle.partitions=200`
- Partition strategy: `year`, `month`
- File-size control using `repartition`/`coalesce` before write
