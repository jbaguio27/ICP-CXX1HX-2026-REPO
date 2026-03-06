# ICP-CXX1HX-2026-REPO

AWS-only data engineering project that processes JODI-Oil CSV files in Amazon S3 into Silver and Gold Parquet datasets, validates them with Athena, and runs a data quality report through EMR Serverless and Step Functions.

<img width="2491" height="1165" alt="image" src="https://github.com/user-attachments/assets/a5eabb25-aff7-4872-91da-a01643bfa39a" />

## What this Repo Contains?

- Weekly project deliverables from planning and environment setup through orchestration and final documentation (`Week1/` to `Week6/`).
- Three PySpark jobs for Silver transformation, Gold transformation, and data quality reporting (`Week3/code/`, `Week4/code/`, `Week5/code/`).
- SQL assets for Athena table creation, partition repair, and validation queries (`Week4/docs/athena_ddl.sql`, `Week4/docs/athena_queries.sql`).
- Terraform and Amazon States Language files for deploying Step Functions orchestration (`infra/terraform/`, `infra/statemachine.asl.json`).
- PowerShell runbooks and UI screenshot guides for setup, S3 upload, EMR Serverless runs, and report verification (`Week*/docs/`, `scripts/aws_prereqs_check.ps1`).

## End to End Process

1. Prepare a local Windows PowerShell environment, create `.env` from `.env.example`, and validate AWS access (`Week1/environment_setup.md`, `scripts/aws_prereqs_check.ps1`).
2. Upload raw JODI-Oil CSV files for years 2021 to 2025 into the S3 raw prefix defined by `RAW_URI` (`Week2/docs/s3_upload.md`).
3. Run `Week3/code/spark_to_silver.py` on EMR Serverless to normalize column names, add ingestion metadata, derive `year` and `month`, and write partitioned Silver Parquet (`Week3/code/spark_to_silver.py`).
4. Run `Week4/code/spark_to_gold.py` on EMR Serverless to build four curated Gold datasets from Silver Parquet (`Week4/code/spark_to_gold.py`).
5. Register or repair Athena tables and run validation SQL for the Silver and Gold outputs (`Week4/docs/athena_ddl.sql`, `Week4/docs/athena_queries.sql`).
6. Run `Week5/code/data_quality_spark.py` to check Silver and Gold outputs and write a JSON report to the S3 reports prefix (`Week5/code/data_quality_spark.py`, `Week5/docs/data_quality_checks.md`).
7. Optionally deploy and run the Step Functions state machine so Silver, Gold, Athena validation, and data quality execute as one workflow (`infra/terraform/main.tf`, `infra/statemachine.asl.json`, `Week6/project_documentation.md`).

## Architecture

- Storage is Amazon S3 with separate prefixes for `raw`, `silver`, `gold`, `reports`, and Athena query results (`.env.example`, `config.yaml`, `Week1/architecture.md`, `infra/terraform/main.tf`).
- Compute is Amazon EMR Serverless Spark for the Silver, Gold, and data quality jobs (`Week3/docs/emr_serverless_submit.md`, `Week4/docs/emr_serverless_submit.md`, `infra/statemachine.asl.json`).
- Metadata and SQL validation use AWS Glue Data Catalog and Amazon Athena (`Week1/project_selection.md`, `Week4/docs/athena_ddl.sql`, `Week4/docs/athena_queries.sql`).
- Orchestration uses AWS Step Functions with synchronous EMR Serverless and Athena service integrations (`infra/statemachine.asl.json`).
- Deployment in this repo covers the Step Functions state machine, its CloudWatch log group, and an IAM support policy on an existing Step Functions role (`infra/terraform/main.tf`).
- Existing AWS prerequisites are still required outside Terraform: an S3 bucket, an Athena workgroup, an EMR Serverless application, and IAM roles (`Week6/project_documentation.md`, `infra/terraform/variables.tf`).

## Tech Stack

- Python 3.12+ for local development and job entrypoints.
  Where in repo: `Week1/environment_setup.md`, `Week3/code/spark_to_silver.py`, `Week4/code/spark_to_gold.py`, `Week5/code/data_quality_spark.py`
- PySpark for Silver, Gold, and data quality processing.
  Where in repo: `Week3/code/spark_to_silver.py`, `Week4/code/spark_to_gold.py`, `Week5/code/data_quality_spark.py`
- Amazon S3 for raw, Silver, Gold, Athena results, EMR logs, and reports.
  Where in repo: `.env.example`, `config.yaml`, `Week1/architecture.md`, `infra/terraform/main.tf`
- Amazon EMR Serverless for Spark job execution.
  Where in repo: `Week1/project_selection.md`, `Week3/docs/emr_serverless_submit.md`, `Week4/docs/emr_serverless_submit.md`, `infra/statemachine.asl.json`
- AWS Glue Data Catalog and Amazon Athena for external tables and validation SQL.
  Where in repo: `Week1/project_selection.md`, `Week4/docs/athena_ddl.sql`, `Week4/docs/athena_queries.sql`, `infra/statemachine.asl.json`
- AWS Step Functions for orchestration.
  Where in repo: `infra/statemachine.asl.json`, `infra/terraform/main.tf`, `Week6/project_documentation.md`
- Terraform for orchestration infrastructure as code.
  Where in repo: `infra/terraform/main.tf`, `infra/terraform/variables.tf`, `infra/terraform/outputs.tf`, `infra/terraform/terraform.tfvars.example`
- PowerShell and AWS CLI for local setup and AWS operations.
  Where in repo: `Week1/environment_setup.md`, `Week2/docs/s3_upload.md`, `Week3/docs/emr_serverless_submit.md`, `Week4/docs/emr_serverless_submit.md`, `scripts/aws_prereqs_check.ps1`
- `uv` for the documented local virtual environment bootstrap.
  Where in repo: `Week1/environment_setup.md`

## Data Sources

- External source files are JODI-Oil CSV files for years 2021 to 2025.
  Where in repo: `Week1/project_selection.md`, `Week2/README.md`, `Week2/docs/s3_upload.md`
- Raw landing zone is the S3 prefix defined by `RAW_URI`, with the documented pattern `s3a://<bucket>/jodi-oil/raw/`.
  Where in repo: `.env.example`, `config.yaml`, `Week2/docs/s3_upload.md`
- Download URL, publisher URL, and acquisition method for the original JODI-Oil files are `TBD`.
  Evidence needed: a downloader script, a source URL in docs, or committed sample raw files.
- Exact raw CSV column schema is `TBD`.
  Evidence needed: a sample raw CSV file in the repo or an upstream schema document.

## Data Model

- Raw dataset: CSV files stored under the S3 raw prefix.
  Schema is `TBD`; the repo does not contain sample raw files.
- Silver dataset: `silver_jodi_oil`.
  Athena analytics schema in repo: `country`, `area`, `product`, `flow`, `unit`, `value`, `source_file_name`, `ingested_at_utc`, partitioned by `year` and `month` (`Week4/docs/athena_ddl.sql`).
- Silver implementation detail: the Spark job preserves normalized raw columns in addition to the metadata columns above, then derives `year` and `month` from source columns or the file name (`Week3/code/spark_to_silver.py`).
- Gold dataset: `gold_monthly_global_production`.
  Columns: `metric_name`, `total_production`, partitioned by `year`, `month` (`Week4/docs/athena_ddl.sql`, `Week4/code/spark_to_gold.py`).
- Gold dataset: `gold_country_production_trend`.
  Columns: `country`, `production_value`, partitioned by `year`, `month` (`Week4/docs/athena_ddl.sql`, `Week4/code/spark_to_gold.py`).
- Gold dataset: `gold_top_producers_by_month`.
  Columns: `country`, `production_value`, `producer_rank`, partitioned by `year`, `month` (`Week4/docs/athena_ddl.sql`, `Week4/code/spark_to_gold.py`).
- Gold dataset: `gold_trade_balance_by_country`.
  Columns: `country`, `imports_value`, `exports_value`, `trade_balance_value`, partitioned by `year`, `month` (`Week4/docs/athena_ddl.sql`, `Week4/code/spark_to_gold.py`).
- Data quality report dataset: JSON payload written under `.../reports/data_quality_report/run_date=YYYY-MM-DD/run_ts=YYYYMMDDTHHMMSSZ/`.
  Top-level keys in repo: `dataset_name`, `mode`, `generated_at_utc`, `input_paths`, `summary`, `checks` (`Week5/code/data_quality_spark.py`).

## Pipelines and Orchestration

- Pipeline A in repo is Raw to Silver to Gold (`Week1/project_selection.md`, `Week3/code/spark_to_silver.py`, `Week4/code/spark_to_gold.py`).
- Pipeline B in repo is Silver and Gold data quality checks to a JSON report in S3 (`Week1/project_selection.md`, `Week5/code/data_quality_spark.py`).
- Step Functions orchestration order is:
  `RunSilverJob` -> `RunGoldJob` -> Athena database and table creation -> Athena `MSCK REPAIR TABLE` steps -> Athena validation queries -> `RunDataQualityJob` (`infra/statemachine.asl.json`).
- The state machine uses `.sync` integrations, so Step Functions waits for EMR Serverless jobs and Athena queries to finish without Lambda glue code (`infra/statemachine.asl.json`, `Week6/project_documentation.md`).
- Terraform in this repo does not upload job scripts to S3 and does not create the EMR Serverless application or the S3 data bucket.
  Evidence in repo: `infra/terraform/main.tf` only creates the Step Functions state machine, CloudWatch log group, and IAM support policy.

## Local Setup

- The documented local setup is Windows PowerShell specific.
  Evidence in repo: `Week1/environment_setup.md`, `scripts/aws_prereqs_check.ps1`
- macOS and Linux setup instructions are `Not in repo yet`.
  Evidence needed: shell-specific setup docs or a dependency manifest such as `requirements.txt` or `pyproject.toml`.
- Documented prerequisites are Python 3.12+, `uv`, AWS CLI v2, Terraform CLI, and VS Code with the Python extension (`Week1/environment_setup.md`).
- Create the virtual environment and install the documented packages:

```powershell
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv pip install --python .venv\Scripts\python.exe pyspark pyyaml python-dotenv duckdb pandas
```

- Create `.env` from the committed template and keep real values local:

```powershell
Copy-Item .env.example .env -Force
```

```dotenv
AWS_PROFILE=<your_aws_sso_profile>
AWS_REGION=<your_aws_region>
DATASET_NAME=jodi_oil
RAW_URI=s3a://<your-bucket>/jodi-oil/raw/
SILVER_URI=s3a://<your-bucket>/jodi-oil/silver/
GOLD_URI=s3a://<your-bucket>/jodi-oil/gold/
GLUE_DATABASE=jodi_oil_db
ATHENA_OUTPUT_S3=s3://<your-bucket>/athena-results/
SPARK_ADAPTIVE_ENABLED=true
SPARK_SHUFFLE_PARTITIONS=200
SPARK_TARGET_FILES_PER_PARTITION=1
```

- Validate local AWS access:

```powershell
.\scripts\aws_prereqs_check.ps1
terraform version
```

- Dependency locking is `Not in repo yet`.
  Evidence needed: `requirements.txt`, `pyproject.toml`, `poetry.lock`, or similar.

## Runbook

### Commands to Run

#### How to Run Ingestion

- Upload raw CSV files into the S3 raw prefix documented by `RAW_URI`:

```powershell
$PROFILE = "<AWS_PROFILE>"
$REGION = "<AWS_REGION>"
$RAW_URI = "s3://<your-bucket>/jodi-oil/raw/"
$LOCAL_CSV_DIR = "C:\\path\\to\\jodi_csv"

aws s3 sync $LOCAL_CSV_DIR $RAW_URI --exclude "*" --include "*.csv" --profile $PROFILE --region $REGION
```

- Full raw upload guidance is documented in `Week2/docs/s3_upload.md`.
- To run the Silver ingestion job on EMR Serverless, use the full PowerShell submit flow in `Week3/docs/emr_serverless_submit.md`.
  That runbook uploads `Week3/code/spark_to_silver.py` and `config.yaml` to S3, then submits `aws emr-serverless start-job-run`.

#### How to Run Transforms

- To run the Gold transform on EMR Serverless, use `Week4/docs/emr_serverless_submit.md`.
  That runbook uploads `Week4/code/spark_to_gold.py` and `config.yaml` to S3, then submits `aws emr-serverless start-job-run`.
- To register tables and validate outputs, execute the SQL in `Week4/docs/athena_ddl.sql` and `Week4/docs/athena_queries.sql` in Athena workgroup `jodi-oil-wg`.
- To run the data quality report locally from the repo root:

```powershell
python Week5\code\data_quality_spark.py --config config.yaml
```

- To write the data quality report to an explicit reports path:

```powershell
python Week5\code\data_quality_spark.py --config config.yaml --report-uri s3a://<your-bucket>/jodi-oil/reports/
```

- To run the full orchestration after the prerequisites exist, deploy the state machine with Terraform:

```powershell
Copy-Item infra\terraform\terraform.tfvars.example infra\terraform\terraform.tfvars
aws sso login --profile <AWS_PROFILE>
cd infra\terraform
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
terraform output state_machine_arn
```

- Start a Step Functions execution with the deployed state machine ARN:

```powershell
$STATE_MACHINE_ARN = "<STATE_MACHINE_ARN>"
$PROFILE = "<AWS_PROFILE>"
$REGION = "<AWS_REGION>"
$EXEC_NAME = "jodi-oil-run-$(Get-Date -Format 'yyyyMMddHHmmss')"

aws stepfunctions start-execution `
  --state-machine-arn $STATE_MACHINE_ARN `
  --name $EXEC_NAME `
  --profile $PROFILE `
  --region $REGION
```

#### How to Run Tests

- Automated unit or integration tests are `Not in repo yet`.
  Evidence needed: a `tests/` directory, `pytest` configuration, or CI workflow files.
- The closest repo-backed validation path today is Athena validation SQL plus the Week 5 data quality job (`Week4/docs/athena_queries.sql`, `Week5/docs/data_quality_checks.md`).

#### How to Build Docs

- Documentation build tooling is `Not in repo yet`.
  Evidence needed: `mkdocs.yml`, Sphinx config, Docusaurus config, or another docs build manifest.

## Data Quality and Testing

- The repo includes a dedicated Spark-based data quality gate in `Week5/code/data_quality_spark.py`.
- Implemented checks are documented in `Week5/docs/data_quality_checks.md` and include:
  Silver readability and non-empty checks, partition column validation, required Gold table readability, Gold non-empty checks, rank validation, uniqueness checks, and trade balance formula validation.
- The data quality job writes a JSON report even when checks fail, then raises a runtime error if any check has status `FAIL` (`Week5/code/data_quality_spark.py`).
- Athena validation SQL is included for curated output verification and partition pruning evidence (`Week4/docs/athena_queries.sql`).
- Automated unit tests, CI checks, and code coverage reporting are `Not in repo yet`.
  Evidence needed: test files, CI workflows, or coverage configuration.

## Observability and Logging

- All three Spark jobs use Python `logging` with timestamped log messages (`Week3/code/spark_to_silver.py`, `Week4/code/spark_to_gold.py`, `Week5/code/data_quality_spark.py`).
- EMR Serverless job monitoring is configured to write logs to an S3 logs prefix (`Week3/docs/emr_serverless_submit.md`, `Week4/docs/emr_serverless_submit.md`, `infra/statemachine.asl.json`, `infra/terraform/main.tf`).
- Step Functions execution logs are enabled at level `ALL` in a CloudWatch log group created by Terraform (`infra/terraform/main.tf`, `infra/terraform/outputs.tf`).
- Step Functions X-Ray tracing is enabled in Terraform (`infra/terraform/main.tf`).

## Repository Structure

```text
.
|-- .env.example
|-- .gitignore
|-- config.yaml
|-- README.md
|-- infra/
|   |-- statemachine.asl.json
|   `-- terraform/
|       |-- main.tf
|       |-- outputs.tf
|       |-- terraform.tfvars.example
|       `-- variables.tf
|-- scripts/
|   `-- aws_prereqs_check.ps1
|-- Week1/
|   |-- README.md
|   |-- architecture.md
|   |-- environment_setup.md
|   |-- project_selection.md
|   `-- docs/
|       `-- ui_runbook/
|           |-- README.md
|           `-- images/
|-- Week2/
|   |-- README.md
|   `-- docs/
|       |-- s3_upload.md
|       `-- ui_runbook/
|           |-- README.md
|           `-- images/
|-- Week3/
|   |-- README.md
|   |-- code/
|   |   `-- spark_to_silver.py
|   `-- docs/
|       |-- emr_serverless_submit.md
|       |-- performance_notes.md
|       `-- ui_runbook/
|           |-- README.md
|           `-- images/
|-- Week4/
|   |-- README.md
|   |-- code/
|   |   `-- spark_to_gold.py
|   `-- docs/
|       |-- athena_ddl.sql
|       |-- athena_queries.sql
|       |-- emr_serverless_submit.md
|       `-- ui_runbook/
|           |-- README.md
|           `-- images/
|-- Week5/
|   |-- README.md
|   |-- code/
|   |   `-- data_quality_spark.py
|   `-- docs/
|       |-- data_quality_checks.md
|       `-- ui_runbook/
|           |-- README.md
|           `-- images/
`-- Week6/
    |-- README.md
    |-- project_documentation.md
    `-- architecture/
        `-- architecture-diagram.png
```

- Local-only artifacts can also appear in the workspace, including `.env`, `.venv/`, `infra/terraform/.terraform/`, `infra/terraform/terraform.tfvars`, and `infra/terraform/*.tfstate`, but `.gitignore` marks them as non-source files.

## Troubleshooting

- If a Spark job fails with an unresolved placeholder error, check `.env` and `config.yaml`.
  The job scripts explicitly fail when values like `${RAW_URI}` or `${GOLD_URI}` remain unresolved (`Week3/code/spark_to_silver.py`, `Week4/code/spark_to_gold.py`, `Week5/code/data_quality_spark.py`).
- If Step Functions deployment succeeds but executions fail immediately, confirm the required existing AWS resources are real and match `terraform.tfvars`.
  The repo documents the S3 bucket, Athena workgroup, EMR Serverless application, and IAM roles as prerequisites rather than Terraform-managed resources (`Week6/project_documentation.md`, `infra/terraform/variables.tf`).
- If the state machine cannot find `config.yaml`, check `config_s3_uri`.
  Terraform defaults to `s3://<bucket>/jodi-oil/jobs/shared/config.yaml`, while the Week 3 and Week 4 runbooks upload `config.yaml` into week-specific paths unless you override the variable (`infra/terraform/main.tf`, `infra/terraform/terraform.tfvars.example`, `Week3/docs/emr_serverless_submit.md`, `Week4/docs/emr_serverless_submit.md`).
- If the data quality job cannot derive a reports location, pass `--report-uri` or provide `REPORTS_URI`.
  The script only auto-derives the reports path when `raw_uri` contains `/raw/` or `gold_uri` contains `/gold/` (`Week5/code/data_quality_spark.py`).
- If you are looking for a packaged dependency manifest, it is `Not in repo yet`.
  Evidence needed: `requirements.txt`, `pyproject.toml`, or another dependency file.
- `Week6/README.md` references `Week6/architecture/README.md`, but that file is not present.
  Current evidence in repo: `Week6/architecture/architecture-diagram.png` exists; add the missing README or update the Week 6 document links.

## Roadmap

- Fill in the benchmark and resource `TODO` items in `Week3/docs/performance_notes.md`.
- Add source provenance for the JODI-Oil files, including a source URL or downloader script.
- Add a dependency manifest and non-Windows setup instructions.
- Add automated tests or CI validation beyond Athena SQL and the data quality Spark job.
- Decide whether to keep `Week6/architecture/architecture-diagram.png` as the canonical architecture asset or add the missing `Week6/architecture/README.md`.
- Add a root `docs/` area if this project is going to keep README image placeholders there.
- Add an explicit license file.

## Contributing

- Keep changes small and update the relevant week folder plus this root README when a pipeline contract, runbook, or infrastructure assumption changes.
- Do not commit `.env`, Terraform state, or account-specific values.
- When changing job inputs, outputs, or partitions, update `config.yaml`, the Athena SQL, and the affected runbooks together.

## License

TBD. No `LICENSE` file or package metadata is present in the repo root, so a license cannot be confirmed from the current repository contents.
