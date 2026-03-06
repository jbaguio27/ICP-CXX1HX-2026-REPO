# Week 6 Project Documentation

This week connects Week3, Week4, and Week5 into one AWS Step Functions workflow.

## Scope
- Infrastructure as code with Terraform.
- Step Functions orchestration using service integrations.
- End-to-end run:
  1. Silver job (EMR Serverless)
  2. Gold job (EMR Serverless)
  3. Athena DDL table creation
  4. Athena partition repair (`MSCK REPAIR TABLE`)
  5. Athena validation queries
  6. Data quality job (EMR Serverless)

## Files Delivered
- `infra/statemachine.asl.json`
- `infra/terraform/main.tf`
- `infra/terraform/variables.tf`
- `infra/terraform/outputs.tf`
- `infra/terraform/terraform.tfvars.example`
- `Week6/architecture/README.md` (manual diagram placeholder)

## Prerequisites
- AWS CLI configured with SSO profile.
- Terraform installed.
- Existing AWS resources:
  - S3 data bucket and prefixes
  - Athena workgroup
  - Glue database
  - EMR Serverless application
  - IAM roles for EMR runtime and Step Functions
- Scripts uploaded to S3:
  - `s3://<bucket>/jodi-oil/jobs/week3/spark_to_silver.py`
  - `s3://<bucket>/jodi-oil/jobs/week4/spark_to_gold.py`
  - `s3://<bucket>/jodi-oil/jobs/week5/data_quality_spark.py`
  - `s3://<bucket>/jodi-oil/jobs/shared/config.yaml` if you use Terraform defaults
  - or an explicit override such as `s3://<bucket>/jodi-oil/jobs/week5/config.yaml`

## Deploy with Terraform
From repo root:

```powershell
Copy-Item infra\terraform\terraform.tfvars.example infra\terraform\terraform.tfvars
```

Edit `infra/terraform/terraform.tfvars` with your values.
If your uploaded `config.yaml` is not at the default shared path, set `config_s3_uri` explicitly.

Refresh AWS SSO session before Terraform:

```powershell
aws sso login --profile <AWS_PROFILE>
```

Then deploy:

```powershell
cd infra\terraform
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

## Start Orchestration
Get state machine ARN:

```powershell
cd infra\terraform
terraform output state_machine_arn
```

Start execution:

```powershell
$STATE_MACHINE_ARN = "<paste-terraform-output>"
$PROFILE = "<AWS_PROFILE>"
$REGION = "<AWS_REGION>"
$EXEC_NAME = "jodi-oil-run-$(Get-Date -Format 'yyyyMMddHHmmss')"

aws stepfunctions start-execution `
  --state-machine-arn $STATE_MACHINE_ARN `
  --name $EXEC_NAME `
  --profile $PROFILE `
  --region $REGION
```

## Where to Verify
1. Step Functions:
   - Open state machine execution graph.
   - Confirm all states reach `Succeeded`.
2. EMR Serverless:
   - Check Silver, Gold, and Data Quality jobs.
3. Athena:
   - Confirm queries completed in workgroup `jodi-oil-wg`.
4. S3:
   - Silver and Gold parquet partitions exist.
   - Data quality report exists under `jodi-oil/reports/data_quality_report/...`.

## Evidence Checklist (Screenshots)
1. Terraform apply output (state machine created).
2. Step Functions execution graph (all states success).
3. EMR Serverless job runs (Silver, Gold, Data Quality).
4. Athena query history showing DDL/repair/validation queries.
5. S3 reports output for latest data quality run.
6. Architecture diagram file in `Week6/architecture/` (added manually by you).

## Notes
- The state machine uses `.sync` integrations for EMR Serverless and Athena tasks.
- `.sync` waits for task completion, satisfying wait/poll requirements without Lambda.
- Re-running is idempotent for DDL due to `IF NOT EXISTS`; partition repair can be repeated safely.
