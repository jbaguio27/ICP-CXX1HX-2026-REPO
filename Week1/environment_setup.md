# Environment Setup (VS Code + uv + AWS)

## Goal
Prepare a local developer environment in VS Code that controls AWS services without storing secrets in code.

## Prerequisites
- Windows PowerShell
- Python 3.12+
- `uv` installed
- AWS CLI v2 installed
- Terraform CLI installed
- VS Code with Python extension

## Repository Root
Run all commands from:
`C:\Users\iosep\ICP-CXX1HX-2026-REPO`

## 1) Create and activate virtual environment
```powershell
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv pip install --python .venv\Scripts\python.exe pyspark pyyaml python-dotenv duckdb pandas
```

## 2) Configure AWS SSO profile
Use AWS CLI full path if `aws` is not in PATH.

```powershell
$aws1 = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
if (-not (Test-Path $aws1)) { $aws1 = "$env:LOCALAPPDATA\Programs\Amazon\AWSCLIV2\aws.exe" }

$PROFILE = "<AWS_PROFILE>"
$REGION = "<AWS_REGION>"

& $aws1 configure sso --profile $PROFILE
& $aws1 sso login --profile $PROFILE
& $aws1 configure set region $REGION --profile $PROFILE
& $aws1 configure set output json --profile $PROFILE
& $aws1 sts get-caller-identity --profile $PROFILE
```

## 3) Create local `.env` from template (not committed)
`.env` is the runtime source on your machine.

```powershell
Copy-Item .env.example .env -Force
```

Use placeholders or your local values in `.env`:

```dotenv
AWS_PROFILE=<your_profile>
AWS_REGION=<your_region>
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

## 4) Validate prerequisites
```powershell
.\scripts\aws_prereqs_check.ps1
terraform version
```

Expected:
- AWS CLI version prints
- `sts get-caller-identity` returns account and role
- configured region prints correctly
- Terraform version prints

## Notes
- Do not store secrets in `.env`.
- Do not commit `.env`.
- Keep real account-specific values in local `.env`, not in committed docs.
- Use IAM roles in AWS for EMR Serverless and Step Functions runtime permissions.
