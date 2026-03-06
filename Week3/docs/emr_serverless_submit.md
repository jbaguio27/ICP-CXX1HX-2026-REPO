# EMR Serverless Submit (Silver Job)

This runbook submits `Week3/code/spark_to_silver.py` to EMR Serverless.

## Prerequisites
- `.env` exists at repo root with at least:
  - `AWS_PROFILE`
  - `AWS_REGION`
  - `RAW_URI`
  - `SILVER_URI`
  - `SPARK_ADAPTIVE_ENABLED`
  - `SPARK_SHUFFLE_PARTITIONS`
  - `SPARK_TARGET_FILES_PER_PARTITION`
- EMR Serverless application created:
  - `EMR_APP_ID=<YOUR_EMR_APPLICATION_ID>`
- EMR runtime role exists:
  - `arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<YOUR_EMR_RUNTIME_ROLE>`
- `Week3/code/spark_to_silver.py` uses only standard library + PySpark at runtime (no extra Python package upload required).

## 1) Parse `.env` and upload artifacts to S3
```powershell
$aws1 = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
if (-not (Test-Path $aws1)) { $aws1 = "$env:LOCALAPPDATA\Programs\Amazon\AWSCLIV2\aws.exe" }

$envMap = @{}
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*$' -or $_ -match '^\s*#') { return }
  $parts = $_.Split('=', 2)
  if ($parts.Count -eq 2) { $envMap[$parts[0].Trim()] = $parts[1].Trim() }
}

$required = @('AWS_PROFILE','AWS_REGION','RAW_URI','SILVER_URI','SPARK_ADAPTIVE_ENABLED','SPARK_SHUFFLE_PARTITIONS','SPARK_TARGET_FILES_PER_PARTITION')
foreach ($k in $required) {
  if (-not $envMap.ContainsKey($k) -or [string]::IsNullOrWhiteSpace($envMap[$k])) {
    throw "Missing required key in .env: $k"
  }
}

$PROFILE = $envMap['AWS_PROFILE']
$REGION = $envMap['AWS_REGION']
$RAW_URI_S3 = $envMap['RAW_URI'] -replace '^s3a://', 's3://'

if ($RAW_URI_S3 -notmatch '^s3://([^/]+)/') {
  throw "RAW_URI must look like s3a://<bucket>/..."
}
$BUCKET = $Matches[1]

$SCRIPT_S3 = "s3://$BUCKET/jodi-oil/jobs/week3/spark_to_silver.py"
$CONFIG_S3 = "s3://$BUCKET/jodi-oil/jobs/week3/config.yaml"

& $aws1 s3 cp Week3/code/spark_to_silver.py $SCRIPT_S3 --profile $PROFILE --region $REGION
& $aws1 s3 cp config.yaml $CONFIG_S3 --profile $PROFILE --region $REGION
```

## 2) Submit EMR Serverless job
```powershell
$EMR_APP_ID = "<YOUR_EMR_APPLICATION_ID>"
$EMR_ROLE_ARN = "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<YOUR_EMR_RUNTIME_ROLE>"

$sparkSubmitParameters = @(
  "--files $CONFIG_S3",
  "--conf spark.sql.adaptive.enabled=$($envMap['SPARK_ADAPTIVE_ENABLED'])",
  "--conf spark.sql.shuffle.partitions=$($envMap['SPARK_SHUFFLE_PARTITIONS'])",
  "--conf spark.emr-serverless.driverEnv.RAW_URI=$($envMap['RAW_URI'])",
  "--conf spark.emr-serverless.driverEnv.SILVER_URI=$($envMap['SILVER_URI'])",
  "--conf spark.emr-serverless.driverEnv.SPARK_ADAPTIVE_ENABLED=$($envMap['SPARK_ADAPTIVE_ENABLED'])",
  "--conf spark.emr-serverless.driverEnv.SPARK_SHUFFLE_PARTITIONS=$($envMap['SPARK_SHUFFLE_PARTITIONS'])",
  "--conf spark.emr-serverless.driverEnv.SPARK_TARGET_FILES_PER_PARTITION=$($envMap['SPARK_TARGET_FILES_PER_PARTITION'])"
) -join ' '

$jobDriver = @{
  sparkSubmit = @{
    entryPoint = $SCRIPT_S3
    entryPointArguments = @('--config','config.yaml')
    sparkSubmitParameters = $sparkSubmitParameters
  }
}

$configurationOverrides = @{
  monitoringConfiguration = @{
    s3MonitoringConfiguration = @{
      logUri = "s3://$BUCKET/jodi-oil/logs/"
    }
  }
}

$jobDriverPath = Join-Path $env:TEMP "jodi-week3-job-driver.json"
$configOverridesPath = Join-Path $env:TEMP "jodi-week3-config-overrides.json"

$jobDriver | ConvertTo-Json -Depth 20 | Set-Content -Path $jobDriverPath -Encoding ascii
$configurationOverrides | ConvertTo-Json -Depth 20 | Set-Content -Path $configOverridesPath -Encoding ascii

$jobRunId = & $aws1 emr-serverless start-job-run `
  --application-id $EMR_APP_ID `
  --execution-role-arn $EMR_ROLE_ARN `
  --job-driver "file://$jobDriverPath" `
  --configuration-overrides "file://$configOverridesPath" `
  --profile $PROFILE `
  --region $REGION `
  --query 'jobRunId' `
  --output text

Write-Host "JOB_RUN_ID=$jobRunId"
```

## 3) Poll status until terminal state
```powershell
$terminal = @('SUCCESS','FAILED','CANCELLED')

while ($true) {
  $state = & $aws1 emr-serverless get-job-run `
    --application-id $EMR_APP_ID `
    --job-run-id $jobRunId `
    --profile $PROFILE `
    --region $REGION `
    --query 'jobRun.state' `
    --output text

  Write-Host "state=$state"

  if ($terminal -contains $state) {
    break
  }

  Start-Sleep -Seconds 20
}

if ($state -ne 'SUCCESS') {
  throw "Silver job finished with state: $state"
}
```

## 4) Verify Silver output
```powershell
$SILVER_URI_S3 = $envMap['SILVER_URI'] -replace '^s3a://', 's3://'
& $aws1 s3 ls $SILVER_URI_S3 --recursive --profile $PROFILE --region $REGION
```
