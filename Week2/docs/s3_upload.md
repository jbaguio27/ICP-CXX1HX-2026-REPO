# S3 Raw Upload Runbook

## Source of Target Path
Use `.env` key `RAW_URI` as the source of truth.

Example in `.env`:

```dotenv
RAW_URI=s3a://<your-bucket>/jodi-oil/raw/
```

For AWS CLI commands, convert `s3a://` to `s3://`.

## Prerequisites
- AWS CLI v2 installed
- SSO profile configured (`AWS_PROFILE` in `.env`)
- Region configured (`AWS_REGION` in `.env`)
- CSV files available for years 2021-2025

## Preferred Method (AWS Console UI)
1. Open your S3 bucket from `.env` `RAW_URI`.
2. Navigate to the `jodi-oil/raw/` prefix.
3. Upload all `.csv` files.
4. Confirm uploaded objects appear in the folder list.

## Optional Method (PowerShell + AWS CLI)
```powershell
$aws1 = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
if (-not (Test-Path $aws1)) { $aws1 = "$env:LOCALAPPDATA\Programs\Amazon\AWSCLIV2\aws.exe" }

$envMap = @{}
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*$' -or $_ -match '^\s*#') { return }
  $parts = $_.Split('=', 2)
  if ($parts.Count -eq 2) { $envMap[$parts[0].Trim()] = $parts[1].Trim() }
}

if (-not $envMap.ContainsKey('RAW_URI')) { throw "RAW_URI is missing in .env" }
if (-not $envMap.ContainsKey('AWS_PROFILE')) { throw "AWS_PROFILE is missing in .env" }
if (-not $envMap.ContainsKey('AWS_REGION')) { throw "AWS_REGION is missing in .env" }

$PROFILE = $envMap['AWS_PROFILE']
$REGION = $envMap['AWS_REGION']
$RAW_URI = $envMap['RAW_URI'] -replace '^s3a://', 's3://'
$LOCAL_CSV_DIR = "C:\path\to\jodi_csv"

& $aws1 s3 sync $LOCAL_CSV_DIR $RAW_URI --exclude "*" --include "*.csv" --profile $PROFILE --region $REGION
```

## Verify Upload (PowerShell)
```powershell
$aws1 = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
if (-not (Test-Path $aws1)) { $aws1 = "$env:LOCALAPPDATA\Programs\Amazon\AWSCLIV2\aws.exe" }

$envMap = @{}
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*$' -or $_ -match '^\s*#') { return }
  $parts = $_.Split('=', 2)
  if ($parts.Count -eq 2) { $envMap[$parts[0].Trim()] = $parts[1].Trim() }
}

$PROFILE = $envMap['AWS_PROFILE']
$REGION = $envMap['AWS_REGION']
$RAW_URI = $envMap['RAW_URI'] -replace '^s3a://', 's3://'

& $aws1 s3 ls $RAW_URI --profile $PROFILE --region $REGION
```

## Notes
- No manifest file is required for this project flow.
- Keep only raw CSV files in the raw prefix.
- Do not manually upload files to `silver/` or `gold/`.
