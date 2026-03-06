$ErrorActionPreference = "Stop"

function Get-DotEnvValue {
  param(
    [string]$Path,
    [string]$Key
  )

  if (-not (Test-Path $Path)) {
    return $null
  }

  foreach ($line in Get-Content $Path) {
    if ($line -match '^\s*$' -or $line -match '^\s*#') {
      continue
    }

    $parts = $line.Split('=', 2)
    if ($parts.Count -ne 2) {
      continue
    }

    if ($parts[0].Trim() -eq $Key) {
      return $parts[1].Trim()
    }
  }

  return $null
}

$dotenvPath = Join-Path (Get-Location) ".env"
$profile = $env:AWS_PROFILE
if ([string]::IsNullOrWhiteSpace($profile)) {
  $profile = Get-DotEnvValue -Path $dotenvPath -Key "AWS_PROFILE"
}

$region = $env:AWS_REGION
if ([string]::IsNullOrWhiteSpace($region)) {
  $region = Get-DotEnvValue -Path $dotenvPath -Key "AWS_REGION"
}
if ([string]::IsNullOrWhiteSpace($region)) {
  $region = aws configure get region
}

$awsArgs = @()
if (-not [string]::IsNullOrWhiteSpace($profile)) {
  $awsArgs += @("--profile", $profile)
}
if (-not [string]::IsNullOrWhiteSpace($region)) {
  $awsArgs += @("--region", $region)
}

Write-Host "AWS CLI version:"
aws --version

Write-Host "`nAWS profile:"
if ([string]::IsNullOrWhiteSpace($profile)) {
  Write-Host "(not set)"
} else {
  Write-Host $profile
}

Write-Host "`nSTS caller identity:"
aws sts get-caller-identity @awsArgs

Write-Host "`nCurrent region:"
if ([string]::IsNullOrWhiteSpace($region)) {
  Write-Host "(not set)"
} else {
  Write-Host $region
}
