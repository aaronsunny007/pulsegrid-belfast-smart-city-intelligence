<#
.SYNOPSIS
    Deploys PulseCity SmartGrid to Databricks Free Edition. Safe to re-run.

.DESCRIPTION
    Logs the Databricks CLI in, deploys the `free` bundle target and runs the setup job.
    The TfL collector runs on GitHub Actions (.github/workflows/collector.yml), because Free
    Edition has no outbound internet. See docs/free_edition.md.

.EXAMPLE
    .\scripts\bootstrap_free.ps1 -WorkspaceUrl https://dbc-xxxx.cloud.databricks.com -Email you@example.com
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WorkspaceUrl,
    [Parameter(Mandatory = $true)][string]$Email,
    [string]$DatabricksProfile = "pulsecity"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

function Run {
    $exe, $rest = $args
    $rest = @($rest)
    & $exe @rest
    if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $($args -join ' ')" }
}

$env:DATABRICKS_CONFIG_PROFILE = $DatabricksProfile
$env:BUNDLE_VAR_notification_email = $Email

# Windows PowerShell 5.1 turns redirected stderr into an error under "Stop", so relax it here.
$ErrorActionPreference = "Continue"
databricks current-user me *> $null
$loggedIn = ($LASTEXITCODE -eq 0)
$ErrorActionPreference = "Stop"
if (-not $loggedIn) { Run databricks auth login --host $WorkspaceUrl --profile $DatabricksProfile }

Run databricks bundle validate -t free
Run databricks bundle deploy -t free
Run databricks bundle run -t free pulsecity_setup

Write-Host "`nDeployed. Next: add the GitHub secrets and merge to main (docs/free_edition.md)." -ForegroundColor Green
