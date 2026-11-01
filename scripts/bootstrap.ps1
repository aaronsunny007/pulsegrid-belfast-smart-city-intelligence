<#
.SYNOPSIS
    Sets up PulseCity SmartGrid on Azure from nothing, end to end. Safe to re-run.

.DESCRIPTION
    1. Creates the resource group and deploys infra/main.bicep (ADLS Gen2, Databricks Premium,
       Access Connector, role assignment, budget alert).
    2. Logs the Databricks CLI in to the new workspace (opens a browser once).
    3. Creates the Unity Catalog storage credential and external location.
    4. Stores the TfL API key in a Databricks secret scope (optional).
    5. Deploys the Asset Bundle, runs the setup job, then runs the collector once as a check.

    Needs: Azure CLI (az), Databricks CLI (databricks) and Python 3.10+ on PATH.

.EXAMPLE
    .\scripts\bootstrap.ps1 -Email you@example.com -TflAppKey <key>
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Email,
    [string]$TflAppKey = "",
    [string]$ResourceGroup = "rg-pulsecity",
    [string]$Location = "uksouth",
    [ValidateSet("dev", "prod")][string]$Target = "prod",
    [int]$MonthlyBudget = 30,
    [string]$DatabricksProfile = "pulsecity"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

function Step([string]$Text) { Write-Host "`n==> $Text" -ForegroundColor Cyan }

# Runs a native command and stops the script if it fails. Plain functions (no param block) so
# that arguments such as -c or --name are passed through to the program untouched.
function Run {
    $exe, $rest = $args
    $rest = @($rest) # a single remaining argument would otherwise be splatted character by character
    & $exe @rest
    if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $($args -join ' ')" }
}

# Runs a native command quietly and returns $true if it succeeded. Windows PowerShell 5.1 turns
# redirected stderr into a terminating error under "Stop", so relax that just for this call.
function Try-Run {
    $exe, $rest = $args
    $rest = @($rest)
    $saved = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { & $exe @rest *> $null } catch { }
    $ErrorActionPreference = $saved
    return ($LASTEXITCODE -eq 0)
}

# Windows PowerShell mangles quotes in JSON passed to native programs, so pass it as a file.
function Json-File([hashtable]$Body) {
    $path = [System.IO.Path]::GetTempFileName()
    $Body | ConvertTo-Json -Depth 5 | Set-Content -Path $path -Encoding ascii
    return $path
}

# --- 0. Prerequisites --------------------------------------------------------------------------
Step "Checking tools"
foreach ($tool in @("az", "databricks", "python")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool is not on PATH. See docs/azure_setup.md, section 'Fast path'."
    }
}
Run python -c "import sys; assert sys.version_info >= (3, 10), sys.version"
Run python -m pip --version

# --- 1. Azure resources ------------------------------------------------------------------------
Step "Signing in to Azure"
if (-not (Try-Run az account show)) { Run az login }
$sub = az account show --query "{name:name, id:id}" -o json | ConvertFrom-Json
Write-Host "Subscription: $($sub.name) ($($sub.id))"
Run az provider register --namespace Microsoft.Databricks
Run az provider register --namespace Microsoft.Storage

Step "Creating resource group $ResourceGroup in $Location"
Run az group create --name $ResourceGroup --location $Location --tags project=pulsecity-smartgrid -o none

# A budget's start date cannot move, so only create it the first time.
if (Try-Run az consumption budget show --budget-name budget-pulsecity --resource-group $ResourceGroup) {
    $MonthlyBudget = 0
}

Step "Deploying infra/main.bicep (Databricks workspace takes 3-5 minutes)"
$outJson = az deployment group create `
    --resource-group $ResourceGroup `
    --name pulsecity-infra `
    --template-file infra/main.bicep `
    --parameters alertEmail=$Email monthlyBudget=$MonthlyBudget `
    --query properties.outputs -o json
if ($LASTEXITCODE -ne 0) { throw "Bicep deployment failed. Details: az deployment group show -g $ResourceGroup -n pulsecity-infra" }
$out = $outJson | ConvertFrom-Json
$WorkspaceUrl = $out.workspaceUrl.value
$ConnectorId = $out.accessConnectorId.value
$StorageRoot = $out.storageRoot.value
Write-Host "Workspace:    $WorkspaceUrl"
Write-Host "Storage root: $StorageRoot"

# --- 2. Databricks CLI login -------------------------------------------------------------------
Step "Logging the Databricks CLI in (a browser window opens the first time)"
$env:DATABRICKS_CONFIG_PROFILE = $DatabricksProfile
if (-not (Try-Run databricks current-user me)) {
    Run databricks auth login --host $WorkspaceUrl --profile $DatabricksProfile
}
$me = databricks current-user me -o json | ConvertFrom-Json
Write-Host "Databricks user: $($me.userName)"

# --- 3. Unity Catalog storage ------------------------------------------------------------------
Step "Unity Catalog: storage credential pulsecity_mi"
if (-not (Try-Run databricks storage-credentials get pulsecity_mi)) {
    $f = Json-File @{
        name                   = "pulsecity_mi"
        comment                = "PulseCity SmartGrid access connector"
        azure_managed_identity = @{ access_connector_id = $ConnectorId }
    }
    Run databricks storage-credentials create --json "@$f"
}

Step "Unity Catalog: external location pulsecity_lakehouse"
if (-not (Try-Run databricks external-locations get pulsecity_lakehouse)) {
    $f = Json-File @{
        name            = "pulsecity_lakehouse"
        url             = $StorageRoot
        credential_name = "pulsecity_mi"
        comment         = "PulseCity SmartGrid lakehouse on ADLS Gen2"
    }
    # A new role assignment can take a few minutes to reach the storage account.
    $created = $false
    for ($i = 1; $i -le 10 -and -not $created; $i++) {
        $created = Try-Run databricks external-locations create --json "@$f"
        if (-not $created) {
            Write-Host "  storage permission not active yet, retrying in 30s ($i/10)"
            Start-Sleep -Seconds 30
        }
    }
    if (-not $created) { Run databricks external-locations create --json "@$f" }
}

# --- 4. TfL API key ----------------------------------------------------------------------------
Step "Secret scope pulsecity"
if (-not (Try-Run databricks secrets list-secrets pulsecity)) {
    Run databricks secrets create-scope pulsecity
}
if ($TflAppKey) {
    Run databricks secrets put-secret pulsecity tfl_app_key --string-value $TflAppKey
    Write-Host "TfL key stored."
} else {
    Write-Host "No -TflAppKey given: the collector will call TfL without a key (lower rate limit, still fine)."
}

# --- 5. Deploy and start -----------------------------------------------------------------------
Step "Deploying the Asset Bundle (target $Target)"
$env:BUNDLE_VAR_notification_email = $Email
$env:BUNDLE_VAR_storage_root = $StorageRoot
Run databricks bundle validate -t $Target
Run databricks bundle deploy -t $Target

Step "Running the setup job (catalog, schemas, volumes)"
Run databricks bundle run -t $Target pulsecity_setup

Step "Running the collector once as a smoke test"
Run databricks bundle run -t $Target pulsecity_collector

$catalog = if ($Target -eq "prod") { "pulsecity" } else { "pulsecity_dev" }
Write-Host ""
Write-Host "Done. PulseCity is live." -ForegroundColor Green
Write-Host "  Workspace:     $WorkspaceUrl"
Write-Host "  Landing files: Catalog > $catalog > landing > raw"
if ($Target -eq "prod") {
    Write-Host "  The collector now polls every 5 minutes; the pipeline runs hourly at :07."
} else {
    Write-Host "  dev schedules are paused. Run: databricks bundle run -t dev pulsecity_pipeline"
}
Write-Host "  Check cost after a day with the query in docs/azure_setup.md, section 6."
