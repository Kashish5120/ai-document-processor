<#
.SYNOPSIS
Starts the Azure Functions app locally with optional remote settings fetch.

.PARAMETER SkipSettings
Skip fetching remote settings from Azure (use existing local.settings.json).

.PARAMETER SkipVenv
Skip virtual environment setup (assume it already exists and has dependencies).

.EXAMPLE
.\startLocal.ps1
# Full startup: fetch settings, setup venv, start function

.EXAMPLE
.\startLocal.ps1 -SkipSettings -SkipVenv
# Quick restart: use existing settings and venv
#>

param(
    [switch]$SkipSettings,
    [switch]$SkipVenv
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve script and repo directories
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$pipelineDir = Join-Path $repoRoot "pipeline"

Set-Location $pipelineDir

# Fetch remote settings unless -SkipSettings is passed
if (-not $SkipSettings) {
    Write-Host "Fetching remote settings..." -ForegroundColor Cyan

    # Load azd environment values
    azd env get-values | ForEach-Object {
        if ($_ -match '^(?<key>[^=]+)=(?<val>.*)$') {
            $k = $matches.key.Trim()
            $v = $matches.val

            # Remove outer quotes if present
            if ($v.Length -ge 2 -and $v.StartsWith('"') -and $v.EndsWith('"')) {
                $v = $v.Substring(1, $v.Length - 2)
                $v = $v -replace '\\"', '"'
            }

            [Environment]::SetEnvironmentVariable($k, $v)
            Set-Variable -Name $k -Value $v -Scope Script -Force
        }
    }

    # Validate required environment variables
    if (-not $env:PROCESSING_FUNCTION_APP_NAME) { throw "PROCESSING_FUNCTION_APP_NAME not set." }
    if (-not $env:APP_CONFIG_NAME) { throw "APP_CONFIG_NAME not set." }
    if (-not $env:AZURE_STORAGE_ACCOUNT) { throw "AZURE_STORAGE_ACCOUNT not set." }
    if (-not $env:RESOURCE_GROUP) { throw "RESOURCE_GROUP not set." }

    # Fetch app settings from Azure
    func azure functionapp fetch-app-settings $env:PROCESSING_FUNCTION_APP_NAME --decrypt
    func settings decrypt

    # Get App Configuration connection string
    $connString = az appconfig credential list `
        --name $env:APP_CONFIG_NAME `
        --query "[?name=='Primary'].connectionString" `
        -o tsv

    if (-not $connString) { throw "Failed to retrieve App Configuration connection string." }

    # Get Storage account connection strings
    $blobFuncConnString = az storage account show-connection-string `
        --name $env:AZURE_STORAGE_ACCOUNT `
        --resource-group $env:RESOURCE_GROUP `
        --query connectionString `
        -o tsv

    if (-not $blobFuncConnString) { throw "Failed to retrieve storage account connection string." }

    $blobDataStorageConnString = az storage account show-connection-string `
        --name $env:AZURE_STORAGE_ACCOUNT `
        --resource-group $env:RESOURCE_GROUP `
        --query connectionString `
        -o tsv

    if (-not $blobDataStorageConnString) { throw "Failed to retrieve data storage connection string." }

    # Update local.settings.json
    $localSettingsPath = "local.settings.json"
    if (-not (Test-Path $localSettingsPath)) { throw "File not found: $localSettingsPath" }

    $json = Get-Content $localSettingsPath -Raw | ConvertFrom-Json -AsHashtable

    if (-not $json.ContainsKey('Values') -or -not $json['Values']) {
        $json['Values'] = @{}
    }

    $json['Values']['AZURE_APPCONFIG_CONNECTION_STRING'] = $connString
    $json['Values']['AzureWebJobsStorage'] = $blobFuncConnString
    $json['Values']['DataStorage'] = $blobDataStorageConnString

    $json | ConvertTo-Json -Depth 10 | Set-Content $localSettingsPath -Encoding UTF8

    Write-Host "Updated local.settings.json" -ForegroundColor Green
}
else {
    Write-Host "Skipping settings fetch (-SkipSettings)" -ForegroundColor Yellow
}

# Set up virtual environment unless -SkipVenv is passed
if (-not $SkipVenv) {
    Write-Host "Setting up Python virtual environment..." -ForegroundColor Cyan

    if (-not (Test-Path .venv)) {
        Write-Host "Creating virtual environment..."
        python -m venv .venv
    }

    # Activate venv
    $activate = Join-Path .venv "Scripts\Activate.ps1"
    if (-not (Test-Path $activate)) {
        throw "Activation script not found at $activate"
    }
    & $activate

    # Install dependencies
    pip install -r requirements.txt
}
else {
    Write-Host "Skipping venv setup (-SkipVenv)" -ForegroundColor Yellow

    # Still need to activate existing venv
    $activate = Join-Path .venv "Scripts\Activate.ps1"
    if (Test-Path $activate) {
        & $activate
    }
}

Write-Host "Starting Azure Functions..." -ForegroundColor Cyan
func start --build
