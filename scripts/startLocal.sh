#!/usr/bin/env bash
set -euo pipefail

# Resolve directory of this script
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
SKIP_SETTINGS=false
SKIP_VENV=false
for arg in "$@"; do
  case $arg in
    --skip-settings) SKIP_SETTINGS=true ;;
    --skip-venv) SKIP_VENV=true ;;
  esac
done

cd "$REPO_ROOT/pipeline"

# Fetch remote settings unless --skip-settings is passed
if [ "$SKIP_SETTINGS" = false ]; then
  echo "Fetching remote settings..."

  eval $(azd env get-values)

  func azure functionapp fetch-app-settings $PROCESSING_FUNCTION_APP_NAME --decrypt
  func settings decrypt

  CONFIG_CONN_STRING=$(az appconfig credential list \
    --name "$APP_CONFIG_NAME" \
    --query "[?name=='Primary'].connectionString" \
    -o tsv)

  BLOB_FUNC_CONN_STRING=$(az storage account show-connection-string \
    --name $AZURE_STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query connectionString \
    -o tsv)

  BLOB_DATA_STORAGE_CONN_STRING=$(az storage account show-connection-string \
    --name $AZURE_STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query connectionString \
    -o tsv)

  jq \
    --arg CONFIG_CONN_STRING "$CONFIG_CONN_STRING" \
    --arg BLOB_FUNC_CONN_STRING "$BLOB_FUNC_CONN_STRING" \
    --arg BLOB_DATA_STORAGE_CONN_STRING "$BLOB_DATA_STORAGE_CONN_STRING" \
    '
    .Values.AZURE_APPCONFIG_CONNECTION_STRING = $CONFIG_CONN_STRING
    | .Values.AzureWebJobsStorage               = $BLOB_FUNC_CONN_STRING
    | .Values.DataStorage                       = $BLOB_DATA_STORAGE_CONN_STRING
    ' local.settings.json > local.settings.tmp && mv local.settings.tmp local.settings.json

  echo "Updated local.settings.json"
else
  echo "Skipping settings fetch (--skip-settings)"
fi

# Set up virtual environment unless --skip-venv is passed
if [ "$SKIP_VENV" = false ]; then
  echo "Setting up Python virtual environment..."
  python -m venv .venv
  source ./.venv/bin/activate
  pip install -r requirements.txt
else
  echo "Skipping venv setup (--skip-venv)"
  source ./.venv/bin/activate
fi

echo "Starting Azure Functions..."
func start --build
