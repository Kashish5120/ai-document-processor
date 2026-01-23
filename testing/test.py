import requests
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# -------------------------
# CONFIG
# -------------------------

LANGUAGE_ENDPOINT = "https://language-humata-pii-redaction.cognitiveservices.azure.com/"
API_VERSION = "2024-11-15-preview"

STORAGE_ACCOUNT_URL = "https://st4bb2mocnzmeqedata.blob.core.windows.net"
SOURCE_CONTAINER = "documents"
SOURCE_BLOB_NAME = "bill2.pdf"
TARGET_CONTAINER = "results"

# Build blob URLs without SAS tokens - Language service will use its managed identity
SOURCE_BLOB_URL = f"{STORAGE_ACCOUNT_URL}/{SOURCE_CONTAINER}/{SOURCE_BLOB_NAME}"
TARGET_CONTAINER_URL = f"{STORAGE_ACCOUNT_URL}/{TARGET_CONTAINER}"

print(f"Using Managed Identity for storage access (no SAS tokens)")
print(f"Source Blob URL: {SOURCE_BLOB_URL}")
print(f"Target Container URL: {TARGET_CONTAINER_URL}\n")

# -------------------------
# AUTH USING MANAGED IDENTITY FOR LANGUAGE SERVICE
# -------------------------

credential = DefaultAzureCredential()

# Get token for Language Service
language_token = credential.get_token(
    "https://cognitiveservices.azure.com/.default"
)

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {language_token.token}"
}

# -------------------------
# REQUEST BODY
# -------------------------

payload = {
    "displayName": "MI Native PII Redaction",
    "analysisInput": {
        "documents": [
            {
                "language": "en-US",
                "id": "Output-1",
                "source": {"location": SOURCE_BLOB_URL},
                "target": {"location": TARGET_CONTAINER_URL},
            }
        ]
    },
    "tasks": [
        {
            "kind": "PiiEntityRecognition",
            "taskName": "Redact PII",
            "parameters": {
                "redactionPolicy": {"policyKind": "entityMask"},
                # "piiCategories": [ 
                #     "Person",
                #     "Organization"
                # ],
                "excludeExtractionData": True,
            },
        }
    ],
}

# -------------------------
# SUBMIT JOB
# -------------------------

submit_url = f"{LANGUAGE_ENDPOINT}/language/analyze-documents/jobs?api-version={API_VERSION}"

resp = requests.post(submit_url, headers=HEADERS, json=payload)

if resp.status_code != 202:
    print(f"Error Response Status: {resp.status_code}")
    print(f"Error Response Body: {resp.text}")

resp.raise_for_status()

operation_url = resp.headers["operation-location"]

print("Job started:", operation_url)

# -------------------------
# POLL STATUS
# -------------------------

while True:
    status_resp = requests.get(operation_url, headers=HEADERS)
    status_resp.raise_for_status()

    status = status_resp.json()["status"]
    print("Status:", status)

    if status in ["succeeded", "failed", "cancelled"]:
        result = status_resp.json()
        print(result)
        
        # Move redacted file to root of target container
        if status == "succeeded" and result.get("tasks", {}).get("items"):
            task_result = result["tasks"]["items"][0].get("results", {})
            documents = task_result.get("documents", [])
            
            if documents:
                targets = documents[0].get("targets", [])
                # Find the redacted PDF (not the .json file)
                redacted_pdf_url = None
                for target in targets:
                    location = target.get("location", "")
                    if not location.endswith(".json"):
                        redacted_pdf_url = location
                        break
                
                if redacted_pdf_url:
                    print(f"\nRedacted file created at: {redacted_pdf_url}")
                    
                    # Move file to root of container
                    blob_service_client = BlobServiceClient(
                        account_url=STORAGE_ACCOUNT_URL,
                        credential=credential
                    )
                    
                    # Extract the blob path from URL
                    blob_path = redacted_pdf_url.split(f"{STORAGE_ACCOUNT_URL}/{TARGET_CONTAINER}/")[1]
                    source_blob = blob_service_client.get_blob_client(TARGET_CONTAINER, blob_path)
                    
                    # New blob name at root
                    new_blob_name = SOURCE_BLOB_NAME.replace(".pdf", "_redacted.pdf")
                    target_blob = blob_service_client.get_blob_client(TARGET_CONTAINER, new_blob_name)
                    
                    # Copy to root
                    print(f"Moving to: {STORAGE_ACCOUNT_URL}/{TARGET_CONTAINER}/{new_blob_name}")
                    target_blob.start_copy_from_url(source_blob.url)
                    
                    # Wait for copy to complete
                    copy_status = target_blob.get_blob_properties().copy.status
                    while copy_status == "pending":
                        time.sleep(0.5)
                        copy_status = target_blob.get_blob_properties().copy.status
                    
                    print(f"File moved successfully to: {new_blob_name}")
                    
                    # Optionally delete the subfolder file
                    source_blob.delete_blob()
        
        break

    time.sleep(2)
  