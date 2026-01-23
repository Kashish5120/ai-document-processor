import azure.functions as func
import logging
import os
import time
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="analyze_document_blob")
@app.blob_trigger(
    arg_name="blob",
    path="bronze/{name}",
    connection="DataStorage"
)

async def analyze_document_blob(blob: func.InputStream):
    """
    Blob-triggered function that redacts PII from documents using Azure Language API with Managed Identity.
    Triggered when a new blob is uploaded to the 'bronze' container.
    Outputs redacted document to the root of the target container.
    """
    logging.info(f"Blob Trigger - Blob Received: {blob.name}")
    logging.info(f"Path: {blob.name}")
    logging.info(f"Size: {blob.length} bytes")
    logging.info(f"URI: {blob.uri}")

    try:
        # Get configuration from environment variables
        language_endpoint = os.environ.get('LANGUAGE_ENDPOINT')
        storage_account_url = os.environ.get('STORAGE_ACCOUNT_URL')
        source_container = os.environ.get('SOURCE_CONTAINER', 'bronze')
        target_container = os.environ.get('TARGET_CONTAINER', 'silver')
        
        if not language_endpoint:
            logging.error("LANGUAGE_ENDPOINT environment variable is required")
            return
        
        if not storage_account_url:
            logging.error("STORAGE_ACCOUNT_URL environment variable is required")
            return
        
        # Extract blob name from path (remove container prefix)
        blob_name = blob.name.split('/')[-1]
        
        # Build blob URLs - Language service will use its managed identity
        source_blob_url = f"{storage_account_url}/{source_container}/{blob_name}"
        target_container_url = f"{storage_account_url}/{target_container}"
        
        logging.info(f"Source Blob: {source_blob_url}")
        logging.info(f"Target Container: {target_container_url}")
        
        # -------------------------
        # AUTH USING MANAGED IDENTITY
        # -------------------------
        credential = DefaultAzureCredential()
        
        # Get token for Language Service
        language_token = credential.get_token(
            "https://cognitiveservices.azure.com/.default"
        )
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {language_token.token}"
        }
        
        # -------------------------
        # PREPARE REQUEST BODY
        # -------------------------
        payload = {
            "displayName": f"PII Redaction - {blob_name}",
            "analysisInput": {
                "documents": [
                    {
                        "language": "en-US",
                        "id": "doc-1",
                        "source": {"location": source_blob_url},
                        "target": {"location": target_container_url},
                    }
                ]
            },
            "tasks": [
                {
                    "kind": "PiiEntityRecognition",
                    "taskName": "Redact PII",
                    "parameters": {
                        "redactionPolicy": {"policyKind": "entityMask"},
                        "excludeExtractionData": True,  # Only output redacted document, no JSON
                    },
                }
            ],
        }
        
        # -------------------------
        # SUBMIT JOB
        # -------------------------
        api_version = "2024-11-15-preview"
        submit_url = f"{language_endpoint}/language/analyze-documents/jobs?api-version={api_version}"
        
        logging.info(f'Submitting job to Language API: {submit_url}')
        response = requests.post(submit_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code != 202:
            logging.error(f"Error Response Status: {response.status_code}")
            logging.error(f"Error Response Body: {response.text}")
            response.raise_for_status()
            return
        
        operation_url = response.headers["operation-location"]
        logging.info(f"Job started: {operation_url}")
        
        # -------------------------
        # POLL STATUS
        # -------------------------
        max_wait_time = 300  # 5 minutes max
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            status_resp = requests.get(operation_url, headers=headers, timeout=30)
            status_resp.raise_for_status()
            
            status = status_resp.json()["status"]
            logging.info(f"Job Status: {status}")
            
            if status in ["succeeded", "failed", "cancelled"]:
                result = status_resp.json()
                
                if status == "succeeded":
                    logging.info("Job completed successfully")
                    
                    # Move redacted file to root of target container
                    if result.get("tasks", {}).get("items"):
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
                                logging.info(f"Redacted file created at: {redacted_pdf_url}")
                                
                                # Move file to root of container
                                blob_service_client = BlobServiceClient(
                                    account_url=storage_account_url,
                                    credential=credential
                                )
                                
                                # Extract the blob path from URL
                                blob_path = redacted_pdf_url.split(f"{storage_account_url}/{target_container}/")[1]
                                source_blob = blob_service_client.get_blob_client(target_container, blob_path)
                                
                                # New blob name at root (preserve original extension)
                                file_extension = blob_name.split('.')[-1] if '.' in blob_name else 'pdf'
                                new_blob_name = blob_name.replace(f".{file_extension}", f"_redacted.{file_extension}")
                                target_blob = blob_service_client.get_blob_client(target_container, new_blob_name)
                                
                                # Copy to root
                                logging.info(f"Moving to: {storage_account_url}/{target_container}/{new_blob_name}")
                                target_blob.start_copy_from_url(source_blob.url)
                                
                                # Wait for copy to complete
                                copy_status = target_blob.get_blob_properties().copy.status
                                while copy_status == "pending":
                                    time.sleep(0.5)
                                    copy_status = target_blob.get_blob_properties().copy.status
                                
                                logging.info(f"File moved successfully to: {new_blob_name}")
                                
                                # Delete the subfolder file
                                source_blob.delete_blob()
                                
                                logging.info(f"Deleted subfolder file: {blob_path}")
                            else:
                                logging.warning("No redacted PDF found in results")
                        else:
                            logging.warning("No documents found in results")
                    else:
                        logging.warning("No task items found in results")
                        
                elif status == "failed":
                    logging.error(f"Job failed: {result}")
                else:
                    logging.warning(f"Job cancelled: {result}")
                
                break
            
            time.sleep(2)  # Poll every 2 seconds
        else:
            logging.error(f"Job timed out after {max_wait_time} seconds")
        
    except requests.exceptions.RequestException as e:
        logging.error(f'Request error: {str(e)}')
    except Exception as e:
        logging.error(f'Unexpected error: {str(e)}')
        import traceback
        logging.error(traceback.format_exc())