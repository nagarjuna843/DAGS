import functions_framework
import google.auth
from google.auth.transport.requests import AuthorizedSession

# CONSTANTS
WEB_SERVER_URL = "https://7e9250b365314dca9e1d1ea1b4354fdd-dot-us-central1.composer.googleusercontent.com"
DAG_ID = "LEVEL_1_DAG"

# Initialize Google credentials with the cloud-platform scope at startup
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

@functions_framework.cloud_event
def trigger_dag_on_gcs_upload(cloud_event):
    """Triggered by a change to a Cloud Storage bucket."""
    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    print(f"File upload detected! Bucket: {bucket_name}, File: {file_name}")

    # Step 1: Create an authorized session using the function's Service Account
    authed_session = AuthorizedSession(CREDENTIALS)

    # Step 2: Build the target API endpoint and trigger payload request
    endpoint = f"{WEB_SERVER_URL.rstrip('/')}/api/v1/dags/{DAG_ID}/dagRuns"
    payload = {
        "conf": {
            "bucket": bucket_name,
            "file_name": file_name
        }
    }

    # Step 3: Dispatch the Airflow POST trigger request
    try:
        # AuthorizedSession automatically injects the correct OAuth2 bearer token headers
        response = authed_session.request(
            method="POST",
            url=endpoint,
            json=payload
        )
        
        if response.status_code in [200, 201]:
            print(f"Successfully triggered DAG '{DAG_ID}'. Response: {response.text}")
        else:
            print(f"Failed to trigger DAG. Status: {response.status_code}, Error: {response.text}")
    except Exception as e:
        print(f"HTTP REST API connection request failed: {str(e)}")

