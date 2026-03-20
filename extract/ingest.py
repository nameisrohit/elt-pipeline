"""
Extract: Pull housing data from CSO and upload to GCS as JSON.
"""
import json
import requests
from datetime import datetime
from google.cloud import storage

# ── CONFIG ──────────────────────────────────────────────
DATASET_ID = "NDQ07"  # New dwelling completions by county
CSO_API_URL = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{DATASET_ID}/JSON-stat/2.0/en"
GCS_BUCKET = "elt-pipeline-raw-data"
# ────────────────────────────────────────────────────────

def extract_from_cso():
    """Pull data from CSO API."""
    print(f"Fetching dataset {DATASET_ID} from CSO...")
    response = requests.get(CSO_API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"Got {len(json.dumps(data))} bytes of data")
    return data

def upload_to_gcs(data: dict):
    """Upload raw JSON to GCS with a timestamp."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # Timestamp the file so each run creates a new snapshot
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"raw/housing/{DATASET_ID}_{timestamp}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
    print(f"Uploaded to gs://{GCS_BUCKET}/{blob_name}")
    return f"gs://{GCS_BUCKET}/{blob_name}"

def main():
    raw_data = extract_from_cso()
    gcs_path = upload_to_gcs(raw_data)
    print(f"Done! Data landed at: {gcs_path}")

if __name__ == "__main__":
    main()