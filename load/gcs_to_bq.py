"""
Load: Read raw JSON from GCS, flatten it, load into BigQuery.
"""
import json
from google.cloud import storage, bigquery
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────
GCS_BUCKET = "elt-pipeline-raw-data"
BQ_PROJECT = "elt-pipeline-490819"         # your GCP project ID
BQ_DATASET = "housing_raw"
BQ_TABLE = "dwelling_completions"
# ────────────────────────────────────────────────────────

def get_latest_file():
    """Find the most recent raw file in GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix="raw/housing/"))
    if not blobs:
        raise FileNotFoundError("No files found in GCS")
    latest = sorted(blobs, key=lambda b: b.name)[-1]
    print(f"Using latest file: {latest.name}")
    return latest

def flatten_json_stat(data: dict) -> list[dict]:
    """
    Convert JSON-stat 2.0 into flat rows.
    Each row = one combination of all dimensions + the value.
    """
    dimensions = data["dimension"]
    dim_ids = list(data["id"])  # ordered dimension names
    dim_sizes = data["size"]    # size of each dimension
    values = data["value"]

    # Build category labels for each dimension
    dim_labels = {}
    for dim_id in dim_ids:
        cats = dimensions[dim_id]["category"]["label"]
        # Maintain order using the index
        idx = dimensions[dim_id]["category"].get("index", {})
        if isinstance(idx, dict):
            ordered = sorted(idx.items(), key=lambda x: x[1])
            dim_labels[dim_id] = [cats[k] for k, _ in ordered]
        else:
            dim_labels[dim_id] = list(cats.values())

    # Flatten: iterate through all combinations
    rows = []
    total = len(values)
    for i in range(total):
        row = {}
        remainder = i
        for j, dim_id in enumerate(dim_ids):
            size = dim_sizes[j]
            # Calculate index for this dimension
            block = 1
            for k in range(j + 1, len(dim_ids)):
                block *= dim_sizes[k]
            idx = remainder // block
            remainder = remainder % block
            row[dim_id] = dim_labels[dim_id][idx]
        # Clean field names for BigQuery
        row = {k.replace("(", "_").replace(")", "").replace(" ", "_"): v for k, v in row.items()}
        row["value"] = values[i] if i < len(values) else None
        row["value"] = values[i] if i < len(values) else None
        row["loaded_at"] = datetime.utcnow().isoformat()
        rows.append(row)

    print(f"Flattened {len(rows)} rows")
    return rows

def load_to_bigquery(rows: list[dict]):
    """Load flattened rows into BigQuery."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Auto-detect schema from the data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace each run
    )

    # Convert to newline-delimited JSON
    ndjson = "\n".join(json.dumps(row) for row in rows)

    import io
    job = client.load_table_from_file(
        io.BytesIO(ndjson.encode()),
        table_ref,
        job_config=job_config,
    )
    job.result()  # Wait for completion
    print(f"Loaded {job.output_rows} rows to {table_ref}")

def main():
    blob = get_latest_file()
    raw_data = json.loads(blob.download_as_text())
    rows = flatten_json_stat(raw_data)
    load_to_bigquery(rows)

if __name__ == "__main__":
    main()