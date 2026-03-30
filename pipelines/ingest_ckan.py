"""Ingest structured CSVs from Uruguay's CKAN open data catalog (OPP organization)."""

import io
import os
from pathlib import Path

import httpx
import polars as pl
from google.cloud import storage

CKAN_API = "https://catalogodatos.gub.uy/api/3/action/package_search"
BUCKET_NAME = os.environ.get("GCS_BUCKET", "opp-data-lake")
GCS_PREFIX = "raw/ckan"
LOCAL_DIR = Path("data/raw/ckan")


def fetch_opp_packages(client: httpx.Client, rows: int = 50) -> list[dict]:
    """Query CKAN API for all OPP organization datasets."""
    resp = client.get(CKAN_API, params={"fq": "organization:opp", "rows": rows})
    resp.raise_for_status()
    return resp.json()["result"]["results"]


def extract_csv_resources(packages: list[dict]) -> list[dict]:
    """Extract download URLs for CSV resources from CKAN packages."""
    resources = []
    for pkg in packages:
        for res in pkg.get("resources", []):
            fmt = (res.get("format") or "").upper()
            if fmt == "CSV" and res.get("url"):
                resources.append(
                    {
                        "package_name": pkg["name"],
                        "resource_id": res["id"],
                        "url": res["url"],
                        "name": res.get("name", "unknown"),
                    }
                )
    return resources


def download_and_convert(
    client: httpx.Client, resource: dict, local_dir: Path
) -> Path | None:
    """Download a CSV resource and convert it to Parquet using Polars."""
    url = resource["url"]
    parquet_name = f"{resource['package_name']}_{resource['resource_id']}.parquet"
    parquet_path = local_dir / parquet_name

    try:
        resp = client.get(url, follow_redirects=True, timeout=60.0)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"  SKIP {url}: {exc}")
        return None

    try:
        df = pl.read_csv(io.BytesIO(resp.content), infer_schema_length=5000)
    except Exception as exc:
        print(f"  SKIP CSV parse error for {url}: {exc}")
        return None

    if df.is_empty():
        print(f"  SKIP empty CSV: {url}")
        return None

    df.write_parquet(parquet_path)
    print(f"  OK {parquet_name} ({df.shape[0]} rows, {df.shape[1]} cols)")
    return parquet_path


def upload_to_gcs(local_path: Path, bucket_name: str, prefix: str) -> None:
    """Upload a local file to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"{prefix}/{local_path.name}"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    print(f"  GCS uploaded: gs://{bucket_name}/{blob_name}")


def main() -> None:
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client() as client:
        print("Fetching OPP packages from CKAN...")
        packages = fetch_opp_packages(client)
        print(f"Found {len(packages)} packages")

        resources = extract_csv_resources(packages)
        print(f"Found {len(resources)} CSV resources")

        parquet_files: list[Path] = []
        for res in resources:
            print(f"Processing: {res['name']} ({res['package_name']})")
            path = download_and_convert(client, res, LOCAL_DIR)
            if path:
                parquet_files.append(path)

    print(f"\nConverted {len(parquet_files)} files to Parquet")

    if os.environ.get("UPLOAD_GCS", "false").lower() == "true":
        print("Uploading to GCS...")
        for path in parquet_files:
            upload_to_gcs(path, BUCKET_NAME, GCS_PREFIX)
        print("GCS upload complete")


if __name__ == "__main__":
    main()
