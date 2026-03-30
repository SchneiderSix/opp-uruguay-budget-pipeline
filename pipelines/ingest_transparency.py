"""Ingest CSVs from Uruguay's Transparency Budget Portal (OPP)."""

import io
import os
from pathlib import Path

import httpx
import polars as pl
from google.cloud import storage

BUCKET_NAME = os.environ.get("GCS_BUCKET", "opp-data-lake")
GCS_PREFIX = "raw/transparency"
LOCAL_DIR = Path("data/raw/transparency")

# Known open data CSV endpoints from the transparency portal.
# These URLs are published at:
#   https://transparenciapresupuestaria.opp.gub.uy/inicio/datos-abiertos
TRANSPARENCY_SOURCES = [
    {
        "name": "ejecucion_presupuestal",
        "description": "Budget execution data",
        "url": "https://transparenciapresupuestaria.opp.gub.uy/descargas/ejecucion.csv",
    },
    {
        "name": "recursos_humanos",
        "description": "Human resources data",
        "url": "https://transparenciapresupuestaria.opp.gub.uy/descargas/rrhh.csv",
    },
    {
        "name": "empresas_publicas",
        "description": "Public enterprises budget data",
        "url": "https://transparenciapresupuestaria.opp.gub.uy/descargas/empresas_publicas.csv",
    },
]


def download_csv(client: httpx.Client, url: str) -> bytes | None:
    """Download a CSV file, returning raw bytes or None on failure."""
    try:
        resp = client.get(url, follow_redirects=True, timeout=60.0)
        resp.raise_for_status()
        return resp.content
    except httpx.HTTPError as exc:
        print(f"  SKIP {url}: {exc}")
        return None


def csv_to_parquet(raw_bytes: bytes, output_path: Path) -> bool:
    """Convert CSV bytes to a Parquet file using Polars."""
    try:
        df = pl.read_csv(io.BytesIO(raw_bytes), infer_schema_length=5000)
    except Exception as exc:
        print(f"  SKIP CSV parse error: {exc}")
        return False

    if df.is_empty():
        print("  SKIP empty CSV")
        return False

    df.write_parquet(output_path)
    print(f"  OK {output_path.name} ({df.shape[0]} rows, {df.shape[1]} cols)")
    return True


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

    parquet_files: list[Path] = []

    with httpx.Client() as client:
        for source in TRANSPARENCY_SOURCES:
            print(f"Downloading: {source['name']} — {source['description']}")
            raw = download_csv(client, source["url"])
            if raw is None:
                continue

            parquet_path = LOCAL_DIR / f"{source['name']}.parquet"
            if csv_to_parquet(raw, parquet_path):
                parquet_files.append(parquet_path)

    print(f"\nConverted {len(parquet_files)} files to Parquet")

    if os.environ.get("UPLOAD_GCS", "false").lower() == "true":
        print("Uploading to GCS...")
        for path in parquet_files:
            upload_to_gcs(path, BUCKET_NAME, GCS_PREFIX)
        print("GCS upload complete")


if __name__ == "__main__":
    main()
