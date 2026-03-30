"""Inspect actual Parquet schemas in GCS to find budget-relevant columns."""

import io
import polars as pl
from google.cloud import storage

BUCKET_NAME = "opp-data-lake-fabled-imagery-488015-p6"
client = storage.Client(project="fabled-imagery-488015-p6")
bucket = client.bucket(BUCKET_NAME)

for prefix in ["raw/ckan/", "raw/transparency/"]:
    print(f"\n{'='*70}")
    print(f"PREFIX: {prefix}")
    print(f"{'='*70}")
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquets = [b for b in blobs if b.name.endswith(".parquet")]

    for blob in parquets[:30]:
        fname = blob.name.split("/")[-1]
        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        try:
            df = pl.read_parquet(buf)
            cols = df.columns
            # Flag files with budget-related columns
            budget_cols = [c for c in cols if any(
                kw in c.lower() for kw in
                ["inciso", "credito", "monto", "ejecucion", "gasto", "presupuest", "anio", "año"]
            )]
            marker = " << BUDGET" if budget_cols else ""
            print(f"\n  {fname} ({df.shape[0]} rows, {df.shape[1]} cols){marker}")
            if budget_cols:
                print(f"    Budget cols: {budget_cols}")
                print(f"    All cols: {cols}")
        except Exception as e:
            print(f"\n  {fname}: ERROR {e}")
