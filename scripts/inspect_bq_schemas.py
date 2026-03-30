"""Inspect BigQuery external table schemas to verify column names."""

from google.cloud import bigquery

PROJECT_ID = "fabled-imagery-488015-p6"
DATASET_ID = "opp_budget"

client = bigquery.Client(project=PROJECT_ID)

for table_id in ["raw_ckan", "raw_transparency", "raw_pdf_extractions"]:
    ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table = client.get_table(ref)
    print(f"\n{'='*60}")
    print(f"{table_id} ({table.num_rows} rows)")
    print(f"{'='*60}")
    for field in table.schema:
        print(f"  {field.name}: {field.field_type}")

    # Preview first 3 rows
    query = f"SELECT * FROM `{ref}` LIMIT 3"
    try:
        rows = list(client.query(query).result())
        if rows:
            print(f"\nSample row:")
            for k, v in dict(rows[0]).items():
                print(f"  {k} = {v}")
    except Exception as e:
        print(f"\nPreview error: {e}")
