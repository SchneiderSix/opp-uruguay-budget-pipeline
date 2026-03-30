"""Create BigQuery external tables pointing to GCS Parquet files."""

from google.cloud import bigquery

PROJECT_ID = "fabled-imagery-488015-p6"
DATASET_ID = "opp_budget"
BUCKET = "opp-data-lake-fabled-imagery-488015-p6"

client = bigquery.Client(project=PROJECT_ID)

TABLES = [
    {
        # 2011-2019: MONTO columns are INT64 in Parquet
        "table_id": "raw_budget_credits_int",
        "uris": [
            f"gs://{BUCKET}/raw/transparency/credito_{y}.parquet"
            for y in range(2011, 2020)
        ],
    },
    {
        # 2020+resumen: MONTO columns are STRING in Parquet
        "table_id": "raw_budget_credits_str",
        "uris": [
            f"gs://{BUCKET}/raw/transparency/credito_2020.parquet",
            f"gs://{BUCKET}/raw/transparency/credito_resumen.parquet",
        ],
    },
    {
        "table_id": "raw_budget_credits_2021",
        "uris": [f"gs://{BUCKET}/raw/transparency/credito_2021.parquet"],
    },
    {
        "table_id": "raw_presupuesto",
        "uris": [
            f"gs://{BUCKET}/raw/transparency/presupuesto_2015_2019.parquet",
            f"gs://{BUCKET}/raw/transparency/presupuesto_2020_2024.parquet",
        ],
    },
    {
        "table_id": "raw_historico",
        "uris": [
            f"gs://{BUCKET}/raw/ckan/opp-historico-de-credito-y-ejecucion-presupuestal-por-ue_1bb23eed-388d-4ff3-8d7e-89f77da6ddd0.parquet",
        ],
    },
    {
        "table_id": "raw_organismos",
        "uris": [f"gs://{BUCKET}/raw/transparency/organismos.parquet"],
    },
    {
        "table_id": "raw_pdf_extractions",
        "uris": [f"gs://{BUCKET}/processed/pdf_extractions/budget_data.parquet"],
    },
]

for t in TABLES:
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{t['table_id']}"
    client.delete_table(table_ref, not_found_ok=True)

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = t["uris"]

    if "schema" in t:
        external_config.autodetect = False
        external_config.schema = t["schema"]
    else:
        external_config.autodetect = True

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config
    table = client.create_table(table)
    print(f"Created: {t['table_id']} -> {len(t['uris'])} file(s)")

# Clean up old tables
for old in ["raw_ckan", "raw_transparency", "raw_budget_credits"]:
    client.delete_table(f"{PROJECT_ID}.{DATASET_ID}.{old}", not_found_ok=True)
    print(f"Dropped old table: {old}")

# Verify
print("\nTables in opp_budget:")
for t in client.list_tables(f"{PROJECT_ID}.{DATASET_ID}"):
    print(f"  {t.table_id} ({t.table_type})")
