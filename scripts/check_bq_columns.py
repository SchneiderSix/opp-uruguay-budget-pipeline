"""Check actual BigQuery column names for the external tables."""

from google.cloud import bigquery

PROJECT_ID = "fabled-imagery-488015-p6"
DATASET_ID = "opp_budget"
client = bigquery.Client(project=PROJECT_ID)

for table_id in ["raw_budget_credits", "raw_budget_credits_2021", "raw_presupuesto", "raw_historico"]:
    ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table = client.get_table(ref)
    print(f"\n{table_id}:")
    for field in table.schema:
        # Print name with repr to see exact bytes
        print(f"  {repr(field.name)}: {field.field_type}")
