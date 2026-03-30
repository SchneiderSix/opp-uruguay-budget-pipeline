.PHONY: setup infra destroy auth auth-set-project auth-service-account transform bq-tables bq-preview audit

TERRAFORM := "C:/Program Files/ProjectsCode/terraform.exe"
GCLOUD    := "C:/Program Files/ProjectsCode/google-cloud-sdk/bin/gcloud"
BQ        := "C:/Program Files/ProjectsCode/google-cloud-sdk/bin/bq"

setup:
	uv sync

audit:
	uv run pip-audit

# --- GCP Authentication ---

auth:
	$(GCLOUD) auth login
	$(GCLOUD) auth application-default login

auth-set-project:
	$(GCLOUD) config set project $(GCP_PROJECT_ID)

auth-service-account:
	$(GCLOUD) iam service-accounts keys create credentials.json \
		--iam-account=opp-pipeline@$(GCP_PROJECT_ID).iam.gserviceaccount.com
	@echo "GOOGLE_APPLICATION_CREDENTIALS=$$(pwd)/credentials.json"

# --- Infrastructure ---

infra:
	cd terraform && $(TERRAFORM) init && $(TERRAFORM) apply -auto-approve

destroy:
	cd terraform && $(TERRAFORM) destroy -auto-approve

# --- BigQuery Source Tables (create from GCS Parquet) ---

GCP_PROJECT_ID := fabled-imagery-488015-p6
BUCKET := opp-data-lake-fabled-imagery-488015-p6

bq-create-sources:
	$(BQ) mk --external_table_definition='gs://$(BUCKET)/raw/ckan/*.parquet@PARQUET' \
		$(GCP_PROJECT_ID):opp_budget.raw_ckan || true
	$(BQ) mk --external_table_definition='gs://$(BUCKET)/raw/transparency/*.parquet@PARQUET' \
		$(GCP_PROJECT_ID):opp_budget.raw_transparency || true
	$(BQ) mk --external_table_definition='gs://$(BUCKET)/processed/pdf_extractions/budget_data.parquet@PARQUET' \
		$(GCP_PROJECT_ID):opp_budget.raw_pdf_extractions || true
	@echo "Source tables created in BigQuery"

# --- Transformations (run after bq-create-sources) ---

transform:
	cd dbt && uv run dbt deps && uv run dbt run && uv run dbt test

# --- BigQuery Queries ---

bq-tables:
	$(BQ) ls $(GCP_PROJECT_ID):opp_budget

bq-preview:
	$(BQ) head -n 10 $(GCP_PROJECT_ID):opp_budget.fct_budget_execution
