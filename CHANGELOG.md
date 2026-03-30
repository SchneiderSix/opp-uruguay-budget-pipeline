# Changelog

## 2026-03-27

### Fixed
- CKAN ingestion: added `decimal_comma=True` fallback for Uruguayan locale numbers (e.g. `208384,26`)
- Replaced broken transparency portal URLs (403) with CKAN Datastore dump API
- Stream A now auto-detects `datastore_active` resources and uses dump API to bypass 403s
- Fixed Makefile paths: absolute quoted paths for terraform/gcloud (spaces in `Program Files`)
- Fixed Terraform ADC credentials: copied from MS Store Python path to standard `%APPDATA%/gcloud/`

### Changed
- GCS bucket and BigQuery dataset region from `southamerica-east1` to `US` (free tier eligible)
- Notebook 02: Qwen model loading from FP16 to BitsAndBytes NF4 4-bit quantization (~5.5 GB VRAM, fits free Colab T4)
- Restructured notebooks for Colab-first workflow (removed local `make ingest` targets)
- Removed old `01_exploration.ipynb`, replaced with `01_ingestion_eda.ipynb`

### Added
- Google Cloud SDK installed at `ProjectsCode/google-cloud-sdk/` (v562.0.0 + bq component)
- Makefile targets: `auth`, `auth-set-project`, `auth-service-account`, `bq-tables`, `bq-preview`
- `terraform.tfvars` with project ID (gitignored)
- Stream B: 18 budget datasets via CKAN Datastore dump (credits 2011-2021, 5-year budgets, organizations)

### Ingestion Results
- 56 CKAN datasets ingested to GCS as Parquet (~340K+ total rows)
- GCP free trial: $298/$300 remaining, expires May 22, 2026

## 2026-03-26

### Added
- Initial project scaffolding: directory structure, pyproject.toml, Makefile
- Terraform configuration for GCP (GCS bucket, BigQuery dataset, service account)
- Ingestion pipelines: CKAN API, Transparency portal, PDF scraping
- dbt models: staging, intermediate, and mart layers
- Kestra orchestration flow definition
- Colab notebook stubs for EDA, PDF extraction, and BQML analysis
