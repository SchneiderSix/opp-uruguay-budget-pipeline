# Changelog

## 2026-04-03

### Changed — Notebook 02 (v3 multi-column extraction)
- Replaced single `monto` column with `credito_vigente`, `credito_ejecutado`, `inversion`
- Updated LLM extraction prompt with amount column rules and label mappings
- Added `migrate_monto_to_columns()` for backward compat with v2 checkpoints
- Validation stage 1 now requires at least one amount >= 1000 (was: monto >= 1000)
- Dedup subset updated to include all three amount columns
- v3 checkpoint prefix (`checkpoints_v3/`) and raw output (`budget_data_v3_raw.parquet`)
- Upload cell reports per-column coverage stats

### Added — CGN SIIF data source (Workstream 1)
- Notebook 01: Stream D — embedded CGN SIIF execution data for 2020 and 2023 (official per-inciso amounts)
- `dbt/models/staging/stg_cgn_execution.sql` — new staging model for CGN data
- `scripts/create_bq_sources.py` — added `raw_cgn_execution` external table
- CGN data uploaded to GCS as `raw/cgn/siif_ejecucion_{year}.parquet`

### Changed — dbt models (multi-column + CGN integration)
- `stg_pdf_extractions.sql` — replaced `monto` with three nullable amount columns
- `int_budget_unified.sql` — PDF CTE maps credito_vigente/credito_ejecutado/inversion properly; added CGN CTE
- `int_budget_enriched.sql` — added `inversion` LAG window and `inversion_yoy_pct_change`
- `fct_budget_execution.sql` — added `total_inversion` aggregation
- `staging/schema.yml` — added `raw_cgn_execution` source, `stg_cgn_execution` model, updated descriptions
- `marts/schema.yml` — added `total_inversion` column documentation

### Fixed — Notebook 01 PDF scraping resilience
- `opp.gub.uy/es/presupuesto-nacional` now returns 404; root domain serves a maintenance page
- Added fallback URL list + graceful GCS cache fallback when OPP site is unavailable

### Note — OPP site suspicious maintenance (2026-04-03)
- `opp.gub.uy` replaced with a standalone maintenance page claiming "Semana de Turismo" until April 6
- The page is a single inline HTML/CSS/JS file with no CMS — firework animations, emoji decorations,
  gradient backgrounds. Looks auto-generated, not typical of `.gub.uy` government sites
- **Suspicious timing**: the site was operational until ~3 days ago, shortly after a LinkedIn post
  about this OPP budget analysis project. The "maintenance" appeared after that post
- `/es/presupuesto-nacional` returns 404 (not even the maintenance page), suggesting the
  Drupal CMS was taken offline entirely, not just put behind a splash screen
- All 575 PDFs remain cached in GCS from prior runs — no data loss

## 2026-04-02

### Analysis
- Ran BigQuery diagnostic queries across all tables to audit LLM PDF extraction quality
- Cross-referenced pipeline data against official SIIF/CGN inciso classifier
- Identified 9 critical data quality issues in the PDF extraction output

### Issues Found
- **Inciso cross-contamination**: LLM assigned wrong inciso numbers to 40-100% of rows (multi-inciso PDFs)
- **Inciso 0 garbage**: 747 rows with invalid inciso=0 (equipment names, city names as denominacion)
- **Percentages as monto**: 507 rows where execution percentages/KPI counts were extracted as budget amounts
- **Category field contamination**: sub-department names, "Total" rows, "Credito Asignado" labels in categoria
- **Wrong entities in dim_incisos**: Inciso 28 should be BPS (not "Instituto Nacional de Investigacion Social"), Inciso 30 should be Deuda Publica (not duplicate ASSE)
- **Future year hallucinations**: Rows for 2025-2029 from projected estimates treated as actual budgets
- **Historical name changes unhandled**: MVOTMA→MVOT (2020), Ministerio de Turismo y Deporte→Turismo (2020), Inciso 36 created 2020

### Changed — Notebook 02 (v2 rewrite, not yet re-run)
- Embedded canonical inciso reference list (36 entries from SIIF/CGN) directly in LLM prompt
- Added PDF filename parser (`parse_inciso_range_from_filename`) to provide expected inciso context per document
- Added strict prompt rules: no percentages, KPIs, equipment names, city names, future years, inciso=0
- Added post-extraction `correct_inciso_name()` and `try_fix_inciso_from_name()` for name normalization
- Replaced single-pass validation with 5-stage pipeline: basic validity → garbage categories → garbage denominacion → canonical name enforcement → dedup
- Tightened filters: inciso 1-36, monto >= 1000 UYU, fiscal_year 2005-2024
- v2 checkpoint prefix (`checkpoints_v2/`) to avoid mixing with v1 data
- Final output still writes to `budget_data.parquet` (overwrites v1 on re-run)

### Added
- `claudedocs/reference_incisos_presupuesto_nacional.md` — official inciso reference from SIIF for pipeline validation

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
