# Uruguay National Budget Data Pipeline (OPP)

End-to-end data engineering pipeline for Uruguay's national budget data from the **Oficina de Planeamiento y Presupuesto (OPP)**. Built as a capstone project for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## Problem Description

Uruguay's national budget data is scattered across multiple government sources with no unified analytical layer:

- **CKAN Open Data Portal** (`catalogodatos.gub.uy`): 56 datasets in CSV format with inconsistent schemas
- **Transparency Portal** (`transparenciapresupuestaria.opp.gub.uy`): Budget credits and execution data (2011-2021) in CSV, converted to Parquet
- **PDF Documents** (`opp.gub.uy`): Historical budget reports in unstructured PDF format (572 PDFs scraped)

This pipeline ingests data from all three sources, extracts structured data from PDFs using a language model (Qwen2.5-3B on Google Colab with T4 GPU), and transforms everything into an analytical star-schema warehouse to answer questions like:

- How has government spending evolved from 2005 to 2024?
- Which agencies (incisos) receive the most funding?
- What is the budget execution rate per agency and year?
- Can we forecast next year's budget allocation?

## Architecture

```
 Data Sources
 +-----------------+   +---------------------+   +-------------------+
 |   CKAN API      |   |  Transparency       |   |  OPP Website      |
 | (catalogodatos) |   |  Portal (CSVs)      |   |  (572 PDFs)       |
 +--------+--------+   +---------+-----------+   +---------+---------+
          |                       |                         |
          +----------+------------+                         |
                     |                           +----------+---------+
                     v                           | Google Colab (GPU) |
          +---------------------+                | Qwen2.5-3B FP16   |
          | Notebook 01         |                | PDF -> Parquet     |
          | Ingestion & EDA     |                +----------+---------+
          +---------+-----------+                           |
                    |                                       |
                    v                                       v
          +----------------------------------------------------+
          |              GCS Data Lake (Parquet)                |
          |  raw/ckan/  raw/transparency/  raw/pdfs/           |
          |  processed/pdf_extractions/                        |
          +--------------------------+-------------------------+
                                     |
                           +---------v---------+
                           | BigQuery External |
                           |     Tables        |
                           +---------+---------+
                                     |
                           +---------v---------+
                           |    dbt (local)    |
                           |  staging ->       |
                           |  intermediate ->  |
                           |  marts            |
                           +---------+---------+
                                     |
                           +---------v---------+
                           | BigQuery Tables   |
                           | fct_budget_exec.  |
                           | (partitioned +    |
                           |  clustered)       |
                           +---------+---------+
                                     |
                           +---------v---------+
                           |   Looker Studio   |
                           |   (Dashboard)     |
                           +-------------------+
```

## Dashboard

**Looker Studio**: [link placeholder -- replace with your shared Looker Studio URL]

Two tiles as required:
1. **Bar chart (categorical)** -- Budget allocation by government agency (`denominacion_inciso`), filterable by fiscal year
2. **Line chart (temporal)** -- Budget evolution over time (2005-2024) showing `credito_vigente` vs `ejecucion`

## Technologies Used

| Component | Technology |
|-----------|------------|
| Cloud | Google Cloud Platform (GCS, BigQuery) |
| Infrastructure as Code | Terraform |
| Data Lake | Google Cloud Storage (Parquet format) |
| Data Warehouse | BigQuery (external + materialized tables) |
| Batch Processing | Google Colab (Python + Qwen2.5-3B LLM) |
| Transformations | dbt-bigquery (staging, intermediate, marts) |
| Orchestration | Kestra (weekly DAG) |
| Dashboard | Looker Studio |
| ML (bonus) | BigQuery ML (Linear Regression, K-Means) |

## Project Structure

```
project-1/
├── terraform/                  # IaC: GCS bucket, BigQuery dataset, service account
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── notebooks/                  # Google Colab notebooks (run on cloud GPU, not locally)
│   ├── 01_ingestion_eda.ipynb  # Data ingestion from 3 sources + EDA
│   ├── 02_pdf_extraction.ipynb # Qwen2.5-3B PDF -> structured Parquet
│   └── 03_bqml_analysis.ipynb  # BigQuery ML experiments + visualizations
├── dbt/                        # Data transformations (run locally)
│   ├── models/
│   │   ├── staging/            # stg_budget_credits, stg_pdf_extractions
│   │   ├── intermediate/       # int_budget_unified, int_budget_enriched
│   │   └── marts/              # fct_budget_execution, dim_incisos, dim_categories
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/                    # BigQuery table management
│   ├── create_bq_sources.py    # Creates external tables from GCS Parquet
│   ├── check_bq_columns.py     # Inspect column names and types
│   └── inspect_gcs_schemas.py  # Check Parquet schemas in GCS
├── pipelines/                  # Ingestion scripts (referenced by Kestra)
│   ├── ingest_ckan.py
│   ├── ingest_transparency.py
│   └── ingest_pdfs.py
├── orchestration/
│   └── opp_pipeline.yml        # Kestra DAG: ingest (parallel) -> dbt run -> dbt test
├── pyproject.toml              # Python dependencies (all versions pinned)
├── Makefile                    # Reproducibility commands
└── README.md
```

## Data Dictionary

### Fact Table: `fct_budget_execution`

Partitioned by `fiscal_year` (integer range 2005-2030, interval 1). Clustered by `inciso`, `categoria`.

| Column | Type | Description |
|--------|------|-------------|
| `execution_id` | STRING | Surrogate key (hash of fiscal_year + inciso + denominacion + categoria) |
| `fiscal_year` | INT64 | Budget year (2005-2024) |
| `inciso` | INT64 | Government agency code (top-level organizational unit) |
| `denominacion_inciso` | STRING | Agency name (e.g., "Ministerio de Educacion y Cultura") |
| `categoria` | STRING | Spending category / type of expenditure |
| `total_credito_vigente` | FLOAT64 | Approved budget amount (Uruguayan pesos, UYU) |
| `total_ejecucion` | FLOAT64 | Actual spending (UYU). NULL when execution data is unavailable |
| `avg_execution_rate_pct` | FLOAT64 | Execution rate: (ejecucion / credito) * 100 |
| `avg_credito_yoy_pct_change` | FLOAT64 | Year-over-year budget change (%) |
| `record_count` | INT64 | Number of source records aggregated into this row |
| `source_count` | INT64 | Number of distinct data sources contributing |

### Dimension Tables

**`dim_incisos`**: Government agencies (one row per unique agency code)

| Column | Type | Description |
|--------|------|-------------|
| `inciso` | INT64 | Agency code |
| `denominacion_inciso` | STRING | Agency name |

**`dim_categories`**: Spending categories

| Column | Type | Description |
|--------|------|-------------|
| `categoria` | STRING | Category name |
| `category_id` | INT64 | Auto-generated sequential ID |

### Spanish Budget Terminology

| Spanish Term | English | Context |
|-------------|---------|---------|
| Inciso | Agency / Section | Top-level government organization (ministry, judiciary, etc.) |
| Denominacion | Name / Denomination | Official name of an entity |
| Credito Vigente | Active Credit | Approved/authorized budget allocation for spending |
| Ejecucion | Execution | Actual money spent against the approved budget |
| Monto Aprobado | Approved Amount | Originally approved budget before modifications |
| Monto Vigente | Current Amount | Budget after mid-year adjustments |
| Monto Ejecutado | Executed Amount | Actual expenditure |
| Organismo | Organization | Government body (synonym for inciso at different granularity) |
| Unidad Ejecutora (UE) | Executing Unit | Sub-unit within an agency responsible for spending |
| Area Programatica (AP) | Program Area | Functional area within the budget |
| Tipo de Gasto | Expenditure Type | Classification of spending (personnel, investment, etc.) |
| Presupuesto | Budget | The national budget |

## Known Data Issues

### Raw Column Name Quirks

| Issue | Details | Resolution |
|-------|---------|------------|
| `A_O` instead of `AÑO` | BigQuery cannot represent `Ñ` in column names from Parquet | Mapped `A_O` -> `fiscal_year` in staging |
| MONTO type mismatch | 2011-2019 files store amounts as `INT64`, 2020+ as `STRING` | Split into two external tables: `raw_budget_credits_int` and `raw_budget_credits_str` |
| 2021 different schema | Uses lowercase names (`organismo_codigo`, `credito`, `ejecutado`) instead of uppercase (`ORG_ID`, `MONTO_VIGENTE`, `MONTO_EJECUTADO`) | Separate CTE in `stg_budget_credits.sql` |
| Missing execution data | PDF extractions and years 2019-2024 lack execution figures | Stored as NULL (not zero) to distinguish from actual zero spend |

### Data Coverage by Year

| Year Range | Source | Budget Data | Execution Data |
|-----------|--------|-------------|----------------|
| 2005-2010 | PDF extractions (Qwen2.5) | Partial | Not available |
| 2011-2018 | Transparency Portal | Complete | Complete |
| 2019 | Transparency Portal | Complete | Missing (data gap) |
| 2020 | Transparency Portal (STRING format) | Complete | Not available |
| 2021 | Separate credits file | Partial | Partial |
| 2022-2024 | PDF extractions | Sparse | Not available |

### PDF Extraction Limitations

- **113 of 572 PDFs** were processed with Qwen2.5-3B-Instruct (FP16 on T4 GPU) due to Google Colab runtime time limits
- The extraction pipeline saves **per-PDF checkpoints** to GCS (`processed/pdf_extractions/checkpoints/`) for resilience against Colab disconnects
- Some PDFs are corrupted (`PDFNoValidXRef` / `PSEOF` errors) and are skipped automatically
- The LLM occasionally hallucinates large numbers; these are filtered out during validation (amounts > 1e15 UYU are excluded, fiscal_year must be 1985-2030)
- All extracted records use a strict schema with explicit column types (`pl.Utf8`) to handle inconsistent LLM output

## dbt Model Lineage

```
Sources (BigQuery External Tables on GCS Parquet)
  raw_budget_credits_int  ── 2011-2019, MONTO columns as INT64
  raw_budget_credits_str  ── 2020 + resumen, MONTO columns as STRING
  raw_budget_credits_2021 ── different column naming convention
  raw_pdf_extractions     ── Qwen2.5-3B extracted data
  raw_presupuesto         ── 5-year national budget plans
  raw_historico           ── historical execution by executing unit
  raw_organismos          ── government organizations reference

Staging (views)
  stg_budget_credits      <- unions 3 credit sources, normalizes columns and types
  stg_pdf_extractions     <- cleans PDF extraction output

Intermediate (views)
  int_budget_unified      <- unions credits + PDF data into consistent schema
  int_budget_enriched     <- adds YoY % change and execution rate metrics

Marts (materialized tables)
  fct_budget_execution    <- aggregated fact table (partitioned by fiscal_year, clustered by inciso + categoria)
  dim_incisos             <- government agencies dimension
  dim_categories          <- spending categories dimension
```

## Orchestration

The Kestra flow (`orchestration/opp_pipeline.yml`) defines a weekly DAG:

1. **Parallel ingestion**: CKAN + Transparency + PDFs run simultaneously in Docker containers
2. **dbt run**: Executes all 7 models (staging -> intermediate -> marts)
3. **dbt test**: Runs 18 data quality tests (not_null, unique, referential integrity)

Schedule: Every Monday at 06:00 UTC.

## How to Reproduce

### Prerequisites

- GCP account with billing enabled
- [Terraform](https://www.terraform.io/downloads) >= 1.0
- Python >= 3.11 with [uv](https://docs.astral.sh/uv/)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) authenticated
- Google Colab account (free tier works; Pro recommended for PDF extraction)

### Step 1: Clone and install

```bash
git clone https://github.com/<your-username>/opp-uruguay-budget-pipeline.git
cd opp-uruguay-budget-pipeline
uv sync
```

### Step 2: Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <YOUR_PROJECT_ID>
```

### Step 3: Provision infrastructure (Terraform)

```bash
cd terraform
# Create terraform.tfvars:
#   project_id = "your-gcp-project-id"
terraform init
terraform apply
cd ..
```

Creates: GCS bucket, BigQuery dataset `opp_budget`, service account with IAM roles.

### Step 4: Ingest data (Google Colab)

Upload and run notebooks **in order** on Google Colab:

1. **`01_ingestion_eda.ipynb`** -- Ingests from CKAN, Transparency Portal, and scrapes PDFs. Uploads Parquet to GCS. (~15 min)
2. **`02_pdf_extraction.ipynb`** -- Extracts structured data from PDFs using Qwen2.5-3B on T4 GPU. Saves per-PDF checkpoints. (~2-4 hours)

### Step 5: Create BigQuery external tables

```bash
uv run python scripts/create_bq_sources.py
```

### Step 6: Run dbt transformations

```bash
export GCP_PROJECT_ID=<YOUR_PROJECT_ID>
cd dbt
uv run dbt deps
uv run dbt run    # 7 models: PASS=7
uv run dbt test   # 18 tests: PASS=18
```

### Step 7: Create dashboard

Open [Looker Studio](https://lookerstudio.google.com), connect to BigQuery table `opp_budget.fct_budget_execution`, and create two tiles:
- Bar chart: budget by `denominacion_inciso` (filtered by `fiscal_year`)
- Line chart: `total_credito_vigente` over `fiscal_year`

### Step 8 (Optional): BigQuery ML

Run `03_bqml_analysis.ipynb` in Google Colab for:
- Linear regression budget forecasting
- K-Means clustering of government agencies by spending patterns

### Quick reference (Makefile)

```bash
make setup       # uv sync
make infra       # terraform apply
make transform   # dbt deps + run + test
make destroy     # terraform destroy
```

## Cleanup

```bash
make destroy     # Removes GCS bucket and BigQuery dataset
```
