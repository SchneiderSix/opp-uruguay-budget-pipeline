terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.24.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "opp-data-lake-${var.project_id}"
}

# --- GCS Data Lake Bucket ---

resource "google_storage_bucket" "data_lake" {
  name          = local.bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# Folder markers for organizational structure
resource "google_storage_bucket_object" "folder_raw_ckan" {
  name    = "raw/ckan/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "folder_raw_transparency" {
  name    = "raw/transparency/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "folder_raw_pdfs" {
  name    = "raw/pdfs/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "folder_processed" {
  name    = "processed/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "folder_parquet" {
  name    = "parquet/"
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

# --- BigQuery Dataset ---

resource "google_bigquery_dataset" "opp_budget" {
  dataset_id = "opp_budget"
  location   = var.bq_location

  description = "Uruguay OPP national budget data warehouse"

  delete_contents_on_destroy = true
}

# --- Service Account ---

resource "google_service_account" "pipeline" {
  account_id   = "opp-pipeline"
  display_name = "OPP Pipeline Service Account"
  description  = "Service account for OPP data pipeline ingestion and transformation"
}

resource "google_project_iam_member" "pipeline_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
