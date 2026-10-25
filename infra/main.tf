provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. GCS Bucket for Raw POS Ingestion & Silver Parquet Storage
resource "google_storage_bucket" "pos_data_lake" {
  name                        = "${var.project_id}-${var.data_lake_bucket}"
  location                    = var.region
  force_destroy               = false
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
      type = "Delete"
    }
  }

  labels = {
    environment = "production"
    team        = "data-engineering"
    application = "retailco-pos"
  }
}

# 2. BigQuery Analytical Warehouse Dataset
resource "google_bigquery_dataset" "warehouse_dataset" {
  dataset_id                 = var.dataset_id
  friendly_name              = "RetailCo POS Analytical Warehouse"
  description                = "Production BigQuery dataset hosting dimensional marts and fact tables."
  location                   = var.region
  delete_contents_on_destroy = false

  labels = {
    environment = "production"
    team        = "data-engineering"
  }
}

# 3. Cloud Pub/Sub Topic & Subscription for Real-Time Streaming
resource "google_pubsub_topic" "pos_events_topic" {
  name = var.pubsub_topic

  labels = {
    environment = "production"
    application = "retailco-streaming"
  }
}

resource "google_pubsub_subscription" "pos_events_sub" {
  name  = "${var.pubsub_topic}-sub"
  topic = google_pubsub_topic.pos_events_topic.name

  ack_deadline_seconds = 60
  retain_acked_messages = false

  expiration_policy {
    ttl = "2592000s" # 30 days
  }
}

# 4. Pipeline Service Account with Least-Privilege IAM
resource "google_service_account" "pipeline_sa" {
  account_id   = "retailco-pipeline-runner"
  display_name = "RetailCo Pipeline Execution Identity"
}

resource "google_project_iam_member" "sa_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "sa_bq_editor" {
  dataset_id = google_bigquery_dataset.warehouse_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_storage_bucket_iam_member" "sa_storage_admin" {
  bucket = google_storage_bucket.pos_data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
