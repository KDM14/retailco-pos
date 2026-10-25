output "bucket_name" {
  description = "GCS Data Lake Bucket Name"
  value       = google_storage_bucket.pos_data_lake.name
}

output "bigquery_dataset_id" {
  description = "BigQuery Analytical Dataset ID"
  value       = google_bigquery_dataset.warehouse_dataset.dataset_id
}

output "pubsub_topic_id" {
  description = "Pub/Sub Topic Name"
  value       = google_pubsub_topic.pos_events_topic.name
}

output "pipeline_service_account" {
  description = "Pipeline Service Account Email"
  value       = google_service_account.pipeline_sa.email
}
