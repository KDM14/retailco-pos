variable "project_id" {
  description = "The Google Cloud Platform project ID."
  type        = string
  default     = "dhanush-week6-2026"
}

variable "region" {
  description = "The GCP region for infrastructure deployment."
  type        = string
  default     = "asia-south1"
}

variable "data_lake_bucket" {
  description = "Primary GCS bucket for bronze raw POS data and silver Parquet outputs."
  type        = string
  default     = "retailco-pos-data-lake-prod"
}

variable "dataset_id" {
  description = "BigQuery dataset ID for production analytical marts."
  type        = string
  default     = "retailco_analytical_marts"
}

variable "pubsub_topic" {
  description = "Pub/Sub topic for real-time POS transaction events."
  type        = string
  default     = "retailco-transactions"
}
