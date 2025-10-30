# Production Environment Configuration
project_id = "your-gcp-project-id"  # Replace with your actual GCP project ID

environment = "prod"

# Resource naming
resource_name_prefix = "gcp-data-platform-demo"

# Networking
vpc_name    = "data-platform-prod-vpc"
subnet_cidr = "10.0.0.0/24"

# Storage
bucket_names = {
  data_lake = "data-lake"
  temp      = "temp-data"
  logs      = "pipeline-logs"
}

bucket_storage_class = "STANDARD"

# BigQuery
bigquery_dataset_name = "data_platform_demo_prod"
bigquery_location     = "US"  # Multi-region for production

# Pub/Sub
pubsub_topics = [
  "data-ingestion-events",
  "pipeline-status-updates",
  "data-quality-alerts"
]

pubsub_subscriptions = {
  "data-ingestion-events" = ["data-ingestion-sub"]
  "pipeline-status-updates" = ["pipeline-status-sub"]
  "data-quality-alerts" = ["data-quality-sub"]
}

# Labels
labels = {
  project     = "gcp-data-platform-demo"
  environment = "prod"
  managed_by  = "terraform"
  cost_center = "data-platform"
  criticality = "high"
}

# Service Account (most restrictive for production)
service_account_roles = [
  "roles/bigquery.dataEditor",
  "roles/storage.objectViewer",
  "roles/storage.objectCreator",
  "roles/dataflow.worker",
  "roles/pubsub.publisher",
  "roles/pubsub.subscriber",
  "roles/cloudfunctions.invoker",
  "roles/monitoring.viewer",
  "roles/logging.logWriter"
]
