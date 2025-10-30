# Service Account for Data Pipelines
resource "google_service_account" "data_pipeline_sa" {
  account_id   = var.service_account_name
  display_name = "Data Pipeline Service Account"
  description  = "Service account for GCP Data Platform Demo pipelines"
}

# IAM Roles for Service Account
resource "google_project_iam_member" "sa_roles" {
  for_each = toset(var.service_account_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.data_pipeline_sa.email}"

  depends_on = [google_service_account.data_pipeline_sa]
}

# VPC Network
resource "google_compute_network" "vpc" {
  name                    = var.vpc_name
  auto_create_subnetworks = false
  description             = "VPC for GCP Data Platform Demo"
}

# Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = "${var.resource_name_prefix}-subnet"
  network       = google_compute_network.vpc.name
  ip_cidr_range = var.subnet_cidr
  region        = var.region

  private_ip_google_access = true

  depends_on = [google_compute_network.vpc]
}

# Firewall Rules
resource "google_compute_firewall" "allow_internal" {
  name    = "${var.resource_name_prefix}-allow-internal"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = [var.subnet_cidr]
  description   = "Allow internal traffic within VPC"
}

# Cloud Storage Buckets
resource "google_storage_bucket" "buckets" {
  for_each = var.bucket_names

  name          = "${var.resource_name_prefix}-${var.environment}-${each.value}"
  location      = var.region
  storage_class = var.bucket_storage_class

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 30
    }
  }

  lifecycle_rule {
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 90
    }
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "storage"
    bucket_type = each.value
  })
}

# BigQuery Dataset
resource "google_bigquery_dataset" "main" {
  dataset_id    = var.bigquery_dataset_name
  friendly_name = "GCP Data Platform Demo Dataset"
  description   = "Main dataset for data platform demonstrations"
  location      = var.bigquery_location

  default_partition_expiration_ms = 2592000000  # 30 days
  default_table_expiration_ms     = 31536000000 # 1 year

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
  })
}

# Pub/Sub Topics
resource "google_pubsub_topic" "topics" {
  for_each = toset(var.pubsub_topics)

  name = "${var.resource_name_prefix}-${var.environment}-${each.value}"

  labels = merge(var.labels, {
    environment = var.environment
    component   = "pubsub"
  })

  message_retention_duration = "86400s"  # 24 hours
}

# Pub/Sub Subscriptions
resource "google_pubsub_subscription" "subscriptions" {
  for_each = merge([
    for topic_name, subs in var.pubsub_subscriptions : {
      for sub in subs : "${topic_name}_${sub}" => {
        topic = topic_name
        name  = sub
      }
    }
  ]...)

  name  = "${var.resource_name_prefix}-${var.environment}-${each.value.name}"
  topic = google_pubsub_topic.topics[each.value.topic].name

  ack_deadline_seconds = 60

  labels = merge(var.labels, {
    environment = var.environment
    component   = "pubsub"
    sub_type    = "subscription"
  })

  depends_on = [google_pubsub_topic.topics]
}

# Cloud Monitoring Alert Policies
resource "google_monitoring_alert_policy" "pipeline_failures" {
  display_name = "Data Pipeline Failures"
  combiner     = "OR"

  conditions {
    display_name = "Dataflow Job Failures"
    condition_threshold {
      filter          = "resource.type = \"dataflow_job\" AND metric.type = \"dataflow.googleapis.com/job/is_failed\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT_TRUE"
      }
    }
  }

  notification_channels = []

  documentation {
    content   = "Alert when Dataflow pipeline jobs fail"
    mime_type = "text/markdown"
  }

  user_labels = merge(var.labels, {
    environment = var.environment
    alert_type  = "pipeline_failure"
  })
}

# Outputs
output "service_account_email" {
  description = "Email of the created service account"
  value       = google_service_account.data_pipeline_sa.email
}

output "vpc_name" {
  description = "Name of the created VPC"
  value       = google_compute_network.vpc.name
}

output "subnet_name" {
  description = "Name of the created subnet"
  value       = google_compute_subnetwork.subnet.name
}

output "bucket_names" {
  description = "Names of the created Cloud Storage buckets"
  value       = [for bucket in google_storage_bucket.buckets : bucket.name]
}

output "bigquery_dataset_id" {
  description = "ID of the created BigQuery dataset"
  value       = google_bigquery_dataset.main.dataset_id
}

output "pubsub_topic_names" {
  description = "Names of the created Pub/Sub topics"
  value       = [for topic in google_pubsub_topic.topics : topic.name]
}

output "pubsub_subscription_names" {
  description = "Names of the created Pub/Sub subscriptions"
  value       = [for sub in google_pubsub_subscription.subscriptions : sub.name]
}
