# Data Pipeline Foundations - Phase 2 Components

# BigQuery Tables for Common Pipeline Patterns
resource "google_bigquery_table" "raw_events" {
  dataset_id = google_bigquery_dataset.main.dataset_id
  table_id   = "raw_events"
  project    = var.project_id

  description = "Raw event data table for ingestion pipelines"

  schema = jsonencode([
    {
      name = "event_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "data"
      type = "JSON"
      mode = "REQUIRED"
    },
    {
      name = "source"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "ingestion_time"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    table_type  = "raw_events"
  })
}

resource "google_bigquery_table" "transformed_data" {
  dataset_id = google_bigquery_dataset.main.dataset_id
  table_id   = "transformed_data"
  project    = var.project_id

  description = "Transformed data table for processed pipeline outputs"

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "data_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "processed_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "original_data"
      type = "JSON"
      mode = "REQUIRED"
    },
    {
      name = "transformed_data"
      type = "JSON"
      mode = "REQUIRED"
    },
    {
      name = "pipeline_version"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "quality_score"
      type = "FLOAT"
      mode = "NULLABLE"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  clustering = ["data_type", "pipeline_version"]

  labels = merge(var.labels, {
    environment   = var.environment
    component     = "bigquery"
    table_type    = "transformed_data"
  })
}

# Pub/Sub Topic Configurations for Pipeline Patterns
resource "google_pubsub_topic" "data_quality_alerts" {
  name = "${var.resource_name_prefix}-${var.environment}-data-quality-alerts"

  labels = merge(var.labels, {
    environment = var.environment
    component   = "pubsub"
    topic_type  = "alerts"
  })

  message_retention_duration = "604800s"  # 7 days for alerts

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_pubsub_topic" "pipeline_status" {
  name = "${var.resource_name_prefix}-${var.environment}-pipeline-status"

  labels = merge(var.labels, {
    environment = var.environment
    component   = "pubsub"
    topic_type  = "status"
  })

  message_retention_duration = "259200s"  # 3 days for status updates

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

# Additional Pub/Sub Topics for Common Pipeline Patterns
resource "google_pubsub_topic" "batch_ingestion" {
  name = "${var.resource_name_prefix}-${var.environment}-batch-ingestion"

  labels = merge(var.labels, {
    environment = var.environment
    component   = "pubsub"
    topic_type  = "ingestion"
  })

  message_retention_duration = "86400s"  # 24 hours
}

resource "google_pubsub_topic" "stream_processing" {
  name = "${var.resource_name_prefix}-${var.environment}-stream-processing"

  labels = merge(var.labels, {
    environment = var.environment
    component   = "pubsub"
    topic_type  = "processing"
  })

  message_retention_duration = "3600s"  # 1 hour for streaming
}

# Dataflow Service Account (separate from general data pipeline SA)
resource "google_service_account" "dataflow_sa" {
  account_id   = "${var.service_account_name}-dataflow"
  display_name = "Dataflow Pipeline Service Account"
  description  = "Service account specifically for Dataflow job execution"
}

resource "google_project_iam_member" "dataflow_sa_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Cloud Storage Bucket for Dataflow Templates
resource "google_storage_bucket" "dataflow_templates" {
  name     = "${var.resource_name_prefix}-${var.environment}-dataflow-templates"
  location = var.region

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90  # Keep templates for 90 days
    }
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "storage"
    bucket_type = "dataflow_templates"
  })
}

# Cloud Scheduler for Pipeline Automation
resource "google_cloud_scheduler_job" "daily_pipeline_trigger" {
  name        = "${var.resource_name_prefix}-${var.environment}-daily-pipeline"
  description = "Daily trigger for batch data processing pipeline"
  schedule    = "0 2 * * *"  # Daily at 2 AM

  pubsub_target {
    topic_name = google_pubsub_topic.pipeline_status.id
    data       = base64encode(jsonencode({
      action     = "start_pipeline"
      pipeline   = "daily_batch_processing"
      schedule   = "daily"
      timestamp  = timestamp()
    }))
  }

  depends_on = [google_pubsub_topic.pipeline_status]
}

# Outputs
output "bigquery_raw_events_table_id" {
  description = "BigQuery table ID for raw events"
  value       = google_bigquery_table.raw_events.table_id
}

output "bigquery_transformed_data_table_id" {
  description = "BigQuery table ID for transformed data"
  value       = google_bigquery_table.transformed_data.table_id
}

output "data_quality_alerts_topic" {
  description = "Pub/Sub topic for data quality alerts"
  value       = google_pubsub_topic.data_quality_alerts.name
}

output "pipeline_status_topic" {
  description = "Pub/Sub topic for pipeline status updates"
  value       = google_pubsub_topic.pipeline_status.name
}

output "batch_ingestion_topic" {
  description = "Pub/Sub topic for batch data ingestion"
  value       = google_pubsub_topic.batch_ingestion.name
}

output "stream_processing_topic" {
  description = "Pub/Sub topic for stream processing"
  value       = google_pubsub_topic.stream_processing.name
}

output "dataflow_service_account_email" {
  description = "Email of the Dataflow service account"
  value       = google_service_account.dataflow_sa.email
}

output "dataflow_templates_bucket" {
  description = "Cloud Storage bucket for Dataflow templates"
  value       = google_storage_bucket.dataflow_templates.name
}
