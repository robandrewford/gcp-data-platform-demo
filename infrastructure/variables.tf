variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Default GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Default GCP zone for resources"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "labels" {
  description = "Common labels to apply to all resources"
  type        = map(string)
  default     = {}
}

# Service account configuration
variable "service_account_name" {
  description = "Name of the service account for data pipelines"
  type        = string
  default     = "data-pipeline-sa"
}

variable "service_account_roles" {
  description = "IAM roles to assign to the service account"
  type        = list(string)
  default = [
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/dataflow.admin",
    "roles/pubsub.editor",
    "roles/cloudfunctions.invoker",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter"
  ]
}

# Resource naming
variable "resource_name_prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "gcp-data-platform-demo"
}

# Networking
variable "vpc_name" {
  description = "Name of the VPC network"
  type        = string
  default     = "data-platform-vpc"
}

variable "subnet_cidr" {
  description = "CIDR block for the subnet"
  type        = string
  default     = "10.0.0.0/24"
}

# Storage
variable "bucket_names" {
  description = "Names of Cloud Storage buckets to create"
  type        = map(string)
  default = {
    data_lake = "data-lake"
    temp      = "temp-data"
    logs      = "pipeline-logs"
  }
}

variable "bucket_storage_class" {
  description = "Storage class for buckets"
  type        = string
  default     = "STANDARD"
}

variable "bucket_lifecycle_rules" {
  description = "Lifecycle rules for buckets"
  type = map(object({
    age                   = number
    storage_class         = string
    type                  = string
  }))
  default = {
    temp_cleanup = {
      age           = 7
      storage_class = "ARCHIVE"
      type          = "SetStorageClass"
    }
    log_cleanup = {
      age           = 30
      storage_class = "ARCHIVE"
      type          = "SetStorageClass"
    }
  }
}

# BigQuery
variable "bigquery_dataset_name" {
  description = "Name of the BigQuery dataset"
  type        = string
  default     = "data_platform_demo"
}

variable "bigquery_location" {
  description = "Location for BigQuery dataset"
  type        = string
  default     = "US"
}

# Pub/Sub
variable "pubsub_topics" {
  description = "Pub/Sub topics to create"
  type        = list(string)
  default = [
    "data-ingestion-events",
    "pipeline-status-updates",
    "data-quality-alerts"
  ]
}

variable "pubsub_subscriptions" {
  description = "Pub/Sub subscriptions to create for topics"
  type        = map(list(string))
  default = {
    "data-ingestion-events" = ["data-ingestion-sub"]
    "pipeline-status-updates" = ["pipeline-status-sub"]
    "data-quality-alerts" = ["data-quality-sub"]
  }
}

# Dataflow
variable "dataflow_temp_bucket" {
  description = "GCS bucket for Dataflow temporary files"
  type        = string
  default     = "dataflow-temp"
}

variable "dataflow_staging_bucket" {
  description = "GCS bucket for Dataflow staging files"
  type        = string
  default     = "dataflow-staging"
}
