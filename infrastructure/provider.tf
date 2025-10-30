terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.63"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.63"
    }
  }

  backend "gcs" {
    bucket = "gcp-data-platform-demo-tf-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "composer.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "dataproc.googleapis.com",
    "datastream.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "storage-api.googleapis.com",
    "storage-component.googleapis.com",
    "vpcaccess.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com"
  ])

  service            = each.value
  disable_on_destroy = false
}
