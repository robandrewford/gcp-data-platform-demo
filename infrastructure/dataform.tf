# Dataform Infrastructure - Phase 3 Component

# Dataform Repository Configuration
resource "google_dataform_repository" "salesforce_repo" {
  provider      = google-beta
  project       = var.project_id
  region        = var.region
  name          = "${var.resource_name_prefix}-${var.environment}-salesforce-dataform"
  display_name   = "Salesforce Dataform Repository - ${var.environment}"

  git_remote_settings {
    url                 = "https://github.com/robandrewford/gcp-data-platform-demo.git"
    default_branch       = "main"
    authentication_token_secret_version = data.google_secret_manager_secret_version.github_token.id
  }

  workspace_ci_settings {
    default_branch = "main"
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataform"
    repository  = "salesforce"
  })
}

# GitHub Token Secret for Dataform Repository Access
data "google_secret_manager_secret_version" "github_token" {
  secret = "github-access-token"
}

# Dataform Workspace Configuration
resource "google_dataform_workspace" "salesforce_workspace" {
  provider      = google-beta
  project       = var.project_id
  region        = var.region
  name          = "${var.resource_name_prefix}-${var.environment}-salesforce-workspace"
  repository_id  = google_dataform_repository.salesforce_repo.id
  display_name   = "Salesforce Workspace - ${var.environment}"

  # BigQuery Connection Settings
  bigquery_connection {
    project_id = var.project_id
    dataset_id = google_bigquery_dataset.salesforce_raw.dataset_id
  }

  # Workspace Settings
  git_commit_settings {
    author_name  = "Data Platform Bot"
    author_email = "data-platform-bot@example.com"
  }

  # Schedule for automatic runs
  release_config {
    code_compilation_settings {
      default_schema = "salesforce"
    }
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataform"
    workspace   = "salesforce"
  })
}

# Dataform Service Account
resource "google_service_account" "dataform_sa" {
  account_id   = "${var.service_account_name}-dataform"
  display_name = "Dataform Service Account"
  description  = "Service account for Dataform repository and workspace operations"
}

# IAM Bindings for Dataform Service Account
resource "google_project_iam_member" "dataform_sa_roles" {
  for_each = toset([
    "roles/dataform.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/secretmanager.secretAccessor"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataform_sa.email}"
}

# BigQuery Dataset for Salesforce Raw Data
resource "google_bigquery_dataset" "salesforce_raw" {
  dataset_id  = "salesforce_raw"
  project     = var.project_id
  description = "Raw Salesforce data extracted from API and CDC events"
  location    = var.region

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    dataset     = "salesforce_raw"
  })

  access {
    role          = "roles/bigquery.dataEditor"
    user_by_email = google_service_account.dataflow_sa.email
  }

  access {
    role          = "roles/bigquery.dataEditor"
    user_by_email = google_service_account.dataform_sa.email
  }
}

# BigQuery Dataset for Salesforce Staging Data
resource "google_bigquery_dataset" "salesforce_staging" {
  dataset_id  = "salesforce_staging"
  project     = var.project_id
  description = "Staging area for Salesforce data transformations"
  location    = var.region

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    dataset     = "salesforce_staging"
  })

  access {
    role          = "roles/bigquery.dataEditor"
    user_by_email = google_service_account.dataform_sa.email
  }
}

# BigQuery Dataset for Salesforce Analytics Data
resource "google_bigquery_dataset" "salesforce" {
  dataset_id  = "salesforce"
  project     = var.project_id
  description = "Analytics-ready Salesforce data mart"
  location    = var.region

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    dataset     = "salesforce"
  })

  access {
    role          = "roles/bigquery.dataEditor"
    user_by_email = google_service_account.dataform_sa.email
  }
}

# Outputs
output "dataform_repository_id" {
  description = "Dataform repository ID"
  value       = google_dataform_repository.salesforce_repo.id
}

output "dataform_workspace_id" {
  description = "Dataform workspace ID"
  value       = google_dataform_workspace.salesforce_workspace.id
}

output "dataform_service_account_email" {
  description = "Email of the Dataform service account"
  value       = google_service_account.dataform_sa.email
}

output "salesforce_raw_dataset_id" {
  description = "BigQuery dataset ID for raw Salesforce data"
  value       = google_bigquery_dataset.salesforce_raw.dataset_id
}

output "salesforce_staging_dataset_id" {
  description = "BigQuery dataset ID for staging Salesforce data"
  value       = google_bigquery_dataset.salesforce_staging.dataset_id
}

output "salesforce_dataset_id" {
  description = "BigQuery dataset ID for Salesforce analytics"
  value       = google_bigquery_dataset.salesforce.dataset_id
}
