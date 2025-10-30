# Dataplex Infrastructure - Phase 3 Component

# Dataplex Lake for Salesforce Data
resource "google_dataplex_lake" "salesforce_lake" {
  provider = google-beta
  project  = var.project_id
  location = var.region
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-lake"
  display_name = "Salesforce Data Lake - ${var.environment}"

  description = "Central data lake for Salesforce data processing and analytics"

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    lake        = "salesforce"
  })
}

# Dataplex Zone for Raw Data
resource "google_dataplex_zone" "salesforce_raw_zone" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_lake.salesforce_lake.location
  lake     = google_dataplex_lake.salesforce_lake.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-raw"
  display_name = "Salesforce Raw Zone - ${var.environment}"
  type     = "RAW"

  description = "Raw Salesforce data from API extraction and CDC events"

  # Resource specifications for raw zone
  resource_specs {
    location = var.region
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    zone        = "salesforce_raw"
    data_quality = "validation"
  })
}

# Dataplex Zone for Staging Data
resource "google_dataplex_zone" "salesforce_staging_zone" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_lake.salesforce_lake.location
  lake     = google_dataplex_lake.salesforce_lake.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-staging"
  display_name = "Salesforce Staging Zone - ${var.environment}"
  type     = "CURATED"

  description = "Staging area for transformed Salesforce data"

  # Resource specifications for staging zone
  resource_specs {
    location = var.region
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    zone        = "salesforce_staging"
    data_quality = "validated"
  })
}

# Dataplex Zone for Analytics Data
resource "google_dataplex_zone" "salesforce_analytics_zone" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_lake.salesforce_lake.location
  lake     = google_dataplex_lake.salesforce_lake.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-analytics"
  display_name = "Salesforce Analytics Zone - ${var.environment}"
  type     = "CURATED"

  description = "Analytics-ready Salesforce data for consumption"

  # Resource specifications for analytics zone
  resource_specs {
    location = var.region
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    zone        = "salesforce_analytics"
    data_quality = "validated"
  })
}

# Dataplex Asset for Cloud Storage Raw Data
resource "google_dataplex_asset" "salesforce_raw_storage" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_zone.salesforce_raw_zone.location
  lake     = google_dataplex_lake.salesforce_lake.name
  zone     = google_dataplex_zone.salesforce_raw_zone.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-raw-storage"
  display_name = "Salesforce Raw Storage - ${var.environment}"

  description = "Cloud Storage bucket for raw Salesforce data"

  resource_spec {
    name = google_storage_bucket.data_lake.name
    type = "STORAGE_BUCKET"
  }

  discovery_spec {
    enabled = true
    include_patterns = ["**/*.parquet", "**/*.json"]
    exclude_patterns = ["_temp/**", "_tmp/**"]
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    asset       = "salesforce_raw_storage"
  })
}

# Dataplex Asset for BigQuery Raw Dataset
resource "google_dataplex_asset" "salesforce_raw_bigquery" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_zone.salesforce_raw_zone.location
  lake     = google_dataplex_lake.salesforce_lake.name
  zone     = google_dataplex_zone.salesforce_raw_zone.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-raw-bigquery"
  display_name = "Salesforce Raw BigQuery - ${var.environment}"

  description = "BigQuery dataset for raw Salesforce data"

  resource_spec {
    name = google_bigquery_dataset.salesforce_raw.dataset_id
    type = "BIGQUERY_DATASET"
  }

  discovery_spec {
    enabled = true
    include_patterns = ["**"]
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    asset       = "salesforce_raw_bigquery"
  })
}

# Dataplex Asset for BigQuery Staging Dataset
resource "google_dataplex_asset" "salesforce_staging_bigquery" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_zone.salesforce_staging_zone.location
  lake     = google_dataplex_lake.salesforce_lake.name
  zone     = google_dataplex_zone.salesforce_staging_zone.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-staging-bigquery"
  display_name = "Salesforce Staging BigQuery - ${var.environment}"

  description = "BigQuery dataset for staged Salesforce data"

  resource_spec {
    name = google_bigquery_dataset.salesforce_staging.dataset_id
    type = "BIGQUERY_DATASET"
  }

  discovery_spec {
    enabled = true
    include_patterns = ["**"]
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    asset       = "salesforce_staging_bigquery"
  })
}

# Dataplex Asset for BigQuery Analytics Dataset
resource "google_dataplex_asset" "salesforce_analytics_bigquery" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_zone.salesforce_analytics_zone.location
  lake     = google_dataplex_lake.salesforce_lake.name
  zone     = google_dataplex_zone.salesforce_analytics_zone.name
  name     = "${var.resource_name_prefix}-${var.environment}-salesforce-analytics-bigquery"
  display_name = "Salesforce Analytics BigQuery - ${var.environment}"

  description = "BigQuery dataset for analytics-ready Salesforce data"

  resource_spec {
    name = google_bigquery_dataset.salesforce.dataset_id
    type = "BIGQUERY_DATASET"
  }

  discovery_spec {
    enabled = true
    include_patterns = ["**"]
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    asset       = "salesforce_analytics_bigquery"
  })
}

# Dataplex Data Quality Rule - Schema Validation
resource "google_dataplex_data_attribute" "salesforce_schema_validation" {
  provider = google-beta
  project  = var.project_id
  location = google_dataplex_lake.salesforce_lake.location
  lake     = google_dataplex_lake.salesforce_lake.name
  name     = "${var.resource_name_prefix}-${var.environment}-schema-validation"
  display_name = "Salesforce Schema Validation - ${var.environment}"

  description = "Data quality rule for Salesforce schema validation"

  attribute_type = "DATA_QUALITY_SPEC"

  data_quality_spec {
    rules {
      name = "required_fields_check"
      description = "Check for required Salesforce fields"
      type = "ROW_CONDITION"
      dimension = "COMPLETENESS"
      threshold {
        value = 0.95
        strict = true
      }
      sql_expression = "event_id IS NOT NULL AND timestamp IS NOT NULL"
    }

    rules {
      name = "data_type_validation"
      description = "Validate data types for Salesforce fields"
      type = "ROW_CONDITION"
      dimension = "VALIDITY"
      threshold {
        value = 0.98
        strict = true
      }
      sql_expression = "REGEXP_CONTAINS(event_id, '^[a-zA-Z0-9_-]+$')"
    }
  }

  labels = merge(var.labels, {
    environment = var.environment
    component   = "dataplex"
    rule        = "schema_validation"
  })
}

# Outputs
output "dataplex_lake_id" {
  description = "Dataplex lake ID"
  value       = google_dataplex_lake.salesforce_lake.id
}

output "salesforce_raw_zone_id" {
  description = "Dataplex raw zone ID"
  value       = google_dataplex_zone.salesforce_raw_zone.id
}

output "salesforce_staging_zone_id" {
  description = "Dataplex staging zone ID"
  value       = google_dataplex_zone.salesforce_staging_zone.id
}

output "salesforce_analytics_zone_id" {
  description = "Dataplex analytics zone ID"
  value       = google_dataplex_zone.salesforce_analytics_zone.id
}
