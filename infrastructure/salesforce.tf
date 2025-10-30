# Salesforce-Specific BigQuery Tables - Phase 3 Component

# Salesforce Accounts Table (Raw)
resource "google_bigquery_table" "salesforce_accounts_raw" {
  dataset_id = google_bigquery_dataset.salesforce_raw.dataset_id
  table_id   = "accounts"
  project    = var.project_id

  description = "Raw Salesforce accounts data from API extraction"

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Salesforce Account ID"
    },
    {
      name = "name"
      type = "STRING"
      mode = "NULLABLE"
      description = "Account name"
    },
    {
      name = "type"
      type = "STRING"
      mode = "NULLABLE"
      description = "Account type (Prospect, Customer, Partner, etc.)"
    },
    {
      name = "industry"
      type = "STRING"
      mode = "NULLABLE"
      description = "Industry classification"
    },
    {
      name = "annual_revenue"
      type = "NUMERIC"
      mode = "NULLABLE"
      description = "Annual revenue"
    },
    {
      name = "phone"
      type = "STRING"
      mode = "NULLABLE"
      description = "Phone number"
    },
    {
      name = "website"
      type = "STRING"
      mode = "NULLABLE"
      description = "Website URL"
    },
    {
      name = "billing_address"
      type = "JSON"
      mode = "NULLABLE"
      description = "Billing address as JSON object"
    },
    {
      name = "shipping_address"
      type = "JSON"
      mode = "NULLABLE"
      description = "Shipping address as JSON object"
    },
    {
      name = "created_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Account creation timestamp"
    },
    {
      name = "last_modified_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Last modification timestamp"
    },
    {
      name = "system_modstamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "System modification timestamp"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Data ingestion timestamp"
    },
    {
      name = "source"
      type = "STRING"
      mode = "REQUIRED"
      description = "Data source (api/batch/streaming)"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = ["id", "type"]

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    table_type  = "salesforce_accounts_raw"
  })
}

# Salesforce Contacts Table (Raw)
resource "google_bigquery_table" "salesforce_contacts_raw" {
  dataset_id = google_bigquery_dataset.salesforce_raw.dataset_id
  table_id   = "contacts"
  project    = var.project_id

  description = "Raw Salesforce contacts data from API extraction"

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Salesforce Contact ID"
    },
    {
      name = "account_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Related Account ID"
    },
    {
      name = "first_name"
      type = "STRING"
      mode = "NULLABLE"
      description = "First name"
    },
    {
      name = "last_name"
      type = "STRING"
      mode = "NULLABLE"
      description = "Last name"
    },
    {
      name = "email"
      type = "STRING"
      mode = "NULLABLE"
      description = "Email address"
    },
    {
      name = "phone"
      type = "STRING"
      mode = "NULLABLE"
      description = "Phone number"
    },
    {
      name = "title"
      type = "STRING"
      mode = "NULLABLE"
      description = "Job title"
    },
    {
      name = "lead_source"
      type = "STRING"
      mode = "NULLABLE"
      description = "Lead source"
    },
    {
      name = "created_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Contact creation timestamp"
    },
    {
      name = "last_modified_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Last modification timestamp"
    },
    {
      name = "system_modstamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "System modification timestamp"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Data ingestion timestamp"
    },
    {
      name = "source"
      type = "STRING"
      mode = "REQUIRED"
      description = "Data source (api/batch/streaming)"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = ["id", "account_id"]

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    table_type  = "salesforce_contacts_raw"
  })
}

# Salesforce Opportunities Table (Raw)
resource "google_bigquery_table" "salesforce_opportunities_raw" {
  dataset_id = google_bigquery_dataset.salesforce_raw.dataset_id
  table_id   = "opportunities"
  project    = var.project_id

  description = "Raw Salesforce opportunities data from API extraction"

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Salesforce Opportunity ID"
    },
    {
      name = "account_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Related Account ID"
    },
    {
      name = "name"
      type = "STRING"
      mode = "NULLABLE"
      description = "Opportunity name"
    },
    {
      name = "stage_name"
      type = "STRING"
      mode = "NULLABLE"
      description = "Opportunity stage"
    },
    {
      name = "type"
      type = "STRING"
      mode = "NULLABLE"
      description = "Opportunity type (New Business, Existing Business, etc.)"
    },
    {
      name = "lead_source"
      type = "STRING"
      mode = "NULLABLE"
      description = "Lead source"
    },
    {
      name = "amount"
      type = "NUMERIC"
      mode = "NULLABLE"
      description = "Opportunity amount"
    },
    {
      name = "probability"
      type = "NUMERIC"
      mode = "NULLABLE"
      description = "Win probability percentage"
    },
    {
      name = "close_date"
      type = "DATE"
      mode = "NULLABLE"
      description = "Expected close date"
    },
    {
      name = "is_won"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether opportunity is won"
    },
    {
      name = "is_closed"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether opportunity is closed"
    },
    {
      name = "created_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Opportunity creation timestamp"
    },
    {
      name = "last_modified_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Last modification timestamp"
    },
    {
      name = "system_modstamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "System modification timestamp"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Data ingestion timestamp"
    },
    {
      name = "source"
      type = "STRING"
      mode = "REQUIRED"
      description = "Data source (api/batch/streaming)"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = ["id", "account_id", "stage_name"]

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    table_type  = "salesforce_opportunities_raw"
  })
}

# Salesforce Cases Table (Raw)
resource "google_bigquery_table" "salesforce_cases_raw" {
  dataset_id = google_bigquery_dataset.salesforce_raw.dataset_id
  table_id   = "cases"
  project    = var.project_id

  description = "Raw Salesforce cases data from API extraction"

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Salesforce Case ID"
    },
    {
      name = "account_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Related Account ID"
    },
    {
      name = "contact_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Related Contact ID"
    },
    {
      name = "subject"
      type = "STRING"
      mode = "NULLABLE"
      description = "Case subject"
    },
    {
      name = "description"
      type = "STRING"
      mode = "NULLABLE"
      description = "Case description"
    },
    {
      name = "status"
      type = "STRING"
      mode = "NULLABLE"
      description = "Case status (New, Working, Escalated, etc.)"
    },
    {
      name = "origin"
      type = "STRING"
      mode = "NULLABLE"
      description = "Case origin (Web, Email, Phone, etc.)"
    },
    {
      name = "priority"
      type = "STRING"
      mode = "NULLABLE"
      description = "Case priority (High, Medium, Low)"
    },
    {
      name = "is_escalated"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether case is escalated"
    },
    {
      name = "is_closed"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether case is closed"
    },
    {
      name = "closed_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
      description = "Case closure timestamp"
    },
    {
      name = "created_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Case creation timestamp"
    },
    {
      name = "last_modified_date"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Last modification timestamp"
    },
    {
      name = "system_modstamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "System modification timestamp"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Data ingestion timestamp"
    },
    {
      name = "source"
      type = "STRING"
      mode = "REQUIRED"
      description = "Data source (api/batch/streaming)"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = ["id", "account_id", "status"]

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    table_type  = "salesforce_cases_raw"
  })
}

# Salesforce History Tables for SCD Type 2
resource "google_bigquery_table" "salesforce_accounts_history" {
  dataset_id = google_bigquery_dataset.salesforce_raw.dataset_id
  table_id   = "accounts_history"
  project    = var.project_id

  description = "Salesforce accounts history for SCD Type 2 tracking"

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
      description = "Salesforce Account ID"
    },
    {
      name = "valid_from"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Validity start timestamp"
    },
    {
      name = "valid_to"
      type = "TIMESTAMP"
      mode = "NULLABLE"
      description = "Validity end timestamp (NULL for current)"
    },
    {
      name = "is_current"
      type = "BOOLEAN"
      mode = "REQUIRED"
      description = "Whether this is the current record"
    },
    {
      name = "change_type"
      type = "STRING"
      mode = "REQUIRED"
      description = "Type of change (INSERT, UPDATE, DELETE)"
    },
    {
      name = "changed_fields"
      type = "JSON"
      mode = "NULLABLE"
      description = "JSON object of changed fields"
    },
    {
      name = "record_data"
      type = "JSON"
      mode = "REQUIRED"
      description = "Complete record data as JSON"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Data ingestion timestamp"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = ["id", "valid_from"]

  labels = merge(var.labels, {
    environment = var.environment
    component   = "bigquery"
    table_type  = "salesforce_accounts_history"
  })
}

# Outputs
output "salesforce_accounts_raw_table_id" {
  description = "BigQuery table ID for raw Salesforce accounts"
  value       = google_bigquery_table.salesforce_accounts_raw.table_id
}

output "salesforce_contacts_raw_table_id" {
  description = "BigQuery table ID for raw Salesforce contacts"
  value       = google_bigquery_table.salesforce_contacts_raw.table_id
}

output "salesforce_opportunities_raw_table_id" {
  description = "BigQuery table ID for raw Salesforce opportunities"
  value       = google_bigquery_table.salesforce_opportunities_raw.table_id
}

output "salesforce_cases_raw_table_id" {
  description = "BigQuery table ID for raw Salesforce cases"
  value       = google_bigquery_table.salesforce_cases_raw.table_id
}

output "salesforce_accounts_history_table_id" {
  description = "BigQuery table ID for Salesforce accounts history"
  value       = google_bigquery_table.salesforce_accounts_history.table_id
}
