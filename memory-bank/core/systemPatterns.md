# System Patterns

## System Architecture

### Data Flow Architecture

```m
Data Sources → Ingestion Layer → Processing Layer → Storage Layer → Transformation Layer → Analytics Layer
```

- **Ingestion Layer**: Pub/Sub for real-time (CDC), Cloud Storage for batch (Parquet)
- **Processing Layer**: Apache Beam (Dataflow) for unified batch/streaming
- **Storage Layer**: BigQuery for analytical data, Cloud Storage for raw data (Parquet)
- **Transformation Layer**: Dataform for SQL transformations (dbt model patterns)
- **Analytics Layer**: Looker Studio/BigQuery ML for visualization and ML
- **Governance Layer**: Dataplex for catalog, Great Expectations for data quality

### Salesforce Data Architecture

```m
Salesforce REST API → Apache Beam → Cloud Storage (Parquet) → BigQuery → Dataform → Analytics
                    ↘ CDC → Pub/Sub → Apache Beam → BigQuery → Dataform → Analytics
```

- **Batch Path**: Daily/hourly extraction from Salesforce API → Parquet files → BigQuery raw tables
- **Streaming Path**: Change Data Capture → Pub/Sub → Real-time Beam pipeline → BigQuery streaming inserts
- **Transformation Path**: Dataform SQLX models adapting dbt Salesforce patterns
- **Quality Path**: Great Expectations in Beam pipelines → Dataplex governance rules

### Infrastructure Architecture

- Terraform-managed GCP resources
- Multi-environment setup (dev/staging/prod)
- Service account-based authentication
- Resource tagging for cost allocation

## Key Technical Decisions

### Data Pipeline Patterns

- **Event-Driven Processing**: Pub/Sub topics trigger Dataflow jobs
- **Batch Processing**: Scheduled Cloud Scheduler triggers Dataflow templates
- **Streaming Analytics**: Real-time windows and aggregations in Dataflow
- **Data Lake Architecture**: Raw data in Cloud Storage, processed data in BigQuery

### Security Patterns

- Least privilege IAM roles
- VPC Service Controls for data perimeter
- CMEK for encryption at rest
- Private Google Access for secure connectivity

### Cost Optimization Patterns

- BigQuery flat-rate pricing for predictable workloads
- Cloud Storage lifecycle policies for data tiering
- Auto-scaling Dataflow workers
- Resource labels for detailed cost analysis

## Design Patterns

### Data Processing Patterns

- **Extract-Transform-Load (ETL)**: Cloud Storage → Dataflow → BigQuery
- **Extract-Load-Transform (ELT)**: BigQuery Native SQL transformations
- **Streaming ETL**: Pub/Sub → Dataflow Streaming → BigQuery
- **Change Data Capture**: Cloud SQL → Pub/Sub → Dataflow → BigQuery

### Salesforce-Specific Patterns

- **Salesforce Batch Extraction**: REST API → Apache Beam → Parquet → BigQuery
- **Salesforce CDC**: Real-time events → Pub/Sub → Beam → BigQuery streaming
- **Salesforce History Tracking**: SCD Type 2 patterns in Dataform (dbt adaptation)
- **Salesforce Relationship Modeling**: Account-Contact-Opportunity joins in Dataform
- **Salesforce Data Quality**: Great Expectations for Salesforce-specific validations

### Infrastructure Patterns

- **Infrastructure as Code**: All GCP resources defined in Terraform
- **GitOps**: Infrastructure and application code in version control
- **CI/CD**: Automated testing and deployment pipelines
- **Immutable Infrastructure**: Container-based deployments

## Component Relationships

### Core Dependencies

- Dataflow depends on temporary Cloud Storage buckets
- BigQuery datasets reference Cloud Storage locations
- Pub/Sub topics connect to Dataflow pipelines
- Cloud Functions trigger data quality checks

### Critical Implementation Paths

1. **Data Ingestion Path**: External data → Cloud Storage/Pub/Sub → Data validation
2. **Processing Path**: Raw data → Dataflow transformation → Quality checks → BigQuery
3. **Analytics Path**: BigQuery tables → Scheduled queries → Dashboard updates
4. **Monitoring Path**: Cloud Logging → Cloud Monitoring → Alerts → Remediation
