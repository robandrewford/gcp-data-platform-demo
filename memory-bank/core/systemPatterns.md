# System Patterns

## System Architecture

### Data Flow Architecture
```
Data Sources → Ingestion Layer → Processing Layer → Storage Layer → Analytics Layer
```

- **Ingestion Layer**: Pub/Sub for real-time, Cloud Storage for batch
- **Processing Layer**: Dataflow for stream/batch processing, Cloud Functions for event-driven
- **Storage Layer**: BigQuery for analytical data, Cloud Storage for raw data
- **Analytics Layer**: Looker Studio/BigQuery ML for visualization and ML

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
