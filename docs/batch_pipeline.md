# Salesforce Batch Ingestion Pipeline

## Overview

The Salesforce Batch Ingestion Pipeline is an Apache Beam-based data pipeline that extracts data from a Salesforce REST API, validates and processes the data, and loads it into Google BigQuery for analytics.

## Architecture

```m
Salesforce API → Apache Beam (Dataflow) → BigQuery
                      ↓
                Cloud Storage (Parquet - future)
```

### Pipeline Flow

1. **Extract**: Fetches data from Salesforce REST API endpoints
2. **Validate**: Performs data quality checks on records
3. **Transform**: Formats data for BigQuery compatibility
4. **Load**: Writes data to BigQuery tables with schema auto-creation

## Components

### 1. Configuration (`config/salesforce_config.yaml`)

Defines:

- API endpoints for Salesforce objects (Account, Contact, Opportunity, Case)
- Data validation rules
- BigQuery table configurations
- Storage settings
- Performance parameters

### 2. Pipeline (`pipelines/salesforce_batch_extract.py`)

Main components:

- `ExtractSalesforceObject`: Extracts data from REST API
- `ValidateRecord`: Validates data quality
- `FormatForBigQuery`: Prepares data for BigQuery
- `get_bigquery_schema`: Defines BigQuery table schemas

### 3. Deployment Script (`scripts/deploy_batch_pipeline.sh`)

Automates:

- Dependency installation
- BigQuery dataset creation
- Dataflow job submission
- Cloud Scheduler configuration

## Prerequisites

### 1. GCP Setup

```bash
# Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# Set your project
export GCP_PROJECT_ID="your-project-id"
gcloud config set project $GCP_PROJECT_ID

# Enable required APIs
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable storage.googleapis.com
```

### 2. Service Account

Create a service account with required permissions:

```bash
# Create service account
gcloud iam service-accounts create dataflow-sa \
    --display-name="Dataflow Service Account"

# Grant necessary roles
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:dataflow-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:dataflow-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:dataflow-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

### 3. Cloud Storage Bucket

```bash
# Create bucket for Dataflow staging and temp files
export GCS_BUCKET="${GCP_PROJECT_ID}-dataflow"
gsutil mb -l us-central1 gs://$GCS_BUCKET

# Create subdirectories
gsutil mkdir gs://$GCS_BUCKET/temp
gsutil mkdir gs://$GCS_BUCKET/staging
gsutil mkdir gs://$GCS_BUCKET/salesforce
```

### 4. Python Dependencies

Install using UV (recommended) or pip:

```bash
# Using UV
uv sync --group core

# Or using pip
pip install apache-beam[gcp]>=2.54.0 \
    google-cloud-bigquery>=3.13.0 \
    google-cloud-storage>=2.14.0 \
    requests>=2.31.0 \
    pyyaml>=6.0.0 \
    pyarrow>=15.0.0
```

## Configuration

### Environment Variables

Create `infrastructure/dev/.env`:

```bash
# GCP Project Configuration
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"

# Storage Configuration
export GCS_BUCKET="your-bucket-name"
export GCS_TEMP_LOCATION="gs://your-bucket-name/temp"
export GCS_STAGING_LOCATION="gs://your-bucket-name/staging"

# BigQuery Configuration
export BQ_DATASET="salesforce_raw"

# Network Configuration
export SUBNETWORK="projects/your-project-id/regions/us-central1/subnetworks/default"

# Service Account
export SERVICE_ACCOUNT="dataflow-sa@your-project-id.iam.gserviceaccount.com"

# Dataflow Configuration
export DATAFLOW_NUM_WORKERS=2
export DATAFLOW_MAX_WORKERS=10
export DATAFLOW_MACHINE_TYPE="n1-standard-4"
export DATAFLOW_DISK_SIZE_GB=50
```

### Salesforce Configuration

Edit `config/salesforce_config.yaml` to customize:

- API endpoints and batch sizes
- Validation rules
- Storage paths
- BigQuery table settings
- Performance parameters

## Usage

### 1. Start the Salesforce API Simulator

```bash
# Generate synthetic data
python -c "
from src.salesforce.data_generator import SalesforceDataGenerator
gen = SalesforceDataGenerator()
accounts = gen.generate_accounts(100)
contacts = gen.generate_contacts(200, [a['id'] for a in accounts])
opportunities = gen.generate_opportunities(150, [a['id'] for a in accounts])
cases = gen.generate_cases(80, [a['id'] for a in accounts], [c['id'] for c in contacts])
gen.save_to_json(accounts, 'accounts')
gen.save_to_json(contacts, 'contacts')
gen.save_to_json(opportunities, 'opportunities')
gen.save_to_json(cases, 'cases')
"

# Start the API simulator (requires implementation)
# python src/salesforce/api_client.py
```

### 2. Deploy the Pipeline

#### Option A: Using the Deployment Script (Recommended)

```bash
# Deploy to dev environment
./scripts/deploy_batch_pipeline.sh dev

# Deploy to staging
./scripts/deploy_batch_pipeline.sh staging

# Deploy to production
./scripts/deploy_batch_pipeline.sh prod
```

#### Option B: Manual Deployment

```bash
# Run locally for testing
python pipelines/salesforce_batch_extract.py \
    --config=config/salesforce_config.yaml \
    --runner=DirectRunner

# Deploy to Dataflow
python pipelines/salesforce_batch_extract.py \
    --config=config/salesforce_config.yaml \
    --runner=DataflowRunner \
    --project=$GCP_PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://$GCS_BUCKET/temp \
    --staging_location=gs://$GCS_BUCKET/staging \
    --job_name=salesforce-batch-$(date +%Y%m%d-%H%M%S) \
    --service_account_email=$SERVICE_ACCOUNT \
    --num_workers=2 \
    --max_num_workers=10 \
    --machine_type=n1-standard-4 \
    --disk_size_gb=50 \
    --subnetwork=$SUBNETWORK \
    --no_use_public_ips
```

### 3. Monitor the Pipeline

```bash
# List Dataflow jobs
gcloud dataflow jobs list --region=us-central1

# Get job details
gcloud dataflow jobs describe <JOB_ID> --region=us-central1

# View logs
gcloud dataflow jobs show <JOB_ID> --region=us-central1 --logs

# Monitor in Console
# https://console.cloud.google.com/dataflow/jobs
```

### 4. Query the Data

```bash
# Check record counts
bq query --use_legacy_sql=false \
  'SELECT object_type, COUNT(*) as count 
   FROM `'$GCP_PROJECT_ID'.'$BQ_DATASET'.raw_*` 
   GROUP BY object_type'

# Query accounts
bq query --use_legacy_sql=false \
  'SELECT * FROM `'$GCP_PROJECT_ID'.'$BQ_DATASET'.raw_account` LIMIT 10'

# Check data quality
bq query --use_legacy_sql=false \
  'SELECT 
     object_type,
     COUNTIF(_is_valid) as valid_records,
     COUNTIF(NOT _is_valid) as invalid_records,
     ROUND(COUNTIF(_is_valid) / COUNT(*) * 100, 2) as quality_percent
   FROM `'$GCP_PROJECT_ID'.'$BQ_DATASET'.raw_*`
   GROUP BY object_type'
```

## BigQuery Schema

All tables include these common fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Salesforce record ID |
| `created_date` | TIMESTAMP | Record creation timestamp |
| `last_modified_date` | TIMESTAMP | Last modification timestamp |
| `system_modstamp` | TIMESTAMP | System modification stamp |
| `ingestion_timestamp` | TIMESTAMP | Pipeline ingestion time |
| `source` | STRING | Data source identifier |
| `object_type` | STRING | Salesforce object type |
| `_is_valid` | BOOLEAN | Data quality validation flag |
| `_validation_errors` | STRING (REPEATED) | List of validation errors |
| `_validation_timestamp` | TIMESTAMP | Validation execution time |

Plus object-specific fields for Account, Contact, Opportunity, and Case.

## Data Quality

The pipeline validates:

1. **Primary Key Checks**: Ensures all records have valid IDs
2. **Timestamp Validation**: Validates ISO 8601 format
3. **Foreign Key Validation**: Checks referential integrity (optional)
4. **Duplicate Detection**: Identifies duplicate IDs (optional)

Invalid records are flagged with `_is_valid = false` and include error details in `_validation_errors`.

## Scheduling

The deployment script automatically creates a Cloud Scheduler job:

- **Schedule**: Daily at 2 AM (configurable)
- **Timezone**: America/Los_Angeles
- **Retry Policy**: 3 attempts with exponential backoff

### Managing the Schedule

```bash
# List scheduler jobs
gcloud scheduler jobs list --location=us-central1

# Pause the schedule
gcloud scheduler jobs pause salesforce-batch-daily-dev --location=us-central1

# Resume the schedule
gcloud scheduler jobs resume salesforce-batch-daily-dev --location=us-central1

# Trigger manually
gcloud scheduler jobs run salesforce-batch-daily-dev --location=us-central1
```

## Performance Tuning

### Adjusting Worker Configuration

Edit `config/salesforce_config.yaml`:

```yaml
performance:
  num_workers: 4              # Starting workers
  max_num_workers: 20         # Maximum autoscale
  machine_type: "n1-standard-8"  # Larger machines
  disk_size_gb: 100           # More disk space
```

### Optimizing API Calls

Adjust batch sizes for each object:

```yaml
objects:
  - name: "Account"
    batch_size: 2000  # Increase for better throughput
```

## Troubleshooting

### Common Issues

1. **Permission Errors**

   ```bash
   # Check service account permissions
   gcloud projects get-iam-policy $GCP_PROJECT_ID \
       --flatten="bindings[].members" \
       --filter="bindings.members:serviceAccount:dataflow-sa@*"
   ```

2. **API Connection Failures**
   - Verify the API simulator is running
   - Check network connectivity
   - Review API endpoint configuration

3. **BigQuery Write Errors**
   - Verify dataset exists
   - Check schema compatibility
   - Review quota limits

4. **Out of Memory**
   - Increase worker machine type
   - Reduce batch sizes
   - Add more workers

### Viewing Logs

```bash
# Dataflow job logs
gcloud logging read "resource.type=dataflow_step" --limit=50

# Filter by job ID
gcloud logging read "resource.labels.job_id=<JOB_ID>" --limit=100
```

## Cost Optimization

- **Use Flex RS slots**: For scheduled, non-urgent jobs
- **Right-size workers**: Start small, monitor, then adjust
- **Use spot instances**: Set `use_public_ips=false` and enable spot VMs
- **Optimize batch sizes**: Larger batches = fewer API calls
- **Schedule off-peak**: Run during low-cost hours

## Next Steps

1. Implement the Streaming CDC Pipeline
2. Add Dataform transformations for analytics
3. Integrate Great Expectations for advanced data quality
4. Set up monitoring and alerting
5. Add cost tracking and optimization

## References

- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Dataflow Documentation](https://cloud.google.com/dataflow/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
