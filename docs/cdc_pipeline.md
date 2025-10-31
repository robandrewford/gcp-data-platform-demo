# Salesforce CDC Streaming Pipeline

## Overview

The Salesforce CDC (Change Data Capture) streaming pipeline provides real-time data ingestion and processing of Salesforce data changes. It implements a dual-path architecture:

1. **Real-time Path**: Immediate ingestion of CDC events to BigQuery raw tables (10-second micro-batches)
2. **History Path**: Hourly batch processing for SCD Type 2 history tracking (1-hour windows)

## Architecture

```
Salesforce CDC Events
        ↓
    Pub/Sub Topic (stream-processing)
        ↓
    Pub/Sub Subscription
        ↓
┌───────────────────────┐
│  Parse & Validate     │  ← CDC Event Parser
│  CDC Events           │  ← CDC Validator
└───────────────────────┘
        ↓
    ┌────────┴────────┐
    ↓                 ↓
[Valid Events]   [Invalid Events]
    ↓                 ↓
Route by Object  Data Quality
Type (A/C/O/C)   Alerts Topic
    ↓
┌───────────┬───────────┐
│ Real-time │  History  │
│   Path    │   Path    │
└───────────┴───────────┘
    ↓            ↓
10-sec        1-hour
Windows      Windows
    ↓            ↓
BigQuery      SCD Type 2
Raw Tables   Processing
              ↓
         History Tables
```

## Components

### 1. CDC Event Parser (`ParseCDCEvent`)
- Parses JSON CDC events from Pub/Sub messages
- Handles malformed JSON gracefully with error logging
- Decodes message bytes to JSON objects

### 2. CDC Validator (`ValidateCDCEvent`)
- Validates event structure and data quality
- Checks:
  - Event type (INSERT/UPDATE/DELETE)
  - Object type (Account/Contact/Opportunity/Case)
  - Record ID format
  - Timestamp format and reasonableness
  - Required field presence
  - Field data types
  - Changed fields (for UPDATE events)
  - Foreign key references
- Routes events to valid/invalid outputs
- Tracks validation statistics

### 3. Record Extractor (`ExtractRecordForBigQuery`)
- Extracts record data from CDC events
- For INSERT/UPDATE: uses 'after' state
- For DELETE: uses 'before' state
- Adds metadata:
  - `ingestion_timestamp`
  - `source` (salesforce_cdc)
  - `_cdc_event_id`
  - `_cdc_event_type`
  - `_cdc_event_timestamp`
- Serializes complex fields (dict/list) to JSON strings

### 4. Object Type Router (`RouteByObjectType`)
- Routes events to object-specific processing paths
- Supports: Account, Contact, Opportunity, Case
- Logs warnings for unknown object types

### 5. SCD Type 2 Processor (`ProcessSCDType2Changes`)
- Processes hourly batches of CDC events
- Implements Slowly Changing Dimension Type 2:
  - Maintains full history of record changes
  - Tracks `valid_from` and `valid_to` timestamps
  - Sets `is_current` flag for active records
  - Records change types and changed fields
- Groups events by record ID
- Sorts chronologically for correct processing
- Closes previous records before inserting new versions

### 6. Data Quality Alert Generator (`CreateDataQualityAlert`)
- Creates structured alerts for validation failures
- Alert format:
  ```json
  {
    "alert_type": "data_quality_violation",
    "severity": "ERROR",
    "timestamp": "2024-01-01T12:00:00Z",
    "event_id": "CDC-ABC123...",
    "object_type": "Account",
    "record_id": "001...",
    "validation_errors": ["error1", "error2"],
    "event_data": { ... }
  }
  ```
- Publishes to `data-quality-alerts` Pub/Sub topic

## Configuration

### Environment Variables

Required variables (set in `infrastructure/<env>/.env`):

```bash
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
BQ_DATASET=salesforce_data
GCS_BUCKET=your-dataflow-bucket
```

### Pipeline Configuration

Edit `config/salesforce_cdc_config.yaml`:

#### Windowing
```yaml
windowing:
  realtime:
    window_duration: "10s"    # Micro-batch size
    allowed_lateness: "600s"  # 10 minutes
  history:
    window_duration: "3600s"  # 1 hour for SCD Type 2
    allowed_lateness: "600s"
```

#### Data Quality
```yaml
data_quality:
  enabled: true
  alert_threshold: 0.05  # Alert if >5% events fail
  validation_rules:
    - validate_event_type
    - validate_object_type
    - validate_record_id_format
    # ... more rules
```

#### BigQuery Tables
```yaml
bigquery:
  raw_tables:
    Account: "raw_accounts"
    Contact: "raw_contacts"
    Opportunity: "raw_opportunities"
    Case: "raw_cases"
  
  history_tables:
    Account: "accounts_history"
    Contact: "contacts_history"
    Opportunity: "opportunities_history"
    Case: "cases_history"
```

## Deployment

### Prerequisites

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Ensure GCP resources exist:
   - Pub/Sub topics and subscriptions
   - BigQuery dataset
   - GCS bucket for staging/temp
   - Service account with appropriate permissions

### Deploy to Development

```bash
./scripts/deploy_cdc_pipeline.sh dev
```

### Deploy to Production

```bash
./scripts/deploy_cdc_pipeline.sh prod
```

### Create Dataflow Template

```bash
./scripts/deploy_cdc_pipeline.sh prod --template
```

### Dry Run (Preview)

```bash
./scripts/deploy_cdc_pipeline.sh dev --dry-run
```

## Monitoring

### Key Metrics

1. **Throughput**:
   - Events received per second
   - Events processed per second
   - Records inserted per second

2. **Latency**:
   - Event ingestion latency
   - Processing latency
   - End-to-end latency

3. **Data Quality**:
   - Validation success rate
   - Error rate by type
   - Alert frequency

4. **SCD Type 2**:
   - History updates per hour
   - Record versions created
   - Processing batch size

### Cloud Monitoring Queries

```sql
-- Event throughput
fetch dataflow_job
| metric 'dataflow.job/user_counter'
| filter resource.job_name =~ 'salesforce-cdc-.*'
| group_by 1m, [value_user_counter_mean: mean(value.user_counter)]

-- Data quality error rate
fetch dataflow_job
| metric 'dataflow.job/user_counter'
| filter resource.job_name =~ 'salesforce-cdc-.*'
    && metric.metric_name = 'validation_errors'
```

### Logs

View pipeline logs in Cloud Logging:

```
resource.type="dataflow_step"
resource.labels.job_name=~"salesforce-cdc-.*"
severity>=ERROR
```

## Data Flow

### Real-time Path

1. Event arrives at Pub/Sub subscription
2. Parse JSON event (10ms)
3. Validate event structure (5ms)
4. Extract record data (2ms)
5. Route by object type (1ms)
6. Window into 10-second micro-batches
7. Stream insert to BigQuery raw table (100-500ms)

**Total latency**: ~1-2 seconds

### History Path

1. Event arrives at Pub/Sub subscription
2. Parse and validate (same as real-time)
3. Route by object type
4. Window into 1-hour batches
5. Group events by record ID
6. Process SCD Type 2 logic:
   - Get current record from history table
   - Detect changes
   - Close current record (set `valid_to`, `is_current=FALSE`)
   - Insert new history record
7. Batch insert to BigQuery history table

**Processing**: Hourly at window close

## BigQuery Schema

### Raw Tables

```sql
CREATE TABLE raw_accounts (
  -- Salesforce fields
  id STRING,
  name STRING,
  type STRING,
  industry STRING,
  annual_revenue INT64,
  phone STRING,
  -- ... more fields
  
  -- CDC metadata
  ingestion_timestamp TIMESTAMP,
  source STRING,
  _cdc_event_id STRING,
  _cdc_event_type STRING,
  _cdc_event_timestamp TIMESTAMP
);
```

### History Tables (SCD Type 2)

```sql
CREATE TABLE accounts_history (
  -- Salesforce fields
  id STRING,
  name STRING,
  type STRING,
  -- ... more fields
  
  -- SCD Type 2 fields
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  change_type STRING,  -- INSERT, UPDATE, DELETE
  changed_fields ARRAY<STRING>,
  
  -- CDC metadata
  _cdc_event_id STRING,
  _cdc_event_timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP
);
```

## Troubleshooting

### High Error Rate

1. Check validation errors:
   ```bash
   gcloud pubsub subscriptions pull data-quality-alerts-sub --limit=10
   ```

2. Review validation statistics in logs

3. Adjust validation rules if needed

### Processing Lag

1. Check Dataflow autoscaling:
   ```bash
   gcloud dataflow jobs describe <JOB_ID>
   ```

2. Increase max workers if needed

3. Review worker CPU/memory usage

### SCD Type 2 Issues

1. Check BigQuery history table permissions
2. Verify CDC events have required fields
3. Review `process_hourly_changes` logs
4. Check for BigQuery quota limits

### Dead Letter Queue

Failed events are published to `cdc-dead-letter` topic:

```bash
gcloud pubsub subscriptions pull cdc-dead-letter-sub --limit=10
```

## Performance Tuning

### Dataflow Settings

```bash
--num_workers=5                    # Initial workers
--max_num_workers=20               # Maximum autoscaling
--machine_type=n1-standard-4       # Worker machine type
--disk_size_gb=50                  # Worker disk size
--autoscaling_algorithm=THROUGHPUT_BASED
```

### BigQuery Optimization

1. **Partition** raw tables by `ingestion_timestamp`
2. **Cluster** by `id` for faster lookups
3. **Partition** history tables by `valid_from`
4. **Cluster** by `id, is_current`

### Pub/Sub Tuning

```yaml
pubsub:
  ack_deadline_seconds: 60  # Processing time allowance
  max_messages: 1000        # Batch size
```

## Best Practices

1. **Monitor data quality metrics** continuously
2. **Set up alerts** for error rate thresholds
3. **Review dead letter queue** regularly
4. **Optimize BigQuery costs** with partitioning/clustering
5. **Test configuration changes** in dev first
6. **Use templates** for production deployments
7. **Version control** pipeline configuration
8. **Document schema changes** in history tables

## Related Documentation

- [Batch Pipeline Documentation](batch_pipeline.md)
- [Salesforce Data Generator](../src/salesforce/data_generator.py)
- [CDC Event Simulator](../src/salesforce/cdc_event_simulator.py)
- [SCD Type 2 Handler](../pipelines/utils/scd_type2_handler.py)
- [CDC Validators](../pipelines/utils/cdc_validators.py)
