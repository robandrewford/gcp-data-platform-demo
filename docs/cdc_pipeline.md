# Salesforce CDC Streaming Pipeline

## Overview

The Salesforce CDC (Change Data Capture) streaming pipeline processes real-time change events from Salesforce objects, performs data quality validation, and writes to BigQuery with support for SCD Type 2 history tracking.

## Architecture

```
CDC Events → Pub/Sub → Apache Beam Pipeline → BigQuery
                              ↓
                       Data Quality Validation
                              ↓
                    ┌─────────┴─────────┐
                    ↓                   ↓
            Valid Events         Invalid Events
                    ↓                   ↓
            BigQuery Tables      Alert Topic
            (Real-time)
                    ↓
            History Tables
            (Hourly Batch - SCD Type 2)
```

## Components

### 1. CDC Event Simulator
**File:** `src/salesforce/cdc_event_simulator.py`

Generates synthetic CDC events for testing:
- INSERT, UPDATE, DELETE operations
- Before/after snapshots
- Changed fields tracking
- Realistic Salesforce data

**Usage:**
```python
from src.salesforce.cdc_event_simulator import CDCEventSimulator

simulator = CDCEventSimulator()
simulator.preload_existing_records('Account', 50)
events = simulator.generate_cdc_events('Account', 100)
```

**CLI:**
```bash
python -m src.salesforce.cdc_event_simulator \
    --object-type Account \
    --count 100 \
    --preload 50 \
    --output cdc_events.json
```

### 2. CDC Event Publisher
**File:** `src/salesforce/cdc_publisher.py`

Publishes CDC events to Pub/Sub:
- Batch publishing support
- Rate limiting
- Retry logic
- Statistics tracking

**Usage:**
```python
from src.salesforce.cdc_publisher import CDCEventPublisher

publisher = CDCEventPublisher(
    project_id='my-project',
    topic_name='stream-processing'
)
publisher.publish_events(events)
```

**CLI:**
```bash
python -m src.salesforce.cdc_publisher \
    --project-id my-project \
    --topic-name stream-processing \
    --object-type Account \
    --count 100 \
    --continuous
```

### 3. CDC Validators
**File:** `pipelines/utils/cdc_validators.py`

Data quality validation rules:
- Event type validation (INSERT/UPDATE/DELETE)
- Object type validation (Account/Contact/Opportunity/Case)
- Record ID format validation (Salesforce ID pattern)
- Timestamp format validation (ISO 8601)
- Null checks for required fields
- Field type validation
- Changed fields validation (UPDATE events)
- Foreign key validation

**Usage:**
```python
from pipelines.utils.cdc_validators import CDCValidator

validator = CDCValidator(alert_threshold=0.05)
is_valid, errors = validator.validate_event(event)
```

### 4. CDC Event Parser
**File:** `pipelines/utils/cdc_event_parser.py`

Parses and extracts data from CDC events:
- Pub/Sub message parsing
- Record extraction for BigQuery
- History record extraction
- Field normalization

**Usage:**
```python
from pipelines.utils.cdc_event_parser import CDCEventParser

parser = CDCEventParser()
event = parser.parse_pubsub_message(message)
record = parser.extract_record_for_bigquery(event)
```

### 5. SCD Type 2 Handler
**File:** `pipelines/utils/scd_type2_handler.py`

Implements Slowly Changing Dimension Type 2 pattern:
- Change detection
- History table updates
- valid_from/valid_to management
- is_current flag tracking
- Hourly batch processing

**Usage:**
```python
from pipelines.utils.scd_type2_handler import SCDType2Handler
from google.cloud import bigquery

client = bigquery.Client()
handler = SCDType2Handler(client, project_id, dataset_id)
stats = handler.process_hourly_changes(events, 'Account')
```

### 6. Streaming Pipeline
**File:** `pipelines/salesforce_streaming_cdc.py`

Main Apache Beam streaming pipeline:
- Reads from Pub/Sub
- Validates events
- Routes by object type
- Real-time BigQuery writes
- Data quality alerting

## Configuration

### CDC Pipeline Configuration
**File:** `config/salesforce_cdc_config.yaml`

Key configuration sections:
- **Pub/Sub:** Topic, subscription, message settings
- **Windowing:** Real-time (10s) and history (1h) windows
- **Data Quality:** Validation rules, alert threshold
- **BigQuery:** Tables, write settings, streaming config
- **Performance:** Worker settings, autoscaling
- **Monitoring:** Metrics, alerts, log levels
- **Error Handling:** Dead letter queue, retry logic

## Data Flow

### Real-time Path

1. **Event Reception**
   - CDC events arrive via Pub/Sub
   - 10-second windowing for micro-batching

2. **Validation**
   - Type checks (event_type, object_type)
   - Null checks (required fields)
   - Format validation (timestamps, IDs)
   - Foreign key validation

3. **Routing**
   - Events routed by object_type
   - Separate processing per object

4. **BigQuery Write**
   - Streaming inserts to raw tables
   - Metadata addition (ingestion_timestamp, source, etc.)
   - Complex fields converted to JSON

### History Path (Hourly)

1. **Event Collection**
   - 1-hour fixed windows
   - Events grouped by record_id

2. **Change Detection**
   - Compare before/after states
   - Identify changed fields
   - Skip unchanged records

3. **History Updates**
   - Close current records (set valid_to, is_current=false)
   - Insert new history records (valid_from=event_timestamp)
   - Maintain audit trail

## CDC Event Format

```json
{
  "event_id": "CDC-ABC123XYZ789",
  "event_type": "UPDATE",
  "object_type": "Account",
  "record_id": "001XXXXXXXXXXXXXXX",
  "event_timestamp": "2025-10-30T15:30:00Z",
  "changed_fields": ["name", "annual_revenue"],
  "before": {
    "id": "001XXXXXXXXXXXXXXX",
    "name": "Acme Corp",
    "annual_revenue": 1000000,
    "created_date": "2025-01-15T10:00:00Z",
    "last_modified_date": "2025-10-30T15:29:00Z"
  },
  "after": {
    "id": "001XXXXXXXXXXXXXXX",
    "name": "Acme Corporation",
    "annual_revenue": 1200000,
    "created_date": "2025-01-15T10:00:00Z",
    "last_modified_date": "2025-10-30T15:30:00Z"
  },
  "source": "salesforce_cdc"
}
```

## Deployment

### Prerequisites

1. **GCP Resources**
   - Pub/Sub topic: `stream-processing`
   - Pub/Sub subscription: `salesforce-cdc-subscription`
   - BigQuery dataset with raw and history tables
   - Cloud Storage bucket for staging/temp
   - VPC subnetwork
   - Service account with permissions

2. **Environment Variables**
   ```bash
   export GCP_PROJECT_ID="my-project"
   export BQ_DATASET="salesforce_raw"
   export GCS_BUCKET="my-dataflow-bucket"
   export SUBNETWORK="projects/my-project/regions/us-central1/subnetworks/my-subnet"
   ```

### Deploy Pipeline

```bash
# Deploy to development
./scripts/deploy_cdc_pipeline.sh dev

# Deploy to production
./scripts/deploy_cdc_pipeline.sh prod
```

### Manual Deployment

```bash
python pipelines/salesforce_streaming_cdc.py \
    --config=config/salesforce_cdc_config.yaml \
    --runner=DataflowRunner \
    --project=my-project \
    --region=us-central1 \
    --job_name=salesforce-cdc-streaming \
    --staging_location=gs://my-bucket/staging \
    --temp_location=gs://my-bucket/temp \
    --num_workers=2 \
    --max_num_workers=10 \
    --streaming
```

## Testing

### Generate and Publish Test Events

```bash
# Generate test events
python -m src.salesforce.cdc_event_simulator \
    --object-type Account \
    --count 1000 \
    --preload 100 \
    --output test_events.json

# Publish to Pub/Sub
python -m src.salesforce.cdc_publisher \
    --project-id my-project \
    --topic-name stream-processing \
    --object-type Account \
    --count 1000 \
    --rate-limit 100
```

### Continuous Testing

```bash
# Continuous event generation
python -m src.salesforce.cdc_publisher \
    --project-id my-project \
    --topic-name stream-processing \
    --object-type Account \
    --count 100 \
    --continuous \
    --interval 10
```

## Monitoring

### Pipeline Metrics

1. **Event Throughput**
   - Events received per second
   - Events processed per second
   - Processing latency

2. **Data Quality**
   - Validation success rate
   - Validation error rate by type
   - Alert frequency

3. **BigQuery Writes**
   - Records inserted per second
   - Failed writes
   - Write latency

4. **History Updates**
   - SCD Type 2 updates per hour
   - History records created
   - Change detection rate

### View Pipeline Status

```bash
# List running jobs
gcloud dataflow jobs list \
    --region=us-central1 \
    --filter="name~salesforce-cdc" \
    --status=active

# View job details
gcloud dataflow jobs describe JOB_ID \
    --region=us-
