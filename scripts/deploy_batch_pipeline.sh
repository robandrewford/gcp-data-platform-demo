#!/bin/bash

# Salesforce Batch Pipeline Deployment Script
#
# This script deploys the Salesforce batch extraction pipeline to Google Cloud Dataflow
# and sets up Cloud Scheduler for periodic execution.
#
# Usage:
#   ./scripts/deploy_batch_pipeline.sh [environment]
#
# Environment options: dev, staging, prod (default: dev)

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
ENVIRONMENT=${1:-dev}

print_info "Deploying Salesforce Batch Pipeline for environment: $ENVIRONMENT"

# Load environment-specific configuration
if [ ! -f "infrastructure/$ENVIRONMENT/.env" ]; then
    print_error "Environment file not found: infrastructure/$ENVIRONMENT/.env"
    print_info "Creating template .env file..."
    
    cat > "infrastructure/$ENVIRONMENT/.env" <<EOF
# GCP Project Configuration
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"

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

# Monitoring
export ALERT_EMAIL="your-email@example.com"

# Dataflow Configuration
export DATAFLOW_NUM_WORKERS=2
export DATAFLOW_MAX_WORKERS=10
export DATAFLOW_MACHINE_TYPE="n1-standard-4"
export DATAFLOW_DISK_SIZE_GB=50
EOF
    
    print_error "Please update infrastructure/$ENVIRONMENT/.env with your configuration"
    exit 1
fi

source "infrastructure/$ENVIRONMENT/.env"

# Validate required environment variables
REQUIRED_VARS=(
    "GCP_PROJECT_ID"
    "GCP_REGION"
    "GCS_BUCKET"
    "GCS_TEMP_LOCATION"
    "GCS_STAGING_LOCATION"
    "BQ_DATASET"
    "SERVICE_ACCOUNT"
)

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

print_info "Environment configuration loaded successfully"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install it from https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
    print_error "Not authenticated with gcloud. Run: gcloud auth login"
    exit 1
fi

# Set active project
print_info "Setting active project to $GCP_PROJECT_ID"
gcloud config set project "$GCP_PROJECT_ID"

# Install Python dependencies
print_info "Installing Python dependencies..."
if command -v uv &> /dev/null; then
    print_info "Using UV for dependency management"
    uv sync --group core
else
    print_warning "UV not found, falling back to pip"
    pip install -r <(echo "apache-beam[gcp]>=2.54.0
google-cloud-bigquery>=3.13.0
google-cloud-storage>=2.14.0
google-cloud-pubsub>=2.19.0
requests>=2.31.0
pyyaml>=6.0.0
pyarrow>=15.0.0")
fi

# Create BigQuery dataset if it doesn't exist
print_info "Ensuring BigQuery dataset exists: $BQ_DATASET"
if ! bq ls -d "$GCP_PROJECT_ID:$BQ_DATASET" &> /dev/null; then
    print_info "Creating BigQuery dataset: $BQ_DATASET"
    bq mk --dataset \
        --location="$GCP_REGION" \
        --description="Salesforce raw data" \
        "$GCP_PROJECT_ID:$BQ_DATASET"
else
    print_info "BigQuery dataset already exists: $BQ_DATASET"
fi

# Deploy Dataflow pipeline
print_info "Deploying Dataflow pipeline..."

PIPELINE_NAME="salesforce-batch-extract-$ENVIRONMENT"
JOB_NAME="$PIPELINE_NAME-$(date +%Y%m%d-%H%M%S)"

python pipelines/salesforce_batch_extract.py \
    --config=config/salesforce_config.yaml \
    --runner=DataflowRunner \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --temp_location="$GCS_TEMP_LOCATION" \
    --staging_location="$GCS_STAGING_LOCATION" \
    --job_name="$JOB_NAME" \
    --service_account_email="$SERVICE_ACCOUNT" \
    --num_workers="$DATAFLOW_NUM_WORKERS" \
    --max_num_workers="$DATAFLOW_MAX_WORKERS" \
    --machine_type="$DATAFLOW_MACHINE_TYPE" \
    --disk_size_gb="$DATAFLOW_DISK_SIZE_GB" \
    --subnetwork="$SUBNETWORK" \
    --no_use_public_ips \
    --setup_file=./setup.py

if [ $? -eq 0 ]; then
    print_info "Dataflow job submitted successfully: $JOB_NAME"
    print_info "Monitor job at: https://console.cloud.google.com/dataflow/jobs/$GCP_REGION/$JOB_NAME?project=$GCP_PROJECT_ID"
else
    print_error "Failed to submit Dataflow job"
    exit 1
fi

# Create Cloud Scheduler job for daily execution
print_info "Setting up Cloud Scheduler for daily execution..."

SCHEDULER_JOB_NAME="salesforce-batch-daily-$ENVIRONMENT"
SCHEDULE="0 2 * * *"  # 2 AM daily
TIMEZONE="America/Los_Angeles"

# Check if scheduler job already exists
if gcloud scheduler jobs describe "$SCHEDULER_JOB_NAME" --location="$GCP_REGION" &> /dev/null; then
    print_info "Updating existing Cloud Scheduler job: $SCHEDULER_JOB_NAME"
    gcloud scheduler jobs update http "$SCHEDULER_JOB_NAME" \
        --location="$GCP_REGION" \
        --schedule="$SCHEDULE" \
        --time-zone="$TIMEZONE" \
        --uri="https://dataflow.googleapis.com/v1b3/projects/$GCP_PROJECT_ID/locations/$GCP_REGION/templates:launch?gcsPath=gs://$GCS_BUCKET/templates/salesforce-batch-extract" \
        --http-method=POST \
        --oauth-service-account-email="$SERVICE_ACCOUNT" \
        --headers="Content-Type=application/json" \
        --message-body="{\"jobName\":\"$PIPELINE_NAME\",\"parameters\":{\"config\":\"config/salesforce_config.yaml\"},\"environment\":{\"tempLocation\":\"$GCS_TEMP_LOCATION\",\"stagingLocation\":\"$GCS_STAGING_LOCATION\"}}"
else
    print_info "Creating new Cloud Scheduler job: $SCHEDULER_JOB_NAME"
    gcloud scheduler jobs create http "$SCHEDULER_JOB_NAME" \
        --location="$GCP_REGION" \
        --schedule="$SCHEDULE" \
        --time-zone="$TIMEZONE" \
        --uri="https://dataflow.googleapis.com/v1b3/projects/$GCP_PROJECT_ID/locations/$GCP_REGION/templates:launch?gcsPath=gs://$GCS_BUCKET/templates/salesforce-batch-extract" \
        --http-method=POST \
        --oauth-service-account-email="$SERVICE_ACCOUNT" \
        --headers="Content-Type=application/json" \
        --message-body="{\"jobName\":\"$PIPELINE_NAME\",\"parameters\":{\"config\":\"config/salesforce_config.yaml\"},\"environment\":{\"tempLocation\":\"$GCS_TEMP_LOCATION\",\"stagingLocation\":\"$GCS_STAGING_LOCATION\"}}"
fi

print_info "Cloud Scheduler job configured: $SCHEDULER_JOB_NAME"

# Summary
print_info ""
print_info "========================================"
print_info "Deployment Summary"
print_info "========================================"
print_info "Environment: $ENVIRONMENT"
print_info "Project: $GCP_PROJECT_ID"
print_info "Region: $GCP_REGION"
print_info "Dataflow Job: $JOB_NAME"
print_info "Scheduler Job: $SCHEDULER_JOB_NAME"
print_info "Schedule: $SCHEDULE ($TIMEZONE)"
print_info "BigQuery Dataset: $BQ_DATASET"
print_info ""
print_info "Next Steps:"
print_info "1. Start the Salesforce API simulator: python src/salesforce/api_client.py"
print_info "2. Monitor Dataflow job: gcloud dataflow jobs list --region=$GCP_REGION"
print_info "3. Query BigQuery tables: bq query 'SELECT COUNT(*) FROM \`$GCP_PROJECT_ID.$BQ_DATASET.raw_account\`'"
print_info "4. View Cloud Scheduler jobs: gcloud scheduler jobs list --location=$GCP_REGION"
print_info ""
print_info "${GREEN}Deployment completed successfully!${NC}"
