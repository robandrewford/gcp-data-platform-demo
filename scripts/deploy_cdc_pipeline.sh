#!/bin/bash
# Deploy Salesforce CDC Streaming Pipeline to Dataflow
#
# Usage:
#   ./scripts/deploy_cdc_pipeline.sh [environment]
#
# Example:
#   ./scripts/deploy_cdc_pipeline.sh dev

set -e

# Configuration
ENVIRONMENT="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="${PROJECT_ROOT}/config/salesforce_cdc_config.yaml"

echo "======================================"
echo "Salesforce CDC Pipeline Deployment"
echo "======================================"
echo "Environment: ${ENVIRONMENT}"
echo "Project Root: ${PROJECT_ROOT}"
echo ""

# Load environment variables
if [ -f "${PROJECT_ROOT}/.env.${ENVIRONMENT}" ]; then
    echo "Loading environment variables from .env.${ENVIRONMENT}"
    set -a
    source "${PROJECT_ROOT}/.env.${ENVIRONMENT}"
    set +a
else
    echo "Warning: .env.${ENVIRONMENT} not found, using default environment variables"
fi

# Required environment variables
: "${GCP_PROJECT_ID:?Environment variable GCP_PROJECT_ID is required}"
: "${BQ_DATASET:?Environment variable BQ_DATASET is required}"
: "${GCS_BUCKET:?Environment variable GCS_BUCKET is required}"
: "${SUBNETWORK:?Environment variable SUBNETWORK is required}"

echo "GCP Project: ${GCP_PROJECT_ID}"
echo "BigQuery Dataset: ${BQ_DATASET}"
echo "GCS Bucket: ${GCS_BUCKET}"
echo "Subnetwork: ${SUBNETWORK}"
echo ""

# Deployment configuration
PIPELINE_NAME="salesforce-cdc-${ENVIRONMENT}"
REGION="us-central1"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-dataflow-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com}"

echo "======================================"
echo "Pre-deployment Checks"
echo "======================================"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI is not installed"
    exit 1
fi

# Check if authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "ERROR: Not authenticated with gcloud. Run 'gcloud auth login'"
    exit 1
fi

# Set project
echo "Setting GCP project..."
gcloud config set project "${GCP_PROJECT_ID}"

# Check if Pub/Sub subscription exists
SUBSCRIPTION_NAME="salesforce-cdc-subscription"
echo "Checking Pub/Sub subscription: ${SUBSCRIPTION_NAME}"
if ! gcloud pubsub subscriptions describe "${SUBSCRIPTION_NAME}" &> /dev/null; then
    echo "Creating Pub/Sub subscription..."
    gcloud pubsub subscriptions create "${SUBSCRIPTION_NAME}" \
        --topic="stream-processing" \
        --ack-deadline=60
else
    echo "Subscription already exists"
fi

# Check if BigQuery dataset exists
echo "Checking BigQuery dataset: ${BQ_DATASET}"
if ! bq ls -d "${GCP_PROJECT_ID}:${BQ_DATASET}" &> /dev/null; then
    echo "ERROR: BigQuery dataset ${BQ_DATASET} does not exist"
    exit 1
fi

echo ""
echo "======================================"
echo "Deploying Pipeline"
echo "======================================"

# Build command
PYTHON_CMD="python3 ${PROJECT_ROOT}/pipelines/salesforce_streaming_cdc.py"

PIPELINE_ARGS=(
    "--config=${CONFIG_FILE}"
    "--runner=DataflowRunner"
    "--project=${GCP_PROJECT_ID}"
    "--region=${REGION}"
    "--job_name=${PIPELINE_NAME}-$(date +%Y%m%d-%H%M%S)"
    "--staging_location=gs://${GCS_BUCKET}/staging"
    "--temp_location=gs://${GCS_BUCKET}/temp"
    "--service_account_email=${SERVICE_ACCOUNT}"
    "--num_workers=2"
    "--max_num_workers=10"
    "--autoscaling_algorithm=THROUGHPUT_BASED"
    "--worker_machine_type=n1-standard-4"
    "--disk_size_gb=50"
    "--subnetwork=${SUBNETWORK}"
    "--no_use_public_ips"
    "--streaming"
    "--enable_streaming_engine"
    "--experiments=use_runner_v2"
)

echo "Deploying streaming pipeline..."
echo "Job Name: ${PIPELINE_NAME}-$(date +%Y%m%d-%H%M%S)"
echo ""

# Execute deployment
$PYTHON_CMD "${PIPELINE_ARGS[@]}"

DEPLOY_STATUS=$?

echo ""
echo "======================================"
if [ $DEPLOY_STATUS -eq 0 ]; then
    echo "✓ Pipeline Deployed Successfully"
    echo "======================================"
    echo ""
    echo "Pipeline Status:"
    echo "  View in Console: https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${GCP_PROJECT_ID}"
    echo ""
    echo "To monitor the pipeline:"
    echo "  gcloud dataflow jobs list --region=${REGION} --filter=\"name~${PIPELINE_NAME}\""
    echo ""
    echo "To cancel the pipeline:"
    echo "  gcloud dataflow jobs cancel <JOB_ID> --region=${REGION}"
else
    echo "✗ Pipeline Deployment Failed"
    echo "======================================"
    exit 1
fi
