#!/bin/bash
# Deploy Salesforce CDC Streaming Pipeline to Google Cloud Dataflow
#
# Usage:
#   ./scripts/deploy_cdc_pipeline.sh <environment> [options]
#
# Arguments:
#   environment: dev, staging, or prod
#
# Options:
#   --dry-run: Show what would be deployed without actually deploying
#   --template: Create a Dataflow template instead of running directly

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
ENVIRONMENT=${1:-dev}
DRY_RUN=false
TEMPLATE_MODE=false

shift || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --template)
            TEMPLATE_MODE=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    echo -e "${RED}Error: Environment must be dev, staging, or prod${NC}"
    exit 1
fi

echo -e "${GREEN}Deploying Salesforce CDC Streaming Pipeline to ${ENVIRONMENT}${NC}"

# Load environment variables
if [ -f "infrastructure/${ENVIRONMENT}/.env" ]; then
    source "infrastructure/${ENVIRONMENT}/.env"
    echo -e "${GREEN}✓ Loaded environment variables from infrastructure/${ENVIRONMENT}/.env${NC}"
else
    echo -e "${YELLOW}Warning: No .env file found at infrastructure/${ENVIRONMENT}/.env${NC}"
fi

# Required environment variables
REQUIRED_VARS=(
    "GCP_PROJECT_ID"
    "GCP_REGION"
    "BQ_DATASET"
    "GCS_BUCKET"
)

# Check required variables
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo -e "${RED}Error: Required environment variable $var is not set${NC}"
        exit 1
    fi
done

echo -e "${GREEN}✓ All required environment variables are set${NC}"

# Configuration
CONFIG_FILE="config/salesforce_cdc_config.yaml"
JOB_NAME="salesforce-cdc-${ENVIRONMENT}-$(date +%Y%m%d-%H%M%S)"
STAGING_LOCATION="gs://${GCS_BUCKET}/staging/${ENVIRONMENT}"
TEMP_LOCATION="gs://${GCS_BUCKET}/temp/${ENVIRONMENT}"

# Dataflow settings
NUM_WORKERS=2
MAX_NUM_WORKERS=10
MACHINE_TYPE="n1-standard-4"
DISK_SIZE_GB=50

# Environment-specific settings
case $ENVIRONMENT in
    prod)
        NUM_WORKERS=5
        MAX_NUM_WORKERS=20
        ;;
    staging)
        NUM_WORKERS=3
        MAX_NUM_WORKERS=15
        ;;
esac

echo ""
echo -e "${YELLOW}Deployment Configuration:${NC}"
echo "  Environment: $ENVIRONMENT"
echo "  Project ID: $GCP_PROJECT_ID"
echo "  Region: $GCP_REGION"
echo "  Job Name: $JOB_NAME"
echo "  Config File: $CONFIG_FILE"
echo "  Workers: $NUM_WORKERS (max: $MAX_NUM_WORKERS)"
echo "  Machine Type: $MACHINE_TYPE"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY RUN MODE - No actual deployment will occur${NC}"
    echo ""
    echo "Would execute deployment with the above configuration"
    exit 0
fi

# Confirm deployment
if [ "$ENVIRONMENT" = "prod" ]; then
    read -p "Deploy to PRODUCTION? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}Deployment cancelled${NC}"
        exit 0
    fi
fi

# Ensure dependencies are installed
echo -e "${GREEN}Installing dependencies...${NC}"
uv sync

# Create GCS buckets if they don't exist
echo -e "${GREEN}Ensuring GCS buckets exist...${NC}"
gsutil ls -b "gs://${GCS_BUCKET}" >/dev/null 2>&1 || \
    gsutil mb -p "${GCP_PROJECT_ID}" -l "${GCP_REGION}" "gs://${GCS_BUCKET}"

# Create BigQuery dataset if it doesn't exist
echo -e "${GREEN}Ensuring BigQuery dataset exists...${NC}"
bq ls -d "${GCP_PROJECT_ID}:${BQ_DATASET}" >/dev/null 2>&1 || \
    bq mk --project_id="${GCP_PROJECT_ID}" --location="${GCP_REGION}" "${BQ_DATASET}"

# Create Pub/Sub topics and subscriptions if they don't exist
echo -e "${GREEN}Ensuring Pub/Sub resources exist...${NC}"

# CDC events topic and subscription
gcloud pubsub topics describe stream-processing --project="${GCP_PROJECT_ID}" >/dev/null 2>&1 || \
    gcloud pubsub topics create stream-processing --project="${GCP_PROJECT_ID}"

gcloud pubsub subscriptions describe salesforce-cdc-subscription --project="${GCP_PROJECT_ID}" >/dev/null 2>&1 || \
    gcloud pubsub subscriptions create salesforce-cdc-subscription \
        --topic=stream-processing \
        --ack-deadline=60 \
        --project="${GCP_PROJECT_ID}"

# Data quality alerts topic
gcloud pubsub topics describe data-quality-alerts --project="${GCP_PROJECT_ID}" >/dev/null 2>&1 || \
    gcloud pubsub topics create data-quality-alerts --project="${GCP_PROJECT_ID}"

# Dead letter topic
gcloud pubsub topics describe cdc-dead-letter --project="${GCP_PROJECT_ID}" >/dev/null 2>&1 || \
    gcloud pubsub topics create cdc-dead-letter --project="${GCP_PROJECT_ID}"

echo -e "${GREEN}✓ GCP resources verified/created${NC}"

# Deploy pipeline
echo ""
echo -e "${GREEN}Deploying streaming pipeline...${NC}"

if [ "$TEMPLATE_MODE" = true ]; then
    # Create Dataflow template
    TEMPLATE_PATH="gs://${GCS_BUCKET}/templates/${ENVIRONMENT}/salesforce-cdc-streaming"
    
    echo -e "${YELLOW}Creating Dataflow template at: ${TEMPLATE_PATH}${NC}"
    
    uv run python pipelines/salesforce_streaming_cdc.py \
        --config="${CONFIG_FILE}" \
        --runner=DataflowRunner \
        --project="${GCP_PROJECT_ID}" \
        --region="${GCP_REGION}" \
        --staging_location="${STAGING_LOCATION}" \
        --temp_location="${TEMP_LOCATION}" \
        --template_location="${TEMPLATE_PATH}" \
        --num_workers="${NUM_WORKERS}" \
        --max_num_workers="${MAX_NUM_WORKERS}" \
        --machine_type="${MACHINE_TYPE}" \
        --disk_size_gb="${DISK_SIZE_GB}" \
        --autoscaling_algorithm=THROUGHPUT_BASED \
        --enable_streaming_engine \
        --use_runner_v2 \
        --experiments=use_runner_v2
    
    echo -e "${GREEN}✓ Dataflow template created: ${TEMPLATE_PATH}${NC}"
else
    # Run pipeline directly
    uv run python pipelines/salesforce_streaming_cdc.py \
        --config="${CONFIG_FILE}" \
        --runner=DataflowRunner \
        --project="${GCP_PROJECT_ID}" \
        --region="${GCP_REGION}" \
        --job_name="${JOB_NAME}" \
        --staging_location="${STAGING_LOCATION}" \
        --temp_location="${TEMP_LOCATION}" \
        --num_workers="${NUM_WORKERS}" \
        --max_num_workers="${MAX_NUM_WORKERS}" \
        --machine_type="${MACHINE_TYPE}" \
        --disk_size_gb="${DISK_SIZE_GB}" \
        --autoscaling_algorithm=THROUGHPUT_BASED \
        --enable_streaming_engine \
        --use_runner_v2 \
        --experiments=use_runner_v2 \
        --streaming
    
    echo ""
    echo -e "${GREEN}✓ Pipeline deployed successfully!${NC}"
    echo ""
    echo "Job Name: ${JOB_NAME}"
    echo "View in Cloud Console:"
    echo "  https://console.cloud.google.com/dataflow/jobs/${GCP_REGION}/${JOB_NAME}?project=${GCP_PROJECT_ID}"
fi

echo ""
echo -e "${GREEN}Deployment complete!${NC}"
