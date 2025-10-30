#!/bin/bash
# GCP Data Platform Demo - Deployment Script

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INFRASTRUCTURE_DIR="$PROJECT_ROOT/infrastructure"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi

    # Check if terraform is installed
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed. Please install it first."
        exit 1
    fi

    # Check if authenticated
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        log_error "Not authenticated with gcloud. Please run 'gcloud auth login' first."
        exit 1
    fi

    log_success "Prerequisites check passed."
}

# Function to setup environment
setup_environment() {
    local env=$1
    log_info "Setting up environment: $env"

    cd "$INFRASTRUCTURE_DIR"

    # Initialize Terraform
    log_info "Initializing Terraform..."
    terraform init -upgrade

    # Select workspace
    if ! terraform workspace select "$env" 2>/dev/null; then
        log_info "Creating new workspace: $env"
        terraform workspace new "$env"
    fi

    log_success "Environment setup completed for: $env"
}

# Function to validate configuration
validate_config() {
    local env=$1
    local tfvars_file="${env}.tfvars"

    log_info "Validating configuration for environment: $env"

    if [ ! -f "$tfvars_file" ]; then
        log_error "Configuration file $tfvars_file not found"
        exit 1
    fi

    # Basic validation
    if ! grep -q "project_id.*your-gcp-project-id" "$tfvars_file"; then
        log_info "Project ID is configured"
    else
        log_warning "Project ID is still set to placeholder. Please update $tfvars_file"
    fi
}

# Function to plan deployment
plan_deployment() {
    local env=$1
    local tfvars_file="${env}.tfvars"

    log_info "Planning deployment for environment: $env"

    terraform plan -var-file="$tfvars_file" -out="tfplan.$env"

    log_success "Plan generated: tfplan.$env"
}

# Function to apply deployment
apply_deployment() {
    local env=$1
    local tfvars_file="${env}.tfvars"

    log_info "Applying deployment for environment: $env"

    # Apply the plan if it exists, otherwise apply directly
    if [ -f "tfplan.$env" ]; then
        terraform apply "tfplan.$env"
    else
        terraform apply -var-file="$tfvars_file" -auto-approve
    fi

    log_success "Deployment applied for environment: $env"
}

# Function to cleanup
cleanup() {
    local env=$1

    log_info "Cleaning up temporary files for environment: $env"

    rm -f "tfplan.$env"

    log_success "Cleanup completed"
}

# Function to show usage
usage() {
    echo "Usage: $0 [COMMAND] [ENVIRONMENT]"
    echo ""
    echo "Commands:"
    echo "  validate    Validate Terraform configuration"
    echo "  plan        Plan the deployment"
    echo "  apply       Apply the deployment"
    echo "  deploy      Full deploy cycle (validate -> plan -> apply)"
    echo "  destroy     Destroy the infrastructure"
    echo ""
    echo "Environments:"
    echo "  dev         Development environment"
    echo "  staging     Staging environment"
    echo "  prod        Production environment"
    echo ""
    echo "Examples:"
    echo "  $0 validate dev"
    echo "  $0 deploy staging"
    echo "  $0 destroy dev"
}

# Main function
main() {
    if [ $# -lt 2 ]; then
        usage
        exit 1
    fi

    local command=$1
    local env=$2

    # Validate environment
    case $env in
        dev|staging|prod)
            ;;
        *)
            log_error "Invalid environment: $env"
            usage
            exit 1
            ;;
    esac

    check_prerequisites
    setup_environment "$env"

    case $command in
        validate)
            validate_config "$env"
            ;;
        plan)
            validate_config "$env"
            plan_deployment "$env"
            ;;
        apply)
            apply_deployment "$env"
            ;;
        deploy)
            validate_config "$env"
            plan_deployment "$env"
            apply_deployment "$env"
            ;;
        destroy)
            log_warning "Destroying infrastructure for environment: $env"
            read -p "Are you sure? (y/N): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                terraform destroy -var-file="${env}.tfvars" -auto-approve
                log_success "Infrastructure destroyed for environment: $env"
            else
                log_info "Destroy cancelled"
            fi
            ;;
        *)
            log_error "Invalid command: $command"
            usage
            exit 1
            ;;
    esac

    cleanup "$env"
}

# Run main function with all arguments
main "$@"
