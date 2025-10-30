# Tech Context

## Technologies Used

### Core GCP Services
- **BigQuery**: Data warehouse and analytics
- **Cloud Storage**: Object storage for data lake
- **Dataflow**: Unified stream and batch data processing
- **Pub/Sub**: Real-time messaging and event ingestion
- **Cloud Functions**: Serverless event-driven computing
- **Cloud Composer**: Managed Apache Airflow for workflow orchestration
- **Cloud Scheduler**: Managed cron job service
- **Datastream (CDC)**: Change data capture for database replication
- **Dataproc**: Managed Spark/Hadoop clusters
- **Cloud Build**: CI/CD platform
- **Cloud Monitoring**: Observability and alerting

### Development Technologies
- **Python 3.9+**: Primary programming language
- **Apache Beam**: Data processing framework (used by Dataflow)
- **Terraform 1.0+**: Infrastructure as Code
- **Docker**: Containerization
- **GitHub Actions**: CI/CD automation

### Supporting Technologies
- **Pandas/NumPy**: Data manipulation and analysis
- **SQL**: BigQuery queries and transformations
- **YAML**: Configuration files
- **Markdown**: Documentation
- **Shell scripts**: Deployment automation

## Development Setup

### Local Development Environment
```bash
# Required tools
- Python 3.9+
- Terraform CLI
- Google Cloud SDK
- Docker Desktop
- VS Code with Python and Terraform extensions

# Authentication setup
gcloud auth application-default login
gcloud config set project [PROJECT_ID]
```

### Project Structure
```
├── infrastructure/     # Terraform configurations
├── src/               # Python data processing code
├── pipelines/         # Dataflow pipeline definitions
├── scripts/           # Deployment and utility scripts
├── config/            # Configuration files
├── docs/              # Documentation
├── tests/             # Unit and integration tests
└── memory-bank/       # Project documentation
```

## Technical Constraints

### GCP Service Limits
- BigQuery: 100 concurrent queries per project
- Dataflow: Regional quotas for worker instances
- Cloud Storage: 5TB/day ingestion limit per bucket
- Pub/Sub: 10GB/day free tier, throughput limits

### Performance Considerations
- Dataflow autoscaling based on throughput
- BigQuery partitioning and clustering for query optimization
- Cloud Storage multi-regional replication for availability
- Cost monitoring and alerting for budget control

### Security Constraints
- All resources must use VPC Service Controls
- Service accounts with minimal required permissions
- Data encryption at rest and in transit
- Audit logging enabled for all services

## Dependencies

### Python Dependencies
```
apache-beam[gcp]==2.46.0    # Dataflow processing
google-cloud-bigquery==3.10.0  # BigQuery client
google-cloud-storage==2.7.0    # Cloud Storage client
pandas==1.5.3                  # Data manipulation
pytest==7.2.1                  # Testing framework
```

### Infrastructure Dependencies
- Terraform Google Provider v4.63+
- Google Cloud APIs enabled for required services
- Service account with necessary IAM roles
- VPC network with private Google access

## Tool Usage Patterns

### Code Quality
- Black for Python code formatting
- Flake8 for linting
- MyPy for type checking
- Pre-commit hooks for automated checks

### Testing Strategy
- Unit tests for data transformations
- Integration tests for pipeline end-to-end
- Load testing for performance validation
- Cost analysis testing for budget compliance

### Deployment Pipeline
1. Code linting and testing
2. Infrastructure validation (terraform plan)
3. Build container images
4. Deploy to development environment
5. Integration testing
6. Promote to staging/production
