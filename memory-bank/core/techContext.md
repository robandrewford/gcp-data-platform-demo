# Tech Context

## Technologies Used

### Core GCP Services

- **BigQuery**: Data warehouse and analytics
- **Cloud Storage**: Object storage for data lake (Parquet format)
- **Dataflow**: Unified stream and batch data processing (Apache Beam)
- **Pub/Sub**: Real-time messaging and event ingestion (CDC)
- **Dataform**: SQL workflow orchestration and transformations
- **Dataplex**: Intelligent data fabric for catalog and governance
- **Cloud Functions**: Serverless event-driven computing
- **Cloud Composer**: Managed Apache Airflow for workflow orchestration
- **Cloud Scheduler**: Managed cron job service
- **Datastream (CDC)**: Change data capture for database replication
- **Dataproc**: Managed Spark/Hadoop clusters
- **Cloud Build**: CI/CD platform
- **Cloud Monitoring**: Observability and alerting
- **Cloud Run**: Serverless container platform (for API simulator)

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

```m
├── infrastructure/     # Terraform configurations
│   ├── dataform.tf     # Dataform repository and resources
│   ├── dataplex.tf     # Dataplex lake and zones
│   └── salesforce.tf   # Salesforce-specific BigQuery datasets
├── src/               # Python data processing code
│   ├── salesforce/    # Salesforce-specific modules
│   │   ├── api_client.py      # REST API client
│   │   ├── data_generator.py  # Synthetic data generation
│   │   └── schemas.py         # Salesforce object schemas
│   └── data_quality/  # Great Expectations suites
│       └── expectations/       # Data quality rules
├── pipelines/         # Dataflow pipeline definitions
│   ├── salesforce_batch_extract.py    # Batch API extraction
│   ├── salesforce_batch_load.py       # Parquet → BigQuery
│   ├── salesforce_streaming_cdc.py    # CDC streaming pipeline
│   └── data_quality_checks.py         # Great Expectations integration
├── dataform/          # Dataform SQL transformations
│   ├── definitions/
│   │   ├── staging/           # Raw → Staging models
│   │   ├── marts/             # Analytics tables
│   │   └── history/           # SCD Type 2 models
│   └── includes/              # Reusable SQL macros
├── scripts/           # Deployment and utility scripts
│   ├── deploy_dataform.sh    # Dataform deployment
│   └── setup_dataplex.sh     # Dataplex configuration
├── config/            # Configuration files
│   ├── salesforce_config.yaml # Salesforce object definitions
│   └── expectations_config.yaml # Great Expectations config
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

```m
apache-beam[gcp]==2.46.0    # Dataflow processing
google-cloud-bigquery==3.10.0  # BigQuery client
google-cloud-storage==2.7.0    # Cloud Storage client
google-cloud-pubsub==2.19.0     # Pub/Sub client
google-cloud-dataplex==1.5.0    # Dataplex client
pandas==1.5.3                  # Data manipulation
faker==18.9.0                  # Synthetic data generation
great-expectations==0.15.47     # Data quality validation
requests==2.31.0                # HTTP client for API simulation
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
