# Active Context

## Current Work Focus

**Phase 3: Salesforce Data Processing Examples** - Implementing comprehensive data pipelines using Salesforce data as the primary use case. This phase demonstrates real-world data platform capabilities with batch and streaming ingestion, transformations, and data quality validation.

## Recent Changes

- ✅ Completed Phase 1: Core Infrastructure and Phase 2: Data Pipeline Foundations
- ✅ Created comprehensive BigQuery schemas for raw events and transformed data
- ✅ Added specialized Pub/Sub topics for pipeline patterns (alerts, status, batch, streaming)
- ✅ Implemented Dataflow service account with appropriate permissions
- ✅ Built Cloud Storage buckets
- ✅ Created Pub/Sub → BigQuery and Cloud Storage → BigQuery pipeline templates
- ✅ Integrated UV and Ruff for dependency management and code quality
- ✅ Refactored `src/` directory into modular Python packages
- ✅ Updated project documentation with comprehensive README
- ✅ Defined Phase 3 technology stack: Parquet, Apache Beam, Dataform, Dataplex
- ✅ Created detailed implementation roadmap for Salesforce data processing

## Next Steps

**Immediate Priorities:**

- Set up Dataform repository and Dataplex configuration
- Create synthetic Salesforce data generator and REST API simulator
- Implement batch extraction pipeline (Salesforce API → Parquet → BigQuery)
- Develop streaming CDC pipeline (Pub/Sub → BigQuery)
- Adapt dbt Salesforce models to Dataform transformations
- Integrate Great Expectations for data quality validation

**Phase 3 Implementation Sequence:**

1. Infrastructure Setup (Dataform, Dataplex, BigQuery datasets)
2. Synthetic Salesforce Data Generator & REST API Simulator
3. Batch Ingestion Pipeline (REST API → Parquet → BigQuery)
4. Streaming CDC Pipeline (Pub/Sub → BigQuery)
5. Dataform Transformations (dbt Salesforce models adaptation)
6. Data Quality Integration (Great Expectations + Dataplex)
7. Monitoring & Observability Setup
8. Documentation & Examples

## Active Decisions

- **Phase 3 Technology Stack**: Parquet for data lake format, Apache Beam for processing, Dataform for transformations, Dataplex for catalog and data quality
- **Salesforce Use Case**: Using Salesforce data as primary example with synthetic data generation and REST API simulation
- **Data Architecture**: Unified batch and streaming patterns with Apache Beam, following dbt Salesforce model patterns adapted to Dataform
- **Data Quality Framework**: Great Expectations integrated with pipelines, Dataplex for catalog-level governance
- Focus on core GCP data services: BigQuery, Cloud Storage, Dataflow, Pub/Sub, Dataform, Dataplex
- Use Infrastructure as Code (Terraform) for resource provisioning
- Implement CI/CD pipelines for automated deployment
- Follow Google Cloud best practices and Well-Architected Framework

## Important Patterns and Preferences

- Use Python for data processing scripts
- Apache Beam for unified batch/streaming processing
- Terraform for infrastructure provisioning
- Docker for containerized deployments
- GitHub Actions for CI/CD pipelines
- Clear separation between infrastructure, application, and documentation code
- Parquet format for data lake storage (optimal for BigQuery analytics)
- JSON for streaming messages through Pub/Sub
- Dataform SQLX for transformations (dbt model adaptation)

## Learnings and Project Insights

- Memory bank approach ensures consistent project understanding
- Early establishment of project structure prevents future refactoring
- GCP data platform requires careful consideration of data lifecycle management
- Cost optimization should be built into architecture from the beginning
- Apache Beam provides excellent unified model for batch and streaming workloads
- Dataform offers native GCP integration for SQL transformations
- Dataplex provides comprehensive data governance without operational overhead
- Great Expectations integrates well with Beam pipelines for data quality
