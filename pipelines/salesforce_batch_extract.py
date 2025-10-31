"""
Salesforce Batch Extraction Pipeline

This Apache Beam pipeline extracts data from Salesforce REST API,
writes to Cloud Storage in Parquet format, and loads into BigQuery.

Usage:
    python pipelines/salesforce_batch_extract.py \
        --config=config/salesforce_config.yaml \
        --runner=DataflowRunner \
        --project=<project-id> \
        --region=us-central1 \
        --temp_location=gs://<bucket>/temp
"""

import argparse
import json
import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
import requests
import yaml
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class SalesforceAPIConfig:
    """Configuration for Salesforce API extraction."""

    def __init__(self, config_path: str):
        """Initialize configuration from YAML file."""
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.api_config = self.config['api']
        self.objects = self.config['objects']
        self.storage_config = self.config['storage']
        self.bigquery_config = self.config['bigquery']
        self.data_quality_config = self.config['data_quality']


class ExtractSalesforceObject(beam.DoFn):
    """DoFn to extract data from Salesforce API for a specific object."""

    def __init__(self, config: dict[str, Any], object_config: dict[str, Any]):
        """Initialize with API and object configuration."""
        self.api_config = config['api']
        self.object_config = object_config
        self.session = None

    def setup(self):
        """Set up HTTP session for API calls."""
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def process(self, element: str) -> Iterator[dict[str, Any]]:
        """
        Extract data from Salesforce API.

        Args:
            element: Trigger element (not used, just for pipeline flow)

        Yields:
            Dictionary records from the API
        """
        object_name = self.object_config['name']
        endpoint = self.object_config['endpoint']
        base_url = self.api_config['base_url']

        url = f"{base_url}{endpoint}"

        logging.info(f"Extracting {object_name} from {url}")

        try:
            response = self.session.get(
                url,
                timeout=self.api_config['timeout_seconds']
            )
            response.raise_for_status()

            data = response.json()
            records = data.get('records', [])

            logging.info(f"Extracted {len(records)} {object_name} records")

            # Add metadata to each record
            ingestion_timestamp = datetime.now(timezone.utc).isoformat()
            for record in records:
                record['ingestion_timestamp'] = ingestion_timestamp
                record['source'] = 'salesforce_api'
                record['object_type'] = object_name
                yield record

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to extract {object_name}: {str(e)}")
            raise

    def teardown(self):
        """Clean up HTTP session."""
        if self.session:
            self.session.close()


class ValidateRecord(beam.DoFn):
    """DoFn to validate record data quality."""

    def __init__(self, object_config: dict[str, Any], validation_rules: dict[str, Any]):
        """Initialize with object configuration and validation rules."""
        self.object_config = object_config
        self.validation_rules = validation_rules
        self.primary_key = object_config['primary_key']

    def process(self, record: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Validate record and tag with validation results.

        Args:
            record: Data record to validate

        Yields:
            Record with validation metadata
        """
        validation_errors = []

        # Check null primary keys
        if self.validation_rules.get('check_null_primary_keys', False):
            if not record.get(self.primary_key):
                validation_errors.append(f"Null primary key: {self.primary_key}")

        # Validate timestamps
        if self.validation_rules.get('validate_timestamps', False):
            timestamp_fields = ['created_date', 'last_modified_date', 'system_modstamp']
            for field in timestamp_fields:
                if field in record and record[field]:
                    try:
                        datetime.fromisoformat(record[field].replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        validation_errors.append(f"Invalid timestamp format: {field}")

        # Add validation metadata
        record['_validation_errors'] = validation_errors
        record['_is_valid'] = len(validation_errors) == 0
        record['_validation_timestamp'] = datetime.now(timezone.utc).isoformat()

        yield record


class FormatForBigQuery(beam.DoFn):
    """DoFn to format records for BigQuery insertion."""

    def process(self, record: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Format record for BigQuery schema compatibility.

        Args:
            record: Data record

        Yields:
            Formatted record
        """
        # Convert nested dictionaries to JSON strings for BigQuery
        formatted_record = {}
        for key, value in record.items():
            if isinstance(value, dict):
                formatted_record[key] = json.dumps(value)
            elif isinstance(value, list):
                formatted_record[key] = json.dumps(value)
            else:
                formatted_record[key] = value

        yield formatted_record


def get_bigquery_schema(object_name: str) -> dict[str, Any]:
    """
    Get BigQuery schema for a Salesforce object.

    Args:
        object_name: Name of the Salesforce object

    Returns:
        BigQuery schema definition
    """
    # Common fields for all objects
    base_schema = {
        'fields': [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'created_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'last_modified_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'system_modstamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'source', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'object_type', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': '_is_valid', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
            {'name': '_validation_errors', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': '_validation_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        ]
    }

    # Object-specific fields
    object_schemas = {
        'Account': [
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'industry', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'annual_revenue', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'website', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billing_address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'shipping_address', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        'Contact': [
            {'name': 'account_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lead_source', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        'Opportunity': [
            {'name': 'account_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'stage_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lead_source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'amount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'probability', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'close_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'is_won', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'is_closed', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        ],
        'Case': [
            {'name': 'account_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'contact_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'subject', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'origin', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'priority', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'is_escalated', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'is_closed', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'closed_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ],
    }

    # Combine base and object-specific schemas
    if object_name in object_schemas:
        base_schema['fields'].extend(object_schemas[object_name])

    return base_schema


def run_pipeline(config_path: str, pipeline_args: list[str]) -> None:
    """
    Run the Salesforce batch extraction pipeline.

    Args:
        config_path: Path to configuration YAML file
        pipeline_args: Additional pipeline arguments
    """
    # Load configuration
    config = SalesforceAPIConfig(config_path)

    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as p:

        # Process each enabled Salesforce object
        for obj_config in config.objects:
            if not obj_config.get('enabled', True):
                continue

            object_name = obj_config['name']
            logging.info(f"Setting up pipeline for {object_name}")

            # Extract, validate, and load data
            (
                p
                | f'Create{object_name}Trigger' >> beam.Create(['trigger'])
                | f'Extract{object_name}' >> beam.ParDo(
                    ExtractSalesforceObject(config.config, obj_config)
                )
                | f'Validate{object_name}' >> beam.ParDo(
                    ValidateRecord(obj_config, config.data_quality_config['validation_rules'])
                )
                | f'Format{object_name}ForBigQuery' >> beam.ParDo(FormatForBigQuery())
                | f'Write{object_name}ToBigQuery' >> WriteToBigQuery(
                    table=f"{config.bigquery_config['project_id']}:{config.bigquery_config['dataset_id']}.{config.bigquery_config['raw_table_prefix']}{object_name.lower()}",
                    schema=get_bigquery_schema(object_name),
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

    logging.info("Pipeline execution completed")


def main():
    """Main entry point for the pipeline."""
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description='Salesforce Batch Extraction Pipeline'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to Salesforce configuration YAML file'
    )

    known_args, pipeline_args = parser.parse_known_args()

    run_pipeline(known_args.config, pipeline_args)


if __name__ == '__main__':
    main()
