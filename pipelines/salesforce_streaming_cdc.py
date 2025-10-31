"""
Salesforce Streaming CDC Pipeline

Apache Beam streaming pipeline for processing Salesforce CDC events.
Implements real-time ingestion to BigQuery and hourly SCD Type 2 updates.

Usage:
    python pipelines/salesforce_streaming_cdc.py \
        --config=config/salesforce_cdc_config.yaml \
        --runner=DataflowRunner \
        --project=<project-id> \
        --region=us-central1
"""

import argparse
import json
import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
import yaml
from apache_beam import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from pipelines.utils.cdc_validators import CDCValidator
from pipelines.utils.scd_type2_handler import SCDType2Handler


class CDCConfig:
    """Configuration for CDC streaming pipeline."""

    def __init__(self, config_path: str):
        """Load configuration from YAML file."""
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.pubsub = self.config['pubsub']
        self.windowing = self.config['windowing']
        self.data_quality = self.config['data_quality']
        self.bigquery = self.config['bigquery']
        self.objects = self.config['objects']
        self.performance = self.config['performance']
        self.monitoring = self.config['monitoring']
        self.error_handling = self.config.get('error_handling', {})


class ParseCDCEvent(beam.DoFn):
    """Parse CDC event from Pub/Sub message."""

    def process(self, element: bytes) -> Iterator[dict[str, Any]]:
        """
        Parse JSON CDC event.

        Args:
            element: Raw message bytes

        Yields:
            Parsed CDC event dictionary
        """
        try:
            event = json.loads(element.decode('utf-8'))
            yield event
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse CDC event: {str(e)}")
        except Exception as e:
            logging.error(f"Error processing CDC event: {str(e)}")


class ValidateCDCEvent(beam.DoFn):
    """Validate CDC event using data quality rules."""

    def __init__(self, alert_threshold: float = 0.05):
        """Initialize validator."""
        self.alert_threshold = alert_threshold
        self.validator = None

    def setup(self):
        """Set up validator instance."""
        self.validator = CDCValidator(alert_threshold=self.alert_threshold)

    def process(self, element: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Validate CDC event.

        Args:
            element: CDC event dictionary

        Yields:
            Tagged tuple: ('valid', event) or ('invalid', event)
        """
        is_valid, errors = self.validator.validate_event(element)

        if is_valid:
            yield beam.pvalue.TaggedOutput('valid', element)
        else:
            # Add validation errors to event
            element['_validation_errors'] = errors
            element['_validation_timestamp'] = datetime.now(timezone.utc).isoformat()
            yield beam.pvalue.TaggedOutput('invalid', element)


class ExtractRecordForBigQuery(beam.DoFn):
    """Extract record data for BigQuery insertion."""

    def process(self, element: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Extract record from CDC event.

        Args:
            element: CDC event dictionary

        Yields:
            Record formatted for BigQuery
        """
        try:
            event_type = element.get('event_type')

            # Extract appropriate data based on event type
            if event_type in ['INSERT', 'UPDATE']:
                record = element.get('after', {}).copy()
            elif event_type == 'DELETE':
                record = element.get('before', {}).copy()
            else:
                logging.error(f"Unknown event type: {event_type}")
                return

            # Add metadata
            record['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
            record['source'] = 'salesforce_cdc'
            record['_cdc_event_id'] = element.get('event_id')
            record['_cdc_event_type'] = event_type
            record['_cdc_event_timestamp'] = element.get('event_timestamp')

            # Format complex fields for BigQuery
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    record[key] = json.dumps(value)

            yield record

        except Exception as e:
            logging.error(f"Failed to extract record: {str(e)}")


class RouteByObjectType(beam.DoFn):
    """Route events to different outputs based on object type."""

    def process(self, element: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Route event by object type.

        Args:
            element: CDC event or record dictionary

        Yields:
            Tagged tuple with object type
        """
        object_type = element.get('object_type')

        if object_type in ['Account', 'Contact', 'Opportunity', 'Case']:
            yield beam.pvalue.TaggedOutput(object_type, element)
        else:
            logging.warning(f"Unknown object type: {object_type}")


class ProcessSCDType2Changes(beam.DoFn):
    """Process CDC events for SCD Type 2 history tracking."""

    def __init__(self, project_id: str, dataset_id: str, table_name: str, object_type: str):
        """Initialize SCD Type 2 processor."""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.object_type = object_type
        self.handler = None

    def setup(self):
        """Set up SCD Type 2 handler."""
        from google.cloud import bigquery

        bigquery_client = bigquery.Client(project=self.project_id)
        self.handler = SCDType2Handler(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            bigquery_client=bigquery_client
        )

    def process(self, element: tuple[str, list[dict[str, Any]]]) -> Iterator[dict[str, Any]]:
        """
        Process windowed CDC events for SCD Type 2.

        Args:
            element: Tuple of (object_type, list of CDC events)

        Yields:
            Processing statistics
        """
        object_type, events = element

        if not events:
            return

        logging.info(f"Processing {len(events)} CDC events for {object_type} SCD Type 2 history")

        # Process the hourly batch
        stats = self.handler.process_hourly_changes(
            object_type=self.object_type,
            events=list(events),
            table_name=self.table_name
        )

        logging.info(f"SCD Type 2 processing completed: {stats}")
        yield stats


class CreateDataQualityAlert(beam.DoFn):
    """Create data quality alert for invalid events."""

    def process(self, element: dict[str, Any]) -> Iterator[bytes]:
        """
        Create alert message for invalid event.

        Args:
            element: Invalid CDC event with validation errors

        Yields:
            Alert message as bytes
        """
        alert = {
            'alert_type': 'data_quality_violation',
            'severity': 'ERROR',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event_id': element.get('event_id'),
            'object_type': element.get('object_type'),
            'record_id': element.get('record_id'),
            'validation_errors': element.get('_validation_errors', []),
            'event_data': element
        }

        yield json.dumps(alert).encode('utf-8')


def get_bigquery_table(project_id: str, dataset_id: str, object_type: str, table_prefix: str = 'raw_') -> str:
    """
    Get BigQuery table reference.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        object_type: Salesforce object type
        table_prefix: Table name prefix

    Returns:
        Fully qualified table name
    """
    table_name = f"{table_prefix}{object_type.lower()}"
    return f"{project_id}:{dataset_id}.{table_name}"


def run_pipeline(config_path: str, pipeline_args: list[str]) -> None:
    """
    Run the Salesforce CDC streaming pipeline.

    Args:
        config_path: Path to configuration YAML file
        pipeline_args: Additional pipeline arguments
    """
    # Load configuration
    config = CDCConfig(config_path)

    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    # Construct Pub/Sub subscription path
    subscription_path = f"projects/{config.pubsub['project_id']}/subscriptions/{config.pubsub['subscription']}"

    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as p:

        # Read CDC events from Pub/Sub
        cdc_events = (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(subscription=subscription_path)
            | 'ParseJSON' >> beam.ParDo(ParseCDCEvent())
        )

        # Validate events
        validated = (
            cdc_events
            | 'ValidateEvents' >> beam.ParDo(ValidateCDCEvent(
                alert_threshold=config.data_quality['alert_threshold']
            )).with_outputs('valid', 'invalid')
        )

        # Process valid events - Real-time path to BigQuery
        valid_events = validated.valid

        # Route by object type
        routed = (
            valid_events
            | 'RouteByObjectType' >> beam.ParDo(RouteByObjectType()).with_outputs(
                'Account', 'Contact', 'Opportunity', 'Case'
            )
        )

        # Process each object type - Real-time ingestion
        for object_type in ['Account', 'Contact', 'Opportunity', 'Case']:
            obj_config = next((obj for obj in config.objects if obj['name'] == object_type), None)

            if obj_config and obj_config.get('enabled', True):
                # Get the routed events for this object type
                object_events = getattr(routed, object_type)

                # Extract records and write to BigQuery
                table_ref = get_bigquery_table(
                    config.bigquery['project_id'],
                    config.bigquery['dataset_id'],
                    object_type,
                    'raw_'
                )

                (
                    object_events
                    | f'Extract{object_type}Record' >> beam.ParDo(ExtractRecordForBigQuery())
                    | f'Window{object_type}' >> beam.WindowInto(
                        window.FixedWindows(10)  # 10-second windows for micro-batching
                    )
                    | f'WriteTo{object_type}BigQuery' >> WriteToBigQuery(
                        table=table_ref,
                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                    )
                )

        # Hourly batch processing for SCD Type 2 history tables
        for object_type in ['Account', 'Contact', 'Opportunity', 'Case']:
            obj_config = next((obj for obj in config.objects if obj['name'] == object_type), None)

            if obj_config and obj_config.get('track_history', False):
                # Get the routed events for this object type
                object_events = getattr(routed, object_type)

                # Window events into 1-hour batches
                history_table = f"{config.bigquery['project_id']}:{config.bigquery['dataset_id']}.{config.bigquery['history_tables'][object_type]}"

                (
                    object_events
                    | f'Window{object_type}Hourly' >> beam.WindowInto(
                        window.FixedWindows(int(config.windowing['history']['window_duration'].replace('s', '')))
                    )
                    | f'Group{object_type}ByWindow' >> beam.GroupBy(lambda x: x.get('object_type'))
                    | f'Process{object_type}SCD2' >> beam.ParDo(
                        ProcessSCDType2Changes(
                            project_id=config.bigquery['project_id'],
                            dataset_id=config.bigquery['dataset_id'],
                            table_name=config.bigquery['history_tables'][object_type],
                            object_type=object_type
                        )
                    )
                )

        # Handle invalid events - Publish to alerts topic
        if config.data_quality.get('enabled', True):
            alerts_topic = f"projects/{config.pubsub['project_id']}/topics/{config.data_quality['alerts_topic']}"

            (
                validated.invalid
                | 'CreateAlerts' >> beam.ParDo(CreateDataQualityAlert())
                | 'PublishAlerts' >> beam.io.WriteToPubSub(topic=alerts_topic)
            )

    logging.info("CDC streaming pipeline execution started")


def main():
    """Main entry point for the pipeline."""
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description='Salesforce CDC Streaming Pipeline'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to CDC configuration YAML file'
    )

    known_args, pipeline_args = parser.parse_known_args()

    run_pipeline(known_args.config, pipeline_args)


if __name__ == '__main__':
    main()
