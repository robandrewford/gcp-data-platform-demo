"""
Basic Dataflow Pipeline: Pub/Sub to BigQuery

This pipeline template demonstrates reading messages from Pub/Sub,
performing basic transformations, and writing to BigQuery.

Usage:
    python pipelines/basic_pubsub_to_bigquery.py \
        --project=YOUR_PROJECT \
        --region=YOUR_REGION \
        --input_topic=projects/YOUR_PROJECT/topics/YOUR_TOPIC \
        --output_table=YOUR_PROJECT:YOUR_DATASET.YOUR_TABLE \
        --runner=DataflowRunner \
        --temp_location=gs://YOUR_BUCKET/temp/ \
        --staging_location=gs://YOUR_BUCKET/staging/ \
        --service_account_email=YOUR_SA@YOUR_PROJECT.iam.gserviceaccount.com
"""

import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from src.core.logging import setup_logging


class ParsePubSubMessage(beam.DoFn):
    """Parse JSON messages from Pub/Sub."""

    def process(self, element):
        """Parse PubSub message and extract data."""
        try:
            # Pub/Sub messages come as bytes, decode to string
            if isinstance(element, bytes):
                element = element.decode('utf-8')

            # Parse JSON
            data = json.loads(element)

            # Add ingestion timestamp
            data['ingestion_time'] = datetime.utcnow().isoformat()

            yield data
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Log and skip malformed messages
            logging.warning(f"Failed to parse message: {e}")
            yield beam.pvalue.TaggedOutput('failed', {
                'error': str(e),
                'original_message': str(element),
                'timestamp': datetime.utcnow().isoformat()
            })


class DataValidation(beam.DoFn):
    """Validate and enrich data."""

    def process(self, element):
        """Validate data and add quality metadata."""
        try:
            # Basic validation
            required_fields = ['event_id', 'event_type', 'timestamp']
            if not all(field in element for field in required_fields):
                yield beam.pvalue.TaggedOutput('invalid', element)
                return

            # Add quality score (simplified)
            element['quality_score'] = 1.0 if 'data' in element else 0.8

            yield element

        except Exception as e:
            logging.error(f"Validation error: {e}")
            yield beam.pvalue.TaggedOutput('error', {
                'error': str(e),
                'data': element,
                'timestamp': datetime.utcnow().isoformat()
            })


class WriteToBigQuery(beam.PTransform):
    """Custom transform to write validated data to BigQuery."""

    def __init__(self, table_spec: str):
        self.table_spec = table_spec

    def expand(self, pcoll):
        """Write data to BigQuery with error handling."""
        return (
            pcoll
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=self.table_spec,
                schema=self._get_schema(),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    @staticmethod
    def _get_schema():
        """Define BigQuery table schema."""
        return {
            'fields': [
                {'name': 'event_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'event_type', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                {'name': 'data', 'type': 'JSON', 'mode': 'REQUIRED'},
                {'name': 'source', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'ingestion_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'quality_score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            ]
        }


def run_pipeline(
    input_topic: str,
    output_table: str,
    pipeline_options: PipelineOptions
):
    """Execute the Pub/Sub to BigQuery pipeline."""

    logger = setup_logging(level='INFO')
    logger.info(f"Starting pipeline: {input_topic} -> {output_table}")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Pub/Sub
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=input_topic)
        )

        # Parse and validate messages
        parsed_messages, failed_messages = (
            messages
            | 'Parse Messages' >> beam.ParDo(ParsePubSubMessage()).with_outputs(
                'failed', main='parsed'
            )
        )

        # Validate and enrich data
        validated_data, invalid_data, error_data = (
            parsed_messages
            | 'Validate Data' >> beam.ParDo(DataValidation()).with_outputs(
                'invalid', 'error', main='valid'
            )
        )

        # Write valid data to BigQuery
        (
            validated_data
            | 'Write Valid Data' >> WriteToBigQuery(output_table)
        )

        # Log pipeline metrics
        (
            validated_data
            | 'Count Valid Records' >> beam.combiners.Count.Globally()
            | 'Log Metrics' >> beam.Map(lambda count: logging.info(f"Processed {count} valid records"))
        )

    logger.info(f"Pipeline completed: {input_topic} -> {output_table}")


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description='Pub/Sub to BigQuery Data Pipeline')

    # Pipeline-specific arguments
    parser.add_argument(
        '--input_topic',
        required=True,
        help='Input Pub/Sub topic (projects/PROJECT/topics/TOPIC)'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='Output BigQuery table (PROJECT:DATASET.TABLE)'
    )

    # Pipeline options
    known_args, pipeline_args = parser.parse_known_args()

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Run the pipeline
    run_pipeline(
        input_topic=known_args.input_topic,
        output_table=known_args.output_table,
        pipeline_options=pipeline_options
    )


if __name__ == '__main__':
    main()
