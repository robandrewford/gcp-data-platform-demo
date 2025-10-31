"""
SCD Type 2 Handler

Implements Slowly Changing Dimension Type 2 pattern for history tracking.
Manages hourly snapshot batching and history table updates.

Usage:
    from pipelines.utils.scd_type2_handler import SCDType2Handler

    handler = SCDType2Handler(bigquery_client, dataset_id, table_id)
    handler.process_hourly_changes(events)
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import bigquery


class SCDType2Handler:
    """Handle SCD Type 2 history updates for CDC events."""

    def __init__(
        self,
        bigquery_client: bigquery.Client,
        project_id: str,
        dataset_id: str,
        history_table_suffix: str = '_history'
    ):
        """
        Initialize SCD Type 2 handler.

        Args:
            bigquery_client: BigQuery client
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            history_table_suffix: Suffix for history tables
        """
        self.client = bigquery_client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.history_table_suffix = history_table_suffix

    def group_events_by_record(
        self,
        events: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Group events by record ID for batch processing.

        Args:
            events: List of CDC events

        Returns:
            Dictionary mapping record_id to list of events
        """
        grouped = defaultdict(list)

        for event in events:
            record_id = event.get('record_id')
            if record_id:
                grouped[record_id].append(event)

        # Sort events by timestamp within each group
        for record_id in grouped:
            grouped[record_id].sort(key=lambda e: e.get('event_timestamp', ''))

        return dict(grouped)

    def detect_changes(
        self,
        before: dict[str, Any],
        after: dict[str, Any],
        tracked_fields: Optional[list[str]] = None
    ) -> dict[str, Any]:
        """
        Detect changes between before and after states.

        Args:
            before: Before state
            after: After state
            tracked_fields: Optional list of fields to track (tracks all if None)

        Returns:
            Dictionary with change details
        """
        changes = {
            'has_changes': False,
            'changed_fields': [],
            'field_changes': {}
        }

        # Determine which fields to check
        if tracked_fields:
            fields_to_check = set(tracked_fields)
        else:
            fields_to_check = set(before.keys()) | set(after.keys())

        # Skip metadata fields
        metadata_fields = {
            'ingestion_timestamp', 'source', '_cdc_event_id',
            '_cdc_event_type', '_cdc_event_timestamp', '_pubsub_message_id',
            '_processing_timestamp', 'system_modstamp'
        }
        fields_to_check = fields_to_check - metadata_fields

        # Compare fields
        for field in fields_to_check:
            before_val = before.get(field)
            after_val = after.get(field)

            if before_val != after_val:
                changes['has_changes'] = True
                changes['changed_fields'].append(field)
                changes['field_changes'][field] = {
                    'before': before_val,
                    'after': after_val
                }

        return changes

    def get_current_record(
        self,
        object_type: str,
        record_id: str
    ) -> Optional[dict[str, Any]]:
        """
        Get current record from history table.

        Args:
            object_type: Salesforce object type
            record_id: Record ID

        Returns:
            Current history record or None
        """
        table_name = f"{object_type.lower()}{self.history_table_suffix}"
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        query = f"""
            SELECT *
            FROM `{table_id}`
            WHERE id = @record_id
              AND is_current = TRUE
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("record_id", "STRING", record_id)
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job.result())

            if results:
                # Convert Row to dictionary
                return dict(results[0])
            return None

        except Exception as e:
            logging.error(f"Failed to get current record {record_id}: {str(e)}")
            return None

    def create_history_record(
        self,
        record_id: str,
        record_data: dict[str, Any],
        valid_from: str,
        change_type: str,
        changed_fields: list[str],
        is_current: bool = True,
        valid_to: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Create history record for insertion.

        Args:
            record_id: Record ID
            record_data: Full record data
            valid_from: Validity start timestamp
            change_type: Type of change (INSERT, UPDATE, DELETE)
            changed_fields: List of changed field names
            is_current: Whether this is the current version
            valid_to: Validity end timestamp (None for current)

        Returns:
            History record dictionary
        """
        return {
            'id': record_id,
            'valid_from': valid_from,
            'valid_to': valid_to,
            'is_current': is_current,
            'change_type': change_type,
            'changed_fields': json.dumps(changed_fields),
            'record_data': json.dumps(record_data),
            'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        }

    def close_current_record(
        self,
        object_type: str,
        record_id: str,
        valid_to: str
    ) -> bool:
        """
        Close current record by setting valid_to and is_current=False.

        Args:
            object_type: Salesforce object type
            record_id: Record ID
            valid_to: Validity end timestamp

        Returns:
            True if successful
        """
        table_name = f"{object_type.lower()}{self.history_table_suffix}"
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        query = f"""
            UPDATE `{table_id}`
            SET valid_to = @valid_to,
                is_current = FALSE
            WHERE id = @record_id
              AND is_current = TRUE
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("record_id", "STRING", record_id),
                bigquery.ScalarQueryParameter("valid_to", "TIMESTAMP", valid_to)
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            logging.info(f"Closed current record {record_id} with valid_to={valid_to}")
            return True

        except Exception as e:
            logging.error(f"Failed to close current record {record_id}: {str(e)}")
            return False

    def insert_history_records(
        self,
        object_type: str,
        records: list[dict[str, Any]]
    ) -> bool:
        """
        Insert history records into BigQuery.

        Args:
            object_type: Salesforce object type
            records: List of history records

        Returns:
            True if successful
        """
        if not records:
            return True

        table_name = f"{object_type.lower()}{self.history_table_suffix}"
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        try:
            errors = self.client.insert_rows_json(table_id, records)

            if errors:
                logging.error(f"Failed to insert history records: {errors}")
                return False

            logging.info(f"Inserted {len(records)} history records for {object_type}")
            return True

        except Exception as e:
            logging.error(f"Failed to insert history records: {str(e)}")
            return False

    def process_hourly_changes(
        self,
        events: list[dict[str, Any]],
        object_type: str
    ) -> dict[str, Any]:
        """
        Process hourly batch of changes for SCD Type 2 updates.

        Args:
            events: List of CDC events from the hour
            object_type: Salesforce object type

        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'total_events': len(events),
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'errors': 0
        }

        # Group events by record ID
        grouped_events = self.group_events_by_record(events)

        new_history_records = []
        records_to_close = []

        # Process each record's events
        for record_id, record_events in grouped_events.items():
            try:
                # Get current state from history table
                current_record = self.get_current_record(object_type, record_id)

                # Process each event for this record
                for event in record_events:
                    event_type = event.get('event_type')
                    event_timestamp = event.get('event_timestamp')

                    if event_type == 'INSERT':
                        # New record - create initial history entry
                        after_data = event.get('after', {})
                        history_rec = self.create_history_record(
                            record_id=record_id,
                            record_data=after_data,
                            valid_from=event_timestamp,
                            change_type='INSERT',
                            changed_fields=[],
                            is_current=True
                        )
                        new_history_records.append(history_rec)
                        stats['records_inserted'] += 1

                    elif event_type == 'UPDATE':
                        before_data = event.get('before', {})
                        after_data = event.get('after', {})

                        # Detect changes
                        changes = self.detect_changes(before_data, after_data)

                        if changes['has_changes']:
                            # Close current record
                            if current_record:
                                records_to_close.append({
                                    'record_id': record_id,
                                    'valid_to': event_timestamp
                                })

                            # Create new history record
                            history_rec = self.create_history_record(
                                record_id=record_id,
                                record_data=after_data,
                                valid_from=event_timestamp,
                                change_type='UPDATE',
                                changed_fields=changes['changed_fields'],
                                is_current=True
                            )
                            new_history_records.append(history_rec)
                            stats['records_updated'] += 1

                    elif event_type == 'DELETE':
                        # Close current record for deletion
                        if current_record:
                            records_to_close.append({
                                'record_id': record_id,
                                'valid_to': event_timestamp
                            })

                stats['records_processed'] += 1

            except Exception as e:
                logging.error(f"Failed to process record {record_id}: {str(e)}")
                stats['errors'] += 1

        # Close records that need to be closed
        for close_info in records_to_close:
            success = self.close_current_record(
                object_type=object_type,
                record_id=close_info['record_id'],
                valid_to=close_info['valid_to']
            )
            if not success:
                stats['errors'] += 1

        # Insert new history records
        if new_history_records:
            success = self.insert_history_records(object_type, new_history_records)
            if not success:
                stats['errors'] += len(new_history_records)

        logging.info(f"SCD Type 2 processing complete: {stats}")
        return stats
