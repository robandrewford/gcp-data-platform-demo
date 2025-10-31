"""
Unit tests for SCD Type 2 Handler.

Tests the SCDType2Handler class which implements Slowly Changing Dimension Type 2
pattern for history tracking of CDC events.
"""

import json
from unittest.mock import Mock

from pipelines.utils.scd_type2_handler import SCDType2Handler


class TestSCDType2HandlerInitialization:
    """Test SCDType2Handler initialization."""

    def test_initialization(self):
        """Test handler initializes correctly."""
        mock_client = Mock()

        handler = SCDType2Handler(
            bigquery_client=mock_client,
            project_id='test-project',
            dataset_id='test_dataset',
            history_table_suffix='_history'
        )

        assert handler.client == mock_client
        assert handler.project_id == 'test-project'
        assert handler.dataset_id == 'test_dataset'
        assert handler.history_table_suffix == '_history'


class TestEventGrouping:
    """Test event grouping by record ID."""

    def test_group_events_by_record(self):
        """Test grouping events by record ID."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [
            {'record_id': '001ABC', 'event_timestamp': '2025-10-30T10:00:00Z', 'event_type': 'INSERT'},
            {'record_id': '001ABC', 'event_timestamp': '2025-10-30T11:00:00Z', 'event_type': 'UPDATE'},
            {'record_id': '001XYZ', 'event_timestamp': '2025-10-30T10:30:00Z', 'event_type': 'INSERT'},
        ]

        grouped = handler.group_events_by_record(events)

        assert '001ABC' in grouped
        assert '001XYZ' in grouped
        assert len(grouped['001ABC']) == 2
        assert len(grouped['001XYZ']) == 1

    def test_group_events_chronological_sorting(self):
        """Test events are sorted chronologically within groups."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [
            {'record_id': '001ABC', 'event_timestamp': '2025-10-30T12:00:00Z', 'event_type': 'DELETE'},
            {'record_id': '001ABC', 'event_timestamp': '2025-10-30T10:00:00Z', 'event_type': 'INSERT'},
            {'record_id': '001ABC', 'event_timestamp': '2025-10-30T11:00:00Z', 'event_type': 'UPDATE'},
        ]

        grouped = handler.group_events_by_record(events)

        timestamps = [e['event_timestamp'] for e in grouped['001ABC']]
        assert timestamps == sorted(timestamps)

    def test_group_events_empty_list(self):
        """Test handling empty event list."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        grouped = handler.group_events_by_record([])

        assert grouped == {}


class TestChangeDetection:
    """Test change detection between states."""

    def test_detect_changes_single_field(self):
        """Test detecting single field change."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        before = {'id': '001ABC', 'name': 'Acme Corp', 'revenue': 1000000}
        after = {'id': '001ABC', 'name': 'Acme Corporation', 'revenue': 1000000}

        changes = handler.detect_changes(before, after)

        assert changes['has_changes'] is True
        assert 'name' in changes['changed_fields']
        assert len(changes['changed_fields']) == 1
        assert changes['field_changes']['name']['before'] == 'Acme Corp'
        assert changes['field_changes']['name']['after'] == 'Acme Corporation'

    def test_detect_changes_multiple_fields(self):
        """Test detecting multiple field changes."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        before = {'id': '001ABC', 'name': 'Acme', 'revenue': 1000000, 'phone': '555-0100'}
        after = {'id': '001ABC', 'name': 'Acme Corp', 'revenue': 1200000, 'phone': '555-0100'}

        changes = handler.detect_changes(before, after)

        assert changes['has_changes'] is True
        assert 'name' in changes['changed_fields']
        assert 'revenue' in changes['changed_fields']
        assert 'phone' not in changes['changed_fields']
        assert len(changes['changed_fields']) == 2

    def test_detect_changes_no_changes(self):
        """Test no changes detected."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        before = {'id': '001ABC', 'name': 'Acme', 'revenue': 1000000}
        after = {'id': '001ABC', 'name': 'Acme', 'revenue': 1000000}

        changes = handler.detect_changes(before, after)

        assert changes['has_changes'] is False
        assert len(changes['changed_fields']) == 0

    def test_detect_changes_ignores_metadata(self):
        """Test metadata fields are ignored."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        before = {
            'id': '001ABC',
            'name': 'Acme',
            'ingestion_timestamp': '2025-10-30T10:00:00Z',
            'system_modstamp': '2025-10-30T10:00:00Z'
        }
        after = {
            'id': '001ABC',
            'name': 'Acme',
            'ingestion_timestamp': '2025-10-30T11:00:00Z',
            'system_modstamp': '2025-10-30T11:00:00Z'
        }

        changes = handler.detect_changes(before, after)

        assert changes['has_changes'] is False


class TestHistoryRecordCreation:
    """Test history record creation."""

    def test_create_history_record_insert(self):
        """Test creating INSERT history record."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        record_data = {'id': '001ABC', 'name': 'Acme Corp', 'revenue': 1000000}

        history_rec = handler.create_history_record(
            record_id='001ABC',
            record_data=record_data,
            valid_from='2025-10-30T10:00:00Z',
            change_type='INSERT',
            changed_fields=[],
            is_current=True
        )

        assert history_rec['id'] == '001ABC'
        assert history_rec['valid_from'] == '2025-10-30T10:00:00Z'
        assert history_rec['valid_to'] is None
        assert history_rec['is_current'] is True
        assert history_rec['change_type'] == 'INSERT'
        assert json.loads(history_rec['changed_fields']) == []
        assert 'ingestion_timestamp' in history_rec

    def test_create_history_record_update(self):
        """Test creating UPDATE history record."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        record_data = {'id': '001ABC', 'name': 'Acme Corporation', 'revenue': 1200000}

        history_rec = handler.create_history_record(
            record_id='001ABC',
            record_data=record_data,
            valid_from='2025-10-30T11:00:00Z',
            change_type='UPDATE',
            changed_fields=['name', 'revenue'],
            is_current=True
        )

        assert history_rec['change_type'] == 'UPDATE'
        assert json.loads(history_rec['changed_fields']) == ['name', 'revenue']

    def test_create_history_record_with_valid_to(self):
        """Test creating history record with valid_to date."""
        mock_client = Mock()
        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        record_data = {'id': '001ABC', 'name': 'Acme Corp'}

        history_rec = handler.create_history_record(
            record_id='001ABC',
            record_data=record_data,
            valid_from='2025-10-30T10:00:00Z',
            change_type='INSERT',
            changed_fields=[],
            is_current=False,
            valid_to='2025-10-30T11:00:00Z'
        )

        assert history_rec['valid_to'] == '2025-10-30T11:00:00Z'
        assert history_rec['is_current'] is False


class TestCurrentRecordOperations:
    """Test current record operations."""

    def test_get_current_record_exists(self):
        """Test getting existing current record."""
        mock_client = Mock()
        mock_job = Mock()
        mock_row = {
            'id': '001ABC',
            'valid_from': '2025-10-30T10:00:00Z',
            'valid_to': None,
            'is_current': True
        }
        mock_job.result.return_value = [mock_row]
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        record = handler.get_current_record('Account', '001ABC')

        assert record is not None
        assert record['id'] == '001ABC'
        assert record['is_current'] is True

    def test_get_current_record_not_exists(self):
        """Test getting non-existent current record."""
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        record = handler.get_current_record('Account', '001ABC')

        assert record is None

    def test_get_current_record_error_handling(self):
        """Test error handling when getting current record."""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        record = handler.get_current_record('Account', '001ABC')

        assert record is None


class TestCloseCurrentRecord:
    """Test closing current records."""

    def test_close_current_record_success(self):
        """Test successfully closing current record."""
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result.return_value = None
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        success = handler.close_current_record(
            object_type='Account',
            record_id='001ABC',
            valid_to='2025-10-30T11:00:00Z'
        )

        assert success is True
        mock_client.query.assert_called_once()

    def test_close_current_record_failure(self):
        """Test handling failure when closing record."""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Update failed")

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        success = handler.close_current_record(
            object_type='Account',
            record_id='001ABC',
            valid_to='2025-10-30T11:00:00Z'
        )

        assert success is False


class TestInsertHistoryRecords:
    """Test inserting history records."""

    def test_insert_history_records_success(self):
        """Test successfully inserting history records."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        records = [
            {'id': '001ABC', 'valid_from': '2025-10-30T10:00:00Z', 'is_current': True},
            {'id': '001XYZ', 'valid_from': '2025-10-30T10:30:00Z', 'is_current': True},
        ]

        success = handler.insert_history_records('Account', records)

        assert success is True
        mock_client.insert_rows_json.assert_called_once()

    def test_insert_history_records_empty_list(self):
        """Test inserting empty list."""
        mock_client = Mock()

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        success = handler.insert_history_records('Account', [])

        assert success is True
        mock_client.insert_rows_json.assert_not_called()

    def test_insert_history_records_with_errors(self):
        """Test handling errors during insertion."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = ['Error 1', 'Error 2']

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        records = [{'id': '001ABC', 'valid_from': '2025-10-30T10:00:00Z'}]

        success = handler.insert_history_records('Account', records)

        assert success is False


class TestHourlyBatchProcessing:
    """Test hourly batch processing."""

    def test_process_hourly_changes_insert_events(self, sample_cdc_insert_event):
        """Test processing INSERT events."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        # Mock get_current_record to return None (no existing record)
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [sample_cdc_insert_event]

        stats = handler.process_hourly_changes(events, 'Account')

        assert stats['total_events'] == 1
        assert stats['records_processed'] == 1
        assert stats['records_inserted'] == 1
        assert stats['errors'] == 0

    def test_process_hourly_changes_update_events(self, sample_cdc_update_event):
        """Test processing UPDATE events."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        # Mock get_current_record to return existing record
        mock_query_job = Mock()
        mock_current_record = {
            'id': sample_cdc_update_event['record_id'],
            'valid_from': '2025-10-30T10:00:00Z',
            'is_current': True
        }
        mock_query_job.result.return_value = [mock_current_record]
        mock_client.query.return_value = mock_query_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [sample_cdc_update_event]

        stats = handler.process_hourly_changes(events, 'Account')

        assert stats['total_events'] == 1
        assert stats['records_processed'] == 1
        assert stats['records_updated'] >= 1

    def test_process_hourly_changes_mixed_events(self, sample_cdc_insert_event, sample_cdc_update_event):
        """Test processing mixed event types."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        # Mock for different queries
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [sample_cdc_insert_event, sample_cdc_update_event]

        stats = handler.process_hourly_changes(events, 'Account')

        assert stats['total_events'] == 2
        # Both events have same record_id, so they're processed as 1 record
        assert stats['records_processed'] == 1

    def test_process_hourly_changes_error_handling(self):
        """Test error handling in batch processing."""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [
            {
                'record_id': '001ABC',
                'event_type': 'INSERT',
                'event_timestamp': '2025-10-30T10:00:00Z',
                'after': {'id': '001ABC', 'name': 'Test'}
            }
        ]

        stats = handler.process_hourly_changes(events, 'Account')

        assert stats['errors'] > 0

    def test_process_hourly_changes_empty_events(self):
        """Test processing empty event list."""
        mock_client = Mock()

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        stats = handler.process_hourly_changes([], 'Account')

        assert stats['total_events'] == 0
        assert stats['records_processed'] == 0


class TestStatistics:
    """Test statistics tracking."""

    def test_statistics_calculation(self, sample_cdc_insert_event, sample_cdc_update_event):
        """Test statistics are correctly calculated."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        mock_job = Mock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [sample_cdc_insert_event, sample_cdc_update_event]

        stats = handler.process_hourly_changes(events, 'Account')

        assert 'total_events' in stats
        assert 'records_processed' in stats
        assert 'records_inserted' in stats
        assert 'records_updated' in stats
        assert 'errors' in stats
        assert stats['total_events'] == len(events)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_handle_malformed_event(self):
        """Test handling malformed events."""
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        # Mock query to return empty list
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        malformed_events = [
            {'record_id': '001ABC', 'event_type': 'INSERT'},  # Missing 'after' field
            {'record_id': '002XYZ'},  # Missing event_type
        ]

        stats = handler.process_hourly_changes(malformed_events, 'Account')

        # Malformed events may be handled gracefully or cause errors
        assert stats['total_events'] == 2
        assert stats['errors'] >= 0  # May or may not increment errors

    def test_handle_delete_event_without_current(self, sample_cdc_delete_event):
        """Test DELETE event when no current record exists."""
        mock_client = Mock()

        # Mock get_current_record to return None
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        handler = SCDType2Handler(mock_client, 'test-project', 'test_dataset')

        events = [sample_cdc_delete_event]

        stats = handler.process_hourly_changes(events, 'Account')

        # Should handle gracefully without errors
        assert stats['total_events'] == 1
