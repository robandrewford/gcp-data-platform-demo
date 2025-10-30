"""
Unit tests for CDC Event Parser.

Tests the CDCEventParser class which parses CDC events from Pub/Sub messages
and extracts data for BigQuery and SCD Type 2 processing.
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock
from typing import Dict, Any

from pipelines.utils.cdc_event_parser import CDCEventParser, BeamCDCEventParser


class TestCDCEventParserInitialization:
    """Test CDCEventParser initialization."""
    
    def test_initialization(self):
        """Test parser initializes correctly."""
        parser = CDCEventParser()
        
        assert parser.parsed_count == 0
        assert parser.error_count == 0


class TestPubSubMessageParsing:
    """Test Pub/Sub message parsing."""
    
    def test_parse_pubsub_message_success(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test successful parsing of Pub/Sub message."""
        parser = CDCEventParser()
        
        # Create mock Pub/Sub message
        message = Mock()
        message.data = json.dumps(sample_cdc_insert_event).encode('utf-8')
        message.message_id = 'test-message-123'
        message.publish_time = datetime.now(timezone.utc)
        message.attributes = {'source': 'test'}
        
        parsed_event = parser.parse_pubsub_message(message)
        
        assert parsed_event is not None
        assert parsed_event['event_type'] == 'INSERT'
        assert parsed_event['object_type'] == 'Account'
        assert parsed_event['_pubsub_message_id'] == 'test-message-123'
        assert '_pubsub_publish_time' in parsed_event
        assert '_pubsub_attributes' in parsed_event
        assert parser.parsed_count == 1
        assert parser.error_count == 0
    
    def test_parse_pubsub_message_invalid_json(self):
        """Test parsing fails gracefully with invalid JSON."""
        parser = CDCEventParser()
        
        # Create mock message with invalid JSON
        message = Mock()
        message.data = b'invalid json {{'
        message.message_id = 'test-message-456'
        
        parsed_event = parser.parse_pubsub_message(message)
        
        assert parsed_event is None
        assert parser.parsed_count == 0
        assert parser.error_count == 1
    
    def test_parse_pubsub_message_no_attributes(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test parsing message without attributes."""
        parser = CDCEventParser()
        
        message = Mock()
        message.data = json.dumps(sample_cdc_insert_event).encode('utf-8')
        message.message_id = 'test-message-789'
        message.publish_time = datetime.now(timezone.utc)
        message.attributes = None
        
        parsed_event = parser.parse_pubsub_message(message)
        
        assert parsed_event is not None
        assert '_pubsub_attributes' not in parsed_event


class TestRecordExtraction:
    """Test record extraction for BigQuery."""
    
    def test_extract_record_insert_event(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test extracting record from INSERT event."""
        parser = CDCEventParser()
        
        record = parser.extract_record_for_bigquery(sample_cdc_insert_event)
        
        assert record is not None
        assert record['id'] == sample_cdc_insert_event['after']['id']
        assert 'name' in record
        assert 'ingestion_timestamp' in record
        assert record['source'] == 'salesforce_cdc'
        assert record['_cdc_event_id'] == sample_cdc_insert_event['event_id']
        assert record['_cdc_event_type'] == 'INSERT'
    
    def test_extract_record_update_event(self, sample_cdc_update_event: Dict[str, Any]):
        """Test extracting record from UPDATE event."""
        parser = CDCEventParser()
        
        record = parser.extract_record_for_bigquery(sample_cdc_update_event)
        
        assert record is not None
        assert record['id'] == sample_cdc_update_event['after']['id']
        assert record['name'] == sample_cdc_update_event['after']['name']
        assert record['_cdc_event_type'] == 'UPDATE'
    
    def test_extract_record_delete_event(self, sample_cdc_delete_event: Dict[str, Any]):
        """Test extracting record from DELETE event."""
        parser = CDCEventParser()
        
        record = parser.extract_record_for_bigquery(sample_cdc_delete_event)
        
        assert record is not None
        assert record['id'] == sample_cdc_delete_event['before']['id']
        assert record['name'] == sample_cdc_delete_event['before']['name']
        assert record['_cdc_event_type'] == 'DELETE'
    
    def test_extract_record_without_metadata(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test extracting record without metadata."""
        parser = CDCEventParser()
        
        record = parser.extract_record_for_bigquery(
            sample_cdc_insert_event,
            include_metadata=False
        )
        
        assert record is not None
        assert 'ingestion_timestamp' not in record
        assert '_cdc_event_id' not in record
        assert '_cdc_event_type' not in record
    
    def test_extract_record_unknown_event_type(self):
        """Test extraction fails for unknown event type."""
        parser = CDCEventParser()
        
        invalid_event = {
            'event_type': 'UNKNOWN',
            'object_type': 'Account',
            'after': {'id': '001ABC', 'name': 'Test'}
        }
        
        record = parser.extract_record_for_bigquery(invalid_event)
        
        assert record is None
    
    def test_extract_record_includes_pubsub_metadata(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test record includes Pub/Sub metadata when available."""
        parser = CDCEventParser()
        
        # Add Pub/Sub metadata to event
        event_with_metadata = sample_cdc_insert_event.copy()
        event_with_metadata['_pubsub_message_id'] = 'msg-123'
        
        record = parser.extract_record_for_bigquery(event_with_metadata)
        
        assert record is not None
        assert record['_pubsub_message_id'] == 'msg-123'


class TestHistoryRecordExtraction:
    """Test history record extraction for SCD Type 2."""
    
    def test_extract_history_record_insert(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test extracting history record from INSERT event."""
        parser = CDCEventParser()
        
        history = parser.extract_history_record(sample_cdc_insert_event)
        
        assert history is not None
        assert history['id'] == sample_cdc_insert_event['record_id']
        assert history['valid_from'] == sample_cdc_insert_event['event_timestamp']
        assert history['valid_to'] is None
        assert history['is_current'] is True
        assert history['change_type'] == 'INSERT'
        assert 'record_data' in history
        assert 'ingestion_timestamp' in history
    
    def test_extract_history_record_update(self, sample_cdc_update_event: Dict[str, Any]):
        """Test extracting history record from UPDATE event."""
        parser = CDCEventParser()
        
        history = parser.extract_history_record(sample_cdc_update_event)
        
        assert history is not None
        assert history['change_type'] == 'UPDATE'
        assert 'changed_fields' in history
        
        # Verify changed_fields is JSON string
        changed_fields = json.loads(history['changed_fields'])
        assert isinstance(changed_fields, list)
        assert len(changed_fields) > 0
    
    def test_extract_history_record_delete(self, sample_cdc_delete_event: Dict[str, Any]):
        """Test extracting history record from DELETE event."""
        parser = CDCEventParser()
        
        history = parser.extract_history_record(sample_cdc_delete_event)
        
        assert history is not None
        assert history['change_type'] == 'DELETE'
        
        # Verify record_data contains before state
        record_data = json.loads(history['record_data'])
        assert record_data['id'] == sample_cdc_delete_event['before']['id']
    
    def test_extract_history_record_unknown_type(self):
        """Test history extraction returns None for unknown type."""
        parser = CDCEventParser()
        
        invalid_event = {
            'event_type': 'UNKNOWN',
            'record_id': '001ABC'
        }
        
        history = parser.extract_history_record(invalid_event)
        
        assert history is None


class TestChangedFieldsExtraction:
    """Test changed fields extraction."""
    
    def test_extract_changed_fields(self, sample_cdc_update_event: Dict[str, Any]):
        """Test extracting changed fields from event."""
        parser = CDCEventParser()
        
        changed_fields = parser.extract_changed_fields(sample_cdc_update_event)
        
        assert isinstance(changed_fields, list)
        assert len(changed_fields) > 0
        assert 'name' in changed_fields
    
    def test_extract_changed_fields_empty(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test extracting changed fields from INSERT event."""
        parser = CDCEventParser()
        
        changed_fields = parser.extract_changed_fields(sample_cdc_insert_event)
        
        assert isinstance(changed_fields, list)
        assert len(changed_fields) == 0


class TestBeforeAfterExtraction:
    """Test before/after state extraction."""
    
    def test_extract_before_after_insert(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test extracting before/after from INSERT event."""
        parser = CDCEventParser()
        
        before, after = parser.extract_before_after(sample_cdc_insert_event)
        
        assert before is None
        assert after is not None
        assert after['id'] == sample_cdc_insert_event['after']['id']
    
    def test_extract_before_after_update(self, sample_cdc_update_event: Dict[str, Any]):
        """Test extracting before/after from UPDATE event."""
        parser = CDCEventParser()
        
        before, after = parser.extract_before_after(sample_cdc_update_event)
        
        assert before is not None
        assert after is not None
        assert before['id'] == after['id']
        assert before['name'] != after['name']
    
    def test_extract_before_after_delete(self, sample_cdc_delete_event: Dict[str, Any]):
        """Test extracting before/after from DELETE event."""
        parser = CDCEventParser()
        
        before, after = parser.extract_before_after(sample_cdc_delete_event)
        
        assert before is not None
        assert after is None
        assert before['id'] == sample_cdc_delete_event['before']['id']


class TestFieldNameNormalization:
    """Test field name normalization."""
    
    def test_normalize_field_names_camel_case(self):
        """Test normalizing camelCase field names."""
        parser = CDCEventParser()
        
        record = {
            'id': '001ABC',
            'firstName': 'John',
            'lastName': 'Doe',
            'emailAddress': 'john@example.com'
        }
        
        normalized = parser.normalize_field_names(record)
        
        assert 'first_name' in normalized
        assert 'last_name' in normalized
        assert 'email_address' in normalized
        assert normalized['first_name'] == 'John'
    
    def test_normalize_field_names_already_normalized(self):
        """Test normalizing already snake_case fields."""
        parser = CDCEventParser()
        
        record = {
            'id': '001ABC',
            'first_name': 'John',
            'last_name': 'Doe'
        }
        
        normalized = parser.normalize_field_names(record)
        
        assert normalized == record


class TestJSONFieldFormatting:
    """Test JSON field formatting."""
    
    def test_format_for_json_field_dict(self):
        """Test formatting dictionary for JSON field."""
        parser = CDCEventParser()
        
        value = {'city': 'San Francisco', 'state': 'CA'}
        formatted = parser.format_for_json_field(value)
        
        assert isinstance(formatted, str)
        parsed = json.loads(formatted)
        assert parsed == value
    
    def test_format_for_json_field_list(self):
        """Test formatting list for JSON field."""
        parser = CDCEventParser()
        
        value = ['field1', 'field2', 'field3']
        formatted = parser.format_for_json_field(value)
        
        assert isinstance(formatted, str)
        parsed = json.loads(formatted)
        assert parsed == value
    
    def test_format_for_json_field_simple_value(self):
        """Test formatting simple value for JSON field."""
        parser = CDCEventParser()
        
        value = 'simple string'
        formatted = parser.format_for_json_field(value)
        
        assert formatted == 'simple string'


class TestStatistics:
    """Test parser statistics tracking."""
    
    def test_get_statistics_initial(self):
        """Test getting initial statistics."""
        parser = CDCEventParser()
        
        stats = parser.get_statistics()
        
        assert stats['parsed_count'] == 0
        assert stats['error_count'] == 0
        assert stats['success_rate'] == 0.0
    
    def test_get_statistics_after_parsing(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test statistics after successful parsing."""
        parser = CDCEventParser()
        
        # Parse valid message
        message = Mock()
        message.data = json.dumps(sample_cdc_insert_event).encode('utf-8')
        message.message_id = 'test-123'
        message.publish_time = datetime.now(timezone.utc)
        message.attributes = {}
        
        parser.parse_pubsub_message(message)
        
        stats = parser.get_statistics()
        
        assert stats['parsed_count'] == 1
        assert stats['error_count'] == 0
        assert stats['success_rate'] == 1.0
    
    def test_get_statistics_with_errors(self):
        """Test statistics with parsing errors."""
        parser = CDCEventParser()
        
        # Parse invalid message
        message = Mock()
        message.data = b'invalid json'
        message.message_id = 'test-456'
        
        parser.parse_pubsub_message(message)
        
        stats = parser.get_statistics()
        
        assert stats['parsed_count'] == 0
        assert stats['error_count'] == 1
        assert stats['success_rate'] == 0.0
    
    def test_get_statistics_mixed(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test statistics with mixed results."""
        parser = CDCEventParser()
        
        # Parse valid message
        valid_message = Mock()
        valid_message.data = json.dumps(sample_cdc_insert_event).encode('utf-8')
        valid_message.message_id = 'valid-123'
        valid_message.publish_time = datetime.now(timezone.utc)
        valid_message.attributes = {}
        
        parser.parse_pubsub_message(valid_message)
        
        # Parse invalid message
        invalid_message = Mock()
        invalid_message.data = b'invalid'
        invalid_message.message_id = 'invalid-456'
        
        parser.parse_pubsub_message(invalid_message)
        
        stats = parser.get_statistics()
        
        assert stats['parsed_count'] == 1
        assert stats['error_count'] == 1
        assert stats['success_rate'] == 0.5
    
    def test_reset_statistics(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test resetting statistics."""
        parser = CDCEventParser()
        
        # Parse some messages
        message = Mock()
        message.data = json.dumps(sample_cdc_insert_event).encode('utf-8')
        message.message_id = 'test-123'
        message.publish_time = datetime.now(timezone.utc)
        message.attributes = {}
        
        parser.parse_pubsub_message(message)
        
        # Reset
        parser.reset_statistics()
        
        stats = parser.get_statistics()
        assert stats['parsed_count'] == 0
        assert stats['error_count'] == 0


class TestBeamCDCEventParser:
    """Test Beam-specific CDC event parser functions."""
    
    def test_parse_json(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test parsing JSON bytes."""
        json_bytes = json.dumps(sample_cdc_insert_event).encode('utf-8')
        
        parsed = BeamCDCEventParser.parse_json(json_bytes)
        
        assert parsed == sample_cdc_insert_event
    
    def test_extract_record_insert(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test extracting record from INSERT event for Beam."""
        record = BeamCDCEventParser.extract_record(sample_cdc_insert_event)
        
        assert record is not None
        assert record['id'] == sample_cdc_insert_event['after']['id']
        assert 'ingestion_timestamp' in record
        assert record['source'] == 'salesforce_cdc'
        assert record['_cdc_event_type'] == 'INSERT'
    
    def test_extract_record_update(self, sample_cdc_update_event: Dict[str, Any]):
        """Test extracting record from UPDATE event for Beam."""
        record = BeamCDCEventParser.extract_record(sample_cdc_update_event)
        
        assert record is not None
        assert record['_cdc_event_type'] == 'UPDATE'
    
    def test_extract_record_delete(self, sample_cdc_delete_event: Dict[str, Any]):
        """Test extracting record from DELETE event for Beam."""
        record = BeamCDCEventParser.extract_record(sample_cdc_delete_event)
        
        assert record is not None
        assert record['_cdc_event_type'] == 'DELETE'
    
    def test_extract_record_unknown_type(self):
        """Test extracting record with unknown event type."""
        invalid_event = {
            'event_type': 'UNKNOWN'
        }
        
        record = BeamCDCEventParser.extract_record(invalid_event)
        
        # Should return empty record with metadata
        assert record is not None
        assert record['source'] == 'salesforce_cdc'
    
    def test_add_processing_timestamp(self):
        """Test adding processing timestamp to record."""
        record = {'id': '001ABC', 'name': 'Test'}
        
        before = datetime.now(timezone.utc)
        result = BeamCDCEventParser.add_processing_timestamp(record)
        after = datetime.now(timezone.utc)
        
        assert '_processing_timestamp' in result
        
        # Verify timestamp is current
        timestamp = datetime.fromisoformat(
            result['_processing_timestamp'].replace('Z', '+00:00')
        )
        assert before <= timestamp <= after
