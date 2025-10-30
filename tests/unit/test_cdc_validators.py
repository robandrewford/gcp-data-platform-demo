"""
Tests for CDC Event Validators.

Tests data quality validation rules for CDC events including type validation,
null checks, format validation, and error tracking.
"""

import pytest
from typing import Dict, Any
from pipelines.utils.cdc_validators import CDCValidator


class TestCDCValidator:
    """Test suite for CDCValidator."""
    
    def test_initialization(self):
        """Test validator initialization with default threshold."""
        validator = CDCValidator()
        assert validator.alert_threshold == 0.05
        assert validator.validation_stats['total'] == 0
        assert validator.validation_stats['valid'] == 0
        assert validator.validation_stats['invalid'] == 0
    
    def test_initialization_custom_threshold(self):
        """Test validator initialization with custom threshold."""
        validator = CDCValidator(alert_threshold=0.10)
        assert validator.alert_threshold == 0.10
    
    def test_validate_event_type_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test validation of valid event type."""
        validator = CDCValidator()
        is_valid, error = validator.validate_event_type(sample_cdc_insert_event)
        assert is_valid is True
        assert error is None
    
    def test_validate_event_type_invalid(self):
        """Test validation of invalid event type."""
        validator = CDCValidator()
        event = {'event_type': 'INVALID_TYPE'}
        is_valid, error = validator.validate_event_type(event)
        assert is_valid is False
        assert 'Invalid event_type' in error
    
    def test_validate_event_type_missing(self):
        """Test validation of missing event type."""
        validator = CDCValidator()
        event = {}
        is_valid, error = validator.validate_event_type(event)
        assert is_valid is False
        assert 'Missing required field: event_type' in error
    
    def test_validate_object_type_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test validation of valid object type."""
        validator = CDCValidator()
        is_valid, error = validator.validate_object_type(sample_cdc_insert_event)
        assert is_valid is True
        assert error is None
    
    def test_validate_object_type_invalid(self):
        """Test validation of invalid object type."""
        validator = CDCValidator()
        event = {'object_type': 'InvalidObject'}
        is_valid, error = validator.validate_object_type(event)
        assert is_valid is False
        assert 'Invalid object_type' in error
    
    def test_validate_object_type_missing(self):
        """Test validation of missing object type."""
        validator = CDCValidator()
        event = {}
        is_valid, error = validator.validate_object_type(event)
        assert is_valid is False
        assert 'Missing required field: object_type' in error
    
    def test_validate_record_id_format_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test validation of valid record ID format."""
        validator = CDCValidator()
        is_valid, error = validator.validate_record_id_format(sample_cdc_insert_event)
        assert is_valid is True
        assert error is None
    
    def test_validate_record_id_format_invalid(self):
        """Test validation of invalid record ID format."""
        validator = CDCValidator()
        event = {'record_id': 'invalid-id'}
        is_valid, error = validator.validate_record_id_format(event)
        assert is_valid is False
        assert 'Invalid record_id format' in error
    
    def test_validate_record_id_format_too_short(self):
        """Test validation of too short record ID."""
        validator = CDCValidator()
        event = {'record_id': '001ABC'}
        is_valid, error = validator.validate_record_id_format(event)
        assert is_valid is False
        assert 'Invalid record_id format' in error
    
    def test_validate_record_id_format_missing(self):
        """Test validation of missing record ID."""
        validator = CDCValidator()
        event = {}
        is_valid, error = validator.validate_record_id_format(event)
        assert is_valid is False
        assert 'Missing required field: record_id' in error
    
    def test_validate_timestamp_format_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test validation of valid timestamp format."""
        validator = CDCValidator()
        is_valid, error = validator.validate_timestamp_format(sample_cdc_insert_event)
        assert is_valid is True
        assert error is None
    
    def test_validate_timestamp_format_invalid(self):
        """Test validation of invalid timestamp format."""
        validator = CDCValidator()
        event = {'event_timestamp': 'not-a-timestamp'}
        is_valid, error = validator.validate_timestamp_format(event)
        assert is_valid is False
        assert 'Invalid event_timestamp format' in error
    
    def test_validate_timestamp_format_missing(self):
        """Test validation of missing timestamp."""
        validator = CDCValidator()
        event = {}
        is_valid, error = validator.validate_timestamp_format(event)
        assert is_valid is False
        assert 'Missing required field: event_timestamp' in error
    
    def test_validate_timestamp_future(self):
        """Test validation of future timestamp."""
        validator = CDCValidator()
        event = {'event_timestamp': '2099-12-31T23:59:59Z'}
        is_valid, error = validator.validate_timestamp_format(event)
        assert is_valid is False
        assert 'in the future' in error
    
    def test_validate_null_required_fields_insert_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test null validation for valid INSERT event."""
        validator = CDCValidator()
        is_valid, errors = validator.validate_null_required_fields(sample_cdc_insert_event)
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_null_required_fields_insert_missing_after(self):
        """Test null validation for INSERT event missing 'after' data."""
        validator = CDCValidator()
        event = {
            'event_type': 'INSERT',
            'object_type': 'Account',
            'after': None
        }
        is_valid, errors = validator.validate_null_required_fields(event)
        assert is_valid is False
        assert len(errors) > 0
        assert any('Missing' in err and 'after' in err for err in errors)
    
    def test_validate_null_required_fields_insert_null_id(self):
        """Test null validation for INSERT event with null ID."""
        validator = CDCValidator()
        event = {
            'event_type': 'INSERT',
            'object_type': 'Account',
            'after': {
                'id': None,
                'name': 'Test',
                'created_date': '2025-10-30T15:30:00Z',
                'last_modified_date': '2025-10-30T15:30:00Z'
            }
        }
        is_valid, errors = validator.validate_null_required_fields(event)
        assert is_valid is False
        assert any('id' in err for err in errors)
    
    def test_validate_null_required_fields_update_valid(self, sample_cdc_update_event: Dict[str, Any]):
        """Test null validation for valid UPDATE event."""
        validator = CDCValidator()
        is_valid, errors = validator.validate_null_required_fields(sample_cdc_update_event)
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_null_required_fields_update_missing_before(self):
        """Test null validation for UPDATE event missing 'before' data."""
        validator = CDCValidator()
        event = {
            'event_type': 'UPDATE',
            'object_type': 'Account',
            'before': None,
            'after': {'id': '001ABC', 'name': 'Test'}
        }
        is_valid, errors = validator.validate_null_required_fields(event)
        assert is_valid is False
        assert any('before' in err for err in errors)
    
    def test_validate_null_required_fields_delete_valid(self, sample_cdc_delete_event: Dict[str, Any]):
        """Test null validation for valid DELETE event."""
        validator = CDCValidator()
        is_valid, errors = validator.validate_null_required_fields(sample_cdc_delete_event)
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_field_types_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test field type validation for valid event."""
        validator = CDCValidator()
        is_valid, errors = validator.validate_field_types(sample_cdc_insert_event)
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_field_types_wrong_type(self):
        """Test field type validation for wrong type."""
        validator = CDCValidator()
        event = {
            'object_type': 'Account',
            'after': {
                'id': 123,  # Should be string
                'name': 'Test'
            }
        }
        is_valid, errors = validator.validate_field_types(event)
        assert is_valid is False
        assert any('wrong type' in err.lower() for err in errors)
    
    def test_validate_changed_fields_update_valid(self, sample_cdc_update_event: Dict[str, Any]):
        """Test changed fields validation for valid UPDATE event."""
        validator = CDCValidator()
        is_valid, error = validator.validate_changed_fields(sample_cdc_update_event)
        assert is_valid is True
        assert error is None
    
    def test_validate_changed_fields_update_empty(self):
        """Test changed fields validation for UPDATE with empty changed_fields."""
        validator = CDCValidator()
        event = {
            'event_type': 'UPDATE',
            'changed_fields': []
        }
        is_valid, error = validator.validate_changed_fields(event)
        assert is_valid is False
        assert 'cannot be empty' in error
    
    def test_validate_changed_fields_update_missing(self):
        """Test changed fields validation for UPDATE with missing changed_fields."""
        validator = CDCValidator()
        event = {
            'event_type': 'UPDATE'
        }
        is_valid, error = validator.validate_changed_fields(event)
        assert is_valid is False
        assert 'Missing' in error
    
    def test_validate_changed_fields_insert_skipped(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test that changed fields validation is skipped for INSERT events."""
        validator = CDCValidator()
        is_valid, error = validator.validate_changed_fields(sample_cdc_insert_event)
        assert is_valid is True
        assert error is None
    
    def test_validate_foreign_keys_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test foreign key validation for valid event."""
        # Modify event to include foreign key
        sample_cdc_insert_event['after']['account_id'] = '001000000000002AAA'
        sample_cdc_insert_event['object_type'] = 'Contact'
        
        validator = CDCValidator()
        is_valid, errors = validator.validate_foreign_keys(sample_cdc_insert_event)
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_foreign_keys_invalid_format(self):
        """Test foreign key validation for invalid format."""
        validator = CDCValidator()
        event = {
            'object_type': 'Contact',
            'after': {
                'account_id': 'invalid-id'
            }
        }
        is_valid, errors = validator.validate_foreign_keys(event)
        assert is_valid is False
        assert any('invalid format' in err.lower() for err in errors)
    
    def test_validate_event_fully_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test complete validation for valid event."""
        validator = CDCValidator()
        is_valid, errors = validator.validate_event(sample_cdc_insert_event)
        assert is_valid is True
        assert len(errors) == 0
        assert validator.validation_stats['total'] == 1
        assert validator.validation_stats['valid'] == 1
        assert validator.validation_stats['invalid'] == 0
    
    def test_validate_event_multiple_errors(self):
        """Test complete validation for event with multiple errors."""
        validator = CDCValidator()
        event = {
            'event_type': 'INVALID',
            'object_type': 'InvalidObject',
            'record_id': 'bad-id',
            'event_timestamp': 'bad-timestamp'
        }
        is_valid, errors = validator.validate_event(event)
        assert is_valid is False
        assert len(errors) > 1
        assert validator.validation_stats['total'] == 1
        assert validator.validation_stats['valid'] == 0
        assert validator.validation_stats['invalid'] == 1
    
    def test_get_error_rate_no_events(self):
        """Test error rate calculation with no events."""
        validator = CDCValidator()
        error_rate = validator.get_error_rate()
        assert error_rate == 0.0
    
    def test_get_error_rate_all_valid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test error rate with all valid events."""
        validator = CDCValidator()
        validator.validate_event(sample_cdc_insert_event)
        error_rate = validator.get_error_rate()
        assert error_rate == 0.0
    
    def test_get_error_rate_some_invalid(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test error rate with some invalid events."""
        validator = CDCValidator()
        # Validate one good event
        validator.validate_event(sample_cdc_insert_event)
        # Validate one bad event
        validator.validate_event({'event_type': 'INVALID'})
        error_rate = validator.get_error_rate()
        assert error_rate == 0.5
    
    def test_should_alert_below_threshold(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test alert threshold with error rate below threshold."""
        validator = CDCValidator(alert_threshold=0.10)
        # 9 valid, 1 invalid = 10% error rate, equals threshold
        for _ in range(9):
            validator.validate_event(sample_cdc_insert_event)
        validator.validate_event({'event_type': 'INVALID'})
        assert validator.should_alert() is False
    
    def test_should_alert_at_threshold(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test alert threshold with error rate at threshold."""
        validator = CDCValidator(alert_threshold=0.10)
        # 9 valid, 1 invalid = 10% error rate
        for _ in range(9):
            validator.validate_event(sample_cdc_insert_event)
        validator.validate_event({'event_type': 'INVALID'})
        # At threshold, should not alert (only exceeds)
        assert validator.should_alert() is False
    
    def test_should_alert_above_threshold(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test alert threshold with error rate above threshold."""
        validator = CDCValidator(alert_threshold=0.05)
        # 1 valid, 1 invalid = 50% error rate
        validator.validate_event(sample_cdc_insert_event)
        validator.validate_event({'event_type': 'INVALID'})
        assert validator.should_alert() is True
    
    def test_get_statistics(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test statistics retrieval."""
        validator = CDCValidator(alert_threshold=0.05)
        validator.validate_event(sample_cdc_insert_event)
        validator.validate_event({'event_type': 'INVALID'})
        
        stats = validator.get_statistics()
        assert stats['total_events'] == 2
        assert stats['valid_events'] == 1
        assert stats['invalid_events'] == 1
        assert stats['error_rate'] == 0.5
        assert stats['exceeds_threshold'] is True
        assert 'error_types' in stats
    
    def test_reset_statistics(self, sample_cdc_insert_event: Dict[str, Any]):
        """Test statistics reset."""
        validator = CDCValidator()
        validator.validate_event(sample_cdc_insert_event)
        validator.validate_event({'event_type': 'INVALID'})
        
        assert validator.validation_stats['total'] == 2
        
        validator.reset_statistics()
        
        assert validator.validation_stats['total'] == 0
        assert validator.validation_stats['valid'] == 0
        assert validator.validation_stats['invalid'] == 0
        assert len(validator.validation_stats['error_types']) == 0
    
    def test_error_types_tracking(self):
        """Test that error types are tracked correctly."""
        validator = CDCValidator()
        
        # Generate errors of different types
        validator.validate_event({'event_type': 'INVALID'})
        validator.validate_event({'event_type': 'INSERT', 'object_type': 'BadType'})
        validator.validate_event({'event_type': 'INSERT', 'object_type': 'Account', 'record_id': 'bad'})
        
        stats = validator.get_statistics()
        assert 'event_type' in stats['error_types']
        assert 'object_type' in stats['error_types']
        assert 'record_id' in stats['error_types']
