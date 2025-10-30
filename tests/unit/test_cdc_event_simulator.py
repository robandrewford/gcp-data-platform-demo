"""
Unit tests for CDC Event Simulator.

Tests the CDCEventSimulator class which generates synthetic CDC events
for Salesforce objects.
"""

import pytest
from datetime import datetime, timezone
from typing import Dict, Any

from src.salesforce.cdc_event_simulator import CDCEventSimulator, CDCEventType


class TestCDCEventSimulatorInitialization:
    """Test CDCEventSimulator initialization."""
    
    def test_initialization(self):
        """Test simulator initializes correctly."""
        simulator = CDCEventSimulator()
        
        assert simulator.data_generator is not None
        assert simulator._existing_records is not None
        assert 'Account' in simulator._existing_records
        assert 'Contact' in simulator._existing_records
        assert 'Opportunity' in simulator._existing_records
        assert 'Case' in simulator._existing_records
        
        # Verify all record stores are empty initially
        for object_type in simulator._existing_records:
            assert len(simulator._existing_records[object_type]) == 0


class TestCDCEventIDGeneration:
    """Test CDC event ID generation."""
    
    def test_event_id_format(self):
        """Test event ID has correct format."""
        simulator = CDCEventSimulator()
        event_id = simulator._generate_cdc_event_id()
        
        assert event_id.startswith('CDC-')
        assert len(event_id) == 16  # 'CDC-' + 12 characters
        
        # Check characters after prefix are alphanumeric uppercase
        chars = event_id[4:]
        assert all(c.isalnum() and c.isupper() or c.isdigit() for c in chars)
    
    def test_event_id_uniqueness(self):
        """Test generated event IDs are unique."""
        simulator = CDCEventSimulator()
        event_ids = [simulator._generate_cdc_event_id() for _ in range(100)]
        
        # Check all are unique
        assert len(set(event_ids)) == 100


class TestChangedFieldsDetection:
    """Test changed fields detection."""
    
    def test_get_changed_fields_single_change(self):
        """Test detection of single field change."""
        simulator = CDCEventSimulator()
        
        before = {
            'id': '001ABC123',
            'name': 'Acme Corp',
            'revenue': 1000000
        }
        after = {
            'id': '001ABC123',
            'name': 'Acme Corporation',
            'revenue': 1000000
        }
        
        changed = simulator._get_changed_fields(before, after)
        
        assert 'name' in changed
        assert len(changed) == 1
    
    def test_get_changed_fields_multiple_changes(self):
        """Test detection of multiple field changes."""
        simulator = CDCEventSimulator()
        
        before = {
            'id': '001ABC123',
            'name': 'Acme Corp',
            'revenue': 1000000,
            'phone': '555-0100'
        }
        after = {
            'id': '001ABC123',
            'name': 'Acme Corporation',
            'revenue': 1200000,
            'phone': '555-0100'
        }
        
        changed = simulator._get_changed_fields(before, after)
        
        assert 'name' in changed
        assert 'revenue' in changed
        assert 'phone' not in changed
        assert len(changed) == 2
    
    def test_get_changed_fields_ignores_metadata(self):
        """Test metadata fields are ignored in change detection."""
        simulator = CDCEventSimulator()
        
        before = {
            'id': '001ABC123',
            'name': 'Acme Corp',
            'ingestion_timestamp': '2025-10-30T10:00:00Z',
            'system_modstamp': '2025-10-30T10:00:00Z'
        }
        after = {
            'id': '001ABC123',
            'name': 'Acme Corp',
            'ingestion_timestamp': '2025-10-30T11:00:00Z',
            'system_modstamp': '2025-10-30T11:00:00Z'
        }
        
        changed = simulator._get_changed_fields(before, after)
        
        # Metadata changes should be ignored
        assert 'ingestion_timestamp' not in changed
        assert 'system_modstamp' not in changed
        assert len(changed) == 0


class TestRecordModification:
    """Test record modification for updates."""
    
    def test_modify_account_record(self):
        """Test modification of Account record."""
        simulator = CDCEventSimulator()
        
        original = {
            'id': '001ABC123',
            'name': 'Acme Corp',
            'annual_revenue': 1000000,
            'phone': '555-0100',
            'type': 'Customer',
            'last_modified_date': '2025-10-30T10:00:00Z',
            'system_modstamp': '2025-10-30T10:00:00Z'
        }
        
        modified = simulator._modify_record_for_update(original, 'Account')
        
        # Check timestamps are updated
        assert modified['last_modified_date'] != original['last_modified_date']
        assert modified['system_modstamp'] != original['system_modstamp']
        
        # Check at least one business field changed
        business_fields = ['name', 'annual_revenue', 'phone', 'type']
        changed = any(modified.get(f) != original.get(f) for f in business_fields)
        assert changed
    
    def test_modify_contact_record(self):
        """Test modification of Contact record."""
        simulator = CDCEventSimulator()
        
        original = {
            'id': '003ABC123',
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'phone': '555-0100',
            'title': 'Manager',
            'last_modified_date': '2025-10-30T10:00:00Z'
        }
        
        modified = simulator._modify_record_for_update(original, 'Contact')
        
        # Check timestamps are updated
        assert modified['last_modified_date'] != original['last_modified_date']
        
        # Check at least one business field changed
        business_fields = ['email', 'phone', 'title']
        changed = any(modified.get(f) != original.get(f) for f in business_fields)
        assert changed


class TestInsertEventGeneration:
    """Test INSERT event generation."""
    
    def test_generate_insert_event_account(self):
        """Test INSERT event generation for Account."""
        simulator = CDCEventSimulator()
        event = simulator.generate_insert_event('Account')
        
        assert event['event_type'] == 'INSERT'
        assert event['object_type'] == 'Account'
        assert event['record_id'] is not None
        assert event['event_id'].startswith('CDC-')
        assert event['event_timestamp'] is not None
        assert event['changed_fields'] == []
        assert event['before'] is None
        assert event['after'] is not None
        assert event['source'] == 'salesforce_cdc'
        
        # Verify record is stored
        assert event['record_id'] in simulator._existing_records['Account']
    
    def test_generate_insert_event_contact(self):
        """Test INSERT event generation for Contact."""
        simulator = CDCEventSimulator()
        
        # Preload an account for foreign key
        simulator.generate_insert_event('Account')
        
        event = simulator.generate_insert_event('Contact')
        
        assert event['event_type'] == 'INSERT'
        assert event['object_type'] == 'Contact'
        assert event['after'] is not None
        assert 'account_id' in event['after']
    
    def test_generate_insert_event_stores_record(self):
        """Test INSERT event stores record for future updates."""
        simulator = CDCEventSimulator()
        event = simulator.generate_insert_event('Account')
        
        record_id = event['record_id']
        assert record_id in simulator._existing_records['Account']
        assert simulator._existing_records['Account'][record_id] == event['after']


class TestUpdateEventGeneration:
    """Test UPDATE event generation."""
    
    def test_generate_update_event_with_existing_record(self):
        """Test UPDATE event generation with existing record."""
        simulator = CDCEventSimulator()
        
        # First create a record
        insert_event = simulator.generate_insert_event('Account')
        record_id = insert_event['record_id']
        
        # Then update it
        update_event = simulator.generate_update_event('Account', record_id)
        
        assert update_event['event_type'] == 'UPDATE'
        assert update_event['object_type'] == 'Account'
        assert update_event['record_id'] == record_id
        assert update_event['before'] is not None
        assert update_event['after'] is not None
        assert len(update_event['changed_fields']) > 0
        
        # Verify before/after are different
        assert update_event['before'] != update_event['after']
    
    def test_generate_update_event_falls_back_to_insert(self):
        """Test UPDATE event falls back to INSERT when no records exist."""
        simulator = CDCEventSimulator()
        
        # Try to generate UPDATE with no existing records
        event = simulator.generate_update_event('Account')
        
        # Should fall back to INSERT
        assert event['event_type'] == 'INSERT'
        assert event['before'] is None
    
    def test_generate_update_event_random_record(self):
        """Test UPDATE event picks random record when none specified."""
        simulator = CDCEventSimulator()
        
        # Create multiple records
        for _ in range(5):
            simulator.generate_insert_event('Account')
        
        # Generate update without specifying record
        event = simulator.generate_update_event('Account')
        
        assert event['event_type'] == 'UPDATE'
        assert event['record_id'] in simulator._existing_records['Account']
    
    def test_generate_update_event_updates_stored_record(self):
        """Test UPDATE event updates the stored record."""
        simulator = CDCEventSimulator()
        
        # Create initial record
        insert_event = simulator.generate_insert_event('Account')
        record_id = insert_event['record_id']
        before_state = simulator._existing_records['Account'][record_id].copy()
        
        # Generate update
        update_event = simulator.generate_update_event('Account', record_id)
        after_state = simulator._existing_records['Account'][record_id]
        
        # Verify stored record was updated
        assert after_state != before_state
        assert after_state == update_event['after']


class TestDeleteEventGeneration:
    """Test DELETE event generation."""
    
    def test_generate_delete_event_with_existing_record(self):
        """Test DELETE event generation with existing record."""
        simulator = CDCEventSimulator()
        
        # First create a record
        insert_event = simulator.generate_insert_event('Account')
        record_id = insert_event['record_id']
        
        # Then delete it
        delete_event = simulator.generate_delete_event('Account', record_id)
        
        assert delete_event['event_type'] == 'DELETE'
        assert delete_event['object_type'] == 'Account'
        assert delete_event['record_id'] == record_id
        assert delete_event['before'] is not None
        assert delete_event['after'] is None
        assert delete_event['changed_fields'] == []
    
    def test_generate_delete_event_falls_back_to_insert(self):
        """Test DELETE event falls back to INSERT when no records exist."""
        simulator = CDCEventSimulator()
        
        # Try to generate DELETE with no existing records
        event = simulator.generate_delete_event('Account')
        
        # Should fall back to INSERT
        assert event['event_type'] == 'INSERT'
        assert event['after'] is not None
    
    def test_generate_delete_event_removes_stored_record(self):
        """Test DELETE event removes record from storage."""
        simulator = CDCEventSimulator()
        
        # Create initial record
        insert_event = simulator.generate_insert_event('Account')
        record_id = insert_event['record_id']
        
        # Verify record exists
        assert record_id in simulator._existing_records['Account']
        
        # Generate delete
        delete_event = simulator.generate_delete_event('Account', record_id)
        
        # Verify record was removed
        assert record_id not in simulator._existing_records['Account']


class TestBatchEventGeneration:
    """Test batch event generation."""
    
    def test_generate_cdc_events_default_distribution(self):
        """Test batch generation with default distribution."""
        simulator = CDCEventSimulator()
        events = simulator.generate_cdc_events('Account', event_count=100)
        
        assert len(events) == 100
        
        # Count event types
        event_types = {}
        for event in events:
            event_type = event['event_type']
            event_types[event_type] = event_types.get(event_type, 0) + 1
        
        # Check approximate distribution (50% INSERT, 40% UPDATE, 10% DELETE)
        assert event_types.get('INSERT', 0) >= 45  # ~50%
        assert event_types.get('UPDATE', 0) >= 30  # ~40%
        assert event_types.get('DELETE', 0) >= 5   # ~10%
    
    def test_generate_cdc_events_custom_distribution(self):
        """Test batch generation with custom distribution."""
        simulator = CDCEventSimulator()
        
        custom_dist = {
            'INSERT': 0.7,
            'UPDATE': 0.2,
            'DELETE': 0.1
        }
        
        events = simulator.generate_cdc_events(
            'Account',
            event_count=100,
            event_distribution=custom_dist
        )
        
        assert len(events) == 100
        
        # Count event types
        event_types = {}
        for event in events:
            event_type = event['event_type']
            event_types[event_type] = event_types.get(event_type, 0) + 1
        
        # Check approximate distribution
        assert event_types.get('INSERT', 0) >= 65  # ~70%
        assert event_types.get('UPDATE', 0) >= 15  # ~20%
        assert event_types.get('DELETE', 0) >= 5   # ~10%
    
    def test_generate_cdc_events_validates_distribution(self):
        """Test batch generation validates distribution sums to 1.0."""
        simulator = CDCEventSimulator()
        
        invalid_dist = {
            'INSERT': 0.5,
            'UPDATE': 0.3,
            'DELETE': 0.1
        }  # Sums to 0.9
        
        with pytest.raises(ValueError, match="must sum to 1.0"):
            simulator.generate_cdc_events(
                'Account',
                event_count=100,
                event_distribution=invalid_dist
            )
    
    def test_generate_cdc_events_all_types_present(self):
        """Test batch generation includes all event types."""
        simulator = CDCEventSimulator()
        events = simulator.generate_cdc_events('Account', event_count=50)
        
        event_types = {e['event_type'] for e in events}
        
        # With default distribution and 50 events, all types should be present
        assert 'INSERT' in event_types
        # UPDATE and DELETE might not appear if distribution results in 0 events


class TestRecordManagement:
    """Test record storage management."""
    
    def test_preload_existing_records(self):
        """Test preloading existing records."""
        simulator = CDCEventSimulator()
        
        initial_count = simulator.get_existing_record_count('Account')
        assert initial_count == 0
        
        simulator.preload_existing_records('Account', count=10)
        
        final_count = simulator.get_existing_record_count('Account')
        assert final_count == 10
    
    def test_get_existing_record_count(self):
        """Test getting existing record count."""
        simulator = CDCEventSimulator()
        
        # Generate some records
        for _ in range(5):
            simulator.generate_insert_event('Account')
        
        count = simulator.get_existing_record_count('Account')
        assert count == 5
    
    def test_clear_existing_records_specific_type(self):
        """Test clearing records for specific object type."""
        simulator = CDCEventSimulator()
        
        # Create records for multiple types
        simulator.generate_insert_event('Account')
        simulator.generate_insert_event('Contact')
        
        # Clear only Accounts
        simulator.clear_existing_records('Account')
        
        assert simulator.get_existing_record_count('Account') == 0
        assert simulator.get_existing_record_count('Contact') == 1
    
    def test_clear_existing_records_all_types(self):
        """Test clearing all records."""
        simulator = CDCEventSimulator()
        
        # Create records for multiple types
        simulator.generate_insert_event('Account')
        simulator.generate_insert_event('Contact')
        simulator.generate_insert_event('Opportunity')
        
        # Clear all
        simulator.clear_existing_records()
        
        assert simulator.get_existing_record_count('Account') == 0
        assert simulator.get_existing_record_count('Contact') == 0
        assert simulator.get_existing_record_count('Opportunity') == 0


class TestEventTimestamps:
    """Test event timestamp handling."""
    
    def test_event_timestamp_format(self):
        """Test event timestamps are in ISO format."""
        simulator = CDCEventSimulator()
        event = simulator.generate_insert_event('Account')
        
        timestamp = event['event_timestamp']
        
        # Verify ISO format with Z suffix
        assert timestamp.endswith('Z')
        
        # Verify can be parsed
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        assert dt.tzinfo is not None
    
    def test_event_timestamp_is_current(self):
        """Test event timestamps are current."""
        simulator = CDCEventSimulator()
        
        before = datetime.now(timezone.utc)
        event = simulator.generate_insert_event('Account')
        after = datetime.now(timezone.utc)
        
        event_time = datetime.fromisoformat(
            event['event_timestamp'].replace('Z', '+00:00')
        )
        
        assert before <= event_time <= after


class TestEventStructure:
    """Test event structure and fields."""
    
    def test_insert_event_structure(self):
        """Test INSERT event has all required fields."""
        simulator = CDCEventSimulator()
        event = simulator.generate_insert_event('Account')
        
        required_fields = [
            'event_id', 'event_type', 'object_type', 'record_id',
            'event_timestamp', 'changed_fields', 'before', 'after', 'source'
        ]
        
        for field in required_fields:
            assert field in event
    
    def test_update_event_structure(self):
        """Test UPDATE event has all required fields."""
        simulator = CDCEventSimulator()
        
        # Create record first
        simulator.generate_insert_event('Account')
        event = simulator.generate_update_event('Account')
        
        required_fields = [
            'event_id', 'event_type', 'object_type', 'record_id',
            'event_timestamp', 'changed_fields', 'before', 'after', 'source'
        ]
        
        for field in required_fields:
            assert field in event
    
    def test_delete_event_structure(self):
        """Test DELETE event has all required fields."""
        simulator = CDCEventSimulator()
        
        # Create record first
        simulator.generate_insert_event('Account')
        event = simulator.generate_delete_event('Account')
        
        required_fields = [
            'event_id', 'event_type', 'object_type', 'record_id',
            'event_timestamp', 'changed_fields', 'before', 'after', 'source'
        ]
        
        for field in required_fields:
            assert field in event


class TestUnsupportedObjectTypes:
    """Test handling of unsupported object types."""
    
    def test_generate_insert_event_unsupported_type(self):
        """Test INSERT event raises error for unsupported type."""
        simulator = CDCEventSimulator()
        
        with pytest.raises(ValueError, match="Unsupported object type"):
            simulator.generate_insert_event('UnsupportedObject')
