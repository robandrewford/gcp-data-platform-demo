"""
CDC Event Validators

Data quality validation rules for CDC events.
Implements type validation and null checks.

Usage:
    from pipelines.utils.cdc_validators import CDCValidator
    
    validator = CDCValidator()
    is_valid, errors = validator.validate_event(event)
"""

import re
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional


class CDCValidator:
    """Validate CDC events for data quality."""
    
    # Valid event types
    VALID_EVENT_TYPES = ['INSERT', 'UPDATE', 'DELETE']
    
    # Valid object types
    VALID_OBJECT_TYPES = ['Account', 'Contact', 'Opportunity', 'Case']
    
    # Salesforce ID pattern (18 chars alphanumeric)
    SALESFORCE_ID_PATTERN = re.compile(r'^[a-zA-Z0-9]{15,18}$')
    
    # Required fields by object type
    REQUIRED_FIELDS = {
        'Account': ['id', 'name', 'created_date', 'last_modified_date'],
        'Contact': ['id', 'first_name', 'last_name', 'created_date', 'last_modified_date'],
        'Opportunity': ['id', 'name', 'stage_name', 'created_date', 'last_modified_date'],
        'Case': ['id', 'subject', 'status', 'created_date', 'last_modified_date']
    }
    
    # Expected field types by object type
    FIELD_TYPES = {
        'Account': {
            'id': str,
            'name': str,
            'type': (str, type(None)),
            'industry': (str, type(None)),
            'annual_revenue': (int, float, type(None)),
            'phone': (str, type(None)),
            'website': (str, type(None)),
            'created_date': str,
            'last_modified_date': str,
        },
        'Contact': {
            'id': str,
            'account_id': (str, type(None)),
            'first_name': str,
            'last_name': str,
            'email': (str, type(None)),
            'phone': (str, type(None)),
            'title': (str, type(None)),
            'created_date': str,
            'last_modified_date': str,
        },
        'Opportunity': {
            'id': str,
            'account_id': (str, type(None)),
            'name': str,
            'stage_name': str,
            'amount': (int, float, type(None)),
            'probability': (int, float, type(None)),
            'close_date': (str, type(None)),
            'is_won': (bool, type(None)),
            'is_closed': (bool, type(None)),
            'created_date': str,
            'last_modified_date': str,
        },
        'Case': {
            'id': str,
            'account_id': (str, type(None)),
            'contact_id': (str, type(None)),
            'subject': str,
            'description': (str, type(None)),
            'status': str,
            'origin': (str, type(None)),
            'priority': (str, type(None)),
            'is_escalated': (bool, type(None)),
            'is_closed': (bool, type(None)),
            'created_date': str,
            'last_modified_date': str,
        }
    }
    
    def __init__(self, alert_threshold: float = 0.05):
        """
        Initialize CDC validator.
        
        Args:
            alert_threshold: Error rate threshold (0-1) for alerting
        """
        self.alert_threshold = alert_threshold
        self.validation_stats = {
            'total': 0,
            'valid': 0,
            'invalid': 0,
            'error_types': {}
        }
    
    def validate_event_type(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate event type field.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        event_type = event.get('event_type')
        
        # Check if event_type exists
        if event_type is None:
            return False, "Missing required field: event_type"
        
        # Check if event_type is valid
        if event_type not in self.VALID_EVENT_TYPES:
            return False, f"Invalid event_type: '{event_type}'. Must be one of {self.VALID_EVENT_TYPES}"
        
        return True, None
    
    def validate_object_type(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate object type field.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        object_type = event.get('object_type')
        
        # Check if object_type exists
        if object_type is None:
            return False, "Missing required field: object_type"
        
        # Check if object_type is valid
        if object_type not in self.VALID_OBJECT_TYPES:
            return False, f"Invalid object_type: '{object_type}'. Must be one of {self.VALID_OBJECT_TYPES}"
        
        return True, None
    
    def validate_record_id_format(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate record ID format.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        record_id = event.get('record_id')
        
        # Check if record_id exists
        if record_id is None:
            return False, "Missing required field: record_id"
        
        # Check if record_id matches Salesforce ID format
        if not self.SALESFORCE_ID_PATTERN.match(str(record_id)):
            return False, f"Invalid record_id format: '{record_id}'. Must be 15-18 alphanumeric characters"
        
        return True, None
    
    def validate_timestamp_format(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate timestamp format.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        event_timestamp = event.get('event_timestamp')
        
        # Check if event_timestamp exists
        if event_timestamp is None:
            return False, "Missing required field: event_timestamp"
        
        # Check if timestamp is valid ISO 8601 format
        try:
            datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return False, f"Invalid event_timestamp format: '{event_timestamp}'. Must be ISO 8601 format"
        
        # Check if timestamp is reasonable (not in future, not too old)
        try:
            ts = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
            now = datetime.now(ts.tzinfo)
            
            # Check if in future
            if ts > now:
                return False, f"event_timestamp is in the future: '{event_timestamp}'"
            
            # Check if too old (more than 1 year)
            age_days = (now - ts).days
            if age_days > 365:
                return False, f"event_timestamp is too old: '{event_timestamp}' ({age_days} days ago)"
        except Exception as e:
            return False, f"Error validating timestamp reasonableness: {str(e)}"
        
        return True, None
    
    def validate_null_required_fields(self, event: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Check for null values in required fields.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Get object type
        object_type = event.get('object_type')
        if not object_type:
            return False, ["Cannot validate required fields without object_type"]
        
        # Get event type
        event_type = event.get('event_type')
        if not event_type:
            return False, ["Cannot validate required fields without event_type"]
        
        # Check 'after' data for INSERT and UPDATE
        if event_type in ['INSERT', 'UPDATE']:
            after_data = event.get('after')
            
            if after_data is None:
                errors.append(f"Missing 'after' data for {event_type} event")
                return False, errors
            
            # Check required fields for this object type
            required_fields = self.REQUIRED_FIELDS.get(object_type, [])
            for field in required_fields:
                if field not in after_data or after_data[field] is None:
                    errors.append(f"Required field '{field}' is null or missing in 'after' data")
        
        # Check 'before' data for UPDATE and DELETE
        if event_type in ['UPDATE', 'DELETE']:
            before_data = event.get('before')
            
            if before_data is None:
                errors.append(f"Missing 'before' data for {event_type} event")
                return False, errors
        
        return len(errors) == 0, errors
    
    def validate_field_types(self, event: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate field data types match expected types.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Get object type
        object_type = event.get('object_type')
        if not object_type or object_type not in self.FIELD_TYPES:
            return True, []  # Skip if unknown object type
        
        expected_types = self.FIELD_TYPES[object_type]
        
        # Check 'after' data if present
        after_data = event.get('after')
        if after_data:
            for field_name, expected_type in expected_types.items():
                if field_name in after_data:
                    value = after_data[field_name]
                    if value is not None and not isinstance(value, expected_type):
                        errors.append(
                            f"Field '{field_name}' has wrong type in 'after': "
                            f"expected {expected_type}, got {type(value).__name__}"
                        )
        
        # Check 'before' data if present
        before_data = event.get('before')
        if before_data:
            for field_name, expected_type in expected_types.items():
                if field_name in before_data:
                    value = before_data[field_name]
                    if value is not None and not isinstance(value, expected_type):
                        errors.append(
                            f"Field '{field_name}' has wrong type in 'before': "
                            f"expected {expected_type}, got {type(value).__name__}"
                        )
        
        return len(errors) == 0, errors
    
    def validate_changed_fields(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate changed_fields for UPDATE events.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        event_type = event.get('event_type')
        
        # Only validate for UPDATE events
        if event_type == 'UPDATE':
            changed_fields = event.get('changed_fields')
            
            if changed_fields is None:
                return False, "Missing 'changed_fields' for UPDATE event"
            
            if not isinstance(changed_fields, list):
                return False, f"'changed_fields' must be a list, got {type(changed_fields).__name__}"
            
            if len(changed_fields) == 0:
                return False, "'changed_fields' cannot be empty for UPDATE event"
        
        return True, None
    
    def validate_foreign_keys(self, event: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate foreign key references.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        object_type = event.get('object_type')
        
        # Define foreign key fields by object type
        foreign_keys = {
            'Contact': ['account_id'],
            'Opportunity': ['account_id'],
            'Case': ['account_id', 'contact_id']
        }
        
        if object_type not in foreign_keys:
            return True, []  # No foreign keys to validate
        
        # Check 'after' data if present
        after_data = event.get('after')
        if after_data:
            for fk_field in foreign_keys[object_type]:
                fk_value = after_data.get(fk_field)
                if fk_value is not None:
                    # Validate format
                    if not self.SALESFORCE_ID_PATTERN.match(str(fk_value)):
                        errors.append(
                            f"Foreign key '{fk_field}' has invalid format: '{fk_value}'"
                        )
        
        return len(errors) == 0, errors
    
    def validate_event(self, event: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Run all validation rules on a CDC event.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.validation_stats['total'] += 1
        errors = []
        
        # Run all validations
        validations = [
            ('event_type', self.validate_event_type),
            ('object_type', self.validate_object_type),
            ('record_id', self.validate_record_id_format),
            ('timestamp', self.validate_timestamp_format),
            ('changed_fields', self.validate_changed_fields),
        ]
        
        for error_type, validation_func in validations:
            is_valid, error = validation_func(event)
            if not is_valid:
                if isinstance(error, list):
                    errors.extend(error)
                else:
                    errors.append(error)
                self.validation_stats['error_types'][error_type] = \
                    self.validation_stats['error_types'].get(error_type, 0) + 1
        
        # Run multi-error validations
        is_valid, null_errors = self.validate_null_required_fields(event)
        if not is_valid:
            errors.extend(null_errors)
            self.validation_stats['error_types']['null_fields'] = \
                self.validation_stats['error_types'].get('null_fields', 0) + 1
        
        is_valid, type_errors = self.validate_field_types(event)
        if not is_valid:
            errors.extend(type_errors)
            self.validation_stats['error_types']['field_types'] = \
                self.validation_stats['error_types'].get('field_types', 0) + 1
        
        is_valid, fk_errors = self.validate_foreign_keys(event)
        if not is_valid:
            errors.extend(fk_errors)
            self.validation_stats['error_types']['foreign_keys'] = \
                self.validation_stats['error_types'].get('foreign_keys', 0) + 1
        
        # Update statistics
        if len(errors) == 0:
            self.validation_stats['valid'] += 1
        else:
            self.validation_stats['invalid'] += 1
        
        return len(errors) == 0, errors
    
    def get_error_rate(self) -> float:
        """
        Get current error rate.
        
        Returns:
            Error rate (0-1)
        """
        if self.validation_stats['total'] == 0:
            return 0.0
        return self.validation_stats['invalid'] / self.validation_stats['total']
    
    def should_alert(self) -> bool:
        """
        Check if error rate exceeds alert threshold.
        
        Returns:
            True if should alert
        """
        return self.get_error_rate() > self.alert_threshold
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get validation statistics.
        
        Returns:
            Dictionary with validation stats
        """
        error_rate = self.get_error_rate()
        
        return {
            'total_events': self.validation_stats['total'],
            'valid_events': self.validation_stats['valid'],
            'invalid_events': self.validation_stats['invalid'],
            'error_rate': error_rate,
            'exceeds_threshold': error_rate > self.alert_threshold,
            'error_types': self.validation_stats['error_types'].copy()
        }
    
    def reset_statistics(self):
        """Reset validation statistics."""
        self.validation_stats = {
            'total': 0,
            'valid': 0,
            'invalid': 0,
            'error_types': {}
        }
