"""
Salesforce Object Schemas

This module defines schemas and validation rules for Salesforce objects
used throughout the data platform for consistency.

Usage:
    from src.salesforce.schemas import SalesforceSchemas
    
    schemas = SalesforceSchemas()
    account_schema = schemas.get_account_schema()
    contact_schema = schemas.get_contact_schema()
"""

from typing import Dict, List, Any
from datetime import datetime


class SalesforceSchemas:
    """Salesforce object schemas and validation rules."""
    
    def __init__(self):
        """Initialize schema definitions."""
        # Field mappings for different objects
        self.field_mappings = {
            'account': {
                'id': 'Id',
                'name': 'Name',
                'type': 'Type',
                'industry': 'Industry',
                'annual_revenue': 'AnnualRevenue',
                'phone': 'Phone',
                'website': 'Website',
                'billing_address': 'BillingAddress',
                'shipping_address': 'ShippingAddress',
                'created_date': 'CreatedDate',
                'last_modified_date': 'LastModifiedDate',
                'system_modstamp': 'SystemModstamp'
            },
            'contact': {
                'id': 'Id',
                'account_id': 'AccountId',
                'first_name': 'FirstName',
                'last_name': 'LastName',
                'email': 'Email',
                'phone': 'Phone',
                'title': 'Title',
                'lead_source': 'LeadSource',
                'created_date': 'CreatedDate',
                'last_modified_date': 'LastModifiedDate',
                'system_modstamp': 'SystemModstamp'
            },
            'opportunity': {
                'id': 'Id',
                'account_id': 'AccountId',
                'name': 'Name',
                'stage_name': 'StageName',
                'type': 'Type',
                'lead_source': 'LeadSource',
                'amount': 'Amount',
                'probability': 'Probability',
                'close_date': 'CloseDate',
                'is_won': 'IsWon',
                'is_closed': 'IsClosed',
                'created_date': 'CreatedDate',
                'last_modified_date': 'LastModifiedDate',
                'system_modstamp': 'SystemModstamp'
            },
            'case': {
                'id': 'Id',
                'account_id': 'AccountId',
                'contact_id': 'ContactId',
                'subject': 'Subject',
                'description': 'Description',
                'status': 'Status',
                'origin': 'Origin',
                'priority': 'Priority',
                'is_escalated': 'IsEscalated',
                'is_closed': 'IsClosed',
                'closed_date': 'ClosedDate',
                'created_date': 'CreatedDate',
                'last_modified_date': 'LastModifiedDate',
                'system_modstamp': 'SystemModstamp'
            }
        }
        
        # Validation rules
        self.validation_rules = {
            'account': self._get_account_validation_rules(),
            'contact': self._get_contact_validation_rules(),
            'opportunity': self._get_opportunity_validation_rules(),
            'case': self._get_case_validation_rules()
        }
    
    def get_account_schema(self) -> Dict[str, Any]:
        """Get Salesforce Account object schema."""
        return {
            'object_name': 'Account',
            'fields': self.field_mappings['account'],
            'required_fields': ['id', 'name', 'created_date', 'last_modified_date', 'system_modstamp'],
            'optional_fields': ['type', 'industry', 'annual_revenue', 'phone', 'website', 'billing_address', 'shipping_address'],
            'field_types': {
                'id': 'STRING',
                'name': 'STRING',
                'type': 'STRING',
                'industry': 'STRING',
                'annual_revenue': 'NUMERIC',
                'phone': 'STRING',
                'website': 'STRING',
                'billing_address': 'JSON',
                'shipping_address': 'JSON',
                'created_date': 'TIMESTAMP',
                'last_modified_date': 'TIMESTAMP',
                'system_modstamp': 'TIMESTAMP'
            },
            'validation_rules': self.validation_rules['account']
        }
    
    def get_contact_schema(self) -> Dict[str, Any]:
        """Get Salesforce Contact object schema."""
        return {
            'object_name': 'Contact',
            'fields': self.field_mappings['contact'],
            'required_fields': ['id', 'first_name', 'last_name', 'created_date', 'last_modified_date', 'system_modstamp'],
            'optional_fields': ['account_id', 'email', 'phone', 'title', 'lead_source'],
            'field_types': {
                'id': 'STRING',
                'account_id': 'STRING',
                'first_name': 'STRING',
                'last_name': 'STRING',
                'email': 'STRING',
                'phone': 'STRING',
                'title': 'STRING',
                'lead_source': 'STRING',
                'created_date': 'TIMESTAMP',
                'last_modified_date': 'TIMESTAMP',
                'system_modstamp': 'TIMESTAMP'
            },
            'validation_rules': self.validation_rules['contact']
        }
    
    def get_opportunity_schema(self) -> Dict[str, Any]:
        """Get Salesforce Opportunity object schema."""
        return {
            'object_name': 'Opportunity',
            'fields': self.field_mappings['opportunity'],
            'required_fields': ['id', 'name', 'stage_name', 'created_date', 'last_modified_date', 'system_modstamp'],
            'optional_fields': ['account_id', 'type', 'lead_source', 'amount', 'probability', 'close_date', 'is_won', 'is_closed'],
            'field_types': {
                'id': 'STRING',
                'account_id': 'STRING',
                'name': 'STRING',
                'stage_name': 'STRING',
                'type': 'STRING',
                'lead_source': 'STRING',
                'amount': 'NUMERIC',
                'probability': 'NUMERIC',
                'close_date': 'DATE',
                'is_won': 'BOOLEAN',
                'is_closed': 'BOOLEAN',
                'created_date': 'TIMESTAMP',
                'last_modified_date': 'TIMESTAMP',
                'system_modstamp': 'TIMESTAMP'
            },
            'validation_rules': self.validation_rules['opportunity']
        }
    
    def get_case_schema(self) -> Dict[str, Any]:
        """Get Salesforce Case object schema."""
        return {
            'object_name': 'Case',
            'fields': self.field_mappings['case'],
            'required_fields': ['id', 'subject', 'status', 'created_date', 'last_modified_date', 'system_modstamp'],
            'optional_fields': ['account_id', 'contact_id', 'description', 'origin', 'priority', 'is_escalated', 'is_closed', 'closed_date'],
            'field_types': {
                'id': 'STRING',
                'account_id': 'STRING',
                'contact_id': 'STRING',
                'subject': 'STRING',
                'description': 'STRING',
                'status': 'STRING',
                'origin': 'STRING',
                'priority': 'STRING',
                'is_escalated': 'BOOLEAN',
                'is_closed': 'BOOLEAN',
                'closed_date': 'TIMESTAMP',
                'created_date': 'TIMESTAMP',
                'last_modified_date': 'TIMESTAMP',
                'system_modstamp': 'TIMESTAMP'
            },
            'validation_rules': self.validation_rules['case']
        }
    
    def get_history_schema(self) -> Dict[str, Any]:
        """Get Salesforce History object schema for SCD Type 2."""
        return {
            'object_name': 'Account_History',
            'fields': {
                'id': 'Id',
                'valid_from': 'ValidFrom',
                'valid_to': 'ValidTo',
                'is_current': 'IsCurrent',
                'change_type': 'ChangeType',
                'changed_fields': 'ChangedFields',
                'record_data': 'RecordData',
                'ingestion_timestamp': 'IngestionTimestamp'
            },
            'required_fields': ['id', 'valid_from', 'is_current', 'change_type', 'record_data', 'ingestion_timestamp'],
            'optional_fields': ['valid_to', 'changed_fields'],
            'field_types': {
                'id': 'STRING',
                'valid_from': 'TIMESTAMP',
                'valid_to': 'TIMESTAMP',
                'is_current': 'BOOLEAN',
                'change_type': 'STRING',
                'changed_fields': 'JSON',
                'record_data': 'JSON',
                'ingestion_timestamp': 'TIMESTAMP'
            },
            'validation_rules': self._get_history_validation_rules()
        }
    
    def _get_account_validation_rules(self) -> List[Dict[str, Any]]:
        """Get account validation rules."""
        return [
            {
                'name': 'required_field',
                'description': 'Account name is required',
                'rule': lambda record: bool(record.get('name') and len(record.get('name', '').strip()) > 0)
            },
            {
                'name': 'valid_account_type',
                'description': 'Account type must be valid',
                'rule': lambda record: record.get('type') in ['Prospect', 'Customer', 'Partner', 'Reseller', 'Channel Partner']
            },
            {
                'name': 'valid_revenue',
                'description': 'Annual revenue must be positive',
                'rule': lambda record: record.get('annual_revenue', 0) >= 0
            },
            {
                'name': 'valid_phone_format',
                'description': 'Phone number must be in valid format',
                'rule': lambda record: self._validate_phone(record.get('phone', ''))
            },
            {
                'name': 'valid_website_format',
                'description': 'Website must be valid URL',
                'rule': lambda record: self._validate_website(record.get('website', ''))
            }
        ]
    
    def _get_contact_validation_rules(self) -> List[Dict[str, Any]]:
        """Get contact validation rules."""
        return [
            {
                'name': 'required_email',
                'description': 'Email is required for contacts',
                'rule': lambda record: bool(record.get('email') and '@' in record.get('email', ''))
            },
            {
                'name': 'valid_name_format',
                'description': 'First and last name must be valid',
                'rule': lambda record: bool(
                    record.get('first_name', '').strip() and 
                    record.get('last_name', '').strip()
                )
            },
            {
                'name': 'valid_title',
                'description': 'Job title must be from allowed list',
                'rule': lambda record: record.get('title') in [
                    'CEO', 'CTO', 'CFO', 'VP of Sales', 'Sales Manager',
                    'Sales Director', 'Account Executive', 'Business Analyst',
                    'IT Manager', 'Operations Manager', 'Marketing Director'
                ]
            }
        ]
    
    def _get_opportunity_validation_rules(self) -> List[Dict[str, Any]]:
        """Get opportunity validation rules."""
        return [
            {
                'name': 'required_stage',
                'description': 'Opportunity stage is required',
                'rule': lambda record: bool(record.get('stage_name'))
            },
            {
                'name': 'valid_probability',
                'description': 'Probability must be between 0 and 100',
                'rule': lambda record: 0 <= record.get('probability', 0) <= 100
            },
            {
                'name': 'valid_amount',
                'description': 'Amount must be positive',
                'rule': lambda record: record.get('amount', 0) >= 0
            },
            {
                'name': 'valid_close_date',
                'description': 'Close date must be after created date',
                'rule': lambda record: (
                    record.get('close_date') is None or
                    record.get('created_date') and record.get('close_date') >= record.get('created_date')
                )
            }
        ]
    
    def _get_case_validation_rules(self) -> List[Dict[str, Any]]:
        """Get case validation rules."""
        return [
            {
                'name': 'required_subject',
                'description': 'Case subject is required',
                'rule': lambda record: bool(record.get('subject') and len(record.get('subject', '').strip()) > 0)
            },
            {
                'name': 'valid_status',
                'description': 'Case status must be valid',
                'rule': lambda record: record.get('status') in ['New', 'Working', 'Escalated', 'Closed']
            },
            {
                'name': 'valid_priority',
                'description': 'Case priority must be valid',
                'rule': lambda record: record.get('priority') in ['High', 'Medium', 'Low']
            },
            {
                'name': 'valid_origin',
                'description': 'Case origin must be valid',
                'rule': lambda record: record.get('origin') in ['Web', 'Email', 'Phone', 'Chat', 'Social Media']
            }
        ]
    
    def _get_history_validation_rules(self) -> List[Dict[str, Any]]:
        """Get history validation rules."""
        return [
            {
                'name': 'valid_dates',
                'description': 'Valid from must be before valid to',
                'rule': lambda record: (
                    record.get('valid_to') is None or
                    record.get('valid_from') < record.get('valid_to')
                )
            },
            {
                'name': 'valid_change_type',
                'description': 'Change type must be valid',
                'rule': lambda record: record.get('change_type') in ['INSERT', 'UPDATE', 'DELETE']
            },
            {
                'name': 'current_record_validation',
                'description': 'Only one record can be current',
                'rule': lambda record: record.get('is_current') == (record.get('valid_to') is None)
            }
        ]
    
    def _validate_phone(self, phone: str) -> bool:
        """Validate phone number format."""
        if not phone:
            return True
        # Simple phone validation - can be enhanced
        import re
        phone_pattern = r'^[\+]?[1-9]\d{1,14}$'
        return bool(re.match(phone_pattern, phone))
    
    def _validate_website(self, website: str) -> bool:
        """Validate website URL format."""
        if not website:
            return True
        return website.startswith(('http://', 'https://'))
    
    def validate_record(self, record: Dict[str, Any], object_type: str) -> Dict[str, Any]:
        """Validate a record against its schema."""
        schema = getattr(self, f'get_{object_type}_schema')()
        validation_result = {
            'valid': True,
            'errors': []
        }
        
        # Check required fields
        for field in schema['required_fields']:
            if not record.get(field):
                validation_result['valid'] = False
                validation_result['errors'].append(f"Missing required field: {field}")
        
        # Check field types
        for field, field_value in record.items():
            if field in schema['field_types']:
                expected_type = schema['field_types'][field]
                if not self._validate_field_type(field_value, expected_type):
                    validation_result['valid'] = False
                    validation_result['errors'].append(f"Invalid type for {field}: expected {expected_type}")
        
        # Apply validation rules
        for rule in schema['validation_rules']:
            try:
                if not rule['rule'](record):
                    validation_result['valid'] = False
                    validation_result['errors'].append(f"Validation failed: {rule['description']}")
            except Exception as e:
                validation_result['valid'] = False
                validation_result['errors'].append(f"Validation error: {e}")
        
        return validation_result
    
    def _validate_field_type(self, value: Any, expected_type: str) -> bool:
        """Validate field type."""
        if value is None:
            return True
        
        type_validators = {
            'STRING': lambda v: isinstance(v, str),
            'NUMERIC': lambda v: isinstance(v, (int, float)),
            'BOOLEAN': lambda v: isinstance(v, bool),
            'TIMESTAMP': lambda v: isinstance(v, (datetime, str)),
            'DATE': lambda v: isinstance(v, (datetime, str)),
            'JSON': lambda v: isinstance(v, (dict, str))
        }
        
        validator = type_validators.get(expected_type)
        return validator(value) if validator else False


def main():
    """Test schema validation."""
    schemas = SalesforceSchemas()
    
    # Test with sample data
    sample_account = {
        'id': '001000000000001',
        'name': 'Test Account',
        'type': 'Customer',
        'annual_revenue': 1000000,
        'created_date': datetime.now().isoformat(),
        'last_modified_date': datetime.now().isoformat(),
        'system_modstamp': datetime.now().isoformat()
    }
    
    result = schemas.validate_record(sample_account, 'account')
    print(f"Account validation result: {result}")
    
    # Test invalid record
    invalid_account = {
        'id': '001000000000002',
        'name': '',  # Missing required field
        'created_date': datetime.now().isoformat(),
        'last_modified_date': datetime.now().isoformat(),
        'system_modstamp': datetime.now().isoformat()
    }
    
    result = schemas.validate_record(invalid_account, 'account')
    print(f"Invalid account validation result: {result}")


if __name__ == '__main__':
    main()
