"""
Unit tests for Salesforce Batch Extraction Pipeline.

This module tests individual components of the batch pipeline including
configuration loading, data extraction, validation, and formatting.
"""

import json
from datetime import datetime
from typing import Any

import pytest
import requests
import requests_mock

from pipelines.salesforce_batch_extract import (
    ExtractSalesforceObject,
    FormatForBigQuery,
    SalesforceAPIConfig,
    ValidateRecord,
    get_bigquery_schema,
)


class TestSalesforceAPIConfig:
    """Test cases for SalesforceAPIConfig class."""

    def test_load_valid_configuration(self, sample_config_yaml: str):
        """Test loading valid configuration from YAML file."""
        config = SalesforceAPIConfig(sample_config_yaml)

        assert config.api_config is not None
        assert config.api_config['base_url'] == 'http://localhost:8080/api/v1'
        assert config.api_config['timeout_seconds'] == 30

        assert len(config.objects) == 1
        assert config.objects[0]['name'] == 'Account'

        assert config.storage_config is not None
        assert config.bigquery_config is not None
        assert config.data_quality_config is not None

    def test_load_missing_file(self):
        """Test handling of missing configuration file."""
        with pytest.raises(FileNotFoundError):
            SalesforceAPIConfig('/nonexistent/config.yaml')

    def test_load_malformed_yaml(self, temp_dir: str):
        """Test handling of malformed YAML."""
        bad_config_path = f"{temp_dir}/bad_config.yaml"
        with open(bad_config_path, 'w') as f:
            f.write("invalid: yaml: content: [[[")

        with pytest.raises(Exception):  # YAML parsing error
            SalesforceAPIConfig(bad_config_path)

    def test_config_attributes(self, sample_config_yaml: str):
        """Test all configuration attributes are accessible."""
        config = SalesforceAPIConfig(sample_config_yaml)

        # API config
        assert 'base_url' in config.api_config
        assert 'timeout_seconds' in config.api_config

        # Storage config
        assert 'bucket_name' in config.storage_config
        assert 'raw_data_path' in config.storage_config

        # BigQuery config
        assert 'project_id' in config.bigquery_config
        assert 'dataset_id' in config.bigquery_config

        # Data quality config
        assert 'validation_rules' in config.data_quality_config


class TestExtractSalesforceObject:
    """Test cases for ExtractSalesforceObject DoFn."""

    def test_successful_extraction(
        self,
        sample_config_dict: dict[str, Any],
        mock_api_response_accounts: dict[str, Any]
    ):
        """Test successful API extraction with mock HTTP response."""
        with requests_mock.Mocker() as m:
            # Mock the API endpoint
            m.get(
                'http://localhost:8080/api/v1/accounts',
                json=mock_api_response_accounts
            )

            # Create DoFn instance
            dofn = ExtractSalesforceObject(
                sample_config_dict,
                sample_config_dict['objects'][0]
            )

            # Setup session
            dofn.setup()

            # Process trigger element
            results = list(dofn.process('trigger'))

            # Cleanup
            dofn.teardown()

            # Assertions
            assert len(results) == 2
            assert results[0]['id'] == '001000000000001AAA'
            assert results[0]['name'] == 'Acme Corp'
            assert results[0]['source'] == 'salesforce_api'
            assert results[0]['object_type'] == 'Account'
            assert 'ingestion_timestamp' in results[0]

    def test_api_connection_error(
        self,
        sample_config_dict: dict[str, Any]
    ):
        """Test handling of API connection errors."""
        with requests_mock.Mocker() as m:
            # Mock connection error
            m.get(
                'http://localhost:8080/api/v1/accounts',
                exc=requests.exceptions.ConnectionError
            )

            dofn = ExtractSalesforceObject(
                sample_config_dict,
                sample_config_dict['objects'][0]
            )

            dofn.setup()

            with pytest.raises(requests.exceptions.ConnectionError):
                list(dofn.process('trigger'))

            dofn.teardown()

    def test_api_timeout(
        self,
        sample_config_dict: dict[str, Any]
    ):
        """Test handling of API timeout errors."""
        with requests_mock.Mocker() as m:
            # Mock timeout
            m.get(
                'http://localhost:8080/api/v1/accounts',
                exc=requests.exceptions.Timeout
            )

            dofn = ExtractSalesforceObject(
                sample_config_dict,
                sample_config_dict['objects'][0]
            )

            dofn.setup()

            with pytest.raises(requests.exceptions.Timeout):
                list(dofn.process('trigger'))

            dofn.teardown()

    def test_api_http_error(
        self,
        sample_config_dict: dict[str, Any]
    ):
        """Test handling of HTTP errors (4xx, 5xx)."""
        with requests_mock.Mocker() as m:
            # Mock 404 error
            m.get(
                'http://localhost:8080/api/v1/accounts',
                status_code=404,
                json={'error': 'Not found'}
            )

            dofn = ExtractSalesforceObject(
                sample_config_dict,
                sample_config_dict['objects'][0]
            )

            dofn.setup()

            with pytest.raises(requests.exceptions.HTTPError):
                list(dofn.process('trigger'))

            dofn.teardown()

    def test_malformed_json_response(
        self,
        sample_config_dict: dict[str, Any]
    ):
        """Test handling of malformed JSON responses."""
        with requests_mock.Mocker() as m:
            # Mock invalid JSON
            m.get(
                'http://localhost:8080/api/v1/accounts',
                text='invalid json content'
            )

            dofn = ExtractSalesforceObject(
                sample_config_dict,
                sample_config_dict['objects'][0]
            )

            dofn.setup()

            with pytest.raises(Exception):  # JSON decode error
                list(dofn.process('trigger'))

            dofn.teardown()

    def test_metadata_addition(
        self,
        sample_config_dict: dict[str, Any],
        mock_api_response_accounts: dict[str, Any]
    ):
        """Test that metadata is correctly added to records."""
        with requests_mock.Mocker() as m:
            m.get(
                'http://localhost:8080/api/v1/accounts',
                json=mock_api_response_accounts
            )

            dofn = ExtractSalesforceObject(
                sample_config_dict,
                sample_config_dict['objects'][0]
            )

            dofn.setup()
            results = list(dofn.process('trigger'))
            dofn.teardown()

            for record in results:
                assert 'ingestion_timestamp' in record
                assert 'source' in record
                assert 'object_type' in record
                assert record['source'] == 'salesforce_api'
                assert record['object_type'] == 'Account'

                # Validate timestamp format
                datetime.fromisoformat(record['ingestion_timestamp'])


class TestValidateRecord:
    """Test cases for ValidateRecord DoFn."""

    def test_validate_valid_record(self, sample_salesforce_account: dict[str, Any]):
        """Test validation of a valid record."""
        object_config = {
            'name': 'Account',
            'primary_key': 'id'
        }

        validation_rules = {
            'check_null_primary_keys': True,
            'validate_timestamps': True
        }

        dofn = ValidateRecord(object_config, validation_rules)
        results = list(dofn.process(sample_salesforce_account))

        assert len(results) == 1
        record = results[0]

        assert record['_is_valid'] is True
        assert record['_validation_errors'] == []
        assert '_validation_timestamp' in record

    def test_detect_null_primary_key(self):
        """Test detection of null primary keys."""
        record = {
            'id': None,
            'name': 'Test Account'
        }

        object_config = {
            'name': 'Account',
            'primary_key': 'id'
        }

        validation_rules = {
            'check_null_primary_keys': True,
            'validate_timestamps': False
        }

        dofn = ValidateRecord(object_config, validation_rules)
        results = list(dofn.process(record))

        assert len(results) == 1
        record = results[0]

        assert record['_is_valid'] is False
        assert len(record['_validation_errors']) > 0
        assert 'Null primary key' in record['_validation_errors'][0]

    def test_detect_missing_primary_key(self):
        """Test detection of missing primary key field."""
        record = {
            'name': 'Test Account'
        }

        object_config = {
            'name': 'Account',
            'primary_key': 'id'
        }

        validation_rules = {
            'check_null_primary_keys': True,
            'validate_timestamps': False
        }

        dofn = ValidateRecord(object_config, validation_rules)
        results = list(dofn.process(record))

        assert len(results) == 1
        assert results[0]['_is_valid'] is False

    def test_validate_invalid_timestamp_format(self):
        """Test detection of invalid timestamp formats."""
        record = {
            'id': '001000000000001AAA',
            'created_date': 'invalid-timestamp',
            'last_modified_date': '2023-01-15T10:30:00Z',
            'system_modstamp': '2023-01-15T10:30:00Z'
        }

        object_config = {
            'name': 'Account',
            'primary_key': 'id'
        }

        validation_rules = {
            'check_null_primary_keys': False,
            'validate_timestamps': True
        }

        dofn = ValidateRecord(object_config, validation_rules)
        results = list(dofn.process(record))

        assert len(results) == 1
        record = results[0]

        assert record['_is_valid'] is False
        assert len(record['_validation_errors']) > 0
        assert any('timestamp' in err.lower() for err in record['_validation_errors'])

    def test_validate_with_optional_fields(self):
        """Test validation handles records with missing optional fields."""
        record = {
            'id': '001000000000001AAA',
            'name': 'Test Account',
            # Missing optional timestamp fields
        }

        object_config = {
            'name': 'Account',
            'primary_key': 'id'
        }

        validation_rules = {
            'check_null_primary_keys': True,
            'validate_timestamps': True
        }

        dofn = ValidateRecord(object_config, validation_rules)
        results = list(dofn.process(record))

        assert len(results) == 1
        assert results[0]['_is_valid'] is True

    def test_validation_metadata_added(self, sample_salesforce_account: dict[str, Any]):
        """Test that validation metadata is correctly added."""
        object_config = {
            'name': 'Account',
            'primary_key': 'id'
        }

        validation_rules = {
            'check_null_primary_keys': True,
            'validate_timestamps': True
        }

        dofn = ValidateRecord(object_config, validation_rules)
        results = list(dofn.process(sample_salesforce_account))

        record = results[0]
        assert '_is_valid' in record
        assert '_validation_errors' in record
        assert '_validation_timestamp' in record
        assert isinstance(record['_is_valid'], bool)
        assert isinstance(record['_validation_errors'], list)


class TestFormatForBigQuery:
    """Test cases for FormatForBigQuery DoFn."""

    def test_convert_nested_dict_to_json(self):
        """Test conversion of nested dictionaries to JSON strings."""
        record = {
            'id': '001000000000001AAA',
            'name': 'Test Account',
            'address': {
                'street': '123 Main St',
                'city': 'San Francisco',
                'state': 'CA'
            }
        }

        dofn = FormatForBigQuery()
        results = list(dofn.process(record))

        assert len(results) == 1
        formatted = results[0]

        assert formatted['id'] == '001000000000001AAA'
        assert formatted['name'] == 'Test Account'
        assert isinstance(formatted['address'], str)

        # Verify JSON is valid
        address = json.loads(formatted['address'])
        assert address['street'] == '123 Main St'

    def test_convert_list_to_json(self):
        """Test conversion of lists to JSON strings."""
        record = {
            'id': '001000000000001AAA',
            'tags': ['customer', 'premium', 'active'],
            'validation_errors': []
        }

        dofn = FormatForBigQuery()
        results = list(dofn.process(record))

        formatted = results[0]
        assert isinstance(formatted['tags'], str)
        assert isinstance(formatted['validation_errors'], str)

        tags = json.loads(formatted['tags'])
        assert tags == ['customer', 'premium', 'active']

    def test_preserve_simple_types(self):
        """Test that simple types are preserved."""
        record = {
            'id': '001000000000001AAA',
            'name': 'Test Account',
            'revenue': 1000000,
            'is_active': True,
            'score': 95.5
        }

        dofn = FormatForBigQuery()
        results = list(dofn.process(record))

        formatted = results[0]
        assert formatted['id'] == '001000000000001AAA'
        assert formatted['name'] == 'Test Account'
        assert formatted['revenue'] == 1000000
        assert formatted['is_active'] is True
        assert formatted['score'] == 95.5

    def test_handle_null_values(self):
        """Test handling of null values."""
        record = {
            'id': '001000000000001AAA',
            'name': 'Test Account',
            'optional_field': None
        }

        dofn = FormatForBigQuery()
        results = list(dofn.process(record))

        formatted = results[0]
        assert formatted['optional_field'] is None


class TestGetBigQuerySchema:
    """Test cases for get_bigquery_schema function."""

    def test_account_schema(self):
        """Test BigQuery schema generation for Account."""
        schema = get_bigquery_schema('Account')

        assert 'fields' in schema
        fields = schema['fields']

        # Check common fields
        field_names = [f['name'] for f in fields]
        assert 'id' in field_names
        assert 'ingestion_timestamp' in field_names
        assert 'source' in field_names
        assert '_is_valid' in field_names

        # Check Account-specific fields
        assert 'name' in field_names
        assert 'type' in field_names
        assert 'industry' in field_names
        assert 'annual_revenue' in field_names

    def test_contact_schema(self):
        """Test BigQuery schema generation for Contact."""
        schema = get_bigquery_schema('Contact')

        fields = schema['fields']
        field_names = [f['name'] for f in fields]

        # Check Contact-specific fields
        assert 'account_id' in field_names
        assert 'first_name' in field_names
        assert 'last_name' in field_names
        assert 'email' in field_names

    def test_opportunity_schema(self):
        """Test BigQuery schema generation for Opportunity."""
        schema = get_bigquery_schema('Opportunity')

        fields = schema['fields']
        field_names = [f['name'] for f in fields]

        # Check Opportunity-specific fields
        assert 'account_id' in field_names
        assert 'stage_name' in field_names
        assert 'amount' in field_names
        assert 'probability' in field_names
        assert 'close_date' in field_names

    def test_case_schema(self):
        """Test BigQuery schema generation for Case."""
        schema = get_bigquery_schema('Case')

        fields = schema['fields']
        field_names = [f['name'] for f in fields]

        # Check Case-specific fields
        assert 'account_id' in field_names
        assert 'contact_id' in field_names
        assert 'subject' in field_names
        assert 'status' in field_names
        assert 'priority' in field_names

    def test_unknown_object_schema(self):
        """Test schema generation for unknown object types."""
        schema = get_bigquery_schema('UnknownObject')

        # Should still have common fields
        fields = schema['fields']
        field_names = [f['name'] for f in fields]
        assert 'id' in field_names
        assert 'ingestion_timestamp' in field_names

    def test_schema_field_types(self):
        """Test that schema fields have correct types."""
        schema = get_bigquery_schema('Account')

        fields = {f['name']: f for f in schema['fields']}

        # Check field types
        assert fields['id']['type'] == 'STRING'
        assert fields['id']['mode'] == 'REQUIRED'
        assert fields['created_date']['type'] == 'TIMESTAMP'
        assert fields['_is_valid']['type'] == 'BOOLEAN'
        assert fields['_validation_errors']['mode'] == 'REPEATED'
