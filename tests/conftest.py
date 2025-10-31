"""
Pytest configuration and shared fixtures for gcp-data-platform-demo tests.

This module provides common fixtures and utilities for testing across all test modules.
"""

import os
import tempfile
from collections.abc import Generator
from typing import Any
from unittest.mock import Mock

import pytest
from faker import Faker
from google.cloud import bigquery, pubsub_v1, storage


@pytest.fixture(scope="session")
def test_project_id() -> str:
    """Test GCP project ID."""
    return os.getenv("GCP_TEST_PROJECT_ID", "test-project-id")


@pytest.fixture(scope="session")
def test_dataset() -> str:
    """Test BigQuery dataset name."""
    return "test_dataset"


@pytest.fixture(scope="session")
def test_bucket() -> str:
    """Test Cloud Storage bucket name."""
    return "test-bucket"


@pytest.fixture(scope="session")
def test_topic() -> str:
    """Test Pub/Sub topic name."""
    return "test-topic"


@pytest.fixture(scope="session")
def fake() -> Faker:
    """Faker instance for generating test data."""
    return Faker("en_US")


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_bigquery_client(test_project_id: str, test_dataset: str) -> Mock:
    """Mock BigQuery client."""
    client = Mock(spec=bigquery.Client)
    client.project = test_project_id

    # Mock dataset reference
    dataset_ref = Mock()
    dataset_ref.dataset_id = test_dataset
    dataset_ref.project = test_project_id
    client.dataset.return_value = dataset_ref

    # Mock table reference
    table_ref = Mock()
    table_ref.table_id = "test_table"
    table_ref.dataset_id = test_dataset
    table_ref.project = test_project_id
    dataset_ref.table.return_value = table_ref

    # Mock query job
    query_job = Mock()
    query_job.result.return_value = []
    client.query.return_value = query_job

    return client


@pytest.fixture
def mock_pubsub_client(test_project_id: str, test_topic: str) -> Mock:
    """Mock Pub/Sub client."""
    client = Mock(spec=pubsub_v1.PublisherClient)

    # Mock topic path
    topic_path = f"projects/{test_project_id}/topics/{test_topic}"
    client.topic_path.return_value = topic_path

    # Mock subscription path
    subscription_path = f"projects/{test_project_id}/subscriptions/test-subscription"
    client.subscription_path.return_value = subscription_path

    # Mock publish
    client.publish.return_value = Mock(future=Mock(result="message-id"))

    return client


@pytest.fixture
def mock_storage_client(test_project_id: str, test_bucket: str) -> Mock:
    """Mock Cloud Storage client."""
    client = Mock(spec=storage.Client)
    client.project = test_project_id

    # Mock bucket
    bucket = Mock()
    bucket.name = test_bucket
    client.bucket.return_value = bucket

    # Mock blob
    blob = Mock()
    bucket.blob.return_value = blob

    return client


@pytest.fixture
def sample_salesforce_account(fake: Faker) -> dict[str, Any]:
    """Sample Salesforce account record for testing."""
    return {
        "id": "001000000000001AAA",
        "name": fake.company(),
        "type": "Customer",
        "industry": "Technology",
        "annual_revenue": 1000000,
        "phone": fake.phone_number(),
        "website": f"https://{fake.domain_name()}",
        "billing_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "postal_code": fake.zipcode(),
            "country": "United States"
        },
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "postal_code": fake.zipcode(),
            "country": "United States"
        },
        "created_date": "2023-01-15T10:30:00Z",
        "last_modified_date": "2023-12-01T15:45:00Z",
        "system_modstamp": "2023-12-01T15:45:00Z",
        "ingestion_timestamp": "2023-12-10T12:00:00Z",
        "source": "test"
    }


@pytest.fixture
def sample_salesforce_contact(fake: Faker, sample_salesforce_account: dict[str, Any]) -> dict[str, Any]:
    """Sample Salesforce contact record for testing."""
    return {
        "id": "003000000000001AAA",
        "account_id": sample_salesforce_account["id"],
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "title": "Sales Manager",
        "lead_source": "Web",
        "created_date": "2023-02-01T09:15:00Z",
        "last_modified_date": "2023-11-15T14:30:00Z",
        "system_modstamp": "2023-11-15T14:30:00Z",
        "ingestion_timestamp": "2023-12-10T12:00:00Z",
        "source": "test"
    }


@pytest.fixture
def sample_salesforce_opportunity(fake: Faker, sample_salesforce_account: dict[str, Any]) -> dict[str, Any]:
    """Sample Salesforce opportunity record for testing."""
    return {
        "id": "006000000000001AAA",
        "account_id": sample_salesforce_account["id"],
        "name": fake.catch_phrase(),
        "stage_name": "Closed Won",
        "type": "New Business",
        "lead_source": "Partner Referral",
        "amount": 150000,
        "probability": 90,
        "close_date": "2023-11-30",
        "is_won": True,
        "is_closed": True,
        "created_date": "2023-03-01T11:00:00Z",
        "last_modified_date": "2023-11-30T16:00:00Z",
        "system_modstamp": "2023-11-30T16:00:00Z",
        "ingestion_timestamp": "2023-12-10T12:00:00Z",
        "source": "test"
    }


@pytest.fixture
def sample_salesforce_case(fake: Faker, sample_salesforce_account: dict[str, Any], sample_salesforce_contact: dict[str, Any]) -> dict[str, Any]:
    """Sample Salesforce case record for testing."""
    return {
        "id": "500000000000001AAA",
        "account_id": sample_salesforce_account["id"],
        "contact_id": sample_salesforce_contact["id"],
        "subject": fake.sentence(nb_words=6),
        "description": fake.paragraph(nb_sentences=3),
        "status": "Closed",
        "origin": "Email",
        "priority": "Medium",
        "is_escalated": False,
        "is_closed": True,
        "closed_date": "2023-11-20T10:30:00Z",
        "created_date": "2023-10-01T08:00:00Z",
        "last_modified_date": "2023-11-20T10:30:00Z",
        "system_modstamp": "2023-11-20T10:30:00Z",
        "ingestion_timestamp": "2023-12-10T12:00:00Z",
        "source": "test"
    }


@pytest.fixture
def sample_pubsub_message() -> dict[str, Any]:
    """Sample Pub/Sub message for testing."""
    return {
        "data": '{"test": "message"}',
        "attributes": {
            "source": "test",
            "timestamp": "2023-12-10T12:00:00Z"
        },
        "messageId": "test-message-id",
        "publishTime": "2023-12-10T12:00:00Z"
    }


@pytest.fixture
def mock_environment_variables(monkeypatch: pytest.MonkeyPatch, test_project_id: str, test_dataset: str, test_bucket: str) -> None:
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("GCP_PROJECT_ID", test_project_id)
    monkeypatch.setenv("BIGQUERY_DATASET", test_dataset)
    monkeypatch.setenv("GCS_BUCKET", test_bucket)
    monkeypatch.setenv("PUBSUB_TOPIC", "test-topic")


# Test data generation helpers
def generate_test_accounts(count: int = 10, fake: Faker = Faker()) -> list[dict[str, Any]]:
    """Generate test account records."""
    accounts = []
    for i in range(count):
        accounts.append({
            "id": f"00100000000000{i:03d}AAA",
            "name": fake.company(),
            "type": fake.random_element(["Prospect", "Customer", "Partner"]),
            "industry": fake.random_element(["Technology", "Healthcare", "Finance"]),
            "annual_revenue": fake.random_int(min=100000, max=10000000),
            "phone": fake.phone_number(),
            "website": f"https://{fake.domain_name()}",
            "created_date": fake.date_time_this_year().isoformat() + "Z",
            "last_modified_date": fake.date_time_this_month().isoformat() + "Z",
            "system_modstamp": fake.date_time_this_month().isoformat() + "Z",
            "ingestion_timestamp": fake.date_time_this_day().isoformat() + "Z",
            "source": "test"
        })
    return accounts


def generate_test_contacts(count: int = 20, account_ids: list[str] | None = None, fake: Faker = Faker()) -> list[dict[str, Any]]:
    """Generate test contact records."""
    if account_ids is None:
        account_ids = [f"00100000000000{i:03d}AAA" for i in range(5)]

    contacts = []
    for i in range(count):
        contacts.append({
            "id": f"00300000000000{i:03d}AAA",
            "account_id": fake.random_element(account_ids),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "title": fake.random_element(["CEO", "CTO", "Sales Manager", "Analyst"]),
            "lead_source": fake.random_element(["Web", "Phone", "Partner"]),
            "created_date": fake.date_time_this_year().isoformat() + "Z",
            "last_modified_date": fake.date_time_this_month().isoformat() + "Z",
            "system_modstamp": fake.date_time_this_month().isoformat() + "Z",
            "ingestion_timestamp": fake.date_time_this_day().isoformat() + "Z",
            "source": "test"
        })
    return contacts


# Common assertion helpers
def assert_salesforce_id_format(record_id: str, prefix: str) -> None:
    """Assert that a Salesforce ID has the correct format."""
    assert record_id.startswith(prefix)
    assert len(record_id) == 18
    assert all(c.isalnum() for c in record_id)


def assert_required_fields(record: dict[str, Any], required_fields: list[str]) -> None:
    """Assert that all required fields are present and non-null."""
    for field in required_fields:
        assert field in record, f"Missing required field: {field}"
        assert record[field] is not None, f"Required field is null: {field}"


def assert_timestamp_format(timestamp: str) -> None:
    """Assert that a timestamp is in ISO format."""
    import re
    # ISO 8601 format check with optional microseconds
    iso_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z'
    assert re.match(iso_pattern, timestamp), f"Invalid timestamp format: {timestamp}"


# Pipeline-specific fixtures
@pytest.fixture
def sample_config_dict() -> dict[str, Any]:
    """Sample pipeline configuration dictionary."""
    return {
        'api': {
            'base_url': 'http://localhost:8080/api/v1',
            'timeout_seconds': 30,
            'max_retries': 3,
            'retry_delay_seconds': 5
        },
        'objects': [
            {
                'name': 'Account',
                'endpoint': '/accounts',
                'primary_key': 'id',
                'batch_size': 1000,
                'enabled': True,
                'fields': ['id', 'name', 'type']
            }
        ],
        'storage': {
            'bucket_name': 'test-bucket',
            'raw_data_path': 'salesforce/raw',
            'parquet_compression': 'snappy'
        },
        'bigquery': {
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'raw_table_prefix': 'raw_',
            'write_disposition': 'WRITE_APPEND'
        },
        'data_quality': {
            'enabled': True,
            'validation_rules': {
                'check_null_primary_keys': True,
                'check_duplicate_ids': False,
                'validate_timestamps': True,
                'validate_foreign_keys': False
            }
        }
    }


@pytest.fixture
def sample_config_yaml(temp_dir: str, sample_config_dict: dict[str, Any]) -> str:
    """Create a temporary YAML config file."""
    import yaml
    config_path = f"{temp_dir}/test_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(sample_config_dict, f)
    return config_path


@pytest.fixture
def mock_api_response_accounts() -> dict[str, Any]:
    """Mock API response for accounts endpoint."""
    return {
        'records': [
            {
                'id': '001000000000001AAA',
                'name': 'Acme Corp',
                'type': 'Customer',
                'industry': 'Technology',
                'annual_revenue': 1000000,
                'phone': '555-0100',
                'website': 'https://acme.example.com',
                'created_date': '2023-01-15T10:30:00Z',
                'last_modified_date': '2023-12-01T15:45:00Z',
                'system_modstamp': '2023-12-01T15:45:00Z'
            },
            {
                'id': '001000000000002AAA',
                'name': 'TechStart Inc',
                'type': 'Prospect',
                'industry': 'Software',
                'annual_revenue': 500000,
                'phone': '555-0101',
                'website': 'https://techstart.example.com',
                'created_date': '2023-02-20T09:15:00Z',
                'last_modified_date': '2023-11-15T14:30:00Z',
                'system_modstamp': '2023-11-15T14:30:00Z'
            }
        ],
        'totalSize': 2,
        'done': True
    }


# CDC-specific fixtures
@pytest.fixture
def sample_cdc_insert_event(fake: Faker) -> dict[str, Any]:
    """Sample CDC INSERT event."""
    return {
        'event_id': 'CDC-ABC123XYZ001',
        'event_type': 'INSERT',
        'object_type': 'Account',
        'record_id': '001000000000001AAA',
        'event_timestamp': '2025-10-30T15:30:00Z',
        'changed_fields': [],
        'before': None,
        'after': {
            'id': '001000000000001AAA',
            'name': fake.company(),
            'type': 'Customer',
            'industry': 'Technology',
            'annual_revenue': 1000000,
            'created_date': '2025-10-30T15:30:00Z',
            'last_modified_date': '2025-10-30T15:30:00Z',
            'system_modstamp': '2025-10-30T15:30:00Z'
        },
        'source': 'salesforce_cdc'
    }


@pytest.fixture
def sample_cdc_update_event(fake: Faker) -> dict[str, Any]:
    """Sample CDC UPDATE event."""
    return {
        'event_id': 'CDC-ABC123XYZ002',
        'event_type': 'UPDATE',
        'object_type': 'Account',
        'record_id': '001000000000001AAA',
        'event_timestamp': '2025-10-30T16:00:00Z',
        'changed_fields': ['name', 'annual_revenue'],
        'before': {
            'id': '001000000000001AAA',
            'name': 'Acme Corp',
            'type': 'Customer',
            'industry': 'Technology',
            'annual_revenue': 1000000,
            'created_date': '2025-10-30T15:30:00Z',
            'last_modified_date': '2025-10-30T15:30:00Z',
            'system_modstamp': '2025-10-30T15:30:00Z'
        },
        'after': {
            'id': '001000000000001AAA',
            'name': 'Acme Corporation',
            'type': 'Customer',
            'industry': 'Technology',
            'annual_revenue': 1200000,
            'created_date': '2025-10-30T15:30:00Z',
            'last_modified_date': '2025-10-30T16:00:00Z',
            'system_modstamp': '2025-10-30T16:00:00Z'
        },
        'source': 'salesforce_cdc'
    }


@pytest.fixture
def sample_cdc_delete_event(fake: Faker) -> dict[str, Any]:
    """Sample CDC DELETE event."""
    return {
        'event_id': 'CDC-ABC123XYZ003',
        'event_type': 'DELETE',
        'object_type': 'Account',
        'record_id': '001000000000001AAA',
        'event_timestamp': '2025-10-30T17:00:00Z',
        'changed_fields': [],
        'before': {
            'id': '001000000000001AAA',
            'name': 'Acme Corporation',
            'type': 'Customer',
            'industry': 'Technology',
            'annual_revenue': 1200000,
            'created_date': '2025-10-30T15:30:00Z',
            'last_modified_date': '2025-10-30T16:00:00Z',
            'system_modstamp': '2025-10-30T16:00:00Z'
        },
        'after': None,
        'source': 'salesforce_cdc'
    }


@pytest.fixture
def sample_cdc_events_batch(sample_cdc_insert_event: dict[str, Any], sample_cdc_update_event: dict[str, Any]) -> list[dict[str, Any]]:
    """Sample batch of CDC events."""
    return [sample_cdc_insert_event, sample_cdc_update_event]


@pytest.fixture
def mock_pubsub_publisher() -> Mock:
    """Mock Pub/Sub publisher for CDC events."""
    publisher = Mock()

    # Mock publish method
    future = Mock()
    future.result.return_value = 'message-id-123'
    publisher.publish.return_value = future

    # Mock topic path
    publisher.topic_path.return_value = 'projects/test-project/topics/stream-processing'

    # Mock stop method
    publisher.stop.return_value = None

    return publisher


# Cleanup utilities
@pytest.fixture(autouse=True)
def cleanup_test_resources():
    """Cleanup test resources after each test."""
    yield
    # Add any cleanup logic here if needed
    pass
