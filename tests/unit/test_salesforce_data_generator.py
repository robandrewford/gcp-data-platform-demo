"""
Tests for Salesforce Data Generator module.

This module tests the synthetic data generation functionality for Salesforce objects.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any

from src.salesforce.data_generator import SalesforceDataGenerator
from tests.conftest import (
    assert_required_fields,
    assert_salesforce_id_format,
    assert_timestamp_format,
)


class TestSalesforceDataGenerator:
    """Test cases for SalesforceDataGenerator class."""

    def test_initialization(self) -> None:
        """Test generator initialization with default locale."""
        generator = SalesforceDataGenerator()
        assert generator.fake is not None
        assert len(generator.account_types) > 0
        assert len(generator.industries) > 0
        assert len(generator.opportunity_stages) > 0

    def test_initialization_with_locale(self) -> None:
        """Test generator initialization with custom locale."""
        generator = SalesforceDataGenerator(locale='en_GB')
        assert generator.fake is not None

    def test_generate_salesforce_id(self) -> None:
        """Test Salesforce ID generation."""
        generator = SalesforceDataGenerator()

        # Test default prefix
        account_id = generator._generate_salesforce_id('001')
        assert_salesforce_id_format(account_id, '001')

        # Test different prefixes
        contact_id = generator._generate_salesforce_id('003')
        assert_salesforce_id_format(contact_id, '003')

        opportunity_id = generator._generate_salesforce_id('006')
        assert_salesforce_id_format(opportunity_id, '006')

    def test_generate_timestamp(self) -> None:
        """Test timestamp generation."""
        generator = SalesforceDataGenerator()

        # Test with default days_back
        timestamp = generator._generate_timestamp()
        assert isinstance(timestamp, datetime)

        # Test with custom days_back
        timestamp = generator._generate_timestamp(days_back=30)
        assert isinstance(timestamp, datetime)

        # Verify it's within the expected range (using timezone-aware datetime)
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        assert timestamp <= now
        assert timestamp >= now - timedelta(days=30)

    def test_generate_address(self) -> None:
        """Test address generation."""
        generator = SalesforceDataGenerator()
        address = generator._generate_address()

        required_fields = ['street', 'city', 'state', 'postal_code', 'country']
        assert_required_fields(address, required_fields)

        assert isinstance(address['street'], str)
        assert isinstance(address['city'], str)
        assert isinstance(address['state'], str)
        assert isinstance(address['postal_code'], str)
        assert isinstance(address['country'], str)

    def test_generate_accounts_default_count(self) -> None:
        """Test account generation with default count."""
        generator = SalesforceDataGenerator()
        accounts = generator.generate_accounts()

        assert len(accounts) == 1000  # default count
        self._validate_accounts(accounts)

    def test_generate_accounts_custom_count(self) -> None:
        """Test account generation with custom count."""
        generator = SalesforceDataGenerator()
        accounts = generator.generate_accounts(count=50)

        assert len(accounts) == 50
        self._validate_accounts(accounts)

    def test_generate_accounts_data_quality(self) -> None:
        """Test data quality of generated accounts."""
        generator = SalesforceDataGenerator()
        accounts = generator.generate_accounts(count=10)

        for account in accounts:
            # Check required fields
            required_fields = [
                'id', 'name', 'type', 'industry', 'annual_revenue',
                'phone', 'website', 'billing_address', 'shipping_address',
                'created_date', 'last_modified_date', 'system_modstamp',
                'ingestion_timestamp', 'source'
            ]
            assert_required_fields(account, required_fields)

            # Check field values
            assert account['type'] in generator.account_types
            assert account['industry'] in generator.industries
            assert isinstance(account['annual_revenue'], int)
            assert account['annual_revenue'] > 0
            assert account['website'].startswith('https://')
            assert account['source'] == 'synthetic_generator'

            # Check timestamp formats
            assert_timestamp_format(account['created_date'])
            assert_timestamp_format(account['last_modified_date'])
            assert_timestamp_format(account['system_modstamp'])
            assert_timestamp_format(account['ingestion_timestamp'])

    def test_generate_contacts_with_account_ids(self) -> None:
        """Test contact generation with provided account IDs."""
        generator = SalesforceDataGenerator()
        account_ids = ['001000000000001AAA', '001000000000002AAA']

        contacts = generator.generate_contacts(count=20, account_ids=account_ids)

        assert len(contacts) == 20
        self._validate_contacts(contacts, account_ids)

    def test_generate_contacts_without_account_ids(self) -> None:
        """Test contact generation without provided account IDs."""
        generator = SalesforceDataGenerator()
        contacts = generator.generate_contacts(count=15)

        assert len(contacts) == 15
        self._validate_contacts(contacts)

    def test_generate_contacts_data_quality(self) -> None:
        """Test data quality of generated contacts."""
        generator = SalesforceDataGenerator()
        account_ids = ['001000000000001AAA']
        contacts = generator.generate_contacts(count=5, account_ids=account_ids)

        for contact in contacts:
            # Check required fields
            required_fields = [
                'id', 'account_id', 'first_name', 'last_name', 'email',
                'phone', 'title', 'lead_source', 'created_date',
                'last_modified_date', 'system_modstamp', 'ingestion_timestamp',
                'source'
            ]
            assert_required_fields(contact, required_fields)

            # Check field values
            assert contact['account_id'] in account_ids
            assert contact['title'] in generator.contact_titles
            assert contact['lead_source'] in generator.lead_sources
            assert '@' in contact['email']
            assert contact['source'] == 'synthetic_generator'

            # Check timestamp formats
            assert_timestamp_format(contact['created_date'])
            assert_timestamp_format(contact['last_modified_date'])
            assert_timestamp_format(contact['system_modstamp'])
            assert_timestamp_format(contact['ingestion_timestamp'])

    def test_generate_opportunities_with_account_ids(self) -> None:
        """Test opportunity generation with provided account IDs."""
        generator = SalesforceDataGenerator()
        account_ids = ['001000000000001AAA', '001000000000002AAA']

        opportunities = generator.generate_opportunities(count=10, account_ids=account_ids)

        assert len(opportunities) == 10
        self._validate_opportunities(opportunities, account_ids)

    def test_generate_opportunities_without_account_ids(self) -> None:
        """Test opportunity generation without provided account IDs."""
        generator = SalesforceDataGenerator()
        opportunities = generator.generate_opportunities(count=8)

        assert len(opportunities) == 8
        self._validate_opportunities(opportunities)

    def test_generate_opportunities_data_quality(self) -> None:
        """Test data quality of generated opportunities."""
        generator = SalesforceDataGenerator()
        account_ids = ['001000000000001AAA']
        opportunities = generator.generate_opportunities(count=5, account_ids=account_ids)

        for opportunity in opportunities:
            # Check required fields (close_date is nullable based on is_closed)
            required_fields = [
                'id', 'account_id', 'name', 'stage_name', 'type',
                'lead_source', 'amount', 'probability',
                'is_won', 'is_closed', 'created_date', 'last_modified_date',
                'system_modstamp', 'ingestion_timestamp', 'source'
            ]
            assert_required_fields(opportunity, required_fields)

            # Check field values
            assert opportunity['account_id'] in account_ids
            assert opportunity['stage_name'] in generator.opportunity_stages
            assert opportunity['type'] in generator.opportunity_types
            assert opportunity['lead_source'] in generator.lead_sources
            assert isinstance(opportunity['amount'], int)
            assert opportunity['amount'] > 0
            assert isinstance(opportunity['probability'], int)
            assert 0 <= opportunity['probability'] <= 100
            assert isinstance(opportunity['is_won'], bool)
            assert isinstance(opportunity['is_closed'], bool)
            assert opportunity['source'] == 'synthetic_generator'

            # Check close_date logic
            if opportunity['is_closed']:
                assert opportunity['close_date'] is not None
            else:
                assert opportunity['close_date'] is None

            # Check timestamp formats
            assert_timestamp_format(opportunity['created_date'])
            assert_timestamp_format(opportunity['last_modified_date'])
            assert_timestamp_format(opportunity['system_modstamp'])
            assert_timestamp_format(opportunity['ingestion_timestamp'])

    def test_generate_cases_with_ids(self) -> None:
        """Test case generation with provided account and contact IDs."""
        generator = SalesforceDataGenerator()
        account_ids = ['001000000000001AAA']
        contact_ids = ['003000000000001AAA']

        cases = generator.generate_cases(
            count=10,
            account_ids=account_ids,
            contact_ids=contact_ids
        )

        assert len(cases) == 10
        self._validate_cases(cases, account_ids, contact_ids)

    def test_generate_cases_without_ids(self) -> None:
        """Test case generation without provided IDs."""
        generator = SalesforceDataGenerator()
        cases = generator.generate_cases(count=8)

        assert len(cases) == 8
        self._validate_cases(cases)

    def test_generate_cases_data_quality(self) -> None:
        """Test data quality of generated cases."""
        generator = SalesforceDataGenerator()
        account_ids = ['001000000000001AAA']
        contact_ids = ['003000000000001AAA']
        cases = generator.generate_cases(
            count=5,
            account_ids=account_ids,
            contact_ids=contact_ids
        )

        for case in cases:
            # Check required fields (closed_date is nullable based on is_closed)
            required_fields = [
                'id', 'account_id', 'contact_id', 'subject', 'description',
                'status', 'origin', 'priority', 'is_escalated', 'is_closed',
                'created_date', 'last_modified_date',
                'system_modstamp', 'ingestion_timestamp', 'source'
            ]
            assert_required_fields(case, required_fields)

            # Check field values
            assert case['account_id'] in account_ids
            assert case['contact_id'] in contact_ids
            assert case['status'] in generator.case_statuses
            assert case['origin'] in generator.case_origins
            assert case['priority'] in generator.case_priorities
            assert isinstance(case['is_escalated'], bool)
            assert isinstance(case['is_closed'], bool)
            assert case['source'] == 'synthetic_generator'

            # Check closed_date logic
            if case['is_closed']:
                assert case['closed_date'] is not None
            else:
                assert case['closed_date'] is None

            # Check timestamp formats
            assert_timestamp_format(case['created_date'])
            assert_timestamp_format(case['last_modified_date'])
            assert_timestamp_format(case['system_modstamp'])
            assert_timestamp_format(case['ingestion_timestamp'])

    def test_generate_historical_snapshots(self) -> None:
        """Test historical snapshot generation."""
        generator = SalesforceDataGenerator()
        accounts = generator.generate_accounts(count=5)

        snapshots = generator.generate_historical_snapshots(accounts, snapshot_count=3)

        # Should have 3 snapshots per account
        assert len(snapshots) == 15

        self._validate_historical_snapshots(snapshots, accounts)

    def test_generate_historical_snapshots_data_quality(self) -> None:
        """Test data quality of historical snapshots."""
        generator = SalesforceDataGenerator()
        accounts = generator.generate_accounts(count=2)

        snapshots = generator.generate_historical_snapshots(accounts, snapshot_count=2)

        for snapshot in snapshots:
            # Check required fields (valid_to is nullable for current snapshots)
            required_fields = [
                'id', 'valid_from', 'is_current', 'change_type',
                'changed_fields', 'record_data', 'ingestion_timestamp'
            ]
            assert_required_fields(snapshot, required_fields)

            # Check field values
            assert snapshot['change_type'] in ['INSERT', 'UPDATE']
            assert isinstance(snapshot['is_current'], bool)
            assert isinstance(snapshot['changed_fields'], list)

            # Check timestamp formats
            assert_timestamp_format(snapshot['valid_from'])
            if snapshot['valid_to']:
                assert_timestamp_format(snapshot['valid_to'])
            assert_timestamp_format(snapshot['ingestion_timestamp'])

            # Check record_data is valid JSON
            record_data = json.loads(snapshot['record_data'])
            assert isinstance(record_data, dict)

    def test_save_to_json(self, temp_dir: str) -> None:
        """Test saving generated data to JSON file."""
        generator = SalesforceDataGenerator()
        accounts = generator.generate_accounts(count=5)

        # Save to temporary directory
        filepath = generator.save_to_json(accounts, 'test_accounts', temp_dir)

        # Verify file exists
        assert os.path.exists(filepath)

        # Verify file content
        with open(filepath) as f:
            loaded_data = json.load(f)

        assert len(loaded_data) == 5

    # Helper methods for validation
    def _validate_accounts(self, accounts: list[dict[str, Any]], account_ids: list[str] | None = None) -> None:
        """Validate account records."""
        for account in accounts:
            assert_salesforce_id_format(account['id'], '001')
            if account_ids:
                assert account['id'] in account_ids
            assert isinstance(account['name'], str)
            assert len(account['name']) > 0

    def _validate_contacts(self, contacts: list[dict[str, Any]], account_ids: list[str] | None = None) -> None:
        """Validate contact records."""
        for contact in contacts:
            assert_salesforce_id_format(contact['id'], '003')
            if account_ids:
                assert contact['account_id'] in account_ids
            assert isinstance(contact['first_name'], str)
            assert isinstance(contact['last_name'], str)

    def _validate_opportunities(self, opportunities: list[dict[str, Any]], account_ids: list[str] | None = None) -> None:
        """Validate opportunity records."""
        for opportunity in opportunities:
            assert_salesforce_id_format(opportunity['id'], '006')
            if account_ids:
                assert opportunity['account_id'] in account_ids
            assert isinstance(opportunity['name'], str)
            assert len(opportunity['name']) > 0

    def _validate_cases(self, cases: list[dict[str, Any]], account_ids: list[str] | None = None, contact_ids: list[str] | None = None) -> None:
        """Validate case records."""
        for case in cases:
            assert_salesforce_id_format(case['id'], '500')
            if account_ids:
                assert case['account_id'] in account_ids
            if contact_ids:
                assert case['contact_id'] in contact_ids
            assert isinstance(case['subject'], str)
            assert len(case['subject']) > 0

    def _validate_historical_snapshots(self, snapshots: list[dict[str, Any]], original_records: list[dict[str, Any]]) -> None:
        """Validate historical snapshot records."""
        original_ids = {record['id'] for record in original_records}
        snapshot_ids = {snapshot['id'] for snapshot in snapshots}

        # All snapshots should correspond to original records
        assert snapshot_ids == original_ids

        # Check that we have the right number of snapshots per record
        snapshots_per_id = {}
        for snapshot in snapshots:
            record_id = snapshot['id']
            snapshots_per_id[record_id] = snapshots_per_id.get(record_id, 0) + 1

        # Should have consistent number of snapshots per record
        expected_snapshots_per_record = len(snapshots) // len(original_records)
        for count in snapshots_per_id.values():
            assert count == expected_snapshots_per_record
