"""
Salesforce Synthetic Data Generator

This module generates realistic synthetic Salesforce data for testing
data pipelines and demonstrating data platform capabilities.

Usage:
    from src.salesforce.data_generator import SalesforceDataGenerator

    generator = SalesforceDataGenerator()
    accounts = generator.generate_accounts(count=1000)
    contacts = generator.generate_contacts(count=5000)
    opportunities = generator.generate_opportunities(count=2000)
    cases = generator.generate_cases(count=1000)
"""

import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from faker import Faker


class SalesforceDataGenerator:
    """Generate synthetic Salesforce data for testing and demonstration."""

    def __init__(self, locale: str = 'en_US'):
        """Initialize with Faker for realistic data generation."""
        self.fake = Faker(locale)
        self.account_types = ['Prospect', 'Customer', 'Partner', 'Reseller', 'Channel Partner']
        self.industries = [
            'Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Retail',
            'Education', 'Government', 'Non-Profit', 'Consulting', 'Real Estate'
        ]
        self.opportunity_stages = [
            'Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition',
            'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost'
        ]
        self.opportunity_types = ['New Business', 'Existing Business', 'New Customer', 'Community']
        self.lead_sources = [
            'Web', 'Phone Inquiry', 'Partner Referral', 'Purchased List',
            'Other', 'Trade Show', 'Website', 'Word of Mouth', 'Employee Referral'
        ]
        self.case_origins = ['Web', 'Email', 'Phone', 'Chat', 'Social Media']
        self.case_statuses = ['New', 'Working', 'Escalated', 'Closed']
        self.case_priorities = ['High', 'Medium', 'Low']
        self.contact_titles = [
            'CEO', 'CTO', 'CFO', 'VP of Sales', 'Sales Manager',
            'Sales Director', 'Account Executive', 'Business Analyst',
            'IT Manager', 'Operations Manager', 'Marketing Director'
        ]

    def _generate_salesforce_id(self, prefix: str = '001') -> str:
        """Generate realistic Salesforce ID."""
        # Salesforce IDs are 18-character case-sensitive alphanumeric strings
        chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        return prefix + ''.join(random.choices(chars, k=15))

    def _generate_timestamp(self, days_back: int = 365) -> datetime:
        """Generate random timestamp within specified days."""
        now = datetime.now(timezone.utc)
        random_days = random.randint(0, days_back)
        random_hours = random.randint(0, 23)
        random_minutes = random.randint(0, 59)
        return now - timedelta(days=random_days, hours=random_hours, minutes=random_minutes)

    def _generate_address(self) -> dict[str, str]:
        """Generate address as JSON object."""
        return {
            'street': self.fake.street_address(),
            'city': self.fake.city(),
            'state': self.fake.state_abbr(),
            'postal_code': self.fake.zipcode(),
            'country': self.fake.country()
        }

    def generate_accounts(self, count: int = 1000) -> list[dict[str, Any]]:
        """Generate synthetic Salesforce accounts."""
        accounts = []

        for _ in range(count):
            created_date = self._generate_timestamp(days_back=730)
            last_modified = self._generate_timestamp(days_back=30)

            account = {
                'id': self._generate_salesforce_id('001'),
                'name': self.fake.company(),
                'type': random.choice(self.account_types),
                'industry': random.choice(self.industries),
                'annual_revenue': random.randint(100000, 10000000),
                'phone': self.fake.phone_number(),
                'website': f"https://{self.fake.domain_name()}",
                'billing_address': self._generate_address(),
                'shipping_address': self._generate_address(),
                'created_date': created_date.isoformat().replace('+00:00', 'Z'),
                'last_modified_date': last_modified.isoformat().replace('+00:00', 'Z'),
                'system_modstamp': last_modified.isoformat().replace('+00:00', 'Z'),
                'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'source': 'synthetic_generator'
            }
            accounts.append(account)

        return accounts

    def generate_contacts(self, count: int = 5000, account_ids: Optional[list[str]] = None) -> list[dict[str, Any]]:
        """Generate synthetic Salesforce contacts."""
        if account_ids is None:
            account_ids = [self._generate_salesforce_id('001') for _ in range(1000)]

        contacts = []

        for _ in range(count):
            created_date = self._generate_timestamp(days_back=365)
            last_modified = self._generate_timestamp(days_back=30)

            contact = {
                'id': self._generate_salesforce_id('003'),
                'account_id': random.choice(account_ids) if account_ids else None,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'title': random.choice(self.contact_titles),
                'lead_source': random.choice(self.lead_sources),
                'created_date': created_date.isoformat().replace('+00:00', 'Z'),
                'last_modified_date': last_modified.isoformat().replace('+00:00', 'Z'),
                'system_modstamp': last_modified.isoformat().replace('+00:00', 'Z'),
                'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'source': 'synthetic_generator'
            }
            contacts.append(contact)

        return contacts

    def generate_opportunities(self, count: int = 2000, account_ids: Optional[list[str]] = None) -> list[dict[str, Any]]:
        """Generate synthetic Salesforce opportunities."""
        if account_ids is None:
            account_ids = [self._generate_salesforce_id('001') for _ in range(1000)]

        opportunities = []

        for _ in range(count):
            created_date = self._generate_timestamp(days_back=365)
            last_modified = self._generate_timestamp(days_back=30)
            close_date = self._generate_timestamp(days_back=60)
            is_won = random.choice([True, False])
            is_closed = random.choice([True, False])

            opportunity = {
                'id': self._generate_salesforce_id('006'),
                'account_id': random.choice(account_ids) if account_ids else None,
                'name': self.fake.catch_phrase(),
                'stage_name': random.choice(self.opportunity_stages),
                'type': random.choice(self.opportunity_types),
                'lead_source': random.choice(self.lead_sources),
                'amount': random.randint(10000, 500000),
                'probability': random.randint(1, 100),
                'close_date': close_date.date().isoformat() if is_closed else None,
                'is_won': is_won,
                'is_closed': is_closed,
                'created_date': created_date.isoformat().replace('+00:00', 'Z'),
                'last_modified_date': last_modified.isoformat().replace('+00:00', 'Z'),
                'system_modstamp': last_modified.isoformat().replace('+00:00', 'Z'),
                'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'source': 'synthetic_generator'
            }
            opportunities.append(opportunity)

        return opportunities

    def generate_cases(self, count: int = 1000, account_ids: Optional[list[str]] = None, contact_ids: Optional[list[str]] = None) -> list[dict[str, Any]]:
        """Generate synthetic Salesforce cases."""
        if account_ids is None:
            account_ids = [self._generate_salesforce_id('001') for _ in range(500)]
        if contact_ids is None:
            contact_ids = [self._generate_salesforce_id('003') for _ in range(2000)]

        cases = []

        for _ in range(count):
            created_date = self._generate_timestamp(days_back=180)
            last_modified = self._generate_timestamp(days_back=30)
            is_closed = random.choice([True, False])
            is_escalated = random.choice([True, False])

            case_data = {
                'id': self._generate_salesforce_id('500'),
                'account_id': random.choice(account_ids) if account_ids else None,
                'contact_id': random.choice(contact_ids) if contact_ids else None,
                'subject': self.fake.sentence(nb_words=6),
                'description': self.fake.paragraph(nb_sentences=3),
                'status': random.choice(self.case_statuses),
                'origin': random.choice(self.case_origins),
                'priority': random.choice(self.case_priorities),
                'is_escalated': is_escalated,
                'is_closed': is_closed,
                'closed_date': self._generate_timestamp(days_back=15).isoformat().replace('+00:00', 'Z') if is_closed else None,
                'created_date': created_date.isoformat().replace('+00:00', 'Z'),
                'last_modified_date': last_modified.isoformat().replace('+00:00', 'Z'),
                'system_modstamp': last_modified.isoformat().replace('+00:00', 'Z'),
                'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'source': 'synthetic_generator'
            }
            cases.append(case_data)

        return cases

    def generate_historical_snapshots(self, records: list[dict[str, Any]], snapshot_count: int = 3) -> list[dict[str, Any]]:
        """Generate historical snapshots for SCD Type 2 testing."""
        historical_data = []

        for record in records:
            original_id = record['id']

            for i in range(snapshot_count):
                # Create variations of the original record
                snapshot = record.copy()
                snapshot['id'] = original_id

                # Modify some fields to simulate changes
                if i > 0:
                    if 'name' in snapshot:
                        snapshot['name'] = f"{snapshot['name']} v{i+1}"
                    if 'annual_revenue' in snapshot:
                        snapshot['annual_revenue'] = int(snapshot['annual_revenue'] * (1 + random.uniform(-0.1, 0.2)))
                    if 'amount' in snapshot:
                        snapshot['amount'] = int(snapshot['amount'] * (1 + random.uniform(-0.1, 0.2)))

                # Add historical tracking fields
                valid_from = self._generate_timestamp(days_back=365 - (i * 90))
                valid_to = None if i == snapshot_count - 1 else self._generate_timestamp(days_back=365 - ((i + 1) * 90))

                historical_record = {
                    'id': original_id,
                    'valid_from': valid_from.isoformat().replace('+00:00', 'Z'),
                    'valid_to': valid_to.isoformat().replace('+00:00', 'Z') if valid_to else None,
                    'is_current': i == snapshot_count - 1,
                    'change_type': 'INSERT' if i == 0 else 'UPDATE',
                    'changed_fields': ['name', 'annual_revenue'] if i > 0 else [],
                    'record_data': json.dumps(snapshot),
                    'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                }

                historical_data.append(historical_record)

        return historical_data

    def save_to_json(self, data: list[dict[str, Any]], filename: str, output_dir: str = 'data'):
        """Save generated data to JSON file."""
        import os
        os.makedirs(output_dir, exist_ok=True)

        filepath = os.path.join(output_dir, f"{filename}.json")
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)

        return filepath


def main():
    """Main function for standalone execution."""
    generator = SalesforceDataGenerator()

    # Generate sample data
    print("Generating Salesforce synthetic data...")

    accounts = generator.generate_accounts(count=100)
    contacts = generator.generate_contacts(count=500, account_ids=[acc['id'] for acc in accounts])
    opportunities = generator.generate_opportunities(count=200, account_ids=[acc['id'] for acc in accounts])
    cases = generator.generate_cases(
        count=100,
        account_ids=[acc['id'] for acc in accounts],
        contact_ids=[contact['id'] for contact in contacts]
    )

    # Generate historical data
    accounts_history = generator.generate_historical_snapshots(accounts, snapshot_count=3)

    # Save to files
    print("Saving data to JSON files...")
    generator.save_to_json(accounts, 'accounts', 'data')
    generator.save_to_json(contacts, 'contacts', 'data')
    generator.save_to_json(opportunities, 'opportunities', 'data')
    generator.save_to_json(cases, 'cases', 'data')
    generator.save_to_json(accounts_history, 'accounts_history', 'data')

    print("Salesforce synthetic data generation complete!")
    print(f"Generated: {len(accounts)} accounts, {len(contacts)} contacts, {len(opportunities)} opportunities, {len(cases)} cases")


if __name__ == '__main__':
    main()
