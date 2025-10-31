"""
Salesforce REST API Client

This module simulates a Salesforce REST API client for testing
data extraction pipelines. It provides realistic API behavior including
pagination, rate limiting, and error handling.

Usage:
    from src.salesforce.api_client import SalesforceAPIClient

    client = SalesforceAPIClient(base_url="http://localhost:8080")
    accounts = client.query_accounts(limit=1000)
"""

import random
import time
from datetime import datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SalesforceAPIClient:
    """Simulated Salesforce REST API client for testing data pipelines."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        api_version: str = "v57.0",
        access_token: str = "mock_token",
        rate_limit_delay: float = 0.1
    ):
        """Initialize the Salesforce API client."""
        self.base_url = base_url.rstrip('/')
        self.api_version = api_version
        self.access_token = access_token
        self.rate_limit_delay = rate_limit_delay
        self.session = self._create_session()

        # Mock data for simulation
        self._mock_data = {}
        self._initialize_mock_data()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers
        session.headers.update({
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

        return session

    def _initialize_mock_data(self):
        """Initialize mock Salesforce data for simulation."""
        from .data_generator import SalesforceDataGenerator

        generator = SalesforceDataGenerator()

        # Generate mock data
        self._mock_data = {
            'accounts': generator.generate_accounts(count=1000),
            'contacts': generator.generate_contacts(count=5000),
            'opportunities': generator.generate_opportunities(count=2000),
            'cases': generator.generate_cases(count=1000)
        }

    def _simulate_rate_limit(self):
        """Simulate API rate limiting."""
        if random.random() < 0.1:  # 10% chance of rate limit
            time.sleep(random.uniform(1.0, 3.0))
            return True
        return False

    def _simulate_error(self, error_rate: float = 0.02) -> Optional[str]:
        """Simulate random API errors."""
        if random.random() < error_rate:
            errors = [
                "INVALID_SESSION", "INVALID_FIELD", "MALFORMED_QUERY",
                "SERVER_UNAVAILABLE", "TIMEOUT_EXCEEDED"
            ]
            return random.choice(errors)
        return None

    def _make_request(self, endpoint: str, params: dict[str, Any] = None) -> dict[str, Any]:
        """Make API request with error handling and rate limiting."""
        url = f"{self.base_url}/services/data/{self.api_version}/{endpoint}"

        # Simulate rate limiting
        if self._simulate_rate_limit():
            raise requests.exceptions.RequestException("Rate limit exceeded. Please try again later.")

        # Simulate random errors
        error = self._simulate_error()
        if error:
            raise requests.exceptions.RequestException(f"Salesforce API Error: {error}")

        # Simulate network delay
        time.sleep(self.rate_limit_delay)

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            raise

    def query_accounts(
        self,
        limit: int = 1000,
        fields: Optional[list[str]] = None,
        where_clause: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Query Salesforce accounts using SOQL-like interface.

        Args:
            limit: Maximum number of records to return
            fields: List of fields to retrieve
            where_clause: WHERE clause for filtering

        Returns:
            List of account records
        """
        # Default fields if not specified
        if fields is None:
            fields = [
                'Id', 'Name', 'Type', 'Industry', 'AnnualRevenue',
                'Phone', 'Website', 'BillingAddress', 'ShippingAddress',
                'CreatedDate', 'LastModifiedDate', 'SystemModstamp'
            ]

        # Build query parameters
        params = {
            'q': self._build_soql_query('Account', fields, where_clause),
            'limit': limit
        }

        print(f"Querying {limit} accounts...")
        result = self._make_request('query', params)

        # Handle pagination
        records = result.get('records', [])
        while 'nextRecordsUrl' in result and len(records) < limit:
            next_url = result['nextRecordsUrl']
            if next_url:
                # Extract query parameters from next URL
                next_params = {'q': next_url.split('q=')[1]}
                next_result = self._make_request('query', next_params)
                records.extend(next_result.get('records', []))
                result = next_result
            else:
                break

        print(f"Retrieved {len(records)} accounts")
        return records[:limit]

    def query_contacts(
        self,
        limit: int = 5000,
        account_ids: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        """Query Salesforce contacts."""
        fields = [
            'Id', 'AccountId', 'FirstName', 'LastName', 'Email', 'Phone',
            'Title', 'LeadSource', 'CreatedDate', 'LastModifiedDate', 'SystemModstamp'
        ]

        # Build WHERE clause for account filtering
        where_clause = None
        if account_ids:
            account_list = "', '".join(account_ids)
            where_clause = f"AccountId IN ('{account_list}')"

        params = {
            'q': self._build_soql_query('Contact', fields, where_clause),
            'limit': limit
        }

        print(f"Querying {limit} contacts...")
        result = self._make_request('query', params)
        return result.get('records', [])

    def query_opportunities(
        self,
        limit: int = 2000,
        account_ids: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        """Query Salesforce opportunities."""
        fields = [
            'Id', 'AccountId', 'Name', 'StageName', 'Type', 'LeadSource',
            'Amount', 'Probability', 'CloseDate', 'IsWon', 'IsClosed',
            'CreatedDate', 'LastModifiedDate', 'SystemModstamp'
        ]

        # Build WHERE clause for account filtering
        where_clause = None
        if account_ids:
            account_list = "', '".join(account_ids)
            where_clause = f"AccountId IN ('{account_list}')"

        params = {
            'q': self._build_soql_query('Opportunity', fields, where_clause),
            'limit': limit
        }

        print(f"Querying {limit} opportunities...")
        result = self._make_request('query', params)
        return result.get('records', [])

    def query_cases(
        self,
        limit: int = 1000,
        account_ids: Optional[list[str]] = None,
        contact_ids: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        """Query Salesforce cases."""
        fields = [
            'Id', 'AccountId', 'ContactId', 'Subject', 'Description', 'Status',
            'Origin', 'Priority', 'IsEscalated', 'IsClosed', 'ClosedDate',
            'CreatedDate', 'LastModifiedDate', 'SystemModstamp'
        ]

        # Build WHERE clause for filtering
        where_clauses = []
        if account_ids:
            account_list = "', '".join(account_ids)
            where_clauses.append(f"AccountId IN ('{account_list}')")
        if contact_ids:
            contact_list = "', '".join(contact_ids)
            where_clauses.append(f"ContactId IN ('{contact_list}')")

        where_clause = ' AND '.join(where_clauses) if where_clauses else None

        params = {
            'q': self._build_soql_query('Case', fields, where_clause),
            'limit': limit
        }

        print(f"Querying {limit} cases...")
        result = self._make_request('query', params)
        return result.get('records', [])

    def _build_soql_query(self, object_name: str, fields: list[str], where_clause: Optional[str]) -> str:
        """Build SOQL query string."""
        fields_str = ', '.join(fields)
        query = f"SELECT {fields_str} FROM {object_name}"

        if where_clause:
            query += f" WHERE {where_clause}"

        query += " ORDER BY CreatedDate DESC"
        return query

    def get_object_describe(self, object_name: str) -> dict[str, Any]:
        """Get object metadata including fields and relationships."""
        endpoint = f"sobjects/{object_name}/describe"

        print(f"Getting describe for {object_name}...")
        return self._make_request(endpoint)

    def get_recent_changes(
        self,
        object_name: str,
        since: datetime,
        limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Get recent changes for CDC simulation."""
        # This simulates the Change Data Capture functionality
        endpoint = f"sobjects/{object_name}/updated"

        params = {
            'start': since.isoformat(),
            'limit': limit
        }

        print(f"Getting recent changes for {object_name} since {since}...")
        return self._make_request(endpoint, params)


class MockSalesforceAPI(SalesforceAPIClient):
    """Mock Salesforce API that serves generated data for testing."""

    def __init__(self, port: int = 8080):
        """Initialize mock API server."""
        super().__init__(base_url=f"http://localhost:{port}")
        self.port = port

    def _make_request(self, endpoint: str, params: dict[str, Any] = None) -> dict[str, Any]:
        """Mock request that returns generated data."""
        # Parse query to determine what data to return
        query = params.get('q', '')

        # Extract object name from query
        if 'FROM Account' in query:
            records = self._mock_data['accounts']
        elif 'FROM Contact' in query:
            records = self._mock_data['contacts']
        elif 'FROM Opportunity' in query:
            records = self._mock_data['opportunities']
        elif 'FROM Case' in query:
            records = self._mock_data['cases']
        else:
            records = []

        # Apply LIMIT clause if present
        if 'LIMIT' in query.upper():
            try:
                limit = int(query.split('LIMIT')[1].strip())
                records = records[:limit]
            except (IndexError, ValueError):
                pass

        # Apply WHERE filtering (simplified)
        if 'AccountId IN' in query:
            # Extract account IDs from WHERE clause
            import re
            account_ids = re.findall(r"'([a-zA-Z0-9]+)'", query)
            if account_ids:
                records = [r for r in records if r.get('account_id') in account_ids]

        # Apply ORDER BY
        if 'ORDER BY CreatedDate DESC' in query:
            records.sort(key=lambda x: x.get('created_date', ''), reverse=True)

        return {
            'records': records,
            'totalSize': len(records),
            'done': True
        }


def main():
    """Main function for testing the API client."""
    # Test the mock API
    client = SalesforceAPIClient()

    try:
        # Test account queries
        accounts = client.query_accounts(limit=10)
        print(f"Retrieved {len(accounts)} accounts")

        # Test contact queries
        contacts = client.query_contacts(limit=10)
        print(f"Retrieved {len(contacts)} contacts")

        # Test opportunity queries
        opportunities = client.query_opportunities(limit=10)
        print(f"Retrieved {len(opportunities)} opportunities")

        # Test case queries
        cases = client.query_cases(limit=10)
        print(f"Retrieved {len(cases)} cases")

        # Test object describe
        account_desc = client.get_object_describe('Account')
        print(f"Account object description: {len(account_desc.get('fields', []))} fields")

    except Exception as e:
        print(f"API client test failed: {e}")


if __name__ == '__main__':
    main()
