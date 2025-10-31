"""
Salesforce Module

This package provides Salesforce-specific functionality for the GCP data platform,
including data generation, API client, and schema validation.

Components:
- data_generator: Generate synthetic Salesforce data for testing
- api_client: Simulate Salesforce REST API client
- schemas: Define Salesforce object schemas and validation rules
"""

from .api_client import MockSalesforceAPI, SalesforceAPIClient
from .data_generator import SalesforceDataGenerator
from .schemas import SalesforceSchemas

__all__ = [
    'SalesforceDataGenerator',
    'SalesforceAPIClient',
    'MockSalesforceAPI',
    'SalesforceSchemas'
]

__version__ = '1.0.0'
