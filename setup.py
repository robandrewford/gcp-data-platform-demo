"""
Setup configuration for Apache Beam Dataflow workers.

This file is used by Dataflow workers to install dependencies.
"""

from setuptools import setup, find_packages

setup(
    name='gcp-data-platform-demo',
    version='0.1.0',
    description='GCP Data Platform Demo - Salesforce Batch Pipeline',
    author='GCP Data Platform Demo',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    python_requires='>=3.9',
    install_requires=[
        'apache-beam[gcp]>=2.54.0',
        'google-cloud-bigquery>=3.13.0',
        'google-cloud-storage>=2.14.0',
        'google-cloud-pubsub>=2.19.0',
        'requests>=2.31.0',
        'pyyaml>=6.0.0',
        'pyarrow>=15.0.0',
    ],
    extras_require={
        'test': [
            'pytest>=8.0.0',
            'pytest-cov>=5.0.0',
            'pytest-mock>=3.12.0',
            'requests-mock>=1.11.0',
            'faker>=25.0.0',
        ],
    },
)
