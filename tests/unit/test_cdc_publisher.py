"""
Unit tests for CDC Event Publisher.

Tests the CDCEventPublisher class which publishes CDC events to Google Cloud Pub/Sub.
"""

import json
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any
from unittest.mock import Mock, patch

import pytest

from src.salesforce.cdc_publisher import CDCEventPublisher


class TestCDCEventPublisherInitialization:
    """Test CDCEventPublisher initialization."""

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_initialization_default_settings(self, mock_publisher_class):
        """Test initialization with default batch settings."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher(
            project_id='test-project',
            topic_name='test-topic'
        )

        assert publisher.project_id == 'test-project'
        assert publisher.topic_name == 'test-topic'
        assert publisher.published_count == 0
        assert publisher.failed_count == 0
        assert mock_publisher_class.called

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_initialization_custom_batch_settings(self, mock_publisher_class):
        """Test initialization with custom batch settings."""
        from google.cloud import pubsub_v1

        mock_client = Mock()
        mock_publisher_class.return_value = mock_client

        custom_settings = pubsub_v1.types.BatchSettings(
            max_bytes=2048,
            max_latency=0.5,
            max_messages=50
        )

        publisher = CDCEventPublisher(
            project_id='test-project',
            topic_name='test-topic',
            batch_settings=custom_settings
        )

        assert publisher.project_id == 'test-project'
        mock_publisher_class.assert_called_once()

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_topic_path_construction(self, mock_publisher_class):
        """Test topic path is correctly constructed."""
        mock_client = Mock()
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher(
            project_id='test-project',
            topic_name='test-topic'
        )

        mock_client.topic_path.assert_called_once_with('test-project', 'test-topic')
        assert publisher.topic_path == 'projects/test-project/topics/test-topic'


class TestMessageSerialization:
    """Test event serialization and message attributes."""

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_serialize_event(self, mock_publisher_class, sample_cdc_insert_event: dict[str, Any]):
        """Test event serialization to bytes."""
        mock_publisher_class.return_value = Mock()

        publisher = CDCEventPublisher('test-project', 'test-topic')
        serialized = publisher._serialize_event(sample_cdc_insert_event)

        assert isinstance(serialized, bytes)

        # Verify can be deserialized
        deserialized = json.loads(serialized.decode('utf-8'))
        assert deserialized['event_type'] == 'INSERT'
        assert deserialized['object_type'] == 'Account'

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_create_message_attributes(self, mock_publisher_class, sample_cdc_update_event: dict[str, Any]):
        """Test message attributes creation."""
        mock_publisher_class.return_value = Mock()

        publisher = CDCEventPublisher('test-project', 'test-topic')
        attributes = publisher._create_message_attributes(sample_cdc_update_event)

        assert attributes['event_type'] == 'UPDATE'
        assert attributes['object_type'] == 'Account'
        assert attributes['source'] == 'salesforce_cdc'
        assert 'event_timestamp' in attributes

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_serialize_event_with_complex_data(self, mock_publisher_class):
        """Test serialization with complex data types."""
        mock_publisher_class.return_value = Mock()

        from datetime import datetime
        event = {
            'event_id': 'test-123',
            'timestamp': datetime(2025, 10, 30, 10, 0, 0),
            'data': {
                'nested': {'field': 'value'}
            }
        }

        publisher = CDCEventPublisher('test-project', 'test-topic')
        serialized = publisher._serialize_event(event)

        assert isinstance(serialized, bytes)
        # Should not raise error


class TestSingleEventPublishing:
    """Test publishing single events."""

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_event_success(self, mock_publisher_class, sample_cdc_insert_event: dict[str, Any]):
        """Test successful single event publish."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'message-id-123'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        message_id = publisher.publish_event(sample_cdc_insert_event)

        assert message_id == 'message-id-123'
        assert publisher.published_count == 1
        assert publisher.failed_count == 0
        mock_client.publish.assert_called_once()

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_event_timeout(self, mock_publisher_class, sample_cdc_insert_event: dict[str, Any]):
        """Test publish timeout handling."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.side_effect = FuturesTimeoutError()
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')

        with pytest.raises(FuturesTimeoutError):
            publisher.publish_event(sample_cdc_insert_event, timeout=1.0)

        assert publisher.published_count == 0
        assert publisher.failed_count == 1

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_event_failure(self, mock_publisher_class, sample_cdc_insert_event: dict[str, Any]):
        """Test publish failure with exception."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.side_effect = Exception("Publish failed")
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')

        with pytest.raises(Exception, match="Publish failed"):
            publisher.publish_event(sample_cdc_insert_event)

        assert publisher.published_count == 0
        assert publisher.failed_count == 1

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_event_statistics_update(self, mock_publisher_class, sample_cdc_insert_event: dict[str, Any]):
        """Test statistics update after publish."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'msg-123'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')

        # Publish multiple events
        publisher.publish_event(sample_cdc_insert_event)
        publisher.publish_event(sample_cdc_insert_event)

        assert publisher.published_count == 2
        assert publisher.failed_count == 0


class TestBatchEventPublishing:
    """Test publishing batches of events."""

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_events_success(self, mock_publisher_class, sample_cdc_events_batch):
        """Test successful batch publish."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'msg-id'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.publish_events(sample_cdc_events_batch)

        assert stats['total_events'] == 2
        assert stats['successful'] == 2
        assert stats['failed'] == 0
        assert publisher.published_count == 2

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_events_mixed_results(self, mock_publisher_class, sample_cdc_events_batch):
        """Test mixed success/failure in batch."""
        mock_client = Mock()

        # First publish succeeds, second fails
        mock_future_success = Mock()
        mock_future_success.result.return_value = 'msg-id'
        mock_future_fail = Mock()
        mock_future_fail.result.side_effect = Exception("Failed")

        mock_client.publish.side_effect = [mock_future_success, mock_future_fail]
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.publish_events(sample_cdc_events_batch)

        assert stats['total_events'] == 2
        assert stats['successful'] == 1
        assert stats['failed'] == 1

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_events_empty_list(self, mock_publisher_class):
        """Test publishing empty event list."""
        mock_client = Mock()
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.publish_events([])

        assert stats['total_events'] == 0
        assert stats['successful'] == 0
        assert stats['failed'] == 0

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_events_statistics_tracking(self, mock_publisher_class, sample_cdc_events_batch):
        """Test statistics tracking across batches."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'msg-id'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')

        # Publish first batch
        stats1 = publisher.publish_events(sample_cdc_events_batch)
        assert stats1['total_published'] == 2

        # Publish second batch
        stats2 = publisher.publish_events(sample_cdc_events_batch)
        assert stats2['total_published'] == 4

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_publish_events_timeout_handling(self, mock_publisher_class, sample_cdc_events_batch):
        """Test timeout handling in batch publish."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.side_effect = FuturesTimeoutError()
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.publish_events(sample_cdc_events_batch, timeout=1.0)

        assert stats['total_events'] == 2
        assert stats['failed'] == 2
        assert stats['successful'] == 0


class TestRateLimitedPublishing:
    """Test rate-limited publishing."""

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    @patch('time.sleep')
    def test_rate_limiting_enforcement(self, mock_sleep, mock_publisher_class, sample_cdc_events_batch):
        """Test rate limiting is enforced."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'msg-id'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.publish_events_with_rate_limit(
            sample_cdc_events_batch,
            events_per_second=10
        )

        assert stats['successful'] == 2
        # Should sleep between events (but not after last one)
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.1)  # 1/10 = 0.1

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    @patch('time.sleep')
    def test_rate_limiting_timing(self, mock_sleep, mock_publisher_class, sample_cdc_events_batch):
        """Test timing between events."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'msg-id'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        publisher.publish_events_with_rate_limit(
            sample_cdc_events_batch,
            events_per_second=2  # 0.5 seconds between events
        )

        mock_sleep.assert_called_with(0.5)

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    @patch('time.sleep')
    def test_rate_limiting_statistics(self, mock_sleep, mock_publisher_class, sample_cdc_events_batch):
        """Test statistics with rate limiting."""
        mock_client = Mock()
        mock_future = Mock()
        mock_future.result.return_value = 'msg-id'
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.publish_events_with_rate_limit(
            sample_cdc_events_batch,
            events_per_second=5
        )

        assert stats['total_events'] == 2
        assert stats['successful'] == 2
        assert stats['total_published'] == 2
        assert stats['total_failed'] == 0


class TestStatisticsAndCleanup:
    """Test statistics and cleanup operations."""

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_get_statistics(self, mock_publisher_class):
        """Test getting publisher statistics."""
        mock_client = Mock()
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        publisher.published_count = 10
        publisher.failed_count = 2

        stats = publisher.get_statistics()

        assert stats['total_published'] == 10
        assert stats['total_failed'] == 2
        assert stats['success_rate'] == pytest.approx(10/12)

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_reset_statistics(self, mock_publisher_class):
        """Test resetting statistics."""
        mock_client = Mock()
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        publisher.published_count = 10
        publisher.failed_count = 2

        publisher.reset_statistics()

        assert publisher.published_count == 0
        assert publisher.failed_count == 0

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_get_statistics_zero_division(self, mock_publisher_class):
        """Test statistics calculation with no events."""
        mock_client = Mock()
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        stats = publisher.get_statistics()

        assert stats['success_rate'] == 0.0

    @patch('src.salesforce.cdc_publisher.pubsub_v1.PublisherClient')
    def test_close_publisher(self, mock_publisher_class):
        """Test closing publisher."""
        mock_client = Mock()
        mock_client.topic_path.return_value = 'projects/test-project/topics/test-topic'
        mock_client.stop = Mock()
        mock_publisher_class.return_value = mock_client

        publisher = CDCEventPublisher('test-project', 'test-topic')
        publisher.close()

        mock_client.stop.assert_called_once()
