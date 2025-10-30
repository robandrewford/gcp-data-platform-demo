"""
Salesforce CDC Event Publisher

Publishes CDC events to Google Cloud Pub/Sub for streaming processing.

Usage:
    from src.salesforce.cdc_publisher import CDCEventPublisher
    
    publisher = CDCEventPublisher(
        project_id='my-project',
        topic_name='stream-processing'
    )
    
    events = [...]  # CDC events from simulator
    publisher.publish_events(events)
"""

import json
import logging
from typing import Dict, List, Any, Optional
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.api_core import retry


class CDCEventPublisher:
    """Publish CDC events to Google Cloud Pub/Sub."""
    
    def __init__(
        self,
        project_id: str,
        topic_name: str,
        batch_settings: Optional[pubsub_v1.types.BatchSettings] = None
    ):
        """
        Initialize CDC event publisher.
        
        Args:
            project_id: GCP project ID
            topic_name: Pub/Sub topic name (without project prefix)
            batch_settings: Optional batch settings for publisher
        """
        self.project_id = project_id
        self.topic_name = topic_name
        
        # Configure batch settings for optimal throughput
        if batch_settings is None:
            batch_settings = pubsub_v1.types.BatchSettings(
                max_bytes=1024 * 1024,  # 1 MB
                max_latency=0.1,  # 100 ms
                max_messages=100,  # messages per batch
            )
        
        # Create publisher client
        self.publisher = pubsub_v1.PublisherClient(batch_settings)
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
        # Statistics
        self.published_count = 0
        self.failed_count = 0
        
        logging.info(f"CDC Publisher initialized for topic: {self.topic_path}")
    
    def _serialize_event(self, event: Dict[str, Any]) -> bytes:
        """
        Serialize CDC event to bytes for Pub/Sub.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Serialized event as bytes
        """
        return json.dumps(event, default=str).encode('utf-8')
    
    def _create_message_attributes(self, event: Dict[str, Any]) -> Dict[str, str]:
        """
        Create Pub/Sub message attributes for routing and filtering.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Dictionary of message attributes
        """
        return {
            'event_type': event.get('event_type', ''),
            'object_type': event.get('object_type', ''),
            'source': event.get('source', 'salesforce_cdc'),
            'event_timestamp': event.get('event_timestamp', ''),
        }
    
    def publish_event(
        self,
        event: Dict[str, Any],
        timeout: float = 30.0
    ) -> str:
        """
        Publish single CDC event to Pub/Sub.
        
        Args:
            event: CDC event dictionary
            timeout: Publish timeout in seconds
            
        Returns:
            Message ID from Pub/Sub
            
        Raises:
            Exception if publish fails
        """
        try:
            # Serialize event
            data = self._serialize_event(event)
            
            # Create message attributes
            attributes = self._create_message_attributes(event)
            
            # Publish to Pub/Sub
            future = self.publisher.publish(
                self.topic_path,
                data=data,
                **attributes
            )
            
            # Wait for publish to complete
            message_id = future.result(timeout=timeout)
            
            self.published_count += 1
            logging.debug(f"Published event {event.get('event_id')} with message ID: {message_id}")
            
            return message_id
            
        except TimeoutError:
            self.failed_count += 1
            logging.error(f"Timeout publishing event {event.get('event_id')}")
            raise
        except Exception as e:
            self.failed_count += 1
            logging.error(f"Failed to publish event {event.get('event_id')}: {str(e)}")
            raise
    
    def publish_events(
        self,
        events: List[Dict[str, Any]],
        timeout: float = 60.0
    ) -> Dict[str, Any]:
        """
        Publish multiple CDC events to Pub/Sub.
        
        Args:
            events: List of CDC event dictionaries
            timeout: Total timeout for all publishes in seconds
            
        Returns:
            Dictionary with publish statistics
        """
        futures = []
        start_count = self.published_count
        
        logging.info(f"Publishing {len(events)} CDC events to {self.topic_path}")
        
        # Publish all events (returns futures)
        for event in events:
            try:
                data = self._serialize_event(event)
                attributes = self._create_message_attributes(event)
                
                future = self.publisher.publish(
                    self.topic_path,
                    data=data,
                    **attributes
                )
                futures.append((event.get('event_id'), future))
                
            except Exception as e:
                logging.error(f"Failed to initiate publish for event {event.get('event_id')}: {str(e)}")
                self.failed_count += 1
        
        # Wait for all futures to complete
        successful = 0
        failed = 0
        
        for event_id, future in futures:
            try:
                message_id = future.result(timeout=timeout)
                successful += 1
                self.published_count += 1
                logging.debug(f"Published event {event_id} with message ID: {message_id}")
            except Exception as e:
                failed += 1
                self.failed_count += 1
                logging.error(f"Failed to publish event {event_id}: {str(e)}")
        
        stats = {
            'total_events': len(events),
            'successful': successful,
            'failed': failed,
            'published_this_batch': successful,
            'total_published': self.published_count,
            'total_failed': self.failed_count
        }
        
        logging.info(f"Publish complete: {successful} successful, {failed} failed")
        
        return stats
    
    def publish_events_with_rate_limit(
        self,
        events: List[Dict[str, Any]],
        events_per_second: int = 100,
        timeout: float = 60.0
    ) -> Dict[str, Any]:
        """
        Publish events with rate limiting.
        
        Args:
            events: List of CDC event dictionaries
            events_per_second: Maximum events to publish per second
            timeout: Timeout per event in seconds
            
        Returns:
            Dictionary with publish statistics
        """
        import time
        
        interval = 1.0 / events_per_second
        start_count = self.published_count
        
        logging.info(f"Publishing {len(events)} CDC events at {events_per_second} events/sec")
        
        for i, event in enumerate(events):
            try:
                self.publish_event(event, timeout=timeout)
                
                # Rate limiting
                if i < len(events) - 1:  # Don't sleep after last event
                    time.sleep(interval)
                    
            except Exception as e:
                logging.error(f"Failed to publish event {i+1}/{len(events)}: {str(e)}")
        
        stats = {
            'total_events': len(events),
            'successful': self.published_count - start_count,
            'failed': self.failed_count - (self.published_count - start_count),
            'total_published': self.published_count,
            'total_failed': self.failed_count
        }
        
        logging.info(f"Rate-limited publish complete: {stats['successful']} successful, {stats['failed']} failed")
        
        return stats
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get publisher statistics.
        
        Returns:
            Dictionary with publish statistics
        """
        return {
            'total_published': self.published_count,
            'total_failed': self.failed_count,
            'success_rate': (
                self.published_count / (self.published_count + self.failed_count)
                if (self.published_count + self.failed_count) > 0
                else 0.0
            )
        }
    
    def reset_statistics(self):
        """Reset publisher statistics."""
        self.published_count = 0
        self.failed_count = 0
    
    def close(self):
        """Close the publisher and flush pending messages."""
        logging.info("Closing CDC publisher...")
        # The publisher client doesn't have an explicit close method
        # But we can ensure all pending messages are sent
        self.publisher.stop()
        logging.info("CDC publisher closed")


def main():
    """Main function for standalone execution."""
    import argparse
    import os
    from .cdc_event_simulator import CDCEventSimulator
    
    parser = argparse.ArgumentParser(description='Publish Salesforce CDC events to Pub/Sub')
    parser.add_argument('--project-id', required=True, help='GCP project ID')
    parser.add_argument('--topic-name', default='stream-processing', help='Pub/Sub topic name')
    parser.add_argument('--object-type', required=True, choices=['Account', 'Contact', 'Opportunity', 'Case'],
                       help='Salesforce object type')
    parser.add_argument('--count', type=int, default=100, help='Number of events to generate and publish')
    parser.add_argument('--preload', type=int, default=50, help='Number of records to preload')
    parser.add_argument('--rate-limit', type=int, help='Events per second (optional)')
    parser.add_argument('--continuous', action='store_true', help='Publish continuously')
    parser.add_argument('--interval', type=int, default=5, help='Interval between batches in continuous mode (seconds)')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize simulator and publisher
    simulator = CDCEventSimulator()
    publisher = CDCEventPublisher(
        project_id=args.project_id,
        topic_name=args.topic_name
    )
    
    try:
        # Preload existing records
        logging.info(f"Preloading {args.preload} {args.object_type} records...")
        simulator.preload_existing_records(args.object_type, args.preload)
        
        if args.continuous:
            # Continuous publishing mode
            logging.info("Starting continuous CDC event publishing (Ctrl+C to stop)...")
            batch_num = 0
            
            while True:
                batch_num += 1
                logging.info(f"\n=== Batch {batch_num} ===")
                
                # Generate events
                events = simulator.generate_cdc_events(args.object_type, args.count)
                
                # Publish events
                if args.rate_limit:
                    stats = publisher.publish_events_with_rate_limit(
                        events,
                        events_per_second=args.rate_limit
                    )
                else:
                    stats = publisher.publish_events(events)
                
                logging.info(f"Batch {batch_num} stats: {stats}")
                
                # Wait before next batch
                import time
                time.sleep(args.interval)
        else:
            # Single batch mode
            logging.info(f"Generating {args.count} CDC events for {args.object_type}...")
            events = simulator.generate_cdc_events(args.object_type, args.count)
            
            # Publish events
            if args.rate_limit:
                stats = publisher.publish_events_with_rate_limit(
                    events,
                    events_per_second=args.rate_limit
                )
            else:
                stats = publisher.publish_events(events)
            
            logging.info(f"\nFinal statistics:")
            logging.info(f"  Total events: {stats['total_events']}")
            logging.info(f"  Successful: {stats['successful']}")
            logging.info(f"  Failed: {stats['failed']}")
            logging.info(f"  Success rate: {stats['successful']/stats['total_events']*100:.1f}%")
    
    except KeyboardInterrupt:
        logging.info("\nStopping CDC event publishing...")
    
    finally:
        # Clean up
        publisher.close()
        logging.info("Publisher closed successfully")


if __name__ == '__main__':
    main()
