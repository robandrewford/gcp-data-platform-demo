"""
CDC Event Parser

Parse and extract data from CDC events for processing.

Usage:
    from pipelines.utils.cdc_event_parser import CDCEventParser
    
    parser = CDCEventParser()
    parsed_event = parser.parse_event(raw_event)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


class CDCEventParser:
    """Parse CDC events from Pub/Sub messages."""
    
    def __init__(self):
        """Initialize CDC event parser."""
        self.parsed_count = 0
        self.error_count = 0
    
    def parse_pubsub_message(self, message) -> Optional[Dict[str, Any]]:
        """
        Parse CDC event from Pub/Sub message.
        
        Args:
            message: Pub/Sub message object
            
        Returns:
            Parsed CDC event dictionary or None if parsing fails
        """
        try:
            # Decode message data
            data = message.data.decode('utf-8')
            
            # Parse JSON
            event = json.loads(data)
            
            # Add Pub/Sub metadata
            event['_pubsub_message_id'] = message.message_id
            event['_pubsub_publish_time'] = message.publish_time.isoformat()
            
            # Add message attributes
            if message.attributes:
                event['_pubsub_attributes'] = dict(message.attributes)
            
            self.parsed_count += 1
            
            return event
            
        except json.JSONDecodeError as e:
            self.error_count += 1
            logging.error(f"Failed to parse JSON from message {message.message_id}: {str(e)}")
            return None
        except Exception as e:
            self.error_count += 1
            logging.error(f"Failed to parse Pub/Sub message {message.message_id}: {str(e)}")
            return None
    
    def extract_record_for_bigquery(
        self,
        event: Dict[str, Any],
        include_metadata: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Extract record data for BigQuery insertion.
        
        Args:
            event: CDC event dictionary
            include_metadata: Whether to include CDC metadata
            
        Returns:
            Dictionary formatted for BigQuery or None if extraction fails
        """
        try:
            event_type = event.get('event_type')
            object_type = event.get('object_type')
            
            # For INSERT and UPDATE, use 'after' data
            if event_type in ['INSERT', 'UPDATE']:
                record_data = event.get('after', {}).copy()
            # For DELETE, use 'before' data
            elif event_type == 'DELETE':
                record_data = event.get('before', {}).copy()
            else:
                logging.error(f"Unknown event type: {event_type}")
                return None
            
            # Add ingestion metadata if requested
            if include_metadata:
                record_data['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                record_data['source'] = 'salesforce_cdc'
                record_data['_cdc_event_id'] = event.get('event_id')
                record_data['_cdc_event_type'] = event_type
                record_data['_cdc_event_timestamp'] = event.get('event_timestamp')
                
                # Add Pub/Sub metadata if available
                if '_pubsub_message_id' in event:
                    record_data['_pubsub_message_id'] = event['_pubsub_message_id']
            
            return record_data
            
        except Exception as e:
            logging.error(f"Failed to extract record from event: {str(e)}")
            return None
    
    def extract_history_record(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract data for SCD Type 2 history table.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Dictionary formatted for history table or None if extraction fails
        """
        try:
            event_type = event.get('event_type')
            
            # Determine record data based on event type
            if event_type in ['INSERT', 'UPDATE']:
                record_data = event.get('after', {})
            elif event_type == 'DELETE':
                record_data = event.get('before', {})
            else:
                return None
            
            # Create history record
            history_record = {
                'id': event.get('record_id'),
                'valid_from': event.get('event_timestamp'),
                'valid_to': None,  # Will be set during SCD Type 2 processing
                'is_current': True,  # Will be updated during SCD Type 2 processing
                'change_type': event_type,
                'changed_fields': json.dumps(event.get('changed_fields', [])),
                'record_data': json.dumps(record_data),
                'ingestion_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }
            
            return history_record
            
        except Exception as e:
            logging.error(f"Failed to extract history record from event: {str(e)}")
            return None
    
    def extract_changed_fields(self, event: Dict[str, Any]) -> List[str]:
        """
        Extract list of changed fields from event.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            List of changed field names
        """
        return event.get('changed_fields', [])
    
    def extract_before_after(self, event: Dict[str, Any]) -> tuple[Optional[Dict], Optional[Dict]]:
        """
        Extract before and after record states.
        
        Args:
            event: CDC event dictionary
            
        Returns:
            Tuple of (before_state, after_state)
        """
        before = event.get('before')
        after = event.get('after')
        return before, after
    
    def normalize_field_names(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize field names to lowercase with underscores.
        
        Args:
            record: Record dictionary
            
        Returns:
            Record with normalized field names
        """
        normalized = {}
        for key, value in record.items():
            # Convert camelCase to snake_case
            normalized_key = ''.join(['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
            normalized[normalized_key] = value
        return normalized
    
    def format_for_json_field(self, value: Any) -> str:
        """
        Format complex values for BigQuery JSON fields.
        
        Args:
            value: Value to format
            
        Returns:
            JSON string representation
        """
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get parser statistics.
        
        Returns:
            Dictionary with parsing stats
        """
        return {
            'parsed_count': self.parsed_count,
            'error_count': self.error_count,
            'success_rate': (
                self.parsed_count / (self.parsed_count + self.error_count)
                if (self.parsed_count + self.error_count) > 0
                else 0.0
            )
        }
    
    def reset_statistics(self):
        """Reset parser statistics."""
        self.parsed_count = 0
        self.error_count = 0


class BeamCDCEventParser:
    """CDC Event Parser for Apache Beam DoFn usage."""
    
    @staticmethod
    def parse_json(element: bytes) -> Dict[str, Any]:
        """
        Parse JSON from bytes (for use in Beam Map).
        
        Args:
            element: JSON bytes
            
        Returns:
            Parsed dictionary
        """
        return json.loads(element.decode('utf-8'))
    
    @staticmethod
    def extract_record(element: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract record for BigQuery (for use in Beam Map).
        
        Args:
            element: CDC event dictionary
            
        Returns:
            Record data
        """
        event_type = element.get('event_type')
        
        if event_type in ['INSERT', 'UPDATE']:
            record = element.get('after', {}).copy()
        elif event_type == 'DELETE':
            record = element.get('before', {}).copy()
        else:
            record = {}
        
        # Add metadata
        record['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        record['source'] = 'salesforce_cdc'
        record['_cdc_event_type'] = event_type
        
        return record
    
    @staticmethod
    def add_processing_timestamp(element: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add processing timestamp to record.
        
        Args:
            element: Record dictionary
            
        Returns:
            Record with processing timestamp
        """
        element['_processing_timestamp'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        return element
