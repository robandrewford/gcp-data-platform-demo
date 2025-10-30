"""
Salesforce CDC Event Simulator

Simulates Change Data Capture (CDC) events for Salesforce objects.
Generates INSERT, UPDATE, and DELETE events with before/after snapshots.

Usage:
    from src.salesforce.cdc_event_simulator import CDCEventSimulator
    
    simulator = CDCEventSimulator()
    events = simulator.generate_cdc_events(
        object_type='Account',
        event_count=100,
        event_distribution={'INSERT': 0.5, 'UPDATE': 0.4, 'DELETE': 0.1}
    )
"""

import json
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from enum import Enum

from .data_generator import SalesforceDataGenerator


class CDCEventType(Enum):
    """CDC event types."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class CDCEventSimulator:
    """Simulate CDC events for Salesforce objects."""
    
    def __init__(self):
        """Initialize CDC event simulator."""
        self.data_generator = SalesforceDataGenerator()
        self._existing_records: Dict[str, Dict[str, Any]] = {
            'Account': {},
            'Contact': {},
            'Opportunity': {},
            'Case': {}
        }
    
    def _generate_cdc_event_id(self) -> str:
        """Generate unique CDC event ID."""
        chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        return 'CDC-' + ''.join(random.choices(chars, k=12))
    
    def _get_changed_fields(self, before: Dict[str, Any], after: Dict[str, Any]) -> List[str]:
        """
        Identify changed fields between before and after states.
        
        Args:
            before: Record state before change
            after: Record state after change
            
        Returns:
            List of field names that changed
        """
        changed = []
        
        # Compare all fields that exist in both records
        all_fields = set(before.keys()) | set(after.keys())
        
        for field in all_fields:
            before_val = before.get(field)
            after_val = after.get(field)
            
            # Skip metadata fields
            if field in ['ingestion_timestamp', 'source', 'system_modstamp']:
                continue
            
            # Compare values
            if before_val != after_val:
                changed.append(field)
        
        return changed
    
    def _modify_record_for_update(self, record: Dict[str, Any], object_type: str) -> Dict[str, Any]:
        """
        Modify a record to simulate an update.
        
        Args:
            record: Original record
            object_type: Salesforce object type
            
        Returns:
            Modified record
        """
        modified = record.copy()
        
        # Update timestamp fields
        modified['last_modified_date'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        modified['system_modstamp'] = modified['last_modified_date']
        
        # Modify specific fields based on object type
        if object_type == 'Account':
            modifications = [
                lambda r: r.update({'name': r['name'] + ' (Updated)'}),
                lambda r: r.update({'annual_revenue': int(r.get('annual_revenue', 0) * random.uniform(0.9, 1.2))}),
                lambda r: r.update({'phone': self.data_generator.fake.phone_number()}),
                lambda r: r.update({'type': random.choice(self.data_generator.account_types)}),
            ]
        elif object_type == 'Contact':
            modifications = [
                lambda r: r.update({'email': self.data_generator.fake.email()}),
                lambda r: r.update({'phone': self.data_generator.fake.phone_number()}),
                lambda r: r.update({'title': random.choice(self.data_generator.contact_titles)}),
            ]
        elif object_type == 'Opportunity':
            modifications = [
                lambda r: r.update({'amount': int(r.get('amount', 0) * random.uniform(0.8, 1.3))}),
                lambda r: r.update({'stage_name': random.choice(self.data_generator.opportunity_stages)}),
                lambda r: r.update({'probability': random.randint(1, 100)}),
                lambda r: r.update({'is_won': random.choice([True, False])}),
            ]
        elif object_type == 'Case':
            modifications = [
                lambda r: r.update({'status': random.choice(self.data_generator.case_statuses)}),
                lambda r: r.update({'priority': random.choice(self.data_generator.case_priorities)}),
                lambda r: r.update({'is_escalated': random.choice([True, False])}),
            ]
        else:
            modifications = []
        
        # Apply 1-3 random modifications
        num_changes = random.randint(1, min(3, len(modifications)))
        for modification in random.sample(modifications, num_changes):
            modification(modified)
        
        return modified
    
    def generate_insert_event(self, object_type: str) -> Dict[str, Any]:
        """
        Generate INSERT CDC event.
        
        Args:
            object_type: Salesforce object type (Account, Contact, etc.)
            
        Returns:
            CDC event dictionary
        """
        # Generate new record
        if object_type == 'Account':
            records = self.data_generator.generate_accounts(count=1)
        elif object_type == 'Contact':
            account_ids = list(self._existing_records['Account'].keys())
            records = self.data_generator.generate_contacts(
                count=1,
                account_ids=account_ids if account_ids else None
            )
        elif object_type == 'Opportunity':
            account_ids = list(self._existing_records['Account'].keys())
            records = self.data_generator.generate_opportunities(
                count=1,
                account_ids=account_ids if account_ids else None
            )
        elif object_type == 'Case':
            account_ids = list(self._existing_records['Account'].keys())
            contact_ids = list(self._existing_records['Contact'].keys())
            records = self.data_generator.generate_cases(
                count=1,
                account_ids=account_ids if account_ids else None,
                contact_ids=contact_ids if contact_ids else None
            )
        else:
            raise ValueError(f"Unsupported object type: {object_type}")
        
        record = records[0]
        record_id = record['id']
        
        # Store for future updates
        self._existing_records[object_type][record_id] = record.copy()
        
        # Create CDC event
        event = {
            'event_id': self._generate_cdc_event_id(),
            'event_type': CDCEventType.INSERT.value,
            'object_type': object_type,
            'record_id': record_id,
            'event_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'changed_fields': [],
            'before': None,
            'after': record,
            'source': 'salesforce_cdc'
        }
        
        return event
    
    def generate_update_event(self, object_type: str, record_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate UPDATE CDC event.
        
        Args:
            object_type: Salesforce object type
            record_id: Optional specific record ID to update
            
        Returns:
            CDC event dictionary
        """
        # Get existing record
        if not self._existing_records[object_type]:
            # No existing records, generate INSERT instead
            return self.generate_insert_event(object_type)
        
        if record_id and record_id in self._existing_records[object_type]:
            before_record = self._existing_records[object_type][record_id]
        else:
            # Pick random existing record
            record_id = random.choice(list(self._existing_records[object_type].keys()))
            before_record = self._existing_records[object_type][record_id]
        
        # Modify record
        after_record = self._modify_record_for_update(before_record.copy(), object_type)
        
        # Update stored record
        self._existing_records[object_type][record_id] = after_record.copy()
        
        # Identify changed fields
        changed_fields = self._get_changed_fields(before_record, after_record)
        
        # Create CDC event
        event = {
            'event_id': self._generate_cdc_event_id(),
            'event_type': CDCEventType.UPDATE.value,
            'object_type': object_type,
            'record_id': record_id,
            'event_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'changed_fields': changed_fields,
            'before': before_record,
            'after': after_record,
            'source': 'salesforce_cdc'
        }
        
        return event
    
    def generate_delete_event(self, object_type: str, record_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate DELETE CDC event.
        
        Args:
            object_type: Salesforce object type
            record_id: Optional specific record ID to delete
            
        Returns:
            CDC event dictionary
        """
        # Get existing record
        if not self._existing_records[object_type]:
            # No existing records, cannot delete
            return self.generate_insert_event(object_type)
        
        if record_id and record_id in self._existing_records[object_type]:
            before_record = self._existing_records[object_type][record_id]
        else:
            # Pick random existing record
            record_id = random.choice(list(self._existing_records[object_type].keys()))
            before_record = self._existing_records[object_type][record_id]
        
        # Remove from stored records
        del self._existing_records[object_type][record_id]
        
        # Create CDC event
        event = {
            'event_id': self._generate_cdc_event_id(),
            'event_type': CDCEventType.DELETE.value,
            'object_type': object_type,
            'record_id': record_id,
            'event_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'changed_fields': [],
            'before': before_record,
            'after': None,
            'source': 'salesforce_cdc'
        }
        
        return event
    
    def generate_cdc_events(
        self,
        object_type: str,
        event_count: int = 100,
        event_distribution: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate multiple CDC events with specified distribution.
        
        Args:
            object_type: Salesforce object type
            event_count: Number of events to generate
            event_distribution: Distribution of event types (default: 50% INSERT, 40% UPDATE, 10% DELETE)
            
        Returns:
            List of CDC events
        """
        if event_distribution is None:
            event_distribution = {
                'INSERT': 0.5,
                'UPDATE': 0.4,
                'DELETE': 0.1
            }
        
        # Validate distribution
        total = sum(event_distribution.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Event distribution must sum to 1.0, got {total}")
        
        events = []
        
        # Calculate event counts
        insert_count = int(event_count * event_distribution.get('INSERT', 0))
        update_count = int(event_count * event_distribution.get('UPDATE', 0))
        delete_count = int(event_count * event_distribution.get('DELETE', 0))
        
        # Adjust for rounding
        remaining = event_count - (insert_count + update_count + delete_count)
        insert_count += remaining
        
        # Generate events in random order
        event_types = (
            [CDCEventType.INSERT] * insert_count +
            [CDCEventType.UPDATE] * update_count +
            [CDCEventType.DELETE] * delete_count
        )
        random.shuffle(event_types)
        
        for event_type in event_types:
            if event_type == CDCEventType.INSERT:
                event = self.generate_insert_event(object_type)
            elif event_type == CDCEventType.UPDATE:
                event = self.generate_update_event(object_type)
            else:  # DELETE
                event = self.generate_delete_event(object_type)
            
            events.append(event)
        
        return events
    
    def preload_existing_records(self, object_type: str, count: int = 100):
        """
        Preload existing records to enable UPDATE and DELETE events.
        
        Args:
            object_type: Salesforce object type
            count: Number of records to preload
        """
        # Generate initial records
        for _ in range(count):
            event = self.generate_insert_event(object_type)
            # Event already stores record in _existing_records
    
    def get_existing_record_count(self, object_type: str) -> int:
        """
        Get count of existing records for an object type.
        
        Args:
            object_type: Salesforce object type
            
        Returns:
            Number of existing records
        """
        return len(self._existing_records.get(object_type, {}))
    
    def clear_existing_records(self, object_type: Optional[str] = None):
        """
        Clear stored existing records.
        
        Args:
            object_type: Optional object type to clear (clears all if None)
        """
        if object_type:
            self._existing_records[object_type] = {}
        else:
            for obj_type in self._existing_records:
                self._existing_records[obj_type] = {}


def main():
    """Main function for standalone execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Salesforce CDC events')
    parser.add_argument('--object-type', required=True, choices=['Account', 'Contact', 'Opportunity', 'Case'],
                       help='Salesforce object type')
    parser.add_argument('--count', type=int, default=100, help='Number of events to generate')
    parser.add_argument('--preload', type=int, default=50, help='Number of records to preload')
    parser.add_argument('--output', default='cdc_events.json', help='Output JSON file')
    
    args = parser.parse_args()
    
    simulator = CDCEventSimulator()
    
    # Preload existing records
    print(f"Preloading {args.preload} {args.object_type} records...")
    simulator.preload_existing_records(args.object_type, args.preload)
    
    # Generate CDC events
    print(f"Generating {args.count} CDC events for {args.object_type}...")
    events = simulator.generate_cdc_events(args.object_type, args.count)
    
    # Save to file
    with open(args.output, 'w') as f:
        json.dump(events, f, indent=2, default=str)
    
    print(f"CDC events saved to {args.output}")
    
    # Print statistics
    event_types = {}
    for event in events:
        event_type = event['event_type']
        event_types[event_type] = event_types.get(event_type, 0) + 1
    
    print("\nEvent distribution:")
    for event_type, count in event_types.items():
        print(f"  {event_type}: {count} ({count/len(events)*100:.1f}%)")


if __name__ == '__main__':
    main()
