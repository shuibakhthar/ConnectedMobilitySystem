# In config/settings.py - add this near other logging setup functions

import json
from pathlib import Path
from datetime import datetime

class EventOrderingLogger:
    """Logs all global events with sequence numbers for causal ordering verification"""
    
    def __init__(self, server_id=None):
        self.server_id = server_id
        self.event_file = Path("logs") / f"events_{server_id[:8]}.json" if server_id else Path("logs") / "events.json"
        self.events = []
        
    def log_event(self, seq, event_type, workflow_id, data=None, timestamp=None):
        """Log an event with sequence number"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
            
        event = {
            "seq": seq,
            "type": event_type,
            "workflow_id": workflow_id,
            "data": data or {},
            "timestamp": timestamp,
            "server_id": self.server_id
        }
        
        self.events.append(event)
        self._write_to_file(event)
        
    def _write_to_file(self, event):
        """Append event to JSON file"""
        try:
            # Read existing events
            if self.event_file.exists():
                with open(self.event_file, 'r') as f:
                    all_events = json.load(f)
            else:
                all_events = []
            
            # Append new event
            all_events.append(event)
            
            # Write back
            with open(self.event_file, 'w') as f:
                json.dump(all_events, f, indent=2)
        except Exception as e:
            print(f"Error writing event to file: {e}")
    
    def verify_causal_ordering(self):
        """Verify that all events are in correct sequence order"""
        try:
            with open(self.event_file, 'r') as f:
                events = json.load(f)
            
            # Group by workflow_id
            workflows = {}
            for event in events:
                wf_id = event['workflow_id']
                if wf_id not in workflows:
                    workflows[wf_id] = []
                workflows[wf_id].append(event)
            
            # Check ordering
            violations = []
            for wf_id, events_list in workflows.items():
                seqs = [e['seq'] for e in events_list]
                if seqs != sorted(seqs):
                    violations.append(f"Workflow {wf_id}: sequences out of order {seqs}")
            
            return {
                "total_events": len(events),
                "total_workflows": len(workflows),
                "violations": violations,
                "valid": len(violations) == 0
            }
        except Exception as e:
            return {"error": str(e)}