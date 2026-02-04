# ===== EVENT ORDERING LOGGER (ADD THIS ENTIRE CLASS) =====
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

class EventOrderingLogger:
    """
    Logs all workflow events with sequence numbers and causal dependencies.
    Each event records what must happen before it (depends_on).
    Every server writes same events to local file for state replication verification.
    """
    
    def __init__(self, server_id=None):
        self.server_id = server_id if server_id else "unknown"
        # Single JSONL file: one event per line
        # self.event_file = Path("logs") / f"events_{self.server_id[:8]}.jsonl"
        
        self.event_file = Path("logs") / f"events.jsonl"
        if self.event_file.exists():
            self.event_file.unlink()
        
        self.seq_counter = 0
        self.load_existing_events()
        # Track workflow seq numbers for dependency building
        self.workflow_last_seq = {}  # workflow_id -> last_seq_for_that_workflow
        
    def load_existing_events(self):
        """Load existing events from file to continue seq numbering"""
        if self.event_file.exists():
            try:
                with open(self.event_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            event = json.loads(line)
                            self.seq_counter = max(self.seq_counter, event.get('seq', 0))
            except Exception as e:
                print(f"Error loading events: {e}")
    
    def log_event(self, sequence_counter, workflow_id, event_type, description, 
                  depends_on=None, data=None, event_sub_type=None):
        """
        Log a single event with causal dependency tracking
        
        Args:
            workflow_id: e.g., "wf_car_1_1"
            event_type: "request_received", "resource_found", "workflow_started", 
                       "command_sent", "status_change", "workflow_completed"
            description: Human readable description
            depends_on: List of seq numbers this depends on (prerequisites)
            data: Dict with additional context (resource_id, status, etc.)
        """
        self.seq_counter = sequence_counter
        
        # Auto-calculate depends_on from last event in this workflow
        if depends_on is None and workflow_id in self.workflow_last_seq:
            depends_on = [self.workflow_last_seq[workflow_id]]
        elif depends_on is None:
            depends_on = []
        
        event = {
            "seq": self.seq_counter,
            "workflow_id": workflow_id,
            "event_type": event_type,
            "event_sub_type": event_sub_type,
            "description": description,
            "depends_on": depends_on,
            "timestamp": datetime.now().isoformat(timespec='milliseconds'),
            "server_id": self.server_id,
            "data": data or {}
        }
        
        # Track this as last event in workflow
        self.workflow_last_seq[workflow_id] = self.seq_counter
        
        # Write to file (append mode, one event per line)
        self._write_to_file(event)
        
        return event
    
    def _write_to_file(self, event):
        """Append event as JSON line"""
        try:
            with open(self.event_file, 'a') as f:
                json.dump(event, f)
                f.write('\n')
        except Exception as e:
            print(f"Error writing event: {e}")
    
    def verify_causal_ordering(self):
        """Verify all dependencies are satisfied"""
        try:
            events_by_seq = {}
            if self.event_file.exists():
                with open(self.event_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            event = json.loads(line)
                            events_by_seq[event['seq']] = event
            
            violations = []
            for seq, event in events_by_seq.items():
                for dep_seq in event.get('depends_on', []):
                    if dep_seq not in events_by_seq:
                        violations.append(f"Event #{seq}: missing dependency #{dep_seq}")
                    elif dep_seq >= seq:
                        violations.append(f"Event #{seq}: depends on future event #{dep_seq}")
            
            return {
                "total_events": len(events_by_seq),
                "valid": len(violations) == 0,
                "violations": violations
            }
        except Exception as e:
            return {"error": str(e)}

# ===== END EVENT ORDERING LOGGER =====