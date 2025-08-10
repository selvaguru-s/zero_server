#!/usr/bin/env python3
"""
data_models.py
Data structures and state management for the ZMQ server
"""

import threading
from collections import OrderedDict
from datetime import datetime, timezone
from mongodb_manager import MongoDBManager

class DataStore:
    """Thread-safe data storage for clients and tasks with MongoDB persistence"""
    
    def __init__(self, logger):
        # In-memory state (for fallback and quick access)
        self.clients = OrderedDict()   # identity (bytes) -> info
        self.clients_lock = threading.Lock()
        
        self.tasks = OrderedDict()
        self.tasks_lock = threading.Lock()
        
        # MongoDB manager
        self.mongodb = MongoDBManager(logger)
        self.logger = logger
    
    def add_or_update_client(self, identity, client_id, hostname=None):
        """Add or update client information"""
        with self.clients_lock:
            entry = self.clients.get(identity)
            is_new = entry is None
            
            if not entry:
                entry = {
                    'identity': identity,
                    'identity_str': client_id,
                    'hostname': hostname,
                    'last_seen': datetime.now(timezone.utc).isoformat()
                }
                self.clients[identity] = entry
            else:
                entry['last_seen'] = datetime.now(timezone.utc).isoformat()
                if hostname:
                    entry['hostname'] = hostname
            
            # Persist to MongoDB
            self.mongodb.upsert_client(client_id, identity, hostname)
            
            return is_new
    
    def get_client_by_id(self, client_id):
        """Find client identity by client_id"""
        # First check in-memory cache
        with self.clients_lock:
            for identity, info in self.clients.items():
                if info.get('identity_str') == client_id:
                    return identity
        
        # If not found in memory, check MongoDB
        identity_bytes = self.mongodb.get_client_by_id(client_id)
        if identity_bytes:
            # Add to in-memory cache for faster future access
            with self.clients_lock:
                if identity_bytes not in self.clients:
                    self.clients[identity_bytes] = {
                        'identity': identity_bytes,
                        'identity_str': client_id,
                        'hostname': None,
                        'last_seen': datetime.now(timezone.utc).isoformat()
                    }
            return identity_bytes
        
        return None
    
    def get_all_clients(self):
        """Get all clients (prioritizing MongoDB data)"""
        # Try to get from MongoDB first
        if self.mongodb.is_connected():
            return self.mongodb.get_all_clients()
        
        # Fallback to in-memory data
        with self.clients_lock:
            return list(self.clients.values())
    
    def add_task(self, task_id, target_identity, mode, payload):
        """Add a new task"""
        now = datetime.now(timezone.utc).isoformat()
        target_str = self._identity_to_str(target_identity)
        
        task_info = {
            'id': task_id,
            'target': target_str,
            'mode': mode,
            'payload': payload,
            'status': 'queued',
            'created_at': now,
            'started_at': None,
            'completed_at': None,
            'exit_code': None
        }
        
        # Store in memory
        with self.tasks_lock:
            self.tasks[task_id] = task_info
        
        # Persist to MongoDB
        self.mongodb.insert_task(task_id, target_str, mode, payload)
    
    def update_task_status(self, task_id, status, **kwargs):
        """Update task status and additional fields"""
        # Update in memory
        with self.tasks_lock:
            task = self.tasks.get(task_id)
            if task:
                task['status'] = status
                for key, value in kwargs.items():
                    task[key] = value
        
        # Update in MongoDB
        self.mongodb.update_task_status(task_id, status, **kwargs)
    
    def get_all_tasks(self):
        """Get all tasks (prioritizing MongoDB data)"""
        # Try to get from MongoDB first
        if self.mongodb.is_connected():
            return self.mongodb.get_all_tasks()
        
        # Fallback to in-memory data
        with self.tasks_lock:
            return list(self.tasks.values())[::-1]
    
    def get_task(self, task_id):
        """Get specific task"""
        # Try MongoDB first
        if self.mongodb.is_connected():
            return self.mongodb.get_task(task_id)
        
        # Fallback to in-memory
        with self.tasks_lock:
            return self.tasks.get(task_id)
    
    def log_client_output(self, client_id, task_id, msg_id, output, timestamp=None):
        """Log client output"""
        return self.mongodb.log_client_output(client_id, task_id, msg_id, output, timestamp)
    
    def log_client_event(self, client_id, event_type, details, task_id=None):
        """Log client events"""
        return self.mongodb.log_client_event(client_id, event_type, details, task_id)
    
    def get_client_logs(self, client_id, limit=100):
        """Get client logs"""
        return self.mongodb.get_client_logs(client_id, limit)
    
    def close(self):
        """Close connections"""
        if hasattr(self, 'mongodb'):
            self.mongodb.close()
    
    @staticmethod
    def _identity_to_str(b):
        """Convert identity bytes to safe string"""
        try:
            s = b.decode('utf-8')
        except Exception:
            s = b.hex()
        return "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in s])[:120]