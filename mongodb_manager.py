#!/usr/bin/env python3
"""
mongodb_manager.py
MongoDB connection and operations manager
"""

import pymongo
from datetime import datetime, timezone
from config import MONGODB_URI, DATABASE_NAME, CLIENTS_COLLECTION, TASKS_COLLECTION, CLIENT_LOGS_COLLECTION

class MongoDBManager:
    """MongoDB connection and operations manager"""
    
    def __init__(self, logger):
        self.logger = logger
        self.client = None
        self.db = None
        self.clients_collection = None
        self.tasks_collection = None
        self.logs_collection = None
        self.connected = False
        
        self._connect()
        self._setup_indexes()
    
    def _connect(self):
        """Establish MongoDB connection"""
        try:
            self.client = pymongo.MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test the connection
            self.client.admin.command('ping')
            
            self.db = self.client[DATABASE_NAME]
            self.clients_collection = self.db[CLIENTS_COLLECTION]
            self.tasks_collection = self.db[TASKS_COLLECTION]
            self.logs_collection = self.db[CLIENT_LOGS_COLLECTION]
            
            self.connected = True
            self.logger.info("Connected to MongoDB at %s", MONGODB_URI)
            
        except Exception as e:
            self.logger.error("Failed to connect to MongoDB: %s", e)
            self.connected = False
    
    def _setup_indexes(self):
        """Create necessary indexes for better performance"""
        if not self.connected:
            return
            
        try:
            # Index for clients collection
            self.clients_collection.create_index("client_id", unique=True)
            self.clients_collection.create_index("last_seen")
            
            # Index for tasks collection
            self.tasks_collection.create_index("id", unique=True)
            self.tasks_collection.create_index("target")
            self.tasks_collection.create_index("status")
            self.tasks_collection.create_index("created_at")
            
            # Index for logs collection
            self.logs_collection.create_index([("client_id", 1), ("timestamp", -1)])
            self.logs_collection.create_index("task_id")
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.error("Failed to create MongoDB indexes: %s", e)
    
    def is_connected(self):
        """Check if MongoDB connection is active"""
        if not self.connected:
            return False
            
        try:
            self.client.admin.command('ping')
            return True
        except Exception:
            self.connected = False
            return False
    
    def upsert_client(self, client_id, identity_bytes, hostname=None):
        """Insert or update client information"""
        if not self.is_connected():
            return False
            
        try:
            doc = {
                'client_id': client_id,
                'identity_hex': identity_bytes.hex(),
                'hostname': hostname,
                'last_seen': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }
            
            result = self.clients_collection.update_one(
                {'client_id': client_id},
                {'$set': doc, '$setOnInsert': {'created_at': datetime.now(timezone.utc)}},
                upsert=True
            )
            
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to upsert client %s: %s", client_id, e)
            return False
    
    def get_all_clients(self):
        """Get all clients from MongoDB"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.clients_collection.find().sort('last_seen', -1)
            clients = []
            for doc in cursor:
                # Convert MongoDB document to the expected format
                client = {
                    'identity_str': doc['client_id'],
                    'hostname': doc.get('hostname'),
                    'last_seen': doc['last_seen'].isoformat() if isinstance(doc['last_seen'], datetime) else doc['last_seen'],
                    'created_at': doc.get('created_at', doc['last_seen']).isoformat() if isinstance(doc.get('created_at', doc['last_seen']), datetime) else doc.get('created_at', doc['last_seen'])
                }
                clients.append(client)
            return clients
            
        except Exception as e:
            self.logger.error("Failed to get clients from MongoDB: %s", e)
            return []
    
    def get_client_by_id(self, client_id):
        """Get specific client by ID"""
        if not self.is_connected():
            return None
            
        try:
            doc = self.clients_collection.find_one({'client_id': client_id})
            if doc:
                return bytes.fromhex(doc['identity_hex'])
            return None
            
        except Exception as e:
            self.logger.error("Failed to get client %s: %s", client_id, e)
            return None
    
    def insert_task(self, task_id, target_client_id, mode, payload):
        """Insert a new task"""
        if not self.is_connected():
            return False
            
        try:
            doc = {
                'id': task_id,
                'target': target_client_id,
                'mode': mode,
                'payload': payload,
                'status': 'queued',
                'created_at': datetime.now(timezone.utc),
                'started_at': None,
                'completed_at': None,
                'exit_code': None
            }
            
            result = self.tasks_collection.insert_one(doc)
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to insert task %s: %s", task_id, e)
            return False
    
    def update_task_status(self, task_id, status, **kwargs):
        """Update task status and additional fields"""
        if not self.is_connected():
            return False
            
        try:
            update_doc = {'status': status, 'updated_at': datetime.now(timezone.utc)}
            
            # Handle datetime fields
            for key, value in kwargs.items():
                if key in ['started_at', 'completed_at'] and isinstance(value, str):
                    try:
                        update_doc[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except:
                        update_doc[key] = value
                else:
                    update_doc[key] = value
            
            result = self.tasks_collection.update_one(
                {'id': task_id},
                {'$set': update_doc}
            )
            
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to update task %s: %s", task_id, e)
            return False
    
    def get_all_tasks(self, limit=100):
        """Get all tasks from MongoDB (latest first)"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.tasks_collection.find().sort('created_at', -1).limit(limit)
            tasks = []
            for doc in cursor:
                task = {
                    'id': doc['id'],
                    'target': doc['target'],
                    'mode': doc.get('mode'),
                    'payload': doc['payload'],
                    'status': doc['status'],
                    'created_at': doc['created_at'].isoformat() if isinstance(doc['created_at'], datetime) else doc['created_at'],
                    'started_at': doc['started_at'].isoformat() if isinstance(doc['started_at'], datetime) and doc['started_at'] else doc['started_at'],
                    'completed_at': doc['completed_at'].isoformat() if isinstance(doc['completed_at'], datetime) and doc['completed_at'] else doc['completed_at'],
                    'exit_code': doc.get('exit_code')
                }
                tasks.append(task)
            return tasks
            
        except Exception as e:
            self.logger.error("Failed to get tasks from MongoDB: %s", e)
            return []
    
    def get_task(self, task_id):
        """Get specific task by ID"""
        if not self.is_connected():
            return None
            
        try:
            return self.tasks_collection.find_one({'id': task_id})
        except Exception as e:
            self.logger.error("Failed to get task %s: %s", task_id, e)
            return None
    
    def log_client_output(self, client_id, task_id, msg_id, output, timestamp=None):
        """Log client output to MongoDB"""
        if not self.is_connected():
            return False
            
        try:
            doc = {
                'client_id': client_id,
                'task_id': task_id,
                'msg_id': msg_id,
                'output': output,
                'timestamp': timestamp or datetime.now(timezone.utc),
                'log_type': 'output'
            }
            
            result = self.logs_collection.insert_one(doc)
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to log client output: %s", e)
            return False
    
    def log_client_event(self, client_id, event_type, details, task_id=None):
        """Log client events (task completion, etc.)"""
        if not self.is_connected():
            return False
            
        try:
            doc = {
                'client_id': client_id,
                'task_id': task_id,
                'event_type': event_type,
                'details': details,
                'timestamp': datetime.now(timezone.utc),
                'log_type': 'event'
            }
            
            result = self.logs_collection.insert_one(doc)
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to log client event: %s", e)
            return False
    
    def get_client_logs(self, client_id, limit=100):
        """Get logs for a specific client"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.logs_collection.find(
                {'client_id': client_id}
            ).sort('timestamp', -1).limit(limit)
            
            return list(cursor)
            
        except Exception as e:
            self.logger.error("Failed to get logs for client %s: %s", client_id, e)
            return []
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.connected = False
            self.logger.info("MongoDB connection closed")