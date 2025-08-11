#!/usr/bin/env python3
"""
mongodb_manager.py
MongoDB connection and operations manager with enhanced error handling
"""

import pymongo
from datetime import datetime, timezone, timedelta
from config import MONGODB_URI, DATABASE_NAME, CLIENTS_COLLECTION, TASKS_COLLECTION, CLIENT_LOGS_COLLECTION
import traceback

class MongoDBManager:
    """MongoDB connection and operations manager with streaming + aggregated storage"""
    
    def __init__(self, logger):
        self.logger = logger
        self.client = None
        self.db = None
        self.clients_collection = None
        self.tasks_collection = None
        self.logs_collection = None
        self.aggregated_logs_collection = None  # New collection for final aggregated output
        self.connected = False
        
        # In-memory buffer for aggregating chunks per task
        self._task_output_buffer = {}  # task_id -> {'chunks': [], 'sequence': int}
        
        self._connect()
        self._setup_indexes()
    
    def _connect(self):
        """Establish MongoDB connection"""
        try:
            self.client = pymongo.MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test the connection
            self.client.admin.command('ping')
            
            self.db = self.client[DATABASE_NAME]
            self.clients_collection = self.db[CLIENTS_COLLECTION]
            self.tasks_collection = self.db[TASKS_COLLECTION]
            self.logs_collection = self.db[CLIENT_LOGS_COLLECTION]
            self.aggregated_logs_collection = self.db['aggregated_task_output']  # New collection
            
            self.connected = True
            self.logger.info("Connected to MongoDB at %s", MONGODB_URI)
            
        except Exception as e:
            self.logger.error("Failed to connect to MongoDB: %s\n%s", str(e), traceback.format_exc())
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
            
            # Index for streaming logs collection (for real-time viewing)
            self.logs_collection.create_index([("client_id", 1), ("timestamp", -1)])
            self.logs_collection.create_index([("task_id", 1), ("sequence", 1)])
            self.logs_collection.create_index("timestamp")
            
            # Index for aggregated logs collection (for final output viewing)
            self.aggregated_logs_collection.create_index("task_id", unique=True)
            self.aggregated_logs_collection.create_index([("client_id", 1), ("completed_at", -1)])
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.error("Failed to create MongoDB indexes: %s\n%s", str(e), traceback.format_exc())
    
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
            self.logger.error("Failed to upsert client %s: %s\n%s", client_id, str(e), traceback.format_exc())
            return False
    
    def get_all_clients(self):
        """Get all clients from MongoDB"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.clients_collection.find().sort('last_seen', -1)
            clients = []
            for doc in cursor:
                try:
                    client = {
                        'identity_str': doc['client_id'],
                        'hostname': doc.get('hostname'),
                        'last_seen': doc['last_seen'].isoformat() if isinstance(doc['last_seen'], datetime) else doc['last_seen'],
                        'created_at': doc.get('created_at', doc['last_seen']).isoformat() if isinstance(doc.get('created_at', doc['last_seen']), datetime) else doc.get('created_at', doc['last_seen'])
                    }
                    clients.append(client)
                except Exception as e:
                    self.logger.warning("Error processing client document: %s", str(e))
                    continue
            return clients
            
        except Exception as e:
            self.logger.error("Failed to get clients from MongoDB: %s\n%s", str(e), traceback.format_exc())
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
            self.logger.error("Failed to get client %s: %s\n%s", client_id, str(e), traceback.format_exc())
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
            
            # Initialize output buffer for this task
            self._task_output_buffer[task_id] = {
                'chunks': [],
                'sequence': 0,
                'client_id': target_client_id,
                'first_chunk_time': None
            }
            
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to insert task %s: %s\n%s", task_id, str(e), traceback.format_exc())
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
            
            # If task is completed, create aggregated output
            if status in ['completed', 'failed']:
                self._create_aggregated_output(task_id, status, kwargs.get('exit_code'))
            
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to update task %s: %s\n%s", task_id, str(e), traceback.format_exc())
            return False
    
    def get_all_tasks(self, limit=100):
        """Get all tasks from MongoDB (latest first)"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.tasks_collection.find().sort('created_at', -1).limit(limit)
            tasks = []
            for doc in cursor:
                try:
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
                except Exception as e:
                    self.logger.warning("Error processing task document: %s", str(e))
                    continue
            return tasks
            
        except Exception as e:
            self.logger.error("Failed to get tasks from MongoDB: %s\n%s", str(e), traceback.format_exc())
            return []
    
    def get_task(self, task_id):
        """Get specific task by ID"""
        if not self.is_connected():
            return None
            
        try:
            return self.tasks_collection.find_one({'id': task_id})
        except Exception as e:
            self.logger.error("Failed to get task %s: %s\n%s", task_id, str(e), traceback.format_exc())
            return None
    
    def log_client_output(self, client_id, task_id, msg_id, output, timestamp=None):
        """Log client output - both streaming and buffered for aggregation"""
        if not self.is_connected():
            return False
            
        try:
            # Parse timestamp
            if isinstance(timestamp, str):
                try:
                    ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    ts = datetime.now(timezone.utc)
            else:
                ts = timestamp or datetime.now(timezone.utc)
            
            # Get sequence number for this task
            if task_id not in self._task_output_buffer:
                self._task_output_buffer[task_id] = {
                    'chunks': [],
                    'sequence': 0,
                    'client_id': client_id,
                    'first_chunk_time': ts
                }
            
            buffer_info = self._task_output_buffer[task_id]
            sequence = buffer_info['sequence']
            buffer_info['sequence'] += 1
            
            # Store streaming chunk for real-time viewing
            streaming_doc = {
                'client_id': client_id,
                'task_id': task_id,
                'msg_id': msg_id,
                'output': output,
                'sequence': sequence,
                'timestamp': ts,
                'log_type': 'output_stream'
            }
            
            result = self.logs_collection.insert_one(streaming_doc)
            
            # Add to buffer for aggregation
            buffer_info['chunks'].append({
                'output': output,
                'sequence': sequence,
                'timestamp': ts,
                'msg_id': msg_id
            })
            
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to log client output: %s\n%s", str(e), traceback.format_exc())
            return False
    
    def _create_aggregated_output(self, task_id, status, exit_code):
        """Create aggregated output when task completes"""
        if task_id not in self._task_output_buffer:
            return False
            
        try:
            buffer_info = self._task_output_buffer[task_id]
            
            # Sort chunks by sequence to ensure correct order
            sorted_chunks = sorted(buffer_info['chunks'], key=lambda x: x['sequence'])
            
            # Combine all output chunks
            combined_output = ''.join(chunk['output'] for chunk in sorted_chunks)
            
            # Create aggregated document
            aggregated_doc = {
                'task_id': task_id,
                'client_id': buffer_info['client_id'],
                'combined_output': combined_output,
                'total_chunks': len(sorted_chunks),
                'first_chunk_time': buffer_info['first_chunk_time'],
                'completed_at': datetime.now(timezone.utc),
                'exit_code': exit_code,
                'status': status,
                'output_size': len(combined_output),
                'log_type': 'output_aggregated'
            }
            
            # Store aggregated output
            result = self.aggregated_logs_collection.insert_one(aggregated_doc)
            
            # Clean up buffer
            del self._task_output_buffer[task_id]
            
            self.logger.info("Created aggregated output for task %s (%d chunks, %d bytes)", 
                           task_id, len(sorted_chunks), len(combined_output))
            
            return result.acknowledged
            
        except Exception as e:
            self.logger.error("Failed to create aggregated output for task %s: %s\n%s", task_id, str(e), traceback.format_exc())
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
            self.logger.error("Failed to log client event: %s\n%s", str(e), traceback.format_exc())
            return False
    
    def get_client_logs(self, client_id, limit=100):
        """Get streaming logs for a specific client (for real-time viewing)"""
        if not self.is_connected():
            self.logger.warning("MongoDB not connected, returning empty logs for client %s", client_id)
            return []
            
        try:
            self.logger.info("Querying logs for client_id: %s, limit: %d", client_id, limit)
            
            # Query with proper error handling
            cursor = self.logs_collection.find(
                {'client_id': client_id}
            ).sort('timestamp', -1).limit(limit)
            
            logs = []
            for doc in cursor:
                try:
                    # Remove MongoDB ObjectId and handle datetime objects
                    log_entry = {
                        'client_id': doc.get('client_id'),
                        'task_id': doc.get('task_id'),
                        'msg_id': doc.get('msg_id'),
                        'output': doc.get('output', ''),
                        'event_type': doc.get('event_type'),
                        'details': doc.get('details'),
                        'timestamp': doc.get('timestamp').isoformat() if isinstance(doc.get('timestamp'), datetime) else str(doc.get('timestamp')),
                        'log_type': doc.get('log_type'),
                        'sequence': doc.get('sequence')
                    }
                    # Remove None values
                    log_entry = {k: v for k, v in log_entry.items() if v is not None}
                    logs.append(log_entry)
                except Exception as e:
                    self.logger.warning("Error processing log document for client %s: %s", client_id, str(e))
                    continue
            
            self.logger.info("Successfully retrieved %d logs for client %s", len(logs), client_id)
            return logs
            
        except Exception as e:
            self.logger.error("Failed to get logs for client %s: %s\n%s", client_id, str(e), traceback.format_exc())
            return []
    
    def get_task_streaming_logs(self, task_id, limit=1000):
        """Get streaming output chunks for a specific task (for live viewing)"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.logs_collection.find(
                {
                    'task_id': task_id,
                    'log_type': 'output_stream'
                }
            ).sort('sequence', 1).limit(limit)
            
            logs = []
            for doc in cursor:
                try:
                    log_entry = {
                        'task_id': doc.get('task_id'),
                        'client_id': doc.get('client_id'),
                        'msg_id': doc.get('msg_id'),
                        'output': doc.get('output', ''),
                        'sequence': doc.get('sequence'),
                        'timestamp': doc.get('timestamp').isoformat() if isinstance(doc.get('timestamp'), datetime) else str(doc.get('timestamp'))
                    }
                    logs.append(log_entry)
                except Exception as e:
                    self.logger.warning("Error processing streaming log for task %s: %s", task_id, str(e))
                    continue
            
            return logs
            
        except Exception as e:
            self.logger.error("Failed to get streaming logs for task %s: %s\n%s", task_id, str(e), traceback.format_exc())
            return []
    
    def get_task_aggregated_output(self, task_id):
        """Get final aggregated output for a completed task"""
        if not self.is_connected():
            return None
            
        try:
            doc = self.aggregated_logs_collection.find_one({'task_id': task_id})
            if doc:
                # Clean up the document for JSON serialization
                return {
                    'task_id': doc.get('task_id'),
                    'client_id': doc.get('client_id'),
                    'combined_output': doc.get('combined_output', ''),
                    'total_chunks': doc.get('total_chunks', 0),
                    'output_size': doc.get('output_size', 0),
                    'completed_at': doc.get('completed_at').isoformat() if isinstance(doc.get('completed_at'), datetime) else str(doc.get('completed_at')),
                    'exit_code': doc.get('exit_code'),
                    'status': doc.get('status')
                }
            return None
            
        except Exception as e:
            self.logger.error("Failed to get aggregated output for task %s: %s\n%s", task_id, str(e), traceback.format_exc())
            return None
    
    def get_client_aggregated_outputs(self, client_id, limit=50):
        """Get aggregated outputs for a client's completed tasks"""
        if not self.is_connected():
            return []
            
        try:
            cursor = self.aggregated_logs_collection.find(
                {'client_id': client_id}
            ).sort('completed_at', -1).limit(limit)
            
            outputs = []
            for doc in cursor:
                try:
                    output = {
                        'task_id': doc.get('task_id'),
                        'client_id': doc.get('client_id'),
                        'combined_output': doc.get('combined_output', ''),
                        'total_chunks': doc.get('total_chunks', 0),
                        'output_size': doc.get('output_size', 0),
                        'completed_at': doc.get('completed_at').isoformat() if isinstance(doc.get('completed_at'), datetime) else str(doc.get('completed_at')),
                        'exit_code': doc.get('exit_code'),
                        'status': doc.get('status')
                    }
                    outputs.append(output)
                except Exception as e:
                    self.logger.warning("Error processing aggregated output for client %s: %s", client_id, str(e))
                    continue
            
            return outputs
            
        except Exception as e:
            self.logger.error("Failed to get aggregated outputs for client %s: %s\n%s", client_id, str(e), traceback.format_exc())
            return []
    
    def cleanup_old_streaming_logs(self, days_old=7):
        """Clean up old streaming logs (keep aggregated ones)"""
        if not self.is_connected():
            return False
            
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
            
            result = self.logs_collection.delete_many({
                'log_type': 'output_stream',
                'timestamp': {'$lt': cutoff_date}
            })
            
            self.logger.info("Cleaned up %d old streaming log entries", result.deleted_count)
            return True
            
        except Exception as e:
            self.logger.error("Failed to cleanup old streaming logs: %s\n%s", str(e), traceback.format_exc())
            return False
    
    def get_database_stats(self):
        """Get database statistics"""
        if not self.is_connected():
            return {}
            
        try:
            stats = {
                'clients_count': self.clients_collection.count_documents({}),
                'tasks_count': self.tasks_collection.count_documents({}),
                'streaming_logs_count': self.logs_collection.count_documents({'log_type': 'output_stream'}),
                'aggregated_logs_count': self.aggregated_logs_collection.count_documents({}),
                'events_count': self.logs_collection.count_documents({'log_type': 'event'})
            }
            return stats
            
        except Exception as e:
            self.logger.error("Failed to get database stats: %s\n%s", str(e), traceback.format_exc())
            return {}
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.connected = False
            self.logger.info("MongoDB connection closed")