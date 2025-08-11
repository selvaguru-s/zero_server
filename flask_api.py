#!/usr/bin/env python3
"""
flask_api.py
Flask REST API for the ZMQ server with Firebase authentication and streaming support
"""

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from utils import sanitize_for_json
from firebase_auth import FirebaseAuthManager
import functools
import json
import time

class FlaskAPI:
    """Flask API wrapper for the ZMQ server with Firebase auth and streaming support"""
    
    def __init__(self, data_store, zmq_server, logger, firebase_service_account_path):
        self.data_store = data_store
        self.zmq_server = zmq_server
        self.logger = logger
        
        # Initialize Firebase auth manager
        self.auth_manager = FirebaseAuthManager(firebase_service_account_path, logger)
        
        # Set up Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Register routes
        self._register_routes()
    
    def _register_routes(self):
        """Register all Flask routes"""
        # Authentication routes
        self.app.route('/api/auth/login', methods=['POST'])(self.api_auth_login)
        self.app.route('/api/auth/logout', methods=['POST'])(self.api_auth_logout)
        self.app.route('/api/auth/verify', methods=['POST'])(self.api_auth_verify)
        
        # Protected routes (require API key)
        self.app.route('/api/clients')(self.require_auth(self.api_clients))
        self.app.route('/api/tasks')(self.require_auth(self.api_tasks))
        self.app.route('/api/send', methods=['POST'])(self.require_auth(self.api_send))
        
        # Logging routes with different views
        self.app.route('/api/client/<client_id>/logs')(self.require_auth(self.api_client_logs))
        self.app.route('/api/client/<client_id>/completed-tasks')(self.require_auth(self.api_client_completed_tasks))
        
        # Task-specific output routes
        self.app.route('/api/task/<task_id>/output/stream')(self.require_auth(self.api_task_streaming_output))
        self.app.route('/api/task/<task_id>/output/aggregated')(self.require_auth(self.api_task_aggregated_output))
        self.app.route('/api/task/<task_id>/output/live')(self.require_auth(self.api_task_live_stream))
        
        # Public route
        self.app.route('/api/status')(self.api_status)
    
    def require_auth(self, f):
        """Decorator to require API key authentication"""
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            # Get API key from Authorization header
            auth_header = request.headers.get('Authorization')
            if not auth_header or not auth_header.startswith('Bearer '):
                return jsonify({"error": "Missing or invalid Authorization header"}), 401
            
            api_key = auth_header.replace('Bearer ', '')
            
            # Validate API key
            if not self.auth_manager.validate_api_key(api_key):
                return jsonify({"error": "Invalid API key"}), 401
            
            # Add user info to request context
            user_info = self.auth_manager.get_user_by_api_key(api_key)
            request.user_info = user_info
            
            return f(*args, **kwargs)
        return decorated_function
    
    def api_auth_login(self):
        """POST /api/auth/login - Authenticate with Firebase ID token"""
        try:
            data = request.get_json(force=True)
            id_token = data.get('idToken')
            
            if not id_token:
                return jsonify({"error": "Missing idToken"}), 400
            
            # Verify Firebase token
            user_info = self.auth_manager.verify_firebase_token(id_token)
            if not user_info:
                return jsonify({"error": "Invalid Firebase token"}), 401
            
            # Create or get API key
            api_key = self.auth_manager.create_or_get_api_key(user_info)
            if not api_key:
                return jsonify({"error": "Failed to generate API key"}), 500
            
            return jsonify({
                "api_key": api_key,
                "user": {
                    "id": user_info['user_id'],
                    "email": user_info.get('email'),
                    "name": user_info.get('name'),
                    "verified": user_info.get('verified', False)
                }
            })
            
        except Exception as e:
            self.logger.error("Auth login error: %s", e)
            return jsonify({"error": "Authentication failed"}), 500
    
    def api_auth_logout(self):
        """POST /api/auth/logout - Logout (client-side only for now)"""
        return jsonify({"message": "Logged out successfully"})
    
    def api_auth_verify(self):
        """POST /api/auth/verify - Verify API key validity"""
        try:
            data = request.get_json(force=True)
            api_key = data.get('api_key')
            
            if not api_key:
                return jsonify({"error": "Missing api_key"}), 400
            
            user_info = self.auth_manager.get_user_by_api_key(api_key)
            if not user_info:
                return jsonify({"valid": False}), 200
            
            return jsonify({
                "valid": True,
                "user": {
                    "id": user_info['user_id'],
                    "email": user_info.get('email'),
                    "name": user_info.get('name'),
                    "verified": user_info.get('verified', False)
                }
            })
            
        except Exception as e:
            self.logger.error("Auth verify error: %s", e)
            return jsonify({"error": "Verification failed"}), 500
    
    def api_clients(self):
        """GET /api/clients - List all connected clients"""
        clients = self.data_store.get_all_clients()
        return jsonify(sanitize_for_json(clients))
    
    def api_tasks(self):
        """GET /api/tasks - List all tasks"""
        tasks = self.data_store.get_all_tasks()
        return jsonify(sanitize_for_json(tasks))
    
    def api_send(self):
        """POST /api/send - Send a task to a client"""
        data = request.get_json(force=True)
        client_id = data.get('client_id')
        mode = data.get('mode')
        payload = data.get('payload')
        
        if not client_id or not payload:
            return jsonify({"error": "Missing required fields"}), 400
        
        # Find target client
        target_identity = self.data_store.get_client_by_id(client_id)
        if not target_identity:
            return jsonify({"error": "Client not found"}), 404
        
        # Send task
        task_id = self.zmq_server.send_task_to_client(target_identity, mode, payload)
        return jsonify({"task_id": task_id, "status": "queued"})
    
    def api_client_logs(self, client_id):
        """GET /api/client/<client_id>/logs - Get streaming logs for a specific client"""
        limit = request.args.get('limit', 100, type=int)
        logs = self.data_store.get_client_logs(client_id, limit)
        return jsonify(sanitize_for_json(logs))
    
    def api_client_completed_tasks(self, client_id):
        """GET /api/client/<client_id>/completed-tasks - Get aggregated outputs for completed tasks"""
        limit = request.args.get('limit', 50, type=int)
        
        if hasattr(self.data_store.mongodb, 'get_client_aggregated_outputs'):
            outputs = self.data_store.mongodb.get_client_aggregated_outputs(client_id, limit)
            return jsonify(sanitize_for_json(outputs))
        else:
            return jsonify({"error": "Aggregated outputs not available"}), 503
    
    def api_task_streaming_output(self, task_id):
        """GET /api/task/<task_id>/output/stream - Get streaming chunks for a task"""
        limit = request.args.get('limit', 1000, type=int)
        
        if hasattr(self.data_store.mongodb, 'get_task_streaming_logs'):
            chunks = self.data_store.mongodb.get_task_streaming_logs(task_id, limit)
            return jsonify(sanitize_for_json(chunks))
        else:
            return jsonify({"error": "Streaming logs not available"}), 503
    
    def api_task_aggregated_output(self, task_id):
        """GET /api/task/<task_id>/output/aggregated - Get final combined output for a task"""
        if hasattr(self.data_store.mongodb, 'get_task_aggregated_output'):
            output = self.data_store.mongodb.get_task_aggregated_output(task_id)
            if output:
                return jsonify(sanitize_for_json(output))
            else:
                return jsonify({"error": "Aggregated output not found"}), 404
        else:
            return jsonify({"error": "Aggregated output not available"}), 503
    
    def api_task_live_stream(self, task_id):
        """GET /api/task/<task_id>/output/live - Server-Sent Events for live output streaming"""
        def generate():
            """Generator for Server-Sent Events"""
            last_sequence = 0
            
            while True:
                try:
                    # Get new chunks since last sequence
                    if hasattr(self.data_store.mongodb, 'get_task_streaming_logs'):
                        chunks = self.data_store.mongodb.get_task_streaming_logs(task_id, 1000)
                        
                        # Filter chunks newer than last_sequence
                        new_chunks = [chunk for chunk in chunks if chunk.get('sequence', 0) > last_sequence]
                        
                        for chunk in sorted(new_chunks, key=lambda x: x.get('sequence', 0)):
                            data = {
                                'sequence': chunk.get('sequence'),
                                'output': chunk.get('output'),
                                'timestamp': chunk.get('timestamp').isoformat() if hasattr(chunk.get('timestamp'), 'isoformat') else str(chunk.get('timestamp')),
                                'msg_id': chunk.get('msg_id')
                            }
                            yield f"data: {json.dumps(data)}\n\n"
                            last_sequence = chunk.get('sequence', last_sequence)
                        
                        # Check if task is completed
                        task = self.data_store.get_task(task_id)
                        if task and task.get('status') in ['completed', 'failed']:
                            # Send completion event
                            completion_data = {
                                'type': 'task_completed',
                                'status': task['status'],
                                'exit_code': task.get('exit_code')
                            }
                            yield f"data: {json.dumps(completion_data)}\n\n"
                            break
                    
                    time.sleep(0.5)  # Poll every 500ms
                    
                except Exception as e:
                    self.logger.error("Error in live stream for task %s: %s", task_id, e)
                    error_data = {'type': 'error', 'message': str(e)}
                    yield f"data: {json.dumps(error_data)}\n\n"
                    break
        
        return Response(
            generate(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            }
        )
    
    def api_status(self):
        """GET /api/status - Get system status including MongoDB connection"""
        base_status = {
            'mongodb_connected': self.data_store.mongodb.is_connected(),
            'total_clients': len(self.data_store.get_all_clients()),
            'total_tasks': len(self.data_store.get_all_tasks()),
            'auth_enabled': True
        }
        
        # Add database statistics if available
        if hasattr(self.data_store.mongodb, 'get_database_stats'):
            db_stats = self.data_store.mongodb.get_database_stats()
            base_status.update(db_stats)
        
        return jsonify(base_status)
    
    def run(self, host, port):
        """Start the Flask server"""
        self.logger.info("Starting Flask web UI with Firebase auth on %s:%d", host, port)
        self.app.run(host=host, port=port, threaded=True)
    
    def close(self):
        """Clean up resources"""
        if self.auth_manager:
            self.auth_manager.close()