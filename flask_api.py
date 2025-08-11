#!/usr/bin/env python3
"""
flask_api.py
Flask REST API for the ZMQ server with Firebase authentication and improved error handling
"""

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from utils import sanitize_for_json
import functools
import json
import time
import traceback

class FlaskAPI:
    """Flask API wrapper for the ZMQ server with Firebase auth and streaming support"""
    
    def __init__(self, data_store, zmq_server, logger, auth_manager):
        self.data_store = data_store
        self.zmq_server = zmq_server
        self.logger = logger
        
        # Use the provided auth manager instead of creating a new one
        self.auth_manager = auth_manager
        
        # Set up Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Register error handlers
        self._register_error_handlers()
        
        # Register routes
        self._register_routes()
    
    def _register_error_handlers(self):
        """Register global error handlers"""
        @self.app.errorhandler(500)
        def internal_server_error(error):
            self.logger.error("Internal server error: %s", str(error))
            return jsonify({"error": "Internal server error", "details": str(error)}), 500
        
        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({"error": "Not found"}), 404
        
        @self.app.errorhandler(Exception)
        def handle_exception(e):
            self.logger.error("Unhandled exception: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Internal server error", "details": str(e)}), 500
    
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
            try:
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
            except Exception as e:
                self.logger.error("Auth error in %s: %s\n%s", f.__name__, str(e), traceback.format_exc())
                return jsonify({"error": "Authentication failed", "details": str(e)}), 500
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
            self.logger.error("Auth login error: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Authentication failed", "details": str(e)}), 500
    
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
            self.logger.error("Auth verify error: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Verification failed", "details": str(e)}), 500
    
    def api_clients(self):
        """GET /api/clients - List all connected clients"""
        try:
            clients = self.data_store.get_all_clients()
            return jsonify(sanitize_for_json(clients))
        except Exception as e:
            self.logger.error("Error fetching clients: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch clients", "details": str(e)}), 500
    
    def api_tasks(self):
        """GET /api/tasks - List all tasks"""
        try:
            tasks = self.data_store.get_all_tasks()
            return jsonify(sanitize_for_json(tasks))
        except Exception as e:
            self.logger.error("Error fetching tasks: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch tasks", "details": str(e)}), 500
    
    def api_send(self):
        """POST /api/send - Send a task to a client"""
        try:
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
        except Exception as e:
            self.logger.error("Error sending task: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Failed to send task", "details": str(e)}), 500
    
    def api_client_logs(self, client_id):
        """GET /api/client/<client_id>/logs - Get streaming logs for a specific client"""
        try:
            self.logger.info("Fetching logs for client: %s", client_id)
            limit = request.args.get('limit', 100, type=int)
            
            # Validate limit
            if limit <= 0 or limit > 1000:
                limit = 100
            
            # Get logs from data store
            logs = self.data_store.get_client_logs(client_id, limit)
            
            # Ensure logs is a list
            if logs is None:
                logs = []
            
            self.logger.info("Retrieved %d logs for client %s", len(logs), client_id)
            
            # Sanitize logs for JSON serialization
            sanitized_logs = sanitize_for_json(logs)
            
            return jsonify(sanitized_logs)
            
        except Exception as e:
            self.logger.error("Error fetching logs for client %s: %s\n%s", client_id, str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch client logs", "details": str(e)}), 500
    
    def api_client_completed_tasks(self, client_id):
        """GET /api/client/<client_id>/completed-tasks - Get aggregated outputs for completed tasks"""
        try:
            limit = request.args.get('limit', 50, type=int)
            
            if hasattr(self.data_store.mongodb, 'get_client_aggregated_outputs'):
                outputs = self.data_store.mongodb.get_client_aggregated_outputs(client_id, limit)
                return jsonify(sanitize_for_json(outputs or []))
            else:
                return jsonify({"error": "Aggregated outputs not available"}), 503
        except Exception as e:
            self.logger.error("Error fetching completed tasks for client %s: %s\n%s", client_id, str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch completed tasks", "details": str(e)}), 500
    
    def api_task_streaming_output(self, task_id):
        """GET /api/task/<task_id>/output/stream - Get streaming chunks for a task"""
        try:
            limit = request.args.get('limit', 1000, type=int)
            
            if hasattr(self.data_store.mongodb, 'get_task_streaming_logs'):
                chunks = self.data_store.mongodb.get_task_streaming_logs(task_id, limit)
                return jsonify(sanitize_for_json(chunks or []))
            else:
                return jsonify({"error": "Streaming logs not available"}), 503
        except Exception as e:
            self.logger.error("Error fetching streaming output for task %s: %s\n%s", task_id, str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch streaming output", "details": str(e)}), 500
    
    def api_task_aggregated_output(self, task_id):
        """GET /api/task/<task_id>/output/aggregated - Get final combined output for a task"""
        try:
            if hasattr(self.data_store.mongodb, 'get_task_aggregated_output'):
                output = self.data_store.mongodb.get_task_aggregated_output(task_id)
                if output:
                    return jsonify(sanitize_for_json(output))
                else:
                    return jsonify({"error": "Aggregated output not found"}), 404
            else:
                return jsonify({"error": "Aggregated output not available"}), 503
        except Exception as e:
            self.logger.error("Error fetching aggregated output for task %s: %s\n%s", task_id, str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch aggregated output", "details": str(e)}), 500
    
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
        try:
            base_status = {
                'mongodb_connected': self.data_store.mongodb.is_connected(),
                'total_clients': len(self.data_store.get_all_clients()),
                'total_tasks': len(self.data_store.get_all_tasks()),
                'auth_enabled': True
            }
            
            # Add database statistics if available
            if hasattr(self.data_store.mongodb, 'get_database_stats'):
                try:
                    db_stats = self.data_store.mongodb.get_database_stats()
                    base_status.update(db_stats)
                except Exception as e:
                    self.logger.warning("Failed to get database stats: %s", str(e))
            
            return jsonify(base_status)
        except Exception as e:
            self.logger.error("Error fetching system status: %s\n%s", str(e), traceback.format_exc())
            return jsonify({"error": "Failed to fetch system status", "details": str(e)}), 500
    
    def run(self, host, port):
        """Start the Flask server"""
        self.logger.info("Starting Flask web UI with Firebase auth on %s:%d", host, port)
        self.app.run(host=host, port=port, threaded=True, debug=False)
    
    def close(self):
        """Clean up resources"""
        # Don't close the auth manager here since it's shared
        # It will be closed in main.py
        pass