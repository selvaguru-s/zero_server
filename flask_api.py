#!/usr/bin/env python3
"""
flask_api.py
Flask REST API for the ZMQ server
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from utils import sanitize_for_json

class FlaskAPI:
    """Flask API wrapper for the ZMQ server"""
    
    def __init__(self, data_store, zmq_server, logger):
        self.data_store = data_store
        self.zmq_server = zmq_server
        self.logger = logger
        
        # Set up Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Register routes
        self._register_routes()
    
    def _register_routes(self):
        """Register all Flask routes"""
        self.app.route('/api/clients')(self.api_clients)
        self.app.route('/api/tasks')(self.api_tasks)
        self.app.route('/api/send', methods=['POST'])(self.api_send)
        self.app.route('/api/client/<client_id>/logs')(self.api_client_logs)
        self.app.route('/api/status')(self.api_status)
    
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
        """GET /api/client/<client_id>/logs - Get logs for a specific client"""
        limit = request.args.get('limit', 100, type=int)
        logs = self.data_store.get_client_logs(client_id, limit)
        return jsonify(sanitize_for_json(logs))
    
    def api_status(self):
        """GET /api/status - Get system status including MongoDB connection"""
        status = {
            'mongodb_connected': self.data_store.mongodb.is_connected(),
            'total_clients': len(self.data_store.get_all_clients()),
            'total_tasks': len(self.data_store.get_all_tasks())
        }
        return jsonify(status)
    
    def run(self, host, port):
        """Start the Flask server"""
        self.logger.info("Starting Flask web UI on %s:%d", host, port)
        self.app.run(host=host, port=port)