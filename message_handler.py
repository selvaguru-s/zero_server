#!/usr/bin/env python3
"""
message_handler.py
Message handling logic for different message types with Firebase auth
"""

import json
import uuid
from datetime import datetime, timezone
from utils import identity_to_str, append_client_log

class MessageHandler:
    """Handle different types of incoming messages with auth"""
    
    def __init__(self, router, data_store, logger, auth_manager):
        self.router = router
        self.data_store = data_store
        self.logger = logger
        self.auth_manager = auth_manager
    
    def handle_hello(self, identity, msg):
        """Handle client hello/registration message with API key validation"""
        client_id = msg.get('client_id')
        api_key = msg.get('api_key')
        
        if not client_id or not api_key:
            self.logger.warning("Rejecting client %s due to missing credentials", client_id or "<no id>")
            self._send_response(identity, {
                'type': 'reject', 
                'reason': 'Missing client_id or api_key'
            })
            return
        
        # Validate API key through Firebase auth manager
        if not self.auth_manager.validate_api_key(api_key):
            self.logger.warning("Rejecting client %s due to invalid API key", client_id)
            self._send_response(identity, {
                'type': 'reject', 
                'reason': 'Invalid API key'
            })
            return
        
        # Get user info for logging
        user_info = self.auth_manager.get_user_by_api_key(api_key)
        user_email = user_info.get('email', 'unknown') if user_info else 'unknown'
        
        is_new = self.data_store.add_or_update_client(
            identity, client_id, msg.get('hostname')
        )
        
        if is_new:
            self.logger.info("Registered new client %s (user: %s)", client_id, user_email)
        else:
            self.logger.info("Updated client %s (user: %s)", client_id, user_email)
        
        # Log authentication event
        self.data_store.log_client_event(
            client_id, 
            'authenticated', 
            {'user_email': user_email, 'hostname': msg.get('hostname')}
        )
        
        self._send_response(identity, {
            'type': 'ack', 
            'ack_for': 'hello', 
            'ts': datetime.now(timezone.utc).isoformat()
        })
    
    def handle_task_started(self, identity, msg):
        """Handle task started notification"""
        task_id = msg.get('task')
        self.data_store.update_task_status(
            task_id, 
            'running', 
            started_at=datetime.now(timezone.utc).isoformat()
        )
        
        self._send_response(identity, {
            'type': 'ack', 
            'ack_for': task_id
        })
        
        self.logger.info("Task %s started by %s", task_id, identity_to_str(identity))
    
    def handle_output(self, identity, msg):
        """Handle task output message"""
        task_id = msg.get('task')
        chunk = msg.get('chunk', '')
        msg_id = msg.get('msg_id') or str(uuid.uuid4())
        ts = msg.get('ts') or datetime.now(timezone.utc).isoformat()
        client_id = identity_to_str(identity)
        
        # Log to file
        append_client_log(
            client_id, 
            f"[{ts}] TASK:{task_id} MSG:{msg_id} OUTPUT:\n{chunk}\n"
        )
        
        # Log to MongoDB
        self.data_store.log_client_output(client_id, task_id, msg_id, chunk, ts)
        
        self._send_response(identity, {
            'type': 'ack', 
            'ack_for': msg_id, 
            'task': task_id, 
            'ts': datetime.now(timezone.utc).isoformat()
        })
    
    def handle_completed(self, identity, msg):
        """Handle task completion notification"""
        task_id = msg.get('task')
        exit_code = msg.get('exit_code')
        ts = msg.get('ts') or datetime.now(timezone.utc).isoformat()
        client_id = identity_to_str(identity)
        
        status = 'completed' if exit_code == 0 else 'failed'
        self.data_store.update_task_status(
            task_id, 
            status, 
            completed_at=ts, 
            exit_code=exit_code
        )
        
        # Log to file
        append_client_log(
            client_id, 
            f"[{ts}] TASK:{task_id} COMPLETED exit_code={exit_code}"
        )
        
        # Log to MongoDB
        self.data_store.log_client_event(
            client_id, 
            'task_completed', 
            {'exit_code': exit_code, 'status': status}, 
            task_id
        )
        
        self._send_response(identity, {
            'type': 'ack', 
            'ack_for': f'completed:{task_id}', 
            'task': task_id, 
            'ts': datetime.now(timezone.utc).isoformat()
        })
    
    def handle_unknown(self, identity, msg_type):
        """Handle unknown message types"""
        self.logger.warning("Unknown message type %s from %s", msg_type, identity_to_str(identity))
        self._send_response(identity, {
            'type': 'ack', 
            'ack_for': 'unknown'
        })
    
    def _send_response(self, identity, response):
        """Send response back to client"""
        self.router.send_multipart([
            identity, b'', 
            json.dumps(response).encode('utf-8')
        ])