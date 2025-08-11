#!/usr/bin/env python3
"""
zmq_server.py
ZMQ ROUTER server handling incoming messages with Firebase auth
"""

import zmq
import json
import uuid
from datetime import datetime, timezone
from config import BIND_ADDR
from utils import parse_router_frames, identity_to_str
from message_handler import MessageHandler

class ZMQServer:
    """ZMQ ROUTER server for handling client connections with Firebase auth"""
    
    def __init__(self, data_store, logger, auth_manager):
        self.data_store = data_store
        self.logger = logger
        
        # Use the shared auth manager instead of creating a new one
        self.auth_manager = auth_manager
        
        # Set up ZMQ
        self.context = zmq.Context.instance()
        self.router = self.context.socket(zmq.ROUTER)
        self.router.bind(BIND_ADDR)
        self.logger.info("Bound ROUTER socket to %s", BIND_ADDR)
        
        # Message handler with shared auth manager
        self.message_handler = MessageHandler(self.router, data_store, logger, self.auth_manager)
    
    def handle_incoming(self):
        """Main loop for handling incoming messages"""
        poller = zmq.Poller()
        poller.register(self.router, zmq.POLLIN)
        
        while True:
            socks = dict(poller.poll(1000))
            if self.router in socks and socks[self.router] == zmq.POLLIN:
                self._process_message()
    
    def _process_message(self):
        """Process a single incoming message"""
        parts = self.router.recv_multipart()
        identity, raw = parse_router_frames(parts)
        
        if identity is None:
            return
        
        if not raw:
            self.logger.warning("Empty payload from %s, ignoring", identity_to_str(identity))
            return
        
        try:
            msg = json.loads(raw.decode('utf-8'))
        except Exception as e:
            self.logger.exception("Invalid JSON from %s: %s -- raw=%r", 
                                identity_to_str(identity), e, raw)
            return
        
        msg_type = msg.get('type')
        self.logger.debug("Recv %s from %s", msg_type, identity_to_str(identity))
        
        # Route message to appropriate handler
        if msg_type == 'hello':
            self.message_handler.handle_hello(identity, msg)
        elif msg_type == 'task_started':
            self.message_handler.handle_task_started(identity, msg)
        elif msg_type == 'output':
            self.message_handler.handle_output(identity, msg)
        elif msg_type == 'completed':
            self.message_handler.handle_completed(identity, msg)
        else:
            self.message_handler.handle_unknown(identity, msg_type)
    
    def send_task_to_client(self, target_identity, mode, payload):
        """Send a task to a specific client"""
        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        
        msg = {
            'type': 'exec',
            'task': task_id,
            'mode': mode,
            'payload': payload,
            'ts': now
        }
        
        # Store task in data store
        self.data_store.add_task(task_id, target_identity, mode, payload)
        
        # Send message to client
        self.router.send_multipart([
            target_identity, b'', 
            json.dumps(msg).encode('utf-8')
        ])
        
        self.logger.info("Sent task %s to %s", task_id, identity_to_str(target_identity))
        return task_id