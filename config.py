#!/usr/bin/env python3
"""
config.py
Configuration settings for the ZMQ server with Firebase auth
"""

import os

# -------- SERVER CONFIGURATION --------
BIND_ADDR = "tcp://0.0.0.0:5555"
WEBUI_ADDR = ("0.0.0.0", 8080)
LOG_DIR = "logs_server"

# -------- MONGODB CONFIGURATION --------
MONGODB_URI = "mongodb://192.168.1.12:27017"
DATABASE_NAME = "zmq_server"
CLIENTS_COLLECTION = "clients"
TASKS_COLLECTION = "tasks"
CLIENT_LOGS_COLLECTION = "client_logs"
API_KEYS_COLLECTION = "api_keys"  # New collection for API keys

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)