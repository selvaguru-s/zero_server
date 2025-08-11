#!/usr/bin/env python3
"""
utils.py
Utility functions for the ZMQ server with enhanced JSON serialization
"""

import os
import json
from datetime import datetime, timezone
from bson import ObjectId
from config import LOG_DIR

def identity_to_str(b):
    """Convert identity bytes to a safe string representation"""
    try:
        s = b.decode('utf-8')
    except Exception:
        s = b.hex()
    return "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in s])[:120]

def append_client_log(client_str, text):
    """Append text to client-specific log file"""
    path = os.path.join(LOG_DIR, f"client_{client_str}.log")
    with open(path, 'a', encoding='utf-8') as f:
        f.write(text + "\n")

def sanitize_for_json(obj):
    """Recursively sanitize object for JSON serialization with enhanced handling"""
    if obj is None:
        return None
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items() if k != '_id'}  # Skip MongoDB _id field
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, tuple):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, set):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, bytes):
        try:
            return obj.decode('utf-8', errors='replace')
        except Exception:
            return obj.hex()
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif hasattr(obj, 'isoformat'):  # Handle datetime-like objects
        try:
            return obj.isoformat()
        except:
            return str(obj)
    elif hasattr(obj, '__dict__'):  # Handle custom objects
        return sanitize_for_json(obj.__dict__)
    elif isinstance(obj, (int, float, str, bool)):
        return obj
    else:
        # For any other type, convert to string
        try:
            return str(obj)
        except:
            return "<unserializable>"

def safe_json_dumps(obj, **kwargs):
    """Safely serialize object to JSON string"""
    try:
        sanitized = sanitize_for_json(obj)
        return json.dumps(sanitized, **kwargs)
    except Exception as e:
        # If all else fails, return error information
        return json.dumps({
            "error": "JSON serialization failed",
            "details": str(e),
            "type": str(type(obj))
        })

def parse_router_frames(parts):
    """
    Parse ZMQ ROUTER frames, accepting both:
      [identity, b'', payload]
    and
      [identity, payload]
    Return (identity_bytes, payload_bytes) or (None, None) if malformed.
    """
    if len(parts) == 3 and parts[1] == b'':
        return parts[0], parts[2]
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, None

def format_client_id(client_id):
    """Format client ID for safe URL usage"""
    if not client_id:
        return "unknown"
    
    # Replace problematic characters with underscores
    safe_chars = "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in client_id])
    return safe_chars[:100]  # Limit length

def validate_client_id(client_id):
    """Validate that client_id is safe for use in URLs and file systems"""
    if not client_id or len(client_id) > 200:
        return False
    
    # Check for potentially dangerous characters
    dangerous_chars = ['/', '\\', '..', '<', '>', ':', '"', '|', '?', '*']
    for char in dangerous_chars:
        if char in client_id:
            return False
    
    return True