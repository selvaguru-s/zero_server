#!/usr/bin/env python3
"""
utils.py
Utility functions for the ZMQ server
"""

import os
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
    """Recursively sanitize object for JSON serialization"""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, bytes):
        try:
            return obj.decode('utf-8', errors='replace')
        except Exception:
            return obj.hex()
    else:
        return obj

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