#!/usr/bin/env python3
"""
logger_setup.py
Logging configuration for the ZMQ server
"""

import logging

def setup_logger(name="server"):
    """Set up and return a configured logger"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Create console handler if it doesn't exist
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(ch)
    
    return logger