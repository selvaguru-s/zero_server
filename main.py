#!/usr/bin/env python3
"""
main.py
Main entry point for the ZMQ server application
"""

import threading
from config import WEBUI_ADDR
from logger_setup import setup_logger
from data_models import DataStore
from zmq_server import ZMQServer
from flask_api import FlaskAPI

def main():
    """Main application entry point"""
    # Set up logging
    logger = setup_logger("server")
    
    # Initialize data store
    data_store = DataStore(logger)
    
    # Initialize ZMQ server
    zmq_server = ZMQServer(data_store, logger)
    
    # Initialize Flask API
    flask_api = FlaskAPI(data_store, zmq_server, logger)
    
    # Start ZMQ message handling in a separate thread
    zmq_thread = threading.Thread(target=zmq_server.handle_incoming, daemon=True)
    zmq_thread.start()
    
    try:
        # Start Flask web server (blocking)
        flask_api.run(WEBUI_ADDR[0], WEBUI_ADDR[1])
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
    finally:
        # Clean shutdown
        data_store.close()
        logger.info("Server shutdown complete")

if __name__ == '__main__':
    main()