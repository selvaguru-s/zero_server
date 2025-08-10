#!/usr/bin/env python3
"""
main.py
Main entry point for the ZMQ server application with Firebase auth
"""

import threading
from config import WEBUI_ADDR, FIREBASE_SERVICE_ACCOUNT_PATH
from logger_setup import setup_logger
from data_models import DataStore
from zmq_server import ZMQServer
from flask_api import FlaskAPI

def main():
    """Main application entry point"""
    # Set up logging
    logger = setup_logger("server")
    
    try:
        # Initialize data store
        data_store = DataStore(logger)
        
        # Initialize ZMQ server
        zmq_server = ZMQServer(data_store, logger)
        
        # Initialize Flask API with Firebase auth
        flask_api = FlaskAPI(data_store, zmq_server, logger, FIREBASE_SERVICE_ACCOUNT_PATH)
        
        # Start ZMQ message handling in a separate thread
        zmq_thread = threading.Thread(target=zmq_server.handle_incoming, daemon=True)
        zmq_thread.start()
        
        logger.info("ZMQ server started with Firebase authentication")
        
        # Start Flask web server (blocking)
        flask_api.run(WEBUI_ADDR[0], WEBUI_ADDR[1])
        
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
    except Exception as e:
        logger.error("Failed to start server: %s", e)
    finally:
        # Clean shutdown
        if 'data_store' in locals():
            data_store.close()
        if 'flask_api' in locals():
            flask_api.close()
        logger.info("Server shutdown complete")

if __name__ == '__main__':
    main()