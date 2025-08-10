# ZMQ Server with MongoDB Integration

A modular ZMQ ROUTER server that handles multiple clients with MongoDB persistence for data storage and logging.

## Features

- **ZMQ ROUTER Server**: Handles multiple client connections with unique identity management
- **MongoDB Integration**: Persistent storage for clients, tasks, and logs
- **RESTful API**: Flask-based web API for client and task management
- **Real-time Logging**: Client output and events stored in MongoDB
- **Thread-safe Operations**: Concurrent handling of multiple clients
- **Fallback Mechanism**: In-memory storage when MongoDB is unavailable

## Architecture

```
├── config.py              # Configuration settings
├── logger_setup.py         # Logging configuration
├── data_models.py          # Data storage with MongoDB integration
├── utils.py               # Utility functions
├── mongodb_manager.py     # MongoDB connection and operations
├── message_handler.py     # Message handling logic
├── zmq_server.py          # ZMQ ROUTER server implementation
├── flask_api.py           # REST API endpoints
├── main.py                # Main application entry point
├── setup_mongodb.py       # MongoDB setup script
└── requirements.txt       # Python dependencies
```

## Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up MongoDB:**
   ```bash
   python setup_mongodb.py
   ```

3. **Start the server:**
   ```bash
   python main.py
   ```

## Configuration

Edit `config.py` to customize:

- **ZMQ Settings**: `BIND_ADDR`, `WEBUI_ADDR`
- **MongoDB**: `MONGODB_URI`, `DATABASE_NAME`
- **Security**: `VALID_API_KEYS`
- **Logging**: `LOG_DIR`

## API Endpoints

### Get All Clients
```http
GET /api/clients
```

### Get All Tasks
```http
GET /api/tasks
```

### Send Task to Client
```http
POST /api/send
Content-Type: application/json

{
  "client_id": "client_name",
  "mode": "shell",
  "payload": "ls -la"
}
```

### Get Client Logs
```http
GET /api/client/<client_id>/logs?limit=100
```

### Get System Status
```http
GET /api/status
```

## MongoDB Collections

### clients
Stores client information:
```javascript
{
  "client_id": "unique_client_name",
  "identity_hex": "hex_encoded_identity",
  "hostname": "client_hostname",
  "last_seen": ISODate("..."),
  "created_at": ISODate("..."),
  "updated_at": ISODate("...")
}
```

### tasks
Stores task information:
```javascript
{
  "id": "uuid_task_id",
  "target": "client_id",
  "mode": "shell",
  "payload": "command_or_data",
  "status": "queued|running|completed|failed",
  "created_at": ISODate("..."),
  "started_at": ISODate("..."),
  "completed_at": ISODate("..."),
  "exit_code": 0
}
```

### client_logs
Stores client output and events:
```javascript
{
  "client_id": "client_name",
  "task_id": "task_uuid",
  "msg_id": "message_uuid",
  "output": "command_output",
  "timestamp": ISODate("..."),
  "log_type": "output|event"
}
```

## Client Message Protocol

### Client Hello (Registration)
```json
{
  "type": "hello",
  "client_id": "unique_client_name",
  "api_key": "supersecret123",
  "hostname": "client_hostname"
}
```

### Task Started Notification
```json
{
  "type": "task_started",
  "task": "task_uuid"
}
```

### Task Output
```json
{
  "type": "output",
  "task": "task_uuid",
  "chunk": "command_output_chunk",
  "msg_id": "message_uuid",
  "ts": "2025-01-01T12:00:00Z"
}
```

### Task Completion
```json
{
  "type": "completed",
  "task": "task_uuid",
  "exit_code": 0,
  "ts": "2025-01-01T12:00:00Z"
}
```

## Features

### Automatic Reconnection
- MongoDB connection is automatically tested and reconnected if needed
- In-memory fallback when MongoDB is unavailable

### Logging
- File-based logging for each client in `logs_server/` directory
- Structured logging in MongoDB for advanced querying
- Real-time event logging (task completion, errors, etc.)

### Security
- API key validation for client connections
- Identity-based client management
- Secure MongoDB connection options

### Performance
- Indexed MongoDB collections for fast queries
- In-memory caching for frequently accessed data
- Thread-safe operations for concurrent access

## Monitoring

The server provides several monitoring endpoints:

- **System Status**: `/api/status` - Shows MongoDB connection status and statistics
- **Client Logs**: `/api/client/<id>/logs` - Per-client activity logs
- **Task History**: `/api/tasks` - Complete task execution history

## Development

To extend the server:

1. **Add new message types**: Extend `MessageHandler` class
2. **Add new API endpoints**: Extend `FlaskAPI` class
3. **Add new data models**: Extend `mongodb_manager.py`
4. **Modify configuration**: Update `config.py`

## Troubleshooting

1. **MongoDB Connection Issues**:
   - Verify MongoDB is running on `192.168.1.12:27017`
   - Check network connectivity
   - Review MongoDB logs

2. **Client Connection Issues**:
   - Verify API keys in `config.py`
   - Check ZMQ port availability
   - Review server logs

3. **Performance Issues**:
   - Monitor MongoDB index usage
   - Check memory usage for large datasets
   - Consider log rotation for file-based logs