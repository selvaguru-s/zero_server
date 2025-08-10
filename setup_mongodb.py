#!/usr/bin/env python3
"""
setup_mongodb.py
MongoDB setup script to create database, collections, and indexes with Firebase auth
"""

import pymongo
from config import MONGODB_URI, DATABASE_NAME, CLIENTS_COLLECTION, TASKS_COLLECTION, CLIENT_LOGS_COLLECTION, API_KEYS_COLLECTION

def setup_mongodb():
    """Set up MongoDB database with required collections and indexes"""
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        print(f"✓ Connected to MongoDB at {MONGODB_URI}")
        
        # Get or create database
        db = client[DATABASE_NAME]
        print(f"✓ Using database: {DATABASE_NAME}")
        
        # Create collections
        collections = [CLIENTS_COLLECTION, TASKS_COLLECTION, CLIENT_LOGS_COLLECTION, API_KEYS_COLLECTION]
        for collection_name in collections:
            if collection_name not in db.list_collection_names():
                db.create_collection(collection_name)
                print(f"✓ Created collection: {collection_name}")
            else:
                print(f"✓ Collection already exists: {collection_name}")
        
        # Create indexes
        print("\nCreating indexes...")
        
        # Clients collection indexes
        clients_coll = db[CLIENTS_COLLECTION]
        clients_coll.create_index("client_id", unique=True)
        clients_coll.create_index("last_seen")
        print("✓ Created indexes for clients collection")
        
        # Tasks collection indexes
        tasks_coll = db[TASKS_COLLECTION]
        tasks_coll.create_index("id", unique=True)
        tasks_coll.create_index("target")
        tasks_coll.create_index("status")
        tasks_coll.create_index("created_at")
        print("✓ Created indexes for tasks collection")
        
        # Client logs collection indexes
        logs_coll = db[CLIENT_LOGS_COLLECTION]
        logs_coll.create_index([("client_id", 1), ("timestamp", -1)])
        logs_coll.create_index("task_id")
        logs_coll.create_index("timestamp")
        print("✓ Created indexes for client_logs collection")
        
        # API keys collection indexes
        api_keys_coll = db[API_KEYS_COLLECTION]
        api_keys_coll.create_index("api_key", unique=True)
        api_keys_coll.create_index("user_id", unique=True)
        api_keys_coll.create_index("created_at")
        api_keys_coll.create_index("active")
        print("✓ Created indexes for api_keys collection")
        
        print("\n✅ MongoDB setup completed successfully!")
        
        # Show database statistics
        stats = db.command("dbstats")
        print(f"\nDatabase statistics:")
        print(f"Collections: {stats['collections']}")
        print(f"Indexes: {stats['indexes']}")
        print(f"Data size: {stats.get('dataSize', 0)} bytes")
        
        client.close()
        
    except Exception as e:
        print(f"❌ MongoDB setup failed: {e}")
        return False
    
    return True

if __name__ == '__main__':
    setup_mongodb()