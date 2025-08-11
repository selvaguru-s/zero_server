#!/usr/bin/env python3
"""
setup_mongodb.py
MongoDB setup script to create database, collections, and indexes with Firebase auth and dual logging
"""

import pymongo
from datetime import datetime, timedelta
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
        collections = [
            CLIENTS_COLLECTION, 
            TASKS_COLLECTION, 
            CLIENT_LOGS_COLLECTION, 
            API_KEYS_COLLECTION,
            'aggregated_task_output'  # New collection for final aggregated output
        ]
        
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
        
        # Client logs collection indexes (for streaming)
        logs_coll = db[CLIENT_LOGS_COLLECTION]
        logs_coll.create_index([("client_id", 1), ("timestamp", -1)])
        logs_coll.create_index([("task_id", 1), ("sequence", 1)])  # For ordered streaming
        logs_coll.create_index("timestamp")
        logs_coll.create_index("log_type")
        print("✓ Created indexes for client_logs collection")
        
        # Aggregated task output collection indexes (for final results)
        aggregated_coll = db['aggregated_task_output']
        aggregated_coll.create_index("task_id", unique=True)
        aggregated_coll.create_index([("client_id", 1), ("completed_at", -1)])
        aggregated_coll.create_index("completed_at")
        aggregated_coll.create_index("status")
        print("✓ Created indexes for aggregated_task_output collection")
        
        # API keys collection indexes
        api_keys_coll = db[API_KEYS_COLLECTION]
        api_keys_coll.create_index("api_key", unique=True)
        api_keys_coll.create_index("user_id", unique=True)
        api_keys_coll.create_index("created_at")
        api_keys_coll.create_index("active")
        print("✓ Created indexes for api_keys collection")
        
        # Clean up any duplicate API keys
        print("\nCleaning up duplicate API keys...")
        cleanup_duplicate_api_keys(db)
        
        print("\n✅ MongoDB setup completed successfully!")
        
        # Show database statistics
        stats = db.command("dbstats")
        print(f"\nDatabase statistics:")
        print(f"Collections: {stats['collections']}")
        print(f"Indexes: {stats['indexes']}")
        print(f"Data size: {stats.get('dataSize', 0)} bytes")
        
        # Show collection counts
        print(f"\nCollection document counts:")
        for collection_name in collections:
            count = db[collection_name].count_documents({})
            print(f"  {collection_name}: {count} documents")
        
        client.close()
        
    except Exception as e:
        print(f"❌ MongoDB setup failed: {e}")
        return False
    
    return True

def cleanup_duplicate_api_keys(db):
    """Clean up duplicate API keys by keeping only the most recent active one per user"""
    try:
        api_keys_coll = db[API_KEYS_COLLECTION]
        
        # Find users with multiple active API keys
        pipeline = [
            {"$match": {"active": True}},
            {"$group": {
                "_id": "$user_id",
                "count": {"$sum": 1},
                "keys": {"$push": {"_id": "$_id", "created_at": "$created_at", "api_key": "$api_key"}}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        duplicates = list(api_keys_coll.aggregate(pipeline))
        
        if not duplicates:
            print("✓ No duplicate API keys found")
            return
        
        total_cleaned = 0
        for user_data in duplicates:
            user_id = user_data["_id"]
            keys = user_data["keys"]
            
            # Sort by created_at to keep the most recent
            keys.sort(key=lambda x: x["created_at"], reverse=True)
            
            # Keep the first (most recent), deactivate the rest
            keys_to_deactivate = keys[1:]
            
            for key in keys_to_deactivate:
                api_keys_coll.update_one(
                    {"_id": key["_id"]},
                    {
                        "$set": {
                            "active": False,
                            "deactivated_at": datetime.now(),
                            "deactivation_reason": "duplicate_cleanup"
                        }
                    }
                )
                total_cleaned += 1
        
        print(f"✓ Cleaned up {total_cleaned} duplicate API keys for {len(duplicates)} users")
        
    except Exception as e:
        print(f"❌ Failed to cleanup duplicate API keys: {e}")

if __name__ == '__main__':
    setup_mongodb()