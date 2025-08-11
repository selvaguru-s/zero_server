#!/usr/bin/env python3
"""
firebase_auth.py
Firebase authentication and API key management
"""

import firebase_admin
from firebase_admin import credentials, auth
import secrets
import string
from datetime import datetime, timezone
from config import DATABASE_NAME, MONGODB_URI
import pymongo

class FirebaseAuthManager:
    """Manage Firebase authentication and API key generation"""
    
    def __init__(self, service_account_path, logger):
        self.logger = logger
        self.mongodb_client = None
        self.db = None
        self.api_keys_collection = None
        
        # Initialize Firebase Admin
        try:
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
            self.logger.info("Firebase Admin SDK initialized")
        except Exception as e:
            self.logger.error("Failed to initialize Firebase: %s", e)
            raise
        
        # Initialize MongoDB connection
        self._init_mongodb()
    
    def _init_mongodb(self):
        """Initialize MongoDB connection for API keys"""
        try:
            self.mongodb_client = pymongo.MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000
            )
            self.mongodb_client.admin.command('ping')
            
            self.db = self.mongodb_client[DATABASE_NAME]
            self.api_keys_collection = self.db['api_keys']
            
            # Create indexes
            self.api_keys_collection.create_index("api_key", unique=True)
            self.api_keys_collection.create_index("user_id", unique=True)
            self.api_keys_collection.create_index("created_at")
            
            self.logger.info("MongoDB connection established for auth")
            
        except Exception as e:
            self.logger.error("Failed to connect to MongoDB for auth: %s", e)
            raise
    
    def verify_firebase_token(self, id_token):
        """Verify Firebase ID token and return user info"""
        try:
            decoded_token = auth.verify_id_token(id_token)
            return {
                'user_id': decoded_token['uid'],
                'email': decoded_token.get('email'),
                'name': decoded_token.get('name'),
                'verified': decoded_token.get('email_verified', False)
            }
        except Exception as e:
            self.logger.error("Firebase token verification failed: %s", e)
            return None
    
    def generate_api_key(self, length=32):
        """Generate a secure random API key"""
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    def create_or_get_api_key(self, user_info):
        """Create or retrieve API key for authenticated user"""
        user_id = user_info['user_id']
        
        try:
            # Use findAndModify operation to handle race conditions
            # First, try to find and update existing active key
            existing = self.api_keys_collection.find_one_and_update(
                {
                    'user_id': user_id,
                    'active': True
                },
                {
                    '$set': {
                        'last_used': datetime.now(timezone.utc),
                        'email': user_info.get('email'),
                        'name': user_info.get('name'),
                        'verified': user_info.get('verified', False)
                    }
                },
                return_document=pymongo.ReturnDocument.AFTER
            )
            
            if existing:
                self.logger.info("Retrieved existing API key for user %s", user_id)
                return existing['api_key']
            
            # No existing active key found, create new one
            # First deactivate any old keys for this user
            self.api_keys_collection.update_many(
                {'user_id': user_id},
                {'$set': {'active': False, 'deactivated_at': datetime.now(timezone.utc)}}
            )
            
            # Generate new API key
            api_key = self.generate_api_key()
            
            # Store new key with upsert to handle edge cases
            doc = {
                'user_id': user_id,
                'api_key': api_key,
                'email': user_info.get('email'),
                'name': user_info.get('name'),
                'verified': user_info.get('verified', False),
                'created_at': datetime.now(timezone.utc),
                'last_used': datetime.now(timezone.utc),
                'active': True
            }
            
            try:
                self.api_keys_collection.insert_one(doc)
                self.logger.info("Created new API key for user %s", user_id)
                return api_key
            except pymongo.errors.DuplicateKeyError:
                # In case of race condition, try to get the existing key
                existing = self.api_keys_collection.find_one({
                    'user_id': user_id,
                    'active': True
                })
                if existing:
                    self.logger.info("Found existing API key after race condition for user %s", user_id)
                    return existing['api_key']
                else:
                    self.logger.error("Race condition in API key creation for user %s", user_id)
                    return None
            
        except Exception as e:
            self.logger.error("Failed to create/get API key for user %s: %s", user_id, e)
            return None
    
    def validate_api_key(self, api_key):
        """Validate API key and update last_used timestamp"""
        try:
            result = self.api_keys_collection.find_one_and_update(
                {
                    'api_key': api_key,
                    'active': True
                },
                {
                    '$set': {'last_used': datetime.now(timezone.utc)}
                },
                return_document=pymongo.ReturnDocument.AFTER
            )
            
            return result is not None
            
        except Exception as e:
            self.logger.error("Failed to validate API key: %s", e)
            return False
    
    def revoke_api_key(self, user_id):
        """Revoke API key for a user"""
        try:
            result = self.api_keys_collection.update_one(
                {'user_id': user_id},
                {'$set': {'active': False, 'revoked_at': datetime.now(timezone.utc)}}
            )
            return result.modified_count > 0
        except Exception as e:
            self.logger.error("Failed to revoke API key for user %s: %s", user_id, e)
            return False
    
    def get_user_by_api_key(self, api_key):
        """Get user information by API key"""
        try:
            doc = self.api_keys_collection.find_one({
                'api_key': api_key,
                'active': True
            })
            return doc
        except Exception as e:
            self.logger.error("Failed to get user by API key: %s", e)
            return None
    
    def cleanup_duplicate_keys(self, user_id):
        """Clean up duplicate API keys for a user (keep the most recent active one)"""
        try:
            # Find all keys for this user
            keys = list(self.api_keys_collection.find({
                'user_id': user_id,
                'active': True
            }).sort('created_at', -1))
            
            if len(keys) <= 1:
                return True  # No duplicates
            
            # Keep the most recent one, deactivate the rest
            keep_key = keys[0]
            for key in keys[1:]:
                self.api_keys_collection.update_one(
                    {'_id': key['_id']},
                    {
                        '$set': {
                            'active': False,
                            'deactivated_at': datetime.now(timezone.utc),
                            'deactivation_reason': 'duplicate_cleanup'
                        }
                    }
                )
            
            self.logger.info("Cleaned up %d duplicate API keys for user %s", len(keys) - 1, user_id)
            return True
            
        except Exception as e:
            self.logger.error("Failed to cleanup duplicate keys for user %s: %s", user_id, e)
            return False
    
    def close(self):
        """Close MongoDB connection"""
        if self.mongodb_client:
            self.mongodb_client.close()
            self.logger.info("Firebase auth MongoDB connection closed")