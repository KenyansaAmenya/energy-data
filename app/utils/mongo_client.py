from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from config.settings import MongoConfig

class MongoDBClient:
    _instance: Optional["MongoDBClient"] = None
    _client: Optional[MongoClient] = None

    def __new__(cls, config: MongoConfig) -> "MongoDBClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._config = config
        return cls._instance

    def _get_client(self) -> MongoClient:
        if self._client is None: 
            self._client = MongoClient(
                self._config.uri, 
                maxPoolSize=50,
                minPoolSize=10,
                serverSelectionTimeoutMS=5000,
                retryWrites=True
            )        
        return self._client

    def get_database(self) -> Database:
        return self._get_client()[self._config.db_name]

    def health_check(self) -> bool:
        try:
            self._get_client().admin.command('ping')
            return True
        except Exception:
            return False

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            MongoDBClient._instance = None

def get_mongo_db(config: MongoConfig) -> Database:
    client = MongoDBClient(config)
    return client.get_database() 