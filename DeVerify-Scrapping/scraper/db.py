# scraper/db.py
import os
from typing import Dict, Optional

# try to import pymongo and fail fast with a clear message if missing
try:
    import pymongo
    from pymongo import MongoClient as _MongoClient
except ImportError:  # pragma: no cover
    def _missing_pymongo(*args, **kwargs):
        raise RuntimeError(
            "pymongo is not installed. Install it with 'pip install pymongo' "
            "before running this code."
        )
    _MongoClient = _missing_pymongo

# env defaults (can be overridden at runtime via set_mongo_uri)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "hackathons")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "hack-info")

# internal client holder
_client: Optional[object] = None

def _ensure_client():
    """
    Ensure a connected MongoClient is available in module scope.
    Will attempt a ping to validate the connection.
    """
    global _client
    if _client is not None:
        return
    if _MongoClient is None:
        raise RuntimeError("pymongo MongoClient is not available.")
    # create client with a short server selection timeout to fail fast
    _client = _MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        # validate connectivity
        _client.admin.command("ping")
    except Exception as e:
        # close and re-raise a clearer error
        try:
            _client.close()
        except Exception:
            pass
        _client = None
        raise RuntimeError(f"Failed to connect to MongoDB at '{MONGO_URI}': {e}") from e

def get_collection(collection_name: str = None):
    """
    Return a collection object. Validates connection first.
    """
    _ensure_client()
    db_name = os.getenv("MONGO_DB", MONGO_DB)
    coll_name = collection_name or os.getenv("MONGO_COLLECTION", MONGO_COLLECTION)
    db = _client[db_name]
    return db[coll_name]

def upsert_hackathon(item: Dict):
    """
    Upsert a hackathon document into the configured collection.
    Raises on missing 'id' or connection errors.
    Returns the pymongo UpdateResult.
    """
    if not isinstance(item, dict):
        raise ValueError("item must be a dict")
    if "id" not in item or not item["id"]:
        raise ValueError("item must contain a non-empty 'id' field for upsert")
    col = get_collection()
    result = col.update_one({"id": item["id"]}, {"$set": item}, upsert=True)
    return result

def set_mongo_uri(uri: str):
    """
    Override the module-level MONGO_URI at runtime and reset the client so
    future calls use the new URI.
    """
    global MONGO_URI, _client
    if not uri:
        return
    MONGO_URI = uri
    if _client:
        try:
            _client.close()
        except Exception:
            pass
    _client = None

def get_mongo_uri() -> str:
    return MONGO_URI
