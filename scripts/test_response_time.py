import argparse
from pymongo import MongoClient
from datetime import datetime
import time
import json

def connect_mongo(mongo_uri):
    print(f'Connecting to {mongo_uri} ...\n')
    return MongoClient(mongo_uri)

def convert_iso_dates(obj):
    """
    Convertit récursivement toutes les chaînes ISO 8601 en datetime.
    """
    if isinstance(obj, dict):
        return {k: convert_iso_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_iso_dates(v) for v in obj]
    elif isinstance(obj, str):
        try:
            # Tente de parser la chaîne comme ISO 8601
            # remplace Z par +00:00 pour timezone UTC
            dt = datetime.fromisoformat(obj.replace("Z", "+00:00"))
            return dt
        except ValueError:
            return obj
    else:
        return obj


def main(args):
     
    mc = connect_mongo(args.uri)
    db = mc[args.db]
    collection = db[args.collection]
    query = convert_iso_dates(json.loads(args.find_query))
    
    start_time = time.time()
    results = list(collection.find(query))
    elapsed_time = (time.time()-start_time) * 1000
    print(f'Query results {len(results)} documents in {elapsed_time:.2f} ms.')
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate JSON from S3 to MongoDB with integrity checks")
    parser.add_argument("--uri", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--find_query", required=True)
    args = parser.parse_args()
    main(args)