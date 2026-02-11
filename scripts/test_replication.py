import argparse
from pymongo import MongoClient

# ---------- Mongo helpers ----------
def connect_mongo(mongo_uri):
    print(f'Connecting to {mongo_uri} ...')
    return MongoClient(mongo_uri)

def display_dbs(client,server):
    print(f"Bases disponibles sur serveur {server}:", client.list_database_names(), "\n")

# ---------- Main ----------
def main(args):
    uri_rs = f"mongodb://{args.u}:{args.p}@{args.adress1},{args.adress2},{args.adress3}/?replicaSet={args.rs}"
    uri_1 = f"mongodb://{args.u}:{args.p}@{args.adress1}/?authSource=admin"
    uri_2 = f"mongodb://{args.u}:{args.p}@{args.adress2}/?authSource=admin"
    uri_3 = f"mongodb://{args.u}:{args.p}@{args.adress3}/?authSource=admin"

    mc_write = connect_mongo(uri_rs)
    db = mc_write[args.db]
    db.create_collection("collection_test")
    print(f'Successfuly created database {args.db}\n')
    
    display_dbs(connect_mongo(uri_1),args.adress1)
    display_dbs(connect_mongo(uri_2),args.adress2)
    display_dbs(connect_mongo(uri_3),args.adress3)
    
    mc_write.drop_database(args.db)
    print(f'Database {args.db} deleted')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate JSON from S3 to MongoDB with integrity checks")
    parser.add_argument("--u", required=True)
    parser.add_argument("--p", required=True)
    parser.add_argument("--adress1", required=True)
    parser.add_argument("--adress2", required=True)
    parser.add_argument("--adress3", required=True)
    parser.add_argument("--rs", default='rs0')
    parser.add_argument("--db", default='test')
    args = parser.parse_args()
    main(args)