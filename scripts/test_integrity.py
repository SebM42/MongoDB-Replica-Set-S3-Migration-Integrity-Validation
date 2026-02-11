import argparse
import json
import re
from pprint import pprint
import pandas as pd
from collections import Counter
from pymongo import MongoClient
from datetime import datetime
from dateutil import parser as date_parser
import boto3


# ---------- S3 helpers ----------
def s3_client(aws_access_key, aws_secret_key, region):
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

def list_json_keys(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".json"):
                keys.append(key)
    return keys

def read_json_from_s3(s3, bucket, key):
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read().decode("utf-8")
    data = json.loads(body)
    # wrap dict into list
    if isinstance(data, dict):
        data = [data]
    return data

# ---------- Mongo helpers ----------
def connect_mongo(mongo_uri):
    print(f'Connecting to {mongo_uri} ...')
    return MongoClient(mongo_uri)

# ---------- Integrity checks ----------
def analyze_records_basic(records):
    if not records:
        return {
            "n_rows": 0,
            "fields": [],
            "dtypes": {},
            "missing": {},
            "exact_duplicates": 0,
            "sample_values": {},
        }
    df = pd.DataFrame(records)
    fields = list(df.columns)
    dtypes = {col: str(df[col].dtype) for col in fields}
    missing = {col: int(df[col].isna().sum()) for col in fields}
    exact_duplicates = int(df.duplicated(keep=False).sum())
    sample_values = {col: df[col].dropna().astype(str).unique()[:5].tolist() for col in fields}
    return {
        "n_rows": len(df),
        "fields": fields,
        "dtypes": dtypes,
        "missing": missing,
        "exact_duplicates": exact_duplicates,
        "sample_values": sample_values,
    }

def normalize_for_dataframe(records):
    normalized = []
    for rec in records:
        new_rec = {}
        for k, v in rec.items():
            if isinstance(v, dict) or isinstance(v, list):
                new_rec[k] = json.dumps(v)  # convertit dict/list en string
            else:
                new_rec[k] = v
        normalized.append(new_rec)
    return normalized

# ---------- Date conversion ----------
DATE_FMT = "%Y-%m-%d %H:%M:%S"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")




def convert_date_fields(records):
    """
    Convertit en-place les champs indiqués (ou auto-detectés) de string -> datetime.
    - date_fields : liste de noms de champs top-level à convertir
    - auto_parse : si True, convertit toute string correspondant à DATE_RE
    Retourne (records, n_converted, n_failed)
    """
    n_converted = 0
    n_failed = 0
    for rec in records:
        for k, v in list(rec.items()):
                if v is None:
                    continue
                # uniquement top-level strings
                if isinstance(v, str) and DATE_RE.match(v.strip()):
                    s = v.strip()
                    try:
                        rec[k] = datetime.strptime(s, DATE_FMT)
                        n_converted += 1
                    except Exception:
                        try:
                            rec[k] = date_parser.parse(s)
                            n_converted += 1
                        except Exception:
                            n_failed += 1
    if n_failed:
        print(f"Warning: {n_failed} date conversions failed (see examples in logs)")
    return records, n_converted, n_failed

def analyze_mongo_collection(coll):
    docs = list(coll.find())
    # Convert ObjectId en string
    for d in docs:
        if "_id" in d:
            d["_id"] = str(d["_id"])
    # Normaliser dict/list
    normalized_docs = normalize_for_dataframe(docs)
    return analyze_records_basic(normalized_docs), docs

def compare_s3_vs_mongo(s3_report, mongo_report):
    s_fields = set(s3_report["fields"])
    m_fields = set(mongo_report["fields"])
    fields_only_in_s3 = sorted(list(s_fields - m_fields))
    fields_only_in_mongo = sorted(list(m_fields - s_fields))
    dtype_mismatches = {}
    for f in s3_report["fields"]:
        sdt = s3_report["dtypes"].get(f)
        mdt = mongo_report["dtypes"].get(f)
        if sdt != mdt:
            dtype_mismatches[f] = {"s3_dtype": sdt, "mongo_dtype": mdt}
    return {
        "fields_only_in_s3": fields_only_in_s3,
        "fields_only_in_mongo": fields_only_in_mongo,
        "dtype_mismatches": dtype_mismatches
    }

# ---------- Main processing ----------
def process_file(s3, bucket, key, db):
    print(f"\n--- Processing S3 object: {key}")
    filename = key.split("/")[-1]
    collection_name = re.sub(r"\.json$", "", filename, flags=re.IGNORECASE)

    records = read_json_from_s3(s3, bucket, key)
    
    # Convert date fields if requested
    records, n_conv, n_fail = convert_date_fields(records)
    print(f"Converted {n_conv} date values (failed: {n_fail})")

    # S3 analysis
    records_for_analysis = normalize_for_dataframe(records)
    s3_report = analyze_records_basic(records_for_analysis)

    # Mongo analysis
    coll = db[collection_name]
    mongo_report, _ = analyze_mongo_collection(coll)

    # Compare
    comparison = compare_s3_vs_mongo(s3_report, mongo_report)

    # Check duplicates on key fields
    duplicate_key_checks = {}
    for candidate_key in ("id", "ID", "Id", "_id"):
        if candidate_key in s3_report["fields"]:
            ids = [r.get(candidate_key) for r in records]
            c = Counter(ids)
            dup = {k: v for k, v in c.items() if k is not None and v > 1}
            duplicate_key_checks[candidate_key] = {"duplicate_count": sum(v-1 for v in dup.values()), "examples": list(dup.items())[:5]}

    # Missing value comparison
    missing_comparison = {}
    for f in set(s3_report["fields"]) | set(mongo_report["fields"]):
        s_missing = s3_report["missing"].get(f, None)
        m_missing = mongo_report["missing"].get(f, None)
        missing_comparison[f] = {"s3_missing": s_missing, "mongo_missing": m_missing}

    report = {
        "s3_key": key,
        "collection": collection_name,
        "n_rows_s3": s3_report["n_rows"],
        "n_rows_mongo": mongo_report["n_rows"],
        "s3_report": s3_report,
        "mongo_report": mongo_report,
        "comparison": comparison,
        "duplicate_key_checks": duplicate_key_checks,
        "missing_comparison": missing_comparison,
    }

    return report


# ---------- Main ----------
def main(args):
    s3 = s3_client(args.aws_access_key, args.aws_secret_key, args.region)
    keys = list_json_keys(s3, args.bucket, args.prefix)
    if not keys:
        print("Aucun fichier JSON trouvé sous", f"s3://{args.bucket}/{args.prefix}")
        return

    client = connect_mongo(args.mongo_uri)

    db = client[args.mongo_db]

    reports = []
    for key in keys:
        try:
            rep = process_file(s3, args.bucket, key, db)
            pprint({
                "file": rep["s3_key"],
                "collection": rep["collection"],
                "rows_s3": rep["n_rows_s3"],
                "rows_mongo": rep["n_rows_mongo"],
                "fields_diff": rep["comparison"],
                "duplicate":rep["duplicate_key_checks"],
                "missing":rep["missing_comparison"]
            })
            reports.append(rep)
        except Exception as e:
            print(f"Erreur lors du traitement de {key}: {e}")
            reports.append({"s3_key": key, "error": str(e)})

    # Write report
    with open("integrity_report.json", "w", encoding="utf-8") as f:
        json.dump(reports, f, default=str, indent=2, ensure_ascii=False)
    print("\nRapport écrit dans integrity_report.json")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate JSON from S3 to MongoDB with integrity checks")
    parser.add_argument("--aws-access-key", required=True)
    parser.add_argument("--aws-secret-key", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--mongo-uri", required=True)
    parser.add_argument("--mongo-db", default="weather_records")
    args = parser.parse_args()
    main(args)