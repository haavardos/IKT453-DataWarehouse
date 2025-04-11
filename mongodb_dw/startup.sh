#!/usr/bin/env bash
set -e

echo "Waiting for MongoDB to be ready..."
until nc -z mongodb 27017; do
  echo "MongoDB is unavailable - sleeping"
  sleep 2
done

echo "MongoDB is up - checking if data is already loaded..."

python3 - <<EOF
from pymongo import MongoClient
client = MongoClient("mongodb://mongodb:27017/")
db = client["movielens"]
if "movies_optimized" not in db.list_collection_names():
    print("Collection not found. Loading data...")
    import load_mongodb
else:
    print("Data already exists. Skipping load.")
EOF

echo "Starting Mongo API..."
python mongo_api.py