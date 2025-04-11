#!/usr/bin/env bash
set -e

echo "Waiting for Neo4j to be ready..."
until nc -z neo4j 7687; do
  echo "Neo4j is unavailable - sleeping"
  sleep 2
done

echo "Neo4j is up - checking if data is already loaded..."

python - <<EOF
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
with driver.session() as session:
    result = session.run("MATCH (n) RETURN count(n) AS count").single()
    count = result["count"]
    if count == 0:
        print("Neo4j is empty. Loading data...")
        import load_neo4j
        load_neo4j.main()
    else:
        print("Data already exists. Skipping load.")
EOF

echo "Starting Neo4j Flask API..."
python neo4j_api.py
