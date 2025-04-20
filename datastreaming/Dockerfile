FROM python:3.12-slim
WORKDIR /app

# For MongoDB
COPY consumer_mongo.py .
COPY producer_mongo.py .

# For Supabase
COPY consumer_supabase.py .
COPY producer_supabase.py .

# For Neo4j
COPY consumer_neo4j.py .
COPY producer_neo4j.py .

RUN pip install kafka-python pymongo psycopg2-binary neo4j
CMD ["python", "-u", "consumer_mongo.py"]
