FROM python:3.12-slim
WORKDIR /app

# For MongoDB
COPY consumer_mongo.py .
COPY producer_mongo.py .

# For Supabase
COPY consumer_supabase.py .
COPY producer_supabase.py .

RUN pip install kafka-python pymongo psycopg2-binary
CMD ["python", "-u", "consumer_mongo.py"]
