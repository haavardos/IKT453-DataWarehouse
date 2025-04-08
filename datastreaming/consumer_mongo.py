from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
import os

KAFKA_TOPIC = "mongo_movies"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    api_version=(2, 0, 2),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="mongo-group"
)

mongo = MongoClient(MONGO_URI)
collection = mongo["movielens"]["movies_optimized"]

print("Kafka-Mongo consumer started. Waiting for messages...")
for msg in consumer:
    try:
        print("Inserting:", msg.value)
        collection.insert_one(msg.value)
    except Exception as e:
        print("Error inserting message:", e)
