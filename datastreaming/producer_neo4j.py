import json
from kafka import KafkaProducer

# === Configuration ===
KAFKA_TOPIC = "neo4j_movies"
KAFKA_BOOTSTRAP_SERVER = "10.0.0.7:29092"
MOVIE_ID = 99988

# === Kafka Message ===
movie_data = {
    "movieId": MOVIE_ID,
    "title": "Kafka Consumer Movie - Neo4j Edition",
    "genres": ["Test"],
    "ratings": [{"userId": 1, "rating": 4.5, "date": "2025-04-15"}],
    "tags": [{"userId": 1, "tag": "test", "date": "2025-04-15"}],
    "links": {"imdbId": "tt9999999", "tmdbId": "99999"}
}

# === Producer ===
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send(KAFKA_TOPIC, movie_data)
producer.flush()

print(f"Sent movie (ID: {MOVIE_ID}) to Kafka topic '{KAFKA_TOPIC}'.")

