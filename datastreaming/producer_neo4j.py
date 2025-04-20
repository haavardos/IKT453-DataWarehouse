from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

message = {
    "movieId": 123456789,
    "title": "Kafka Test Movie - Neo4j",
    "genres": ["Drama", "Test"],
    "ratings": [{"userId": 1, "rating": 4.0, "date": "2025-04-15"}],
    "tags": [{"userId": 1, "tag": "test", "date": "2025-04-15"}],
    "links": {
        "imdbId": "tt1234567",
        "tmdbId": "123456"
    }
}

producer.send("neo4j_movies", message)
producer.flush()
print("Test movie sent to Kafka (neo4j_movies)")
