from kafka import KafkaProducer
import json

producer = KafkaProducer(
    # Use the externally accessible address and port
    bootstrap_servers='localhost:29092',  # Or your machine's IP instead of localhost
    api_version=(2, 0, 2),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

message = {
    "movieId": 199999,
    "title": "Kafka the Movie",
    "genres": ["Fantasy", "Mystery"],
    "ratings": [
        {"userId": 1, "rating": 4.5, "date": "2025-04-08"}
    ],
    "tags": [
        {"userId": 1, "tag": "existential", "date": "2025-04-08"}
    ],
    "links": {
        "imdbId": "tt9999999",
        "tmdbId": "99999"
    }
}

try:
    producer.send("supabase_movies", message)
    producer.flush()
    print("Message sent to Supabase topic.")
except Exception as e:
    print(f"Error sending message: {e}")