from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    #Change to your ip address
    bootstrap_servers='10.0.0.4:29092', 
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

producer.send("mongo_movies", message)
producer.flush()
print("Message sent.")
