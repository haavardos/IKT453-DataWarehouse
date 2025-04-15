import time
import json
import requests
from kafka import KafkaProducer
from pymongo import MongoClient

# === CONFIG ===
API_URL = "http://localhost:5000"
MOVIE_ID = 99999
KAFKA_TOPIC = "mongo_movies"
KAFKA_BOOTSTRAP_SERVER = "localhost:29092"
MONGO_URI = "mongodb://localhost:27017/"

# === FUNCTIONS ===

def test_all_endpoints(movie_id=1, user_id=1, runs=3):
    endpoints = {
        "GET /movies": "movies",
        "GET /movies/{movieId}": f"movies/{movie_id}",
        "GET /movies/{movieId}/ratings": f"movies/{movie_id}/ratings",
        "GET /movies/{movieId}/tags": f"movies/{movie_id}/tags",
        "GET /top-rated": "top-rated",
        "GET /most-popular": "most-popular",
        "GET /user-recommendations/{userId}": f"user-recommendations/{user_id}",
        "GET /avg-rating-by-genre": "avg-rating-by-genre"
    }

    print(f"Endpoint timing test (average over {runs} runs):")
    for label, endpoint in endpoints.items():
        times = []
        for _ in range(runs):
            start = time.time()
            try:
                r = requests.get(f"{API_URL}/{endpoint}")
                r.raise_for_status()
                times.append(time.time() - start)
            except Exception as e:
                print(f"Error on {label}: {e}")
        avg_time = sum(times) / len(times) if times else 0
        print(f"{label:<40}: {avg_time:.3f}s")

def send_kafka_message(MOVIE_ID):
    movie = {
        "movieId": MOVIE_ID,
        "title": "Kafka Speed Test",
        "genres": ["Test"],
        "ratings": [{"userId": 1, "rating": 4.5, "date": "2025-04-15"}],
        "tags": [{"userId": 1, "tag": "test", "date": "2025-04-15"}],
        "links": {"imdbId": "tt9999999", "tmdbId": "99999"}
    }
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(KAFKA_TOPIC, movie)
    producer.flush()
    print("Sent movie to Kafka topic.")
    return MOVIE_ID

def poll_api_for_movie(movie_id, timeout=30):
    while timeout > 0:
        try:
            res = requests.get(f"{API_URL}/movies/{movie_id}")
            if res.status_code == 200:
                return True
        except:
            pass
        time.sleep(1)
        timeout -= 1
    return None

def measure_mongo_db_size():
    client = MongoClient(MONGO_URI)
    stats = client["movielens"].command("dbstats")
    size_mb = stats["storageSize"] / 1024**2
    return round(size_mb, 2)

# === MAIN SCRIPT ===

# 1. Measure Query Times
test_all_endpoints(movie_id=1, user_id=1)

# 2. Kafka Stream Time
print("\nKafka streaming test:")
start_time = time.time()
start_id = send_kafka_message(MOVIE_ID)
delay = poll_api_for_movie(start_id)
total_delay = time.time() - start_time

print(f"Kafka to API delay: {total_delay:.2f}s" if delay else "Movie not found in time.")

# 3. Database Size
size = measure_mongo_db_size()
print(f"MongoDB storage used: {size} MB")
