import time
import json
import requests
from kafka import KafkaProducer
import psycopg2
import os

# === CONFIG ===
API_URL = "http://localhost:5050"
MOVIE_ID = 99999
KAFKA_TOPIC = "supabase_movies"
KAFKA_BOOTSTRAP_SERVER = "localhost:29092"

# PostgreSQL config
DB_HOST = os.getenv("SUPABASE_DB_HOST", "localhost")
DB_NAME = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "yourpassword")
DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

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

def measure_postgres_db_size():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute("SELECT pg_database_size(%s);", (DB_NAME,))
        size_bytes = cur.fetchone()[0]
        size_mb = size_bytes / (1024 ** 2)
        return round(size_mb, 2)
    except Exception as e:
        print(f"Error getting DB size: {e}")
        return -1

# === MAIN SCRIPT ===

# 1. Measure query times
test_all_endpoints(movie_id=1, user_id=1)

# 2. Kafka stream time
print("\nKafka streaming test:")
start_time = time.time()
start_id = send_kafka_message(MOVIE_ID)
delay = poll_api_for_movie(start_id)
total_delay = time.time() - start_time

print(f"Kafka to API delay: {total_delay:.2f}s" if delay else "Movie not found in time.")

# 3. Database size
size = measure_postgres_db_size()
print(f"PostgreSQL (Supabase) storage used: {size} MB")
