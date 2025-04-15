import time
import json
import requests
from kafka import KafkaProducer
from neo4j import GraphDatabase

# === CONFIG ===
API_URL = "http://localhost:6060"
MOVIE_ID = 99988
KAFKA_TOPIC = "neo4j_movies"
KAFKA_BOOTSTRAP_SERVER = "localhost:29092"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# === FUNCTIONS ===

def send_kafka_message(movie_id):
    movie = {
        "movieId": movie_id,
        "title": "Kafka Speed Test - Neo4j",
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
    return movie_id

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

def measure_neo4j_db_size():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        size_info = session.run("CALL db.stats.retrieve('GRAPH COUNTS') YIELD data RETURN data").single()
        return size_info["data"]

# === MAIN SCRIPT ===
print("\nKafka streaming test (Neo4j):")
start_time = time.time()
start_id = send_kafka_message(MOVIE_ID)
delay = poll_api_for_movie(start_id)
total_delay = time.time() - start_time

print(f"Kafka to API delay: {total_delay:.2f}s" if delay else "Movie not found in time.")

# Check Database Size/Stats
size = measure_neo4j_db_size()
print(f"Neo4j Database Stats: {size}")
