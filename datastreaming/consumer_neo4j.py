import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase

# --- Config ---
KAFKA_TOPIC = "neo4j_movies"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# --- Insert movie and relationships ---
def insert_full_movie(tx, movie):
    # Create movie node
    tx.run("""
        MERGE (m:Movie {movieId: $movieId})
        SET m.title = $title,
            m.genres = $genres,
            m.imdbId = $imdbId,
            m.tmdbId = $tmdbId
    """, movie)

    # Add ratings
    for r in movie.get("ratings", []):
        if r.get("userId") is not None and r.get("rating") is not None:
            tx.run("""
                MERGE (u:User {userId: $userId})
                MERGE (m:Movie {movieId: $movieId})
                MERGE (u)-[r:RATED]->(m)
                SET r.rating = $rating, r.date = $date
            """, {
                "userId": r["userId"],
                "movieId": movie["movieId"],
                "rating": r["rating"],
                "date": r.get("date")
            })

    # Add tags
    for t in movie.get("tags", []):
        if t.get("userId") is not None and t.get("tag"):
            tx.run("""
                MERGE (u:User {userId: $userId})
                MERGE (m:Movie {movieId: $movieId})
                MERGE (u)-[t:TAGGED]->(m)
                SET t.tag = $tag, t.date = $date
            """, {
                "userId": t["userId"],
                "movieId": movie["movieId"],
                "tag": t["tag"],
                "date": t.get("date")
            })
    

# --- Consume Kafka messages and insert into Neo4j ---
def consume():
    # Wait for Kafka to be available
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="neo4j-group"
            )
            break
        except NoBrokersAvailable:
            print(f"[Retry] Kafka not available yet. Attempt {attempt+1}/10...")
            time.sleep(5)
    else:
        print("Failed to connect to Kafka after retries.")
        return

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    print("Kafka-Neo4j consumer started.")
    for message in consumer:
        movie = message.value
        print(f"Received movie: {movie.get('title')}")

        movie_data = {
            "movieId": movie.get("movieId"),
            "title": movie.get("title"),
            "genres": movie.get("genres"),
            "imdbId": movie.get("links", {}).get("imdbId"),
            "tmdbId": movie.get("links", {}).get("tmdbId"),
            "ratings": movie.get("ratings", []),
            "tags": movie.get("tags", [])
        }

        try:
            with driver.session() as session:
                session.execute_write(insert_full_movie, movie_data)
                print(f" Movie {movie_data['movieId']} inserted.")
        except Exception as e:
            print(f"Failed to insert movie {movie_data['movieId']}: {e}")

# --- Entry Point ---
if __name__ == "__main__":
    consume()
