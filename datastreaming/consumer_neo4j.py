import json
from kafka import KafkaConsumer
from neo4j import GraphDatabase

KAFKA_TOPIC = "neo4j_movies"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

def insert_movie(tx, movie):
    tx.run("""
        MERGE (m:Movie {movieId: $movieId})
        SET m.title=$title, m.genres=$genres,
            m.imdbId=$imdbId, m.tmdbId=$tmdbId,
            m.ratings=$ratings, m.tags=$tags
    """, **movie)

def consume():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    for message in consumer:
        movie = message.value
        print(f"Received: {movie}")
        
        movie_data = {
            "movieId": movie["movieId"],
            "title": movie["title"],
            "genres": movie["genres"],
            "imdbId": movie["links"]["imdbId"],
            "tmdbId": movie["links"]["tmdbId"],
            "ratings": json.dumps(movie["ratings"]),
            "tags": json.dumps(movie["tags"]),
        }
        
        with driver.session() as session:
            session.execute_write(insert_movie, movie_data)
            print("Movie inserted into Neo4j.")

if __name__ == "__main__":
    consume()
