import pandas as pd
from neo4j import GraphDatabase
from datetime import datetime
from tqdm import tqdm

# === CONFIGURATION ===
NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

MOVIES_CSV_PATH = "movielens/movies.csv"
RATINGS_CSV_PATH = "movielens/ratings.csv"
TAGS_CSV_PATH = "movielens/tags.csv"
LINKS_CSV_PATH = "movielens/links.csv"

BATCH_SIZE = 1000


def create_indexes_and_constraints(session):
    """
    Create indexes/constraints to speed up MERGE operations
    and ensure uniqueness for Movie and User nodes.
    """
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.movieId IS UNIQUE")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE")


def clear_database(session):
    """
    (Optional) Clear the entire database, removing all existing data.
    """
    session.run("MATCH (n) DETACH DELETE n")


def load_movies(session, movies_df, links_df):
    """
    Create (or merge) Movie nodes in Neo4j.
    Store movieId, title, genres, and any link info (imdbId, tmdbId).
    """
    links_dict = links_df.set_index("movieId").to_dict("index")

    batch = []
    for _, row in tqdm(movies_df.iterrows(), total=len(movies_df), desc="Movies"):
        movie_id = row["movieId"]
        title = row["title"]
        genres = row["genres"].split("|") if pd.notna(row["genres"]) else []

        imdb_id = None
        tmdb_id = None
        if movie_id in links_dict:
            imdb_id = links_dict[movie_id].get("imdbId")
            tmdb_id = links_dict[movie_id].get("tmdbId")

        # We'll batch the Cypher queries for better performance
        cypher_query = (
            "MERGE (m:Movie {movieId: $movieId}) "
            "SET m.title = $title, m.genres = $genres, m.imdbId = $imdbId, m.tmdbId = $tmdbId"
        )
        params = {
            "movieId": int(movie_id),
            "title": title,
            "genres": genres,
            "imdbId": str(imdb_id) if pd.notna(imdb_id) else None,
            "tmdbId": str(tmdb_id) if pd.notna(tmdb_id) else None,
        }
        batch.append((cypher_query, params))

        # Execute batch if it reached BATCH_SIZE
        if len(batch) >= BATCH_SIZE:
            _run_batch(session, batch)
            batch = []

    # Insert any remaining movies
    if batch:
        _run_batch(session, batch)


def load_ratings(session, ratings_df):
    """
    Create User nodes and RATED relationships to Movie nodes.
    We'll store rating and date on the relationship.
    """
    # Convert timestamp to 'YYYY-MM-DD'
    ratings_df["date"] = ratings_df["timestamp"].apply(
        lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
    )

    batch = []
    for _, row in tqdm(ratings_df.iterrows(), total=len(ratings_df), desc="Ratings"):
        user_id = int(row["userId"])
        movie_id = int(row["movieId"])
        rating = float(row["rating"])
        date_str = row["date"]  # 'YYYY-MM-DD'

        cypher_query = (
            "MERGE (u:User {userId: $userId}) "
            "MERGE (m:Movie {movieId: $movieId}) "
            "CREATE (u)-[r:RATED {rating: $rating, date: $date}]->(m)"
        )
        params = {
            "userId": user_id,
            "movieId": movie_id,
            "rating": rating,
            "date": date_str,
        }
        batch.append((cypher_query, params))

        if len(batch) >= BATCH_SIZE:
            _run_batch(session, batch)
            batch = []

    # Insert any remaining relationships
    if batch:
        _run_batch(session, batch)


def load_tags(session, tags_df):
    """
    Create TAGGED relationships to Movie nodes.
    We'll store tag and date on the relationship.
    """
    # Convert timestamp to 'YYYY-MM-DD'
    tags_df["date"] = tags_df["timestamp"].apply(
        lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
    )

    batch = []
    for _, row in tqdm(tags_df.iterrows(), total=len(tags_df), desc="Tags"):
        user_id = int(row["userId"])
        movie_id = int(row["movieId"])
        tag = row["tag"]
        date_str = row["date"]

        cypher_query = (
            "MERGE (u:User {userId: $userId}) "
            "MERGE (m:Movie {movieId: $movieId}) "
            "CREATE (u)-[t:TAGGED {tag: $tag, date: $date}]->(m)"
        )
        params = {
            "userId": user_id,
            "movieId": movie_id,
            "tag": tag,
            "date": date_str,
        }
        batch.append((cypher_query, params))

        if len(batch) >= BATCH_SIZE:
            _run_batch(session, batch)
            batch = []

    # Insert any remaining relationships
    if batch:
        _run_batch(session, batch)


def _run_batch(session, batch):
    """ Helper function to run a batch of queries in a single transaction. """
    with session.begin_transaction() as tx:
        for query, params in batch:
            tx.run(query, **params)


def main():
    # --- Connect to Neo4j ---
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    # --- Load CSV files into DataFrames ---
    movies = pd.read_csv(MOVIES_CSV_PATH)
    ratings = pd.read_csv(RATINGS_CSV_PATH)
    tags = pd.read_csv(TAGS_CSV_PATH)
    links = pd.read_csv(LINKS_CSV_PATH)

    with driver.session() as session:
        # (Optional) Clear the entire database
        clear_database(session)

        # Create constraints (will speed up merges and ensure unique IDs)
        create_indexes_and_constraints(session)

        # 1. Load Movies (including link info)
        load_movies(session, movies, links)

        # 2. Load Ratings (User -> [RATED] -> Movie)
        load_ratings(session, ratings)

        # 3. Load Tags (User -> [TAGGED] -> Movie)
        load_tags(session, tags)

    print("Finished loading Neo4j!")


if __name__ == "__main__":
    main()
