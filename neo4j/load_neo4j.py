import pandas as pd
from neo4j import GraphDatabase
from datetime import datetime
from tqdm import tqdm
import time

# === CONFIGURATION ===
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

MOVIES_CSV_PATH = "../movielens/movies.csv"
RATINGS_CSV_PATH = "../movielens/ratings.csv"
TAGS_CSV_PATH = "../movielens/tags.csv"
LINKS_CSV_PATH = "../movielens/links.csv"

BATCH_SIZE = 1000


def create_indexes_and_constraints(session):
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.movieId IS UNIQUE")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")


def clear_database(session):
    session.run("MATCH (n) DETACH DELETE n")


def load_movies(session, movies_df, links_df):
    links_dict = links_df.set_index("movieId").to_dict("index")

    movie_batch = []
    genre_batch = set()
    relationship_batch = []

    for _, row in tqdm(movies_df.iterrows(), total=len(movies_df), desc="Movies"):
        movie_id = row["movieId"]
        title = row["title"]
        genres = row["genres"].split("|") if pd.notna(row["genres"]) else []
        imdb_id = tmdb_id = None

        if movie_id in links_dict:
            imdb_id = links_dict[movie_id].get("imdbId")
            tmdb_id = links_dict[movie_id].get("tmdbId")

        # Extract year from title
        year = None
        if "(" in title and title.strip().endswith(")"):
            try:
                year = int(title.strip()[-5:-1])
            except ValueError:
                pass

        # Movie MERGE query
        movie_query = (
            "MERGE (m:Movie {movieId: $movieId}) "
            "SET m.title = $title, m.genres = $genres, "
            "m.imdbId = $imdbId, m.tmdbId = $tmdbId, m.year = $year"
        )
        movie_params = {
            "movieId": int(movie_id),
            "title": title,
            "genres": genres,
            "imdbId": str(imdb_id) if pd.notna(imdb_id) else None,
            "tmdbId": str(tmdb_id) if pd.notna(tmdb_id) else None,
            "year": year,
        }
        movie_batch.append((movie_query, movie_params))

        for genre in genres:
            genre = genre.strip()
            if genre:
                genre_batch.add(genre)
                relationship_batch.append({
                    "movieId": int(movie_id),
                    "genreName": genre
                })

        if len(movie_batch) >= BATCH_SIZE:
            _run_batch(session, movie_batch)
            _create_genres_and_relationships(session, genre_batch, relationship_batch)
            movie_batch = []
            genre_batch = set()
            relationship_batch = []

    if movie_batch:
        _run_batch(session, movie_batch)
        _create_genres_and_relationships(session, genre_batch, relationship_batch)


def _create_genres_and_relationships(session, genre_set, relationship_list):
    with session.begin_transaction() as tx:
        for genre in genre_set:
            tx.run("MERGE (:Genre {name: $name})", name=genre)
        for rel in relationship_list:
            tx.run(
                """
                MATCH (m:Movie {movieId: $movieId})
                MATCH (g:Genre {name: $genreName})
                MERGE (m)-[:IN_GENRE]->(g)
                """,
                movieId=rel["movieId"],
                genreName=rel["genreName"]
            )


def load_ratings(session, ratings_df):
    ratings_df["date"] = ratings_df["timestamp"].apply(
        lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
    )

    batch = []
    for _, row in tqdm(ratings_df.iterrows(), total=len(ratings_df), desc="Ratings"):
        user_id = int(row["userId"])
        movie_id = int(row["movieId"])
        rating = float(row["rating"])
        date_str = row["date"]

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

    if batch:
        _run_batch(session, batch)


def load_tags(session, tags_df):
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

    if batch:
        _run_batch(session, batch)


def _run_batch(session, batch):
    with session.begin_transaction() as tx:
        for query, params in batch:
            tx.run(query, **params)


def main():
    start = time.time()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    movies = pd.read_csv(MOVIES_CSV_PATH)
    ratings = pd.read_csv(RATINGS_CSV_PATH)
    tags = pd.read_csv(TAGS_CSV_PATH)
    links = pd.read_csv(LINKS_CSV_PATH)

    with driver.session() as session:
        clear_database(session)
        create_indexes_and_constraints(session)
        load_movies(session, movies, links)
        load_ratings(session, ratings)
        load_tags(session, tags)

    end = time.time()
    print(f"Neo4j loading took {end - start:.2f} seconds")


if __name__ == "__main__":
    main()
