import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
from tqdm import tqdm
import os
import time

# === CONFIGURATION ===
# For local Docker PostgreSQL container
SUPABASE_HOST = os.getenv("SUPABASE_DB_HOST", "postgres_supabase")  
SUPABASE_DB = os.getenv("SUPABASE_DB_NAME", "postgres")
SUPABASE_USER = os.getenv("SUPABASE_DB_USER", "postgres")
SUPABASE_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "evtoqEb6YUflIeC0")
SUPABASE_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

MOVIES_CSV_PATH = "movielens/movies.csv"
RATINGS_CSV_PATH = "movielens/ratings.csv"
TAGS_CSV_PATH = "movielens/tags.csv"
LINKS_CSV_PATH = "movielens/links.csv"

BATCH_SIZE = 1000

def get_connection():
    """Establish connection to the Supabase PostgreSQL database"""
    # Add retry logic for Docker environment (containers may start at different times)
    max_retries = 5
    retry_delay = 5 
    
    for attempt in range(max_retries):
        try:
            print(f"Connecting to PostgreSQL at {SUPABASE_HOST}:{SUPABASE_PORT}...")
            conn = psycopg2.connect(
                host=SUPABASE_HOST,
                database=SUPABASE_DB,
                user=SUPABASE_USER,
                password=SUPABASE_PASSWORD,
                port=SUPABASE_PORT
            )
            conn.autocommit = False
            print("Connection successful!")
            return conn
        except Exception as e:
            print(f"Connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    raise Exception("Failed to connect to the database after multiple attempts")

def create_schema(conn):
    """Create the star schema tables"""
    print("Creating schema...")
    with conn.cursor() as cur:
        # Create dimension tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id INT PRIMARY KEY
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_movie (
            movie_id INT PRIMARY KEY,
            title TEXT,
            genres TEXT
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            timestamp_id BIGINT PRIMARY KEY,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_link (
            movie_id INT PRIMARY KEY,
            imdb_id TEXT,
            tmdb_id TEXT
        )
        """)
        
        # Create dim_tag table without foreign key constraints first
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_tag (
            tag_id SERIAL PRIMARY KEY,
            user_id INT,
            movie_id INT,
            tag TEXT,
            timestamp_id BIGINT
        )
        """)
        
        # Create fact table without foreign key constraints first
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_ratings (
            rating_id SERIAL PRIMARY KEY,
            user_id INT,
            movie_id INT,
            timestamp_id BIGINT,
            rating NUMERIC(2,1)
        )
        """)
        
        # Create materialized view for average ratings
        cur.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS avg_rating_per_movie AS
        SELECT 
            m.movie_id,
            m.title,
            AVG(r.rating) as avg_rating,
            COUNT(r.rating_id) as num_ratings
        FROM dim_movie m
        JOIN fact_ratings r ON m.movie_id = r.movie_id
        GROUP BY m.movie_id, m.title
        """)
        
        conn.commit()
        print("Schema created successfully!")

def load_users(conn, ratings_df, tags_df):
    """Load unique users into dim_user"""
    print("Loading users...")
    
    # Get unique user IDs from both ratings and tags
    user_ids = pd.concat([ratings_df['userId'], tags_df['userId']]).unique()
    
    with conn.cursor() as cur:
        batch = []
        for user_id in tqdm(user_ids):
            batch.append((int(user_id),))
            
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, "INSERT INTO dim_user (user_id) VALUES (%s) ON CONFLICT DO NOTHING", batch)
                conn.commit()
                batch = []
        
        # Insert any remaining users
        if batch:
            execute_batch(cur, "INSERT INTO dim_user (user_id) VALUES (%s) ON CONFLICT DO NOTHING", batch)
            conn.commit()

def load_movies(conn, movies_df):
    """Load movies into dim_movie"""
    print("Loading movies...")
    
    with conn.cursor() as cur:
        batch = []
        for _, row in tqdm(movies_df.iterrows(), total=len(movies_df)):
            batch.append((
                int(row['movieId']),
                row['title'],
                row['genres']
            ))
            
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, "INSERT INTO dim_movie (movie_id, title, genres) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", batch)
                conn.commit()
                batch = []
        
        # Insert any remaining movies
        if batch:
            execute_batch(cur, "INSERT INTO dim_movie (movie_id, title, genres) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", batch)
            conn.commit()

def load_links(conn, links_df):
    """Load links into dim_link"""
    print("Loading links...")
    
    with conn.cursor() as cur:
        batch = []
        for _, row in tqdm(links_df.iterrows(), total=len(links_df)):
            imdb_id = str(row['imdbId']) if pd.notna(row['imdbId']) else None
            tmdb_id = str(row['tmdbId']) if pd.notna(row['tmdbId']) else None
            
            batch.append((
                int(row['movieId']),
                imdb_id,
                tmdb_id
            ))
            
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, "INSERT INTO dim_link (movie_id, imdb_id, tmdb_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", batch)
                conn.commit()
                batch = []
        
        # Insert any remaining links
        if batch:
            execute_batch(cur, "INSERT INTO dim_link (movie_id, imdb_id, tmdb_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", batch)
            conn.commit()

def load_times(conn, ratings_df, tags_df):
    """Load unique timestamps into dim_time"""
    print("Loading time dimension...")
    
    # Combine timestamps from both ratings and tags
    timestamps = pd.concat([ratings_df['timestamp'], tags_df['timestamp']]).unique()
    
    with conn.cursor() as cur:
        batch = []
        for ts in tqdm(timestamps):
            dt = datetime.utcfromtimestamp(ts)
            batch.append((
                int(ts),  # timestamp_id
                dt.year,  # year
                dt.month, # month
                dt.day,   # day
                dt.hour   # hour
            ))
            
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, "INSERT INTO dim_time (timestamp_id, year, month, day, hour) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", batch)
                conn.commit()
                batch = []
        
        # Insert any remaining timestamps
        if batch:
            execute_batch(cur, "INSERT INTO dim_time (timestamp_id, year, month, day, hour) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", batch)
            conn.commit()

def load_ratings(conn, ratings_df):
    """Load ratings into fact_ratings"""
    print("Loading ratings...")
    
    with conn.cursor() as cur:
        batch = []
        for _, row in tqdm(ratings_df.iterrows(), total=len(ratings_df)):
            batch.append((
                int(row['userId']),
                int(row['movieId']),
                int(row['timestamp']),
                float(row['rating'])
            ))
            
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, """
                    INSERT INTO fact_ratings (user_id, movie_id, timestamp_id, rating) 
                    VALUES (%s, %s, %s, %s)
                """, batch)
                conn.commit()
                batch = []
        
        # Insert any remaining ratings
        if batch:
            execute_batch(cur, """
                INSERT INTO fact_ratings (user_id, movie_id, timestamp_id, rating) 
                VALUES (%s, %s, %s, %s)
            """, batch)
            conn.commit()

def load_tags(conn, tags_df):
    """Load tags into dim_tag"""
    print("Loading tags...")
    
    with conn.cursor() as cur:
        batch = []
        for _, row in tqdm(tags_df.iterrows(), total=len(tags_df)):
            batch.append((
                int(row['userId']),
                int(row['movieId']),
                row['tag'],
                int(row['timestamp'])
            ))
            
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, """
                    INSERT INTO dim_tag (user_id, movie_id, tag, timestamp_id) 
                    VALUES (%s, %s, %s, %s)
                """, batch)
                conn.commit()
                batch = []
        
        # Insert any remaining tags
        if batch:
            execute_batch(cur, """
                INSERT INTO dim_tag (user_id, movie_id, tag, timestamp_id) 
                VALUES (%s, %s, %s, %s)
            """, batch)
            conn.commit()

def refresh_materialized_views(conn):
    """Refresh materialized views"""
    print("Refreshing materialized views...")
    
    with conn.cursor() as cur:
        try:
            cur.execute("REFRESH MATERIALIZED VIEW avg_rating_per_movie")
            conn.commit()
            print("Materialized views refreshed successfully!")
        except Exception as e:
            print(f"Error refreshing materialized view: {e}")
            conn.rollback()

def execute_batch(cur, query, args_list):
    """Helper function to execute a batch of similar queries"""
    for args in args_list:
        try:
            cur.execute(query, args)
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Query: {query}")
            print(f"Args: {args}")
            raise

def main():
    start_time = time.time()
    try:
        # Load CSV files
        print("Loading CSV files...")
        try:
            movies_df = pd.read_csv(MOVIES_CSV_PATH)
            ratings_df = pd.read_csv(RATINGS_CSV_PATH)
            tags_df = pd.read_csv(TAGS_CSV_PATH)
            links_df = pd.read_csv(LINKS_CSV_PATH)
            print("CSV files loaded successfully!")
        except Exception as e:
            print(f"Error loading CSV files: {e}")
            print("Make sure the CSV files are in the correct location:")
            print(f"  Movies: {MOVIES_CSV_PATH}")
            print(f"  Ratings: {RATINGS_CSV_PATH}")
            print(f"  Tags: {TAGS_CSV_PATH}")
            print(f"  Links: {LINKS_CSV_PATH}")
            return
        
        # Connect to Supabase
        conn = get_connection()
        
        try:
            # Create schema
            create_schema(conn)
            
            # Load dimension tables
            load_users(conn, ratings_df, tags_df)
            load_movies(conn, movies_df)
            load_times(conn, ratings_df, tags_df)
            load_links(conn, links_df)
            
            # Load fact table and tags
            load_ratings(conn, ratings_df)
            load_tags(conn, tags_df)
            
            # Refresh materialized views
            refresh_materialized_views(conn)
            
            print("Data loading completed successfully!")
            
        except Exception as e:
            print(f"Error during data loading: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")
            end_time = time.time()
            print(f"Total loading time: {round(end_time - start_time, 2)} seconds")
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()