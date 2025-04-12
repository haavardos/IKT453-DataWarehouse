from kafka import KafkaConsumer
import json
import time
import os
import psycopg2
import psycopg2.extras
from datetime import datetime

# Configuration
KAFKA_TOPIC = "supabase_movies"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DB_HOST = os.getenv("SUPABASE_DB_HOST", "postgres_supabase")
DB_NAME = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "evtoqEb6YUflIeC0")
DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    api_version=(2, 0, 2),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="supabase-group"
)

def get_db_connection():
    """Create a connection to the Supabase PostgreSQL database"""
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            conn.autocommit = True
            print("Connection successful!")
            return conn
        except Exception as e:
            print(f"Connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    raise Exception("Failed to connect to the database after multiple attempts")

def process_message(msg):
    """Process a message from Kafka and insert into Supabase"""
    print("Received message:", msg)
    try:
        # Connect to database
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Extract message data
        movie_id = msg.get("movieId")
        title = msg.get("title")
        genres = msg.get("genres")
        ratings = msg.get("ratings", [])
        tags = msg.get("tags", [])
        links = msg.get("links", {})
        
        # Insert movie if it doesn't exist
        cur.execute("""
            INSERT INTO dim_movie (movie_id, title, genres)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id) DO UPDATE
            SET title = EXCLUDED.title, genres = EXCLUDED.genres
        """, (movie_id, title, ",".join(genres) if isinstance(genres, list) else genres))
        
        # Insert links if they exist
        if links:
            cur.execute("""
                INSERT INTO dim_link (movie_id, imdb_id, tmdb_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (movie_id) DO UPDATE
                SET imdb_id = EXCLUDED.imdb_id, tmdb_id = EXCLUDED.tmdb_id
            """, (movie_id, links.get("imdbId"), links.get("tmdbId")))
        
        # Process ratings
        for rating in ratings:
            user_id = rating.get("userId")
            rating_value = rating.get("rating")
            date_str = rating.get("date")
            
            # Create timestamp from date
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            timestamp = int(dt.timestamp())
            
            # Ensure user exists
            cur.execute("""
                INSERT INTO dim_user (user_id)
                VALUES (%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (user_id,))
            
            # Ensure timestamp exists
            cur.execute("""
                INSERT INTO dim_time (timestamp_id, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (timestamp_id) DO NOTHING
            """, (timestamp, dt.year, dt.month, dt.day, dt.hour))
            
            # Insert rating
            cur.execute("""
                INSERT INTO fact_ratings (user_id, movie_id, timestamp_id, rating)
                VALUES (%s, %s, %s, %s)
            """, (user_id, movie_id, timestamp, rating_value))
        
        # Process tags
        for tag in tags:
            user_id = tag.get("userId")
            tag_text = tag.get("tag")
            date_str = tag.get("date")
            
            # Create timestamp from date
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            timestamp = int(dt.timestamp())
            
            # Ensure user exists
            cur.execute("""
                INSERT INTO dim_user (user_id)
                VALUES (%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (user_id,))
            
            # Ensure timestamp exists
            cur.execute("""
                INSERT INTO dim_time (timestamp_id, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (timestamp_id) DO NOTHING
            """, (timestamp, dt.year, dt.month, dt.day, dt.hour))
            
            # Insert tag
            cur.execute("""
                INSERT INTO dim_tag (user_id, movie_id, tag, timestamp_id)
                VALUES (%s, %s, %s, %s)
            """, (user_id, movie_id, tag_text, timestamp))
        
        # Refresh materialized view
        cur.execute("REFRESH MATERIALIZED VIEW avg_rating_per_movie")
        
        print(f"Successfully processed movie {movie_id}: {title}")
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error processing message: {e}")

print("Kafka-Supabase consumer started. Waiting for messages...")
for msg in consumer:
    process_message(msg.value)