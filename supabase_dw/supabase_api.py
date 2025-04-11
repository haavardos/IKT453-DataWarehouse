from flask import Flask, jsonify
import os
import psycopg2
import psycopg2.extras
import time
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Supabase PostgreSQL connection info
DB_HOST = os.getenv("SUPABASE_DB_HOST", "postgres_supabase")
DB_NAME = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "evtoqEb6YUflIeC0")
DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

def get_db_connection():
    """Create a connection to the Supabase PostgreSQL database with retry logic"""
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT} (attempt {attempt+1}/{max_retries})...")
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                sslmode='disable',
                connect_timeout=10
            )
            conn.autocommit = True
            logger.info("Database connection successful!")
            return conn
        except Exception as e:
            logger.error(f"Connection attempt {attempt+1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    raise Exception("Failed to connect to the database after multiple attempts")

@app.route('/')
def home():
    logger.info("Home endpoint called")
    return jsonify({"message": "Welcome to the MovieLens API (Supabase)!", "status": "API is running"})

@app.route('/healthcheck')
def healthcheck():
    """Simple endpoint to check if API is running and database is accessible"""
    logger.info("Healthcheck endpoint called")
    
    # Check if the API is running
    api_status = "OK"
    
    # Check database connection
    db_status = "Unknown"
    db_error = None
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        db_status = "OK"
    except Exception as e:
        db_status = "Error"
        db_error = str(e)
        logger.error(f"Database healthcheck failed: {str(e)}")
    
    return jsonify({
        "api_status": api_status,
        "database_status": db_status,
        "database_error": db_error,
        "database_host": DB_HOST,
        "database_port": DB_PORT,
        "database_name": DB_NAME,
        "database_user": DB_USER
    })

@app.route('/movies', methods=['GET'])
def get_movies():
    """Get a list of movies (limited to 10)"""
    logger.info("Get movies endpoint called")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT movie_id as "movieId", title, genres
            FROM dim_movie
            LIMIT 10
        """)
        
        movies = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify(movies)
    except Exception as e:
        logger.error(f"Error in get_movies: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/movies/<int:movie_id>', methods=['GET'])
def get_movie_by_id(movie_id):
    """Get details for a specific movie by ID"""
    logger.info(f"Get movie by ID endpoint called for movie_id={movie_id}")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get movie details
        cur.execute("""
            SELECT movie_id as "movieId", title, genres
            FROM dim_movie
            WHERE movie_id = %s
        """, (movie_id,))
        
        movie = cur.fetchone()
        
        if not movie:
            cur.close()
            conn.close()
            return jsonify({"error": "Movie not found"}), 404
        
        # Get ratings for this movie
        cur.execute("""
            SELECT 
                fr.user_id as "userId", 
                fr.rating, 
                fr.timestamp_id as timestamp
            FROM fact_ratings fr
            WHERE fr.movie_id = %s
        """, (movie_id,))
        
        ratings = cur.fetchall()
        
        # Format timestamp to date if needed
        for rating in ratings:
            if 'timestamp' in rating and rating['timestamp']:
                try:
                    # Convert to string format if needed
                    rating['date'] = str(rating['timestamp'])
                    # You can delete the timestamp field if you want
                    del rating['timestamp']
                except Exception as e:
                    logger.warning(f"Error formatting timestamp: {str(e)}")
        
        # Get tags for this movie
        cur.execute("""
            SELECT 
                t.user_id as "userId", 
                t.tag,
                t.timestamp_id as timestamp
            FROM dim_tag t
            WHERE t.movie_id = %s
        """, (movie_id,))
        
        tags = cur.fetchall()
        
        # Format timestamp for tags if needed
        for tag in tags:
            if 'timestamp' in tag and tag['timestamp']:
                try:
                    tag['date'] = str(tag['timestamp'])
                    del tag['timestamp']
                except Exception as e:
                    logger.warning(f"Error formatting timestamp: {str(e)}")
        
        # Get links for this movie
        cur.execute("""
            SELECT imdb_id as "imdbId", tmdb_id as "tmdbId"
            FROM dim_link
            WHERE movie_id = %s
        """, (movie_id,))
        
        links = cur.fetchone() or {}
        
        # Calculate average rating and count
        rating_values = [r["rating"] for r in ratings if r["rating"] is not None]
        avg_rating = round(sum(rating_values) / len(rating_values), 2) if rating_values else None
        
        # Build the complete response
        movie["ratings"] = ratings
        movie["tags"] = tags
        movie["links"] = links
        movie["avgRating"] = avg_rating
        movie["ratingCount"] = len(rating_values)
        
        cur.close()
        conn.close()
        
        return jsonify(movie)
    except Exception as e:
        logger.error(f"Error in get_movie_by_id: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/movies/<int:movie_id>/ratings', methods=['GET'])
def get_movie_ratings(movie_id):
    """Get all ratings for a specific movie"""
    logger.info(f"Get movie ratings endpoint called for movie_id={movie_id}")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT 
                fr.user_id as "userId", 
                fr.rating, 
                fr.timestamp_id as timestamp
            FROM fact_ratings fr
            WHERE fr.movie_id = %s
        """, (movie_id,))
        
        ratings = cur.fetchall()
        
        # Format timestamp if needed
        for rating in ratings:
            if 'timestamp' in rating and rating['timestamp']:
                try:
                    rating['date'] = str(rating['timestamp'])
                    del rating['timestamp']
                except Exception as e:
                    logger.warning(f"Error formatting timestamp: {str(e)}")
        
        cur.close()
        conn.close()
        
        return jsonify(ratings)
    except Exception as e:
        logger.error(f"Error in get_movie_ratings: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/movies/<int:movie_id>/tags', methods=['GET'])
def get_movie_tags(movie_id):
    """Get all tags for a specific movie"""
    logger.info(f"Get movie tags endpoint called for movie_id={movie_id}")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT 
                t.user_id as "userId", 
                t.tag, 
                t.timestamp_id as timestamp
            FROM dim_tag t
            WHERE t.movie_id = %s
        """, (movie_id,))
        
        tags = cur.fetchall()
        
        # Format timestamp if needed
        for tag in tags:
            if 'timestamp' in tag and tag['timestamp']:
                try:
                    tag['date'] = str(tag['timestamp'])
                    del tag['timestamp']
                except Exception as e:
                    logger.warning(f"Error formatting timestamp: {str(e)}")
        
        cur.close()
        conn.close()
        
        return jsonify(tags)
    except Exception as e:
        logger.error(f"Error in get_movie_tags: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/top-rated', methods=['GET'])
def get_top_rated():
    """Get top rated movies"""
    logger.info("Get top rated movies endpoint called")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT 
                movie_id as "movieId", 
                title, 
                avg_rating as "avgRating"
            FROM avg_rating_per_movie
            ORDER BY avg_rating DESC
            LIMIT 10
        """)
        
        top_rated = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify(top_rated)
    except Exception as e:
        logger.error(f"Error in get_top_rated: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/most-popular', methods=['GET'])
def get_most_popular():
    """Get most popular movies (by number of ratings)"""
    logger.info("Get most popular movies endpoint called")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT 
                dm.movie_id as "movieId", 
                dm.title, 
                COUNT(fr.rating_id) as "ratingCount"
            FROM dim_movie dm
            JOIN fact_ratings fr ON dm.movie_id = fr.movie_id
            GROUP BY dm.movie_id, dm.title
            ORDER BY "ratingCount" DESC
            LIMIT 10
        """)
        
        most_popular = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify(most_popular)
    except Exception as e:
        logger.error(f"Error in get_most_popular: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/user-recommendations/<int:user_id>', methods=['GET'])
def get_user_recommendations(user_id):
    """Get recommendations based on user's highly-rated movies"""
    logger.info(f"Get user recommendations endpoint called for user_id={user_id}")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT 
                dm.movie_id as "movieId", 
                dm.title,
                ARRAY_AGG(fr.rating) as "highRatings"
            FROM dim_movie dm
            JOIN fact_ratings fr ON dm.movie_id = fr.movie_id
            WHERE fr.user_id = %s AND fr.rating >= 4
            GROUP BY dm.movie_id, dm.title
        """, (user_id,))
        
        results = cur.fetchall()
        
        if not results:
            cur.close()
            conn.close()
            return jsonify({"error": "User has no highly-rated movies"}), 404
        
        # Process the results to match the format from other APIs
        cleaned = []
        for movie in results:
            rating_vals = movie["highRatings"]
            if rating_vals:
                cleaned.append({
                    "movieId": movie["movieId"],
                    "title": movie["title"],
                    "numHighRatings": len(rating_vals),
                    "avgHighRating": round(sum(rating_vals) / len(rating_vals), 2)
                })
        
        cur.close()
        conn.close()
        
        if not cleaned:
            return jsonify({"error": "User has no highly-rated movies"}), 404
        
        return jsonify(cleaned)
    except Exception as e:
        logger.error(f"Error in get_user_recommendations: {str(e)}")
        return jsonify({"error": str(e)}), 500

def check_tables_exist():
    """Check if required tables exist in the database"""
    logger.info("Checking if required tables exist in the database...")
    
    required_tables = [
        "dim_movie", "dim_user", "dim_time", "dim_tag", "dim_link", 
        "fact_ratings", "avg_rating_per_movie"
    ]
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check each table
        existing_tables = []
        missing_tables = []
        
        for table in required_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
            """, (table,))
            
            exists = cur.fetchone()[0]
            if exists:
                existing_tables.append(table)
            else:
                missing_tables.append(table)
        
        cur.close()
        conn.close()
        
        if missing_tables:
            logger.warning(f"Missing tables: {', '.join(missing_tables)}")
        else:
            logger.info("All required tables exist")
        
        return existing_tables, missing_tables
    except Exception as e:
        logger.error(f"Error checking tables: {str(e)}")
        return [], required_tables

def test_connection():
    try:
        logger.info("Testing database connection...")
        logger.info(f"Host: {DB_HOST}")
        logger.info(f"Database: {DB_NAME}")
        logger.info(f"User: {DB_USER}")
        
        conn = get_db_connection()
        logger.info("Connection established!")
        
        logger.info("Querying database tables...")
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cur.fetchall()
        
        logger.info("Connection successful!")
        logger.info("Tables in your database:")
        for table in tables:
            logger.info(f"- {table[0]}")
        
        # Check if required tables exist
        existing_tables, missing_tables = check_tables_exist()
        
        cur.close()
        conn.close()
        return True, existing_tables, missing_tables
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        logger.error("Check your database credentials and network connectivity")
        return False, [], []

if __name__ == '__main__':
    try:
        connection_ok, existing_tables, missing_tables = test_connection()
        
        if connection_ok:
            logger.info("Database connection verified, starting API server...")
            if missing_tables:
                logger.warning(f"Some required tables are missing: {', '.join(missing_tables)}")
                logger.warning("The API may not function correctly until these tables are created.")
                logger.warning("Make sure to run the data loading script first.")
        else:
            logger.warning("Database connection test failed!")
            logger.warning("Starting API server anyway for development purposes...")
        
        app.run(host='0.0.0.0', port=5050, debug=True)
    except Exception as e:
        logger.error(f"Error starting the application: {str(e)}")