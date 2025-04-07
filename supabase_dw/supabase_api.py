from flask import Flask, jsonify
import os
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# Supabase PostgreSQL connection info
DB_HOST = os.getenv("SUPABASE_DB_HOST", "cczocsyuijrjuqnvrywd.supabase-postgres.co")
DB_NAME = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "evtoqEb6YUflIeC0")
DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

def get_db_connection():
    """Create a connection to the Supabase PostgreSQL database"""
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode='require'  # This is important for Supabase
    )
    conn.autocommit = True
    return conn

@app.route('/')
def home():
    return jsonify({"message": "Welcome to the MovieLens API (Supabase)!"})

@app.route('/movies', methods=['GET'])
def get_movies():
    """Get a list of movies (limited to 10)"""
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

@app.route('/movies/<int:movie_id>', methods=['GET'])
def get_movie_by_id(movie_id):
    """Get details for a specific movie by ID"""
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
            TO_CHAR(dt.timestamp_id, 'YYYY-MM-DD') as date
        FROM fact_ratings fr
        JOIN dim_time dt ON fr.timestamp_id = dt.timestamp_id
        WHERE fr.movie_id = %s
    """, (movie_id,))
    
    ratings = cur.fetchall()
    
    # Get tags for this movie
    cur.execute("""
        SELECT 
            t.user_id as "userId", 
            t.tag, 
            TO_CHAR(dt.timestamp_id, 'YYYY-MM-DD') as date
        FROM dim_tag t
        JOIN dim_time dt ON t.timestamp_id = dt.timestamp_id
        WHERE t.movie_id = %s
    """, (movie_id,))
    
    tags = cur.fetchall()
    
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

@app.route('/movies/<int:movie_id>/ratings', methods=['GET'])
def get_movie_ratings(movie_id):
    """Get all ratings for a specific movie"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            fr.user_id as "userId", 
            fr.rating, 
            TO_CHAR(dt.timestamp_id, 'YYYY-MM-DD') as date
        FROM fact_ratings fr
        JOIN dim_time dt ON fr.timestamp_id = dt.timestamp_id
        WHERE fr.movie_id = %s
    """, (movie_id,))
    
    ratings = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return jsonify(ratings)

@app.route('/movies/<int:movie_id>/tags', methods=['GET'])
def get_movie_tags(movie_id):
    """Get all tags for a specific movie"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            t.user_id as "userId", 
            t.tag, 
            TO_CHAR(dt.timestamp_id, 'YYYY-MM-DD') as date
        FROM dim_tag t
        JOIN dim_time dt ON t.timestamp_id = dt.timestamp_id
        WHERE t.movie_id = %s
    """, (movie_id,))
    
    tags = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return jsonify(tags)

@app.route('/top-rated', methods=['GET'])
def get_top_rated():
    """Get top rated movies"""
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

@app.route('/most-popular', methods=['GET'])
def get_most_popular():
    """Get most popular movies (by number of ratings)"""
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

@app.route('/user-recommendations/<int:user_id>', methods=['GET'])
def get_user_recommendations(user_id):
    """Get recommendations based on user's highly-rated movies"""
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

def test_connection():
    try:
        print("Attempting connection to Supabase...")
        print(f"Host: {DB_HOST}")
        print(f"Database: {DB_NAME}")
        print(f"User: {DB_USER}")
        conn = get_db_connection()
        print("Connection established!")
        
        print("Querying database tables...")
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cur.fetchall()
        print("Connection successful!")
        print("Tables in your database:")
        for table in tables:
            print(f"- {table[0]}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        print("Check your database credentials and network connectivity")
        return False

if __name__ == '__main__':
    try:
        test_connection()
        print("Database connection verified, starting API server...")
        app.run(host='0.0.0.0', port=5050, debug=True)
    except Exception as e:
        print(f"WARNING: Connection test failed: {str(e)}")
        print("Starting API server anyway for development purposes...")
        app.run(host='0.0.0.0', port=5050, debug=True)