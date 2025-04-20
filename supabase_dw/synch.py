import psycopg2
import os

SUPABASE_HOST = os.getenv("SUPABASE_DB_HOST", "postgres_supabase")
SUPABASE_DB = os.getenv("SUPABASE_DB_NAME", "postgres")
SUPABASE_USER = os.getenv("SUPABASE_DB_USER", "postgres")
SUPABASE_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "evtoqEb6YUflIeC0")
SUPABASE_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

def sync_summary():
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        database=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        port=SUPABASE_PORT
    )
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO movie_summary (movie_id, title, avg_rating, num_ratings, num_tags, distinct_users)
        SELECT 
            m.movie_id,
            m.title,
            COALESCE(AVG(r.rating), 0) AS avg_rating,
            COUNT(r.rating_id) AS num_ratings,
            COUNT(t.tag_id) AS num_tags,
            COUNT(DISTINCT u.user_id) AS distinct_users
        FROM dim_movie m
        LEFT JOIN fact_ratings r ON m.movie_id = r.movie_id
        LEFT JOIN dim_tag t ON m.movie_id = t.movie_id
        LEFT JOIN (
            SELECT user_id, movie_id FROM fact_ratings
            UNION
            SELECT user_id, movie_id FROM dim_tag
        ) u ON u.movie_id = m.movie_id
        GROUP BY m.movie_id, m.title
        ON CONFLICT (movie_id) DO UPDATE SET
            avg_rating = EXCLUDED.avg_rating,
            num_ratings = EXCLUDED.num_ratings,
            num_tags = EXCLUDED.num_tags,
            distinct_users = EXCLUDED.distinct_users
        """)
        conn.commit()
    conn.close()
    print("Movie summary table synced successfully.")

if __name__ == "__main__":
    sync_summary()
