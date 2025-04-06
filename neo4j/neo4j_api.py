from flask import Flask, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)

NEO4J_URI = "bolt://host.docker.internal:7687"  # Add "bolt://neo4j:7687" if we need to run from docker-compose
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

@app.route('/')
def home():
    return jsonify({"message": "Welcome to the MovieLens API (Neo4j)!"})

# 1. /movies (limit 10)
@app.route('/movies', methods=['GET'])
def get_movies():
    query = """
        MATCH (m:Movie)
        RETURN m.movieId AS movieId, m.title AS title, m.genres AS genres
        LIMIT 10
    """
    with driver.session() as session:
        results = session.run(query).data()
    return jsonify(results)

# 2. /movies/<id>
@app.route('/movies/<int:movie_id>', methods=['GET'])
def get_movie_by_id(movie_id):
    query = """
        MATCH (m:Movie {movieId: $movie_id})
        OPTIONAL MATCH (m)<-[r:RATED]-(u:User)
        OPTIONAL MATCH (m)<-[t:TAGGED]-(u2:User)
        RETURN 
          m.movieId AS movieId,
          m.title AS title,
          m.genres AS genres,
          m.imdbId AS imdbId,
          m.tmdbId AS tmdbId,
          COLLECT(DISTINCT {
            userId: u.userId,
            rating: r.rating,
            date: r.date
          }) AS ratings,
          COLLECT(DISTINCT {
            userId: u2.userId,
            tag: t.tag,
            date: t.date
          }) AS tags
    """
    with driver.session() as session:
        result = session.run(query, movie_id=movie_id).data()
    if not result or not result[0]['movieId']:
        return jsonify({"error": "Movie not found"}), 404

    movie = result[0]

    # Compute average rating and ratingCount
    rating_list = movie.get('ratings', [])
    rating_values = [r['rating'] for r in rating_list if isinstance(r['rating'], (int, float))]
    movie['avgRating'] = round(sum(rating_values)/len(rating_values), 2) if rating_values else None
    movie['ratingCount'] = len(rating_values)

    # Turn imdbId/tmdbId into a links dict to mimic your Mongo code
    links = {}
    if movie.get('imdbId'):
        links['imdbId'] = movie.pop('imdbId')
    if movie.get('tmdbId'):
        links['tmdbId'] = movie.pop('tmdbId')
    movie['links'] = links

    return jsonify(movie), 200

# 3. /movies/<id>/ratings
@app.route('/movies/<int:movie_id>/ratings', methods=['GET'])
def get_movie_ratings(movie_id):
    query = """
        MATCH (m:Movie {movieId: $movie_id})<-[r:RATED]-(u:User)
        RETURN COLLECT({
            userId: u.userId,
            rating: r.rating,
            date: r.date
        }) AS ratings
    """
    with driver.session() as session:
        result = session.run(query, movie_id=movie_id).data()

    if not result:
        return jsonify([])

    # result[0]['ratings'] might be an empty list if no ratings
    return jsonify(result[0]['ratings'])

# 4. /movies/<int:movie_id>/tags
@app.route('/movies/<int:movie_id>/tags', methods=['GET'])
def get_movie_tags(movie_id):
    query = """
        MATCH (m:Movie {movieId: $movie_id})<-[t:TAGGED]-(u:User)
        RETURN COLLECT({
            userId: u.userId,
            tag: t.tag,
            date: t.date
        }) AS tags
    """
    with driver.session() as session:
        result = session.run(query, movie_id=movie_id).data()

    if not result:
        return jsonify([])

    return jsonify(result[0]['tags'])

# 5. /top-rated
@app.route('/top-rated', methods=['GET'])
def get_top_rated():
    query = """
        MATCH (m:Movie)<-[r:RATED]-(u:User)
        WITH m, avg(r.rating) AS avgRating
        RETURN m.movieId AS movieId, m.title AS title, avgRating
        ORDER BY avgRating DESC
        LIMIT 10
    """
    with driver.session() as session:
        results = session.run(query).data()
    return jsonify(results)

# 6. /most-popular
@app.route('/most-popular', methods=['GET'])
def get_most_popular():
    query = """
        MATCH (m:Movie)<-[r:RATED]-(u:User)
        WITH m, count(r) AS ratingCount
        RETURN m.movieId AS movieId, m.title AS title, ratingCount
        ORDER BY ratingCount DESC
        LIMIT 10
    """
    with driver.session() as session:
        results = session.run(query).data()
    return jsonify(results)

# 7. /user-recommendations/<user_id>
@app.route('/user-recommendations/<int:user_id>', methods=['GET'])
def user_recommendations(user_id):
    query = """
        MATCH (u:User {userId: $user_id})-[r:RATED]->(m:Movie)
        WHERE r.rating >= 4
        RETURN m.movieId AS movieId,
               m.title AS title,
               COLLECT(r.rating) AS highRatings
    """
    with driver.session() as session:
        results = session.run(query, user_id=user_id).data()

    if not results:
        return jsonify({"error": "User not found or no ratings"}), 404

    cleaned = []
    for record in results:
        rating_vals = record["highRatings"]
        if rating_vals:
            cleaned.append({
                "movieId": record["movieId"],
                "title": record["title"],
                "numHighRatings": len(rating_vals),
                "avgHighRating": round(sum(rating_vals) / len(rating_vals), 2)
            })

    if not cleaned:
        return jsonify({"error": "User has no highly-rated movies"}), 404

    return jsonify(cleaned), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6060, debug=True)
