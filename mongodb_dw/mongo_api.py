from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client["movielens"]

def clean_object_ids(doc):
    """ Recursively converts ObjectId fields to strings """
    if isinstance(doc, list):
        return [clean_object_ids(item) for item in doc]
    elif isinstance(doc, dict):
        return {k: str(v) if isinstance(v, ObjectId) else clean_object_ids(v) for k, v in doc.items()}
    return doc

@app.route('/')
def home():
    return jsonify({"message": "Welcome to the MovieLens API with Optimized Data!"})

# Get all movies (limit 10 for testing)
@app.route('/movies', methods=['GET'])
def get_movies():
    movies = list(db.movies_optimized.find({}, {"_id": 0, "movieId": 1, "title": 1, "genres": 1}).limit(10))
    return jsonify(movies)

# Get a movie by ID
@app.route('/movies/<int:movie_id>', methods=['GET'])
def get_movie_by_id(movie_id):
    movie = db.movies_optimized.find_one(
        {"movieId": movie_id},
        {"_id": 0, "movieId": 1, "title": 1, "genres": 1, "ratings": 1, "tags": 1, "links": 1}
    )

    if not movie:
        return jsonify({"error": "Movie not found"}), 404

   
    ratings = movie.get("ratings", [])
    rating_values = [r.get("rating") for r in ratings if isinstance(r.get("rating"), (int, float))]

    movie["avgRating"] = round(sum(rating_values) / len(rating_values), 2) if rating_values else None
    movie["ratingCount"] = len(rating_values)


    return jsonify(clean_object_ids(movie)), 200


#Get all ratings for a movie
@app.route('/movies/<int:movie_id>/ratings', methods=['GET'])
def get_movie_ratings(movie_id):
    movie = db.movies_optimized.find_one({"movieId": movie_id}, {"_id": 0, "ratings": 1})
    return jsonify(clean_object_ids(movie["ratings"])) if movie else jsonify([])

#Get all tags for a movie
@app.route('/movies/<int:movie_id>/tags', methods=['GET'])
def get_movie_tags(movie_id):
    movie = db.movies_optimized.find_one({"movieId": movie_id}, {"_id": 0, "tags": 1})
    return jsonify(clean_object_ids(movie["tags"])) if movie else jsonify([])

#Get top-rated movies
@app.route('/top-rated', methods=['GET'])
def get_top_rated_movies():
    top_movies = list(db.movies_optimized.aggregate([
        {"$project": {"_id": 0, "movieId": 1, "title": 1,"ratingCount": {"$size": "$ratings"}, "avgRating": {"$avg": "$ratings.rating"}}},
        {"$match": {"ratingCount": {"$gte": 25}}},
        {"$sort": {"avgRating": -1}},
        {"$limit": 20}
    ]))
    return jsonify(top_movies)

#Get most popular movies (by number of ratings)
@app.route('/most-popular', methods=['GET'])
def get_most_rated_movies():
    most_rated = list(db.movies_optimized.aggregate([
        {"$project": {"_id": 0, "movieId": 1, "title": 1, "ratingCount": {"$size": "$ratings"}}},
        {"$sort": {"ratingCount": -1}},{"$limit": 10}
    ]))
    return jsonify(most_rated)

#User-based recommendations get all moves a user has rated above 4
@app.route('/user-recommendations/<int:user_id>', methods=['GET'])
def get_user_recommendations(user_id):
    recommended_movies = list(db.movies_optimized.aggregate([
    {"$match": {
        "ratings": {
            "$elemMatch": {
                "userId": user_id,
                "rating": {"$gte": 4}
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "movieId": 1,
        "title": 1,
        "userHighRating": {
            "$first": {
                "$filter": {
                    "input": "$ratings",
                    "as": "r",
                    "cond": {
                        "$and": [
                            {"$eq": ["$$r.userId", user_id]},
                            {"$gte": ["$$r.rating", 4]}
                        ]
                    }
                }
            }
        }
    }}
]))


    # Process to avoid passing nested arrays to Dash
    cleaned = []
    for movie in recommended_movies:
        rating_entry = movie.pop("userHighRating", None)
        if rating_entry:
            movie["userRating"] = rating_entry.get("rating")
            movie["ratingDate"] = rating_entry.get("date")
            cleaned.append(movie)

    return (jsonify({"error": "User has rated no movies"}), 404) if not cleaned else jsonify(clean_object_ids(cleaned)), 200

@app.route('/avg-rating-by-genre', methods=['GET'])
def get_avg_rating_by_genre():
    pipeline = [
        {"$unwind": "$genres"}, 
        {"$project": {
            "genre": "$genres",
            "avgRating": {"$avg": "$ratings.rating"},
            "ratingCount": {"$size": "$ratings"}
        }},
        {"$match": {"ratingCount": {"$gte": 10}}},  
        {"$group": {
            "_id": "$genre",
            "avgGenreRating": {"$avg": "$avgRating"},
            "moviesInGenre": {"$sum": 1}
        }},
        # Rename _id to genre in the projection
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "avgGenreRating": 1,
            "moviesInGenre": 1
        }},
        {"$sort": {"avgGenreRating": -1}}
    ]

    results = list(db.movies_optimized.aggregate(pipeline))
    return jsonify(clean_object_ids(results)), 200

# Run Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
