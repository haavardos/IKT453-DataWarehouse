import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm

# Setup MongoDB connection
client = MongoClient("mongodb://mongodb:27017/")
db = client["movielens"]
collection = db["movies_optimized"]
collection.drop() 

# Load CSV files
movies = pd.read_csv("movielens/movies.csv")
ratings = pd.read_csv("movielens/ratings.csv")
tags = pd.read_csv("movielens/tags.csv")
links = pd.read_csv("movielens/links.csv")

#Convert timestamps to 'YYYY-MM-DD' format
ratings["date"] = ratings["timestamp"].apply(lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))
tags["date"] = tags["timestamp"].apply(lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))

#Group ratings and tags per movieId
grouped_ratings = ratings.groupby("movieId").apply(
    lambda df: df[["userId", "rating", "date"]].to_dict("records")
)

grouped_tags = tags.groupby("movieId").apply(
    lambda df: df[["userId", "tag", "date"]].to_dict("records")
)

# Convert links into a dictionary for fast lookup
links_dict = links.set_index("movieId").to_dict("index")

#Build documents and insert in batches
batch = []
batch_size = 1000

for _, row in tqdm(movies.iterrows(), total=len(movies)):
    movie_id = row["movieId"]
    document = {
        "movieId": movie_id,
        "title": row["title"],
        "genres": row["genres"].split("|") if pd.notna(row["genres"]) else [],
        "ratings": grouped_ratings.get(movie_id, []),
        "tags": grouped_tags.get(movie_id, []),
        "links": links_dict.get(movie_id, {})
    }
    batch.append(document)

    if len(batch) >= batch_size:
        collection.insert_many(batch)
        batch = []

# Insert any remaining documents
if batch:
    collection.insert_many(batch)

print("Finished loading MongoDB!")
