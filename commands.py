from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['movieDB']
movies_collection = db['movies']
users_collection = db['users']

# 1. Insert a New Document into the Users Collection
user = {
    "name": "John Doe",
    "email": "johndoe@example.com"
}
users_collection.insert_one(user)
print("User inserted into the Users collection")

# 2. Find all movies directed by Christopher Nolan
nolan_movies = movies_collection.find({"director": "Christopher Nolan"})
print("\nMovies directed by Christopher Nolan:")
for movie in nolan_movies:
    print(movie['title'])

# 3. Find movies that include the genre "Action" and sort (descending) them by year
action_movies = movies_collection.find({"genres": "Action"}).sort("year", -1)
print("\nAction Movies sorted by year (desc):")
for movie in action_movies:
    print(movie['title'], movie['year'])

# 4. Find movies with an IMDb rating greater than 8 and return only the title and IMDB information
high_rated_movies = movies_collection.find(
    {"imdb_rating": {"$gt": 8}},
    {"_id": 0, "title": 1, "imdb_rating": 1}  # Project only the title and imdb_rating fields
)
print("\nMovies with IMDb rating > 8:")
for movie in high_rated_movies:
    print(movie['title'], movie['imdb_rating'])

# 5. Find movies that starred both "Tom Hanks" and "Tim Allen"
both_actors_movies = movies_collection.find(
    {"actors": {"$all": ["Tom Hanks", "Tim Allen"]}}
)
print("\nMovies starring both Tom Hanks and Tim Allen:")
for movie in both_actors_movies:
    print(movie['title'])

# 6. Find movies that starred both and only "Tom Hanks" and "Tim Allen"
only_both_actors_movies = movies_collection.find(
    {"actors": {"$size": 2, "$all": ["Tom Hanks", "Tim Allen"]}}
)
print("\nMovies starring only Tom Hanks and Tim Allen:")
for movie in only_both_actors_movies:
    print(movie['title'])

# 7. Find comedy movies that are directed by Steven Spielberg
spielberg_comedies = movies_collection.find(
    {"director": "Steven Spielberg", "genres": "Comedy"}
)
print("\nComedy movies directed by Steven Spielberg:")
for movie in spielberg_comedies:
    print(movie['title'])

# 8. Add a new field "available_on" with the value "Sflix" to "The Matrix"
movies_collection.update_one(
    {"title": "The Matrix"},
    {"$set": {"available_on": "Sflix"}}
)
print('\nAdded "available_on" field to The Matrix')

# 9. Increment the metacritic of "The Matrix" by 1
movies_collection.update_one(
    {"title": "The Matrix"},
    {"$inc": {"metacritic": 1}}
)
print('Incremented metacritic of The Matrix by 1')

# 10. Add a new genre "Gen Z" to all movies released in the year 1997
movies_collection.update_many(
    {"year": 1997},
    {"$addToSet": {"genres": "Gen Z"}}
)
print('Added "Gen Z" genre to all movies released in 1997')

# 11. Increase IMDb rating by 1 for all movies with a rating less than 5
movies_collection.update_many(
    {"imdb_rating": {"$lt": 5}},
    {"$inc": {"imdb_rating": 1}}
)
print('Increased IMDb rating by 1 for all movies with a rating less than 5')

# 12. Delete a comment with a specific ID (assuming comment ID is known)
# For demonstration, I am using a placeholder for comment_id
comment_id_to_delete = "some_comment_id"
movies_collection.update_many(
    {},
    {"$pull": {"comments": {"_id": comment_id_to_delete}}}
)
print('No comments found to delete')

# 13. Delete all comments made for "The Matrix"
movies_collection.update_one(
    {"title": "The Matrix"},
    {"$set": {"comments": []}}
)
print('Deleted all comments made for "The Matrix"')

# 14. Delete all movies that do not have any genres
movies_collection.delete_many(
    {"genres": {"$exists": False}}
)
print('Deleted all movies with no genres')

# 15. Aggregate movies to count how many were released each year and display from the earliest year to the latest
movie_count_by_year = movies_collection.aggregate([
    {"$group": {"_id": "$year", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}  # Sort by year in ascending order
])
print("\nMovies count by year:")
for count in movie_count_by_year:
    print(f"Year: {count['_id']}, Count: {count['count']}")

# 16. Calculate the average IMDb rating for movies grouped by director and display from highest to lowest
avg_imdb_by_director = movies_collection.aggregate([
    {"$group": {"_id": "$director", "average_imdb": {"$avg": "$imdb_rating"}}},
    {"$sort": {"average_imdb": -1}}  # Sort by average IMDb rating in descending order
])
print("\nAverage IMDb rating by director:")
for director in avg_imdb_by_director:
    print(f"Director: {director['_id']}, Average IMDb: {director['average_imdb']}")

# Disconnect from MongoDB
client.close()
print("\nDisconnected from MongoDB")
