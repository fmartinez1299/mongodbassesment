const { MongoClient, ObjectId } = require('mongodb');

// Replace with your MongoDB URI
const uri = 'mongodb+srv://martinezfrancisco254:a8Un9b70wiqud5FG@cluster1.gfr4erl.mongodb.net/?retryWrites=true&w=majority';

async function run() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const database = client.db('moviesdb');  // Make sure to use the correct database name
    const moviesCollection = database.collection('movies');
    const commentsCollection = database.collection('comments');
    const usersCollection = database.collection('users');

    // 1. Insert a New Document into the Users Collection
    const newUser = {
      name: 'John Doe',
      email: 'johndoe@example.com'
    };
    await usersCollection.insertOne(newUser);
    console.log('User inserted into the Users collection');

    // 2. Find all movies directed by Christopher Nolan
    const nolanMovies = await moviesCollection.find({ director: 'Christopher Nolan' }).toArray();
    console.log('Movies directed by Christopher Nolan:', nolanMovies);

    // 3. Find movies that include the genre "Action" and sort them by year descending
    const actionMovies = await moviesCollection.find({ genres: 'Action' }).sort({ year: -1 }).toArray();
    console.log('Action Movies sorted by year (desc):', actionMovies);

    // 4. Find movies with IMDb rating > 8 and return only title and IMDb info
    const highRatedMovies = await moviesCollection.find({ 'imdb.rating': { $gt: 8 } }, { projection: { title: 1, imdb: 1, _id: 0 } }).toArray();
    console.log('Movies with IMDb rating > 8:', highRatedMovies);

    // 5. Find movies starring both Tom Hanks and Tim Allen
    const moviesWithHanksAndAllen = await moviesCollection.find({ cast: { $all: ['Tom Hanks', 'Tim Allen'] } }).toArray();
    console.log('Movies starring both Tom Hanks and Tim Allen:', moviesWithHanksAndAllen);

    // 6. Find movies starring only Tom Hanks and Tim Allen
    const moviesWithOnlyHanksAndAllen = await moviesCollection.find({
      cast: { $all: ['Tom Hanks', 'Tim Allen'], $size: 2 }
    }).toArray();
    console.log('Movies starring only Tom Hanks and Tim Allen:', moviesWithOnlyHanksAndAllen);

    // 7. Find comedy movies directed by Steven Spielberg
    const spielbergComedyMovies = await moviesCollection.find({
      genres: 'Comedy',
      director: 'Steven Spielberg'
    }).toArray();
    console.log('Comedy movies directed by Steven Spielberg:', spielbergComedyMovies);

    // 8. Add a new field "available_on" with the value "Sflix" to "The Matrix"
    await moviesCollection.updateOne({ title: 'The Matrix' }, { $set: { available_on: 'Sflix' } });
    console.log('Added "available_on" field to The Matrix');

    // 9. Increment the metacritic of "The Matrix" by 1
    await moviesCollection.updateOne({ title: 'The Matrix' }, { $inc: { metacritic: 1 } });
    console.log('Incremented metacritic of The Matrix by 1');

    // 10. Add a new genre "Gen Z" to all movies released in the year 1997
    await moviesCollection.updateMany({ year: 1997 }, { $addToSet: { genres: 'Gen Z' } });
    console.log('Added "Gen Z" genre to all movies released in 1997');

    // 11. Increase IMDb rating by 1 for all movies with a rating less than 5
    await moviesCollection.updateMany({ 'imdb.rating': { $lt: 5 } }, { $inc: { 'imdb.rating': 1 } });
    console.log('Increased IMDb rating by 1 for all movies with a rating less than 5');

    // 12. Delete a comment with a specific ID
    // If you don't have the specific ObjectId, you can fetch one from the comments collection
    const comment = await commentsCollection.findOne();  // Fetching the first comment as an example
    if (comment) {
      const commentId = comment._id;  // Valid ObjectId
      await commentsCollection.deleteOne({ _id: new ObjectId(commentId) });
      console.log(`Deleted comment with ID: ${commentId}`);
    } else {
      console.log('No comments found to delete');
    }

    // 13. Delete all comments made for "The Matrix"
    const movie = await moviesCollection.findOne({ title: 'The Matrix' });
    if (movie) {
      await commentsCollection.deleteMany({ movie_id: movie._id });
      console.log('Deleted all comments made for The Matrix');
    }

    // 14. Delete all movies that do not have any genres
    await moviesCollection.deleteMany({ genres: { $exists: true, $eq: [] } });
    console.log('Deleted all movies with no genres');

    // 15. Aggregate movies to count how many were released each year, sorted by year
    const moviesCountByYear = await moviesCollection.aggregate([
      { $group: { _id: '$year', count: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('Movies count by year:', moviesCountByYear);

    // 16. Calculate the average IMDb rating for movies grouped by director, sorted by rating
    const avgRatingByDirector = await moviesCollection.aggregate([
      { $group: { _id: '$director', avg_rating: { $avg: '$imdb.rating' } } },
      { $sort: { avg_rating: -1 } }
    ]).toArray();
    console.log('Average IMDb rating by director:', avgRatingByDirector);

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
    console.log('Disconnected from MongoDB');
  }
}

run();
