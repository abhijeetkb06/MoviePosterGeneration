import aiohttp
import asyncio
import json
from aiohttp import ClientSession

# API Key
api_key = "cde3e1dfe7a0390deea7cdd362bb769c"

# Full list of genres with their IDs
genres = {
    "Action": 28,
    "Adventure": 12,
    "Animation": 16,
    "Comedy": 35,
    "Crime": 80,
    "Documentary": 99,
    "Drama": 18,
    "Family": 10751,
    "Fantasy": 14,
    "History": 36,
    "Horror": 27,
    "Music": 10402,
    "Mystery": 9648,
    "Romance": 10749,
    "Science Fiction": 878,
    "TV Movie": 10770,
    "Thriller": 53,
    "War": 10752,
    "Western": 37
}

# Function to fetch movies by genre, with retries and periodic saving
async def fetch_movies_by_genre(session, genre_id, genre_name, page_limit, all_movies, batch_num):
    genre_movies = []
    print(f"Starting to fetch movies for genre: {genre_name} (ID: {genre_id})")
    for page in range(1, page_limit + 1):
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&with_genres={genre_id}&page={page}"
        retries = 3
        while retries > 0:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        for result in data.get('results', []):
                            poster_path = result.get("poster_path")
                            full_poster_url = f"https://image.tmdb.org/t/p/original{poster_path}" if poster_path else None
                            genre_movies.append({
                                "title": result.get("title"),
                                "description": result.get("overview"),
                                "genre": [genre_name],
                                "poster_url": full_poster_url
                            })
                        print(f"Fetched page {page} of genre {genre_name} (Total movies so far: {len(genre_movies) + len(all_movies)})")
                        break  # Break out of retry loop on success
            except Exception as e:
                retries -= 1
                print(f"Error fetching page {page} of genre {genre_name}: {e}, retries left: {retries}")
                await asyncio.sleep(2)  # Wait before retrying
            
        # Save progress periodically to avoid data loss
        if page % 50 == 0 or len(all_movies) + len(genre_movies) >= 1000000:  # Adjust threshold if needed
            all_movies.extend(genre_movies)
            genre_movies.clear()
            filename = f"data/intermediate_movies_batch_{batch_num}.json"
            with open(filename, 'w') as f:
                json.dump(all_movies, f, indent=4)
            print(f"Saved intermediate data with {len(all_movies)} movies to {filename}")
            if len(all_movies) >= 1000000:
                return  # Stop if reaching a very large number of movies
        await asyncio.sleep(1)  # Delay to avoid hitting rate limits

    all_movies.extend(genre_movies)  # Append any remaining movies
    print(f"Finished fetching movies for genre: {genre_name} (Total: {len(all_movies)})")

# Main function to generate the movie list with poster URLs
async def generate_movie_list():
    async with ClientSession() as session:
        all_movies = []
        batch_num = 1
        page_limit_per_genre = 1000  # Set high limit to fetch maximum data

        print("Starting movie data fetching process...")
        for genre_name, genre_id in genres.items():
            await fetch_movies_by_genre(session, genre_id, genre_name, page_limit_per_genre, all_movies, batch_num)
            batch_num += 1

        # Save final data with maximum movies fetched
        filename = "data/max_movies_with_posters.json"
        with open(filename, 'w') as f:
            json.dump(all_movies, f, indent=4)
        print(f"Final movie data saved with {len(all_movies)} movies in {filename}")

# Run the main function
asyncio.run(generate_movie_list())
