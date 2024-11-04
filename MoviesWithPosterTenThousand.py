import aiohttp
import asyncio
import json
import time
from aiohttp import ClientSession

# API Key
api_key = "cde3e1dfe7a0390deea7cdd362bb769c"

# Specify genres and number of movies per genre to reach 10,000
genres = {
    "Action": 28,
    "Adventure": 12,
    "Drama": 18,
    "Comedy": 35,
    "Science Fiction": 878,
    "Family": 10751
}
target_movie_count = 10000  # Total target movies

# Function to fetch movies by genre, with retries and a save mechanism if stuck
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
            
        # Save progress if we’ve gathered enough or in case it gets stuck
        if len(all_movies) + len(genre_movies) >= target_movie_count or page % 50 == 0:
            all_movies.extend(genre_movies)
            genre_movies.clear()
            filename = f"data/intermediate_movies_batch_{batch_num}.json"
            with open(filename, 'w') as f:
                json.dump(all_movies, f, indent=4)
            print(f"Saved intermediate data with {len(all_movies)} movies to {filename}")
            if len(all_movies) >= target_movie_count:
                return  # Exit if target is met
        await asyncio.sleep(1)  # Delay to avoid hitting rate limits

    all_movies.extend(genre_movies)  # Append any remaining movies
    print(f"Finished fetching movies for genre: {genre_name} (Total: {len(all_movies)})")

# Main function to generate the movie list with poster URLs
async def generate_movie_list():
    async with ClientSession() as session:
        all_movies = []
        batch_num = 1
        page_limit_per_genre = 500  # Adjust to limit the number of pages per genre

        print("Starting movie data fetching process...")
        for genre_name, genre_id in genres.items():
            await fetch_movies_by_genre(session, genre_id, genre_name, page_limit_per_genre, all_movies, batch_num)
            batch_num += 1
            if len(all_movies) >= target_movie_count:
                break

        # Save final data
        filename = "data/final_movies_with_posters.json"
        with open(filename, 'w') as f:
            json.dump(all_movies[:target_movie_count], f, indent=4)
        print(f"Final movie data saved with {len(all_movies[:target_movie_count])} movies in {filename}")

# Run the main function
asyncio.run(generate_movie_list())
