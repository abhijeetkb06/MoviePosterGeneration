import aiohttp
import asyncio
import json
from aiohttp import ClientSession

# API Key
api_key = "cde3e1dfe7a0390deea7cdd362bb769c"

# Define genres for blockbuster selection
genres = {
    "Action": 28,
    "Adventure": 12,
    "Drama": 18,
    "Comedy": 35,
    "Science Fiction": 878,
    "Family": 10751,
    "Animation": 16
}

# Target blockbuster movie count
target_movie_count = 1000

# Function to fetch blockbuster movies with popularity and rating filters
async def fetch_blockbuster_movies(session, genre_id, genre_name, all_movies, batch_num):
    genre_movies = []
    page_limit = 20  # Limits pages per genre to 20 for focused selection

    print(f"Fetching blockbuster movies for genre: {genre_name} (ID: {genre_id})")
    for page in range(1, page_limit + 1):
        url = (
            f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}"
            f"&with_genres={genre_id}&sort_by=popularity.desc&vote_average.gte=7&page={page}"
        )
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
                        break  # Exit retry loop on success
            except Exception as e:
                retries -= 1
                print(f"Error fetching page {page} for genre {genre_name}: {e}, retries left: {retries}")
                await asyncio.sleep(2)  # Retry delay

        # Save intermediate data periodically to avoid data loss
        if len(all_movies) + len(genre_movies) >= target_movie_count or page % 5 == 0:
            all_movies.extend(genre_movies)
            genre_movies.clear()
            filename = f"data/blockbuster_movies_batch_{batch_num}.json"
            with open(filename, 'w') as f:
                json.dump(all_movies, f, indent=4)
            print(f"Saved {len(all_movies)} movies to {filename}")
            if len(all_movies) >= target_movie_count:
                return  # Exit if target is reached
        await asyncio.sleep(1)  # Delay for rate limiting

    all_movies.extend(genre_movies)  # Add remaining movies to list
    print(f"Completed fetching for genre: {genre_name} (Total movies: {len(all_movies)})")

# Main function to generate blockbuster movie list
async def generate_blockbuster_movie_list():
    async with ClientSession() as session:
        all_movies = []
        batch_num = 1

        print("Starting blockbuster movie data fetching...")
        for genre_name, genre_id in genres.items():
            await fetch_blockbuster_movies(session, genre_id, genre_name, all_movies, batch_num)
            batch_num += 1
            if len(all_movies) >= target_movie_count:
                break  # Stop if we reach target count

        # Final save for blockbuster movies
        filename = "data/blockbuster_movies.json"
        with open(filename, 'w') as f:
            json.dump(all_movies[:target_movie_count], f, indent=4)
        print(f"Final blockbuster movie list saved with {len(all_movies[:target_movie_count])} movies in {filename}")

# Run the main function
asyncio.run(generate_blockbuster_movie_list())
