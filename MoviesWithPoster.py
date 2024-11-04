import aiohttp
import asyncio
import json
from aiohttp import ClientSession

# API Key
api_key = "cde3e1dfe7a0390deea7cdd362bb769c"

# Specify genres and number of movies per genre
genres = {
    "Action": 28,
    "Adventure": 12,
    "Drama": 18,
    "Comedy": 35,
    "Science Fiction": 878,
    "Family": 10751
}
movies_per_genre = 200  # Adjust to meet total requirement

# Function to fetch movies by genre, now including poster path
async def fetch_movies_by_genre(session, genre_id, genre_name):
    genre_movies = []
    for page in range(1, (movies_per_genre // 20) + 1):  # Assuming 20 results per page
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&with_genres={genre_id}&page={page}"
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
    return genre_movies

# Main function to generate the movie list with poster URLs
async def generate_movie_list():
    async with ClientSession() as session:
        tasks = []
        for genre_name, genre_id in genres.items():
            task = asyncio.ensure_future(fetch_movies_by_genre(session, genre_id, genre_name))
            tasks.append(task)
        
        all_movies = await asyncio.gather(*tasks)
        movies = [movie for genre_movies in all_movies for movie in genre_movies]
        
        # Save to JSON
        with open('data/popular_movies_with_posters.json', 'w') as f:
            json.dump(movies, f, indent=4)
        print(f"Movie list with posters saved to data/popular_movies_with_posters.json")

# Run the main function
asyncio.run(generate_movie_list())
