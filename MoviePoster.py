import aiohttp
import asyncio
import json
from aiohttp import ClientSession

# Load json
input_file = 'data/MovieSample.json'
output_file = 'data/updated_movies_with_posters.json'

# API key
api_key = "cde3e1dfe7a0390deea7cdd362bb769c"

async def fetch_poster_path(session, title):
    search_url = f"https://api.themoviedb.org/3/search/movie?query={title}&api_key={api_key}"
    async with session.get(search_url) as response:
        if response.status == 200:
            data = await response.json()
            results = data.get('results')
            if results:
                # Assuming the first result is the most relevant
                return results[0].get('poster_path')
        return None

async def append_poster_urls(movies):
    async with ClientSession() as session:
        tasks = []
        for movie in movies:
            task = asyncio.ensure_future(fetch_poster_path(session, movie['title']))
            tasks.append(task)
        poster_paths = await asyncio.gather(*tasks)
        for movie, poster_path in zip(movies, poster_paths):
            if poster_path:
                movie['poster_url'] = f"https://image.tmdb.org/t/p/original{poster_path}"
        return movies

def load_movies(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def save_movies(movies, filename):
    with open(filename, 'w') as file:
        json.dump(movies, file, indent=4)

def main():
    movies = load_movies(input_file)
    updated_movies = asyncio.run(append_poster_urls(movies))
    save_movies(updated_movies, output_file)
    print(f"Updated movies saved to {output_file}")

if __name__ == "__main__":
    main()
