import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, timezone

# TMDb API details
API_KEY = "6cdf5e25afce3722ee5f0c5bd30cc56c"
BASE_URL = "https://api.tmdb.org/3/discover/movie"
REGION = "IN"
LANGUAGE = "en-US"
MOVIE_LANGUAGES_ARRAY = ["hi", "ta", "te", "ml", "kn"]
MOVIE_LANGUAGES_PARAM = "hi|ta|te|ml|kn"

# IST timezone (+5:30)
IST = timezone(timedelta(hours=5, minutes=30))
now_ist = datetime.now(IST)
today = now_ist.strftime("%Y-%m-%d")
DATE_FETCHED = now_ist.strftime("%Y%m%d")
LAST_FETCHED = now_ist.strftime("%Y-%m-%d %H:%M IST")

# File paths
DATA_DIR = "HYPE/Data"
OUTPUT_FILE = f"{DATA_DIR}/{DATE_FETCHED}_hypemeter.json"

os.makedirs(DATA_DIR, exist_ok=True)

async def fetch_json(session, url):
    try:
        async with session.get(url) as response:
            return await response.json()
    except Exception as e:
        print(f"Request failed: {e}")
        return {}

async def fetch_movies_by_language(session, language):
    movies = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        url = f"{BASE_URL}?api_key={API_KEY}&language={LANGUAGE}&with_original_language={language}&include_adult=false&include_video=false&page={page}"
        data = await fetch_json(session, url)
        if not data or "results" not in data or not data["results"]:
            break
        movies.extend(data["results"])
        total_pages = data.get("total_pages", 1)
        page += 1
    return movies

async def fetch_upcoming_movies_page(session, page):
    url = f"{BASE_URL}?api_key={API_KEY}&language={LANGUAGE}&region={REGION}&primary_release_date.gte={today}&with_original_language={MOVIE_LANGUAGES_PARAM}&include_adult=false&include_video=false&page={page}"
    data = await fetch_json(session, url)
    return data.get("results", [])

async def fetch_unreleased_movies(session):
    tasks = [fetch_movies_by_language(session, lang) for lang in MOVIE_LANGUAGES_ARRAY]
    results = await asyncio.gather(*tasks)
    all_movies = [movie for result in results for movie in result]
    unreleased = [m for m in all_movies if not m.get("release_date") or m.get("release_date").strip() == ""]
    unique = {}
    for m in unreleased:
        if m["id"] not in unique:
            unique[m["id"]] = {
                "id": m["id"],
                "name": m["title"],
                "release_date": "TBD",
                "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "No Poster Available",
                "popularity": m.get("popularity", 0),
                "rating": m.get("vote_average", 0),
                "votes": m.get("vote_count", 0)
            }
    return sorted(unique.values(), key=lambda x: x["popularity"], reverse=True)

async def fetch_upcoming_movies(session):
    tasks = [fetch_upcoming_movies_page(session, page) for page in range(1, 11)]
    results = await asyncio.gather(*tasks)
    all_movies = [movie for result in results for movie in result]
    formatted = []
    for m in all_movies:
        formatted.append({
            "id": m["id"],
            "name": m["title"],
            "release_date": m.get("release_date") or "TBA",
            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "No Poster Available",
            "popularity": m.get("popularity", 0),
            "rating": m.get("vote_average", 0),
            "votes": m.get("vote_count", 0)
        })
    return sorted(formatted, key=lambda x: x["popularity"], reverse=True)

async def main():
    async with aiohttp.ClientSession() as session:
        unreleased = await fetch_unreleased_movies(session)
        upcoming = await fetch_upcoming_movies(session)
        combined = {
            "last_fetched": LAST_FETCHED,
            "unreleased": unreleased,
            "upcoming": upcoming
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2)
        print(f"Saved data to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
