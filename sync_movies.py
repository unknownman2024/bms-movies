import json
import re
from pathlib import Path
import datetime
import time
import concurrent.futures
import requests

# ================= CONFIG =================

SOURCE_FILE = Path("output/movies.json")
MASTER_FILE = Path("moviedata.json")

OLD_POSTER = "https://in.bmscdn.com/events/moviecard/"
NEW_POSTER = "https://assets-in.bmscdn.com/iedb/movies/images/mobile/listing/xlarge/"

# ---------- Validation Configuration ----------
VALIDATION_DAYS = 30                # Re‑validate every 30 days
MAX_WORKERS = 10                    # Concurrent validation threads
REQUEST_TIMEOUT = 10                # Seconds per request
MAX_RETRIES = 3                     # Retries for transient failures
BACKOFF_FACTOR = 1                  # Exponential backoff base (seconds)

# ================= HELPERS =================

def normalize(title: str):
    t = title.lower()
    t = re.sub(r"\([^)]*\)", "", t)
    t = re.sub(r"[^a-z0-9]+", "", t)
    return t.strip()


def fix_poster(url, title=None):
    if not url:
        return None

    # Force custom poster for Dhurandhar The Revenge
    if title and title.lower() == "dhurandhar the revenge":
        return "/images/d2.jpg"

    # Convert old poster domain
    if url.startswith(OLD_POSTER):
        url = url.replace(OLD_POSTER, NEW_POSTER)

    return url


def score(m):
    s = 0
    if m.get("Poster"): s += 2
    if m.get("Genres"): s += 2
    if m.get("Variants"): s += 3
    if m.get("Rating"): s += 1
    if m.get("EventDate"): s += 1
    return s


# ================= VALIDATION HELPERS (NEW) =================

def parse_vd(vd_int: int) -> datetime.date:
    """
    Convert integer DDMMYYYY to a date object.
    Raises ValueError if format is invalid.
    """
    s = str(vd_int)
    if len(s) != 8:
        raise ValueError(f"Invalid vd format: {vd_int}")
    day = int(s[0:2])
    month = int(s[2:4])
    year = int(s[4:8])
    return datetime.date(year, month, day)


def next_vd() -> int:
    """Return today + VALIDATION_DAYS as integer DDMMYYYY."""
    future = datetime.date.today() + datetime.timedelta(days=VALIDATION_DAYS)
    return int(future.strftime("%d%m%Y"))


def check_poster(url: str):
    """
    Validate a poster URL using HEAD (fallback to GET if needed).
    Returns (is_valid, failure_type)
        is_valid   : True if HTTP 200
        failure_type: None (success), 'http' (HTTP error), 'network' (connection/timeout)
    Retries transient errors (5xx, timeouts, connection errors) with exponential backoff.
    Permanent HTTP errors (404,403,410) are not retried.
    """
    retries = MAX_RETRIES
    backoff = BACKOFF_FACTOR

    for attempt in range(retries):
        try:
            with requests.Session() as session:
                # Try HEAD first
                resp = session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)

                # Fallback to GET if server doesn't support HEAD
                if resp.status_code in (405, 501):
                    resp = session.get(url, timeout=REQUEST_TIMEOUT,
                                       allow_redirects=True, stream=True)
                    # Discard body (we only need status)
                    resp.close()

                if resp.status_code == 200:
                    return (True, None)

                # Permanent failures – no retry
                if resp.status_code in (404, 403, 410):
                    return (False, 'http')

                # Server errors – retry
                if 500 <= resp.status_code < 600:
                    if attempt < retries - 1:
                        time.sleep(backoff * (2 ** attempt))
                        continue
                    return (False, 'http')

                # Any other status -> treat as permanent HTTP error
                return (False, 'http')

        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            return (False, 'network')

        except requests.exceptions.ConnectionError:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            return (False, 'network')

        except Exception:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            return (False, 'network')

    # Should never reach here, but fallback
    return (False, 'network')


def validate_movie(movie: dict):
    """
    Validate a single movie's 'New Poster' URL.
    Updates the movie dict in‑place:
      - Always sets 'vd' to next validation date.
      - Removes 'New Poster' on failure.
    Returns (success, failure_type)
    """
    url = movie.get("New Poster")
    if not url:   # No URL to validate (shouldn't happen here, but defensive)
        return (False, None)

    is_valid, failure_type = check_poster(url)

    # Always update validation date
    movie["vd"] = next_vd()

    if is_valid:
        # Keep poster
        return (True, None)
    else:
        # Remove invalid poster
        movie.pop("New Poster", None)
        return (False, failure_type)


# ================= LOAD =================

def load_json(path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


# ================= MAIN MERGE =================

def main():
    source = load_json(SOURCE_FILE)
    master = load_json(MASTER_FILE)

    merged = {}

    # Load master first
    for m in master:
        key = normalize(m["Title"])
        merged[key] = {
            "Title": m["Title"],
            "Poster": m.get("Poster"),
            "New Poster": fix_poster(m.get("Poster"), m.get("Title")),
            "Genres": set(m.get("Genres", [])),
            "Rating": m.get("Rating"),
            "Duration": m.get("Duration"),
            "EventDate": m.get("EventDate"),
            "Languages": set(m.get("Languages", [])),
            "_score": score(m)
        }

        # Preserve existing vd if present (from previous runs)
        if "vd" in m:
            merged[key]["vd"] = m["vd"]

    # Merge new source
    for m in source:
        title = m.get("Title")
        if not title:
            continue

        key = normalize(title)
        s = score(m)

        if key not in merged:
            merged[key] = {
                "Title": title,
                "Poster": m.get("Poster"),
                "New Poster": fix_poster(m.get("Poster"), title),
                "Genres": set(m.get("Genres", [])),
                "Rating": m.get("Rating"),
                "Duration": m.get("Duration"),
                "EventDate": m.get("EventDate"),
                "Languages": set(),
                "_score": s
            }
        else:
            cur = merged[key]

            # Upgrade record
            if s > cur["_score"]:
                cur["Title"] = title
                cur["Rating"] = m.get("Rating")
                cur["Duration"] = m.get("Duration")
                cur["EventDate"] = m.get("EventDate")
                cur["_score"] = s

            # Poster
            if not cur["Poster"] and m.get("Poster"):
                cur["Poster"] = m["Poster"]
                cur["New Poster"] = fix_poster(m["Poster"], title)

            # Genres
            cur["Genres"].update(m.get("Genres", []))

            # Languages from variants
            for v in m.get("Variants", []):
                lang = v.get("Language")
                if lang:
                    cur["Languages"].add(lang)

    # ================= POSTER VALIDATION (NEW) =================

    movies = list(merged.values())
    total_movies = len(movies)

    # Prepare tasks: movies that have a 'New Poster' and need validation
    tasks = []
    skipped_count = 0

    today = datetime.date.today()

    for movie in movies:
        # If no New Poster, skip (do nothing)
        if not movie.get("New Poster"):
            skipped_count += 1
            continue

        # Check validation date
        vd_raw = movie.get("vd")
        if vd_raw is not None:
            try:
                vd_date = parse_vd(vd_raw)
                if today < vd_date:
                    # Validation not due yet
                    skipped_count += 1
                    continue
            except ValueError:
                # Invalid vd → treat as missing and validate
                pass

        # Validation is due
        tasks.append(movie)

    validated_count = len(tasks)
    confirmed = 0
    removed_http = 0
    removed_network = 0

    # Progress indicator (try tqdm, fallback to simple prints)
    try:
        from tqdm import tqdm
        pbar = tqdm(total=validated_count, desc="Validating posters", unit="movie")
    except ImportError:
        pbar = None
        print(f"Validating {validated_count} posters...")

    start_time = time.time()

    if tasks:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_movie = {
                executor.submit(validate_movie, movie): movie
                for movie in tasks
            }

            for future in concurrent.futures.as_completed(future_to_movie):
                movie = future_to_movie[future]
                try:
                    success, failure_type = future.result()
                    if success:
                        confirmed += 1
                    else:
                        if failure_type == 'http':
                            removed_http += 1
                        else:  # 'network' or None
                            removed_network += 1
                except Exception as e:
                    # Unexpected error – treat as network failure, and ensure removal
                    removed_network += 1
                    movie.pop("New Poster", None)
                    movie["vd"] = next_vd()

                if pbar:
                    pbar.update(1)

        if pbar:
            pbar.close()

    elapsed = time.time() - start_time

    # Log summary
    print("\n--- Poster Validation Summary ---")
    print(f"Total movies            : {total_movies}")
    print(f"Skipped (no poster / not due): {skipped_count}")
    print(f"Validated (attempted)   : {validated_count}")
    print(f"Posters confirmed       : {confirmed}")
    print(f"Posters removed (HTTP)  : {removed_http}")
    print(f"Failed validations (network): {removed_network}")
    print(f"Total execution time    : {elapsed:.2f}s")
    print("--------------------------------\n")

    # ================= CLEANUP & OUTPUT =================

    output = []
    for m in merged.values():
        m.pop("_score", None)
        m["Genres"] = sorted(m["Genres"])
        m["Languages"] = sorted(m["Languages"])
        output.append(m)

    with open(MASTER_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Synced {len(output)} movies")


if __name__ == "__main__":
    main()
