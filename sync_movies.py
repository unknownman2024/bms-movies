import json
import re
from pathlib import Path


# ================= CONFIG =================

SOURCE_FILE = Path("output/movies.json")
MASTER_FILE = Path("moviedata.json")

OLD_POSTER = "https://in.bmscdn.com/events/moviecard/"
NEW_POSTER = "https://assets-in.bmscdn.com/iedb/movies/images/mobile/listing/xlarge/"


# ================= HELPERS =================

def normalize(title: str) -> str:
    t = title.lower()
    t = re.sub(r"\([^)]*\)", "", t)
    t = re.sub(r"[^a-z0-9]+", "", t)
    return t.strip()


def fix_poster(url):
    if not url:
        return None

    if url.startswith(OLD_POSTER):
        return url.replace(OLD_POSTER, NEW_POSTER)

    return url


def score(m):
    s = 0
    if m.get("Poster"): s += 2
    if m.get("Genres"): s += 2
    if m.get("Variants"): s += 3
    if m.get("Rating"): s += 1
    if m.get("EventDate"): s += 1
    return s


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
            "New Poster": fix_poster(m.get("Poster")),
            "Genres": set(m.get("Genres", [])),
            "Rating": m.get("Rating"),
            "Duration": m.get("Duration"),
            "EventDate": m.get("EventDate"),
            "Languages": set(m.get("Languages", [])),
            "_score": score(m)
        }


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
                "New Poster": fix_poster(m.get("Poster")),
                "Genres": set(m.get("Genres", [])),
                "Rating": m.get("Rating"),
                "Duration": m.get("Duration"),
                "EventDate": m.get("EventDate"),
                "Languages": set(),
                "_score": s
            }

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
            cur["New Poster"] = fix_poster(m["Poster"])

        # Genres
        cur["Genres"].update(m.get("Genres", []))

        # Languages from variants
        for v in m.get("Variants", []):
            lang = v.get("Language")
            if lang:
                cur["Languages"].add(lang)


    # Cleanup
    output = []

    for m in merged.values():

        m.pop("_score", None)

        m["Genres"] = sorted(m["Genres"])
        m["Languages"] = sorted(m["Languages"])

        output.append(m)


    # Save
    with open(MASTER_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)


    print(f"✅ Synced {len(output)} movies")


if __name__ == "__main__":
    main()
