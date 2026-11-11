"""Builds streets.json — every Kyiv street YASNO knows, with its id.

YASNO's street search needs at least 3 characters, matches anywhere in the name
and returns at most 20 results, so the full list is collected by narrowing:
a query that returns 20 results is split into longer queries (one per next
character) until each returns fewer than 20. Run it now and then (monthly):

    python tools/build_streets.py            -> streets.json next to index.html
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2/streets"
PARAMS = {"regionId": "25", "dsoId": "902"}
LIMIT = 20
ALPHABET = "абвгґдеєжзиіїйклмнопрстуфхцчшщьюя'’-. ()0123456789"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "streets.json")

found, calls = {}, 0


def search(q):
    global calls
    for attempt in range(4):
        try:
            url = URL + "?" + urllib.parse.urlencode({**PARAMS, "query": q})
            req = urllib.request.Request(url, headers={"User-Agent": "svitlo-kyiv-streets/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                calls += 1
                time.sleep(0.15)  # be gentle: ~6 requests per second
                return json.loads(r.read())
        except Exception as e:
            print("  retry", repr(q), e, file=sys.stderr)
            time.sleep(2 * (attempt + 1))
    return []


def crawl(q, depth=0):
    res = search(q)
    for s in res:
        found[s["id"]] = s["value"]
    if len(res) >= LIMIT and depth < 6:
        for ch in ALPHABET:
            crawl(q + ch, depth + 1)


def main():
    # 1) discover street types ("вул.", "просп.", …) from a few broad queries
    types = set()
    for q in ("вул", "про", "бул", "пл.", "пер", "узв", "наб", "шос", "туп", "але", "дор", "ост", "сад", "кв-"):
        for s in search(q):
            found[s["id"]] = s["value"]
            types.add(s["value"].split(" ")[0])
    types = sorted(t for t in types if len(t) <= 8)
    print("street types:", types)
    # 2) every type + every first letter, narrowed where needed
    for t in types:
        for ch in ALPHABET[:33]:
            crawl(f"{t} {ch}")
        print(f"  {t}: total so far {len(found)} streets, {calls} requests", flush=True)
    # 3) safety net: 3-letter queries over the most common letter pairs catch odd names
    for a in "абвгдеіклмнопрстх":
        for b in "аеиіоуя":
            crawl(a + b + " ")
    streets = [{"id": i, "value": v} for i, v in found.items()]
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"built": time.strftime("%Y-%m-%d"), "streets": streets}, f, ensure_ascii=False, separators=(",", ":"))
    print(f"saved {len(streets)} streets to {OUT} ({calls} requests)")


if __name__ == "__main__":
    main()
