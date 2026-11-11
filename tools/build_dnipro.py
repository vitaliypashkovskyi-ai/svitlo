"""Builds dnipro_streets.json — every town of Dnipropetrovsk region (DTEK Dnipro Grids) with its streets.

YASNO's town search returns all matches, so towns come from a few broad queries. Street search
returns at most 20 matches per query, so streets are collected per town with narrowing queries
('вул. ' -> 'вул. А', 'вул. Б', …) until each returns fewer than 20. Progress is kept in
tools/dnipro_cache.json, so an interrupted run resumes where it stopped.

    python -X utf8 tools/build_dnipro.py         -> dnipro_streets.json next to streets.json
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.join(ROOT, "tools", "dnipro_cache.json")
OUT = os.path.join(ROOT, "dnipro_streets.json")
BASE = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2/"
PARAMS = {"regionId": "3", "dsoId": "301"}  # Дніпропетровська обл., ДТЕК Дніпровські електромережі
LIMIT = 20
TOWN_QUERIES = ["м. ", "с. ", "смт", "с-ще", "селищ", "рада", "СТ ", "с/т", "сел. "]
STREET_TYPES = ["вул. ", "пров. ", "просп. ", "пл. ", "бул. ", "тупик", "узвіз", "шосе", "ж/м", "проїзд",
                "наб", "Без", "мкр", "кв-л", "м-н", "площа", "СТ ", "с/т", "дорога", "алея"]
NEXT = "абвгґдеєжзиіїйклмнопрстуфхцчшщьюяАБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЮЯ0123456789'’-. "
calls = [0]


def get(ep, **p):
    url = BASE + ep + "?" + urllib.parse.urlencode({**PARAMS, **p})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "svitlo-kyiv/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"}), timeout=30) as r:
                calls[0] += 1
                time.sleep(0.12)
                return json.loads(r.read())
        except Exception as e:
            print("   retry", ep, p.get("query"), e, file=sys.stderr)
            time.sleep(3 * (attempt + 1))
    return []


def crawl_streets(town_id, prefix, found, depth=0):
    res = get("streets", query=prefix, cityId=town_id)
    for s in res:
        found[str(s["id"])] = s["value"]
    if len(res) >= LIMIT and depth < 4:
        tried = set()
        for ch in NEXT:  # 'вул. ' + 'А' and 'вул. ' + 'а' match the same, so skip case twins
            key = (prefix + ch).lower()
            if key in tried:
                continue
            tried.add(key)
            crawl_streets(town_id, prefix + ch, found, depth + 1)


def main():
    try:
        cache = json.load(open(CACHE, encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {"towns": {}, "streets": {}}
    if not cache["towns"]:
        for q in TOWN_QUERIES:
            for t in get("cities", query=q):
                cache["towns"][str(t["id"])] = t["value"]
        json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)
    towns = cache["towns"]
    print(f"{len(towns)} towns", flush=True)
    # biggest places first, so the useful part is ready early
    order = sorted(towns, key=lambda i: (not towns[i].startswith("м. "), not towns[i].startswith("смт"), towns[i]))
    for n, tid in enumerate(order, 1):
        if tid in cache["streets"]:
            continue
        found = {}
        for t in STREET_TYPES:
            crawl_streets(tid, t, found)
        cache["streets"][tid] = found
        if n % 10 == 0 or towns[tid].startswith("м. "):
            json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)
            total = sum(len(v) for v in cache["streets"].values())
            print(f"  {n}/{len(order)} towns ({towns[tid]}: {len(found)} streets), {total} streets, {calls[0]} requests", flush=True)
    json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)
    out = {"built": time.strftime("%Y-%m-%d"), "region": "dnipro",
           "towns": [{"id": int(tid), "value": towns[tid],
                      "streets": [{"id": int(k), "value": v} for k, v in cache["streets"].get(tid, {}).items()]}
                     for tid in order if cache["streets"].get(tid)]}
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"saved {OUT}: {len(out['towns'])} towns with streets, {sum(len(t['streets']) for t in out['towns'])} streets, {calls[0]} requests")


if __name__ == "__main__":
    main()
