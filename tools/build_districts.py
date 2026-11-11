"""Builds districts.json — which Kyiv district each YASNO street lies in.

YASNO knows no districts. For every street from streets.json this asks OpenStreetMap's
Nominatim geocoder ("вулиця Хрещатик, Київ") and takes the district ("Шевченківський район")
of the matching Kyiv streets. A street that crosses districts is listed in each. Streets
Nominatim can't place stay reachable through "all streets A–Я" in the bot.

Nominatim's usage policy allows at most one request a second, so a full run takes about
an hour. Answers are kept in tools/nominatim_cache.json, so an interrupted run resumes.

    python -X utf8 tools/build_districts.py      -> districts.json next to streets.json
"""
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.join(ROOT, "tools", "nominatim_cache.json")
DISTRICTS = ["Голосіївський", "Дарницький", "Деснянський", "Дніпровський", "Оболонський",
             "Печерський", "Подільський", "Святошинський", "Солом'янський", "Шевченківський"]
FULL = {"вул.": "вулиця", "пров.": "провулок", "просп.": "проспект", "бул.": "бульвар", "пл.": "площа",
        "наб.": "набережна", "ал.": "алея", "туп.": "тупик", "пр-д": "проїзд"}
UA = "svitlo-kyiv/1.0 (outage schedule; github.com/vitaliypashkovskyi-ai/svitlo)"


def split(value):
    """'вул. Сікорського Ігоря (ур. Х)' -> ('вулиця', 'Сікорського Ігоря')."""
    v = re.sub(r"\(.*?\)", "", value).strip()
    m = re.match(r"^([^\s.]{1,8}\.)\s*(\S.*)$", v)
    if m:
        return FULL.get(m.group(1), m.group(1)), m.group(2).strip()
    head, _, rest = v.partition(" ")
    if rest and head.lower() in ("площа", "шосе", "узвіз", "алея", "тупик", "проїзд", "набережна", "майдан", "урочище"):
        return head.lower(), rest
    return "", v


def queries(value):
    typ, name = split(value)
    words = name.split()
    out = [f"{typ} {name}".strip()]
    if len(words) >= 2:  # YASNO puts the surname first: 'Сікорського Ігоря' -> 'Ігоря Сікорського'
        out.append(f"{typ} {words[-1]} {' '.join(words[:-1])}".strip())
    if typ:
        out.append(name)
    return out


last_call = [0.0]


def nominatim(street):
    wait = 1.1 - (time.time() - last_call[0])
    if wait > 0:
        time.sleep(wait)
    params = {"street": street, "city": "Київ", "country": "Україна", "format": "jsonv2",
              "addressdetails": 1, "limit": 10, "accept-language": "uk"}
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    for attempt in range(4):
        try:
            last_call[0] = time.time()
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            print("   retry", street, e, file=sys.stderr)
            time.sleep(10 * (attempt + 1))
    return []


def districts_of(results):
    found = set()
    for x in results:
        a = x.get("address", {})
        if (a.get("city") or a.get("state")) != "Київ":
            continue
        for k in ("borough", "city_district", "suburb", "district"):
            name = (a.get(k) or "").replace("’", "'")
            for d in DISTRICTS:
                if name.startswith(d):
                    found.add(d)
    return found


def main():
    streets = json.load(open(os.path.join(ROOT, "streets.json"), encoding="utf-8"))["streets"]
    try:
        cache = json.load(open(CACHE, encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}
    result = {d: [] for d in DISTRICTS}
    placed = 0
    for i, s in enumerate(streets, 1):
        sid = str(s["id"])
        if sid not in cache:
            ds = set()
            for q in queries(s["value"]):
                ds = districts_of(nominatim(q))
                if ds:
                    break
            cache[sid] = sorted(ds)
            if i % 25 == 0:
                json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)
                print(f"  {i}/{len(streets)} streets, {sum(1 for v in cache.values() if v)} placed", flush=True)
        for d in cache[sid]:
            result[d].append(s["id"])
        placed += bool(cache[sid])
    json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)
    out = os.path.join(ROOT, "districts.json")
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"built": time.strftime("%Y-%m-%d"), "source": "OpenStreetMap contributors (Nominatim), ODbL",
                   "districts": {d: sorted(ids, key=int) for d, ids in result.items()}}, f, ensure_ascii=False, separators=(",", ":"))
    for d in DISTRICTS:
        print(f"  {d}: {len(result[d])} streets")
    print(f"saved {out}: {placed} of {len(streets)} streets placed ({100 * placed // len(streets)}%)")


if __name__ == "__main__":
    main()
