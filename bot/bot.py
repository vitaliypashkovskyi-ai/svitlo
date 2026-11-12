"""Світло Київ — Telegram bot (@LightUkraine_bot).

Finds a DTEK Kyiv outage queue by address (YASNO), shows the schedule with buttons,
keeps several addresses per person and sends reminders: before an outage, when power
is due back, when today's schedule changes and when tomorrow's is published.
Standard library only (sqlite3, threads) — no pip install needed.

    python bot.py                         # run (long polling, no open ports)
    python bot.py --test                  # offline self-test, no Telegram needed

Settings (environment or .env next to this file):
    BOT_TOKEN       token from @BotFather (required to run)
    ADMIN_CHAT_ID   your chat id for "data is stale" alerts (send /myid to the bot)
    DATA_DIR        where bot.db lives (default ./data)
    SITE_URL        website link for the "Website" button (empty = no button)
"""
import html
import json
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import struct
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))


def load_env():
    path = os.path.join(HERE, ".env")
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "").strip()
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(HERE, "data"))
CHANNEL = os.environ.get("CHANNEL", "@LightUkraine").strip()  # public channel for the daily digests; empty = off
SITE_URL = os.environ.get("SITE_URL", "").strip()  # the public site was taken down on 2026-11-11
BOT_USERNAME = os.environ.get("BOT_USERNAME", "LightUkraine_bot").strip().lstrip("@")  # refreshed from getMe at start


def deep_link(region, queue):
    """t.me link that opens the bot on a queue: ?start=q4_1 (Kyiv), ?start=odesa_q3_2 (other regions)."""
    return f"https://t.me/{BOT_USERNAME}?start={'' if region == 'kyiv' else region + '_'}q{queue.replace('.', '_')}"


def parse_start(param):
    """'q4_1' -> ('kyiv', '4.1'); 'kyiv-region_q2_1' -> ('kyiv-region', '2.1'); anything else -> None."""
    import re
    m = re.fullmatch(r"(?:([a-z-]+)_)?q(\d{1,2})(?:_(\d))?", param or "")
    if not m or (m.group(1) and m.group(1) not in REGIONS):
        return None
    return (m.group(1) or "kyiv"), m.group(2) + (f".{m.group(3)}" if m.group(3) else "")
DATA_BASE = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/"
YASNO = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2"
# Regions with DTEK schedules. yasno: (regionId, dsoId) for address search; cities: search asks for the town first.
REGIONS = {
    "kyiv": {"uk": "м. Київ", "en": "Kyiv city", "yasno": ("25", "902"), "cities": False,
             "dtek": "https://www.dtek-kem.com.ua/ua/shutdowns",
             "co": {"uk": "ДТЕК Київські електромережі", "en": "DTEK Kyiv Electric Networks"},
             "phones": ["1588", "0 800 501 588"], "tg": "https://t.me/DTEKKyivskielectromerezhibot",
             "viber": "https://chats.viber.com/dtekkyivskielectromerezhi", "city": ("1551", "https://t.me/Kontaktny_centre_1551_bot", "1551.gov.ua")},
    "kyiv-region": {"uk": "Київська область", "en": "Kyiv region", "yasno": None, "cities": False,
                    "dtek": "https://www.dtek-krem.com.ua/ua/shutdowns",
                    "co": {"uk": "ДТЕК Київські регіональні електромережі", "en": "DTEK Kyiv Regional Electric Networks"},
                    "phones": ["0 800 400 740", "(044) 459 07 40"], "tg": "https://t.me/DTEKKyivRegionElektromerezhiBot",
                    "viber": None, "city": None},
    "dnipro": {"uk": "Дніпропетровська область", "en": "Dnipropetrovsk region", "yasno": ("3", "301"), "cities": True,
               "dtek": "https://www.dtek-dnem.com.ua/ua/shutdowns",
               "co": {"uk": "ДТЕК Дніпровські електромережі", "en": "DTEK Dnipro Electric Networks"},
               "phones": ["0 800 301 497", "(056) 790 99 00"], "tg": "https://t.me/DTEKDniprovskiElektromerezhiBot",
               "viber": "https://chats.viber.com/dtekdniprovskielektromerezhi", "city": None},
    "odesa": {"uk": "Одеська область", "en": "Odesa region", "yasno": None, "cities": False,
              "dtek": "https://www.dtek-oem.com.ua/ua/shutdowns",
              "co": {"uk": "ДТЕК Одеські електромережі", "en": "DTEK Odesa Electric Networks"},
              "phones": ["0 800 409 090", "(048) 705 90 90"], "tg": "https://t.me/DTEKOdeskiElektromerezhiBot",
              "viber": "https://chats.viber.com/dtekodeskielektromerezhi", "city": None},
    # read straight from each oblenergo's official API ("source"); no address search there
    "lviv": {"uk": "Львівська область", "en": "Lviv region", "yasno": None, "cities": False, "source": "loe", "lookup": "loe",
             "dtek": "https://poweron.loe.lviv.ua", "co": {"uk": "Львівобленерго", "en": "Lvivoblenergo"},
             "phones": ["0 800 30 15 68"], "tg": "https://t.me/loeserviceBot", "viber": "https://www.viber.com/loebot", "city": None},
    "mykolaiv": {"uk": "Миколаївська область", "en": "Mykolaiv region", "yasno": None, "cities": False, "source": "mk", "lookup": "mk",
                 "dtek": "https://off.energy.mk.ua", "co": {"uk": "Миколаївобленерго", "en": "Mykolaivoblenergo"},
                 "phones": ["(067) 690 40 09", "(093) 170 30 79"], "tg": "https://t.me/mykolaivoblenergo_bot", "viber": None, "city": None},
    "ivano-frankivsk": {"uk": "Івано-Франківська область", "en": "Ivano-Frankivsk region", "yasno": None, "cities": False,
                        "source": "if", "dtek": "https://svitlo.oe.if.ua",
                        "co": {"uk": "Прикарпаттяобленерго", "en": "Prykarpattiaoblenergo"},
                        "phones": ["0 800 50 40 20"], "tg": "https://t.me/oeifuabot", "viber": None, "city": None},
    "cherkasy": {"uk": "Черкаська область", "en": "Cherkasy region", "yasno": None, "cities": False, "source": "ck",
                 "dtek": "https://www.cherkasyoblenergo.com", "co": {"uk": "Черкасиобленерго", "en": "Cherkasyoblenergo"},
                 "phones": ["0 800 501 328", "093 170 40 57"], "tg": "https://t.me/Cherkasyoblenergo_bot",
                 "viber": "https://chats.viber.com/cherkasyoblenergo", "city": None},
    "poltava": {"uk": "Полтавська область", "en": "Poltava region", "yasno": None, "cities": False, "source": "pl", "lookup": "pl",
                "dtek": "https://www.poe.pl.ua/disconnection/power-outages/", "co": {"uk": "Полтаваобленерго", "en": "Poltavaoblenergo"},
                "phones": ["0 800 205 035", "(0532) 510-910"], "tg": "https://t.me/PltOblEnergo_bot", "viber": None, "city": None},
    "zaporizhzhia": {"uk": "Запорізька область", "en": "Zaporizhzhia region", "yasno": None, "cities": False, "source": "zp", "lookup": "zpa",
                     "dtek": "https://www.zoe.com.ua/outage/", "co": {"uk": "Запоріжжяобленерго", "en": "Zaporizhzhiaoblenergo"},
                     "phones": ["0 800 304 502"], "tg": None, "viber": None, "city": None},
    "rivne": {"uk": "Рівненська область", "en": "Rivne region", "yasno": None, "cities": False, "source": "rv",
              "dtek": "https://www.roe.vsei.ua/disconnections", "co": {"uk": "Рівнеобленерго", "en": "Rivneoblenergo"},
              "phones": ["(0362) 69-45-55", "(067) 620-44-11"], "tg": None, "viber": None, "city": None},
    "kharkiv": {"uk": "Харківська область", "en": "Kharkiv region", "yasno": None, "cities": False, "source": "kh",
                "dtek": "https://oblenergo.kharkiv.ua/uk", "co": {"uk": "Харківобленерго", "en": "Kharkivoblenergo"},
                "phones": ["0 800 508 413", "057 34 24 413"], "tg": None, "viber": None, "city": None},
    "kirovohrad": {"uk": "Кіровоградська область", "en": "Kirovohrad region", "yasno": None, "cities": False, "source": "kr",
                   "dtek": "https://kiroe.com.ua/energy/emergency", "co": {"uk": "Кіровоградобленерго", "en": "Kirovohradoblenergo"},
                   "phones": ["0 800 501 148"], "tg": None, "viber": None, "city": None},
    "zhytomyr": {"uk": "Житомирська область", "en": "Zhytomyr region", "yasno": None, "cities": False, "source": "zt", "lookup": "zt",
                 "dtek": "https://www.ztoe.com.ua/unhooking-search.php", "co": {"uk": "Житомиробленерго", "en": "Zhytomyroblenergo"},
                 "phones": ["0 800 30 92 82", "(0412) 55-92-82"], "tg": None, "viber": None, "city": None},
    "khmelnytskyi": {"uk": "Хмельницька область", "en": "Khmelnytskyi region", "yasno": None, "cities": False, "source": "hoe",
                     "dtek": "https://hoe.com.ua/post/novini-kompaniji", "co": {"uk": "Хмельницькобленерго", "en": "Khmelnytskoblenergo"},
                     "phones": ["067 010 50 62", "050 010 50 62", "0 800 50 50 62"], "tg": None, "viber": None, "city": None},
    "volyn": {"uk": "Волинська область", "en": "Volyn region", "yasno": None, "cities": False, "source": "voe", "lookup": "voe",
              "dtek": "https://energy.volyn.ua/spozhyvacham/perervy-u-elektropostachanni/hrafik-vidkliuchen/",
              "co": {"uk": "Волиньобленерго", "en": "Volynoblenergo"},
              "phones": ["(0332) 78-48-48", "(068) 478-48-48", "(095) 478-48-48"], "tg": None, "viber": None, "city": None},
    "chernivtsi": {"uk": "Чернівецька область", "en": "Chernivtsi region", "yasno": None, "cities": False, "source": "cv", "lookup": "cvs",
                   "groups": [str(i) for i in range(1, 13)],  # 12 queues, no subqueues
                   "dtek": "https://oblenergo.cv.ua/shutdowns/", "co": {"uk": "Чернівціобленерго", "en": "Chernivtsioblenergo"},
                   "phones": ["(099) 230-99-00", "(067) 230-99-00"], "tg": None, "viber": None, "city": None},
}
STANDARD_GROUPS = [f"{q}.{s_}" for q in range(1, 7) for s_ in (1, 2)]
OE_TTL = 600  # official oblenergo APIs: every 10 minutes is plenty and stays polite

SCHEDULE_TTL = 300            # refresh the schedule every 5 min
STALE_AFTER = 3600            # alert the admin if data is older than 1 h
MAX_ADDRESSES = 5
WARN_CHOICES = (15, 30, 60)
QUIET_FROM, QUIET_TO = 23 * 60, 7 * 60
RETURN_WINDOW = 10            # "power is back" message within 10 min after the scheduled time
CACHE_TTL = {"cities": 7 * 86400, "streets": 86400, "houses": 7 * 86400, "group": 7 * 86400}
SEND_RATE = 25                # messages per second, below Telegram's ~30/s limit
BULK_RATE = 20                # of those, mass notifications use at most this many, so reminders never wait behind them
# Self-update: once an hour the bot compares these files with the GitHub repo, runs the new
# version's self-test and only then switches to it. AUTO_UPDATE=0 in .env turns it off.
AUTO_UPDATE = os.environ.get("AUTO_UPDATE", "1").strip() != "0"
UPDATE_URL = os.environ.get("UPDATE_URL", "https://raw.githubusercontent.com/vitaliypashkovskyi-ai/svitlo/main/bot/")
UPDATE_FILES = ("bot.py", "streets.json", "districts.json", "dnipro_streets.json")  # data files are optional
UPDATE_EVERY = 3600
# People's reports ("no power" / "power is on"): a report counts for an hour; an alert needs at
# least CROWD_MIN people of the same queue saying "no power" while the schedule says power is on.
REPORT_TTL = 3600
REPORT_COOLDOWN = 120
CROWD_MIN = 2
BOT_VERSION = "2026-11-12.20"  # bump on every release; shown in the log at start
STREETS_FILE = os.environ.get("STREETS_FILE", os.path.join(HERE, "streets.json"))
LETTERS = "АБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЮЯ"
PAGE = 10                     # streets per page in the list
HPAGE = 30                    # house buttons per page (6 rows of 5)
DISTRICTS_FILE = os.environ.get("DISTRICTS_FILE", os.path.join(HERE, "districts.json"))


def street_main(value):
    """'просп. Гонгадзе Георгія' -> 'Гонгадзе Георгія' (the part people look up by)."""
    import re
    m = re.match(r"^[^\s.]{1,8}\.\s*(\S.*)$", value)  # 'вул. X', 'вул.X'
    if m:
        return m.group(1)
    head, _, rest = value.partition(" ")
    return rest if rest and head in ("площа", "шосе", "узвіз", "алея", "тупик", "проїзд", "набережна", "майдан", "урочище") else value


def ua_key(s):
    order = {ch: i for i, ch in enumerate("абвгґдеєжзиіїйклмнопрстуфхцчшщьюя")}
    return [order.get(ch, 100 + ord(ch)) for ch in s.lower()]


def load_streets():
    """Full street list built by tools/build_streets.py: {letter: [(main, id, value)]}."""
    try:
        with open(STREETS_FILE, encoding="utf-8") as f:
            items = json.load(f)["streets"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return {}, {}
    by_letter, by_id = {}, {}
    for s in items:
        main = street_main(s["value"])
        first = main[:1].upper()
        letter = first if first in LETTERS else "0–9"
        by_letter.setdefault(letter, []).append((main, str(s["id"]), s["value"]))
        by_id[str(s["id"])] = s["value"]
    for v in by_letter.values():
        v.sort(key=lambda x: (ua_key(x[0]), x[2]))
    return by_letter, by_id


STREETS, STREET_BY_ID = load_streets()


def load_districts():
    """Kyiv districts built by tools/build_districts.py: [(name, {street ids})]. Empty if no file."""
    try:
        with open(DISTRICTS_FILE, encoding="utf-8") as f:
            d = json.load(f)["districts"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return []
    return [(name, {str(i) for i in ids}) for name, ids in d.items() if ids]


DISTRICTS = load_districts()
DNIPRO_FILE = os.environ.get("DNIPRO_FILE", os.path.join(HERE, "dnipro_streets.json"))


def load_dnipro():
    """Towns of Dnipropetrovsk region with their streets (tools/build_dnipro.py).
    Returns {town_id: (town_name, {letter: [(main, street_id, street_value)]}, {street_id: value})}."""
    try:
        with open(DNIPRO_FILE, encoding="utf-8") as f:
            towns = json.load(f)["towns"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return {}
    out = {}
    for tw in towns:
        by_letter, by_id = {}, {}
        for st in tw["streets"]:
            main = street_main(st["value"])
            first = main[:1].upper()
            by_letter.setdefault(first if first in LETTERS else "0–9", []).append((main, str(st["id"]), st["value"]))
            by_id[str(st["id"])] = st["value"]
        for v in by_letter.values():
            v.sort(key=lambda x: (ua_key(x[0]), x[2]))
        out[str(tw["id"])] = (tw["value"], by_letter, by_id)
    return out


def town_main(value):
    """'м. Дніпро' -> 'Дніпро', 'с-ще Вишневе' -> 'Вишневе', 'смт Губиниха' -> 'Губиниха'."""
    for pre in ("м. ", "с. ", "смт ", "с-ще ", "сел. "):
        if value.startswith(pre):
            return value[len(pre):].strip()
    return value


DNIPRO = load_dnipro()

# ===================================================================== texts
T = {
    "uk": {
        "welcome": "Привіт! Я показую графік відключень світла у Києві та областях — Київській, Дніпропетровській, Одеській, Львівській, Миколаївській, Івано-Франківській, Черкаській і Полтавській (/region).\n\nНапишіть назву вашої вулиці українською, наприклад <b>Хрещатик</b>, і я знайду вашу чергу.\n\nАбо одразу вкажіть чергу: <code>/queue 4.1</code>",
        "ask_street": "Напишіть назву вулиці українською, наприклад <b>Хрещатик</b>.",
        "help": "Що я вмію:\n/now — графік зараз\n/address — мої адреси (до 5)\n/region — вибрати регіон\n/settings — нагадування й мова\n/queue 4.1 — додати чергу вручну\n\nНапишіть назву вулиці, щоб додати адресу.",
        "b_region": "📍 Регіон: {r}", "pick_region": "Виберіть регіон. Графіки — з офіційних джерел ДТЕК та обленерго:",
        "region_set": "Регіон: <b>{r}</b>.",
        "region_no_search": "Пошук за адресою для регіону «{r}» недоступний. Дізнайтеся свою чергу на <a href=\"{url}\">сайті ДТЕК</a> і надішліть її, наприклад <code>/queue 3.2</code>",
        "queue_pick": "<b>{r}</b>\nЧергу за адресою знає лише {co}, тому:\n\n1️⃣ Дізнайтеся свою чергу на сайті {co}{bot} — кнопки нижче.\n2️⃣ Натисніть свою чергу тут.\n\nДалі все як завжди: графік, нагадування, повідомлення про аварії.",
        "b_dtek_queue_site": "🌐 Дізнатися чергу на сайті", "b_dtek_queue_bot": "💬 Чат-бот {co}", "or_bot": " або в їхньому чат-боті",
        "ask_city": "Напишіть назву населеного пункту, наприклад <b>Дніпро</b>.\n<i>Пошук працює для адрес ДТЕК Дніпровські електромережі.</i>",
        "pick_city": "Виберіть населений пункт:", "no_cities": "Не знайшов такого населеного пункту. Напишіть хоча б 3 літери назви.",
        "ask_street_in": "<b>{city}</b>\nТепер напишіть назву вулиці.",
        "short": "Напишіть хоча б 3 літери назви вулиці або виберіть її зі списку.",
        "b_list": "🔤 Вибрати зі списку", "b_letters": "🔤 Літери", "b_find": "🔎 Знайти вулицю",
        "b_cities": "🏙 Міста", "b_villages": "🏘 Села та селища А–Я", "b_towns": "⬅️ Населені пункти",
        "pick_town": "Виберіть місто або натисніть «Села та селища А–Я»:\n<i>Пошук працює для адрес ДТЕК Дніпровські електромережі.</i>",
        "village_letter": "Виберіть першу літеру назви села чи селища:",
        "village_page": "Села та селища на «{l}» · {p}/{n}",
        "town_letters": "<b>{t}</b> · {n} вулиць\nВиберіть першу літеру назви вулиці:",
        "town_page": "<b>{t}</b> · вулиці на «{l}» · {p}/{n}",
        "town_streets": "<b>{t}</b> · виберіть вулицю:",
        "inline_kyiv": "м. Київ", "inline_hint": "Почніть писати назву вулиці…", "inline_none": "Не знайшов такої вулиці",
        "inline_none_d": "Спробуйте інакше: без «вул.» або лише частину назви",
        "pick_letter": "Виберіть першу літеру назви вулиці.\n<i>Наприклад, для «просп. Гонгадзе Георгія» — літера Г.</i>",
        "letter_page": "Вулиці на «{l}» · сторінка {p} з {n}",
        "no_streets": "Не знайшов такої вулиці. Спробуйте без «вул.» або лише частину назви.",
        "pick_street": "Виберіть вулицю:",
        "ask_house": "<b>{street}</b>\nТепер напишіть номер будинку, наприклад <code>22</code>.",
        "no_houses": "Не знайшов будинок «{q}» на цій вулиці. Спробуйте лише номер, без літери чи корпусу.",
        "pick_house": "Виберіть будинок:",
        "loading_houses": "<b>{street}</b>\n⏳ Завантажую номери будинків…",
        "houses_page": "<b>{street}</b> · {n}\nВиберіть номер будинку або напишіть його:",
        "houses_n": lambda n: f"{n} {'будинок' if n % 10 == 1 and n % 100 != 11 else 'будинки' if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else 'будинків'}",
        "b_other_street": "⬅️ Інша вулиця",
        "pick_district": "Виберіть район Києва:\n<i>Не знаєте район? Натисніть «Усі вулиці А–Я».</i>",
        "b_all_streets": "🔤 Усі вулиці А–Я", "b_districts": "⬅️ Райони",
        "district_letters": "<b>{d} район</b> · {n} вулиць\nВиберіть першу літеру назви вулиці:",
        "district_page": "<b>{d} район</b> · вулиці на «{l}» · {p}/{n}",
        "no_group": "Для цієї адреси YASNO не вказує чергу. Спробуйте сусідній будинок або вкажіть чергу: <code>/queue 4.1</code>",
        "yasno_err": "Сервіс пошуку адрес зараз не відповідає. Спробуйте пізніше або вкажіть чергу: <code>/queue 4.1</code>",
        "too_many": "Можна зберегти до {n} адрес. Видаліть одну в /address.",
        "need_addr": "Спершу додайте адресу — напишіть назву вулиці. Або чергу: <code>/queue 4.1</code>",
        "bad_queue": "Не знаю такої черги. Приклад: <code>/queue 4.1</code>",
        "queue_label": "Черга {q}",
        "no_data": "Не вдалося завантажити графік. Спробуйте за кілька хвилин.",
        "title": "Черга {q}",
        "on_now": "🟢 Зараз світло є", "off_now": "⚫ Зараз світла немає", "maybe_now": "🟡 Зараз можливе відключення",
        "next_off": "відключення {d}о <b>{t}</b> (через {left})", "next_on": "увімкнуть {d}о <b>{t}</b> (через {left})",
        "next_maybe": "можливе відключення {d}о <b>{t}</b> (через {left})", "tomorrow_word": "завтра ",
        "no_change": "змін до кінця графіка немає",
        "today": "Сьогодні", "tomorrow": "Завтра", "no_outages": "відключень немає", "off_total": "без світла {d}",
        "not_published": "графік ще не опубліковано", "maybe": "можливо",
        "bar_legend": "▰ = 30 хв без світла", "bar_legend_maybe": ", ▱ = можливе відключення",
        "updated_at": "Оновлено {t}",
        "dur": lambda h, m: f"{h} год {m:02d} хв" if h else f"{m} хв",
        # buttons
        "b_refresh": "🔄 Оновити", "b_site": "🌐 Сайт", "b_addr": "🏠 Адреси", "b_settings": "⚙️ Налаштування",
        "b_rep_off": "⚡ У мене немає світла", "b_rep_on": "💡 Світло є",
        "b_share": "📤 Поділитися",
        "share_text": "Графік відключень світла для черги {q} ({r}) і нагадування перед відключенням — у Telegram-боті:",
        "share_street": "Сусіди! Графік відключень світла для «{street}»: відкрийте бота, виберіть свій будинок — і отримуйте нагадування перед відключенням.",
        "deep_added": "Додав чергу {q} ({r}). Щоб знайти свою чергу за адресою — /address",
        "g_welcome": "👋 <b>Привіт!</b> Я надсилатиму в цей чат графік відключень світла для вашого будинку: графік на завтра, попередження за 30 хв до відключення і коли світло має повернутися. Вночі (23:00–07:00) мовчу.\n\n<b>Адмін чату, вкажіть адресу будинку:</b>\n<code>/address Шевченка</code> — назва вулиці, далі виберіть вулицю й будинок кнопками.\nЯкщо будинок не в Києві — спершу /region.\nЯкщо я попрошу щось написати — відповідайте (Reply) на моє повідомлення.\n\n/now — графік зараз · /pic — картинка",
        "g_admin_only": "Налаштовувати бота в цьому чаті може лише адмін чату.",
        "g_admin_toast": "Лише адмін чату",
        "b_group": "➕ Додати бота в чат будинку",
        "group_info": "<b>Бот у чаті вашого будинку</b>\n\nДодайте мене в груповий чат будинку чи ОСББ — і я сам надсилатиму туди графік на завтра, попередження перед відключенням і коли світло має повернутися. Сусідам нічого не треба встановлювати.\n\nНатисніть кнопку нижче, виберіть чат, а потім адмін чату напише мені адресу.",
        "b_img": "🖼 Картинка", "img_caption": "<b>Черга {q}</b> · {label}\nЗелене — світло є, червоне — немає, жовте — можливо.",
        "b_dtek": "📞 Повідомити ДТЕК", "b_oe": "📞 Повідомити обленерго",
        "lookup_hint": "\n\n🔎 <b>Або знайдіть чергу за адресою:</b> напишіть назву населеного пункту, наприклад <b>Львів</b>.",
        "zpa_hint": "\n\n🔎 <b>Або знайдіть чергу за адресою:</b> напишіть вулицю (м. Запоріжжя), наприклад <b>Авраменка</b>, або населений пункт Запорізького району і вулицю через кому: <b>Сонячне, Ясна</b>.",
        "zp_ask_house": "<b>{label}</b>\nНапишіть номер будинку, наприклад <b>16</b> або <b>12а</b>.",
        "zp_many": "За списками {co} будинок <b>{h}</b> на «{street}» має кілька записів (різні під'їзди чи лінії):\n{lines}\nВиберіть свою чергу:",
        "zp_no_house": "Будинку <b>{h}</b> на «{street}» немає у списках {co}. Перевірте номер або виберіть чергу кнопками.",
        "zp_none": "У списках {co} немає вулиці «{q}». Напишіть частину назви, наприклад <b>Шевченка</b>, або для району — «населений пункт, вулиця».",
        "voe_hint": "\n\n🔎 <b>Або знайдіть чергу за адресою:</b> напишіть населений пункт і вулицю через кому, наприклад <b>Луцьк, Волі</b>.",
        "voe_ask_street": "Населений пункт: <b>{city}</b>. Тепер напишіть назву вулиці, наприклад <b>Шевченка</b>.",
        "voe_not_in": "🟢 Будинок {h}: за даними {co} він <b>не залучений</b> до графіків погодинних відключень — за графіком світло тут не вимикають.\n<i>Аварійні відключення все одно можливі.</i>",
        "voe_none": "{co} не знайшло вулицю «{street}» у «{city}». Перевірте назву (без «вул.», повністю) або виберіть чергу кнопками.",
        "b_by_address": "🔎 Знайти чергу за адресою", "mk_filial": "Виберіть дільницю Миколаївобленерго.\n<i>Місто Миколаїв — «Миколаївський РЕМ».</i>",
        "cvs_filial": "Виберіть дільницю Чернівціобленерго.\n<i>Місто Чернівці — «ЧЕРНІВЦІ».</i>",
        "zt_filial": "Виберіть РЕМ Житомиробленерго.\n<i>Місто Житомир — «Житомирський».</i>",
        "pl_filial": "Виберіть філію або дільницю Полтаваобленерго.\n<i>Місто Полтава — «Полтавська об’єднана філія».</i>",
        "mk_level": {"ns": "Виберіть населений пункт", "street": "Виберіть вулицю"},
        "mk_letters": "{what}: виберіть першу літеру або напишіть назву.", "mk_page": "{what} · {p}/{n}", "b_back_level": "⬅️ Назад",
        "loe_no_queue": "Для будинку {h} {co} не вказує черги графіка ({v}). Виберіть чергу кнопками або перевірте на сайті {co}.",
        "loe_busy": "Довідник адрес {co} зараз не відповідає. Спробуйте за хвилину або виберіть чергу кнопками.", "b_dtek_tg": "💬 Чат-бот {co} (Telegram)", "b_dtek_viber": "💬 Viber {co}",
        "b_dtek_site": "🌐 Сайт ДТЕК", "b_city": "🏙 Звернення {n} (Telegram)",
        "dtek_title": "<b>Повідомити {co}</b>\nЗаявку подаєте ви самі — так вона потрапить у роботу ДТЕК.",
        "dtek_phones": "📞 Телефони: {p}", "dtek_free": " (безкоштовно)",
        "dtek_bots": "💬 Або через офіційний чат-бот {co} — кнопки нижче.", "oe_site": "🌐 Подати звернення можна на офіційному сайті — кнопка нижче.",
        "dtek_text_title": "Готовий текст заявки (натисніть, щоб скопіювати):",
        "dtek_text": "Немає світла за адресою: {addr} (черга {q}){since}.{sched}",
        "dtek_since": ", з {t}", "dtek_sched": " За графіком світло має бути.",
        "dtek_city": "🏙 Якщо проблема не в електромережі (будинок, двір) — Контактний центр міста: {n}, Telegram-бот (кнопка нижче) або сайт {site}.",
        "dtek_112": "🚨 Якщо через відсутність світла під загрозою життя чи здоров'я — телефонуйте 112.",
        "rep_thanks_off": "Дякую! Врахували: світла немає.", "rep_thanks_on": "Дякую! Врахували: світло є.",
        "rep_wait": "Ви вже повідомили. Можна змінити за кілька хвилин.",
        "crowd_off": "⚠️ <b>Схоже на аварійне відключення:</b> {n} з черги {q} повідомили, що світла немає, хоча за графіком воно має бути.",
        "crowd_on": "💡 {n} з черги {q} повідомили, що світло вже є.",
        "crowd_counts": "<i>Повідомлення людей за годину: ⚫ {off} · 💡 {on}</i>",
        "notice_emergency": "⚠️ <b>{co}: з {t} діють графіки аварійних відключень (ГАВ).</b> Розклад нижче зараз може не виконуватися — <a href=\"{url}\">деталі</a>.",
        "notice_cancel": "⚠️ <b>{co} повідомило ({t}): графіки погодинних відключень скасовано.</b> Розклад нижче може не відповідати дійсності — <a href=\"{url}\">деталі</a>.",
        "crowd_alert": "⚠️ <b>Можливе аварійне відключення</b> у черзі {q}: {n} повідомили, що світла немає, хоча за графіком воно має бути.\nЯкщо у вас світло є — натисніть «💡 Світло є».",
        "people": lambda n: f"{n} {'людина' if n % 10 == 1 and n % 100 != 11 else 'людини' if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else 'людей'}",
        "b_add": "➕ Додати адресу", "b_back": "⬅️ Назад",
        # addresses
        "addr_title": "<b>Ваші адреси</b> (до {n}). Натисніть, щоб зробити адресу основною. Нагадування приходять для всіх адрес.",
        "addr_empty": "Адрес ще немає.",
        "addr_saved": "Адресу збережено: {label} — черга {q}.",
        "addr_deleted": "Адресу видалено.",
        # settings
        "set_title": "<b>Налаштування</b>",
        "s_notify": "🔔 Нагадування", "s_ret": "💡 Коли світло повертається", "s_quiet": "🌙 Тихі години 23:00–07:00",
        "s_before": "За скільки попереджати:", "s_min": "{m} хв", "on": "увімк.", "off": "вимк.",
        "s_lang": "🇬🇧 English",
        # notifications
        "warn": "⚠️ Через {m} хв відключення світла ({t}).",
        "charge": "🔋 <b>Зарядіть телефон і павербанк.</b>\nО {t} вимкнуть світло на <b>{h}</b> (до {end}).",
        "now_title": "<b>Зараз, {t}</b>", "now_on": "🟢 є до {t}", "now_off": "🔴 немає до {t}", "now_maybe": "🟡 можливо немає до {t}",
        "now_on_end": "🟢 є", "now_off_end": "🔴 немає", "now_none": "графік не опубліковано", "now_noqueue": "такої черги немає в графіку — перевірте адресу",
        "week_title": "<b>Ваш тиждень</b> · {label} · черга {q}",
        "week_total": "Без світла <b>{h}</b> за 7 днів.", "week_more": " Це на {d} більше, ніж тижнем раніше.",
        "week_less": " Це на {d} менше, ніж тижнем раніше.", "week_same": "",
        "week_worst": "Найважчий день — {day} ({h}).", "week_none": "Тижневої статистики ще немає — вона з'явиться через кілька днів.",
        "days_short": ["пн", "вт", "ср", "чт", "пт", "сб", "нд"], "days_full": ["понеділок", "вівторок", "середа", "четвер", "пʼятниця", "субота", "неділя"],
        "ret": "💡 За графіком світло мало повернутися о {t}.",
        "changed_today": "🔁 ДТЕК змінив графік на сьогодні.",
        "tomorrow_published": "📅 Опубліковано графік на завтра.",
        "tomorrow_changed": "🔁 ДТЕК змінив графік на завтра.",
        "for_addr": "{label} · черга {q}",
        "stale": "⚠️ Дані графіка не оновлювалися {m} хв. Останнє оновлення: {t}.",
        "stale_ok": "✅ Дані графіка знову оновлюються.",
        "myid": "Ваш chat id: <code>{id}</code>\nДодайте в .env: <code>ADMIN_CHAT_ID={id}</code>",
        "stats": "📊 <b>Статистика бота</b>\n\n👥 Користувачів: <b>{total}</b>\n✅ Активних: {active}\n🚫 Заблокували бота: {blocked}\n"
                 "🏠 З адресою: {with_addr}\n🔔 З нагадуваннями: {notify}\n🆕 Нових за добу: {d1} · за тиждень: {d7}\n\n"
                 "📍 <b>Регіони</b>\n{regions}\n\n🗂 Адрес усього: {addrs}\n🔢 Популярні черги: {queues}\n🌐 Мова: {langs}",
        "commands": [("now", "Графік зараз"), ("all", "Усі мої адреси зараз"), ("group", "Бот у чаті будинку"), ("pic", "Картинка графіка"), ("week", "Мій тиждень"), ("address", "Мої адреси"), ("region", "Вибрати регіон"), ("settings", "Нагадування й мова"),
                     ("queue", "Додати чергу вручну, напр. /queue 4.1"), ("help", "Допомога")],
    },
    "en": {
        "welcome": "Hi! I show power outage schedules for Kyiv and the Kyiv, Dnipropetrovsk, Odesa, Lviv, Mykolaiv, Ivano-Frankivsk, Cherkasy and Poltava regions (/region).\n\nType your street name in Ukrainian, for example <b>Хрещатик</b>, and I'll find your queue.\n\nOr set the queue directly: <code>/queue 4.1</code>",
        "ask_street": "Type the street name in Ukrainian, for example <b>Хрещатик</b>.",
        "help": "What I can do:\n/now — schedule now\n/address — my addresses (up to 5)\n/region — choose the region\n/settings — reminders and language\n/queue 4.1 — add a queue manually\n\nType a street name to add an address.",
        "b_region": "📍 Region: {r}", "pick_region": "Pick your region. Schedules come from DTEK and the oblenergos' official sources:",
        "region_set": "Region: <b>{r}</b>.",
        "region_no_search": "Address search isn't available for {r}. Look up your queue on the <a href=\"{url}\">DTEK website</a> and send it, for example <code>/queue 3.2</code>",
        "queue_pick": "<b>{r}</b>\nOnly {co} knows which queue an address is in, so:\n\n1️⃣ Look up your queue on the {co} website{bot} — buttons below.\n2️⃣ Tap your queue here.\n\nThen everything works as usual: schedule, reminders, outage reports.",
        "b_dtek_queue_site": "🌐 Find my queue on their site", "b_dtek_queue_bot": "💬 {co} chatbot", "or_bot": " or chatbot",
        "ask_city": "Type the town or village name in Ukrainian, for example <b>Дніпро</b>.\n<i>Search covers addresses served by DTEK Dnipro Electric Networks.</i>",
        "pick_city": "Pick the town or village:", "no_cities": "No such town found. Type at least 3 letters of the name.",
        "ask_street_in": "<b>{city}</b>\nNow type the street name.",
        "short": "Type at least 3 letters of the street name, or pick it from the list.",
        "b_list": "🔤 Pick from the list", "b_letters": "🔤 Letters", "b_find": "🔎 Find a street",
        "b_cities": "🏙 Cities", "b_villages": "🏘 Villages A–Я", "b_towns": "⬅️ Towns",
        "pick_town": "Pick a city or tap «Villages A–Я»:\n<i>Search covers addresses served by DTEK Dnipro Electric Networks.</i>",
        "village_letter": "Pick the first letter of the village name:",
        "village_page": "Villages starting with «{l}» · {p}/{n}",
        "town_letters": "<b>{t}</b> · {n} streets\nPick the first letter of the street name:",
        "town_page": "<b>{t}</b> · streets starting with «{l}» · {p}/{n}",
        "town_streets": "<b>{t}</b> · pick your street:",
        "inline_kyiv": "Kyiv city", "inline_hint": "Start typing a street name in Ukrainian…", "inline_none": "No such street",
        "inline_none_d": "Try without «вул.» or just part of the name",
        "pick_letter": "Pick the first letter of the street name.\n<i>For example, «просп. Гонгадзе Георгія» is under Г.</i>",
        "letter_page": "Streets starting with «{l}» · page {p} of {n}",
        "no_streets": "No such street found. Try in Ukrainian, without «вул.», or just part of the name.",
        "pick_street": "Pick your street:",
        "ask_house": "<b>{street}</b>\nNow type the house number, for example <code>22</code>.",
        "no_houses": "House «{q}» not found on this street. Try just the number, without a letter or block.",
        "pick_house": "Pick your house:",
        "loading_houses": "<b>{street}</b>\n⏳ Loading house numbers…",
        "houses_page": "<b>{street}</b> · {n}\nPick the house number or type it:",
        "houses_n": lambda n: f"{n} {'house' if n == 1 else 'houses'}",
        "b_other_street": "⬅️ Another street",
        "pick_district": "Pick a Kyiv district:\n<i>Not sure? Tap «All streets A–Я».</i>",
        "b_all_streets": "🔤 All streets A–Я", "b_districts": "⬅️ Districts",
        "district_letters": "<b>{d} district</b> · {n} streets\nPick the first letter of the street name:",
        "district_page": "<b>{d} district</b> · streets starting with «{l}» · {p}/{n}",
        "no_group": "YASNO lists no queue for this address. Try a neighbouring house or set the queue: <code>/queue 4.1</code>",
        "yasno_err": "The address service isn't responding. Try later or set the queue: <code>/queue 4.1</code>",
        "too_many": "You can save up to {n} addresses. Delete one in /address.",
        "need_addr": "First add an address — type a street name. Or a queue: <code>/queue 4.1</code>",
        "bad_queue": "Unknown queue. Example: <code>/queue 4.1</code>",
        "queue_label": "Queue {q}",
        "no_data": "Couldn't load the schedule. Try again in a few minutes.",
        "title": "Queue {q}",
        "on_now": "🟢 Power is on now", "off_now": "⚫ Power is off now", "maybe_now": "🟡 Possible outage now",
        "next_off": "outage {d}at <b>{t}</b> (in {left})", "next_on": "back on {d}at <b>{t}</b> (in {left})",
        "next_maybe": "possible outage {d}at <b>{t}</b> (in {left})", "tomorrow_word": "tomorrow ",
        "no_change": "no changes until the end of the schedule",
        "today": "Today", "tomorrow": "Tomorrow", "no_outages": "no outages", "off_total": "{d} without power",
        "not_published": "not published yet", "maybe": "possible",
        "bar_legend": "▰ = 30 min without power", "bar_legend_maybe": ", ▱ = possible outage",
        "updated_at": "Updated {t}",
        "dur": lambda h, m: f"{h} h {m:02d} min" if h else f"{m} min",
        "b_refresh": "🔄 Refresh", "b_site": "🌐 Website", "b_addr": "🏠 Addresses", "b_settings": "⚙️ Settings",
        "b_rep_off": "⚡ No power here", "b_rep_on": "💡 Power is on",
        "b_share": "📤 Share",
        "share_text": "Power outage schedule for queue {q} ({r}) and reminders before outages — in this Telegram bot:",
        "share_street": "Neighbours! The power outage schedule for «{street}»: open the bot, pick your house and get reminders before outages.",
        "deep_added": "Added queue {q} ({r}). To find your own queue by address — /address",
        "g_welcome": "👋 <b>Hi!</b> I'll post this building's power outage schedule here: tomorrow's schedule, a warning 30 min before an outage and when the power should be back. I stay quiet at night (23:00–07:00).\n\n<b>Chat admin, set the building's address:</b>\n<code>/address Шевченка</code> — the street name, then pick the street and house with the buttons.\nIf it's not in Kyiv, use /region first.\nIf I ask you to type something, reply to my message.\n\n/now — schedule now · /pic — picture",
        "g_admin_only": "Only this chat's admins can set up the bot here.",
        "g_admin_toast": "Chat admins only",
        "b_group": "➕ Add the bot to your building's chat",
        "group_info": "<b>The bot in your building's chat</b>\n\nAdd me to your building's group chat and I'll post tomorrow's schedule, warnings before outages and when the power should be back. Neighbours don't need to install anything.\n\nTap the button below, pick the chat, then a chat admin sends me the address.",
        "b_img": "🖼 Picture", "img_caption": "<b>Queue {q}</b> · {label}\nGreen — power on, red — off, yellow — maybe.",
        "b_dtek": "📞 Report to DTEK", "b_oe": "📞 Report to the grid operator",
        "lookup_hint": "\n\n🔎 <b>Or find the queue by address:</b> type the town name in Ukrainian, e.g. <b>Львів</b>.",
        "zpa_hint": "\n\n🔎 <b>Or find the queue by address:</b> type the street in Ukrainian (Zaporizhzhia city), e.g. <b>Авраменка</b>, or a Zaporizhzhia district town and street separated by a comma: <b>Сонячне, Ясна</b>.",
        "zp_ask_house": "<b>{label}</b>\nType the house number, e.g. <b>16</b> or <b>12а</b>.",
        "zp_many": "{co}'s lists have several entries for house <b>{h}</b> on «{street}» (different entrances or lines):\n{lines}\nPick your queue:",
        "zp_no_house": "House <b>{h}</b> on «{street}» isn't in {co}'s lists. Check the number or pick the queue with the buttons.",
        "zp_none": "{co}'s lists have no street «{q}». Type part of the name, e.g. <b>Шевченка</b>, or for the district «town, street».",
        "voe_hint": "\n\n🔎 <b>Or find the queue by address:</b> type the town and street in Ukrainian, separated by a comma, e.g. <b>Луцьк, Волі</b>.",
        "voe_ask_street": "Town: <b>{city}</b>. Now type the street name in Ukrainian, e.g. <b>Шевченка</b>.",
        "voe_not_in": "🟢 House {h}: {co} says it is <b>not included</b> in the hourly outage schedules, so the power isn't cut here by schedule.\n<i>Emergency outages are still possible.</i>",
        "voe_none": "{co} found no street «{street}» in «{city}». Check the name (full, without «вул.») or pick the queue with the buttons.",
        "b_by_address": "🔎 Find the queue by address", "mk_filial": "Pick the Mykolaivoblenergo district office.\n<i>Mykolaiv city is «Миколаївський РЕМ».</i>",
        "cvs_filial": "Pick the Chernivtsioblenergo district office.\n<i>Chernivtsi city is «ЧЕРНІВЦІ».</i>",
        "zt_filial": "Pick the Zhytomyroblenergo district office (РЕМ).\n<i>Zhytomyr city is «Житомирський».</i>",
        "pl_filial": "Pick the Poltavaoblenergo branch or district office.\n<i>Poltava city is «Полтавська об’єднана філія».</i>",
        "mk_level": {"ns": "Pick the town or village", "street": "Pick the street"},
        "mk_letters": "{what}: pick the first letter or type the name.", "mk_page": "{what} · {p}/{n}", "b_back_level": "⬅️ Back",
        "loe_no_queue": "{co} lists no schedule queue for house {h} ({v}). Pick the queue with the buttons or check on the {co} website.",
        "loe_busy": "The {co} address directory isn't responding. Try again in a minute or pick the queue with the buttons.", "b_dtek_tg": "💬 {co} chatbot (Telegram)", "b_dtek_viber": "💬 {co} on Viber",
        "b_dtek_site": "🌐 DTEK website", "b_city": "🏙 City request {n} (Telegram)",
        "dtek_title": "<b>Report to {co}</b>\nPlease send the report yourself — that's how it gets to DTEK's crews.",
        "dtek_phones": "📞 Phone: {p}", "dtek_free": " (free)",
        "dtek_bots": "💬 Or use the official {co} chatbot — buttons below.", "oe_site": "🌐 You can file a report on the official website — button below.",
        "dtek_text_title": "Ready-made text (tap to copy; DTEK works in Ukrainian):",
        "dtek_text": "Немає світла за адресою: {addr} (черга {q}){since}.{sched}",
        "dtek_since": ", з {t}", "dtek_sched": " За графіком світло має бути.",
        "dtek_city": "🏙 If the problem isn't the grid (your building or yard) — the city contact centre: {n}, its Telegram bot (button below) or {site}.",
        "dtek_112": "🚨 If lives or health are at risk because of the outage — call 112.",
        "rep_thanks_off": "Thanks! Noted: no power.", "rep_thanks_on": "Thanks! Noted: power is on.",
        "rep_wait": "You've already reported. You can change it in a few minutes.",
        "crowd_off": "⚠️ <b>Looks like an emergency outage:</b> {n} in queue {q} reported no power, although the schedule says it should be on.",
        "crowd_on": "💡 {n} in queue {q} reported that power is already back.",
        "crowd_counts": "<i>People's reports in the last hour: ⚫ {off} · 💡 {on}</i>",
        "notice_emergency": "⚠️ <b>{co}: emergency outage schedules (ГАВ) apply since {t}.</b> The schedule below may not hold right now — <a href=\"{url}\">details</a>.",
        "notice_cancel": "⚠️ <b>{co} announced ({t}) that the hourly outage schedules are cancelled.</b> The schedule below may not be accurate — <a href=\"{url}\">details</a>.",
        "crowd_alert": "⚠️ <b>Possible emergency outage</b> in queue {q}: {n} reported no power, although the schedule says it should be on.\nIf you do have power, tap «💡 Power is on».",
        "people": lambda n: f"{n} {'person' if n == 1 else 'people'}",
        "b_add": "➕ Add address", "b_back": "⬅️ Back",
        "addr_title": "<b>Your addresses</b> (up to {n}). Tap one to make it the main one. Reminders come for all of them.",
        "addr_empty": "No addresses yet.",
        "addr_saved": "Address saved: {label} — queue {q}.",
        "addr_deleted": "Address deleted.",
        "set_title": "<b>Settings</b>",
        "s_notify": "🔔 Reminders", "s_ret": "💡 When power comes back", "s_quiet": "🌙 Quiet hours 23:00–07:00",
        "s_before": "Warn me before an outage:", "s_min": "{m} min", "on": "on", "off": "off",
        "s_lang": "🇺🇦 Українська",
        "warn": "⚠️ Power outage in {m} min ({t}).",
        "charge": "🔋 <b>Charge your phone and power bank.</b>\nThe power goes off at {t} for <b>{h}</b> (until {end}).",
        "now_title": "<b>Now, {t}</b>", "now_on": "🟢 on until {t}", "now_off": "🔴 off until {t}", "now_maybe": "🟡 maybe off until {t}",
        "now_on_end": "🟢 on", "now_off_end": "🔴 off", "now_none": "schedule not published", "now_noqueue": "this queue isn't in the schedule — check the address",
        "week_title": "<b>Your week</b> · {label} · queue {q}",
        "week_total": "No power for <b>{h}</b> in 7 days.", "week_more": " That's {d} more than the week before.",
        "week_less": " That's {d} less than the week before.", "week_same": "",
        "week_worst": "The hardest day was {day} ({h}).", "week_none": "No weekly stats yet — they'll appear in a few days.",
        "days_short": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"], "days_full": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
        "ret": "💡 Power was scheduled to come back at {t}.",
        "changed_today": "🔁 DTEK changed today's schedule.",
        "tomorrow_published": "📅 Tomorrow's schedule is out.",
        "tomorrow_changed": "🔁 DTEK changed tomorrow's schedule.",
        "for_addr": "{label} · queue {q}",
        "stale": "⚠️ Schedule data hasn't updated for {m} min. Last update: {t}.",
        "stale_ok": "✅ Schedule data is updating again.",
        "myid": "Your chat id: <code>{id}</code>\nAdd to .env: <code>ADMIN_CHAT_ID={id}</code>",
        "stats": "📊 <b>Bot stats</b>\n\n👥 Users: <b>{total}</b>\n✅ Active: {active}\n🚫 Blocked the bot: {blocked}\n"
                 "🏠 With an address: {with_addr}\n🔔 With reminders: {notify}\n🆕 New in 24 h: {d1} · in 7 days: {d7}\n\n"
                 "📍 <b>Regions</b>\n{regions}\n\n🗂 Addresses in total: {addrs}\n🔢 Top queues: {queues}\n🌐 Language: {langs}",
        "commands": [("now", "Schedule now"), ("all", "All my addresses now"), ("group", "The bot in your building's chat"), ("pic", "Schedule picture"), ("week", "My week"), ("address", "My addresses"), ("region", "Choose the region"), ("settings", "Reminders and language"),
                     ("queue", "Add a queue manually, e.g. /queue 4.1"), ("help", "Help")],
    },
}


def log(*a):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


# ===================================================================== http
def http_json(url, data=None, timeout=20):
    headers = {"User-Agent": "svitlo-kyiv-bot/2.0", "Accept": "application/json"}
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    with urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers), timeout=timeout) as r:
        return json.loads(r.read())


# ===================================================================== storage (SQLite)
class DB:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.con = sqlite3.connect(path, check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.con.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users(chat_id INTEGER PRIMARY KEY, lang TEXT DEFAULT 'uk',
                notify INTEGER DEFAULT 1, warn_min INTEGER DEFAULT 30, ret INTEGER DEFAULT 0,
                quiet INTEGER DEFAULT 0, active INTEGER, state TEXT, blocked INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS addresses(id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER,
                label TEXT, queue TEXT, created INTEGER);
            CREATE INDEX IF NOT EXISTS addr_chat ON addresses(chat_id);
            CREATE INDEX IF NOT EXISTS addr_queue ON addresses(queue);
            CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, value TEXT, at INTEGER);
            CREATE TABLE IF NOT EXISTS queue_state(queue TEXT PRIMARY KEY, day_ts INTEGER, today_sig TEXT,
                tom_ts INTEGER, tom_sig TEXT);
            CREATE TABLE IF NOT EXISTS sent(chat_id INTEGER, mark TEXT, at INTEGER, PRIMARY KEY(chat_id, mark));
            CREATE TABLE IF NOT EXISTS pending(chat_id INTEGER, key TEXT, text TEXT, at INTEGER, PRIMARY KEY(chat_id, key));
            CREATE TABLE IF NOT EXISTS history(region TEXT, queue TEXT, day_ts INTEGER, off_min INTEGER,
                PRIMARY KEY(region, queue, day_ts));
            CREATE TABLE IF NOT EXISTS pubs(region TEXT, day_ts INTEGER, at INTEGER, changes INTEGER DEFAULT 0,
                PRIMARY KEY(region, day_ts));
            CREATE TABLE IF NOT EXISTS reports(chat_id INTEGER, region TEXT, queue TEXT, state TEXT, at INTEGER,
                PRIMARY KEY(chat_id, region, queue));
            """)
            for table in ("users", "addresses"):  # v3: every person and address belongs to a region
                cols = [r[1] for r in self.con.execute(f"PRAGMA table_info({table})")]
                if "region" not in cols:
                    self.con.execute(f"ALTER TABLE {table} ADD COLUMN region TEXT DEFAULT 'kyiv'")
            if "joined" not in [r[1] for r in self.con.execute("PRAGMA table_info(users)")]:
                self.con.execute("ALTER TABLE users ADD COLUMN joined INTEGER")  # v4: when the user first wrote (for /stats)
            self.con.commit()

    def q(self, sql, args=()):
        with self.lock:
            cur = self.con.execute(sql, args)
            self.con.commit()
            return cur.fetchall()

    def one(self, sql, args=()):
        rows = self.q(sql, args)
        return rows[0] if rows else None

    def migrate_json(self, path):
        """One-time import of users.json from the first version of the bot."""
        if not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as f:
                old = json.load(f)
        except Exception:
            return
        for cid, u in old.items():
            self.q("INSERT OR IGNORE INTO users(chat_id, lang, notify) VALUES(?,?,?)",
                   (int(cid), u.get("lang", "uk"), 1 if u.get("notify", True) else 0))
            if u.get("queue") and not self.one("SELECT 1 FROM addresses WHERE chat_id=?", (int(cid),)):
                label = u.get("address") or T[u.get("lang", "uk")]["queue_label"].format(q=u["queue"])
                self.q("INSERT INTO addresses(chat_id,label,queue,created) VALUES(?,?,?,?)",
                       (int(cid), label, u["queue"], int(time.time())))
                aid = self.one("SELECT last_insert_rowid() AS id")["id"]
                self.q("UPDATE users SET active=? WHERE chat_id=?", (aid, int(cid)))
        os.replace(path, path + ".imported")
        log("imported", len(old), "users from users.json")


# ===================================================================== schedule
SLOT = {"yes": "++", "no": "--", "first": "-+", "second": "+-", "maybe": "??", "mfirst": "?+", "msecond": "+?"}
_scheds = {r: {"at": 0, "ok_at": 0, "data": None, "err": None, "err_at": 0} for r in REGIONS}
HEALTH_EVERY = 30 * 60      # the watchdog reads every source this often
HEALTH_FAIL = 3 * 3600      # a source failing this long -> tell the admin
HEALTH_EMPTY = 36 * 3600    # no schedule this long while most regions have one -> tell the admin
_sched = _scheds["kyiv"]  # kept for the self-test
_sched_lock = threading.Lock()


def schedule(region="kyiv", force=False):
    sc = _scheds[region]
    src = REGIONS[region].get("source")
    ttl = OE_TTL if src else SCHEDULE_TTL
    with _sched_lock:
        if force or time.time() - sc["at"] > ttl or not sc["data"]:
            sc["at"] = time.time()
            ok = False
            try:
                sc["data"] = OE_READERS[src]() if src else http_json(DATA_BASE + f"{region}.json?t={int(time.time())}", timeout=30)
                sc["ok_at"] = time.time()
                sc["err"] = None
                ok = True
                if src in NOTICE_READERS:
                    try:
                        n = NOTICE_READERS[src]()
                        sc["data"]["notices"] = [n] if n else []
                    except Exception as e:
                        log("notice read failed:", region, e)
            except Exception as e:
                log("schedule fetch failed:", region, e)
                sc["err"], sc["err_at"] = (repr(e)[:200], time.time())
                sc["at"] = time.time() - SCHEDULE_TTL + 60  # retry in a minute
            # backup: YASNO's own planned outages when outage-data-ua is down or has nothing for today
            if not src and REGIONS[region].get("yasno") and not (ok and sc["data"] and days_from_today(sc["data"], time.time())):
                try:
                    backup = read_yasno(*REGIONS[region]["yasno"])
                    if days_from_today(backup, time.time()):
                        sc["data"], sc["ok_at"], sc["at"] = backup, time.time(), time.time()
                        log("schedule from YASNO (backup):", region)
                except Exception as e:
                    log("YASNO backup failed:", region, e)
        return sc["data"]


# ===================================================================== official oblenergo APIs
# Each reader returns the same shape as outage-data-ua ({"fact": {"data": {day_ts: {"GPV1.1": {hour: state}}}}}),
# so the rest of the bot works unchanged. Interval parsing follows fastiuk/ukraine-energy-monitor (MIT).
import calendar
import re as _re

UA_MONTHS = {"січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6, "липня": 7,
             "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12}
HOUR_STATE = {"++": "yes", "--": "no", "-+": "first", "+-": "second", "??": "maybe", "?+": "mfirst", "+?": "msecond",
              "-?": "no", "?-": "no"}


def kyiv_midnight(y, m, d):
    """Unix time of 00:00 Kyiv time (EET/EEST; DST from the last Sunday of March to the last Sunday of October)."""
    def last_sunday(month):
        last = calendar.monthrange(y, month)[1]
        return last - (calendar.weekday(y, month, last) + 1) % 7
    summer = (m, d) >= (3, last_sunday(3)) and (m, d) < (10, last_sunday(10))
    return calendar.timegm((y, m, d, 0, 0, 0)) - (3 if summer else 2) * 3600


NET_CAP = [None]  # the self-test caps waits on slow sites so an update check stays well inside its time limit


def net_timeout(t):
    return min(t, NET_CAP[0]) if NET_CAP[0] else t


def oe_get(url, timeout=30):
    timeout = net_timeout(timeout)
    req = urllib.request.Request(url, headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)",
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def oe_payload(days):
    """{date_tuple: {group: [(from_min, to_min, possible)]}} -> outage-data-ua shape."""
    fact = {}
    for (y, m, d), groups in days.items():
        hours_by_group = {}
        for g, ivs in groups.items():
            sl = ["+"] * 48
            for a, b, possible in ivs:
                for i in range(max(0, a // 30), min(48, (b + 29) // 30)):
                    if sl[i] != "-":
                        sl[i] = "?" if possible else "-"
            hours_by_group["GPV" + g] = {str(h): HOUR_STATE["".join(sl[2 * h - 2: 2 * h])] for h in range(1, 25)}
        fact[str(kyiv_midnight(y, m, d))] = hours_by_group
    return {"lastUpdated": datetime.utcnow().isoformat() + "Z", "fact": {"data": fact, "update": time.strftime("%d.%m.%Y %H:%M")}}


def _hm(h, m):
    return min(1440, int(h) * 60 + int(m))


def read_intervals(text):
    """'з 10:00 до 12:00, з 21:00 до 02:00' or '10:00 - 12:00' -> [(600, 720, False), (1260, 1440, False)]."""
    out = []
    for h1, m1, h2, m2 in _re.findall(r"(\d{1,2})\s*:\s*(\d{2})\s*(?:-|–|—|до)\s*(\d{1,2})\s*:\s*(\d{2})", text):
        a, b = _hm(h1, m1), _hm(h2, m2)
        if b <= a:  # crosses midnight: keep this day's part
            b = 1440
        out.append((a, b, False))
    return out


def read_loe():
    """Lvivoblenergo: text schedule in the photo-grafic menu ('Група 4.2. Електроенергії немає з 10:00 до 12:00')."""
    menus = oe_get("https://api.loe.lviv.ua/api/menus?page=1&type=photo-grafic", timeout=40).get("hydra:member") or []
    menu = next((m for m in menus if isinstance(m, dict) and m.get("type") == "photo-grafic"), {})
    days = {}
    for item in menu.get("menuItems") or []:
        raw = item.get("rawHtml") or "" if isinstance(item, dict) else ""
        if not raw.strip() or len(raw) > 100_000:
            continue
        text = html.unescape(_re.sub(r"<[^>]+>", "\n", raw))
        dm = _re.search(r"графік\b[^\n]{0,200}?\bна\s+(\d{1,2})\.(\d{1,2})\.(\d{4})", text, _re.I)
        if not dm:
            continue
        day = (int(dm[3]), int(dm[2]), int(dm[1]))
        groups = {}
        for line in text.split("\n"):
            gm = _re.search(r"Груп[аи]\s+(\d)\s*\.\s*(\d)\.?", line, _re.I)
            if gm:
                groups[f"{gm[1]}.{gm[2]}"] = read_intervals(line[gm.end():])
        if groups:
            days[day] = groups
    return oe_payload(days)


_mk_meta = {}


def read_mk():
    """Mykolaivoblenergo: 30-minute cells of the active schedules."""
    base = "https://off.energy.mk.ua/api"
    if not _mk_meta or time.time() - _mk_meta.get("at", 0) > 86400:
        queues = {q["id"]: q["name"] for q in oe_get(f"{base}/outage-queue/by-type/3")
                  if isinstance(q, dict) and _re.fullmatch(r"\d\.\d", str(q.get("name")))}
        series = {}
        for x in oe_get(f"{base}/schedule/time-series"):
            a = _re.fullmatch(r"(\d{2}):(\d{2})(?::\d{2})?", str(x.get("start")))
            b = _re.fullmatch(r"(\d{2}):(\d{2})(?::\d{2})?", str(x.get("end")))
            if a and b:
                end = _hm(b[1], b[2]) or 1440
                series[x.get("id")] = (_hm(a[1], a[2]), end)
        _mk_meta.update(queues=queues, series=series, at=time.time())
    days = {}
    for item in oe_get(f"{base}/v2/schedule/active"):
        start = datetime.fromisoformat(str(item.get("from")).replace("Z", "+00:00")).timestamp()
        local = time.gmtime(start + 3 * 3600 + 1)  # Kyiv date of a schedule that starts at local midnight
        day = (local.tm_year, local.tm_mon, local.tm_mday)
        groups = {g: [] for g in _mk_meta["queues"].values()}
        for cell in item.get("series") or []:
            typ = cell.get("type")
            g, sl = _mk_meta["queues"].get(cell.get("outage_queue_id")), _mk_meta["series"].get(cell.get("time_series_id"))
            if g and sl and typ in ("OFF", "SURE_OFF", "PROBABLY_OFF"):
                groups[g].append((sl[0], sl[1], typ == "PROBABLY_OFF"))
        days[day] = groups
    return oe_payload(days)


def read_if():
    """Prykarpattiaoblenergo: GET schedule-by-queue?queue=X.Y (one response may carry several queues)."""
    days = {}
    for g in STANDARD_GROUPS:
        if days and all(g in groups for groups in days.values()):
            continue
        for item in oe_get("https://be-svitlo.oe.if.ua/schedule-by-queue?queue=" + g) or []:
            dm = _re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", str(item.get("eventDate")))
            if not dm or not isinstance(item.get("queues"), dict):
                continue
            day = days.setdefault((int(dm[3]), int(dm[2]), int(dm[1])), {})
            for q, ivs in item["queues"].items():
                if q in STANDARD_GROUPS and q not in day and isinstance(ivs, list):
                    day[q] = read_intervals("; ".join(f"{i.get('from')}-{i.get('to')}" for i in ivs if isinstance(i, dict)))
        if not days:
            break  # nothing published for any day
    return oe_payload(days)


def read_ck():
    """Cherkasyoblenergo: news posts 'Оновлено графік погодинних відключень (ГПВ) на 17 жовтня'."""
    base = "https://www.cherkasyoblenergo.com/api/v1/posts"
    posts = oe_get(f"{base}/main?lang=uk&limit=60").get("data") or []
    now = time.gmtime(time.time() + 2 * 3600)
    picked = {}
    for p in posts:  # newest first: the first post for a day is the latest version
        title = str(p.get("title") or "")
        m = _re.search(r"граф[іi]к\w*\s+погодинних\s+відключень.*?\bна\s+(\d{1,2})\s+([а-яіїє]+)", title, _re.I)
        if not m or m[2].lower() not in UA_MONTHS:
            continue
        month = UA_MONTHS[m[2].lower()]
        year = now.tm_year + (1 if month < now.tm_mon - 6 else 0)
        day = (year, month, int(m[1]))
        if day not in picked:
            picked[day] = p.get("slug")
    days = {}
    for day, slug in sorted(picked.items())[-3:]:
        content = str((oe_get(f"{base}/{slug}?lang=uk").get("data") or {}).get("content") or "")
        text = _re.sub(r"(?i)</p>|<br\s*/?>|</div>|</li>", "\n", content)  # paragraphs end lines …
        text = html.unescape(_re.sub(r"<[^>]+>", "", text))  # … other tags may split '10' and ':00'
        groups = {}
        for gm in _re.finditer(r"(?<![\d:])([1-6])\.([12])(?![\d:])\s*((?:\d{1,2}\s*:\s*\d{2}\s*[-–]\s*\d{1,2}\s*:\s*\d{2}[\s,;]*)*)", text):
            groups[f"{gm[1]}.{gm[2]}"] = read_intervals(gm[3])
        if groups:
            days[day] = groups
    return oe_payload(days)


def kyiv_today(offset_days=0):
    """(y, m, d) of today (or a later day) in Kyiv."""
    now = time.time() + offset_days * 86400
    g = time.gmtime(now + 2 * 3600)
    if now >= kyiv_midnight(g.tm_year, g.tm_mon, g.tm_mday) + 86400:  # summer time already past midnight
        g = time.gmtime(now + 3 * 3600)
    return g.tm_year, g.tm_mon, g.tm_mday


def read_pl():
    """Poltavaoblenergo: POST newgpv-info.php with the date; the first table.gpv-table is that day,
    a row per subqueue with 48 half-hour cells (gpv-s1 on, gpv-s2 off, gpv-s3 maybe: switching time)."""
    days = {}
    for k in (0, 1):
        y, m, d = kyiv_today(k)
        body = urllib.parse.urlencode({"seldate": json.dumps({"date_in": f"{d:02d}-{m:02d}-{y}"})}).encode()
        req = urllib.request.Request("https://www.poe.pl.ua/customs/newgpv-info.php", data=body,
                                     headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"})
        with urllib.request.urlopen(req, timeout=40) as r:
            page = r.read(2_000_000).decode("utf-8", "replace")
        tm = _re.search(r'<table[^>]*class="[^"]*gpv-table[^"]*"[^>]*>(.*?)</table>', page, _re.S)
        if not tm:
            continue
        groups = {}
        for g, rest in _re.findall(r'<td[^>]*gpv-q[^>]*>\s*([1-6]\.[12])\s*</td>(.*?)</tr>', tm.group(1), _re.S):
            cells = _re.findall(r'<td[^>]*class="([^"]*gpv-c[^"]*)"', rest)
            if len(cells) != 48:
                continue
            ivs = []
            for i, c in enumerate(cells):
                if "gpv-s2" in c or "gpv-s3" in c:
                    ivs.append((i * 30, i * 30 + 30, "gpv-s3" in c))
            groups[g] = ivs
        if groups:
            days[(y, m, d)] = groups
    return oe_payload(days)


def yasno_payload(obj):
    """YASNO planned-outages ({group: {today|tomorrow: {date, status, slots[{start, end, type}]}}}) -> our shape.
    Only days with status ScheduleApplies count; 'Definite' slots are outages, 'Possible…' ones maybe."""
    days = {}
    for g, v in (obj or {}).items():
        if not isinstance(v, dict) or not _re.fullmatch(r"\d{1,2}\.\d", str(g)):
            continue
        for key in ("today", "tomorrow"):
            day = v.get(key)
            dm = _re.match(r"(\d{4})-(\d{2})-(\d{2})", str((day or {}).get("date")))
            if not dm or day.get("status") != "ScheduleApplies":
                continue
            ivs = [(int(x["start"]), int(x["end"]), "possib" in str(x.get("type")).lower())
                   for x in day.get("slots") or [] if isinstance(x, dict) and str(x.get("type")) != "NotPlanned"
                   and isinstance(x.get("start"), (int, float)) and isinstance(x.get("end"), (int, float))]
            days.setdefault((int(dm[1]), int(dm[2]), int(dm[3])), {})[g] = ivs
    return oe_payload(days)


def read_yasno(region_id, dso_id):
    return yasno_payload(http_json(f"https://app.yasno.ua/api/blackout-service/public/shutdowns/regions/{region_id}"
                                   f"/dsos/{dso_id}/planned-outages", timeout=30))


def oe_page(url, data=None):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"})
    with urllib.request.urlopen(req, timeout=net_timeout(40)) as r:
        return r.read(3_000_000).decode("utf-8", "replace")


def html_text(fragment):
    """Text of an HTML fragment with paragraphs and <br> as line breaks."""
    text = _re.sub(r"(?i)</p>|<br\s*/?>|</div>|</li>|</tr>", "\n", fragment)
    return html.unescape(_re.sub(r"<[^>]+>", "", text))


def groups_in_text(text):
    """'1.1: 10:30 – 13:30, 17:00 – 18:30' or '1.1 07:00-10:30; 17:30-21:00' lines -> {group: intervals}."""
    out = {}
    for gm in _re.finditer(r"(?m)^\s*([1-6])\.([12])(?![\d.])\s*:?\s*((?:\d{1,2}\s*:\s*\d{2}\s*[-–—]\s*\d{1,2}\s*:\s*\d{2}[\s,;]*)*)", text):
        out.setdefault(f"{gm[1]}.{gm[2]}", read_intervals(gm[3]))
    return out


def ua_day(day, month_word):
    """('12', 'листопада') -> (year, 11, 12) near today; None for an unknown month."""
    month = UA_MONTHS.get(month_word.lower())
    if not month:
        return None
    now = time.gmtime(time.time() + 2 * 3600)
    return now.tm_year + (1 if month < now.tm_mon - 6 else 0) - (1 if month > now.tm_mon + 6 else 0), month, int(day)


def read_zp():
    """Zaporizhzhiaoblenergo: <article>s on zoe.com.ua/outage/, newest first: 'ОНОВЛЕНО ГПВ НА 11 ЛИСТОПАДА …'
    with '1.1: 15:30 – 19:30' lines; queues not listed have no outages."""
    page = oe_page("https://www.zoe.com.ua/outage/")
    days = {}
    for art in _re.findall(r"<article.*?</article>", page, _re.S):
        h = _re.search(r"<h2[^>]*>(.*?)</h2>", art, _re.S)
        title = html_text(h.group(1)).strip() if h else ""
        low = title.lower()
        if "промислов" in low or not ("гпв" in low or "погодинн" in low):
            continue
        dm = _re.search(r"(\d{1,2})\s+([а-яіїє]+)", low)
        day = ua_day(dm[1], dm[2]) if dm else None
        if not day or day in days:
            continue
        if "не заплановані" in low or "не застосовуват" in low:
            days[day] = {g: [] for g in STANDARD_GROUPS}
            continue
        groups = groups_in_text(html_text(art))
        if groups:
            days[day] = {g: groups.get(g, []) for g in STANDARD_GROUPS}
    return oe_payload(dict(sorted(days.items())[-3:]))


def read_rv():
    """Rivneoblenergo: table in div#fetched-data-container — header row 1.1 … 6.2, then a row per day
    ('11.11.2026', then a cell per subqueue with HH:MM-HH:MM lines or 'Очікується')."""
    page = oe_page("https://www.roe.vsei.ua/disconnections")
    i = page.find("fetched-data-container")
    table = page[i:page.find("</table>", i)] if i >= 0 else ""
    cols, days = [], {}
    for row in _re.findall(r"<tr[^>]*>(.*?)</tr>", table, _re.S):
        cells = [html_text(c).strip() for c in _re.findall(r"<td[^>]*>(.*?)</td>", row, _re.S)]
        if cells and all(_re.fullmatch(r"[1-6]\.[12]", c) for c in cells[1:]) and len(cells) > 1:
            cols = cells[1:]
            continue
        dm = _re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", cells[0]) if cells else None
        if not dm or not cols:
            continue
        groups = {g: read_intervals(c) for g, c in zip(cols, cells[1:]) if "очікується" not in c.lower()}
        if groups:
            days[(int(dm[3]), int(dm[2]), int(dm[1]))] = groups
    return oe_payload(days)


def read_kh():
    """Kharkivoblenergo: RSS items 'У Харківській області 12 листопада діятимуть графіки погодинних відключень'
    with '1.1 02:30-06:00; 09:30-16:30' lines in the body. Newest first."""
    feed = oe_page("https://oblenergo.kharkiv.ua/uk/rss.xml")
    days = {}
    for item in _re.findall(r"<item>(.*?)</item>", feed, _re.S):
        tm = _re.search(r"<title>(.*?)</title>", item, _re.S)
        title = html.unescape(tm.group(1)) if tm else ""
        m = _re.search(r"(\d{1,2})\s+([а-яіїє]+)\s+діятимуть\s+(?:графіки\s+)?погодинн", title, _re.I)
        day = ua_day(m[1], m[2]) if m else None
        if not day or day in days:
            continue
        dm = _re.search(r"<description>(.*?)</description>", item, _re.S)
        body = html.unescape(dm.group(1)) if dm else ""
        b = body.find("field--name-body")
        groups = groups_in_text(html_text(body[b:] if b >= 0 else body))
        if groups:
            days[day] = {g: groups.get(g, []) for g in STANDARD_GROUPS}
    return oe_payload(dict(sorted(days.items())[-3:]))


def read_cv():
    """Chernivtsioblenergo: oblenergo.cv.ua/shutdowns/ (and ?next for tomorrow): div#inf1 … inf12 with 48
    half-hour cells — 'в' off, 'з' on, 'мз' maybe on. The bold date in div#gsv_t is the page's day."""
    days = {}
    for url in ("https://oblenergo.cv.ua/shutdowns/", "https://oblenergo.cv.ua/shutdowns/?next"):
        page = oe_page(url)
        dm = _re.search(r'id="gsv_t".*?<b>\s*(\d{2})\.(\d{2})\.(\d{4})\s*</b>', page, _re.S)
        if not dm:
            continue
        groups = {}
        for g, cells_html in _re.findall(r'<div id="inf(\d{1,2})"[^>]*>(.*?)</div>', page, _re.S):
            cells = [c.strip().lower() for c in _re.findall(r"<\w+>([^<]*)</\w+>", cells_html)]
            if len(cells) != 48:
                continue
            groups[g] = [(i * 30, i * 30 + 30, c == "мз") for i, c in enumerate(cells) if c in ("в", "мз")]
        if groups:
            days[(int(dm[3]), int(dm[2]), int(dm[1]))] = groups
    return oe_payload(days)


def read_kr():
    """Kirovohradoblenergo: its site is behind a browser check, so the schedule comes from the public preview of
    the official Oleksandriia district channel (t.me/s/…), which posts the oblast's whole schedule as text:
    '12.11.2026 за розпорядженням НЕК "Укренерго" буде діяти …' then 'Черга 1.1: 02-04, 10-12' ('-' = none).
    Newest post per day wins; queues a post leaves out have no outages; forecasts are skipped."""
    page = oe_page("https://t.me/s/SvitloOleksandriyskohoRaionu")
    posts = []
    for body in _re.findall(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', page, _re.S):
        posts.append(html.unescape(_re.sub(r"<[^>]+>", "", _re.sub(r"(?i)<br\s*/?>", "\n", body))))
    days = {}
    for text in reversed(posts):  # the preview lists oldest first
        if _re.search(r"прогноз", text, _re.I):
            continue
        dm = (_re.search(r"(\d{2})\.(\d{2})\.(\d{4})[^\n]{0,15}?за\s+розпорядженням", text)
              or _re.search(r"Укренерго\W{0,3}\s*(\d{2})\.(\d{2})\.(\d{4})", text))
        if not dm:
            continue
        day = (int(dm[3]), int(dm[2]), int(dm[1]))
        if day in days:
            continue
        if _re.search(r"застосування\s+графіка[^\n]{0,80}скасовано", text, _re.I):
            days[day] = {g: [] for g in STANDARD_GROUPS}
            continue
        groups = {}
        for g1, g2, rest in _re.findall(r"Черга\s*([1-6])\s*\.\s*([12])\s*:\s*([^\n]*)", text):
            ivs = []
            for h1, m1, h2, m2 in _re.findall(r"(?<![\d:])(\d{1,2})(?::(\d{2}))?\s*[-–—]\s*(\d{1,2})(?::(\d{2}))?(?![\d:])", rest):
                a, b = _hm(h1, m1 or 0), _hm(h2, m2 or 0)
                ivs.append((a, 1440 if b <= a else b, False))
            groups[f"{g1}.{g2}"] = ivs
        if groups:
            days[day] = {g: groups.get(g, []) for g in STANDARD_GROUPS}
    return oe_payload(dict(sorted(days.items())[-3:]))


def read_zt():
    """Zhytomyroblenergo: unhooking-search.php — per day a <td colspan=48> date header, then a row per subqueue
    ('1.1' then 48 half-hour cells, background #ffffff power / #ff3333 no power). With no tables and
    'наразі не діють' (and no emergency outages mentioned) nothing is scheduled today."""
    req = urllib.request.Request("https://www.ztoe.com.ua/unhooking-search.php",
                                 headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"})
    with urllib.request.urlopen(req, timeout=net_timeout(40)) as r:
        page = r.read(3_000_000).decode("cp1251", "replace")
    heads = [(m.start(), m.group(1)) for m in _re.finditer(r'colspan="48"[^>]*>\s*(?:<[^>]+>\s*)*(\d{2}\.\d{2}\.\d{4})', page)]
    days = {}
    for k, (pos, date) in enumerate(heads):
        part = page[pos:heads[k + 1][0] if k + 1 < len(heads) else len(page)]
        groups = {}
        for row in _re.findall(r"<tr>(.*?)</tr>", part, _re.S):
            g = _re.search(r">\s*([1-6]\.[12])\s*</b>", row)
            cells = _re.findall(r'<td[^>]*background:\s*#([0-9a-fA-F]{6})', row)
            if not g or len(cells) != 48:
                continue
            if any(c.lower() not in ("ffffff", "ff3333") for c in cells):
                continue  # an unknown colour: don't guess
            groups[g.group(1)] = [(i * 30, i * 30 + 30, False) for i, c in enumerate(cells) if c.lower() == "ff3333"]
        d, m, y = (int(x) for x in date.split("."))
        if len(groups) == 12:
            days[(y, m, d)] = groups
    if not heads and "наразі не діють" in page and "аварійн" not in page.lower():
        days[kyiv_today()] = {g: [] for g in STANDARD_GROUPS}
    return oe_payload(days)


def read_hoe():
    """Khmelnytskoblenergo: news posts 'Погодинні відключення на 11 листопада' / 'Прогноз обмежень на 12 листопада'
    (newest first) whose text says '12 листопада на Хмельниччині будуть застосовані графіки погодинних відключень'
    and lists 'Підчерга 1.1 – 06:00-09:00; 16:00-19:00.' Corrections go on top of the same post: the first line per
    subqueue wins."""
    news = oe_page("https://hoe.com.ua/post/novini-kompaniji")
    links = [u for u in dict.fromkeys(_re.findall(r"/post/(?:prognoz|pogodinni)[a-z0-9-]*-na-\d{1,2}-[a-z]+(?:-\d+)?\.html", news))][:4]
    days = {}
    for link in links:
        page = oe_page("https://hoe.com.ua" + link)
        i = page.find('<div class="post">')
        text = html_text(page[i:page.find("page__arrows", i)] if i >= 0 else "")
        dm = _re.search(r"(\d{1,2})\s+([а-яіїє]+)\s+на\s+Хмельниччині\s+(?:будуть\s+)?застосов", text, _re.I)
        day = ua_day(dm[1], dm[2]) if dm else None
        if not day or day in days:
            continue
        groups = {}
        for g1, g2, rest in _re.findall(r"[Пп]ідчерга\s*([1-6])\.([12])\s*[–—-]\s*([^\n]*)", text):
            groups.setdefault(f"{g1}.{g2}", read_intervals(rest))
        if groups:
            days[day] = {g: groups.get(g, []) for g in STANDARD_GROUPS}
    return oe_payload(dict(sorted(days.items())[-3:]))


# ---- Volynoblenergo: the schedule is only a picture (two 12x24 grids of half-hour cells), read by cell colour
def png_rgb(data):
    """8-bit RGB/RGBA non-interlaced PNG -> (width, height, [row bytes as RGB])."""
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError("not a PNG")
    pos, idat, ihdr = 8, [], None
    while pos < len(data):
        n, kind = struct.unpack(">I4s", data[pos:pos + 8])
        body = data[pos + 8:pos + 8 + n]
        if kind == b"IHDR":
            ihdr = struct.unpack(">IIBBBBB", body)
        elif kind == b"IDAT":
            idat.append(body)
        elif kind == b"IEND":
            break
        pos += 12 + n
    w, h, depth, ctype, _, _, interlace = ihdr
    if depth != 8 or ctype not in (2, 6) or interlace:
        raise ValueError(f"unsupported PNG {ihdr}")
    bpp = 3 if ctype == 2 else 4
    raw = zlib.decompress(b"".join(idat))
    stride = w * bpp
    rows, prev = [], bytes(stride)
    for y in range(h):
        f = raw[y * (stride + 1)]
        line = bytearray(raw[y * (stride + 1) + 1:(y + 1) * (stride + 1)])
        if f == 1:
            for i in range(bpp, stride):
                line[i] = (line[i] + line[i - bpp]) & 255
        elif f == 2:
            line = bytearray((a + b) & 255 for a, b in zip(line, prev))
        elif f == 3:
            for i in range(stride):
                line[i] = (line[i] + ((line[i - bpp] if i >= bpp else 0) + prev[i]) // 2) & 255
        elif f == 4:
            for i in range(stride):
                a = line[i - bpp] if i >= bpp else 0
                b = prev[i]
                c = prev[i - bpp] if i >= bpp else 0
                p = a + b - c
                pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
                line[i] = (line[i] + (a if pa <= pb and pa <= pc else b if pb <= pc else c)) & 255
        prev = bytes(line)
        rows.append(prev if bpp == 3 else bytes(b for i, b in enumerate(prev) if i % 4 != 3))
    return w, h, rows



VOE_ON, VOE_OFF = (185, 200, 215), (68, 110, 155)
_VOE_INK = bytes(1 if v < 236 else 0 for v in range(256))


def _voe_runs(flags):
    out, start = [], None
    for i, f in enumerate(list(flags) + [False]):
        if f and start is None:
            start = i
        elif not f and start is not None:
            out.append((start, i))
            start = None
    return out


def _voe_even(spans):
    sizes = [b - a for a, b in spans]
    return min(sizes) >= 30 and max(sizes) - min(sizes) <= 4 and all(spans[i + 1][0] - spans[i][1] <= 8 for i in range(len(spans) - 1))


def _voe_near(c, ref, tol=24):
    return all(abs(a - b) <= tol for a, b in zip(c, ref))


def voe_grid(png):
    """{group: [48 bools, True = no power]} from a Volynoblenergo schedule PNG, or None if the picture
    isn't the expected two 12x24 grids in the expected colours (then nothing is shown rather than a guess)."""
    w, h, rows = png_rgb(png)
    px = lambda x, y: tuple(rows[y][3 * x:3 * x + 3])  # noqa: E731
    reds = [r[0::3] for r in rows]  # the table is bluish: red < 236 is ink, the lines between cells are white
    counts = [bytes(col).translate(_VOE_INK).count(1) for col in zip(*reds)]
    cols = _voe_runs(c > max(counts) // 2 for c in counts)
    if len(cols) != 26:
        return None
    label, cols = cols[1], cols[2:]
    if not _voe_even(cols):
        return None
    xs = [label[0] + 3] + [a + 3 for a, _ in cols]
    bands = _voe_runs(all(reds[y][x] < 236 for x in xs) for y in range(h))
    if len(bands) != 26:
        return None
    halves = []
    for grid in (bands[:13], bands[13:]):
        header, body = grid[0], grid[1:]
        if not _voe_even(body) or body[0][0] - header[1] > 8:
            return None
        mid = (header[0] + header[1]) // 2
        if not all(_voe_near(px(label[0] + 3, (a + b) // 2), VOE_ON) for a, b in body):
            return None
        if not all(_voe_near(px(a + 3, mid), VOE_OFF) for a, _ in cols):
            return None
        halves.append(body)
    out = {}
    for i, g in enumerate(f"{q}.{s_}" for q in range(1, 7) for s_ in (1, 2)):
        cells = []
        for body in halves:
            y0, y1 = body[i][0] + 6, body[i][1] - 6
            for x0, x1 in cols:
                x0, x1 = x0 + 6, x1 - 6
                pts = [px(x0 + (x1 - x0) * fx // 4, y0 + (y1 - y0) * fy // 4) for fx in range(5) for fy in range(5)]
                if all(_voe_near(p, VOE_OFF) for p in pts):
                    cells.append(True)
                elif all(_voe_near(p, VOE_ON) for p in pts):
                    cells.append(False)
                else:
                    return None
        out[g] = cells
    return out


VOE_API = "https://api-voe-poweron.inneti.net"
_voe_cache = {}  # image path -> grid (or None if the picture didn't look as expected); paths change on every upload


def read_voe():
    """Volynoblenergo: the images on show (options pw_gpv_image_today/tomorrow), dated by the archive."""
    keys = ("pw_gpv_image_today", "pw_gpv_image_tomorrow")
    opts = oe_get(f"{VOE_API}/api/options?" + "&".join(f"option_key[]={k}" for k in keys))
    paths = [str(x.get("value")) for x in opts if isinstance(x, dict) and x.get("option_key") in keys
             and _re.fullmatch(r"/media/[\w/.-]{1,120}\.png", str(x.get("value") or ""))]
    days = {}
    if not paths:
        return oe_payload(days)
    y, m, d = kyiv_today(-1)
    arch = oe_get(f"{VOE_API}/api/archive_gpv_graphs?dateGraph[after]={y}-{m:02d}-{d:02d}")
    day_of = {}
    for x in arch if isinstance(arch, list) else []:
        img = x.get("imageUrl") if isinstance(x, dict) else None
        dm = _re.match(r"(\d{4})-(\d{2})-(\d{2})", str(x.get("dateGraph"))) if isinstance(img, dict) else None
        if dm and img.get("contentUrl"):
            day_of[img["contentUrl"]] = (int(dm[1]), int(dm[2]), int(dm[3]))  # Kyiv wall-clock day
    for path in paths:
        day = day_of.get(path)
        if not day:
            continue  # an older day's picture still on show, or not archived yet
        if path not in _voe_cache:
            req = urllib.request.Request(VOE_API + path, headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"})
            with urllib.request.urlopen(req, timeout=60) as r:
                png = r.read(8_000_000)
            try:
                _voe_cache[path] = voe_grid(png)
            except Exception as e:
                log("Volyn picture unreadable:", path, repr(e))
                _voe_cache[path] = None
            if _voe_cache[path] is None:
                log("Volyn picture doesn't look as expected:", path)
            while len(_voe_cache) > 8:
                _voe_cache.pop(next(iter(_voe_cache)))
        grid = _voe_cache[path]
        if grid:
            days[day] = {g: [(i * 30, i * 30 + 30, False) for i, off in enumerate(cells) if off] for g, cells in grid.items()}
    return oe_payload(days)


OE_READERS = {"loe": read_loe, "mk": read_mk, "if": read_if, "ck": read_ck, "pl": read_pl,
              "zp": read_zp, "rv": read_rv, "kh": read_kh, "cv": read_cv, "kr": read_kr, "voe": read_voe, "zt": read_zt, "hoe": read_hoe}


NOTICE_TTL = 6 * 3600  # a notice holds until a newer schedule post, or this long at most
NOTICE_EMERG = _re.compile(r"аварійн\w*\s+відключ|\bГАВ\b", _re.I)
NOTICE_END = _re.compile(r"(аварійн\w*\s+відключ\w*|\bГАВ\b)[^.\n]{0,40}скасов|скасов\w*[^.\n]{0,40}(аварійн|\bГАВ\b)", _re.I)
NOTICE_CANCEL = _re.compile(r"скасов\w*[^.\n]{0,60}(графік|відключ)|(графік|відключ)\w*[^.\n]{0,60}скасов|не\s+застосовуватим", _re.I)
NOTICE_SCHED = _re.compile(r"графік\w*\s+погодинн|\bГПВ\b|черга\s*\d\.\d", _re.I)


def kyiv_ts(y, mo, d, h=0, mi=0):
    return kyiv_midnight(int(y), int(mo), int(d)) + int(h) * 3600 + int(mi) * 60


def pick_notice(items, url, now=None):
    """items: [(ts, text)] newest first. The newest post that is a notice or a schedule decides:
    emergency outages / cancelled schedules -> a notice; a schedule post or 'ГАВ скасовано' -> none."""
    now = now or time.time()
    for ts, text in items:
        if not ts or ts > now + 600:
            continue
        if now - ts > NOTICE_TTL:
            break
        if NOTICE_END.search(text):
            return None
        if NOTICE_EMERG.search(text):
            return {"kind": "emergency", "at": int(ts), "text": text.strip()[:300], "url": url}
        if NOTICE_CANCEL.search(text):
            return {"kind": "cancel", "at": int(ts), "text": text.strip()[:300], "url": url}
        if NOTICE_SCHED.search(text):
            return None
    return None


def notice_ck():
    posts = oe_get("https://www.cherkasyoblenergo.com/api/v1/posts/main?lang=uk&limit=20").get("data") or []
    items = []
    for p in posts:
        m = _re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})", str(p.get("publishedAt")))
        if m:
            items.append((kyiv_ts(*m.groups()), str(p.get("title") or "")))
    return pick_notice(sorted(items, reverse=True), "https://www.cherkasyoblenergo.com")


def notice_kh():
    feed = oe_page("https://oblenergo.kharkiv.ua/uk/rss.xml")
    items = []
    for item in _re.findall(r"<item>(.*?)</item>", feed, _re.S):
        tm = _re.search(r"<title>(.*?)</title>", item, _re.S)
        dm = _re.search(r"<description>(.*?)</description>", item, _re.S)
        body = html_text(html.unescape(dm.group(1))) if dm else ""
        when = _re.search(r"(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2}):(\d{2})", body)  # the post's own date and time
        if when:
            items.append((kyiv_ts(when[3], when[2], when[1], when[4], when[5]), html.unescape(tm.group(1) if tm else "")))
    return pick_notice(sorted(items, reverse=True), "https://oblenergo.kharkiv.ua/uk")


def notice_kr():
    page = oe_page("https://t.me/s/SvitloOleksandriyskohoRaionu")
    items = []
    for m in _re.finditer(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>.*?<time[^>]*datetime="([^"]+)"', page, _re.S):
        try:
            ts = datetime.fromisoformat(m.group(2)).timestamp()
        except ValueError:
            continue
        items.append((ts, html_text(m.group(1))))
    return pick_notice(sorted(items, reverse=True), "https://t.me/SvitloOleksandriyskohoRaionu")


def notice_zp():
    """Articles carry a date but no time: the newest article about today counts as fresh."""
    page = oe_page("https://www.zoe.com.ua/outage/")
    y, m, d = kyiv_today()
    for art in _re.findall(r"<article.*?</article>", page, _re.S)[:1]:
        h = _re.search(r"<h2[^>]*>(.*?)</h2>", art, _re.S)
        title = html_text(h.group(1)).strip() if h else ""
        dm = _re.search(r"(\d{1,2})\s+([а-яіїє]+)", title.lower())
        if dm and ua_day(dm[1], dm[2]) == (y, m, d) and "промислов" not in title.lower():
            return pick_notice([(time.time(), title)], "https://www.zoe.com.ua/outage/")
    return None


def notice_zt():
    req = urllib.request.Request("https://www.ztoe.com.ua/unhooking-search.php",
                                 headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"})
    with urllib.request.urlopen(req, timeout=net_timeout(40)) as r:
        page = r.read(3_000_000).decode("cp1251", "replace")
    i = page.find("УВАГА")
    top = html_text(page[i:i + 3000]) if i >= 0 else ""
    if NOTICE_EMERG.search(top) and not NOTICE_END.search(top):
        return {"kind": "emergency", "at": int(time.time()), "text": " ".join(top.split())[:300], "url": "https://www.ztoe.com.ua/unhooking-search.php"}
    return None


NOTICE_READERS = {"ck": notice_ck, "kh": notice_kh, "kr": notice_kr, "zp": notice_zp, "zt": notice_zt}


def active_notice(data, now=None):
    now = now or time.time()
    n = ((data or {}).get("notices") or [None])[0]
    return n if n and now - n["at"] <= NOTICE_TTL else None


def region_groups(region, data=None):
    """Queues of a region: today's schedule if published, else the standard 1.1 … 6.2."""
    data = data or schedule(region)
    days = days_from_today(data, time.time()) if data else []
    if days and days[0][1]:
        return sorted((k[3:] for k in days[0][1]), key=lambda q: [int(x) for x in q.split(".")])
    return [] if region == "kyiv" else list(REGIONS[region].get("groups") or STANDARD_GROUPS)


def days_from_today(data, now):
    """[(day_ts, groups)] from today on. Day keys are Kyiv midnights, so Kyiv wall-clock
    time = seconds since today's key (no tz database needed)."""
    keys = sorted(int(k) for k in data["fact"]["data"])
    return [(k, data["fact"]["data"][str(k)]) for k in keys if k + 86400 > now]


def slots(hours):
    return "".join(SLOT.get((hours or {}).get(str(h)), "++") for h in range(1, 25))  # + on, - off, ? maybe


def queue_key(q):
    return "GPV" + q


def norm_queue(q):
    q = (q or "").strip().replace(",", ".")
    return q if "." in q else (q + ".1" if q else q)


def fmt(m):
    m = int(round(m)) % 1440
    return f"{m // 60:02d}:{m % 60:02d}"


def fmt_left(m):
    m = max(0, int(round(m)))
    return f"{m // 60}:{m % 60:02d}"


BAR_MAX = 16  # blocks that still fit a phone screen next to the times


def outage_line(kind, a, b, t):
    """'06:30–11:30  5:00 ▰▰▰▰▰▰▰▰▰▰' — one block per 30 min; ▱ for a possible outage."""
    mins = b - a
    blocks = mins // 30
    bar = ("▰" if kind == "-" else "▱") * min(blocks, BAR_MAX) + ("…" if blocks > BAR_MAX else "")
    line = f"{fmt(a)}–{'24:00' if b == 1440 else fmt(b)}  {mins // 60}:{mins % 60:02d} {bar}"
    return f"<code>{line}</code>" + (f" <i>{t['maybe']}</i>" if kind == "?" else "")


def intervals(s):
    res, i = [], 0
    while i < 48:
        if s[i] == "+":
            i += 1
            continue
        j = i
        while j < 48 and s[j] == s[i]:
            j += 1
        res.append((s[i], i * 30, j * 30))
        i = j
    return res


def kyiv_minutes(data, now):
    days = days_from_today(data, now) if data else []
    if days and days[0][0] <= now:
        return days[0][0], (now - days[0][0]) / 60
    return None, None


def region_name(region, lang):
    return REGIONS.get(region, REGIONS["kyiv"])[lang]


def site_link(region, queue):
    return f"{SITE_URL}#{'' if region == 'kyiv' else region + '-'}q{queue.replace('.', '-')}"


def status_text(queue, lang, label, now=None, region="kyiv"):
    t = T[lang]
    now = now or time.time()
    data = schedule(region)
    if not data:
        return t["no_data"]
    days = days_from_today(data, now)
    key = queue_key(queue)
    head = f"<b>{t['title'].format(q=queue)}</b>"
    if label and label not in (T["uk"]["queue_label"].format(q=queue), T["en"]["queue_label"].format(q=queue)):
        head += f" · {html.escape(label)}"
    if region != "kyiv":
        head += f"\n<i>{region_name(region, lang)}</i>"
    lines = [head]
    note = active_notice(data, now)
    if note:
        g = time.gmtime(note["at"])
        off = calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0)) - kyiv_midnight(g.tm_year, g.tm_mon, g.tm_mday)
        lines += [t["notice_" + note["kind"]].format(co=html.escape((REGIONS[region].get("co") or {}).get(lang, "")),
                                                    t=time.strftime("%H:%M", time.gmtime(note["at"] + off)), url=note["url"]), ""]
    today = days[0] if days and days[0][0] <= now else None
    if today:
        seq = "".join(slots(g.get(key)) for _, g in days)
        now_min = (now - today[0]) / 60
        idx = min(47, int(now_min // 30))
        cur = seq[idx]
        state = {"+": t["on_now"], "-": t["off_now"], "?": t["maybe_now"]}[cur]
        j = idx + 1
        while j < len(seq) and seq[j] == cur:
            j += 1
        if j < len(seq):
            nxt = {"+": "next_on", "-": "next_off", "?": "next_maybe"}[seq[j]]
            state += " — " + t[nxt].format(d=t["tomorrow_word"] if j >= 48 else "", t=fmt(j * 30),
                                           left=fmt_left(j * 30 - now_min))
        else:
            state += " — " + t["no_change"]
        lines += [state, ""]
    bars_shown = maybe_shown = False
    for n, (name, day) in enumerate(((t["today"], today), (t["tomorrow"], next((d for d in days if d[0] > now), None)))):
        if n:
            lines.append("")
        if not day:
            lines.append(f"<b>{name}:</b> {t['not_published']}")
            continue
        s = slots(day[1].get(key))
        iv = intervals(s)
        off = s.count("-") * 30
        lines.append(f"<b>{name}:</b> " + (t["off_total"].format(d=t["dur"](off // 60, off % 60)) if iv else t["no_outages"]))
        for kind, a, b in iv:
            lines.append(outage_line(kind, a, b, t))
            bars_shown = True
            maybe_shown |= kind == "?"
    if bars_shown:
        lines.append(f"<i>{t['bar_legend']}{t['bar_legend_maybe'] if maybe_shown else ''}</i>")
    if today:
        lines += ["", f"<i>{t['updated_at'].format(t=fmt((now - today[0]) / 60))}</i>"]
    return "\n".join(lines)


# ===================================================================== telegram sending (rate-limited)

# ===================================================================== pictures (stdlib PNG + a 5x7 pixel font)
_GLYPHS = """
А .###. #...# #...# ##### #...# #...# #...#
Б ##### #.... #.... ####. #...# #...# ####.
В ####. #...# #...# ####. #...# #...# ####.
Г ##### #.... #.... #.... #.... #.... #....
Ґ ....# ##### #.... #.... #.... #.... #....
Д .###. .#.#. .#.#. .#.#. .#.#. ##### #...#
Е ##### #.... #.... ####. #.... #.... #####
Є .###. #...# #.... ###.. #.... #...# .###.
Ж #.#.# #.#.# .###. ..#.. .###. #.#.# #.#.#
З .###. #...# ....# ..##. ....# #...# .###.
И #...# #...# #..## #.#.# ##..# #...# #...#
І .###. ..#.. ..#.. ..#.. ..#.. ..#.. .###.
Ї .#.#. ..... .###. ..#.. ..#.. ..#.. .###.
Й .#.#. ..#.. #...# #..## #.#.# ##..# #...#
К #...# #..#. #.#.. ##... #.#.. #..#. #...#
Л ..### .#..# .#..# .#..# .#..# .#..# #...#
М #...# ##.## #.#.# #.#.# #...# #...# #...#
Н #...# #...# #...# ##### #...# #...# #...#
О .###. #...# #...# #...# #...# #...# .###.
П ##### #...# #...# #...# #...# #...# #...#
Р ####. #...# #...# ####. #.... #.... #....
С .###. #...# #.... #.... #.... #...# .###.
Т ##### ..#.. ..#.. ..#.. ..#.. ..#.. ..#..
У #...# #...# #...# .#### ....# #...# .###.
Ф ..#.. .###. #.#.# #.#.# .###. ..#.. ..#..
Х #...# #...# .#.#. ..#.. .#.#. #...# #...#
Ц #..#. #..#. #..#. #..#. #..#. ##### ....#
Ч #...# #...# #...# .#### ....# ....# ....#
Ш #.#.# #.#.# #.#.# #.#.# #.#.# #.#.# #####
Щ #.#.# #.#.# #.#.# #.#.# #.#.# ##### ....#
Ь #.... #.... #.... ####. #...# #...# ####.
Ю #..#. #.#.# #.#.# ###.# #.#.# #.#.# #..#.
Я .#### #...# #...# .#### ..#.# .#..# #...#
0 .###. #...# #..## #.#.# ##..# #...# .###.
1 ..#.. .##.. ..#.. ..#.. ..#.. ..#.. .###.
2 .###. #...# ....# ...#. ..#.. .#... #####
3 ####. ....# ....# .###. ....# ....# ####.
4 ...#. ..##. .#.#. #..#. ##### ...#. ...#.
5 ##### #.... ####. ....# ....# #...# .###.
6 ..##. .#... #.... ####. #...# #...# .###.
7 ##### ....# ...#. ..#.. .#... .#... .#...
8 .###. #...# #...# .###. #...# #...# .###.
9 .###. #...# #...# .#### ....# ...#. .##..
: ..... ..#.. ..#.. ..... ..#.. ..#.. .....
. ..... ..... ..... ..... ..... .##.. .##..
, ..... ..... ..... ..... .##.. ..#.. .#...
- ..... ..... ..... .###. ..... ..... .....
– ..... ..... ..... ####. ..... ..... .....
· ..... ..... ..... ..#.. ..... ..... .....
’ ..#.. ..#.. .#... ..... ..... ..... .....
( ...#. ..#.. .#... .#... .#... ..#.. ...#.
) .#... ..#.. ...#. ...#. ...#. ..#.. .#...
"""
FONT = {ln.split()[0]: ln.split()[1:] for ln in _GLYPHS.strip().splitlines()}
FONT[" "] = ["....."] * 7
C_ON, C_OFF, C_MAYBE, C_NONE = (151, 196, 89), (226, 75, 74), (239, 159, 39), (211, 209, 199)
C_INK, C_MUTED, C_BG = (44, 44, 42), (136, 135, 128), (255, 255, 255)


class Canvas:
    def __init__(self, w, h, bg=C_BG):
        self.w, self.h = w, h
        self.px = bytearray(bytes(bg) * (w * h))

    def rect(self, x, y, w, h, c):
        x0, y0, x1, y1 = max(0, int(x)), max(0, int(y)), min(self.w, int(x + w)), min(self.h, int(y + h))
        if x1 <= x0 or y1 <= y0:
            return
        row = bytes(c) * (x1 - x0)
        for yy in range(y0, y1):
            i = (yy * self.w + x0) * 3
            self.px[i:i + len(row)] = row

    @staticmethod
    def text_w(text, scale=3):
        return len(text) * 6 * scale

    def text(self, x, y, text, c=C_INK, scale=3):
        for ch in text.upper():
            for r, line in enumerate(FONT.get(ch, FONT[" "])):
                for col, bit in enumerate(line):
                    if bit == "#":
                        self.rect(x + col * scale, y + r * scale, scale, scale, c)
            x += 6 * scale
        return x

    def png(self):
        raw = b"".join(b"\x00" + bytes(self.px[y * self.w * 3:(y + 1) * self.w * 3]) for y in range(self.h))

        def chunk(kind, data):
            return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", self.w, self.h, 8, 2, 0, 0, 0))
                + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))


def _mix(a, b, f):
    return tuple(int(x + (y - x) * f) for x, y in zip(a, b))


def short_region(region):
    return region_name(region, "uk").replace(" область", "")


def queue_png(region, queue, now=None):
    """Today's and tomorrow's day strip for a queue: green power, red none, amber maybe; a line at 'now'."""
    now = now or time.time()
    data = schedule(region)
    days = [d for d in (days_from_today(data, now) if data else []) if queue_key(queue) in d[1]][:2]
    cell, x0, W = 18, 48, 960
    H = 90 + 170 * max(1, len(days)) + 60
    cv = Canvas(W, H)
    cv.text(x0, 24, f"ЧЕРГА {queue} · {short_region(region)}", C_INK, 4)
    y = 90
    for n, (day_ts, groups) in enumerate(days):
        sl = slots(groups.get(queue_key(queue)))
        off = sl.count("-") * 30
        g = time.gmtime(day_ts + 12 * 3600)
        label = ("СЬОГОДНІ" if day_ts <= now else "ЗАВТРА") + f" {g.tm_mday:02d}.{g.tm_mon:02d} · БЕЗ СВІТЛА {off // 60}"
        label += (f",5 ГОД" if off % 60 else " ГОД")
        cv.text(x0, y, label, C_INK, 3)
        by = y + 40
        for i, ch in enumerate(sl):
            cv.rect(x0 + i * cell, by, cell - 1, 64, {"+": C_ON, "-": C_OFF, "?": C_MAYBE}.get(ch, C_NONE))
        if day_ts <= now < day_ts + 86400:
            nx = x0 + int((now - day_ts) / 1800 * cell)
            cv.rect(nx - 2, by - 8, 4, 80, C_INK)
        for h in range(0, 25, 3):
            t = f"{h:02d}"
            cv.text(x0 + h * 2 * cell - cv.text_w(t, 2) // 2, by + 74, t, C_MUTED, 2)
        y += 170
    if not days:
        cv.text(x0, y, "ГРАФІК ЩЕ НЕ ОПУБЛІКОВАНО", C_MUTED, 3)
    ly = H - 44
    x = x0
    for c, t in ((C_ON, "Є СВІТЛО"), (C_OFF, "НЕМАЄ"), (C_MAYBE, "МОЖЛИВО")):
        cv.rect(x, ly, 22, 22, c)
        x = cv.text(x + 32, ly + 1, t, C_MUTED, 3) + 36
    return cv.png()


def regions_png(day_mid, title, cur):
    """All regions on one picture: per half-hour, the share of a region's queues without power (green -> red)."""
    names = [(r, short_region(r)) for r in REGIONS]
    name_w = max(Canvas.text_w(n, 3) for _, n in names) + 30
    cell, row_h, x0 = 13, 34, 30
    W = x0 + name_w + 48 * cell + 30
    H = 100 + row_h * len(names) + 110
    cv = Canvas(W, H)
    cv.text(x0, 30, title, C_INK, 4)
    y = 100
    for region, name in names:
        cv.text(x0, y + 6, name, C_INK if region in cur else C_MUTED, 3)
        bx = x0 + name_w
        groups = cur.get(region)
        for i in range(48):
            if groups:
                f = sum(1 for sl in groups.values() if sl[i] == "-") / len(groups)
                c = C_ON if f == 0 else _mix((245, 190, 180), C_OFF, f)
            else:
                c = C_NONE
            cv.rect(bx + i * cell, y + 2, cell - 1, row_h - 6, c)
        y += row_h
    bx = x0 + name_w
    for h in range(0, 25, 3):
        t = f"{h:02d}"
        cv.text(bx + h * 2 * cell - cv.text_w(t, 2) // 2, y + 8, t, C_MUTED, 2)
    ly = y + 50
    x = x0
    for c, t in ((C_ON, "Є СВІТЛО"), (_mix((245, 190, 180), C_OFF, 0.4), "ЧАСТИНА ЧЕРГ БЕЗ СВІТЛА"), (C_OFF, "УСІ ЧЕРГИ"), (C_NONE, "НЕ ОПУБЛІКОВАНО")):
        cv.rect(x, ly, 20, 20, c)
        x = cv.text(x + 28, ly + 1, t, C_MUTED, 2) + 30
    return cv.png()


class Telegram:
    def __init__(self, token):
        self.base = f"https://api.telegram.org/bot{token}/"
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.on_blocked = lambda chat_id: None

    def call(self, method, timeout=65, **params):
        return http_json(self.base + method, params, timeout=timeout)

    def _throttle(self):
        with self.lock:
            wait = self.next_at - time.time()
            self.next_at = max(time.time(), self.next_at) + 1.0 / SEND_RATE
        if wait > 0:
            time.sleep(wait)

    def _send(self, method, chat_id, **params):
        for attempt in range(3):
            self._throttle()
            try:
                return self.call(method, timeout=30, chat_id=chat_id, **params)
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", "replace")
                if e.code == 429:  # too many requests: wait as told
                    try:
                        retry = json.loads(body)["parameters"]["retry_after"]
                    except Exception:
                        retry = 5
                    log("429, waiting", retry)
                    time.sleep(retry + 0.5)
                    continue
                if e.code == 403:
                    self.on_blocked(chat_id)
                    return None
                if "message is not modified" in body:
                    return None
                log(method, "failed", chat_id, e.code, body[:200])
                return None
            except Exception as e:
                log(method, "failed", chat_id, repr(e))
                time.sleep(1)
        return None

    def send(self, chat_id, text, buttons=None):
        p = {"text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        if buttons:
            p["reply_markup"] = {"inline_keyboard": buttons}
        return self._send("sendMessage", chat_id, **p)

    def photo(self, chat_id, png, caption="", buttons=None):
        boundary = "svitlo" + str(int(time.time() * 1000))
        fields = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
        if buttons:
            fields["reply_markup"] = json.dumps({"inline_keyboard": buttons}, ensure_ascii=False)
        body = b"".join(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode() for k, v in fields.items())
        body += (f'--{boundary}\r\nContent-Disposition: form-data; name="photo"; filename="svitlo.png"\r\n'
                 f"Content-Type: image/png\r\n\r\n").encode() + png + f"\r\n--{boundary}--\r\n".encode()
        self._throttle()
        try:
            req = urllib.request.Request(self.base + "sendPhoto", data=body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 403:
                self.on_blocked(chat_id)
            log("sendPhoto failed", chat_id, e.code, e.read()[:200])
        except Exception as e:
            log("sendPhoto failed", chat_id, repr(e))
        return None

    def edit(self, chat_id, message_id, text, buttons=None):
        p = {"message_id": message_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        if buttons:
            p["reply_markup"] = {"inline_keyboard": buttons}
        return self._send("editMessageText", chat_id, **p)


# ===================================================================== the bot
class Bot:
    def __init__(self, db, tg):
        self.db, self.tg = db, tg
        tg.on_blocked = lambda cid: db.q("UPDATE users SET blocked=1 WHERE chat_id=?", (cid,))
        self.chat_locks = {}
        self.locks_lock = threading.Lock()
        self.stale_alerted = False
        self.admin_cache = {}
        import queue
        self.bulk = queue.PriorityQueue()  # (priority, seq, chat_id, text): mass notifications, soonest outage first
        self.bulk_seq = 0

    def send_bulk(self, chat_id, text, priority=10 ** 6):
        """Queue a mass notification (schedule changes, tomorrow's schedule, notices, weekly stats).
        Lower priority = sent first: the minutes until that person's next outage."""
        if isinstance(self.tg, FakeTG):
            return self.tg.send(chat_id, text)
        self.bulk_seq += 1
        self.bulk.put((priority, self.bulk_seq, chat_id, text))

    def bulk_sender(self):
        while True:
            _, _, chat_id, text = self.bulk.get()
            t0 = time.time()
            self.safe(self.tg.send, chat_id, text)
            time.sleep(max(0.0, 1.0 / BULK_RATE - (time.time() - t0)))

    # ---------- helpers
    def user(self, chat_id, lang_code=None):
        u = self.db.one("SELECT * FROM users WHERE chat_id=?", (chat_id,))
        if not u:
            self.db.q("INSERT INTO users(chat_id, lang, joined) VALUES(?,?,?)",
                      (chat_id, "en" if (lang_code or "").startswith("en") else "uk", int(time.time())))
            u = self.db.one("SELECT * FROM users WHERE chat_id=?", (chat_id,))
        elif u["blocked"]:
            self.db.q("UPDATE users SET blocked=0 WHERE chat_id=?", (chat_id,))
        return u

    def set_user(self, chat_id, **kw):
        cols = ", ".join(f"{k}=?" for k in kw)
        self.db.q(f"UPDATE users SET {cols} WHERE chat_id=?", (*kw.values(), chat_id))

    def state(self, u):
        try:
            return json.loads(u["state"] or "{}")
        except json.JSONDecodeError:
            return {}

    def set_state(self, chat_id, st):
        self.set_user(chat_id, state=json.dumps(st, ensure_ascii=False) if st else None)

    def addresses(self, chat_id):
        return self.db.q("SELECT * FROM addresses WHERE chat_id=? ORDER BY id", (chat_id,))

    def active_address(self, u):
        a = self.db.one("SELECT * FROM addresses WHERE id=? AND chat_id=?", (u["active"], u["chat_id"])) if u["active"] else None
        return a or self.db.one("SELECT * FROM addresses WHERE chat_id=? ORDER BY id LIMIT 1", (u["chat_id"],))

    def add_address(self, chat_id, label, queue, region="kyiv"):
        u = self.user(chat_id)
        t = T[u["lang"]]
        same = self.db.one("SELECT id FROM addresses WHERE chat_id=? AND label=? AND queue=? AND region=?",
                           (chat_id, label, queue, region))
        if same:
            aid = same["id"]
        else:
            if len(self.addresses(chat_id)) >= MAX_ADDRESSES:
                self.tg.send(chat_id, t["too_many"].format(n=MAX_ADDRESSES))
                return None
            self.db.q("INSERT INTO addresses(chat_id,label,queue,created,region) VALUES(?,?,?,?,?)",
                      (chat_id, label, queue, int(time.time()), region))
            aid = self.db.one("SELECT max(id) AS id FROM addresses WHERE chat_id=?", (chat_id,))["id"]
        self.set_user(chat_id, active=aid, state=None)
        return aid

    def yasno(self, endpoint, region="kyiv", **params):
        """YASNO lookup with a cache: fresh hits are instant; if YASNO is down, stale data is used."""
        rid, dso = REGIONS[region]["yasno"]
        params = {"regionId": rid, "dsoId": dso, **params}
        key = endpoint + ":" + json.dumps(params, sort_keys=True, ensure_ascii=False)
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < CACHE_TTL[endpoint]:
            return json.loads(row["value"])
        try:
            q = urllib.parse.urlencode(params)
            val = http_json(f"{YASNO}/{endpoint}?{q}", timeout=10)
            self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)",
                      (key, json.dumps(val, ensure_ascii=False), int(time.time())))
            return val
        except Exception as e:
            if row:
                log("yasno failed, using cache:", e)
                return json.loads(row["value"])
            raise

    # ---------- views
    @staticmethod
    def street_param(a):
        """A /start parameter that opens the bot on this address's street (Kyiv, Dnipro region), or None."""
        parts = [x.strip() for x in str(a["label"]).split(",")]
        if a["region"] == "kyiv" and STREET_BY_ID and len(parts) >= 2:
            sid = next((k for k, v in STREET_BY_ID.items() if v == parts[0]), None)
            return (f"st_{sid}", parts[0]) if sid else None
        if a["region"] == "dnipro" and DNIPRO and len(parts) >= 3:
            for tid, (tname, _, by_id) in DNIPRO.items():
                if tname == parts[0]:
                    sid = next((k for k, v in by_id.items() if v == parts[1]), None)
                    return (f"ds_{tid}_{sid}", parts[1]) if sid else None
        return None

    def share_url(self, u, a):
        """Telegram's share sheet: neighbours get a link to this street (they pick their house), else to the queue."""
        t = T[u["lang"]]
        sp = self.street_param(a)
        if sp:
            return "https://t.me/share/url?" + urllib.parse.urlencode(
                {"url": f"https://t.me/{BOT_USERNAME}?start={sp[0]}", "text": t["share_street"].format(street=sp[1])})
        text = t["share_text"].format(q=a["queue"], r=region_name(a["region"], u["lang"]))
        return "https://t.me/share/url?" + urllib.parse.urlencode({"url": deep_link(a["region"], a["queue"]), "text": text})

    def status_view(self, u, now=None):
        a = self.active_address(u)
        t = T[u["lang"]]
        if not a:
            return t["need_addr"], None
        if u["chat_id"] < 0:
            return status_text(a["queue"], u["lang"], a["label"], now, a["region"]), \
                [[{"text": t["b_refresh"], "callback_data": "r"}, {"text": t["b_addr"], "callback_data": "a"}]]
        buttons = [[{"text": t["b_rep_off"], "callback_data": "rp:off"}, {"text": t["b_rep_on"], "callback_data": "rp:on"}],
                   [{"text": t["b_oe"] if REGIONS[a["region"]].get("source") else t["b_dtek"], "callback_data": "dtek"}],
                   [{"text": t["b_refresh"], "callback_data": "r"}, {"text": t["b_share"], "url": self.share_url(u, a)}]
                   + ([{"text": t["b_site"], "url": site_link(a["region"], a["queue"])}] if SITE_URL else []),
                   [{"text": t["b_addr"], "callback_data": "a"}, {"text": t["b_settings"], "callback_data": "st"}]]
        text = status_text(a["queue"], u["lang"], a["label"], now, a["region"])
        line = self.crowd_line(a["region"], a["queue"], u["lang"], now)
        return (text + "\n\n" + line if line else text), buttons

    def now_line(self, a, lang, now):
        t = T[lang]
        data = schedule(a["region"])
        days = days_from_today(data, now) if data else []
        if not days or days[0][0] > now:
            return t["now_none"]
        if queue_key(a["queue"]) not in days[0][1]:
            return t["now_noqueue"]
        seq = "".join(slots(g.get(queue_key(a["queue"]))) for _, g in days)
        idx = min(len(seq) - 1, int((now - days[0][0]) // 1800))
        cur, j = seq[idx], idx + 1
        while j < len(seq) and seq[j] == cur:
            j += 1
        key = {"+": "now_on", "-": "now_off", "?": "now_maybe"}[cur]
        return t[key].format(t=fmt(j * 30)) if j < len(seq) else t.get(key + "_end", t["now_on_end"])

    def now_view(self, u, now=None):
        now = now or time.time()
        t = T[u["lang"]]
        g = time.gmtime(now)
        off = calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0)) - kyiv_midnight(g.tm_year, g.tm_mon, g.tm_mday)
        lines = [t["now_title"].format(t=time.strftime("%H:%M", time.gmtime(now + off))), ""]
        for a in self.addresses(u["chat_id"]):
            qlabel = a["label"] in (T["uk"]["queue_label"].format(q=a["queue"]), T["en"]["queue_label"].format(q=a["queue"]))
            lines.append(f"<b>{html.escape(a['label'])}</b>" + ("" if qlabel else f" · {a['queue']}") + f" — {self.now_line(a, u['lang'], now)}")
        return "\n".join(lines), [[{"text": t["b_refresh"], "callback_data": "nw"}, {"text": t["b_addr"], "callback_data": "a"}]]

    # ---------- 4. weekly stats from the daily history
    def record_history(self, region, queue, days):
        for day_ts, groups in days:
            sl = slots(groups.get(queue_key(queue)))
            self.db.q("INSERT OR REPLACE INTO history(region,queue,day_ts,off_min) VALUES(?,?,?,?)",
                      (region, queue, day_ts, sl.count("-") * 30))

    def week_text(self, u, now):
        t = T[u["lang"]]
        today = kyiv_midnight(*kyiv_today())
        h = lambda m: (f"{m // 60} год" + (f" {m % 60} хв" if m % 60 else "")) if u["lang"] == "uk" else (f"{m // 60} h" + (f" {m % 60} min" if m % 60 else ""))  # noqa: E731
        parts = []
        for a in self.addresses(u["chat_id"]):
            rows = self.db.q("SELECT day_ts, off_min FROM history WHERE region=? AND queue=? AND day_ts>? AND day_ts<=?",
                             (a["region"], a["queue"], today - 14 * 86400, today))
            week = {r["day_ts"]: r["off_min"] for r in rows if r["day_ts"] > today - 7 * 86400}
            prev = [r["off_min"] for r in rows if r["day_ts"] <= today - 7 * 86400]
            if len(week) < 3:
                continue
            total = sum(week.values())
            text = t["week_title"].format(label=html.escape(a["label"]), q=a["queue"]) + "\n" + t["week_total"].format(h=h(total))
            if len(prev) >= 3:
                diff = total - sum(prev) * 7 // len(prev)
                text += (t["week_more"] if diff > 0 else t["week_less"]).format(d=h(abs(diff))) if abs(diff) >= 60 else ""
            bars = []
            for day_ts in sorted(week):
                wd = time.gmtime(day_ts + 12 * 3600).tm_wday
                bars.append(f"<code>{t['days_short'][wd]} {'▰' * (week[day_ts] // 60)}{'▱' if week[day_ts] % 60 else ''}</code> {h(week[day_ts])}")
            worst = max(week, key=week.get)
            text += "\n\n" + "\n".join(bars) + "\n\n" + t["week_worst"].format(day=t["days_full"][time.gmtime(worst + 12 * 3600).tm_wday], h=h(week[worst]))
            parts.append(text)
        return "\n\n".join(parts) if parts else t["week_none"]

    def week_tick(self, now):
        """Sunday 19:00: everyone with notifications on gets their week."""
        today = kyiv_midnight(*kyiv_today())
        mins = (now - today) / 60
        if time.gmtime(today + 12 * 3600).tm_wday != 6 or not (19 * 60 <= mins < 19 * 60 + 30):
            return
        for u in self.db.q("SELECT * FROM users WHERE notify=1 AND blocked=0"):
            if self.mark(u["chat_id"], f"week:{today}"):
                text = self.week_text(u, now)
                if text != T[u["lang"]]["week_none"]:
                    self.send_bulk(u["chat_id"], text)

    # ---------- 6. when each oblenergo publishes tomorrow's schedule, and how often it changes it
    def pubs_post(self, now):
        today = kyiv_midnight(*kyiv_today())
        mins = (now - today) / 60
        if time.gmtime(today + 12 * 3600).tm_wday != 6 or not (12 * 60 <= mins < 12 * 60 + 30) or not self.mark(0, f"chan:pubs:{today}"):
            return
        rows = self.db.q("SELECT region, day_ts, at, changes FROM pubs WHERE day_ts>? AND day_ts<=?", (today - 6 * 86400, today + 86400))
        by = {}
        for r in rows:
            by.setdefault(r["region"], []).append(((r["at"] - (r["day_ts"] - 86400)) // 60, r["changes"]))
        avg = sorted((sum(m for m, _ in v) // len(v), r) for r, v in by.items() if len(v) >= 3)
        if len(avg) < 3:
            return
        def when(m):
            return f"{fmt(m)}" + (" (вночі)" if m >= 1440 else "")
        lines = [f"{i}. {short_region(r)} — {when(m)}" for i, (m, r) in enumerate(avg, 1)]
        ch = sorted(((sum(c for _, c in v), r) for r, v in by.items()), reverse=True)
        text = ("📊 <b>Коли обленерго публікували графік на завтра</b>\n<i>середній час за тиждень</i>\n\n" + "\n".join(lines))
        if ch and ch[0][0] > 0:
            text += f"\n\nНайчастіше змінювали графік: {short_region(ch[0][1])} ({ch[0][0]} разів за тиждень)."
        self.channel_post(text, [[{"text": "💡 Моя черга — у боті", "url": f"https://t.me/{BOT_USERNAME}"}]])

    def dtek_view(self, u, now=None):
        """Official ways to report an outage, plus a ready-made text. The person sends it themselves."""
        now = now or time.time()
        t = T[u["lang"]]
        a = self.active_address(u)
        region = a["region"] if a else u["region"]
        R = REGIONS[region]
        phones = ", ".join(f"<b>{p}</b>" + (t["dtek_free"] if p.startswith("0 800") else "") for p in R["phones"])
        lines = [t["dtek_title"].format(co=R["co"][u["lang"]]), ""]
        if R["phones"]:
            lines.append(t["dtek_phones"].format(p=phones))
        co_short = "ДТЕК" if not R.get("source") else R["co"][u["lang"]]
        lines.append(t["dtek_bots"].format(co=co_short) if R.get("tg") else t["oe_site"])
        if a:
            since = ""
            rep_row = self.db.one("SELECT at FROM reports WHERE chat_id=? AND region=? AND queue=? AND state='off' AND at>=?",
                                  (u["chat_id"], region, a["queue"], int(now - REPORT_TTL)))
            data = schedule(region)
            days = days_from_today(data, now) if data else []
            if rep_row and days and days[0][0] <= now:
                since = t["dtek_since"].format(t=fmt((rep_row["at"] - days[0][0]) / 60))
            sched = t["dtek_sched"] if self.scheduled_now(region, a["queue"], now) == "+" else ""
            town = "м. Київ, " if region == "kyiv" else ""
            queue_only = a["label"] in (T["uk"]["queue_label"].format(q=a["queue"]), T["en"]["queue_label"].format(q=a["queue"]))
            addr = town + ("[вкажіть адресу]" if queue_only else a["label"])
            text = t["dtek_text"].format(addr=html.escape(addr), q=a["queue"], since=since, sched=sched)
            lines += ["", t["dtek_text_title"], f"<code>{text}</code>"]
        lines.append("")
        if R.get("city"):
            lines.append(t["dtek_city"].format(n=f"<b>{R['city'][0]}</b>", site=R["city"][2]))
        lines.append(t["dtek_112"])
        rows = [[{"text": t["b_dtek_tg"].format(co=co_short), "url": R["tg"]}]] if R.get("tg") else []
        if R.get("viber"):
            rows.append([{"text": t["b_dtek_viber"].format(co=co_short), "url": R["viber"]}])
        if R.get("source"):  # an oblenergo: its own site as is
            rows.append([{"text": "🌐 " + R["co"][u["lang"]], "url": R["dtek"]}])
        else:
            rows.append([{"text": t["b_dtek_site"], "url": R["dtek"].rsplit("/ua/", 1)[0] + "/ua"}])
        if R.get("city"):
            rows.append([{"text": t["b_city"].format(n=R["city"][0]), "url": R["city"][1]}])
        rows.append([{"text": t["b_back"], "callback_data": "r"}])
        return "\n".join(lines), rows

    # ---------- people's reports
    def crowd(self, region, queue, now=None):
        now = now or time.time()
        rows = self.db.q("SELECT state, COUNT(*) c FROM reports WHERE region=? AND queue=? AND at>=? GROUP BY state",
                         (region, queue, int(now - REPORT_TTL)))
        got = {r["state"]: r["c"] for r in rows}
        return got.get("off", 0), got.get("on", 0)

    def scheduled_now(self, region, queue, now=None):
        now = now or time.time()
        data = schedule(region)
        days = days_from_today(data, now) if data else []
        if not days or days[0][0] > now:
            return None
        return slots(days[0][1].get(queue_key(queue)))[min(47, int((now - days[0][0]) // 1800))]

    def crowd_line(self, region, queue, lang, now=None):
        off, on = self.crowd(region, queue, now)
        if not off and not on:
            return ""
        t = T[lang]
        sched = self.scheduled_now(region, queue, now)
        if sched == "+" and off >= CROWD_MIN and off > on:
            return t["crowd_off"].format(n=t["people"](off), q=queue)
        if sched == "-" and on >= CROWD_MIN and on > off:
            return t["crowd_on"].format(n=t["people"](on), q=queue)
        return t["crowd_counts"].format(off=off, on=on)

    def report(self, chat_id, u, state, now=None):
        """Save 'off'/'on' for the person's main address. Returns the toast text."""
        now = now or time.time()
        t = T[u["lang"]]
        a = self.active_address(u)
        if not a:
            return t["need_addr"].split(".")[0]
        prev = self.db.one("SELECT state, at FROM reports WHERE chat_id=? AND region=? AND queue=?", (chat_id, a["region"], a["queue"]))
        if prev and prev["state"] == state and now - prev["at"] < REPORT_COOLDOWN:
            return t["rep_wait"]
        self.db.q("INSERT OR REPLACE INTO reports(chat_id,region,queue,state,at) VALUES(?,?,?,?,?)",
                  (chat_id, a["region"], a["queue"], state, int(now)))
        return t["rep_thanks_off"] if state == "off" else t["rep_thanks_on"]

    def addr_view(self, u):
        t = T[u["lang"]]
        active = self.active_address(u)
        rows = []
        for a in self.addresses(u["chat_id"]):
            mark = "✓ " if active and a["id"] == active["id"] else ""
            where = "" if a["region"] == "kyiv" else " · " + region_name(a["region"], u["lang"]).split()[0]
            rows.append([{"text": f"{mark}{a['label'][:36]} · {a['queue']}{where}", "callback_data": f"as:{a['id']}"},
                         {"text": "🗑", "callback_data": f"ad:{a['id']}"}])
        if len(rows) < MAX_ADDRESSES:
            add = [{"text": t["b_add"], "callback_data": "aa"}]
            if (STREETS and u["region"] == "kyiv") or (DNIPRO and u["region"] == "dnipro"):  # suggestions while typing
                add.append({"text": t["b_find"], "switch_inline_query_current_chat": ""})
            rows.append(add)
        rows.append([{"text": t["b_back"], "callback_data": "r"}])
        text = t["addr_title"].format(n=MAX_ADDRESSES) + ("" if len(rows) > 2 else "\n\n" + t["addr_empty"])
        return text, rows

    def list_button(self, u):
        t = T[u["lang"]]
        if u["region"] == "dnipro" and DNIPRO:
            return self.dnipro_buttons(u) + [[{"text": t["b_region"].format(r=region_name(u["region"], u["lang"])), "callback_data": "rg"}]]
        rows = ([[{"text": t["b_find"], "switch_inline_query_current_chat": ""}], [{"text": t["b_list"], "callback_data": "L"}]]
                if STREETS and u["region"] == "kyiv" else [])
        rows.append([{"text": T[u["lang"]]["b_region"].format(r=region_name(u["region"], u["lang"])), "callback_data": "rg"}])
        return rows

    def region_view(self, u):
        t = T[u["lang"]]
        rows = [[{"text": ("✓ " if k == u["region"] else "") + region_name(k, u["lang"]), "callback_data": f"rg:{k}"}] for k in REGIONS]
        return t["pick_region"], rows

    def ask_address(self, chat_id, u):
        """Start adding an address in the user's region."""
        t = T[u["lang"]]
        R = REGIONS[u["region"]]
        self.set_state(chat_id, {})
        if not R["yasno"]:
            return self.tg.send(chat_id, *self.queue_pick_view(u))
        self.tg.send(chat_id, t["ask_city"] if R["cities"] else t["ask_street"], self.list_button(u))

    def queue_pick_view(self, u):
        """Regions without address search: DTEK links to look the queue up, then tap it."""
        t = T[u["lang"]]
        R = REGIONS[u["region"]]
        data = schedule(u["region"])
        days = days_from_today(data, time.time()) if data else []
        groups = region_groups(u["region"], data)
        co = R["co"][u["lang"]]
        rows = [[{"text": t["b_dtek_queue_site"], "url": R["dtek"]}]]
        if R.get("tg"):
            rows.append([{"text": t["b_dtek_queue_bot"].format(co=co), "url": R["tg"]}])
        btns = [{"text": q, "callback_data": f"qq:{q}"} for q in groups]
        rows += [btns[i:i + 4] for i in range(0, len(btns), 4)]
        rows.append([{"text": t["b_region"].format(r=region_name(u["region"], u["lang"])), "callback_data": "rg"}])
        text = t["queue_pick"].format(r=region_name(u["region"], u["lang"]), co=co, bot=t["or_bot"] if R.get("tg") else "")
        if R.get("lookup") in ("mk", "pl", "zt", "cvs"):
            rows.insert(0, [{"text": t["b_by_address"], "callback_data": "MK"}])
            return text, rows
        return text + (t.get(f"{R.get('lookup')}_hint") or (t["lookup_hint"] if R.get("lookup") else "")), rows

    # ---------- Dnipro region: town -> (letter) -> street -> house, all by tapping
    def dnipro_buttons(self, u):
        t = T[u["lang"]]
        return [[{"text": t["b_find"], "switch_inline_query_current_chat": ""}],
                [{"text": t["b_cities"], "callback_data": "DT"}, {"text": t["b_villages"], "callback_data": "DV"}]]

    def towns_view(self, u):
        t = T[u["lang"]]
        cities = sorted(((tid, v[0]) for tid, v in DNIPRO.items() if v[0].startswith("м. ")), key=lambda x: ua_key(town_main(x[1])))
        btns = [{"text": town_main(name), "callback_data": f"DX:{tid}"} for tid, name in cities]
        rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
        rows.append([{"text": t["b_villages"], "callback_data": "DV"}])
        return t["pick_town"], rows

    def village_letters_view(self, u):
        t = T[u["lang"]]
        letters = sorted({town_main(v[0])[:1].upper() for v in DNIPRO.values() if not v[0].startswith("м. ")} & set(LETTERS),
                         key=LETTERS.index)
        rows = [[{"text": l, "callback_data": f"DVL:{l}:1"} for l in letters[i:i + 6]] for i in range(0, len(letters), 6)]
        rows.append([{"text": t["b_towns"], "callback_data": "DT"}])
        return t["village_letter"], rows

    def village_page_view(self, u, letter, page):
        t = T[u["lang"]]
        items = sorted(((tid, v[0]) for tid, v in DNIPRO.items()
                        if not v[0].startswith("м. ") and town_main(v[0])[:1].upper() == letter), key=lambda x: ua_key(town_main(x[1])))
        pages = max(1, (len(items) + PAGE - 1) // PAGE)
        page = min(max(1, page), pages)
        rows = [[{"text": name[:60], "callback_data": f"DX:{tid}"}] for tid, name in items[(page - 1) * PAGE: page * PAGE]]
        nav = []
        if page > 1:
            nav.append({"text": "◀️", "callback_data": f"DVL:{letter}:{page - 1}"})
        nav.append({"text": f"{page}/{pages}", "callback_data": "x"})
        if page < pages:
            nav.append({"text": "▶️", "callback_data": f"DVL:{letter}:{page + 1}"})
        rows += [nav, [{"text": t["b_letters"], "callback_data": "DV"}, {"text": t["b_towns"], "callback_data": "DT"}]]
        return t["village_page"].format(l=letter, p=page, n=pages), rows

    def town_view(self, u, tid):
        """A small village: all its streets at once; a town: letters first."""
        t = T[u["lang"]]
        name, by_letter, by_id = DNIPRO[tid]
        if len(by_id) <= 24:
            streets = sorted(by_id.items(), key=lambda x: ua_key(street_main(x[1])))
            rows = [[{"text": v[:60], "callback_data": f"DS:{tid}:{sid}"}] for sid, v in streets]
            rows.append([{"text": t["b_towns"], "callback_data": "DT"}])
            return t["town_streets"].format(t=html.escape(name)), rows
        letters = [l for l in LETTERS if l in by_letter] + (["0–9"] if "0–9" in by_letter else [])
        rows = [[{"text": l, "callback_data": f"DL:{tid}:{l}:1"} for l in letters[i:i + 6]] for i in range(0, len(letters), 6)]
        rows.append([{"text": t["b_towns"], "callback_data": "DT"}])
        return t["town_letters"].format(t=html.escape(name), n=len(by_id)), rows

    def town_page_view(self, u, tid, letter, page):
        t = T[u["lang"]]
        name, by_letter, _ = DNIPRO[tid]
        items = by_letter.get(letter, [])
        pages = max(1, (len(items) + PAGE - 1) // PAGE)
        page = min(max(1, page), pages)
        rows = [[{"text": v[:60], "callback_data": f"DS:{tid}:{sid}"}] for _, sid, v in items[(page - 1) * PAGE: page * PAGE]]
        nav = []
        if page > 1:
            nav.append({"text": "◀️", "callback_data": f"DL:{tid}:{letter}:{page - 1}"})
        nav.append({"text": f"{page}/{pages}", "callback_data": "x"})
        if page < pages:
            nav.append({"text": "▶️", "callback_data": f"DL:{tid}:{letter}:{page + 1}"})
        rows += [nav, [{"text": t["b_letters"], "callback_data": f"DX:{tid}"}, {"text": t["b_towns"], "callback_data": "DT"}]]
        return t["town_page"].format(t=html.escape(name), l=letter, p=page, n=pages), rows

    def pick_dnipro_street(self, chat_id, u, tid, sid, message_id=None):
        if tid not in DNIPRO or sid not in DNIPRO[tid][2]:
            return
        if u["region"] != "dnipro":
            self.set_user(chat_id, region="dnipro")
        self.set_state(chat_id, {"city": {"id": tid, "name": DNIPRO[tid][0]}, "streets": {sid: DNIPRO[tid][2][sid]}})
        self.pick_street(chat_id, self.user(chat_id), sid, message_id)

    def district_view(self, u):
        t = T[u["lang"]]
        names = [{"text": name, "callback_data": f"LL:{i}"} for i, (name, _) in enumerate(DISTRICTS)]
        rows = [names[i:i + 2] for i in range(0, len(names), 2)]
        rows.append([{"text": t["b_all_streets"], "callback_data": "LL:-"}])
        return t["pick_district"], rows

    def district_streets(self, d):
        """STREETS limited to one district (d = index into DISTRICTS) or all of them (d = '-')."""
        if d == "-" or not DISTRICTS or not d.isdigit() or int(d) >= len(DISTRICTS):
            return STREETS
        ids = DISTRICTS[int(d)][1]
        return {l: [x for x in items if x[1] in ids] for l, items in STREETS.items() if any(x[1] in ids for x in items)}

    def letters_view(self, u, d="-"):
        t = T[u["lang"]]
        streets = self.district_streets(d)
        letters = [l for l in LETTERS if l in streets] + (["0–9"] if "0–9" in streets else [])
        rows = [[{"text": l, "callback_data": f"L:{d}:{l}:1"} for l in letters[i:i + 6]] for i in range(0, len(letters), 6)]
        if DISTRICTS:
            rows.append([{"text": t["b_districts"], "callback_data": "LD"}])
        if d != "-" and DISTRICTS:
            name = DISTRICTS[int(d)][0]
            return t["district_letters"].format(d=name, n=sum(len(v) for v in streets.values())), rows
        return t["pick_letter"], rows

    def letter_page_view(self, u, letter, page, d="-"):
        t = T[u["lang"]]
        items = self.district_streets(d).get(letter, [])
        pages = max(1, (len(items) + PAGE - 1) // PAGE)
        page = min(max(1, page), pages)
        rows = [[{"text": value[:60], "callback_data": f"ls:{sid}"}] for _, sid, value in items[(page - 1) * PAGE: page * PAGE]]
        nav = []
        if page > 1:
            nav.append({"text": "◀️", "callback_data": f"L:{d}:{letter}:{page - 1}"})
        nav.append({"text": f"{page}/{pages}", "callback_data": "x"})
        if page < pages:
            nav.append({"text": "▶️", "callback_data": f"L:{d}:{letter}:{page + 1}"})
        back = [{"text": t["b_letters"], "callback_data": f"LL:{d}"}]
        if DISTRICTS:
            back.append({"text": t["b_districts"], "callback_data": "LD"})
        rows += [nav, back]
        if d != "-" and DISTRICTS:
            return t["district_page"].format(d=DISTRICTS[int(d)][0], l=letter, p=page, n=pages), rows
        return t["letter_page"].format(l=letter, p=page, n=pages), rows

    def list_start(self, u):
        """First screen of 'pick from the list': districts if we have them, else letters."""
        return self.district_view(u) if DISTRICTS else self.letters_view(u)

    def settings_view(self, u):
        t = T[u["lang"]]
        onoff = lambda v: t["on"] if v else t["off"]  # noqa: E731
        rows = [[{"text": f"{t['s_notify']}: {onoff(u['notify'])}", "callback_data": "sn"}],
                [{"text": ("✓ " if u["warn_min"] == m else "") + t["s_min"].format(m=m), "callback_data": f"sw:{m}"} for m in WARN_CHOICES],
                [{"text": f"{t['s_ret']}: {onoff(u['ret'])}", "callback_data": "sr"}],
                [{"text": f"{t['s_quiet']}: {onoff(u['quiet'])}", "callback_data": "sq"}],
                [{"text": t["b_region"].format(r=region_name(u["region"], u["lang"])), "callback_data": "rg"}],
                [{"text": t["s_lang"], "callback_data": "sl"}]]
        if u["chat_id"] > 0:
            rows.append([{"text": t["b_group"], "url": f"https://t.me/{BOT_USERNAME}?startgroup=1"}])
        rows.append([{"text": t["b_back"], "callback_data": "r"}])
        return f"{t['set_title']}\n\n{t['s_before']}", rows

    # ---------- incoming
    def chat_lock(self, chat_id):
        with self.locks_lock:
            return self.chat_locks.setdefault(chat_id, threading.Lock())

    GROUP_FREE_CALLBACKS = ("r", "x", "nw", "hp")  # anyone in a group may refresh or page; the rest is for admins

    def is_group_admin(self, chat_id, user_id):
        key = (chat_id, user_id)
        hit = self.admin_cache.get(key)
        if hit and time.time() - hit[1] < 600:
            return hit[0]
        try:
            st = (self.tg.call("getChatMember", timeout=10, chat_id=chat_id, user_id=user_id).get("result") or {}).get("status")
        except Exception:
            st = None
        ok = st in ("creator", "administrator")
        self.admin_cache[key] = (ok, time.time())
        return ok

    def group_user(self, chat_id):
        """A group chat is stored like a person: its address, its reminders. Groups default to quiet nights and power-back messages."""
        if not self.db.one("SELECT 1 FROM users WHERE chat_id=?", (chat_id,)):
            self.user(chat_id, "uk")
            self.set_user(chat_id, quiet=1, ret=1)
        return self.user(chat_id)

    def group_message(self, m):
        chat_id = m["chat"]["id"]
        if m.get("migrate_to_chat_id"):  # the group became a supergroup: it keeps its address
            new = m["migrate_to_chat_id"]
            for table in ("users", "addresses"):
                self.db.q(f"UPDATE {table} SET chat_id=? WHERE chat_id=?", (new, chat_id))
            return
        me = BOT_USERNAME.lower()
        if any((x.get("username") or "").lower() == me for x in m.get("new_chat_members") or []) or m.get("group_chat_created"):
            u = self.group_user(chat_id)
            if u["blocked"]:
                self.set_user(chat_id, blocked=0)
            return self.tg.send(chat_id, T[u["lang"]]["g_welcome"])
        text = (m.get("text") or "").strip()
        if not text:
            return
        rest = ""
        reply_to_me = ((m.get("reply_to_message") or {}).get("from") or {}).get("username", "").lower() == me
        if text.startswith("/"):
            cmd, _, rest = text.partition(" ")
            name, _, target = cmd.partition("@")
            if target and target.lower() != me:
                return  # a command for another bot
            text = (name + (" " + rest if rest else "")).strip()
            name = name.lower()
        elif not reply_to_me:
            return  # ordinary chat messages are not for the bot
        else:
            name = ""
        u = self.group_user(chat_id)
        t = T[u["lang"]]
        if name in ("/start", "/help"):
            return self.tg.send(chat_id, t["g_welcome"])
        if name in ("/now", "/pic", "/all", "/week"):
            return self.text(chat_id, text)
        if not self.is_group_admin(chat_id, (m.get("from") or {}).get("id")):
            return self.tg.send(chat_id, t["g_admin_only"])
        st = self.state(u)
        if name == "/address" and rest and st.get("mk") or (name == "/address" and rest and st.get("zp_street")):
            return self.free_text(chat_id, u, rest)  # a house number in a flow that's already going
        return self.text(chat_id, text)

    def handle(self, upd):
        if "callback_query" in upd:
            cq = upd["callback_query"]
            if "message" not in cq:
                return
            chat_id = cq["message"]["chat"]["id"]
            if chat_id < 0 and cq.get("data", "").partition(":")[0] not in self.GROUP_FREE_CALLBACKS \
                    and not self.is_group_admin(chat_id, cq.get("from", {}).get("id")):
                u = self.group_user(chat_id)
                try:
                    self.tg.call("answerCallbackQuery", timeout=10, callback_query_id=cq["id"], text=T[u["lang"]]["g_admin_toast"], show_alert=True)
                except Exception:
                    pass
                return
            with self.chat_lock(chat_id):
                self.callback(cq)
        elif "inline_query" in upd:
            self.inline(upd["inline_query"])
        elif "message" in upd and upd["message"].get("chat", {}).get("type") in ("group", "supergroup"):
            with self.chat_lock(upd["message"]["chat"]["id"]):
                self.group_message(upd["message"])
        elif "message" in upd and "text" in upd["message"]:
            m = upd["message"]
            with self.chat_lock(m["chat"]["id"]):
                self.text(m["chat"]["id"], m["text"], m.get("from", {}).get("language_code"))

    # ---------- inline search: '@LightUkraine_bot Свобо' shows matching Kyiv streets while typing
    def street_matches(self, q, limit=20):
        ql = q.lower().replace("’", "'").strip()
        res = [(sid, v) for sid, v in STREET_BY_ID.items() if ql in v.lower().replace("’", "'")]
        res.sort(key=lambda x: (not street_main(x[1]).lower().startswith(ql), ua_key(street_main(x[1]))))
        return res[:limit]

    def inline(self, iq):
        lang = "en" if (iq.get("from", {}).get("language_code") or "").startswith("en") else "uk"
        row = self.db.one("SELECT lang FROM users WHERE chat_id=?", (iq.get("from", {}).get("id"),))
        t = T[row["lang"] if row else lang]
        q = (iq.get("query") or "").strip()
        districts = {}
        for name, ids in DISTRICTS:
            for sid in ids:
                districts.setdefault(sid, []).append(name)
        results = []
        region = (self.db.one("SELECT region FROM users WHERE chat_id=?", (iq.get("from", {}).get("id"),)) or {"region": "kyiv"})["region"]
        if len(q) >= 2 and region == "dnipro" and DNIPRO:
            ql = q.lower().replace("’", "'")
            hits = []
            for tid, (tname, _, by_id) in DNIPRO.items():
                for sid, value in by_id.items():
                    if ql in value.lower().replace("’", "'"):
                        hits.append((not tname.startswith("м. "), not street_main(value).lower().startswith(ql), ua_key(street_main(value)), tid, sid, value, tname))
                if len(hits) > 400:
                    break
            for *_, tid, sid, value, tname in sorted(hits)[:20]:
                results.append({"type": "article", "id": f"d{tid}_{sid}", "title": value, "description": tname,
                                "input_message_content": {"message_text": f"📍 {value} · {tname}"}})
            if not results:
                results.append({"type": "article", "id": "none", "title": t["inline_none"], "description": t["inline_none_d"],
                                "input_message_content": {"message_text": q}})
        elif len(q) >= 2 and STREET_BY_ID:
            for sid, value in self.street_matches(q):
                where = ", ".join(districts.get(sid, [])) or t["inline_kyiv"]
                results.append({"type": "article", "id": f"s{sid}", "title": value, "description": where,
                                "input_message_content": {"message_text": f"📍 {value}"}})
            if not results:
                results.append({"type": "article", "id": "none", "title": t["inline_none"], "description": t["inline_none_d"],
                                "input_message_content": {"message_text": q}})
        params = {"inline_query_id": iq["id"], "results": results, "cache_time": 300}
        if len(q) < 2:
            params["button"] = {"text": t["inline_hint"], "start_parameter": "find"}
        try:
            self.tg.call("answerInlineQuery", timeout=10, **params)
        except Exception as e:
            log("answerInlineQuery failed:", repr(e))

    def text(self, chat_id, msg, lang_code=None):
        u = self.user(chat_id, lang_code)
        t = T[u["lang"]]
        m = msg.strip()
        cmd, _, arg = m.partition(" ")
        cmd = cmd.split("@")[0].lower()
        arg = arg.strip()
        sm = _re.fullmatch(r"st_(\d{1,9})|ds_(\d{1,9})_(\d{1,9})", arg) if cmd == "/start" else None
        if sm and sm[1] and sm[1] in STREET_BY_ID:  # a neighbour's link: open on their street, they pick the house
            self.set_user(chat_id, region="kyiv")
            self.set_state(chat_id, {"streets": {sm[1]: STREET_BY_ID[sm[1]]}})
            return self.pick_street(chat_id, self.user(chat_id), sm[1])
        if sm and sm[2]:
            return self.pick_dnipro_street(chat_id, u, sm[2], sm[3])
        if cmd == "/start" and parse_start(arg):
            region, q = parse_start(arg)
            data = schedule(region)
            days = days_from_today(data, time.time()) if data else []
            if q in region_groups(region, data):
                label = T[u["lang"]]["queue_label"].format(q=q)
                if not self.db.one("SELECT 1 FROM addresses WHERE chat_id=? AND region=? AND queue=?", (chat_id, region, q)):
                    if self.add_address(chat_id, label, q, region):
                        self.tg.send(chat_id, t["deep_added"].format(q=q, r=region_name(region, u["lang"])))
                else:
                    aid = self.db.one("SELECT id FROM addresses WHERE chat_id=? AND region=? AND queue=?", (chat_id, region, q))["id"]
                    self.set_user(chat_id, active=aid)
                self.set_user(chat_id, region=region)
                self.tg.send(chat_id, *self.status_view(self.user(chat_id)))
            else:
                self.tg.send(chat_id, t["welcome"], self.list_button(u))
        elif cmd == "/start":
            if self.active_address(u):
                self.tg.send(chat_id, *self.status_view(u))
            else:
                self.tg.send(chat_id, t["welcome"], self.list_button(u))
        elif cmd == "/region":
            self.tg.send(chat_id, *self.region_view(u))
        elif cmd == "/help":
            self.tg.send(chat_id, t["help"])
        elif cmd == "/now":
            self.tg.send(chat_id, *self.status_view(u))
        elif cmd == "/all":
            self.tg.send(chat_id, *self.now_view(u))
        elif cmd == "/group":
            self.tg.send(chat_id, t["group_info"], [[{"text": t["b_group"], "url": f"https://t.me/{BOT_USERNAME}?startgroup=1"}]])
        elif cmd == "/pic":
            a = self.active_address(u)
            if not a:
                self.tg.send(chat_id, t["need_addr"])
            else:
                self.tg.photo(chat_id, queue_png(a["region"], a["queue"]), t["img_caption"].format(q=a["queue"], label=html.escape(a["label"])))
        elif cmd == "/week":
            self.tg.send(chat_id, self.week_text(u, time.time()))
        elif cmd in ("/address", "/addresses"):
            if arg:
                self.set_state(chat_id, {})
                self.free_text(chat_id, self.user(chat_id), arg)
            else:
                self.tg.send(chat_id, *self.addr_view(u))
        elif cmd == "/settings":
            self.tg.send(chat_id, *self.settings_view(u))
        elif cmd == "/queue":
            data = schedule(u["region"])
            q = arg.strip() if arg.strip() in region_groups(u["region"], data) else norm_queue(arg)
            days = days_from_today(data, time.time()) if data else []
            if not q or q not in region_groups(u["region"], data):
                self.tg.send(chat_id, t["bad_queue"])
            elif self.add_address(chat_id, t["queue_label"].format(q=q), q, u["region"]):
                self.tg.send(chat_id, *self.status_view(self.user(chat_id)))
        elif cmd == "/lang":
            self.set_user(chat_id, lang="en" if u["lang"] == "uk" else "uk")
            self.tg.send(chat_id, *self.settings_view(self.user(chat_id)))
        elif cmd == "/myid":
            self.tg.send(chat_id, t["myid"].format(id=chat_id))
        elif cmd == "/stats" and ADMIN_CHAT_ID and str(chat_id) == ADMIN_CHAT_ID:
            self.tg.send(chat_id, self.stats_text(u["lang"]))
        elif cmd == "/health" and ADMIN_CHAT_ID and str(chat_id) == ADMIN_CHAT_ID:
            self.tg.send(chat_id, self.health_text())
        elif cmd.startswith("/"):
            self.tg.send(chat_id, t["help"])
        elif m.startswith("📍") and " · " in m and DNIPRO:
            street, _, town = m.lstrip("📍").strip().rpartition(" · ")
            tid = next((k for k, v in DNIPRO.items() if v[0] == town), None)
            sid = next((k for k, v in DNIPRO[tid][2].items() if v == street), None) if tid else None
            if tid and sid:
                self.pick_dnipro_street(chat_id, u, tid, sid)
            else:
                self.free_text(chat_id, u, street)
        elif m.startswith("📍") and STREET_BY_ID:
            name = m.lstrip("📍").strip()
            sid = next((k for k, v in STREET_BY_ID.items() if v == name), None)
            if not sid:
                return self.free_text(chat_id, u, name)
            if u["region"] != "kyiv":  # inline search covers Kyiv streets
                self.set_user(chat_id, region="kyiv")
            self.set_state(chat_id, {"streets": {sid: name}})
            self.pick_street(chat_id, self.user(chat_id), sid)
        else:
            self.free_text(chat_id, u, m)

    # ---------- watchdog: every source is read every 30 min; failures and long silences go to the admin
    def health(self, region):
        row = self.db.one("SELECT value FROM cache WHERE key=?", (f"health:{region}",))
        return json.loads(row["value"]) if row else {}

    def set_health(self, region, h):
        self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (f"health:{region}", json.dumps(h), int(time.time())))

    @staticmethod
    def kyiv_time(ts):
        g = time.gmtime(ts)
        off = calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0)) - kyiv_midnight(g.tm_year, g.tm_mon, g.tm_mday)
        return time.strftime("%d.%m %H:%M", time.gmtime(ts + off))

    def health_tick(self, now=None, fetch=True):
        now = now or time.time()
        if not ADMIN_CHAT_ID:
            return
        has = {}
        for region in REGIONS:
            data = schedule(region) if fetch else _scheds[region]["data"]
            has[region] = bool(data and days_from_today(data, now))
        most = sum(has.values()) >= len(REGIONS) / 2
        for region in REGIONS:
            sc, h = _scheds[region], self.health(region)
            co = REGIONS[region].get("co", {}).get("uk") or "outage-data-ua (ДТЕК)"
            name = f"{region_name(region, 'uk')} — {co}"
            if sc.get("err") and not h.get("fail_since"):
                h["fail_since"], h["err"] = int(sc["err_at"]), sc["err"]
            elif sc.get("err"):
                h["err"] = sc["err"]
            elif not sc.get("err") and h.get("fail_since"):
                h.pop("fail_since")
                if h.get("alert") == "fail":
                    h.pop("alert")
                    self.admin(f"✅ <b>{html.escape(name)}</b>: знову читається.")
            if has[region]:
                h["data_at"] = int(now)
                if h.get("alert") == "empty":
                    h.pop("alert")
                    self.admin(f"✅ <b>{html.escape(name)}</b>: графік знову є.")
            if h.get("fail_since") and now - h["fail_since"] >= HEALTH_FAIL and h.get("alert") != "fail":
                h["alert"] = "fail"
                self.admin(f"⚠️ <b>{html.escape(name)}</b>: джерело не читається з {self.kyiv_time(h['fail_since'])}.\n"
                           f"<code>{html.escape(h.get('err') or '')}</code>\n"
                           f"<i>Користувачі бачать останній збережений графік. Можливо, сайт змінився або не працює.</i>")
            elif (not h.get("fail_since") and not has[region] and most and h.get("data_at")
                  and now - h["data_at"] >= HEALTH_EMPTY and h.get("alert") != "empty"):
                h["alert"] = "empty"
                self.admin(f"⚠️ <b>{html.escape(name)}</b>: графіка немає з {self.kyiv_time(h['data_at'])}, хоча в більшості областей він є.\n"
                           f"<i>Сайт читається без помилок — можливо, змінився формат. Перевірте: {REGIONS[region]['dtek']}</i>")
            self.set_health(region, h)

    def health_text(self):
        now = time.time()
        lines = ["<b>Джерела графіків</b>"]
        for region in REGIONS:
            sc, h = _scheds[region], self.health(region)
            data = sc["data"]
            days = len(days_from_today(data, now)) if data else 0
            if sc.get("err"):
                mark = f"❌ помилка з {self.kyiv_time(h.get('fail_since') or sc['err_at'])}"
            elif not sc["ok_at"]:
                mark = "… ще не читалося"
            else:
                mark = f"✅ {self.kyiv_time(sc['ok_at'])[6:]}"
            got = f"днів: {days}" if days else ("графіка немає" + (f" з {self.kyiv_time(h['data_at'])}" if h.get("data_at") else ""))
            lines.append(f"{region_name(region, 'uk')}: {mark} · {got}")
        return "\n".join(lines)

    def stats_text(self, lang):
        """Admin-only summary of who uses the bot (/stats, only for ADMIN_CHAT_ID)."""
        n = lambda sql, a=(): self.db.one(sql, a)[0]  # noqa: E731
        now = int(time.time())
        regions = self.db.q("SELECT region, COUNT(*) c FROM users WHERE blocked=0 GROUP BY region ORDER BY c DESC")
        queues = self.db.q("SELECT queue, COUNT(DISTINCT chat_id) c FROM addresses GROUP BY queue ORDER BY c DESC, queue LIMIT 5")
        langs = self.db.q("SELECT lang, COUNT(*) c FROM users WHERE blocked=0 GROUP BY lang ORDER BY c DESC")
        return T[lang]["stats"].format(
            total=n("SELECT COUNT(*) FROM users"),
            active=n("SELECT COUNT(*) FROM users WHERE blocked=0"),
            blocked=n("SELECT COUNT(*) FROM users WHERE blocked=1"),
            with_addr=n("SELECT COUNT(DISTINCT u.chat_id) FROM users u JOIN addresses a ON a.chat_id=u.chat_id WHERE u.blocked=0"),
            notify=n("SELECT COUNT(*) FROM users WHERE blocked=0 AND notify=1"),
            d1=n("SELECT COUNT(*) FROM users WHERE joined>=?", (now - 86400,)),
            d7=n("SELECT COUNT(*) FROM users WHERE joined>=?", (now - 7 * 86400,)),
            regions="\n".join(f"• {region_name(r['region'], lang)}: {r['c']}" for r in regions) or "—",
            addrs=n("SELECT COUNT(*) FROM addresses"),
            queues=", ".join(f"{r['queue']} ({r['c']})" for r in queues) or "—",
            langs=", ".join(f"{r['lang']} {r['c']}" for r in langs) or "—",
        )

    def free_text(self, chat_id, u, m):
        """Plain text: a house number, a street name or (in regions with towns) a town name."""
        st = self.state(u)
        R = REGIONS[u["region"]]
        if R.get("lookup") in ("mk", "pl", "zt", "cvs") and st.get("mk"):
            if st.get("street"):
                hit = next((k for k, v in st.get("houses", {}).items() if v.lower() == m.strip().lower()), None)
                return self.pick_house(chat_id, u, hit) if hit else self.tg.send(chat_id, T[u["lang"]]["no_houses"].format(q=html.escape(m)))
            return self.tg.send(chat_id, *self.mk_level_view(u, query=m))
        if R.get("lookup") == "voe":
            return self.voe_text(chat_id, u, m)
        if R.get("lookup") == "zpa":
            return self.zp_text(chat_id, u, m)
        if R.get("lookup") == "loe":
            if st.get("street"):
                return self.loe_house_text(chat_id, u, m)
            if st.get("city"):
                return self.loe_streets(chat_id, u, m)
            return self.loe_cities(chat_id, u, m)
        if not R["yasno"]:
            return self.ask_address(chat_id, u)
        if st.get("street"):
            return self.search_houses(chat_id, u, m)
        if R["cities"] and not st.get("city"):
            return self.search_cities(chat_id, u, m)
        return self.search_streets(chat_id, u, m)

    # ---------- Mykolaivoblenergo directory (off.energy.mk.ua): дільниця -> town -> street -> house -> queue
    def mk_get(self, path):
        key = "mk:" + path
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < 7 * 86400 and "outage-queue" not in path:
            return json.loads(row["value"])
        try:
            val = http_json("https://off.energy.mk.ua/api" + path, timeout=30)
        except Exception:
            if row:
                return json.loads(row["value"])
            raise
        self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(val, ensure_ascii=False), int(time.time())))
        return val

    def pl_post(self, url, obj, cache=True):
        """Poltavaoblenergo's address directory (POST disconn=<json>); lists cached a week."""
        key = "pl:" + json.dumps(obj, ensure_ascii=False, sort_keys=True)
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,)) if cache else None
        if row and time.time() - row["at"] < 7 * 86400:
            return json.loads(row["value"])
        try:
            val = json.loads(oe_page(url, urllib.parse.urlencode({"disconn": json.dumps(obj, ensure_ascii=False)}).encode()))
        except Exception:
            if row:
                return json.loads(row["value"])
            raise
        if cache:
            self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(val, ensure_ascii=False), int(time.time())))
        return val

    def pl_list(self, op, fid="", city="", street=""):
        res = self.pl_post("https://www.poe.pl.ua/customs/search-disconnection-cabinet.php",
                           {"operation_type": op, "filial_id": fid, "city_id": city, "street_id": street})
        return [[str(x["instance_id"]), str(x["instance_name"]).strip()] for x in res if isinstance(x, dict)
                and x.get("instance_id") is not None and str(x.get("instance_name") or "").strip() not in ("", "_")
                and not str(x.get("instance_name")).strip().endswith(" _")]

    def zt_get(self, query, ttl=7 * 86400):
        """Zhytomyroblenergo's unhooking-search.php (windows-1251), cached."""
        key = "zt:" + query
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < ttl:
            return row["value"]
        try:
            req = urllib.request.Request("https://www.ztoe.com.ua/unhooking-search.php" + query,
                                         headers={"User-Agent": "svitlo-kyiv-bot/1.0 (github.com/vitaliypashkovskyi-ai/svitlo)"})
            with urllib.request.urlopen(req, timeout=40) as r:
                page = r.read(3_000_000).decode("cp1251", "replace")
        except Exception:
            if row:
                return row["value"]
            raise
        self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, page, int(time.time())))
        return page

    @staticmethod
    def zt_options(page, name):
        m = _re.search(r'<select name="' + name + r'"[^>]*>(.*?)</select>', page, _re.S)
        return [[v, html_text(t).strip()] for v, t in _re.findall(r'<option[^>]*value="?(\d+)"?[^>]*>(.*?)</option>', m.group(1) if m else "")
                if v != "0" and html_text(t).strip()]

    def zt_rows(self, path, street_id):
        """[(houses, queue or 'NOT_IN', household?)] for a street."""
        page = self.zt_get(f"?rem_id={path['fil']['id']}&naspunkt_id={path['ns']['id']}&vulica_id={street_id}&all=1", ttl=86400)
        out = []
        k = page.find("</select>", page.find('name="vulica_id"'))
        for tr in _re.findall(r"<tr.*?</tr>", page[k:], _re.S):
            cells = [html_text(c).strip() for c in _re.findall(r"<td[^>]*>(.*?)</td>", tr, _re.S)]
            if len(cells) < 7 or cells[0] == "РЕМ":
                continue
            q = f"{cells[4]}.{cells[5]}" if cells[4].isdigit() and cells[5].isdigit() else ("NOT_IN" if "не вимикається" in cells[4] else "")
            out.append((cells[3], q, cells[6].startswith("побутові")))
        return out

    def cv_dir(self, diln):
        """Chernivtsioblenergo's published list for a district office: {town: {street: {house: group}}}. Cached a day."""
        key = f"cv:dir:{diln}"
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < 86400:
            return json.loads(row["value"])
        try:
            page = oe_page(f"https://oblenergo.cv.ua/shutdowns-search/{int(diln)}/")
        except Exception:
            if row:
                return json.loads(row["value"])
            raise
        out = {}
        for town, street, house, group in _re.findall(r"<div>(?:<s>(.*?)</s>)?<p>(.*?)</p><b>(.*?)</b><u>(\d{1,2})</u></div>", page):
            street, house = html.unescape(street).strip(), html.unescape(house).strip()
            if not street or _re.fullmatch(r"[0-9a-f-]{30,}", street) or _re.fullmatch(r"[0-9a-f-]{30,}", house):
                continue
            house = _re.sub(r"^буд\.\s*", "", house)[:20]
            town = html.unescape(town).strip() or "м. Чернівці"
            out.setdefault(town, {}).setdefault(street, {})[house] = group
        if out:
            self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(out, ensure_ascii=False), int(time.time())))
        return out

    def cv_streets(self, path):
        return sorted(self.cv_dir(path["fil"]["id"]).get(path["ns"]["name"], {}), key=ua_key)

    def dir_filials(self, region):
        if region == "cvs":
            page = oe_page("https://oblenergo.cv.ua/shutdowns-search/")
            m = _re.search(r'<select name="diln"[^>]*>(.*?)</select>', page, _re.S)
            items = [[v, html.unescape(n).strip()] for v, n in _re.findall(r'value="(\d+)"[^>]*>([^<]+)', m.group(1) if m else "") if v != "0"]
            return sorted(items, key=lambda x: (x[1] != "ЧЕРНІВЦІ", ua_key(x[1])))
        if region == "zt":
            return sorted(self.zt_options(self.zt_get(""), "rem_id"), key=lambda x: (x[1] != "Житомирський", ua_key(x[1])))
        if region == "pl":
            key = "pl:filials"
            row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
            if row and time.time() - row["at"] < 7 * 86400:
                items = json.loads(row["value"])
            else:
                page = oe_page("https://www.poe.pl.ua/disconnection/power-outages/")
                sel = _re.search(r'<select[^>]*id="input-filial-main"[^>]*>(.*?)</select>', page, _re.S)
                items = [[i, html.unescape(n).strip()] for i, n in _re.findall(r'<option[^>]*value="(\d+)"[^>]*>(.*?)</option>', sel.group(1) if sel else "")
                         if i != "0"]
                if items:
                    self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(items, ensure_ascii=False), int(time.time())))
                elif row:
                    items = json.loads(row["value"])
            return sorted(items, key=lambda x: (not x[1].startswith("Полтавська об"), ua_key(x[1])))
        return sorted(([str(x["idfilial"]), x["fullname"]] for x in self.mk_get("/addr/filii") if x.get("idfilial")),
                      key=lambda x: (not x[1].startswith("Миколаївський"), ua_key(x[1])))

    def dir_towns(self, region, fid):
        if region == "cvs":  # ids are positions in the sorted list (names can be too long for a button)
            return [[str(i), n] for i, n in enumerate(sorted(self.cv_dir(fid), key=ua_key))]
        if region == "zt":
            return self.zt_options(self.zt_get(f"?rem_id={fid}"), "naspunkt_id")
        if region == "pl":
            return self.pl_list("city", fid)
        return [[str(x["idnaspunkt"]), x["naznaspunkt"]] for x in self.mk_get(f"/addr/filii/{fid}/ns") if x.get("idnaspunkt")]

    def dir_streets(self, region, path, town_id):
        if region == "cvs":
            return [[str(i), n] for i, n in enumerate(self.cv_streets(path))]
        if region == "zt":
            return self.zt_options(self.zt_get(f"?rem_id={path['fil']['id']}&naspunkt_id={town_id}"), "vulica_id")
        if region == "pl":
            return self.pl_list("street", path["fil"]["id"], town_id)
        return [[str(x["idstreet"]), x["nazstreet"]] for x in self.mk_get(f"/addr/ns/{town_id}/street") if x.get("idstreet")]

    def dir_houses(self, region, path, street_id):
        if region == "cvs":
            streets = self.cv_streets(path)
            name = streets[int(street_id)] if street_id.isdigit() and int(street_id) < len(streets) else None
            return [(h, h) for h in self.cv_dir(path["fil"]["id"]).get(path["ns"]["name"], {}).get(name, {})] if name else []
        if region == "zt":  # houses by name; their queue is looked up again from the same (cached) rows
            names = []
            for houses, _, _ in self.zt_rows(path, street_id):
                for h in houses.split(","):
                    h = h.strip()[:20]
                    if h and any(ch.isdigit() for ch in h) and h not in names:
                        names.append(h)
            return [(h, h) for h in names]
        if region == "pl":  # the result is asked by the house's name, so the name is its id too
            return [(n, n) for _, n in self.pl_list("building", path["fil"]["id"], path["ns"]["id"], street_id)]
        return [(str(x["iddom"]), str(x["nazdom"]).strip()) for x in self.mk_get(f"/addr/street/{street_id}/dom") if x.get("iddom")]

    def dir_queue(self, region, path, street_id, hid):
        if region == "cvs":
            streets = self.cv_streets(path)
            name = streets[int(street_id)] if street_id.isdigit() and int(street_id) < len(streets) else None
            return self.cv_dir(path["fil"]["id"]).get(path["ns"]["name"], {}).get(name, {}).get(hid) if name else None
        if region == "zt":  # households first (a building's shops can sit on another line), then the most common
            rows = [(q, home) for houses, q, home in self.zt_rows(path, street_id) if hid in (h.strip() for h in houses.split(","))]
            for pick in ([q for q, home in rows if home], [q for q, _ in rows]):
                std = [q for q in pick if q in STANDARD_GROUPS]
                if std:
                    return max(set(std), key=std.count)
                if "NOT_IN" in pick:
                    return "NOT_IN"
            return None
        if region == "pl":
            res = self.pl_post("https://www.poe.pl.ua/customs/disconnectinforesult-cabinet.php",
                               {"cityid": path["ns"]["id"], "streerid": street_id, "house": hid}, cache=False)
            x = res[0] if isinstance(res, list) and res and isinstance(res[0], dict) else {}
            return f"{x.get('gp_qnum_1')}.{x.get('gp_qnum_2')}" if x.get("gp_qnum_1") else None
        res = self.mk_get(f"/addr/dom/{hid}/outage-queue")
        return next((str(x["outage"]["name"]) for x in res if isinstance(x, dict) and isinstance(x.get("outage"), dict)
                     and isinstance(x.get("outageType"), dict) and x["outageType"].get("name") == "ГПВ"), None)

    def mk_filials_view(self, u):
        t = T[u["lang"]]
        lk = REGIONS[u["region"]].get("lookup")
        btns = [{"text": n[:40], "callback_data": f"MKF:{i}"} for i, n in self.dir_filials(lk)]
        return t[f"{lk}_filial"], [btns[i:i + 2] for i in range(0, len(btns), 2)]

    def mk_set_level(self, chat_id, level, items, path):
        self.set_state(chat_id, {"mk": {"level": level, "items": items, "path": path}})

    def mk_level_view(self, u, letter=None, page=1, query=None):
        """The current list (towns or streets): letters if it's long, else (or after a letter/text) paged buttons."""
        t = T[u["lang"]]
        mk = self.state(u)["mk"]
        items, what = mk["items"], t["mk_level"][mk["level"]]
        if query:
            ql = query.lower().strip()
            sel = [x for x in items if ql in x[1].lower()]
            sel.sort(key=lambda x: (not street_main(x[1]).lower().startswith(ql), ua_key(street_main(x[1]))))
            letter = "?"
        elif letter:
            sel = [x for x in items if street_main(x[1])[:1].upper() == letter]
        elif len(items) > 30:
            letters = sorted({street_main(x[1])[:1].upper() for x in items} & set(LETTERS), key=LETTERS.index)
            rows = [[{"text": l, "callback_data": f"MKL:{l}:1"} for l in letters[i:i + 6]] for i in range(0, len(letters), 6)]
            rows.append([{"text": t["b_back_level"], "callback_data": "MK"}])
            return t["mk_letters"].format(what=what), rows
        else:
            sel = sorted(items, key=lambda x: ua_key(street_main(x[1])))
        pages = max(1, (len(sel) + PAGE - 1) // PAGE)
        page = min(max(1, page), pages)
        rows = [[{"text": n[:60], "callback_data": f"MKI:{i}"}] for i, n in sel[(page - 1) * PAGE: page * PAGE]]
        if pages > 1 and letter != "?":
            nav = ([{"text": "◀️", "callback_data": f"MKL:{letter or '-'}:{page - 1}"}] if page > 1 else []) + \
                  [{"text": f"{page}/{pages}", "callback_data": "x"}] + \
                  ([{"text": "▶️", "callback_data": f"MKL:{letter or '-'}:{page + 1}"}] if page < pages else [])
            rows.append(nav)
        rows.append([{"text": t["b_back_level"], "callback_data": "MKB"}])
        return t["mk_page"].format(what=what, p=page, n=pages), rows

    def mk_pick(self, chat_id, u, item_id, message_id):
        mk = self.state(u).get("mk") or {}
        name = dict((i, n) for i, n in mk.get("items", [])).get(item_id)
        if not name:
            return
        path = {**mk.get("path", {})}
        if mk["level"] == "ns":
            path["ns"] = {"id": item_id, "name": name}
            items = self.dir_streets(REGIONS[u["region"]].get("lookup"), path, item_id)
            self.mk_set_level(chat_id, "street", items, path)
            return self.tg.edit(chat_id, message_id, *self.mk_level_view(self.user(chat_id)))
        # a street: its houses as buttons
        houses = self.dir_houses(REGIONS[u["region"]].get("lookup"), path, item_id)

        def natural(h):
            n = ""
            for ch in h[1]:
                if not ch.isdigit():
                    break
                n += ch
            return (int(n) if n else 10 ** 9, h[1])
        houses.sort(key=natural)
        self.set_state(chat_id, {"mk": {**mk, "path": path}, "street": {"id": item_id, "name": name},
                                 "houses": dict(houses), "house_order": [h for h, _ in houses]})
        return self.tg.edit(chat_id, message_id, *self.houses_view(self.user(chat_id), 1))

    def mk_house(self, chat_id, u, hid, message_id):
        t = T[u["lang"]]
        st = self.state(u)
        house = st.get("houses", {}).get(hid)
        co = REGIONS[u["region"]]["co"][u["lang"]]
        try:
            q = self.dir_queue(REGIONS[u["region"]].get("lookup"), st.get("mk", {}).get("path", {}), st["street"]["id"], hid)
        except Exception:
            return self.tg.send(chat_id, t["loe_busy"].format(co=co))
        if q == "NOT_IN":
            return self.tg.send(chat_id, t["voe_not_in"].format(h=html.escape(house or ""), co=co))
        if q not in (REGIONS[u["region"]].get("groups") or STANDARD_GROUPS):
            return self.tg.send(chat_id, t["loe_no_queue"].format(h=html.escape(house or ""), co=co, v="—"), self.queue_pick_view(u)[1])
        ns = (st.get("mk", {}).get("path", {}).get("ns") or {}).get("name", "")
        label = f"{ns}, {st['street']['name']}, {house}"
        if not self.add_address(chat_id, label, q, u["region"]):
            return
        if message_id:
            self.tg.edit(chat_id, message_id, t["addr_saved"].format(label=html.escape(label), q=q))
        return self.tg.send(chat_id, *self.status_view(self.user(chat_id)))

    # ---------- Zaporizhzhiaoblenergo: per-queue address sheets (Google Sheets linked from zoe.com.ua), read as CSV
    def zp_index(self):
        """{'Запоріжжя|вул. X': [(houses token, queue)], 'с-ще Сонячне|вул. Ясна': [...]} — households only. Cached a week."""
        import csv
        import io
        key = "zp:index"
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < 7 * 86400:
            return json.loads(row["value"])
        idx, sheets = {}, 0
        try:
            for page_url in ("https://www.zoe.com.ua/%d0%bf%d0%b5%d1%80%d0%b5%d0%bb%d1%96%d0%ba-%d0%b0%d0%b4%d1%80%d0%b5%d1%81-%d0%b4%d0%bb%d1%8f-%d0%ba%d0%be%d0%b6%d0%bd%d0%be%d1%97-%d0%b7-%d1%87%d0%b5%d1%80%d0%b3-%d0%b7-%d0%bf%d1%96%d0%b4%d1%87/",
                             "https://www.zoe.com.ua/%d0%bf%d0%b5%d1%80%d0%b5%d0%bb%d1%96%d0%ba-%d0%b0%d0%b4%d1%80%d0%b5%d1%81-%d0%b4%d0%bb%d1%8f-%d0%ba%d0%be%d0%b6%d0%bd%d0%be%d1%97-%d0%b7-%d1%87%d0%b5%d1%80%d0%b3-%d0%b7-%d0%bf%d1%96%d0%b4%d1%87%d0%b5/"):
                page = oe_page(page_url)
                art = page[page.find("<article"):page.find("</article>")]
                for sid, label in _re.findall(r'href="https://docs\.google\.com/spreadsheets/d/([\w-]{20,80})[^"]*"[^>]*>(.*?)</a>', art, _re.S):
                    body = oe_page(f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv")
                    rows = list(csv.reader(io.StringIO(body)))
                    sheets += 1
                    lq = _re.search(r"Черга\s*(\d)\D+підчерга\s*(\d)", html_text(label))
                    for r in rows[2:]:
                        if lq and len(r) >= 2 and r[0].strip() and r[1].strip():  # city: street | houses
                            for tok in r[1].split("|"):
                                idx.setdefault(f"м. Запоріжжя|{r[0].strip()}", []).append((tok.strip(), f"{lq[1]}.{lq[2]}"))
                        elif len(r) >= 10 and r[3].strip() and r[5].strip().lower().startswith("побут"):  # district
                            q, sub = r[8].strip() or r[6].strip(), (r[9].strip() or r[7].strip()).split()[0] if (r[9].strip() or r[7].strip()) else ""
                            sub = {"І": "1", "ІІ": "2", "I": "1", "II": "2"}.get(sub, sub)
                            if q.isdigit() and sub in ("1", "2"):
                                for tok in r[4].split("|"):
                                    if tok.strip() and not tok.strip().startswith("будинки без"):
                                        idx.setdefault(f"{r[2].strip()}|{r[3].strip()}", []).append((tok.strip(), f"{q}.{sub}"))
        except Exception:
            if row:
                return json.loads(row["value"])
            raise
        if idx and sheets >= 20:
            self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(idx, ensure_ascii=False), int(time.time())))
        elif row:
            return json.loads(row["value"])
        return idx

    @staticmethod
    def zp_match(token, house):
        """Is house ('16', '12а') covered by a list token ('16 (п.1-4)', '13-23', '2-10 парні', '33а')?"""
        tok = token.lower().replace("–", "-").replace("—", "-").strip()
        h = house.lower().replace(" ", "").replace("–", "-")
        base = _re.split(r"\s*\(|\s+п\.", tok)[0].replace(" ", "")
        if base == h:
            return True
        m = _re.fullmatch(r"(\d+)[а-яієїґ]?\s*-\s*(\d+)\s*(парні|непарні)?", tok)
        if m and h.isdigit():
            a, b, n = int(m[1]), int(m[2]), int(h)
            if a <= n <= b:
                if m[3]:
                    return n % 2 == (0 if m[3] == "парні" else 1)
                return n % 2 == a % 2 if a % 2 == b % 2 else True
        return False

    def zp_text(self, chat_id, u, m):
        t = T[u["lang"]]
        co = REGIONS[u["region"]]["co"][u["lang"]]
        st = self.state(u)
        m = m.strip()
        if st.get("zp_street") and "," not in m and any(ch.isdigit() for ch in m) and len(m) <= 12:
            return self.zp_house(chat_id, u, m)
        try:
            idx = self.zp_index()
        except Exception:
            return self.tg.send(chat_id, t["loe_busy"].format(co=co))
        town, _, street = m.rpartition(",")
        town, street = town.strip().lower(), street.strip().lower()
        for pre in ("вулиця ", "вул. ", "вул ", "провулок ", "пров. "):
            street = street[len(pre):] if street.startswith(pre) else street
        if len(street) < 2:
            return self.tg.send(chat_id, t["short"])
        hits = [k for k in idx if street in k.split("|", 1)[1].lower()
                and (not town or town.replace("м. ", "").replace("с. ", "") in k.split("|", 1)[0].lower())]
        hits.sort(key=lambda k: (not k.startswith("м. Запоріжжя|"), not k.split("|", 1)[1].lower().split(" ", 1)[-1].startswith(street), ua_key(k)))
        if not hits:
            return self.tg.send(chat_id, t["zp_none"].format(co=co, q=html.escape(m)))
        hits = hits[:30]
        self.set_state(chat_id, {"zp_hits": hits})
        exact = [i for i, k in enumerate(hits) if k.split("|", 1)[1].lower().split(" ", 1)[-1] == street]
        if not town and len([i for i in exact if hits[i].startswith("м. Запоріжжя|")]) == 1:
            exact = [i for i in exact if hits[i].startswith("м. Запоріжжя|")]  # no town given: the city
        if len(hits) == 1 or len(exact) == 1:
            return self.zp_pick(chat_id, self.user(chat_id), str(exact[0] if len(exact) == 1 else 0))
        return self.tg.send(chat_id, t["pick_street"], [[{"text": f"{k.split('|')[1]} — {k.split('|')[0]}"[:60], "callback_data": f"s:{i}"}]
                                                        for i, k in enumerate(hits)])

    def zp_pick(self, chat_id, u, idx, message_id=None):
        st = self.state(u)
        hits = st.get("zp_hits") or []
        if not idx.isdigit() or int(idx) >= len(hits):
            return
        key = hits[int(idx)]
        self.set_state(chat_id, {"zp_hits": hits, "zp_street": key})
        text = T[u["lang"]]["zp_ask_house"].format(label=html.escape(key.replace("|", ", ")))
        return self.tg.edit(chat_id, message_id, text) if message_id else self.tg.send(chat_id, text)

    def zp_house(self, chat_id, u, house):
        t = T[u["lang"]]
        co = REGIONS[u["region"]]["co"][u["lang"]]
        key = self.state(u)["zp_street"]
        town, street = key.split("|", 1)
        found = [(tok, q) for tok, q in self.zp_index().get(key, []) if self.zp_match(tok, house)]
        queues = sorted({q for _, q in found})
        if not queues:
            return self.tg.send(chat_id, t["zp_no_house"].format(h=html.escape(house), street=html.escape(street), co=co), self.queue_pick_view(u)[1])
        if len(queues) == 1:
            label = f"{town}, {street}, {house}"
            if self.add_address(chat_id, label, queues[0], u["region"]):
                return self.tg.send(chat_id, *self.status_view(self.user(chat_id)))
            return
        lines = "\n".join(f"• {html.escape(tok)} — <b>{q}</b>" for tok, q in found[:12])
        return self.tg.send(chat_id, t["zp_many"].format(h=html.escape(house), street=html.escape(street), co=co, lines=lines),
                            [[{"text": q, "callback_data": f"qq:{q}"} for q in queues]])

    # ---------- Volynoblenergo: the 'queue by address' form on energy.volyn.ua (town + street -> houses with their queue)
    def voe_rows(self, city, street):
        key = f"voe:addr:{city.lower()}:{street.lower()}"
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < 86400:
            return json.loads(row["value"])
        try:
            page = oe_page("https://energy.volyn.ua/spozhyvacham/perervy-u-elektropostachanni/cherga/",
                           urllib.parse.urlencode({"formCity": city, "formStreet": street}).encode())
        except Exception:
            if row:
                return json.loads(row["value"])
            raise
        out = []
        for table in _re.findall(r"<table.*?</table>", page, _re.S):
            for tr in _re.findall(r"<tr.*?</tr>", table, _re.S):
                cells = [html_text(c).strip() for c in _re.findall(r"<td[^>]*>(.*?)</td>", tr, _re.S)]
                if len(cells) >= 5 and cells[1] != "Нас. пункт" and not cells[4].startswith("Черга"):
                    out.append(cells[1:5])  # town, street, houses '1, 1А, 3', queue '1.2' or 'Не залучено'
        self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(out, ensure_ascii=False), int(time.time())))
        return out

    def voe_text(self, chat_id, u, m):
        st = self.state(u)
        m = m.strip()
        if st.get("street") and "," not in m and any(ch.isdigit() for ch in m):
            hit = next((k for k, v in st.get("houses", {}).items() if v.lower().replace(" ", "") == m.lower().replace(" ", "")), None)
            return self.pick_house(chat_id, u, hit) if hit else self.tg.send(chat_id, T[u["lang"]]["no_houses"].format(q=html.escape(m)))
        if "," in m:
            city, street = (x.strip() for x in m.split(",", 1))
        elif st.get("voe_city"):
            city, street = st["voe_city"], m
        else:
            self.set_state(chat_id, {"voe_city": m[:60]})
            return self.tg.send(chat_id, T[u["lang"]]["voe_ask_street"].format(city=html.escape(m[:60])))
        return self.voe_search(chat_id, u, city[:60], street[:60])

    def voe_search(self, chat_id, u, city, street):
        t = T[u["lang"]]
        co = REGIONS[u["region"]]["co"][u["lang"]]
        for pre in ("вулиця ", "вул. ", "вул ", "м. ", "с. ", "смт "):  # the form wants bare names
            street = street[len(pre):] if street.lower().startswith(pre) else street
            city = city[len(pre):] if city.lower().startswith(pre) else city
        if len(city) < 2 or len(street) < 2:
            return self.tg.send(chat_id, t["short"])
        try:
            rows = self.voe_rows(city, street)
        except Exception:
            return self.tg.send(chat_id, t["loe_busy"].format(co=co))
        combos = {}
        for town, name, houses, queue in rows:
            hs = combos.setdefault(f"{town}|{name}", {})
            for h in houses.split(","):
                h = h.strip()[:20]
                if h and any(ch.isdigit() for ch in h) and not h.startswith("-"):
                    hs.setdefault(h, []).append(queue.strip())
        if not combos:
            self.set_state(chat_id, {"voe_city": city})
            return self.tg.send(chat_id, t["voe_none"].format(co=co, city=html.escape(city), street=html.escape(street)))
        items = sorted(combos.items(), key=lambda kv: (kv[0].split("|")[1].lower() != street.lower(), ua_key(kv[0])))[:30]
        self.set_state(chat_id, {"voe_city": city, "voe_combos": [[k, v] for k, v in items]})
        exact = [i for i, (k, _) in enumerate(items) if k.split("|")[1].lower() == street.lower()]
        if len(items) == 1 or len(exact) == 1:
            return self.voe_pick(chat_id, self.user(chat_id), str(exact[0] if exact else 0))
        return self.tg.send(chat_id, t["pick_street"], [[{"text": f"{k.split('|')[1]} — {k.split('|')[0]}"[:60], "callback_data": f"s:{i}"}]
                                                        for i, (k, _) in enumerate(items)])

    def voe_pick(self, chat_id, u, idx, message_id=None):
        st = self.state(u)
        combos = st.get("voe_combos") or []
        if not idx.isdigit() or int(idx) >= len(combos):
            return
        key, houses = combos[int(idx)]
        town, name = key.split("|", 1)

        def best(vals):  # a house can sit on several lines: a schedule queue wins over 'not included'
            std = [v for v in vals if v in STANDARD_GROUPS]
            return max(set(std), key=std.count) if std else (vals[0] if vals else "")

        def natural(h):
            n = ""
            for ch in h:
                if not ch.isdigit():
                    break
                n += ch
            return (int(n) if n else 10 ** 9, h)
        order = sorted(houses, key=natural)
        self.set_state(chat_id, {"voe_city": st.get("voe_city"), "voe_combos": combos,
                                 "city": {"id": town, "name": town}, "street": {"id": idx, "name": name},
                                 "houses": {str(i): h for i, h in enumerate(order)}, "house_order": [str(i) for i in range(len(order))],
                                 "loe_q": {str(i): best(houses[h]) for i, h in enumerate(order)}})
        view = self.houses_view(self.user(chat_id), 1)
        return self.tg.edit(chat_id, message_id, *view) if message_id else self.tg.send(chat_id, *view)

    # ---------- Lvivoblenergo directory (power-api.loe.lviv.ua): town -> street -> buildings with their queue
    def loe_get(self, path, **params):
        q = urllib.parse.urlencode({"pagination": "false", **params})
        return http_json(f"https://power-api.loe.lviv.ua/api/{path}?{q}", timeout=60)

    def loe_cities(self, chat_id, u, q):
        t = T[u["lang"]]
        if len(q) < 2:
            return self.tg.send(chat_id, t["no_cities"])
        try:
            res = [c for c in self.loe_get("pw_cities", name=q[:60]) if isinstance(c, dict) and c.get("id") and c.get("name")]
        except Exception:
            return self.tg.send(chat_id, t["loe_busy"].format(co=REGIONS["lviv"]["co"][u["lang"]]))
        if not res:
            return self.tg.send(chat_id, t["no_cities"])
        label = lambda c: c["name"] + (f" ({c['otg']['name']} громада)" if isinstance(c.get("otg"), dict) and c["otg"].get("name") else "")  # noqa: E731
        self.set_state(chat_id, {"cities": {str(c["id"]): label(c) for c in res[:30]}})
        exact = [c for c in res if c["name"].lower() == q.lower()]
        if len(res) == 1 or len(exact) == 1:
            return self.pick_city(chat_id, self.user(chat_id), str((exact or res)[0]["id"]))
        self.tg.send(chat_id, t["pick_city"], [[{"text": label(c)[:60], "callback_data": f"c:{c['id']}"}] for c in res[:30]])

    def loe_streets(self, chat_id, u, q):
        t = T[u["lang"]]
        st = self.state(u)
        if len(q) < 2:
            return self.tg.send(chat_id, t["short"])
        try:
            res = [x for x in self.loe_get("pw_streets", **{"city.id": st["city"]["id"], "name": q[:60]})
                   if isinstance(x, dict) and x.get("id") and x.get("name")]
        except Exception:
            return self.tg.send(chat_id, t["loe_busy"].format(co=REGIONS["lviv"]["co"][u["lang"]]))
        if not res:
            return self.tg.send(chat_id, t["no_streets"])
        res.sort(key=lambda x: (not x["name"].lower().startswith(q.lower()), ua_key(x["name"])))
        self.set_state(chat_id, {"city": st["city"], "streets": {str(x["id"]): x["name"] for x in res[:30]}})
        exact = [x for x in res if x["name"].lower() == q.lower()]
        if len(res) == 1 or len(exact) == 1:
            return self.loe_pick_street(chat_id, self.user(chat_id), str((exact or res)[0]["id"]))
        self.tg.send(chat_id, t["pick_street"], [[{"text": x["name"][:60], "callback_data": f"s:{x['id']}"}] for x in res[:30]])

    def loe_buildings(self, city_id, street_id):
        """{building: queue or other value} — the most common chergGpv of the building's consumers. Cached a day."""
        key = f"loe:bld:{city_id}:{street_id}"
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < 86400:
            return json.loads(row["value"])
        try:
            rows = self.loe_get("pw_accounts", **{"city.id": city_id, "street.id": street_id})
        except Exception:
            if row:
                return json.loads(row["value"])
            raise
        counts = {}
        for x in rows if isinstance(rows, list) else []:
            name = str(x.get("buildingName") or "").strip()[:30] if isinstance(x, dict) else ""
            if not name:
                continue
            v = str(x.get("chergGpv") or "").strip()[:40]
            counts.setdefault(name, {})
            if v:
                counts[name][v] = counts[name].get(v, 0) + 1
        out = {b: (max(c, key=lambda k: (k in STANDARD_GROUPS, c[k])) if c else "") for b, c in counts.items()}
        self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(out, ensure_ascii=False), int(time.time())))
        return out

    def loe_pick_street(self, chat_id, u, sid, message_id=None):
        t = T[u["lang"]]
        st = self.state(u)
        name = st.get("streets", {}).get(sid)
        if not name or not st.get("city"):
            return
        wait = t["loading_houses"].format(street=html.escape(name))
        if message_id:
            self.tg.edit(chat_id, message_id, wait)
        else:
            res = self.tg.send(chat_id, wait)
            message_id = (res or {}).get("result", {}).get("message_id") if isinstance(res, dict) else None
        try:
            blds = self.loe_buildings(st["city"]["id"], sid)
        except Exception:
            text = t["loe_busy"].format(co=REGIONS["lviv"]["co"][u["lang"]])
            return self.tg.edit(chat_id, message_id, text) if message_id else self.tg.send(chat_id, text)

        def natural(b):
            n = ""
            for ch in b:
                if not ch.isdigit():
                    break
                n += ch
            return (int(n) if n else 10 ** 9, b)
        order = sorted(blds, key=natural)
        st = {"city": st["city"], "street": {"id": sid, "name": name},
              "houses": {str(i): b for i, b in enumerate(order)}, "house_order": [str(i) for i in range(len(order))],
              "loe_q": {str(i): blds[b] for i, b in enumerate(order)}}
        self.set_state(chat_id, st)
        view = self.houses_view(self.user(chat_id), 1)
        return self.tg.edit(chat_id, message_id, *view) if message_id else self.tg.send(chat_id, *view)

    def loe_house_text(self, chat_id, u, q):
        st = self.state(u)
        hit = next((k for k, v in st.get("houses", {}).items() if v.lower() == q.strip().lower()), None)
        if hit:
            return self.pick_house(chat_id, u, hit)
        if not any(ch.isdigit() for ch in q):  # a new street name
            self.set_state(chat_id, {"city": st["city"]})
            return self.loe_streets(chat_id, self.user(chat_id), q)
        self.tg.send(chat_id, T[u["lang"]]["no_houses"].format(q=html.escape(q)))

    def search_cities(self, chat_id, u, q):
        t = T[u["lang"]]
        if len(q) < 3:
            return self.tg.send(chat_id, t["no_cities"])
        try:
            res = self.yasno("cities", u["region"], query=q[:60])
        except Exception:
            return self.tg.send(chat_id, t["yasno_err"])
        if not res:
            return self.tg.send(chat_id, t["no_cities"])
        self.set_state(chat_id, {"cities": {str(c["id"]): c["value"] for c in res[:30]}})
        exact = [c for c in res if c["value"].split(" ", 1)[-1].lower() == q.lower()]
        if len(res) == 1 or len(exact) == 1:
            return self.pick_city(chat_id, self.user(chat_id), str((exact or res)[0]["id"]))
        self.tg.send(chat_id, t["pick_city"], [[{"text": c["value"][:60], "callback_data": f"c:{c['id']}"}] for c in res[:30]])

    def pick_city(self, chat_id, u, cid, message_id=None):
        name = self.state(u).get("cities", {}).get(cid)
        if not name:
            return
        self.set_state(chat_id, {"city": {"id": cid, "name": name}})
        text = T[u["lang"]]["ask_street_in"].format(city=html.escape(name))
        if message_id:
            self.tg.edit(chat_id, message_id, text)
        else:
            self.tg.send(chat_id, text)

    def search_streets(self, chat_id, u, q):
        t = T[u["lang"]]
        st0 = self.state(u)
        town = {"cityId": st0["city"]["id"]} if st0.get("city") else {}
        if STREET_BY_ID and u["region"] == "kyiv" and len(q) >= 2:
            # local list: instant, works with short input, no load on YASNO
            ql = q.lower().replace("’", "'")
            res = [{"id": sid, "value": v} for sid, v in STREET_BY_ID.items() if ql in v.lower().replace("’", "'")]
            res.sort(key=lambda s: (not street_main(s["value"]).lower().startswith(ql), ua_key(street_main(s["value"]))))
        elif len(q) < 3:
            return self.tg.send(chat_id, t["short"], self.list_button(u))
        else:
            try:
                res = self.yasno("streets", u["region"], query=q[:60], **town)
            except Exception:
                return self.tg.send(chat_id, t["yasno_err"], self.list_button(u))
        if not res:
            return self.tg.send(chat_id, t["no_streets"], self.list_button(u))
        st = {"streets": {str(s["id"]): s["value"] for s in res[:30]}, **({"city": st0["city"]} if st0.get("city") else {})}
        self.set_state(chat_id, st)
        if len(res) == 1:
            return self.pick_street(chat_id, self.user(chat_id), str(res[0]["id"]))
        self.tg.send(chat_id, t["pick_street"], [[{"text": s["value"][:60], "callback_data": f"s:{s['id']}"}] for s in res[:30]])

    def pick_street(self, chat_id, u, sid, message_id=None):
        if REGIONS[u["region"]].get("lookup") == "loe":
            return self.loe_pick_street(chat_id, u, sid, message_id)
        if REGIONS[u["region"]].get("lookup") == "voe":
            return self.voe_pick(chat_id, u, sid, message_id)
        if REGIONS[u["region"]].get("lookup") == "zpa":
            return self.zp_pick(chat_id, u, sid, message_id)
        t = T[u["lang"]]
        st = self.state(u)
        name = st.get("streets", {}).get(sid)
        if not name:
            return
        st["street"] = {"id": sid, "name": name}
        self.set_state(chat_id, st)
        wait = t["loading_houses"].format(street=html.escape(name))
        if message_id:
            self.tg.edit(chat_id, message_id, wait)
        else:
            res = self.tg.send(chat_id, wait)
            message_id = (res or {}).get("result", {}).get("message_id") if isinstance(res, dict) else None
        try:
            houses = self.all_houses(u["region"], sid, st.get("city", {}).get("id"))
        except Exception as e:
            log("house list failed:", repr(e))
            houses = []
        if not houses:  # fall back to typing the number
            text = t["ask_house"].format(street=html.escape(name))
            return self.tg.edit(chat_id, message_id, text) if message_id else self.tg.send(chat_id, text)
        st["houses"] = {str(h["id"]): h["value"] for h in houses}
        st["house_order"] = [str(h["id"]) for h in houses]
        self.set_state(chat_id, st)
        view = self.houses_view(self.user(chat_id), 1)
        return self.tg.edit(chat_id, message_id, *view) if message_id else self.tg.send(chat_id, *view)

    def houses_view(self, u, page):
        t = T[u["lang"]]
        st = self.state(u)
        order = st.get("house_order", [])
        pages = max(1, (len(order) + HPAGE - 1) // HPAGE)
        page = min(max(1, page), pages)
        chunk = order[(page - 1) * HPAGE: page * HPAGE]
        rows = [[{"text": st["houses"][h][:14], "callback_data": f"h:{h}"} for h in chunk[i:i + 5]] for i in range(0, len(chunk), 5)]
        if pages > 1:
            nav = []
            if page > 1:
                nav.append({"text": "◀️", "callback_data": f"hp:{page - 1}"})
            nav.append({"text": f"{page}/{pages}", "callback_data": "x"})
            if page < pages:
                nav.append({"text": "▶️", "callback_data": f"hp:{page + 1}"})
            rows.append(nav)
        rows.append([{"text": t["b_other_street"], "callback_data": "aa"}])
        name = html.escape(st.get("street", {}).get("name", ""))
        return t["houses_page"].format(street=name, n=t["houses_n"](len(order))), rows

    def all_houses(self, region, street_id, city_id=None):
        """Every house of a street. YASNO returns at most 20 matches per query, so queries
        are narrowed ('1' -> '10', '11', …) until each returns fewer. Cached for a week."""
        key = f"allhouses:{region}:{city_id or ''}:{street_id}"
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < CACHE_TTL["houses"]:
            return json.loads(row["value"])
        rid, dso = REGIONS[region]["yasno"]
        base = {"regionId": rid, "dsoId": dso, "streetId": street_id, **({"cityId": city_id} if city_id else {})}
        found, calls = {}, [0]

        def q(prefix):
            calls[0] += 1
            if calls[0] > (30 if NET_CAP[0] else 400):  # the self-test only needs to see houses arrive
                return []
            return http_json(f"{YASNO}/houses?" + urllib.parse.urlencode({**base, "query": prefix}), timeout=15)

        def crawl(prefix, depth=0):
            res = q(prefix)
            for h in res:
                found[str(h["id"])] = h["value"]
            if len(res) >= 20 and depth < 4:
                for ch in "0123456789/-абвгдежзиклмнопрс":
                    crawl(prefix + ch, depth + 1)

        try:
            for d in "123456789":
                crawl(d)
        except Exception:
            if row:
                return json.loads(row["value"])
            raise

        def natural(h):  # 2, 2А, 10, 10/1, 100 …
            v = h["value"]
            n = ""
            for ch in v:  # leading digits only: '1/2' -> 1, '10Б' -> 10
                if not ch.isdigit():
                    break
                n += ch
            return (int(n) if n else 10 ** 9, v)
        houses = sorted(({"id": k, "value": v} for k, v in found.items()), key=natural)
        self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)",
                  (key, json.dumps(houses, ensure_ascii=False), int(time.time())))
        log(f"house list: street {street_id}: {len(houses)} houses, {calls[0]} requests")
        return houses

    def search_houses(self, chat_id, u, q):
        t = T[u["lang"]]
        st = self.state(u)
        try:
            town = {"cityId": st["city"]["id"]} if st.get("city") else {}
            res = self.yasno("houses", u["region"], streetId=st["street"]["id"], query=q[:20], **town)
        except Exception:
            return self.tg.send(chat_id, t["yasno_err"])
        if not res:
            if not any(ch.isdigit() for ch in q):  # probably a new street name
                self.set_state(chat_id, {"city": st["city"]} if st.get("city") else {})
                return self.search_streets(chat_id, self.user(chat_id), q)
            return self.tg.send(chat_id, t["no_houses"].format(q=html.escape(q)))

        def natural(h):  # 2, 2А, 10, 10/1, 100 …
            v = h["value"]
            n = ""
            for ch in v:  # leading digits only: '1/2' -> 1, '10Б' -> 10
                if not ch.isdigit():
                    break
                n += ch
            return (int(n) if n else 10 ** 9, v)
        res = sorted(res, key=natural)
        st["houses"] = {**st.get("houses", {}), **{str(h["id"]): h["value"] for h in res[:32]}}
        self.set_state(chat_id, st)
        exact = [h for h in res if h["value"].lower() == q.lower()]
        if len(res) == 1 or len(exact) == 1:
            return self.pick_house(chat_id, self.user(chat_id), str((exact or res)[0]["id"]))
        rows, row = [], []
        for h in res[:32]:
            row.append({"text": h["value"][:20], "callback_data": f"h:{h['id']}"})
            if len(row) == 4:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        self.tg.send(chat_id, t["pick_house"], rows)

    def pick_house(self, chat_id, u, hid, message_id=None):
        t = T[u["lang"]]
        st = self.state(u)
        house = st.get("houses", {}).get(hid)
        if not house or not st.get("street"):
            return
        if REGIONS[u["region"]].get("lookup") in ("mk", "pl", "zt", "cvs"):
            return self.mk_house(chat_id, u, hid, message_id)
        if REGIONS[u["region"]].get("lookup") in ("loe", "voe"):
            value = st.get("loe_q", {}).get(hid, "")
            co = REGIONS[u["region"]]["co"][u["lang"]]
            if value.lower().startswith("не залуч"):
                return self.tg.send(chat_id, t["voe_not_in"].format(h=html.escape(house), co=co))
            if value not in STANDARD_GROUPS:
                return self.tg.send(chat_id, t["loe_no_queue"].format(h=html.escape(house), co=co, v=html.escape(value or "—")),
                                    self.queue_pick_view(u)[1])
            town = st["city"]["name"].split(" (")[0]
            label = f"{town}, {st['street']['name']}, {house}"
            if not self.add_address(chat_id, label, value, u["region"]):
                return
            if message_id:
                self.tg.edit(chat_id, message_id, t["addr_saved"].format(label=html.escape(label), q=value))
            return self.tg.send(chat_id, *self.status_view(self.user(chat_id)))
        try:
            town = {"cityId": st["city"]["id"]} if st.get("city") else {}
            g = self.yasno("group", u["region"], streetId=st["street"]["id"], houseId=hid, **town)
        except Exception:
            return self.tg.send(chat_id, t["yasno_err"])
        if g.get("group") is None:
            return self.tg.send(chat_id, t["no_group"])
        queue = f"{g['group']}.{g.get('subgroup') or 1}"
        label = (f"{st['city']['name']}, " if st.get("city") else "") + f"{st['street']['name']}, {house}"
        if not self.add_address(chat_id, label, queue, u["region"]):
            return
        u = self.user(chat_id)
        if message_id:
            self.tg.edit(chat_id, message_id, t["addr_saved"].format(label=html.escape(label), q=queue))
        self.tg.send(chat_id, *self.status_view(u))

    def callback(self, cq):
        chat_id = cq["message"]["chat"]["id"]
        mid = cq["message"]["message_id"]
        u = self.user(chat_id, cq["from"].get("language_code"))
        t = T[u["lang"]]
        kind, _, val = cq.get("data", "").partition(":")
        toast = self.report(chat_id, u, val) if kind == "rp" and val in ("off", "on") else None
        try:
            self.tg.call("answerCallbackQuery", timeout=10, callback_query_id=cq["id"], **({"text": toast} if toast else {}))
        except Exception:
            pass
        if kind == "rp":
            return self.tg.edit(chat_id, mid, *self.status_view(self.user(chat_id)))
        if kind == "s":
            return self.pick_street(chat_id, u, val, mid)
        if kind == "c":
            return self.pick_city(chat_id, u, val, mid)
        if kind == "rg":
            if not val:
                return self.tg.edit(chat_id, mid, *self.region_view(u))
            if val in REGIONS:
                self.set_user(chat_id, region=val, state=None)
                u = self.user(chat_id)
                self.tg.edit(chat_id, mid, T[u["lang"]]["region_set"].format(r=region_name(val, u["lang"])))
                return self.ask_address(chat_id, u)
            return
        if kind == "h":
            return self.pick_house(chat_id, u, val, mid)
        if kind == "hp":
            return self.tg.edit(chat_id, mid, *self.houses_view(u, int(val) if val.isdigit() else 1))
        if kind == "r":
            return self.tg.edit(chat_id, mid, *self.status_view(u))
        if kind == "nw":
            return self.tg.edit(chat_id, mid, *self.now_view(u))
        if kind == "img":
            a = self.active_address(u)
            if a:
                return self.tg.photo(chat_id, queue_png(a["region"], a["queue"]),
                                     t["img_caption"].format(q=a["queue"], label=html.escape(a["label"])))
            return
        if kind == "a":
            return self.tg.edit(chat_id, mid, *self.addr_view(u))
        if kind == "as":
            self.set_user(chat_id, active=int(val))
            return self.tg.edit(chat_id, mid, *self.status_view(self.user(chat_id)))
        if kind == "ad":
            self.db.q("DELETE FROM addresses WHERE id=? AND chat_id=?", (int(val), chat_id))
            if u["active"] == int(val):
                self.set_user(chat_id, active=None)
            return self.tg.edit(chat_id, mid, *self.addr_view(self.user(chat_id)))
        if kind == "aa":
            return self.ask_address(chat_id, u)
        if kind == "LD":  # back to districts
            return self.tg.edit(chat_id, mid, *self.district_view(u))
        if kind == "LL":  # letters (of a district, or all): edit in place
            return self.tg.edit(chat_id, mid, *self.letters_view(u, val or "-"))
        if kind == "L":
            if not val:  # from a text message (welcome, "not found"): keep that text, open the list below
                return self.tg.send(chat_id, *self.list_start(u))
            parts = val.split(":")
            if len(parts) == 2:  # buttons from before districts: L:<letter>:<page>
                parts = ["-"] + parts
            d, letter, page = parts[0], parts[1], parts[2]
            return self.tg.edit(chat_id, mid, *self.letter_page_view(u, letter, int(page) if page.isdigit() else 1, d))
        if kind == "ls":
            name = STREET_BY_ID.get(val)
            if not name:
                return
            self.set_state(chat_id, {"streets": {val: name}})
            return self.pick_street(chat_id, self.user(chat_id), val, mid)
        if kind == "x":
            return
        if kind == "MK":
            return self.tg.edit(chat_id, mid, *self.mk_filials_view(u))
        if kind == "MKF":
            lk = REGIONS[u["region"]].get("lookup")
            fil = next((n for i, n in self.dir_filials(lk) if i == val), "")
            items = self.dir_towns(lk, val)
            self.mk_set_level(chat_id, "ns", items, {"fil": {"id": val, "name": fil}})
            if len(items) == 1:  # e.g. Mykolaiv city: straight to its streets
                return self.mk_pick(chat_id, self.user(chat_id), items[0][0], mid)
            return self.tg.edit(chat_id, mid, *self.mk_level_view(self.user(chat_id)))
        if kind == "MKL" and self.state(u).get("mk"):
            letter, _, page = val.partition(":")
            return self.tg.edit(chat_id, mid, *self.mk_level_view(u, None if letter == "-" else letter, int(page) if page.isdigit() else 1))
        if kind == "MKI" and self.state(u).get("mk"):
            return self.mk_pick(chat_id, u, val, mid)
        if kind == "MKB":
            mk = self.state(u).get("mk") or {}
            if mk.get("level") == "street" and mk.get("path", {}).get("fil"):
                fid = mk["path"]["fil"]["id"]
                items = self.dir_towns(REGIONS[u["region"]].get("lookup"), fid)
                if len(items) > 1:
                    self.mk_set_level(chat_id, "ns", items, {"fil": mk["path"]["fil"]})
                    return self.tg.edit(chat_id, mid, *self.mk_level_view(self.user(chat_id)))
            return self.tg.edit(chat_id, mid, *self.mk_filials_view(u))
        if kind == "qq":
            q = val if _re.fullmatch(r"\d{1,2}(\.\d)?", val) else None
            if q and self.add_address(chat_id, t["queue_label"].format(q=q), q, u["region"]):
                return self.tg.edit(chat_id, mid, *self.status_view(self.user(chat_id)))
            return
        if kind == "DT":
            return self.tg.edit(chat_id, mid, *self.towns_view(u)) if DNIPRO else None
        if kind == "DV":
            return self.tg.edit(chat_id, mid, *self.village_letters_view(u)) if DNIPRO else None
        if kind == "DVL":
            letter, _, page = val.partition(":")
            return self.tg.edit(chat_id, mid, *self.village_page_view(u, letter, int(page) if page.isdigit() else 1))
        if kind == "DX" and val in DNIPRO:
            return self.tg.edit(chat_id, mid, *self.town_view(u, val))
        if kind == "DL":
            tid, letter, page = (val.split(":") + ["1", "1"])[:3]
            if tid in DNIPRO:
                return self.tg.edit(chat_id, mid, *self.town_page_view(u, tid, letter, int(page) if page.isdigit() else 1))
            return
        if kind == "DS":
            tid, _, sid = val.partition(":")
            return self.pick_dnipro_street(chat_id, u, tid, sid, mid)
        if kind == "dtek":
            return self.tg.send(chat_id, *self.dtek_view(u))
        if kind == "st":
            return self.tg.edit(chat_id, mid, *self.settings_view(u))
        if kind in ("sn", "sr", "sq", "sw", "sl"):
            if kind == "sn":
                self.set_user(chat_id, notify=0 if u["notify"] else 1)
            elif kind == "sr":
                self.set_user(chat_id, ret=0 if u["ret"] else 1)
            elif kind == "sq":
                self.set_user(chat_id, quiet=0 if u["quiet"] else 1)
            elif kind == "sw" and val.isdigit() and int(val) in WARN_CHOICES:
                self.set_user(chat_id, warn_min=int(val))
            elif kind == "sl":
                self.set_user(chat_id, lang="en" if u["lang"] == "uk" else "uk")
            return self.tg.edit(chat_id, mid, *self.settings_view(self.user(chat_id)))

    # ---------- notifications
    def notify_tick(self, now=None):
        now = now or time.time()
        self.check_stale(schedule("kyiv"), now)
        regions = [r["region"] for r in self.db.q("SELECT DISTINCT region FROM addresses")] or ["kyiv"]
        for region in regions:
            if region in REGIONS:
                self.notify_region(region, now)
        if True:
            data = schedule("kyiv")
            days = days_from_today(data, now) if data else []
            if days and days[0][0] <= now:
                now_min = (now - days[0][0]) / 60
                if not (now_min >= QUIET_FROM or now_min < QUIET_TO):  # flush what waited for the end of quiet hours
                    for p in self.db.q("SELECT * FROM pending ORDER BY at"):
                        self.send_bulk(p["chat_id"], p["text"], 100)
                        self.db.q("DELETE FROM pending WHERE chat_id=? AND key=?", (p["chat_id"], p["key"]))
        if CHANNEL and not isinstance(self.tg, FakeTG):  # the self-test calls channel_tick itself
            self.safe(self.channel_tick, now)
            self.safe(self.pubs_post, now)
        if not isinstance(self.tg, FakeTG):
            self.safe(self.week_tick, now)
        if not isinstance(self.tg, FakeTG) and now - getattr(self, "health_at", 0) >= HEALTH_EVERY:
            self.health_at = now
            self.safe(self.health_tick, now)
        self.db.q("DELETE FROM sent WHERE at < ?", (int(now) - 3 * 86400,))
        self.db.q("DELETE FROM reports WHERE at < ?", (int(now) - 86400,))

    def notify_region(self, region, now):
        data = schedule(region)
        if not data:
            return
        days = days_from_today(data, now)
        if not days or days[0][0] > now:
            return
        today_ts, groups_today = days[0]
        now_min = (now - today_ts) / 60
        quiet_now = now_min >= QUIET_FROM or now_min < QUIET_TO
        tomorrow = next((d for d in days if d[0] > now), None)
        queues = [r["queue"] for r in self.db.q("SELECT DISTINCT queue FROM addresses WHERE region=?", (region,))]

        # one pass per queue (≤ 60), not per user
        for qn in queues:
            key = queue_key(qn)
            if key not in groups_today:
                continue
            seq = "".join(slots(g.get(key)) for _, g in days)
            today_sig = seq[:48]
            tom_sig = slots(tomorrow[1].get(key)) if tomorrow else None
            events = []

            skey = qn if region == "kyiv" else f"{region}:{qn}"
            row = self.db.one("SELECT * FROM queue_state WHERE queue=?", (skey,))
            if row:
                if row["day_ts"] == today_ts and row["today_sig"] != today_sig:
                    events.append(("changed_today", f"chg:{today_ts}:{today_sig}"))
                if tomorrow and row["tom_ts"] != tomorrow[0]:
                    events.append(("tomorrow_published", f"tom:{tomorrow[0]}"))
                elif tomorrow and row["tom_sig"] != tom_sig:
                    events.append(("tomorrow_changed", f"tomchg:{tomorrow[0]}:{tom_sig}"))
            self.db.q("INSERT OR REPLACE INTO queue_state(queue,day_ts,today_sig,tom_ts,tom_sig) VALUES(?,?,?,?,?)",
                      (skey, today_ts, today_sig, tomorrow[0] if tomorrow else None, tom_sig))

            idx = int(now_min // 30)
            nxt_off = next((j for j in range(idx + 1, len(seq)) if seq[j] == "-" and seq[j - 1] != "-"), None)
            mins_to_off = nxt_off * 30 - now_min if nxt_off is not None else None
            off_len = 0
            while nxt_off is not None and nxt_off + off_len < len(seq) and seq[nxt_off + off_len] == "-":
                off_len += 1
            self.record_history(region, qn, days[:2])
            back = next((j for j in range(max(1, idx - 1), idx + 1)
                         if seq[j] == "+" and seq[j - 1] == "-" and 0 <= now_min - j * 30 < RETURN_WINDOW), None)

            subs = self.db.q("""SELECT a.id AS aid, a.label, u.* FROM addresses a JOIN users u ON u.chat_id=a.chat_id
                                WHERE a.queue=? AND a.region=? AND u.notify=1 AND u.blocked=0""", (qn, region))
            c_off, c_on = self.crowd(region, qn, now)
            crowd_alarm = seq[idx] == "+" and c_off >= CROWD_MIN and c_off > c_on
            reporters = {r["chat_id"] for r in self.db.q(
                "SELECT chat_id FROM reports WHERE region=? AND queue=? AND at>=?", (region, qn, int(now - REPORT_TTL)))}
            for s in subs:
                t = T[s["lang"]]
                cid = s["chat_id"]
                user_quiet = s["quiet"] and quiet_now
                tag = "\n" + t["for_addr"].format(label=html.escape(s["label"]), q=qn)
                if (mins_to_off is not None and 0 < mins_to_off <= 60 and s["warn_min"] < 60 and off_len >= 8
                        and self.mark(cid, f"charge:{s['aid']}:{today_ts}:{nxt_off}") and not user_quiet):
                    hl = f"{off_len // 2} год" + (" 30 хв" if off_len % 2 else "") if s["lang"] == "uk" else f"{off_len / 2:g} h"
                    self.tg.send(cid, t["charge"].format(t=fmt(nxt_off * 30), h=hl, end=fmt((nxt_off + off_len) * 30)) + tag)
                if mins_to_off is not None and 0 < mins_to_off <= s["warn_min"]:
                    if self.mark(cid, f"warn:{s['aid']}:{today_ts}:{nxt_off}") and not user_quiet:
                        self.tg.send(cid, t["warn"].format(m=int(round(mins_to_off)), t=fmt(nxt_off * 30)) + tag)
                if back is not None and s["ret"]:
                    if self.mark(cid, f"ret:{s['aid']}:{today_ts}:{back}") and not user_quiet:
                        self.tg.send(cid, t["ret"].format(t=fmt(back * 30)) + tag)
                if crowd_alarm and cid not in reporters and cid > 0:  # at most once an hour per address
                    if self.mark(cid, f"crowd:{s['aid']}:{today_ts}:{int(now_min // 60)}") and not user_quiet:
                        t2 = T[s["lang"]]
                        self.tg.send(cid, t2["crowd_alert"].format(q=qn, n=t2["people"](c_off)) + tag,
                                     [[{"text": t2["b_rep_off"], "callback_data": "rp:off"},
                                       {"text": t2["b_rep_on"], "callback_data": "rp:on"}],
                                      [{"text": t2["b_dtek"], "callback_data": "dtek"}]])
                note = active_notice(data, now)
                if note and self.mark(cid, f"notice:{region}:{note['at']}:{s['aid']}"):
                    nt = status_text(qn, s["lang"], s["label"], now, region)
                    if user_quiet:
                        self.db.q("INSERT OR REPLACE INTO pending(chat_id,key,text,at) VALUES(?,?,?,?)", (cid, f"ntc:{s['aid']}", nt, int(now)))
                    else:
                        self.send_bulk(cid, nt, 0)  # emergency notices go first
                for ev, mark in events:
                    if not self.mark(cid, f"{mark}:{s['aid']}"):
                        continue
                    text = t[ev] + "\n\n" + status_text(qn, s["lang"], s["label"], now, region)
                    if user_quiet:  # deliver at 07:00; keep only the latest version per address
                        self.db.q("INSERT OR REPLACE INTO pending(chat_id,key,text,at) VALUES(?,?,?,?)",
                                  (cid, f"{ev[:3]}:{s['aid']}", text, int(now)))
                    else:
                        self.send_bulk(cid, text, int(mins_to_off) if mins_to_off is not None else 10 ** 5)


    # ---------- channel: an intro once, then 07:00 'today' and 22:00 'tomorrow' digests for all regions
    def channel_digest(self, day_mid, title, now):
        lines = []
        for region in REGIONS:
            data = schedule(region)
            groups = (data or {}).get("fact", {}).get("data", {}).get(str(day_mid)) if data else None
            name = region_name(region, "uk")
            if not groups:
                lines.append(f"<b>{name}</b> — графік ще не опубліковано")
                continue
            offs = []
            for hours in groups.values():
                st = slots(hours)
                offs.append(st.count("-") * 30)
            hi, lo = max(offs), min(offs)
            if hi == 0:
                lines.append(f"<b>{name}</b> — відключень не заплановано")
                continue
            h = lambda m: f"{m / 60:g}".replace(".", ",")  # noqa: E731
            span = f"до {h(hi)} год" if lo == 0 or lo == hi else f"від {h(lo)} до {h(hi)} год"
            lines.append(f"<b>{name}</b> — {span} без світла")
        y, m, d = time.gmtime(day_mid + 12 * 3600)[:3]
        month = next(k for k, v in UA_MONTHS.items() if v == m)
        text = (f"{title}, <b>{d} {month}</b>\n\n" + "\n".join(lines)
                + "\n\n<i>За даними обленерго та ДТЕК. Розклад своєї черги, сповіщення перед відключенням "
                  "і пошук черги за адресою — у боті:</i>")
        return text, [[{"text": "💡 Моя черга — у боті", "url": f"https://t.me/{BOT_USERNAME}"}]]

    def channel_region_spans(self, day_mid):
        """{region: 'до 8 год без світла' / 'не заплановано'} for the regions that published day_mid."""
        out = {}
        for region in REGIONS:
            data = schedule(region)
            groups = (data or {}).get("fact", {}).get("data", {}).get(str(day_mid)) if data else None
            if not groups:
                continue
            offs = [slots(hours).count("-") * 30 for hours in groups.values()]
            hi, lo = max(offs), min(offs)
            h = lambda m: f"{m / 60:g}".replace(".", ",")  # noqa: E731
            out[region] = ("відключень не заплановано" if hi == 0 else
                           f"до {h(hi)} год без світла" if lo == 0 or lo == hi else f"{h(lo)}–{h(hi)} год без світла")
        return out

    def channel_tomorrow_text(self, day_mid, title="🌙 <b>Графіки на завтра"):
        """A live post: published regions, then who we still wait for. None until one region publishes."""
        spans = self.channel_region_spans(day_mid)
        if not spans:
            return None
        short = lambda r: region_name(r, "uk").replace(" область", "")  # noqa: E731
        y, m, d = time.gmtime(day_mid + 12 * 3600)[:3]
        month = next(k for k, v in UA_MONTHS.items() if v == m)
        lines = [f"<b>{short(r)}</b> — {spans[r]}" for r in REGIONS if r in spans]
        waiting = [short(r) for r in REGIONS if r not in spans]
        text = f"{title}, {d} {month}</b>\n<i>доповнюється, щойно обленерго публікують</i>\n\n" + "\n".join(lines)
        if waiting:
            more = f" та ще {len(waiting) - 4}" if len(waiting) > 4 else ""
            text += "\n\n⏳ Ще чекаємо: " + ", ".join(waiting[:4]) + more
        return text

    def channel_post(self, text, buttons=None):
        """The sent message (truthy) when posted. On failure tells the admin, at most once an hour."""
        if isinstance(self.tg, FakeTG):
            self.tg.send(CHANNEL, text, buttons)
            return {"result": {"message_id": len(self.tg.out)}}
        res = self.tg.send(CHANNEL, text, buttons)
        if res is not None:
            return res
        if self.mark(0, f"chan:fail:{int(time.time() // 3600)}"):
            self.admin(f"Не вдалося опублікувати в {CHANNEL}. Перевірте, що бот — адміністратор каналу з правом публікувати.")
        return False

    def channel_tick(self, now=None):
        now = now or time.time()
        today = kyiv_midnight(*kyiv_today())
        mins = (now - today) / 60
        if not self.db.one("SELECT 1 FROM cache WHERE key=?", ("chan:intro",)) and self.channel_post("💡 <b>Світло — графіки відключень по Україні</b>\n\n"
                              "Щодня о 07:00 — графіки на сьогодні, а графік кожної області на завтра — окремим постом, щойно обленерго його публікує, для всіх областей, "
                              "де обленерго публікують їх у відкритому доступі.\n\n"
                              "У боті можна вибрати свою чергу або знайти її за адресою, отримувати сповіщення "
                              "перед відключенням і повідомити, якщо світла немає поза графіком.",
                              [[{"text": "💡 Відкрити бота", "url": f"https://t.me/{BOT_USERNAME}"}]]):
            self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", ("chan:intro", CHANNEL, int(now)))
        for region in REGIONS:
            note = active_notice(_scheds[region]["data"], now)
            if note and self.mark(0, f"chan:notice:{region}:{note['at']}"):
                what = ("діють графіки аварійних відключень (ГАВ)" if note["kind"] == "emergency"
                        else "графіки погодинних відключень скасовано")
                self.channel_post(f"⚠️ <b>{region_name(region, 'uk')}: {what}.</b>\nРозклад погодинних відключень зараз може не виконуватися.\n\n"
                                  f"<i>{html.escape(REGIONS[region]['co']['uk'])}:</i> «{html.escape(note['text'][:250])}»\n<a href=\"{note['url']}\">Джерело</a>",
                                  [[{"text": "💡 Моя черга — у боті", "url": f"https://t.me/{BOT_USERNAME}"}]])
        self.channel_feed(today, now, is_today=True)
        if mins >= 12 * 60:  # tomorrow's schedules from noon on
            self.channel_feed(kyiv_midnight(*kyiv_today(1)), now, is_today=False)

    # ---------- the channel feed: a summary first, then a post per region as it publishes, and a post per change
    def feed_slots(self, day_mid):
        """{region: {group: 48-char slots}} for the regions that published day_mid."""
        out = {}
        for region in REGIONS:
            data = schedule(region)
            groups = (data or {}).get("fact", {}).get("data", {}).get(str(day_mid)) if data else None
            if groups:
                out[region] = {k[3:]: slots(v) for k, v in groups.items()}
        return out

    @staticmethod
    def feed_span(groups):
        offs = [sl.count("-") * 30 for sl in groups.values()]
        hi, lo = max(offs), min(offs)
        h = lambda m: f"{m / 60:g}".replace(".", ",")  # noqa: E731
        return ("відключень не заплановано" if hi == 0 else
                f"до {h(hi)} год без світла" if lo == 0 or lo == hi else f"{h(lo)}–{h(hi)} год без світла")

    @staticmethod
    def feed_runs(sl):
        """'----++…' -> '00:00–02:00, 10:00–12:00' ('' when there are no outages)."""
        out, i = [], 0
        while i < len(sl):
            if sl[i] == "-":
                j = i
                while j < len(sl) and sl[j] == "-":
                    j += 1
                out.append(f"{fmt(i * 30)}–{'24:00' if j == 48 else fmt(j * 30)}")
                i = j
            else:
                i += 1
        return ", ".join(out)

    @staticmethod
    def feed_order(groups):
        return sorted(groups, key=lambda q: [int(x) for x in q.split(".")])

    def feed_day_words(self, day_mid, is_today):
        y, m, d = time.gmtime(day_mid + 12 * 3600)[:3]
        return ("на сьогодні" if is_today else "на завтра"), f"{d} {next(k for k, v in UA_MONTHS.items() if v == m)}"

    def feed_region_text(self, region, groups, rel, date):
        name = region_name(region, "uk")
        head = f"⚡ <b>{name}</b> — графік {rel}, {date}"
        if all("-" not in sl for sl in groups.values()):
            return head + "\n\nВідключень не заплановано."
        if len(groups) > 12:  # Kyiv: 60 groups — too long for the channel
            return head + f"\n\n{self.feed_span(groups).capitalize()} залежно від групи ({len(groups)} груп).\n<i>Свою групу — у боті.</i>"
        by = {}
        for g in self.feed_order(groups):
            by.setdefault(groups[g], []).append(g)
        lines = [f"<code>{' '.join(gs)}</code> · {self.feed_runs(sl) or 'без відключень'}" for sl, gs in by.items()]
        return head + "\n\n" + "\n".join(lines)

    def feed_change_text(self, region, old, new, rel, date):
        name = region_name(region, "uk")
        head = f"🔄 <b>{name}</b> — зміни в графіку {rel}, {date}"
        changed = [g for g in self.feed_order(new) if old.get(g) != new[g]]
        if len(new) > 12 or len(changed) > 12:
            return head + f"\n\nТепер: {self.feed_span(new)}.\n<i>Свою групу — у боті.</i>"
        lines = [f"<code>{g}</code> · було {self.feed_runs(old.get(g, '')) or 'без відключень'} → тепер <b>{self.feed_runs(new[g]) or 'без відключень'}</b>"
                 for g in changed]
        return head + "\n\n" + "\n".join(lines)

    def feed_summary_text(self, cur, title, date):
        short = lambda r: region_name(r, "uk").replace(" область", "")  # noqa: E731
        lines = [f"<b>{short(r)}</b> — {self.feed_span(cur[r])}" for r in REGIONS if r in cur]
        waiting = [short(r) for r in REGIONS if r not in cur]
        text = f"{title}, {date}</b>\n\n" + "\n".join(lines)
        if waiting:
            more = f" та ще {len(waiting) - 5}" if len(waiting) > 5 else ""
            text += f"\n\n⏳ Ще чекаємо: {', '.join(waiting[:5])}{more} — опублікуємо окремо, щойно з'являться."
        return text

    def channel_feed(self, day_mid, now, is_today):
        """Tomorrow: a 'first regions' summary as soon as any region publishes, then one post per region.
        Today: the 07:00 summary of everything known, then one post per late region. Changes get their own post."""
        mins = (now - kyiv_midnight(*kyiv_today())) / 60
        if is_today and mins < 7 * 60:
            return  # overnight news waits for the morning summary
        cur = self.feed_slots(day_mid)
        if not cur:
            return
        key = f"chan:feed:{day_mid}"
        row = self.db.one("SELECT value FROM cache WHERE key=?", (key,))
        st = json.loads(row["value"]) if row else {"regs": {}}
        save = lambda: self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(st), int(now)))  # noqa: E731
        btn = [[{"text": "💡 Моя черга — у боті", "url": f"https://t.me/{BOT_USERNAME}"}],
               [{"text": "➕ Додати бота в чат будинку", "url": f"https://t.me/{BOT_USERNAME}?startgroup=1"}]]
        rel, date = self.feed_day_words(day_mid, is_today)
        summary = None
        if is_today and not st.get("morning"):
            summary, flag = "☀️ <b>Графіки на сьогодні", "morning"
        elif not is_today and not st.get("head"):
            summary, flag = "🌙 <b>Графіки на завтра — перші області", "head"
        if not is_today:
            for region in cur:
                self.db.q("INSERT OR IGNORE INTO pubs(region,day_ts,at,changes) VALUES(?,?,?,0)", (region, day_mid, int(now)))
        if summary:
            if flag == "morning":
                try:
                    self.tg.photo(CHANNEL, regions_png(day_mid, f"ГРАФІКИ НА СЬОГОДНІ · {date.upper()}", cur),
                                  "☀️ Графіки на сьогодні по областях. Що темніше червоне — то більше черг без світла.")
                except Exception as e:
                    log("morning picture failed:", repr(e))
            if self.channel_post(self.feed_summary_text(cur, summary, date), btn):
                st[flag], st["regs"] = True, cur
                save()
            return
        for region in REGIONS:
            if region not in cur or st["regs"].get(region) == cur[region]:
                continue
            old = st["regs"].get(region)
            text = (self.feed_change_text(region, old, cur[region], rel, date) if old
                    else self.feed_region_text(region, cur[region], rel, date))
            if not self.channel_post(text, btn):
                return
            if old:
                self.db.q("UPDATE pubs SET changes=changes+1 WHERE region=? AND day_ts=?", (region, day_mid))
            st["regs"][region] = cur[region]
            save()

    def channel_live(self, key, body, now):
        """Send the post once, then edit it whenever its body changes."""
        if body:
            row = self.db.one("SELECT value FROM cache WHERE key=?", (key,))
            st = json.loads(row["value"]) if row else {}
            if st.get("body") != body:
                g = time.gmtime(now)
                off = calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0)) - kyiv_midnight(g.tm_year, g.tm_mon, g.tm_mday)
                text = body + f"\n\n<i>Оновлено {time.strftime('%H:%M', time.gmtime(now + off))}</i>"
                btn = [[{"text": "💡 Моя черга — у боті", "url": f"https://t.me/{BOT_USERNAME}"}]]
                if st.get("mid"):
                    self.tg.edit(CHANNEL, st["mid"], text, btn)
                else:
                    res = self.channel_post(text, btn)
                    st["mid"] = ((res or {}).get("result") or {}).get("message_id") if isinstance(res, dict) else None
                if st.get("mid"):
                    st["body"] = body
                    self.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", (key, json.dumps(st, ensure_ascii=False), int(now)))

    def mark(self, chat_id, mark):
        """True the first time a mark is seen for this chat (so each reminder is sent once)."""
        if self.db.one("SELECT 1 FROM sent WHERE chat_id=? AND mark=?", (chat_id, mark)):
            return False
        self.db.q("INSERT INTO sent(chat_id,mark,at) VALUES(?,?,?)", (chat_id, mark, int(time.time())))
        return True

    def check_stale(self, data, now):
        if not ADMIN_CHAT_ID:
            return
        last = _sched["ok_at"] or now
        if data and data.get("lastUpdated"):
            try:
                last = min(last, datetime.fromisoformat(data["lastUpdated"].replace("Z", "+00:00")).timestamp())
            except ValueError:
                pass
        age = now - last
        lang = (self.db.one("SELECT lang FROM users WHERE chat_id=?", (int(ADMIN_CHAT_ID),)) or {"lang": "uk"})["lang"]
        if age > STALE_AFTER and not self.stale_alerted:
            self.stale_alerted = True
            self.tg.send(int(ADMIN_CHAT_ID), T[lang]["stale"].format(
                m=int(age // 60), t=time.strftime("%d.%m %H:%M", time.localtime(last))))
        elif age <= STALE_AFTER and self.stale_alerted:
            self.stale_alerted = False
            self.tg.send(int(ADMIN_CHAT_ID), T[lang]["stale_ok"])

    # ---------- main loop
    def run(self):
        me = self.tg.call("getMe")["result"]
        global BOT_USERNAME
        BOT_USERNAME = me["username"]
        log(f"Bot @{me['username']} started, version {BOT_VERSION}, auto-update {'on' if AUTO_UPDATE else 'off'}")
        for lang in ("uk", "en"):
            try:
                self.tg.call("setMyCommands", commands=[{"command": c, "description": d} for c, d in T[lang]["commands"]],
                             **({"language_code": "en"} if lang == "en" else {}))
            except Exception as e:
                log("setMyCommands failed:", e)
        threading.Thread(target=self.notifier, daemon=True).start()
        threading.Thread(target=self.bulk_sender, daemon=True).start()
        if AUTO_UPDATE:
            threading.Thread(target=self.updater, daemon=True).start()
        pool = ThreadPoolExecutor(max_workers=8)  # a slow YASNO answer never blocks other people
        offset = 0
        while True:
            try:
                res = self.tg.call("getUpdates", offset=offset, timeout=50,
                                   allowed_updates=["message", "callback_query", "inline_query"])["result"]
            except Exception as e:
                log("getUpdates failed:", e)
                time.sleep(5)
                continue
            for upd in res:
                offset = upd["update_id"] + 1
                pool.submit(self.safe, self.handle, upd)

    def notifier(self):
        while True:
            self.safe(self.notify_tick)
            time.sleep(60 - time.time() % 60 + 1)  # once a minute, just after :00

    # ---------- self-update from the GitHub repo
    def updater(self):
        time.sleep(120)  # let the bot settle after a start
        while True:
            self.safe(self.check_update)
            time.sleep(UPDATE_EVERY)

    def admin(self, text):
        if ADMIN_CHAT_ID:
            self.tg.send(int(ADMIN_CHAT_ID), text)

    def check_update(self):
        """Download bot.py and streets.json from GitHub; if they changed, run the new code's
        self-test in a temporary folder and only then replace the files. A new bot.py makes the
        process exit, and the container's restart policy starts it again on the new version."""
        import hashlib
        import subprocess
        import tempfile
        sha = lambda b: hashlib.sha256(b).hexdigest()  # noqa: E731
        new = {}
        for name in UPDATE_FILES:
            try:
                url = UPDATE_URL + name + (f"?t={int(time.time())}" if UPDATE_URL.startswith("http") else "")
                req = urllib.request.Request(url, headers={"User-Agent": "svitlo-kyiv-bot/updater"})
                with urllib.request.urlopen(req, timeout=60) as r:
                    new[name] = r.read()
            except Exception as e:
                if name != "bot.py":  # data files are optional: missing or unreachable -> keep the local copy
                    if not (isinstance(e, urllib.error.HTTPError) and e.code == 404):
                        log("update check: skipped", name, e)
                    continue
                log("update check failed:", name, e)
                return
        local = {}
        for name in UPDATE_FILES:
            try:
                with open(os.path.join(HERE, name), "rb") as f:
                    local[name] = f.read()
            except FileNotFoundError:
                local[name] = b""
        changed = [n for n in new if sha(new[n]) != sha(local[n])]
        if not changed:
            return
        version = sha(b"".join(new[n] for n in UPDATE_FILES if n in new))[:10]
        state_file = os.path.join(DATA_DIR, "update_state.json")
        try:
            with open(state_file, encoding="utf-8") as f:
                state = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            state = {}
        tries = state.get("tries", {})
        if isinstance(state.get("failed"), list):  # older format: a plain list of failed versions
            tries.update({v: 1 for v in state["failed"]})
        if tries.get(version, 0) >= 3:
            return  # failed its self-test three times (an hour apart) — wait for the next version
        log("update found:", ", ".join(changed), version)
        with tempfile.TemporaryDirectory() as tmp:
            for name in new:
                with open(os.path.join(tmp, name), "wb") as f:
                    f.write(new[name])
            env = {**os.environ, "BOT_TOKEN": "", "ADMIN_CHAT_ID": "", "DATA_DIR": os.path.join(tmp, "data"),
                   "STREETS_FILE": os.path.join(tmp, "streets.json"), "DISTRICTS_FILE": os.path.join(tmp, "districts.json"),
                   "DNIPRO_FILE": os.path.join(tmp, "dnipro_streets.json")}
            try:
                res = subprocess.run([sys.executable, "-X", "utf8", os.path.join(tmp, "bot.py"), "--test"], cwd=tmp, env=env,
                                     capture_output=True, text=True, timeout=1800)
                passed = res.returncode == 0
                tail = (res.stdout + res.stderr)[-600:]
            except Exception as e:
                passed, tail = False, repr(e)
            if not passed:  # a real bug, or YASNO was down for a moment: try again next hour, give up after 3
                tries[version] = tries.get(version, 0) + 1
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(state_file, "w", encoding="utf-8") as f:
                    json.dump({"tries": tries}, f)
                log(f"update rejected ({tries[version]}/3), self-test failed:\n" + tail)
                if tries[version] >= 3:
                    self.admin(f"❌ Оновлення бота {version} тричі не пройшло самоперевірку — працює попередня версія.\n<pre>{html.escape(tail[-400:])}</pre>")
                return
        # Install. The temp folder may be on another disk than the bot folder (inside a container it is),
        # and a rename can't cross disks — so write each file next to its target first, then swap it in
        # with a same-folder rename, which is atomic: bot.py is never missing, even for a moment.
        import shutil
        for name in changed:
            dst = os.path.join(HERE, name)
            with open(dst + ".new", "wb") as f:
                f.write(new[name])
                f.flush()
                os.fsync(f.fileno())
            if os.path.exists(dst):
                shutil.copy2(dst, dst + ".bak")
            os.replace(dst + ".new", dst)
        log("updated:", ", ".join(changed), version)
        self.admin(f"🔄 Бот оновлено ({', '.join(changed)}, версія {version}).")
        if "bot.py" in changed:
            log("restarting on the new version")
            os._exit(0)  # docker 'restart: unless-stopped' starts the new code
        global STREETS, STREET_BY_ID, DISTRICTS, DNIPRO
        STREETS, STREET_BY_ID = load_streets()
        DISTRICTS = load_districts()
        DNIPRO = load_dnipro()

    @staticmethod
    def safe(fn, *a):
        try:
            fn(*a)
        except Exception as e:
            import traceback
            log("error:", repr(e))
            traceback.print_exc()


# ===================================================================== offline self-test
class FakeTG:
    def __init__(self):
        self.out = []
        self.on_blocked = lambda cid: None

    def send(self, chat_id, text, buttons=None):
        self.out.append(("send", chat_id, text, buttons))

    def edit(self, chat_id, mid, text, buttons=None):
        self.out.append(("edit", chat_id, text, buttons))

    def photo(self, chat_id, png, caption="", buttons=None):
        self.out.append(("photo", chat_id, caption, buttons))
        self.photos = getattr(self, "photos", []) + [png]
        return {"result": {"message_id": len(self.out)}}

    def call(self, *a, **k):
        if a and a[0] == "getChatMember":
            return {"result": {"status": "administrator" if k.get("user_id") in getattr(self, "admins", ()) else "member"}}
        return {}


def self_test():
    NET_CAP[0] = 12
    import copy
    import tempfile
    tmp = tempfile.mkdtemp()
    db = DB(os.path.join(tmp, "bot.db"))
    tg = FakeTG()
    bot = Bot(db, tg)
    first = lambda: tg.out[0][2].split("\n")[0][:80] if tg.out else "-"  # noqa: E731
    btns = lambda i=-1: [b["text"] for r in (tg.out[i][3] or []) for b in r] if tg.out else []  # noqa: E731
    ok = True

    def step(label, fn, expect):
        nonlocal ok
        tg.out.clear()
        fn()
        text = " | ".join(o[2].split("\n")[0][:70] for o in tg.out)
        good = expect(text)
        ok &= good
        print(("PASS " if good else "FAIL ") + label + " -> " + text)

    C = 777
    step("start", lambda: bot.text(C, "/start", "uk"), lambda s: "Привіт" in s)
    step("street", lambda: bot.text(C, "Перемиш"), lambda s: "Перемишльська" in s)
    step("house", lambda: bot.text(C, "2"), lambda s: "Черга 4.1" in s)
    print("     buttons:", btns())
    t0 = time.time()
    step("cached street", lambda: bot.text(C, "Перемиш"), lambda s: "Перемишльська" in s)
    print(f"     cached lookup took {time.time() - t0:.3f}s")
    step("cached house (same address, no duplicate)", lambda: bot.text(C, "2"), lambda s: "Черга 4.1" in s)
    step("second address via /queue", lambda: bot.text(C, "/queue 7.1"), lambda s: "Черга 7.1" in s)
    print("     addresses in db:", [(a["label"], a["queue"]) for a in bot.addresses(C)])
    msg = {"chat": {"id": C}, "message_id": 1}
    cb = lambda data: bot.callback({"id": "x", "from": {}, "message": msg, "data": data})  # noqa: E731
    step("addresses view", lambda: cb("a"), lambda s: "адреси" in s)
    print("     buttons:", btns())
    first_id = bot.addresses(C)[0]["id"]
    step("select first address", lambda: cb(f"as:{first_id}"), lambda s: "Черга 4.1" in s)
    step("settings", lambda: cb("st"), lambda s: "Налаштування" in s)
    step("warn 60", lambda: cb("sw:60"), lambda s: True)
    step("power-back on", lambda: cb("sr"), lambda s: True)
    print("     buttons:", btns())
    step("language", lambda: cb("sl"), lambda s: "Settings" in s)
    cb("sl")
    step("bad queue", lambda: bot.text(C, "/queue 99"), lambda s: "Не знаю" in s)
    global ADMIN_CHAT_ID
    saved_admin, ADMIN_CHAT_ID = ADMIN_CHAT_ID, ""
    step("/stats hidden from non-admins", lambda: bot.text(C, "/stats"), lambda s: "Статистика" not in s)
    ADMIN_CHAT_ID = str(C)
    step("/stats for admin", lambda: bot.text(C, "/stats"), lambda s: "Статистика" in s)
    print("     " + tg.out[-1][2].replace("\n", " / ")[:300] if tg.out else "")
    step("/health for admin", lambda: bot.text(C, "/health"), lambda s: "Джерела графіків" in s)
    print("     " + tg.out[-1][2].replace("\n", " / ")[:400] if tg.out else "")
    # watchdog: a source failing 4 h -> one alert; fixed -> one 'ok'; nothing for others
    rv, saved_rv = _scheds["rivne"], dict(_scheds["rivne"])
    rv.update(err="URLError('timed out')", err_at=time.time() - 4 * 3600)
    step("watchdog: failing source -> alert", lambda: bot.health_tick(fetch=False),
         lambda s: s.count("⚠️") == 1 and "Рівнеобленерго" in s and "не читається" in s)
    step("watchdog: no repeat", lambda: bot.health_tick(fetch=False), lambda s: s == "")
    rv.update(err=None, ok_at=time.time())
    step("watchdog: fixed -> ok", lambda: bot.health_tick(fetch=False), lambda s: "✅" in s and "Рівнеобленерго" in s and "знову читається" in s)
    rv.update(saved_rv)
    ADMIN_CHAT_ID = saved_admin
    if STREETS:
        step("list: start (districts if known, else letters)", lambda: cb("L"), lambda s: ("район" in s) if DISTRICTS else ("літеру" in s))
        print("     letters:", "".join(btns()))
        gong = next(i for i, x in enumerate(STREETS["Г"]) if x[0] == "Гонгадзе Георгія")
        page = gong // PAGE + 1
        step(f"list: Г page {page}", lambda: cb(f"L:Г:{page}"), lambda s: "«Г»" in s)
        print("     buttons:", btns()[:4], "…", btns()[-4:])
        sid = STREETS["Г"][gong][1]
        step("list: pick Гонгадзе", lambda: cb(f"ls:{sid}"), lambda s: "Гонгадзе" in s)
        step("house 1/102", lambda: bot.text(C, "1/102"), lambda s: "Черга" in s)
        step("local search, 2 letters", lambda: bot.text(C, "Хр"), lambda s: "Виберіть вулицю" in s or "Хрещатик" in s)
        print("     buttons:", btns()[:5])

    data = schedule()
    days = days_from_today(data, time.time())
    today_ts = days[0][0]
    seq = slots(days[0][1].get("GPV4.1"))
    j = next(i for i in range(2, 48) if seq[i] == "-" and seq[i - 1] != "-")
    jb = next((i for i in range(2, 48) if seq[i] == "+" and seq[i - 1] == "-"), None)
    _sched["at"] = time.time() + 10 ** 6  # freeze the schedule for the simulation
    bot.notify_tick(now=today_ts + 60)  # first observation
    step("warn 45 min before (setting 60)", lambda: bot.notify_tick(now=today_ts + (j * 30 - 45) * 60),
         lambda s: "Через 45 хв" in s)
    step("no duplicate warning", lambda: bot.notify_tick(now=today_ts + (j * 30 - 44) * 60), lambda s: s == "")
    if jb:
        step("power back", lambda: bot.notify_tick(now=today_ts + (jb * 30 + 2) * 60), lambda s: "мало повернутися" in s)
    changed = copy.deepcopy(data)
    hrs = changed["fact"]["data"][str(today_ts)]["GPV4.1"]
    hrs["24"] = "no" if hrs.get("24") != "no" else "yes"
    _sched["data"] = changed
    step("today's schedule changed", lambda: bot.notify_tick(now=today_ts + 12 * 3600), lambda s: "змінив графік на сьогодні" in s)
    tom = copy.deepcopy(changed)
    tom["fact"]["data"][str(today_ts + 86400)] = changed["fact"]["data"][str(today_ts)]
    _sched["data"] = tom
    bot.set_user(C, quiet=1)
    step("tomorrow published at 23:30 waits (quiet)", lambda: bot.notify_tick(now=today_ts + 23.5 * 3600), lambda s: s == "")
    step("…and arrives after 07:00", lambda: bot.notify_tick(now=today_ts + 7 * 3600 + 60),
         lambda s: "графік на завтра" in s)
    _sched["data"] = data
    bot.set_user(C, quiet=0)

    # regions
    step("region picker", lambda: cb("rg"), lambda s: "Виберіть регіон" in s)
    print("     buttons:", btns())
    step("Odesa: DTEK links + queue buttons", lambda: cb("rg:odesa"),
         lambda s: any("Чергу за адресою знає лише ДТЕК" in o[2] for o in tg.out)
         and any(b.get("callback_data") == "qq:3.2" for r in tg.out[-1][3] for b in r))
    step("Odesa: tap queue 3.2", lambda: cb("qq:3.2"), lambda s: "Черга 3.2" in s)
    step("Odesa: /queue 3.2", lambda: bot.text(C, "/queue 3.2"), lambda s: "Черга 3.2" in s)
    print("     ", tg.out[-1][2].split("\n")[1], "| site:", [b.get("url") for r in tg.out[-1][3] for b in r if b.get("url")])
    step("Dnipro: asks for town", lambda: cb("rg:dnipro"), lambda s: "населеного пункту" in s)
    step("Dnipro: town", lambda: bot.text(C, "Дніпро"), lambda s: "м. Дніпро" in s)
    step("Dnipro: street", lambda: bot.text(C, "Яворн"), lambda s: "Виберіть вулицю" in s or "Яворницького" in s)
    sid_d = next((b["callback_data"][2:] for o in tg.out[-1:] for r in (o[3] or []) for b in r
                  if b.get("callback_data", "").startswith("s:")), None)
    if sid_d:
        step("Dnipro: pick street", lambda: cb(f"s:{sid_d}"), lambda s: "Яворницького" in s)
    step("Dnipro: house 1", lambda: bot.text(C, "1"), lambda s: "Черга" in s or "Виберіть будинок" in s)
    hid_d = next((b["callback_data"][2:] for o in tg.out[-1:] for r in (o[3] or []) for b in r
                  if b.get("callback_data", "").startswith("h:")), None)
    if hid_d and tg.out[-1][2].startswith(("Виберіть будинок", "Pick")):
        step("Dnipro: pick house", lambda: cb(f"h:{hid_d}"), lambda s: "Черга" in s)
    print("     addresses:", [(a["label"], a["queue"], a["region"]) for a in bot.addresses(C)])
    step("reminders across regions run", lambda: bot.notify_tick(), lambda s: True)

    # whole address by taps: (district) -> letter -> street -> house buttons
    bot.set_user(C, region="kyiv", state=None)
    bot.db.q("DELETE FROM addresses WHERE chat_id=?", (C,))  # the test user hit the 5-address limit above
    if STREETS:
        step("list start", lambda: cb("L"), lambda s: ("район" in s) if DISTRICTS else ("літеру" in s))
        d = "-"
        if DISTRICTS:
            print("     districts:", btns())
            d = str(next(i for i, (name, _) in enumerate(DISTRICTS) if name.startswith("Шевченків")))
            step("district letters", lambda: cb(f"LL:{d}"), lambda s: "район" in s and "вулиць" in s)
            print("     letters:", "".join(b for b in btns() if len(b) <= 3))
        xs = bot.district_streets(d).get("Х", [])
        xpage = next((i // PAGE + 1 for i, x in enumerate(xs) if x[2] == "вул. Хрещатик"), 1)
        step(f"streets on Х, page {xpage}", lambda: cb(f"L:{d}:Х:{xpage}"), lambda s: "«Х»" in s)
        xid = next((b["callback_data"][3:] for r in tg.out[-1][3] for b in r if b.get("text") == "вул. Хрещатик"), None)
        print("     streets:", btns()[:6])
        if xid:
            step("tap Хрещатик -> house buttons", lambda: cb(f"ls:{xid}"), lambda s: "Хрещатик" in s and "будин" in s)
            print("     houses:", btns()[:12], "…", btns()[-4:])
            step("houses page 2", lambda: cb("hp:2"), lambda s: "Хрещатик" in s)
            hid22 = next((k for k, v in bot.state(bot.user(C))["houses"].items() if v == "22"), None)
            step("tap house 22", lambda: cb(f"h:{hid22}"), lambda s: "Черга 20.1" in s)
        step("old-style list button still works", lambda: cb("L:Г:1"), lambda s: "«Г»" in s)

        # inline search: typing '@bot Свобо' lists streets; picking one goes straight to the houses
        calls = []
        real_call = bot.tg.call
        bot.tg.call = lambda method, **k: calls.append((method, k)) or {}
        step("welcome has 'Find a street' button", lambda: bot.text(4242, "/start", "uk"),
             lambda s: "Привіт" in s and any(b.get("switch_inline_query_current_chat") == "" for r in tg.out[-1][3] for b in r))
        bot.inline({"id": "q1", "from": {"id": C, "language_code": "uk"}, "query": "Свобо"})
        titles = [r["title"] for r in calls[-1][1]["results"]] if calls else []
        step("inline 'Свобо' -> street list", lambda: tg.out.append(("send", C, " / ".join(titles[:4]), None)),
             lambda s: "Свободи" in s)
        bot.inline({"id": "q2", "from": {"id": C}, "query": "С"})
        step("inline with 1 letter -> hint button only", lambda: tg.out.append(("send", C, str(calls[-1][1].get("button", {}).get("text")), None)),
             lambda s: "Почніть" in s)
        picked = calls[-2][1]["results"][0]["input_message_content"]["message_text"] if len(calls) >= 2 else ""
        bot.tg.call = real_call
        bot.db.q("DELETE FROM addresses WHERE chat_id=?", (C,))
        step(f"picked '{picked}' -> houses for that street", lambda: bot.text(C, picked),
             lambda s: picked.lstrip("📍 ") in s and bot.state(bot.user(C)).get("street", {}).get("name") == picked.lstrip("📍 "))

    # Dnipro region: towns -> (letter) -> street -> houses, and inline search with the town shown
    if DNIPRO:
        Z = 6161
        bot.user(Z, "uk")
        bot.set_user(Z, region="dnipro")
        cbz = lambda data: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": Z}, "message_id": 9}, "data": data})  # noqa: E731
        step("Dnipro: add address shows town buttons", lambda: bot.ask_address(Z, bot.user(Z)),
             lambda s: any(b.get("callback_data") == "DT" for r in tg.out[-1][3] for b in r))
        step("Dnipro: cities", lambda: cbz("DT"), lambda s: "місто" in s)
        tz = next(b["callback_data"][3:] for r in tg.out[-1][3] for b in r if b.get("callback_data", "").startswith("DX:"))
        step("Dnipro: open a town", lambda: cbz(f"DX:{tz}"), lambda s: DNIPRO[tz][0] in s)
        nxt = tg.out[-1][3][0][0]["callback_data"]
        if nxt.startswith("DL:"):
            step("Dnipro: a letter", lambda: cbz(nxt), lambda s: "«" in s)
        sz = next(b["callback_data"] for r in tg.out[-1][3] for b in r if b.get("callback_data", "").startswith("DS:"))
        step("Dnipro: pick a street -> houses", lambda: cbz(sz), lambda s: DNIPRO[tz][2][sz.split(":")[2]] in s)
        calls_d = []
        real_d = bot.tg.call
        bot.tg.call = lambda method, **k: calls_d.append(k) or {}
        bot.inline({"id": "qd", "from": {"id": Z}, "query": "Шевч"})
        bot.tg.call = real_d
        res_d = calls_d[-1]["results"] if calls_d else []
        step("Dnipro: inline shows the town", lambda: tg.out.append(("send", Z, " / ".join(f"{r['title']} — {r['description']}" for r in res_d[:3]), None)),
             lambda s: "Шевченка" in s and "м. " in s)
        if res_d and res_d[0]["id"] != "none":
            pick_d = res_d[0]["input_message_content"]["message_text"]
            step("Dnipro: picked inline result -> houses", lambda: bot.text(Z, pick_d),
                 lambda s: bot.state(bot.user(Z)).get("city", {}).get("name", "") in pick_d)

    # channel feed: 'first regions' summary, then a post per new region, a post per change; 07:00 summary; intro once
    D = kyiv_midnight(*kyiv_today(1))
    A = "-" * 4 + "+" * 16 + "-" * 4 + "+" * 24
    B = "+" * 20 + "-" * 8 + "+" * 20
    feed = {"zhytomyr": {g: A for g in STANDARD_GROUPS}}
    bot.feed_slots = lambda day: {r: dict(v) for r, v in feed.items()}
    bot.db.q("INSERT OR REPLACE INTO cache(key,value,at) VALUES(?,?,?)", ("chan:intro", CHANNEL, 0))
    eve = kyiv_midnight(*kyiv_today()) + 18 * 3600

    def posts_after(fn):
        n0 = len(tg.out)
        fn()
        return [o[2] for o in tg.out[n0:] if o[1] == CHANNEL and o[0] != "photo"]
    p1 = posts_after(lambda: bot.channel_feed(D, eve, is_today=False))
    p2 = posts_after(lambda: bot.channel_feed(D, eve + 60, is_today=False))
    feed["kharkiv"] = {g: (B if g.endswith(".1") else A) for g in STANDARD_GROUPS}
    p3 = posts_after(lambda: bot.channel_feed(D, eve + 120, is_today=False))
    feed["zhytomyr"]["2.2"] = B
    p4 = posts_after(lambda: bot.channel_feed(D, eve + 180, is_today=False))
    feed["lviv"] = {g: "+" * 48 for g in STANDARD_GROUPS}
    morning = D + 7 * 3600 + 120
    real_today = kyiv_today
    globals()["kyiv_today"] = lambda k=0: real_today(k + 1)  # pretend it's tomorrow morning
    np5 = len(tg.out)
    p5 = posts_after(lambda: bot.channel_feed(D, morning, is_today=True))
    morning_photo = [o for o in tg.out[np5:] if o[0] == "photo" and o[1] == CHANNEL]
    feed["rivne"] = {g: A for g in STANDARD_GROUPS}
    p6 = posts_after(lambda: bot.channel_feed(D, morning + 60, is_today=True))
    globals()["kyiv_today"] = real_today
    ok_f = (len(p1) == 1 and "перші області" in p1[0] and "Житомирська</b> — до 4 год" in p1[0] and "опублікуємо окремо" in p1[0]
            and p2 == []
            and len(p3) == 1 and "Харківська область</b> — графік на завтра" in p3[0] and "1.1 2.1 3.1 4.1 5.1 6.1</code> · 10:00–14:00" in p3[0]
            and len(p4) == 1 and "зміни" in p4[0] and "2.2</code> · було 00:00–02:00, 10:00–12:00 → тепер <b>10:00–14:00</b>" in p4[0]
            and len(p5) == 1 and "Графіки на сьогодні" in p5[0] and "Львівська</b> — відключень не заплановано" in p5[0]
            and len(p6) == 1 and "Рівненська область</b> — графік на сьогодні" in p6[0] and len(morning_photo) == 1)
    for i, x in enumerate((p1, p3, p4, p5, p6), 1):
        print(f"     feed {i}:", (x[0] if x else x).replace("\n", " | ")[:260])
    step("channel feed: summary, per-region, changes, morning", lambda: tg.out.append(("send", 1, "OK" if ok_f else "BAD", None)), lambda s: s == "OK")
    feed.clear()
    bot.db.q("DELETE FROM cache WHERE key=?", ("chan:intro",))
    pi = posts_after(lambda: (bot.channel_tick(eve), bot.channel_tick(eve + 60)))
    step("channel: intro once", lambda: tg.out.append(("send", 1, "OK" if len(pi) == 1 and "Світло" in pi[0] else f"BAD {len(pi)}", None)), lambda s: s == "OK")
    del bot.feed_slots
    u7 = bot.user(7300, "uk")
    bot.set_user(7300, region="chernivtsi")
    step("chernivtsi: /queue 7 stays 7", lambda: bot.text(7300, "/queue 7"), lambda s: "Черга 7</b>" in s)

    # Volyn: a known archived picture (17.10.2026, 16:21) read cell by cell; skipped if the archive is gone
    try:
        req = urllib.request.Request(VOE_API + "/media/2026/10/6ad37651be5c4_GPV.png", headers={"User-Agent": "svitlo-kyiv-bot/1.0"})
        vg = voe_grid(urllib.request.urlopen(req, timeout=60).read())
        spans = lambda c: [i for i, off in enumerate(c) if off]  # noqa: E731
        ok_v = (vg is not None and spans(vg["1.1"]) == list(range(20, 28)) + [42, 43] and spans(vg["2.2"]) == list(range(29, 36)) + [42, 43]
                and spans(vg["6.1"]) == list(range(36, 44)) and not any(vg["5.1"][:35]))
        step("Volyn: archived picture -> outages", lambda: tg.out.append(("send", 1, "OK" if ok_v else f"BAD {vg and vg.get('1.1')}", None)),
             lambda s: s == "OK")
    except urllib.error.URLError as e:
        print("     Volyn picture test skipped:", e)

    # /now for several addresses, weekly stats, publication stats
    NW = 7990
    bot.user(NW, "uk")
    schedule("zhytomyr")
    zgr = next(iter(region_groups("zhytomyr")), "1.1")
    bot.add_address(NW, "Дім", zgr, "zhytomyr")
    bot.add_address(NW, "Дача", "1.1", "volyn")
    step("/now: the full status as before", lambda: bot.text(NW, "/now"), lambda s: "Черга" in s and "Зараз," not in s)
    step("/all: one line per address", lambda: bot.text(NW, "/all"), lambda s: "Зараз" in s)
    nv = tg.out[-1][2] if tg.out else ""
    step("/now: both addresses listed", lambda: tg.out.append(("send", 1, "OK" if "Дім" in nv and "Дача" in nv and "графік не опубліковано" in nv else "BAD " + nv[:200], None)),
         lambda s: s == "OK")
    step("/pic sends the queue picture", lambda: bot.text(NW, "/pic"), lambda s: "Черга" in s and tg.out[-1][0] == "photo")
    td = kyiv_midnight(*kyiv_today())
    for k, m in enumerate((600, 480, 300, 240, 0)):
        bot.db.q("INSERT OR REPLACE INTO history(region,queue,day_ts,off_min) VALUES(?,?,?,?)", ("zhytomyr", zgr, td - k * 86400, m))
    wk = bot.week_text(bot.user(NW), time.time())
    print("     week:", wk.replace("\n", " | ")[:300])
    step("/week: total and hardest day", lambda: tg.out.append(("send", 1, "OK" if "27 год" in wk and "Найважчий день" in wk and "10 год" in wk else "BAD", None)),
         lambda s: s == "OK")
    sun = td + ((6 - time.gmtime(td + 43200).tm_wday) % 7) * 86400
    for r_, mins_ in (("zhytomyr", 17 * 60), ("kharkiv", 21 * 60), ("rivne", 27 * 60 + 30)):
        for k in range(3):
            d_ = sun - k * 86400
            bot.db.q("INSERT OR REPLACE INTO pubs(region,day_ts,at,changes) VALUES(?,?,?,?)", (r_, d_, d_ - 86400 + mins_ * 60, 1 if r_ == "kharkiv" else 0))
    real_today = kyiv_today
    globals()["kyiv_today"] = lambda k=0: time.gmtime(sun + 43200 + k * 86400)[:3]
    n0 = len(tg.out)
    bot.pubs_post(sun + 12 * 3600 + 60)
    globals()["kyiv_today"] = real_today
    pp = [o[2] for o in tg.out[n0:] if o[1] == CHANNEL]
    print("     pubs:", pp[0].replace("\n", " | ")[:300] if pp else pp)
    step("publication stats post", lambda: tg.out.append(("send", 1, "OK" if pp and "1. Житомирська — 17:00" in pp[0] and "03:30 (вночі)" in pp[0] and "Харківська (3 разів" in pp[0] else "BAD", None)),
         lambda s: s == "OK")

    # pictures
    cvp = Canvas(40, 20)
    cvp.text(1, 1, "ЧЕРГА 1.1", C_INK, 1)
    png1 = cvp.png()
    try:
        w1, h1, rows1 = png_rgb(png1)
        ok_p = (w1, h1) == (40, 20) and rows1[1][3 * 1:3 * 2] == bytes(C_INK) and all(ch in FONT for ch in "АБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЬЮЯ0123456789:.")
    except Exception as e:
        ok_p = False
        print("     png:", e)
    step("pictures: PNG round-trip and font", lambda: tg.out.append(("send", 1, "OK" if ok_p else "BAD", None)), lambda s: s == "OK")
    schedule("zhytomyr")
    qp = queue_png("zhytomyr", "2.2")
    rp = regions_png(kyiv_midnight(*kyiv_today()), "ГРАФІКИ НА СЬОГОДНІ · 12 ЛИСТОПАДА", {"zhytomyr": {g: "-" * 8 + "+" * 40 for g in STANDARD_GROUPS}})
    open(os.path.join(tmp, "q.png"), "wb").write(qp)
    open(os.path.join(tmp, "r.png"), "wb").write(rp)
    print("     pictures:", os.path.join(tmp, "q.png"), len(qp), os.path.join(tmp, "r.png"), len(rp))
    step("pictures: queue and regions render", lambda: tg.out.append(("send", 1, "OK" if qp[:4] == b"\x89PNG" and rp[:4] == b"\x89PNG" else "BAD", None)),
         lambda s: s == "OK")

    # 'invite neighbours': sharing an address with a street opens the bot on that street
    sp = bot.street_param({"label": "вул. Перемишльська, 2", "region": "kyiv"})
    su = bot.share_url(bot.user(C), {"label": "вул. Перемишльська, 2", "region": "kyiv", "queue": "4.1"}) if sp else ""
    step("invite: share link points to the street", lambda: tg.out.append(("send", 1, "OK" if sp and "start%3Dst_" in su and "Сусіди" in urllib.parse.unquote_plus(su) else f"BAD {sp}", None)),
         lambda s: s == "OK")
    if sp:
        step("invite: a neighbour opens it -> the street's houses", lambda: bot.text(8101, f"/start {sp[0]}", "uk"), lambda s: "Перемишльська" in s)

    # group chats
    G, ADM, MEM = -100777, 501, 502
    tg.admins = {ADM}
    gm = lambda text, uid=ADM, **extra: bot.handle({"message": {"chat": {"id": G, "type": "group"}, "from": {"id": uid}, "text": text, **extra}})  # noqa: E731
    gcb = lambda data, uid=ADM: bot.handle({"callback_query": {"id": "q", "from": {"id": uid}, "data": data, "message": {"chat": {"id": G, "type": "group"}, "message_id": 1}}})  # noqa: E731
    step("group: bot added -> welcome", lambda: bot.handle({"message": {"chat": {"id": G, "type": "group"}, "from": {"id": ADM},
                                                                         "new_chat_members": [{"id": 1, "username": BOT_USERNAME}]}}),
         lambda s: "Привіт" in s)
    step("group: plain chatter is ignored", lambda: gm("привіт сусіди"), lambda s: s == "")
    step("group: command for another bot is ignored", lambda: gm("/now@OtherBot"), lambda s: s == "")
    step("group: non-admin can't set the address", lambda: gm("/address Перемиш", uid=MEM), lambda s: "лише адмін" in s)
    step("group: admin sets the street", lambda: gm(f"/address@{BOT_USERNAME} Перемиш"), lambda s: "Перемишльська" in s)
    gsid = next((b["callback_data"] for r in (tg.out[-1][3] or []) for b in r if b["callback_data"].startswith(("s:", "h:"))), None)
    if gsid and gsid.startswith("s:"):
        gcb(gsid)
    gh = next((b["callback_data"] for o in reversed(tg.out) for r in (o[3] or []) for b in r if b.get("callback_data", "").startswith("h:")), None)
    n0 = len(tg.out)
    gcb(gh, uid=MEM)
    blocked_cb = len(tg.out) == n0
    step("group: admin picks the house -> status with group buttons", lambda: gcb(gh) if gh else None,
         lambda s: "Черга" in s and blocked_cb)
    gst = tg.out[-1][3] or []
    gu = bot.user(G)
    step("group: quiet nights + power-back on, no report buttons", lambda: tg.out.append(("send", 1, "OK" if gu["quiet"] and gu["ret"] and not any(b.get("callback_data", "").startswith("rp") for r in gst for b in r) else "BAD", None)),
         lambda s: s == "OK")
    step("group: anyone can ask /now", lambda: gm("/now", uid=MEM), lambda s: "Черга" in s)
    step("private: /group gives the link", lambda: bot.text(5151, "/group"), lambda s: "чаті вашого будинку" in s and "startgroup" in str(tg.out[-1][3]))

    # mass notifications go out soonest-outage first
    import queue as _q
    pq = _q.PriorityQueue()
    for prio, cid_ in ((300, "late"), (5, "soon"), (10 ** 6, "none"), (0, "notice")):
        pq.put((prio, 0, cid_, "x"))
    order = [pq.get()[2] for _ in range(4)]
    step("bulk: soonest outage first", lambda: tg.out.append(("send", 1, "OK" if order == ["notice", "soon", "late", "none"] else f"BAD {order}", None)),
         lambda s: s == "OK")

    # notices: emergency outages and cancelled schedules
    nw = time.time()
    n1 = pick_notice([(nw - 600, "По Черкаській області застосовані графіки аварійних відключень (ГАВ)"),
                      (nw - 9000, "Оновлено графік погодинних відключень (ГПВ) на 7 листопада")], "u", nw)
    n2 = pick_notice([(nw - 600, "Оновлено графік погодинних відключень (ГПВ) на 7 листопада"),
                      (nw - 3000, "По Черкаській області застосовані графіки аварійних відключень (ГАВ)")], "u", nw)
    n3 = pick_notice([(nw - 8 * 3600, "застосовані графіки аварійних відключень (ГАВ)")], "u", nw)
    n4 = pick_notice([(nw - 60, "Графіки аварійних відключень скасовано")], "u", nw)
    n5 = pick_notice([(nw - 60, "До 15:00 скасовані відключення згідно із графіком")], "u", nw)
    ok_n = (n1 and n1["kind"] == "emergency") and n2 is None and n3 is None and n4 is None and (n5 and n5["kind"] == "cancel")
    step("notices: ГАВ on, superseded by a schedule post, expired, ended; cancel", lambda: tg.out.append(("send", 1, "OK" if ok_n else f"BAD {n1} {n2} {n3} {n4} {n5}", None)),
         lambda s: s == "OK")
    schedule("cherkasy")
    csc = _scheds["cherkasy"]
    saved_c = dict(csc)
    if csc["data"]:
        csc["data"] = dict(csc["data"], notices=[{"kind": "emergency", "at": int(nw - 300), "text": "ГАВ", "url": "https://www.cherkasyoblenergo.com"}])
        csc["at"] = time.time()
        st_n = status_text("1.1", "uk", None, nw, "cherkasy")
        step("notices: warning on top of the schedule", lambda: tg.out.append(("send", 1, "OK" if "діють графіки аварійних відключень" in st_n and "деталі" in st_n else "BAD " + st_n[:200], None)),
             lambda s: s == "OK")
        n0 = len(tg.out)
        bot.channel_tick(nw)
        cp = [o[2] for o in tg.out[n0:] if o[1] == CHANNEL and "⚠️" in o[2]]
        bot.channel_tick(nw + 60)
        cp2 = [o[2] for o in tg.out[n0:] if o[1] == CHANNEL and "⚠️" in o[2]]
        step("notices: one channel post", lambda: tg.out.append(("send", 1, "OK" if len(cp) == 1 and len(cp2) == 1 and "Черкаська область" in cp[0] else f"BAD {cp2}", None)),
             lambda s: s == "OK")
    csc.clear()
    csc.update(saved_c)

    # YASNO backup: planned-outages JSON -> same hours as the main source would give
    y, mo, d = kyiv_today()
    sample = {"4.1": {"today": {"date": f"{y}-{mo:02d}-{d:02d}T00:00:00+02:00", "status": "ScheduleApplies",
                                "slots": [{"start": 0, "end": 90, "type": "Definite"}, {"start": 90, "end": 1440, "type": "NotPlanned"}]},
                      "tomorrow": {"date": "2000-01-01T00:00:00+02:00", "status": "WaitingForSchedule", "slots": []}}}
    yd = yasno_payload(sample)["fact"]["data"]
    yday = yd.get(str(kyiv_midnight(y, mo, d)), {}).get("GPV4.1", {})
    ok_y = len(yd) == 1 and yday.get("1") == "no" and yday.get("2") == "first" and yday.get("3") == "yes"
    step("YASNO backup: slots -> hours, waiting day skipped", lambda: tg.out.append(("send", 1, "OK" if ok_y else f"BAD {yd}"[:200], None)),
         lambda s: s == "OK")

    # regions read from the oblenergos' official APIs: pick the region, tap the queue
    for rg, co, q1 in (("mykolaiv", "Миколаївобленерго", "1.1"), ("lviv", "Львівобленерго", "1.1"),
                       ("ivano-frankivsk", "Прикарпаттяобленерго", "1.1"), ("cherkasy", "Черкасиобленерго", "1.1"),
                       ("poltava", "Полтаваобленерго", "1.1"), ("zaporizhzhia", "Запоріжжяобленерго", "1.1"),
                       ("rivne", "Рівнеобленерго", "1.1"), ("kharkiv", "Харківобленерго", "1.1"),
                       ("kirovohrad", "Кіровоградобленерго", "1.1"), ("volyn", "Волиньобленерго", "1.1"), ("zhytomyr", "Житомиробленерго", "1.1"), ("khmelnytskyi", "Хмельницькобленерго", "1.1"), ("chernivtsi", "Чернівціобленерго", "1")):
        W = 7000 + len(rg)
        bot.user(W, "uk")
        cbw = lambda data, W=W: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": W}, "message_id": 3}, "data": data})  # noqa: E731
        step(f"{rg}: pick region -> {co} queue buttons", lambda: cbw(f"rg:{rg}"),
             lambda s, co=co, q1=q1: any(co in o[2] for o in tg.out) and any(b.get("callback_data") == "qq:" + q1 for r in tg.out[-1][3] for b in r))
        step(f"{rg}: tap {q1} -> schedule", lambda q1=q1: cbw("qq:" + q1), lambda s, q1=q1: f"Черга {q1}" in s)
        dv = bot.dtek_view(bot.user(W))
        ok_dv = co in dv[0] and all(b.get("url") for r in dv[1] for b in r if "url" in b)
        step(f"{rg}: report view names {co}", lambda: tg.out.append(("send", W, "OK" if ok_dv else "BAD " + dv[0][:120], None)),
             lambda s: s == "OK")

    # Lviv region: address search in Lvivoblenergo's official directory
    V = 7700
    bot.user(V, "uk")
    bot.set_user(V, region="lviv")
    step("Lviv: town 'Львів'", lambda: bot.text(V, "Львів"), lambda s: "Львів" in s)
    step("Lviv: street 'Наукова' -> house buttons", lambda: bot.text(V, "Наукова"), lambda s: "Наукова" in s)
    h96 = next((k for k, v in bot.state(bot.user(V)).get("houses", {}).items() if v == "96"), None)
    if h96:
        step("Lviv: house 96 -> its queue", lambda: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": V}, "message_id": 4}, "data": f"h:{h96}"}),
             lambda s: "Черга" in s and "Наукова, 96" in s)

    # Mykolaiv region: Mykolaivoblenergo's directory, district office -> town -> street -> house
    M = 7800
    bot.user(M, "uk")
    bot.set_user(M, region="mykolaiv")
    cbm = lambda data: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": M}, "message_id": 5}, "data": data})  # noqa: E731
    step("Mykolaiv: district offices", lambda: cbm("MK"), lambda s: "дільницю" in s)
    fid = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"].startswith("Миколаївський")), None)
    if fid:
        step("Mykolaiv: city -> streets", lambda: cbm(f"MKF:{fid}"), lambda s: "вулицю" in s)
        step("Mykolaiv: type 'Соборна'", lambda: bot.text(M, "Соборна"), lambda s: "1/1" in s or "вулицю" in s)
        sid_m = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"] == "вул. Соборна"), None)
        if sid_m:
            step("Mykolaiv: street -> houses", lambda: cbm(f"MKI:{sid_m}"), lambda s: "Соборна" in s)
            h1 = next((k for k, v in bot.state(bot.user(M)).get("houses", {}).items() if v == "1"), None)
            if h1:
                step("Mykolaiv: house 1 -> queue", lambda: cbm(f"h:{h1}"), lambda s: "Черга" in s and "Соборна, 1" in s)

    # Zaporizhzhia: street -> house number from the operator's per-queue sheets
    ZA = 7980
    bot.user(ZA, "uk")
    bot.set_user(ZA, region="zaporizhzhia")
    m_ok = (bot.zp_match("13-23", "15") and not bot.zp_match("13-23", "16") and bot.zp_match("16 (п.1-4)", "16")
            and bot.zp_match("2-10 парні", "4") and not bot.zp_match("2-10 парні", "5") and bot.zp_match("33а", "33А")
            and bot.zp_match("108–136", "120") and not bot.zp_match("108–136", "121"))
    step("Zaporizhzhia: house ranges", lambda: tg.out.append(("send", 1, "OK" if m_ok else "BAD", None)), lambda s: s == "OK")
    step("Zaporizhzhia: 'Абрикосова' -> asks the house", lambda: bot.text(ZA, "Абрикосова"), lambda s: "Абрикосова" in s)
    step("Zaporizhzhia: house 15 -> queue 3.1", lambda: bot.text(ZA, "15"), lambda s: "Черга 3.1" in s and "Абрикосова, 15" in s)
    step("Zaporizhzhia: district 'Сонячне, Ясна' -> house 13 -> 1.2", lambda: (bot.text(ZA, "Сонячне, Ясна"), bot.text(ZA, "13")),
         lambda s: "Черга 1.2" in s and "Ясна, 13" in s)

    # Chernivtsi region: дільниця -> town -> street -> house from the operator's published lists
    CV = 7970
    bot.user(CV, "uk")
    bot.set_user(CV, region="chernivtsi")
    cbc = lambda data: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": CV}, "message_id": 9}, "data": data})  # noqa: E731
    step("Chernivtsi: дільниці", lambda: cbc("MK"), lambda s: "Чернівціобленерго" in s)
    cf = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"] == "ЧЕРНІВЦІ"), None)
    if cf:
        step("Chernivtsi: city -> streets", lambda: cbc(f"MKF:{cf}"), lambda s: "вулицю" in s)
        step("Chernivtsi: type 'ГЕРОЇВ КРУТ'", lambda: bot.text(CV, "героїв крут"), lambda s: "вулицю" in s)
        cs = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if "ГЕРОЇВ КРУТ" in b["text"]), None)
        if cs:
            step("Chernivtsi: street -> houses", lambda: cbc(f"MKI:{cs}"), lambda s: "ГЕРОЇВ КРУТ" in s and "будин" in s)
            step("Chernivtsi: house 10 -> group 4", lambda: bot.text(CV, "10"), lambda s: "Черга 4</b>" in s and "ГЕРОЇВ КРУТ, 10" in s)
    ok &= bool(cf)

    # Zhytomyr region: РЕМ -> town -> street -> house on unhooking-search.php
    Z = 7960
    bot.user(Z, "uk")
    bot.set_user(Z, region="zhytomyr")
    cbz = lambda data: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": Z}, "message_id": 8}, "data": data})  # noqa: E731
    step("Zhytomyr: РЕМи", lambda: cbz("MK"), lambda s: "Житомиробленерго" in s)
    zf = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"] == "Житомирський"), None)
    if zf:
        step("Zhytomyr: РЕМ -> towns", lambda: cbz(f"MKF:{zf}"), lambda s: "населений пункт" in s)
        step("Zhytomyr: type 'Житомир'", lambda: bot.text(Z, "Житомир"), lambda s: "населений пункт" in s)
        zt_town = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"] == "місто Житомир"), None)
        if zt_town:
            step("Zhytomyr: city -> streets", lambda: cbz(f"MKI:{zt_town}"), lambda s: "вулицю" in s)
            step("Zhytomyr: type 'Київська'", lambda: bot.text(Z, "Київська"), lambda s: "вулицю" in s)
            zs = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"] == "вул. Київська"), None)
            if zs:
                step("Zhytomyr: street -> houses", lambda: cbz(f"MKI:{zs}"), lambda s: "Київська" in s and "будин" in s)
                step("Zhytomyr: house 102 -> queue 5.2", lambda: bot.text(Z, "102"), lambda s: "Черга 5.2" in s and "Київська, 102" in s)
    ok &= bool(zf)

    # Volyn region: energy.volyn.ua 'queue by address' form, town + street -> houses -> queue
    VO = 7950
    bot.user(VO, "uk")
    bot.set_user(VO, region="volyn")
    step("Volyn: queue picker explains the address search", lambda: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": VO}, "message_id": 7}, "data": "rg:volyn"}),
         lambda s: any("Луцьк, Волі" in o[2] for o in tg.out))
    step("Volyn: 'Луцьк, Волі' -> houses", lambda: bot.text(VO, "Луцьк, Волі"), lambda s: "Волі" in s and "будин" in s)
    step("Volyn: house 11 -> queue 4.2", lambda: bot.text(VO, "11"), lambda s: "Черга 4.2" in s and "Луцьк, Волі, 11" in s)

    # Poltava region: Poltavaoblenergo's directory, branch -> town -> street -> house
    P = 7900
    bot.user(P, "uk")
    bot.set_user(P, region="poltava")
    cbp = lambda data: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": P}, "message_id": 6}, "data": data})  # noqa: E731
    step("Poltava: queue picker has the address button", lambda: cbp("rg:poltava"),
         lambda s: any(b.get("callback_data") == "MK" for r in tg.out[-1][3] for b in r))
    step("Poltava: branches", lambda: cbp("MK"), lambda s: "Полтаваобленерго" in s)
    pfid = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"].startswith("Полтавська об")), None)
    if pfid:
        step("Poltava: branch -> towns", lambda: cbp(f"MKF:{pfid}"), lambda s: "населений пункт" in s or "вулицю" in s)
        step("Poltava: type 'Полтава'", lambda: bot.text(P, "Полтава"), lambda s: "населений пункт" in s)
        tid = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if b["text"] == "місто Полтава"), None)
        if tid:
            step("Poltava: city -> streets", lambda: cbp(f"MKI:{tid}"), lambda s: "вулицю" in s)
            step("Poltava: type 'Соборності'", lambda: bot.text(P, "Соборності"), lambda s: "вулицю" in s)
            psid = next((b["callback_data"][4:] for r in (tg.out[-1][3] or []) for b in r if "Соборності" in b["text"]), None)
            if psid:
                step("Poltava: street -> houses", lambda: cbp(f"MKI:{psid}"), lambda s: "Соборності" in s)
                step("Poltava: house 10 -> queue", lambda: bot.text(P, "10"), lambda s: "Черга" in s and "Соборності" in s and ", 10" in s)
    ok &= bool(pfid)

    # links that open a queue, and the share button
    step("link ?start=q4_1 opens queue 4.1", lambda: bot.text(5151, "/start q4_1", "uk"), lambda s: "Черга 4.1" in s)
    share = [b.get("url", "") for r in tg.out[-1][3] for b in r if b["text"].startswith("📤")]
    share_ok = bool(share) and share[0].startswith("https://t.me/share/url?") and "start%3Dq4_1" in share[0]
    step("share button has a ready link", lambda: tg.out.append(("send", 5151, "OK" if share_ok else "BAD " + str(share), None)),
         lambda s: s == "OK")
    step("link ?start=odesa_q3_2 opens Odesa 3.2", lambda: bot.text(5151, "/start odesa_q3_2", "uk"), lambda s: "Черга 3.2" in s)
    step("bad link -> normal welcome", lambda: bot.text(5252, "/start evil_q1_1", "uk"), lambda s: "Привіт" in s)

    # people's reports: two people in Kyiv queue 4.1 say "no power" while the schedule says it's on
    seq41 = slots(days[0][1].get("GPV4.1"))
    j_on = next(i for i in range(2, 46) if seq41[i] == "+" and seq41[i + 1] == "+")
    T0 = today_ts + j_on * 1800 + 60
    A, B, D = 9001, 9002, 9003
    for cid in (A, B, D):
        bot.user(cid, "uk")
        bot.add_address(cid, "Черга 4.1", "4.1", "kyiv")
    step("report: first 'no power'", lambda: tg.out.append(("send", A, bot.report(A, bot.user(A), "off", now=T0), None)),
         lambda s: "Врахували" in s)
    step("report: same again too soon", lambda: tg.out.append(("send", A, bot.report(A, bot.user(A), "off", now=T0 + 30), None)),
         lambda s: "вже повідомили" in s)
    step("one report: just a count, no alarm", lambda: tg.out.append(("send", A, bot.status_view(bot.user(A), now=T0)[0].split("\n\n")[-1], None)),
         lambda s: "⚫ 1" in s)
    bot.report(B, bot.user(B), "off", now=T0 + 60)
    step("two reports: 'looks like an emergency'", lambda: tg.out.append(("send", A, bot.status_view(bot.user(A), now=T0 + 90)[0].split("\n\n")[-1], None)),
         lambda s: "аварійне" in s and "2 людини" in s)
    print("     buttons:", [b["text"] for r in bot.status_view(bot.user(A), now=T0)[1] for b in r])
    step("queue alert: subscribers who didn't report get it, reporters don't", lambda: bot.notify_tick(now=T0 + 120),
         lambda s: any(o[1] == D and "Можливе аварійне" in o[2] for o in tg.out)
         and not any(o[1] in (A, B) and "Можливе аварійне" in o[2] for o in tg.out))
    step("no repeat within the hour", lambda: bot.notify_tick(now=T0 + 300), lambda s: "Можливе аварійне" not in s)
    step("report expires after an hour", lambda: tg.out.append(("send", A, bot.crowd_line("kyiv", "4.1", "uk", now=T0 + 3700), None)),
         lambda s: s == "")
    step("button press via callback", lambda: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": D}, "message_id": 5}, "data": "rp:on"}),
         lambda s: "Черга 4.1" in s)

    # report to DTEK: official contacts + ready-made text; the person sends it
    step("status has 'Report to DTEK'", lambda: tg.out.append(("send", A, " ".join(b["text"] for r in bot.status_view(bot.user(A))[1] for b in r), None)),
         lambda s: "Повідомити ДТЕК" in s)
    step("DTEK view: Kyiv contacts + text", lambda: bot.callback({"id": "x", "from": {}, "message": {"chat": {"id": A}, "message_id": 6}, "data": "dtek"}),
         lambda s: "Повідомити ДТЕК Київські електромережі" in s)
    print("     ", [b.get("url") for r in tg.out[-1][3] for b in r if b.get("url")])
    print("     ", [ln for ln in tg.out[-1][2].split("\n") if ln.startswith(("📞", "<code>"))])
    for rg, ph in (("kyiv-region", "0 800 400 740"), ("dnipro", "0 800 301 497"), ("odesa", "0 800 409 090")):
        E = 9100 + len(rg)
        bot.user(E, "uk")
        bot.set_user(E, region=rg)
        bot.add_address(E, "Черга 1.1", "1.1", rg)
        v = bot.dtek_view(bot.user(E))[0]
        good = ph in v and "[вкажіть адресу]" in v and "1551" not in v
        step(f"DTEK view: {rg} ({ph}, no 1551)", lambda: tg.out.append(("send", E, "OK" if good else "BAD " + v[-200:], None)),
             lambda s: s == "OK")
    print("\nALL PASS" if ok else "\nSOME FAILED")
    return ok


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--test":
        sys.exit(0 if self_test() else 1)
    if not TOKEN:
        sys.exit("BOT_TOKEN is not set. Put it in .env next to bot.py (BOT_TOKEN=123456:ABC...)")
    database = DB(os.path.join(DATA_DIR, "bot.db"))
    database.migrate_json(os.path.join(DATA_DIR, "users.json"))
    Bot(database, Telegram(TOKEN)).run()
