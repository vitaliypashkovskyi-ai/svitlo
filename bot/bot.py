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
    SITE_URL        website link (default GitHub Pages)
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
SITE_URL = os.environ.get("SITE_URL", "https://vitaliypashkovskyi-ai.github.io/svitlo/")
DATA_BASE = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/"
YASNO = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2"
# Regions with DTEK schedules. yasno: (regionId, dsoId) for address search; cities: search asks for the town first.
REGIONS = {
    "kyiv": {"uk": "м. Київ", "en": "Kyiv city", "yasno": ("25", "902"), "cities": False,
             "dtek": "https://www.dtek-kem.com.ua/ua/shutdowns"},
    "kyiv-region": {"uk": "Київська область", "en": "Kyiv region", "yasno": None, "cities": False,
                    "dtek": "https://www.dtek-krem.com.ua/ua/shutdowns"},
    "dnipro": {"uk": "Дніпропетровська область", "en": "Dnipropetrovsk region", "yasno": ("3", "301"), "cities": True,
               "dtek": "https://www.dtek-dnem.com.ua/ua/shutdowns"},
    "odesa": {"uk": "Одеська область", "en": "Odesa region", "yasno": None, "cities": False,
              "dtek": "https://www.dtek-oem.com.ua/ua/shutdowns"},
}

SCHEDULE_TTL = 300            # refresh the schedule every 5 min
STALE_AFTER = 3600            # alert the admin if data is older than 1 h
MAX_ADDRESSES = 5
WARN_CHOICES = (15, 30, 60)
QUIET_FROM, QUIET_TO = 23 * 60, 7 * 60
RETURN_WINDOW = 10            # "power is back" message within 10 min after the scheduled time
CACHE_TTL = {"cities": 7 * 86400, "streets": 86400, "houses": 7 * 86400, "group": 7 * 86400}
SEND_RATE = 25                # messages per second, below Telegram's ~30/s limit
# Self-update: once an hour the bot compares these files with the GitHub repo, runs the new
# version's self-test and only then switches to it. AUTO_UPDATE=0 in .env turns it off.
AUTO_UPDATE = os.environ.get("AUTO_UPDATE", "1").strip() != "0"
UPDATE_URL = os.environ.get("UPDATE_URL", "https://raw.githubusercontent.com/vitaliypashkovskyi-ai/svitlo/main/bot/")
UPDATE_FILES = ("bot.py", "streets.json")
UPDATE_EVERY = 3600
BOT_VERSION = "2026-11-11.4"  # bump on every release; shown in the log at start
STREETS_FILE = os.environ.get("STREETS_FILE", os.path.join(HERE, "streets.json"))
LETTERS = "АБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЮЯ"
PAGE = 10                     # streets per page in the list


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

# ===================================================================== texts
T = {
    "uk": {
        "welcome": "Привіт! Я показую графік відключень світла ДТЕК у Києві.\n\nНапишіть назву вашої вулиці українською, наприклад <b>Хрещатик</b>, і я знайду вашу чергу.\n\nАбо одразу вкажіть чергу: <code>/queue 4.1</code>",
        "ask_street": "Напишіть назву вулиці українською, наприклад <b>Хрещатик</b>.",
        "help": "Що я вмію:\n/now — графік зараз\n/address — мої адреси (до 5)\n/region — вибрати регіон\n/settings — нагадування й мова\n/queue 4.1 — додати чергу вручну\n\nНапишіть назву вулиці, щоб додати адресу.",
        "b_region": "📍 Регіон: {r}", "pick_region": "Виберіть регіон. Графіки є для мереж ДТЕК у цих регіонах:",
        "region_set": "Регіон: <b>{r}</b>.",
        "region_no_search": "Пошук за адресою для регіону «{r}» недоступний. Дізнайтеся свою чергу на <a href=\"{url}\">сайті ДТЕК</a> і надішліть її, наприклад <code>/queue 3.2</code>",
        "ask_city": "Напишіть назву населеного пункту, наприклад <b>Дніпро</b>.\n<i>Пошук працює для адрес ДТЕК Дніпровські електромережі.</i>",
        "pick_city": "Виберіть населений пункт:", "no_cities": "Не знайшов такого населеного пункту. Напишіть хоча б 3 літери назви.",
        "ask_street_in": "<b>{city}</b>\nТепер напишіть назву вулиці.",
        "short": "Напишіть хоча б 3 літери назви вулиці або виберіть її зі списку.",
        "b_list": "🔤 Вибрати зі списку", "b_letters": "🔤 Літери",
        "pick_letter": "Виберіть першу літеру назви вулиці.\n<i>Наприклад, для «просп. Гонгадзе Георгія» — літера Г.</i>",
        "letter_page": "Вулиці на «{l}» · сторінка {p} з {n}",
        "no_streets": "Не знайшов такої вулиці. Спробуйте без «вул.» або лише частину назви.",
        "pick_street": "Виберіть вулицю:",
        "ask_house": "<b>{street}</b>\nТепер напишіть номер будинку, наприклад <code>22</code>.",
        "no_houses": "Не знайшов будинок «{q}» на цій вулиці. Спробуйте лише номер, без літери чи корпусу.",
        "pick_house": "Виберіть будинок:",
        "no_group": "Для цієї адреси YASNO не вказує чергу. Спробуйте сусідній будинок або вкажіть чергу: <code>/queue 4.1</code>",
        "yasno_err": "Сервіс пошуку адрес зараз не відповідає. Спробуйте пізніше або вкажіть чергу: <code>/queue 4.1</code>",
        "too_many": "Можна зберегти до {n} адрес. Видаліть одну в /address.",
        "need_addr": "Спершу додайте адресу — напишіть назву вулиці. Або чергу: <code>/queue 4.1</code>",
        "bad_queue": "Не знаю такої черги. Приклад: <code>/queue 4.1</code>",
        "queue_label": "Черга {q}",
        "no_data": "Не вдалося завантажити графік. Спробуйте за кілька хвилин.",
        "title": "Черга {q}",
        "on_now": "🟢 Зараз світло є", "off_now": "⚫ Зараз світла немає", "maybe_now": "🟡 Зараз можливе відключення",
        "next_off": "відключення о <b>{t}</b> (через {left})", "next_on": "увімкнуть о <b>{t}</b> (через {left})",
        "next_maybe": "можливе відключення о <b>{t}</b> (через {left})", "tomorrow_word": "завтра ",
        "no_change": "змін до кінця графіка немає",
        "today": "Сьогодні", "tomorrow": "Завтра", "no_outages": "відключень немає", "off_total": "без світла {d}",
        "not_published": "графік ще не опубліковано", "maybe": "можливо",
        "updated_at": "Оновлено {t}",
        "dur": lambda h, m: f"{h} год {m:02d} хв" if h else f"{m} хв",
        # buttons
        "b_refresh": "🔄 Оновити", "b_site": "🌐 Сайт", "b_addr": "🏠 Адреси", "b_settings": "⚙️ Налаштування",
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
        "commands": [("now", "Графік зараз"), ("address", "Мої адреси"), ("region", "Вибрати регіон"), ("settings", "Нагадування й мова"),
                     ("queue", "Додати чергу вручну, напр. /queue 4.1"), ("help", "Допомога")],
    },
    "en": {
        "welcome": "Hi! I show DTEK's power outage schedule for Kyiv.\n\nType your street name in Ukrainian, for example <b>Хрещатик</b>, and I'll find your queue.\n\nOr set the queue directly: <code>/queue 4.1</code>",
        "ask_street": "Type the street name in Ukrainian, for example <b>Хрещатик</b>.",
        "help": "What I can do:\n/now — schedule now\n/address — my addresses (up to 5)\n/region — choose the region\n/settings — reminders and language\n/queue 4.1 — add a queue manually\n\nType a street name to add an address.",
        "b_region": "📍 Region: {r}", "pick_region": "Pick your region. Schedules are available for DTEK networks in these regions:",
        "region_set": "Region: <b>{r}</b>.",
        "region_no_search": "Address search isn't available for {r}. Look up your queue on the <a href=\"{url}\">DTEK website</a> and send it, for example <code>/queue 3.2</code>",
        "ask_city": "Type the town or village name in Ukrainian, for example <b>Дніпро</b>.\n<i>Search covers addresses served by DTEK Dnipro Electric Networks.</i>",
        "pick_city": "Pick the town or village:", "no_cities": "No such town found. Type at least 3 letters of the name.",
        "ask_street_in": "<b>{city}</b>\nNow type the street name.",
        "short": "Type at least 3 letters of the street name, or pick it from the list.",
        "b_list": "🔤 Pick from the list", "b_letters": "🔤 Letters",
        "pick_letter": "Pick the first letter of the street name.\n<i>For example, «просп. Гонгадзе Георгія» is under Г.</i>",
        "letter_page": "Streets starting with «{l}» · page {p} of {n}",
        "no_streets": "No such street found. Try in Ukrainian, without «вул.», or just part of the name.",
        "pick_street": "Pick your street:",
        "ask_house": "<b>{street}</b>\nNow type the house number, for example <code>22</code>.",
        "no_houses": "House «{q}» not found on this street. Try just the number, without a letter or block.",
        "pick_house": "Pick your house:",
        "no_group": "YASNO lists no queue for this address. Try a neighbouring house or set the queue: <code>/queue 4.1</code>",
        "yasno_err": "The address service isn't responding. Try later or set the queue: <code>/queue 4.1</code>",
        "too_many": "You can save up to {n} addresses. Delete one in /address.",
        "need_addr": "First add an address — type a street name. Or a queue: <code>/queue 4.1</code>",
        "bad_queue": "Unknown queue. Example: <code>/queue 4.1</code>",
        "queue_label": "Queue {q}",
        "no_data": "Couldn't load the schedule. Try again in a few minutes.",
        "title": "Queue {q}",
        "on_now": "🟢 Power is on now", "off_now": "⚫ Power is off now", "maybe_now": "🟡 Possible outage now",
        "next_off": "outage at <b>{t}</b> (in {left})", "next_on": "back on at <b>{t}</b> (in {left})",
        "next_maybe": "possible outage at <b>{t}</b> (in {left})", "tomorrow_word": "tomorrow ",
        "no_change": "no changes until the end of the schedule",
        "today": "Today", "tomorrow": "Tomorrow", "no_outages": "no outages", "off_total": "{d} without power",
        "not_published": "not published yet", "maybe": "possible",
        "updated_at": "Updated {t}",
        "dur": lambda h, m: f"{h} h {m:02d} min" if h else f"{m} min",
        "b_refresh": "🔄 Refresh", "b_site": "🌐 Website", "b_addr": "🏠 Addresses", "b_settings": "⚙️ Settings",
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
        "commands": [("now", "Schedule now"), ("address", "My addresses"), ("region", "Choose the region"), ("settings", "Reminders and language"),
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
_scheds = {r: {"at": 0, "ok_at": 0, "data": None} for r in REGIONS}
_sched = _scheds["kyiv"]  # kept for the self-test
_sched_lock = threading.Lock()


def schedule(region="kyiv", force=False):
    sc = _scheds[region]
    with _sched_lock:
        if force or time.time() - sc["at"] > SCHEDULE_TTL or not sc["data"]:
            sc["at"] = time.time()
            try:
                sc["data"] = http_json(DATA_BASE + f"{region}.json?t={int(time.time())}", timeout=30)
                sc["ok_at"] = time.time()
            except Exception as e:
                log("schedule fetch failed:", region, e)
                sc["at"] = time.time() - SCHEDULE_TTL + 60  # retry in a minute
        return sc["data"]


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
    a, b = queue.split(".")
    return f"{SITE_URL}#{'' if region == 'kyiv' else region + '-'}q{a}-{b}"


def status_text(queue, lang, label, now=None, region="kyiv"):
    t = T[lang]
    now = now or time.time()
    data = schedule(region)
    if not data:
        return t["no_data"]
    days = days_from_today(data, now)
    key = queue_key(queue)
    head = f"<b>{t['title'].format(q=queue)}</b>"
    if label and label != T[lang]["queue_label"].format(q=queue):
        head += f" · {html.escape(label)}"
    if region != "kyiv":
        head += f"\n<i>{region_name(region, lang)}</i>"
    lines = [head]
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
            state += " — " + t[nxt].format(t=(t["tomorrow_word"] if j >= 48 else "") + fmt(j * 30),
                                           left=fmt_left(j * 30 - now_min))
        else:
            state += " — " + t["no_change"]
        lines += [state, ""]
    for name, day in ((t["today"], today), (t["tomorrow"], next((d for d in days if d[0] > now), None))):
        if not day:
            lines.append(f"<b>{name}:</b> {t['not_published']}")
            continue
        s = slots(day[1].get(key))
        iv = intervals(s)
        off = s.count("-") * 30
        lines.append(f"<b>{name}:</b> " + (t["off_total"].format(d=t["dur"](off // 60, off % 60)) if iv else t["no_outages"]))
        for kind, a, b in iv:
            lines.append(f"  {'▪️' if kind == '-' else '▫️'} {fmt(a)}–{'24:00' if b == 1440 else fmt(b)}"
                         + (f" ({t['maybe']})" if kind == "?" else ""))
    if today:
        lines += ["", f"<i>{t['updated_at'].format(t=fmt((now - today[0]) / 60))}</i>"]
    return "\n".join(lines)


# ===================================================================== telegram sending (rate-limited)
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
    def status_view(self, u, now=None):
        a = self.active_address(u)
        t = T[u["lang"]]
        if not a:
            return t["need_addr"], None
        buttons = [[{"text": t["b_refresh"], "callback_data": "r"}, {"text": t["b_site"], "url": site_link(a["region"], a["queue"])}],
                   [{"text": t["b_addr"], "callback_data": "a"}, {"text": t["b_settings"], "callback_data": "st"}]]
        return status_text(a["queue"], u["lang"], a["label"], now, a["region"]), buttons

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
            rows.append([{"text": t["b_add"], "callback_data": "aa"}])
        rows.append([{"text": t["b_back"], "callback_data": "r"}])
        text = t["addr_title"].format(n=MAX_ADDRESSES) + ("" if len(rows) > 2 else "\n\n" + t["addr_empty"])
        return text, rows

    def list_button(self, u):
        rows = [[{"text": T[u["lang"]]["b_list"], "callback_data": "L"}]] if STREETS and u["region"] == "kyiv" else []
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
            return self.tg.send(chat_id, t["region_no_search"].format(r=region_name(u["region"], u["lang"]), url=R["dtek"]),
                                [[{"text": t["b_region"].format(r=region_name(u["region"], u["lang"])), "callback_data": "rg"}]])
        self.tg.send(chat_id, t["ask_city"] if R["cities"] else t["ask_street"], self.list_button(u))

    def letters_view(self, u):
        letters = [l for l in LETTERS if l in STREETS] + (["0–9"] if "0–9" in STREETS else [])
        rows = [[{"text": l, "callback_data": f"L:{l}:1"} for l in letters[i:i + 6]] for i in range(0, len(letters), 6)]
        return T[u["lang"]]["pick_letter"], rows

    def letter_page_view(self, u, letter, page):
        t = T[u["lang"]]
        items = STREETS.get(letter, [])
        pages = max(1, (len(items) + PAGE - 1) // PAGE)
        page = min(max(1, page), pages)
        rows = [[{"text": value[:60], "callback_data": f"ls:{sid}"}] for _, sid, value in items[(page - 1) * PAGE: page * PAGE]]
        nav = []
        if page > 1:
            nav.append({"text": "◀️", "callback_data": f"L:{letter}:{page - 1}"})
        nav.append({"text": f"{page}/{pages}", "callback_data": "x"})
        if page < pages:
            nav.append({"text": "▶️", "callback_data": f"L:{letter}:{page + 1}"})
        rows += [nav, [{"text": t["b_letters"], "callback_data": "LL"}]]
        return t["letter_page"].format(l=letter, p=page, n=pages), rows

    def settings_view(self, u):
        t = T[u["lang"]]
        onoff = lambda v: t["on"] if v else t["off"]  # noqa: E731
        rows = [[{"text": f"{t['s_notify']}: {onoff(u['notify'])}", "callback_data": "sn"}],
                [{"text": ("✓ " if u["warn_min"] == m else "") + t["s_min"].format(m=m), "callback_data": f"sw:{m}"} for m in WARN_CHOICES],
                [{"text": f"{t['s_ret']}: {onoff(u['ret'])}", "callback_data": "sr"}],
                [{"text": f"{t['s_quiet']}: {onoff(u['quiet'])}", "callback_data": "sq"}],
                [{"text": t["b_region"].format(r=region_name(u["region"], u["lang"])), "callback_data": "rg"}],
                [{"text": t["s_lang"], "callback_data": "sl"}],
                [{"text": t["b_back"], "callback_data": "r"}]]
        return f"{t['set_title']}\n\n{t['s_before']}", rows

    # ---------- incoming
    def chat_lock(self, chat_id):
        with self.locks_lock:
            return self.chat_locks.setdefault(chat_id, threading.Lock())

    def handle(self, upd):
        if "callback_query" in upd:
            cq = upd["callback_query"]
            if "message" not in cq:
                return
            chat_id = cq["message"]["chat"]["id"]
            with self.chat_lock(chat_id):
                self.callback(cq)
        elif "message" in upd and "text" in upd["message"]:
            m = upd["message"]
            with self.chat_lock(m["chat"]["id"]):
                self.text(m["chat"]["id"], m["text"], m.get("from", {}).get("language_code"))

    def text(self, chat_id, msg, lang_code=None):
        u = self.user(chat_id, lang_code)
        t = T[u["lang"]]
        m = msg.strip()
        cmd, _, arg = m.partition(" ")
        cmd = cmd.split("@")[0].lower()
        arg = arg.strip()
        if cmd == "/start":
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
        elif cmd in ("/address", "/addresses"):
            if arg:
                self.set_state(chat_id, {})
                self.free_text(chat_id, self.user(chat_id), arg)
            else:
                self.tg.send(chat_id, *self.addr_view(u))
        elif cmd == "/settings":
            self.tg.send(chat_id, *self.settings_view(u))
        elif cmd == "/queue":
            q = norm_queue(arg)
            data = schedule(u["region"])
            days = days_from_today(data, time.time()) if data else []
            if not q or not days or queue_key(q) not in days[0][1]:
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
        elif cmd.startswith("/"):
            self.tg.send(chat_id, t["help"])
        else:
            self.free_text(chat_id, u, m)

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
        if not R["yasno"]:
            return self.ask_address(chat_id, u)
        if st.get("street"):
            return self.search_houses(chat_id, u, m)
        if R["cities"] and not st.get("city"):
            return self.search_cities(chat_id, u, m)
        return self.search_streets(chat_id, u, m)

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
        st = self.state(u)
        name = st.get("streets", {}).get(sid)
        if not name:
            return
        st["street"] = {"id": sid, "name": name}
        self.set_state(chat_id, st)
        text = T[u["lang"]]["ask_house"].format(street=html.escape(name))
        if message_id:
            self.tg.edit(chat_id, message_id, text)
        else:
            self.tg.send(chat_id, text)

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
            n = "".join(ch for ch in v[:6] if ch.isdigit()) if v[:1].isdigit() else ""
            return (int(n) if n else 10 ** 9, v)
        res = sorted(res, key=natural)
        st["houses"] = {str(h["id"]): h["value"] for h in res[:32]}
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
        try:
            self.tg.call("answerCallbackQuery", timeout=10, callback_query_id=cq["id"])
        except Exception:
            pass
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
        if kind == "r":
            return self.tg.edit(chat_id, mid, *self.status_view(u))
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
        if kind == "LL":  # back to letters from inside the list: edit in place
            return self.tg.edit(chat_id, mid, *self.letters_view(u))
        if kind == "L":
            if not val:  # from a text message (welcome, "not found"): keep that text, open the list below
                return self.tg.send(chat_id, *self.letters_view(u))
            letter, _, page = val.rpartition(":")
            return self.tg.edit(chat_id, mid, *self.letter_page_view(u, letter, int(page) if page.isdigit() else 1))
        if kind == "ls":
            name = STREET_BY_ID.get(val)
            if not name:
                return
            self.set_state(chat_id, {"streets": {val: name}})
            return self.pick_street(chat_id, self.user(chat_id), val, mid)
        if kind == "x":
            return
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
                        self.tg.send(p["chat_id"], p["text"])
                        self.db.q("DELETE FROM pending WHERE chat_id=? AND key=?", (p["chat_id"], p["key"]))
        self.db.q("DELETE FROM sent WHERE at < ?", (int(now) - 3 * 86400,))

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
            back = next((j for j in range(max(1, idx - 1), idx + 1)
                         if seq[j] == "+" and seq[j - 1] == "-" and 0 <= now_min - j * 30 < RETURN_WINDOW), None)

            subs = self.db.q("""SELECT a.id AS aid, a.label, u.* FROM addresses a JOIN users u ON u.chat_id=a.chat_id
                                WHERE a.queue=? AND a.region=? AND u.notify=1 AND u.blocked=0""", (qn, region))
            for s in subs:
                t = T[s["lang"]]
                cid = s["chat_id"]
                user_quiet = s["quiet"] and quiet_now
                tag = "\n" + t["for_addr"].format(label=html.escape(s["label"]), q=qn)
                if mins_to_off is not None and 0 < mins_to_off <= s["warn_min"]:
                    if self.mark(cid, f"warn:{s['aid']}:{today_ts}:{nxt_off}") and not user_quiet:
                        self.tg.send(cid, t["warn"].format(m=int(round(mins_to_off)), t=fmt(nxt_off * 30)) + tag)
                if back is not None and s["ret"]:
                    if self.mark(cid, f"ret:{s['aid']}:{today_ts}:{back}") and not user_quiet:
                        self.tg.send(cid, t["ret"].format(t=fmt(back * 30)) + tag)
                for ev, mark in events:
                    if not self.mark(cid, f"{mark}:{s['aid']}"):
                        continue
                    text = t[ev] + "\n\n" + status_text(qn, s["lang"], s["label"], now, region)
                    if user_quiet:  # deliver at 07:00; keep only the latest version per address
                        self.db.q("INSERT OR REPLACE INTO pending(chat_id,key,text,at) VALUES(?,?,?,?)",
                                  (cid, f"{ev[:3]}:{s['aid']}", text, int(now)))
                    else:
                        self.tg.send(cid, text)


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
        log(f"Bot @{me['username']} started, version {BOT_VERSION}, auto-update {'on' if AUTO_UPDATE else 'off'}")
        for lang in ("uk", "en"):
            try:
                self.tg.call("setMyCommands", commands=[{"command": c, "description": d} for c, d in T[lang]["commands"]],
                             **({"language_code": "en"} if lang == "en" else {}))
            except Exception as e:
                log("setMyCommands failed:", e)
        threading.Thread(target=self.notifier, daemon=True).start()
        if AUTO_UPDATE:
            threading.Thread(target=self.updater, daemon=True).start()
        pool = ThreadPoolExecutor(max_workers=8)  # a slow YASNO answer never blocks other people
        offset = 0
        while True:
            try:
                res = self.tg.call("getUpdates", offset=offset, timeout=50,
                                   allowed_updates=["message", "callback_query"])["result"]
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
                log("update check failed:", name, e)
                return
        local = {}
        for name in UPDATE_FILES:
            try:
                with open(os.path.join(HERE, name), "rb") as f:
                    local[name] = f.read()
            except FileNotFoundError:
                local[name] = b""
        changed = [n for n in UPDATE_FILES if sha(new[n]) != sha(local[n])]
        if not changed:
            return
        version = sha(b"".join(new[n] for n in UPDATE_FILES))[:10]
        state_file = os.path.join(DATA_DIR, "update_state.json")
        try:
            with open(state_file, encoding="utf-8") as f:
                state = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            state = {}
        if version in state.get("failed", []):
            return  # this version already failed its self-test; wait for the next one
        log("update found:", ", ".join(changed), version)
        with tempfile.TemporaryDirectory() as tmp:
            for name in UPDATE_FILES:
                with open(os.path.join(tmp, name), "wb") as f:
                    f.write(new[name])
            env = {**os.environ, "BOT_TOKEN": "", "ADMIN_CHAT_ID": "", "DATA_DIR": os.path.join(tmp, "data"),
                   "STREETS_FILE": os.path.join(tmp, "streets.json")}
            try:
                res = subprocess.run([sys.executable, "-X", "utf8", os.path.join(tmp, "bot.py"), "--test"], cwd=tmp, env=env,
                                     capture_output=True, text=True, timeout=900)
                passed = res.returncode == 0
                tail = (res.stdout + res.stderr)[-600:]
            except Exception as e:
                passed, tail = False, repr(e)
            if not passed:
                state.setdefault("failed", []).append(version)
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(state_file, "w", encoding="utf-8") as f:
                    json.dump(state, f)
                log("update rejected, self-test failed:\n" + tail)
                self.admin(f"❌ Оновлення бота {version} не пройшло самоперевірку — працює попередня версія.\n<pre>{html.escape(tail[-400:])}</pre>")
                return
            for name in changed:
                dst = os.path.join(HERE, name)
                if os.path.exists(dst):
                    os.replace(dst, dst + ".bak")
                os.replace(os.path.join(tmp, name), dst)
        log("updated:", ", ".join(changed), version)
        self.admin(f"🔄 Бот оновлено ({', '.join(changed)}, версія {version}).")
        if "bot.py" in changed:
            log("restarting on the new version")
            os._exit(0)  # docker 'restart: unless-stopped' starts the new code
        global STREETS, STREET_BY_ID
        STREETS, STREET_BY_ID = load_streets()

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

    def call(self, *a, **k):
        return {}


def self_test():
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
    ADMIN_CHAT_ID = saved_admin
    if STREETS:
        step("list: letters", lambda: cb("L"), lambda s: "літеру" in s)
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
    step("Odesa: no address search", lambda: cb("rg:odesa"), lambda s: "недоступний" in s)
    step("Odesa: /queue 3.2", lambda: bot.text(C, "/queue 3.2"), lambda s: "Черга 3.2" in s)
    print("     ", tg.out[-1][2].split("\n")[1], "| site:", [b.get("url") for r in tg.out[-1][3] for b in r if b.get("url")])
    step("Dnipro: asks for town", lambda: cb("rg:dnipro"), lambda s: "населеного пункту" in s)
    step("Dnipro: town", lambda: bot.text(C, "Дніпро"), lambda s: "м. Дніпро" in s)
    step("Dnipro: street", lambda: bot.text(C, "Яворн"), lambda s: "Виберіть вулицю" in s or "Яворницького" in s)
    if tg.out and tg.out[-1][3]:
        sid_d = tg.out[-1][3][0][0]["callback_data"].split(":")[1]
        step("Dnipro: pick street", lambda: cb(f"s:{sid_d}"), lambda s: "Яворницького" in s)
    step("Dnipro: house 1", lambda: bot.text(C, "1"), lambda s: "Черга" in s or "Виберіть будинок" in s)
    if tg.out and tg.out[-1][2].startswith(("Виберіть будинок", "Pick")):
        hid_d = tg.out[-1][3][0][0]["callback_data"].split(":")[1]
        step("Dnipro: pick house", lambda: cb(f"h:{hid_d}"), lambda s: "Черга" in s)
    print("     addresses:", [(a["label"], a["queue"], a["region"]) for a in bot.addresses(C)])
    step("reminders across regions run", lambda: bot.notify_tick(), lambda s: True)
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
