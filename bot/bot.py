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
SCHEDULE_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/kyiv.json"
YASNO = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2"
YASNO_PARAMS = {"regionId": "25", "dsoId": "902"}  # Київ, ДТЕК Київські електромережі

SCHEDULE_TTL = 300            # refresh the schedule every 5 min
STALE_AFTER = 3600            # alert the admin if data is older than 1 h
MAX_ADDRESSES = 5
WARN_CHOICES = (15, 30, 60)
QUIET_FROM, QUIET_TO = 23 * 60, 7 * 60
RETURN_WINDOW = 10            # "power is back" message within 10 min after the scheduled time
CACHE_TTL = {"streets": 86400, "houses": 7 * 86400, "group": 7 * 86400}
SEND_RATE = 25                # messages per second, below Telegram's ~30/s limit

# ===================================================================== texts
T = {
    "uk": {
        "welcome": "Привіт! Я показую графік відключень світла ДТЕК у Києві.\n\nНапишіть назву вашої вулиці українською, наприклад <b>Хрещатик</b>, і я знайду вашу чергу.\n\nАбо одразу вкажіть чергу: <code>/queue 4.1</code>",
        "ask_street": "Напишіть назву вулиці українською, наприклад <b>Хрещатик</b>.",
        "help": "Що я вмію:\n/now — графік зараз\n/address — мої адреси (до 5)\n/settings — нагадування й мова\n/queue 4.1 — додати чергу вручну\n\nНапишіть назву вулиці, щоб додати адресу.",
        "short": "Напишіть хоча б 2 літери назви вулиці.",
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
        "commands": [("now", "Графік зараз"), ("address", "Мої адреси"), ("settings", "Нагадування й мова"),
                     ("queue", "Додати чергу вручну, напр. /queue 4.1"), ("help", "Допомога")],
    },
    "en": {
        "welcome": "Hi! I show DTEK's power outage schedule for Kyiv.\n\nType your street name in Ukrainian, for example <b>Хрещатик</b>, and I'll find your queue.\n\nOr set the queue directly: <code>/queue 4.1</code>",
        "ask_street": "Type the street name in Ukrainian, for example <b>Хрещатик</b>.",
        "help": "What I can do:\n/now — schedule now\n/address — my addresses (up to 5)\n/settings — reminders and language\n/queue 4.1 — add a queue manually\n\nType a street name to add an address.",
        "short": "Type at least 2 letters of the street name.",
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
        "commands": [("now", "Schedule now"), ("address", "My addresses"), ("settings", "Reminders and language"),
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
_sched = {"at": 0, "ok_at": 0, "data": None}
_sched_lock = threading.Lock()


def schedule(force=False):
    with _sched_lock:
        if force or time.time() - _sched["at"] > SCHEDULE_TTL or not _sched["data"]:
            _sched["at"] = time.time()
            try:
                _sched["data"] = http_json(SCHEDULE_URL + f"?t={int(time.time())}", timeout=30)
                _sched["ok_at"] = time.time()
            except Exception as e:
                log("schedule fetch failed:", e)
                _sched["at"] = time.time() - SCHEDULE_TTL + 60  # retry in a minute
        return _sched["data"]


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


def status_text(queue, lang, label, now=None):
    t = T[lang]
    now = now or time.time()
    data = schedule()
    if not data:
        return t["no_data"]
    days = days_from_today(data, now)
    key = queue_key(queue)
    head = f"<b>{t['title'].format(q=queue)}</b>"
    if label and label != T[lang]["queue_label"].format(q=queue):
        head += f" · {html.escape(label)}"
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
            self.db.q("INSERT INTO users(chat_id, lang) VALUES(?,?)",
                      (chat_id, "en" if (lang_code or "").startswith("en") else "uk"))
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

    def add_address(self, chat_id, label, queue):
        u = self.user(chat_id)
        t = T[u["lang"]]
        same = self.db.one("SELECT id FROM addresses WHERE chat_id=? AND label=? AND queue=?", (chat_id, label, queue))
        if same:
            aid = same["id"]
        else:
            if len(self.addresses(chat_id)) >= MAX_ADDRESSES:
                self.tg.send(chat_id, t["too_many"].format(n=MAX_ADDRESSES))
                return None
            self.db.q("INSERT INTO addresses(chat_id,label,queue,created) VALUES(?,?,?,?)",
                      (chat_id, label, queue, int(time.time())))
            aid = self.db.one("SELECT max(id) AS id FROM addresses WHERE chat_id=?", (chat_id,))["id"]
        self.set_user(chat_id, active=aid, state=None)
        return aid

    def yasno(self, endpoint, **params):
        """YASNO lookup with a cache: fresh hits are instant; if YASNO is down, stale data is used."""
        key = endpoint + ":" + json.dumps(params, sort_keys=True, ensure_ascii=False)
        row = self.db.one("SELECT value, at FROM cache WHERE key=?", (key,))
        if row and time.time() - row["at"] < CACHE_TTL[endpoint]:
            return json.loads(row["value"])
        try:
            q = urllib.parse.urlencode({**YASNO_PARAMS, **params})
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
        qa, qb = a["queue"].split(".")
        buttons = [[{"text": t["b_refresh"], "callback_data": "r"}, {"text": t["b_site"], "url": f"{SITE_URL}#q{qa}-{qb}"}],
                   [{"text": t["b_addr"], "callback_data": "a"}, {"text": t["b_settings"], "callback_data": "st"}]]
        return status_text(a["queue"], u["lang"], a["label"], now), buttons

    def addr_view(self, u):
        t = T[u["lang"]]
        active = self.active_address(u)
        rows = []
        for a in self.addresses(u["chat_id"]):
            mark = "✓ " if active and a["id"] == active["id"] else ""
            rows.append([{"text": f"{mark}{a['label'][:40]} · {a['queue']}", "callback_data": f"as:{a['id']}"},
                         {"text": "🗑", "callback_data": f"ad:{a['id']}"}])
        if len(rows) < MAX_ADDRESSES:
            rows.append([{"text": t["b_add"], "callback_data": "aa"}])
        rows.append([{"text": t["b_back"], "callback_data": "r"}])
        text = t["addr_title"].format(n=MAX_ADDRESSES) + ("" if len(rows) > 2 else "\n\n" + t["addr_empty"])
        return text, rows

    def settings_view(self, u):
        t = T[u["lang"]]
        onoff = lambda v: t["on"] if v else t["off"]  # noqa: E731
        rows = [[{"text": f"{t['s_notify']}: {onoff(u['notify'])}", "callback_data": "sn"}],
                [{"text": ("✓ " if u["warn_min"] == m else "") + t["s_min"].format(m=m), "callback_data": f"sw:{m}"} for m in WARN_CHOICES],
                [{"text": f"{t['s_ret']}: {onoff(u['ret'])}", "callback_data": "sr"}],
                [{"text": f"{t['s_quiet']}: {onoff(u['quiet'])}", "callback_data": "sq"}],
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
                self.tg.send(chat_id, t["welcome"])
        elif cmd == "/help":
            self.tg.send(chat_id, t["help"])
        elif cmd == "/now":
            self.tg.send(chat_id, *self.status_view(u))
        elif cmd in ("/address", "/addresses"):
            if arg:
                self.set_state(chat_id, {})
                self.search_streets(chat_id, self.user(chat_id), arg)
            else:
                self.tg.send(chat_id, *self.addr_view(u))
        elif cmd == "/settings":
            self.tg.send(chat_id, *self.settings_view(u))
        elif cmd == "/queue":
            q = norm_queue(arg)
            data = schedule()
            days = days_from_today(data, time.time()) if data else []
            if not q or not days or queue_key(q) not in days[0][1]:
                self.tg.send(chat_id, t["bad_queue"])
            elif self.add_address(chat_id, t["queue_label"].format(q=q), q):
                self.tg.send(chat_id, *self.status_view(self.user(chat_id)))
        elif cmd == "/lang":
            self.set_user(chat_id, lang="en" if u["lang"] == "uk" else "uk")
            self.tg.send(chat_id, *self.settings_view(self.user(chat_id)))
        elif cmd == "/myid":
            self.tg.send(chat_id, t["myid"].format(id=chat_id))
        elif cmd.startswith("/"):
            self.tg.send(chat_id, t["help"])
        elif self.state(u).get("street"):
            self.search_houses(chat_id, u, m)
        else:
            self.search_streets(chat_id, u, m)

    def search_streets(self, chat_id, u, q):
        t = T[u["lang"]]
        if len(q) < 2:
            return self.tg.send(chat_id, t["short"])
        try:
            res = self.yasno("streets", query=q[:60])
        except Exception:
            return self.tg.send(chat_id, t["yasno_err"])
        if not res:
            return self.tg.send(chat_id, t["no_streets"])
        st = {"streets": {str(s["id"]): s["value"] for s in res[:30]}}
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
            res = self.yasno("houses", streetId=st["street"]["id"], query=q[:20])
        except Exception:
            return self.tg.send(chat_id, t["yasno_err"])
        if not res:
            if not any(ch.isdigit() for ch in q):  # probably a new street name
                self.set_state(chat_id, {})
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
            g = self.yasno("group", streetId=st["street"]["id"], houseId=hid)
        except Exception:
            return self.tg.send(chat_id, t["yasno_err"])
        if g.get("group") is None:
            return self.tg.send(chat_id, t["no_group"])
        queue = f"{g['group']}.{g.get('subgroup') or 1}"
        label = f"{st['street']['name']}, {house}"
        if not self.add_address(chat_id, label, queue):
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
            self.set_state(chat_id, {})
            return self.tg.send(chat_id, t["ask_street"])
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
        data = schedule()
        self.check_stale(data, now)
        if not data:
            return
        days = days_from_today(data, now)
        if not days or days[0][0] > now:
            return
        today_ts, groups_today = days[0]
        now_min = (now - today_ts) / 60
        quiet_now = now_min >= QUIET_FROM or now_min < QUIET_TO
        tomorrow = next((d for d in days if d[0] > now), None)
        queues = [r["queue"] for r in self.db.q("SELECT DISTINCT queue FROM addresses")]

        # one pass per queue (≤ 60), not per user
        for qn in queues:
            key = queue_key(qn)
            if key not in groups_today:
                continue
            seq = "".join(slots(g.get(key)) for _, g in days)
            today_sig = seq[:48]
            tom_sig = slots(tomorrow[1].get(key)) if tomorrow else None
            events = []

            row = self.db.one("SELECT * FROM queue_state WHERE queue=?", (qn,))
            if row:
                if row["day_ts"] == today_ts and row["today_sig"] != today_sig:
                    events.append(("changed_today", f"chg:{today_ts}:{today_sig}"))
                if tomorrow and row["tom_ts"] != tomorrow[0]:
                    events.append(("tomorrow_published", f"tom:{tomorrow[0]}"))
                elif tomorrow and row["tom_sig"] != tom_sig:
                    events.append(("tomorrow_changed", f"tomchg:{tomorrow[0]}:{tom_sig}"))
            self.db.q("INSERT OR REPLACE INTO queue_state(queue,day_ts,today_sig,tom_ts,tom_sig) VALUES(?,?,?,?,?)",
                      (qn, today_ts, today_sig, tomorrow[0] if tomorrow else None, tom_sig))

            idx = int(now_min // 30)
            nxt_off = next((j for j in range(idx + 1, len(seq)) if seq[j] == "-" and seq[j - 1] != "-"), None)
            mins_to_off = nxt_off * 30 - now_min if nxt_off is not None else None
            back = next((j for j in range(max(1, idx - 1), idx + 1)
                         if seq[j] == "+" and seq[j - 1] == "-" and 0 <= now_min - j * 30 < RETURN_WINDOW), None)

            subs = self.db.q("""SELECT a.id AS aid, a.label, u.* FROM addresses a JOIN users u ON u.chat_id=a.chat_id
                                WHERE a.queue=? AND u.notify=1 AND u.blocked=0""", (qn,))
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
                    text = t[ev] + "\n\n" + status_text(qn, s["lang"], s["label"], now)
                    if user_quiet:  # deliver at 07:00; keep only the latest version per address
                        self.db.q("INSERT OR REPLACE INTO pending(chat_id,key,text,at) VALUES(?,?,?,?)",
                                  (cid, f"{ev[:3]}:{s['aid']}", text, int(now)))
                    else:
                        self.tg.send(cid, text)

        if not quiet_now:  # flush what waited for the end of quiet hours
            for p in self.db.q("SELECT * FROM pending ORDER BY at"):
                self.tg.send(p["chat_id"], p["text"])
                self.db.q("DELETE FROM pending WHERE chat_id=? AND key=?", (p["chat_id"], p["key"]))
        self.db.q("DELETE FROM sent WHERE at < ?", (int(now) - 3 * 86400,))

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
        log(f"Bot @{me['username']} started")
        for lang in ("uk", "en"):
            try:
                self.tg.call("setMyCommands", commands=[{"command": c, "description": d} for c, d in T[lang]["commands"]],
                             **({"language_code": "en"} if lang == "en" else {}))
            except Exception as e:
                log("setMyCommands failed:", e)
        threading.Thread(target=self.notifier, daemon=True).start()
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
