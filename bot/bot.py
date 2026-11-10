"""Світло Київ — Telegram bot.

Finds a DTEK Kyiv outage queue by address (YASNO), shows today's and tomorrow's
schedule, and sends reminders 30 minutes before an outage and when tomorrow's
schedule is published. Standard library only — no pip install needed.

    BOT_TOKEN=... python bot.py              # run the bot (long polling, no open ports)
    python bot.py --test "Хрещатик" 22       # try search + messages without Telegram

Settings come from environment variables (or a .env file next to this script):
    BOT_TOKEN   token from @BotFather (required to run the bot)
    DATA_DIR    where users.json is kept (default: ./data)
    SITE_URL    link to the website (default: GitHub Pages)
"""
import html
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


def load_env():
    path = os.path.join(HERE, ".env")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(HERE, "data"))
SITE_URL = os.environ.get("SITE_URL", "https://vitaliypashkovskyi-ai.github.io/svitlo/")
SCHEDULE_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/kyiv.json"
YASNO = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2"
YASNO_PARAMS = {"regionId": "25", "dsoId": "902"}  # Київ, ДТЕК Київські електромережі
WARN_MIN = 30          # remind this many minutes before an outage
SCHEDULE_TTL = 300     # refresh schedule every 5 minutes

# ---------------------------------------------------------------- texts
T = {
    "uk": {
        "start": "Привіт! Я показую графік відключень світла ДТЕК у Києві.\n\nНапишіть назву вашої вулиці українською, наприклад <b>Хрещатик</b>, і я знайду вашу чергу.\n\nАбо одразу вкажіть чергу: <code>/queue 4.1</code>",
        "help": "Що я вмію:\n/now — графік вашої черги зараз\n/address — знайти чергу за адресою\n/queue 4.1 — вибрати чергу вручну\n/notify — увімкнути або вимкнути нагадування\n/lang — English\n\nПросто напишіть назву вулиці, щоб змінити адресу.",
        "short": "Напишіть хоча б 2 літери назви вулиці.",
        "searching": "Шукаю…",
        "no_streets": "Не знайшов такої вулиці. Спробуйте інакше, наприклад без «вул.» або лише частину назви.",
        "pick_street": "Виберіть вулицю:",
        "ask_house": "<b>{street}</b>\nТепер напишіть номер будинку, наприклад <code>22</code>.",
        "no_houses": "Не знайшов будинок «{q}» на цій вулиці. Спробуйте лише номер, без літери чи корпусу.",
        "pick_house": "Виберіть будинок:",
        "no_group": "Для цієї адреси YASNO не вказує чергу. Спробуйте сусідній будинок або вкажіть чергу вручну: <code>/queue 4.1</code>",
        "yasno_err": "Сервіс пошуку адрес зараз не відповідає. Спробуйте пізніше або вкажіть чергу вручну: <code>/queue 4.1</code>",
        "saved": "Запам'ятав. Нагадування увімкнені: напишу за {m} хв до відключення і коли опублікують графік на завтра. Вимкнути: /notify",
        "need_queue": "Спочатку вкажіть адресу — напишіть назву вулиці. Або чергу: <code>/queue 4.1</code>",
        "bad_queue": "Не знаю такої черги. Приклад: <code>/queue 4.1</code>",
        "queue_set": "Черга {q} збережена.",
        "notify_on": "Нагадування увімкнені.",
        "notify_off": "Нагадування вимкнені. Увімкнути знову: /notify",
        "no_data": "Не вдалося завантажити графік. Спробуйте за кілька хвилин.",
        "title": "Черга {q}",
        "on_now": "🟢 Зараз світло є",
        "off_now": "⚫ Зараз світла немає",
        "maybe_now": "🟡 Зараз можливе відключення",
        "next_off": "відключення о <b>{t}</b> (через {left})",
        "next_on": "увімкнуть о <b>{t}</b> (через {left})",
        "next_maybe": "можливе відключення о <b>{t}</b> (через {left})",
        "tomorrow_word": "завтра ",
        "no_change": "змін до кінця графіка немає",
        "today": "Сьогодні",
        "tomorrow": "Завтра",
        "no_outages": "відключень немає",
        "off_total": "без світла {d}",
        "not_published": "графік ще не опубліковано",
        "maybe": "можливо",
        "site": "Графік на сайті",
        "warn": "⚠️ Через {m} хв відключення світла ({t}). Черга {q}.",
        "tomorrow_published": "📅 Опубліковано графік на завтра.",
        "dur": lambda h, m: f"{h} год {m:02d} хв" if h else f"{m} хв",
        "lang_set": "Мову змінено на українську.",
        "commands": [("now", "Графік моєї черги зараз"), ("address", "Знайти чергу за адресою"),
                     ("queue", "Вибрати чергу вручну, напр. /queue 4.1"), ("notify", "Нагадування: увімк./вимк."),
                     ("lang", "English"), ("help", "Допомога")],
    },
    "en": {
        "start": "Hi! I show DTEK's power outage schedule for Kyiv.\n\nType your street name in Ukrainian, for example <b>Хрещатик</b>, and I'll find your queue.\n\nOr set the queue directly: <code>/queue 4.1</code>",
        "help": "What I can do:\n/now — your queue's schedule now\n/address — find your queue by address\n/queue 4.1 — set the queue manually\n/notify — turn reminders on or off\n/lang — українська\n\nType a street name any time to change the address.",
        "short": "Type at least 2 letters of the street name.",
        "searching": "Searching…",
        "no_streets": "No such street found. Try in Ukrainian, without «вул.», or just part of the name.",
        "pick_street": "Pick your street:",
        "ask_house": "<b>{street}</b>\nNow type the house number, for example <code>22</code>.",
        "no_houses": "House «{q}» not found on this street. Try just the number, without a letter or block.",
        "pick_house": "Pick your house:",
        "no_group": "YASNO lists no queue for this address. Try a neighbouring house or set the queue manually: <code>/queue 4.1</code>",
        "yasno_err": "The address service isn't responding. Try later or set the queue manually: <code>/queue 4.1</code>",
        "saved": "Saved. Reminders are on: I'll message you {m} min before an outage and when tomorrow's schedule is published. Turn off: /notify",
        "need_queue": "First tell me your address — type a street name. Or set a queue: <code>/queue 4.1</code>",
        "bad_queue": "Unknown queue. Example: <code>/queue 4.1</code>",
        "queue_set": "Queue {q} saved.",
        "notify_on": "Reminders are on.",
        "notify_off": "Reminders are off. Turn back on: /notify",
        "no_data": "Couldn't load the schedule. Try again in a few minutes.",
        "title": "Queue {q}",
        "on_now": "🟢 Power is on now",
        "off_now": "⚫ Power is off now",
        "maybe_now": "🟡 Possible outage now",
        "next_off": "outage at <b>{t}</b> (in {left})",
        "next_on": "back on at <b>{t}</b> (in {left})",
        "next_maybe": "possible outage at <b>{t}</b> (in {left})",
        "tomorrow_word": "tomorrow ",
        "no_change": "no changes until the end of the schedule",
        "today": "Today",
        "tomorrow": "Tomorrow",
        "no_outages": "no outages",
        "off_total": "{d} without power",
        "not_published": "not published yet",
        "maybe": "possible",
        "site": "Schedule on the website",
        "warn": "⚠️ Power outage in {m} min ({t}). Queue {q}.",
        "tomorrow_published": "📅 Tomorrow's schedule is out.",
        "dur": lambda h, m: f"{h} h {m:02d} min" if h else f"{m} min",
        "lang_set": "Language set to English.",
        "commands": [("now", "My queue's schedule now"), ("address", "Find queue by address"),
                     ("queue", "Set queue manually, e.g. /queue 4.1"), ("notify", "Reminders on/off"),
                     ("lang", "Українська"), ("help", "Help")],
    },
}

# ---------------------------------------------------------------- http helpers
def http_json(url, data=None, timeout=20):
    body = None
    headers = {"User-Agent": "svitlo-kyiv-bot/1.0", "Accept": "application/json"}
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def yasno(endpoint, **params):
    q = urllib.parse.urlencode({**YASNO_PARAMS, **params})
    return http_json(f"{YASNO}/{endpoint}?{q}", timeout=15)


# ---------------------------------------------------------------- schedule
_sched = {"at": 0, "data": None}
SLOT = {"yes": "++", "no": "--", "first": "-+", "second": "+-", "maybe": "??", "mfirst": "?+", "msecond": "+?"}


def schedule():
    if time.time() - _sched["at"] > SCHEDULE_TTL or not _sched["data"]:
        try:
            _sched["data"] = http_json(SCHEDULE_URL + f"?t={int(time.time())}", timeout=30)
            _sched["at"] = time.time()
        except Exception as e:  # keep the last good copy
            print("schedule fetch failed:", e, flush=True)
            _sched["at"] = time.time() - SCHEDULE_TTL + 60
    return _sched["data"]


def days_from_today(data, now=None):
    """Published days from today on: [(day_ts, groups)]. Day keys are Kyiv midnights,
    so Kyiv wall-clock time = seconds since today's key (no tz database needed)."""
    now = now or time.time()
    keys = sorted(int(k) for k in data["fact"]["data"])
    out = [(k, data["fact"]["data"][str(k)]) for k in keys if k + 86400 > now]
    return out


def slots(hours):
    s = "".join(SLOT.get((hours or {}).get(str(h)), "++") for h in range(1, 25))
    return s  # 48 chars: + on, - off, ? maybe


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


def queue_key(q):
    q = q.strip().replace(",", ".")
    if "." not in q:
        q += ".1"
    return "GPV" + q


# ---------------------------------------------------------------- messages
def status_text(q, lang, address=None, now=None):
    t = T[lang]
    now = now or time.time()
    data = schedule()
    if not data:
        return t["no_data"]
    days = days_from_today(data, now)
    key = queue_key(q)
    lines = [f"<b>{t['title'].format(q=q)}</b>" + (f" · {html.escape(address)}" if address else "")]
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
            when = (t["tomorrow_word"] if j >= 48 else "") + fmt(j * 30)
            state += " — " + t[nxt].format(t=when, left=fmt_left(j * 30 - now_min))
        else:
            state += " — " + t["no_change"]
        lines += [state, ""]
    for label, day in ((t["today"], today), (t["tomorrow"], next((d for d in days if d[0] > now), None))):
        if not day:
            lines.append(f"<b>{label}:</b> {t['not_published']}")
            continue
        s = slots(day[1].get(key))
        iv = intervals(s)
        off = s.count("-") * 30
        head = f"<b>{label}:</b> " + (t["off_total"].format(d=t["dur"](off // 60, off % 60)) if iv else t["no_outages"])
        lines.append(head)
        for kind, a, b in iv:
            lines.append(f"  {'▪️' if kind == '-' else '▫️'} {fmt(a)}–{'24:00' if b == 1440 else fmt(b)}" +
                         (f" ({t['maybe']})" if kind == "?" else ""))
    a, b = key[3:].split(".")
    lines += ["", f'<a href="{SITE_URL}#q{a}-{b}">{t["site"]}</a>']
    return "\n".join(lines)


# ---------------------------------------------------------------- storage
USERS_FILE = os.path.join(DATA_DIR, "users.json")


def load_users():
    try:
        with open(USERS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_users(users):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = USERS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=1)
    os.replace(tmp, USERS_FILE)


# ---------------------------------------------------------------- telegram
def tg(method, **params):
    return http_json(f"https://api.telegram.org/bot{TOKEN}/{method}", params, timeout=65)


def send(chat_id, text, buttons=None):
    p = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if buttons:
        p["reply_markup"] = {"inline_keyboard": buttons}
    try:
        tg("sendMessage", **p)
    except urllib.error.HTTPError as e:
        print("send failed", chat_id, e.code, e.read()[:200], flush=True)
        if e.code == 403:  # user blocked the bot
            return "blocked"
    except Exception as e:
        print("send failed", chat_id, e, flush=True)


def kb(items, prefix):
    rows, row = [], []
    for it in items[:30]:
        row.append({"text": it["value"][:60], "callback_data": f"{prefix}:{it['id']}"})
        if len(row) == (1 if prefix == "s" else 4):
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


class Bot:
    def __init__(self):
        self.users = load_users()

    def user(self, chat_id, lang_code=None):
        u = self.users.setdefault(str(chat_id), {"lang": "en" if (lang_code or "").startswith("en") else "uk", "notify": True})
        return u

    def text(self, chat_id, msg, lang_code):
        u = self.user(chat_id, lang_code)
        t = T[u["lang"]]
        m = msg.strip()
        cmd, _, arg = m.partition(" ")
        cmd = cmd.split("@")[0].lower()
        if cmd == "/start":
            send(chat_id, t["start"])
        elif cmd == "/help":
            send(chat_id, t["help"])
        elif cmd == "/now":
            send(chat_id, status_text(u["queue"], u["lang"], u.get("address")) if u.get("queue") else t["need_queue"])
        elif cmd == "/address":
            u.pop("street", None)
            if arg:
                self.search_streets(chat_id, u, arg)
            else:
                send(chat_id, t["start"].split("\n\n")[1])
        elif cmd == "/queue":
            data = schedule()
            key = queue_key(arg or "")
            groups = days_from_today(data)[0][1] if data and days_from_today(data) else {}
            if not arg or key not in groups:
                send(chat_id, t["bad_queue"])
            else:
                u["queue"], u["address"] = key[3:], None
                send(chat_id, t["queue_set"].format(q=u["queue"]))
                send(chat_id, status_text(u["queue"], u["lang"]))
        elif cmd == "/notify":
            u["notify"] = not u.get("notify", True)
            send(chat_id, t["notify_on"] if u["notify"] else t["notify_off"])
        elif cmd == "/lang":
            u["lang"] = "en" if u["lang"] == "uk" else "uk"
            send(chat_id, T[u["lang"]]["lang_set"])
        elif cmd.startswith("/"):
            send(chat_id, t["help"])
        elif u.get("street"):
            self.search_houses(chat_id, u, m)
        else:
            self.search_streets(chat_id, u, m)
        save_users(self.users)

    def search_streets(self, chat_id, u, q):
        t = T[u["lang"]]
        if len(q) < 2:
            return send(chat_id, t["short"])
        try:
            res = yasno("streets", query=q[:60])
        except Exception:
            return send(chat_id, t["yasno_err"])
        if not res:
            return send(chat_id, t["no_streets"])
        u["streets"] = {str(s["id"]): s["value"] for s in res[:30]}
        if len(res) == 1:
            return self.pick_street(chat_id, u, str(res[0]["id"]))
        send(chat_id, t["pick_street"], kb(res, "s"))

    def pick_street(self, chat_id, u, sid):
        name = (u.get("streets") or {}).get(sid)
        if not name:
            return
        u["street"] = {"id": sid, "name": name}
        send(chat_id, T[u["lang"]]["ask_house"].format(street=html.escape(name)))

    def search_houses(self, chat_id, u, q):
        t = T[u["lang"]]
        try:
            res = yasno("houses", streetId=u["street"]["id"], query=q[:20])
        except Exception:
            return send(chat_id, t["yasno_err"])
        if not res:
            # maybe they typed a new street instead of a house number
            if not any(ch.isdigit() for ch in q):
                u.pop("street", None)
                return self.search_streets(chat_id, u, q)
            return send(chat_id, t["no_houses"].format(q=html.escape(q)))
        def natural(h):  # 2, 2А, 10, 10/1, 100 …
            v = h["value"]
            n = "".join(ch for ch in v[:6] if ch.isdigit()) if v[:1].isdigit() else ""
            return (int(n) if n else 10**9, v)
        res = sorted(res, key=natural)
        u["houses"] = {str(h["id"]): h["value"] for h in res[:30]}
        exact = [h for h in res if h["value"].lower() == q.lower()]
        if len(res) == 1 or len(exact) == 1:
            return self.pick_house(chat_id, u, str((exact or res)[0]["id"]))
        send(chat_id, t["pick_house"], kb(res, "h"))

    def pick_house(self, chat_id, u, hid):
        t = T[u["lang"]]
        house = (u.get("houses") or {}).get(hid)
        if not house or not u.get("street"):
            return
        try:
            g = yasno("group", streetId=u["street"]["id"], houseId=hid)
        except Exception:
            return send(chat_id, t["yasno_err"])
        if g.get("group") is None:
            return send(chat_id, t["no_group"])
        u["queue"] = f"{g['group']}.{g.get('subgroup') or 1}"
        u["address"] = f"{u['street']['name']}, {house}"
        for k in ("street", "streets", "houses"):
            u.pop(k, None)
        send(chat_id, status_text(u["queue"], u["lang"], u["address"]))
        if u.get("notify", True):
            send(chat_id, t["saved"].format(m=WARN_MIN))

    def callback(self, cq):
        chat_id = cq["message"]["chat"]["id"]
        u = self.user(chat_id, cq["from"].get("language_code"))
        kind, _, val = cq.get("data", "").partition(":")
        try:
            tg("answerCallbackQuery", callback_query_id=cq["id"])
        except Exception:
            pass
        if kind == "s":
            self.pick_street(chat_id, u, val)
        elif kind == "h":
            self.pick_house(chat_id, u, val)
        save_users(self.users)

    def notify_tick(self, now=None):
        """30-minute outage warnings and 'tomorrow published' messages."""
        now = now or time.time()
        data = schedule()
        if not data:
            return
        days = days_from_today(data, now)
        if not days or days[0][0] > now:
            return
        today_ts = days[0][0]
        tomorrow = next((d for d in days if d[0] > now), None)
        changed = False
        for cid, u in list(self.users.items()):
            if not u.get("notify", True) or not u.get("queue"):
                continue
            t = T[u["lang"]]
            key = queue_key(u["queue"])
            seq = "".join(slots(g.get(key)) for _, g in days)
            now_min = (now - today_ts) / 60
            for j in range(int(now_min // 30) + 1, len(seq)):
                start = j * 30
                if start - now_min > WARN_MIN:
                    break
                if seq[j] == "-" and seq[j - 1] != "-":
                    mark = f"{today_ts}:{j}"
                    if u.get("warned") != mark:
                        u["warned"] = mark
                        changed = True
                        if send(cid, t["warn"].format(m=int(round(start - now_min)), t=fmt(start), q=u["queue"])) == "blocked":
                            u["notify"] = False
                    break
            if tomorrow and u.get("tomorrow_sent") != tomorrow[0]:
                u["tomorrow_sent"] = tomorrow[0]
                changed = True
                if u.get("seen_today") == today_ts:  # don't spam brand-new users with an old publication
                    send(cid, t["tomorrow_published"] + "\n\n" + status_text(u["queue"], u["lang"], u.get("address"), now))
            if u.get("seen_today") != today_ts:
                u["seen_today"] = today_ts
                changed = True
        if changed:
            save_users(self.users)

    def run(self):
        me = tg("getMe")["result"]
        print(f"Bot @{me['username']} started", flush=True)
        for lang in ("uk", "en"):
            try:
                tg("setMyCommands", commands=[{"command": c, "description": d} for c, d in T[lang]["commands"]],
                   **({"language_code": "en"} if lang == "en" else {}))
            except Exception as e:
                print("setMyCommands failed:", e, flush=True)
        offset, last_tick = 0, 0
        while True:
            try:
                res = tg("getUpdates", offset=offset, timeout=50, allowed_updates=["message", "callback_query"])["result"]
            except Exception as e:
                print("getUpdates failed:", e, flush=True)
                time.sleep(5)
                res = []
            for upd in res:
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        self.callback(upd["callback_query"])
                    elif "message" in upd and "text" in upd["message"]:
                        m = upd["message"]
                        self.text(m["chat"]["id"], m["text"], m.get("from", {}).get("language_code"))
                except Exception as e:
                    print("update failed:", repr(e), flush=True)
            if time.time() - last_tick >= 60:
                last_tick = time.time()
                try:
                    self.notify_tick()
                except Exception as e:
                    print("notify failed:", repr(e), flush=True)


# ---------------------------------------------------------------- offline test
def offline_test(street_q, house_q):
    print("streets:", [s["value"] for s in yasno("streets", query=street_q)][:5])
    s = yasno("streets", query=street_q)[0]
    houses = yasno("houses", streetId=s["id"], query=house_q)
    h = next((x for x in houses if x["value"] == house_q), houses[0])
    g = yasno("group", streetId=s["id"], houseId=h["id"])
    q = f"{g['group']}.{g.get('subgroup') or 1}"
    print(f"{s['value']}, {h['value']} -> {q}\n")
    for lang in ("uk", "en"):
        print(status_text(q, lang, f"{s['value']}, {h['value']}"), "\n")
    b = Bot.__new__(Bot)
    b.users = {"1": {"lang": "uk", "notify": True, "queue": q}}
    global send
    sent = []
    send = lambda cid, text, buttons=None: sent.append(text)  # noqa: E731
    days = days_from_today(schedule())
    seq = "".join(slots(g_.get(queue_key(q))) for _, g_ in days)
    j = next((i for i in range(1, 48) if seq[i] == "-" and seq[i - 1] != "-"), None)
    if j:
        b.notify_tick(now=days[0][0] + j * 30 * 60 - 20 * 60)  # 20 min before an outage
        print("warning test:", sent)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--test":
        offline_test(sys.argv[2] if len(sys.argv) > 2 else "Хрещатик", sys.argv[3] if len(sys.argv) > 3 else "22")
    elif not TOKEN:
        sys.exit("BOT_TOKEN is not set. Put it in .env next to bot.py (BOT_TOKEN=123456:ABC...)")
    else:
        Bot().run()
