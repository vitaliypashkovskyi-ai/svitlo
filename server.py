"""Svitlo Kyiv — local server.

Serves the page from this folder and proxies address lookups to YASNO
(the browser can't call YASNO directly because it sends no CORS headers).

    python server.py            -> http://127.0.0.1:8777
    python server.py 8080 0.0.0.0   (port, host — e.g. to open from a phone on the same Wi-Fi)
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

YASNO = "https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2"
REGION_ID = "25"   # Київ
DSO_ID = "902"     # ДТЕК Київські електромережі
ROOT = os.path.dirname(os.path.abspath(__file__))

# endpoint -> allowed query params passed through from the browser
ROUTES = {
    "streets": ("query",),
    "houses": ("streetId", "query"),
    "group": ("streetId", "houseId"),
}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def do_GET(self):
        url = urllib.parse.urlsplit(self.path)
        if url.path == "/api.php":  # same URL the PHP version uses on QNAP
            qs = urllib.parse.parse_qs(url.query)
            return self.proxy((qs.get("ep") or [""])[0], qs)
        if url.path.startswith("/api/"):
            return self.proxy(url.path[5:], urllib.parse.parse_qs(url.query))
        return super().do_GET()

    def proxy(self, name, qs):
        if name == "ping":
            return self.send_json(200, {"ok": True})
        if name not in ROUTES:
            return self.send_json(404, {"error": "unknown endpoint"})
        params = {"regionId": REGION_ID, "dsoId": DSO_ID}
        for key in ROUTES[name]:
            val = (qs.get(key) or [""])[0][:100]
            if not val:
                return self.send_json(400, {"error": f"missing {key}"})
            params[key] = val
        target = f"{YASNO}/{name}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(target, headers={
            "User-Agent": "svitlo-kyiv-local/1.0",
            "Accept": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                body = r.read()
            json.loads(body)  # make sure upstream answered with JSON
        except Exception as e:  # noqa: BLE001
            return self.send_json(502, {"error": f"YASNO unavailable: {e}"})
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if "/api/" in str(args[0] if args else "") or not str(fmt).startswith('"%s"'):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8777
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    print(f"Svitlo Kyiv: http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
