"""Phase 3 end-to-end: Telegram + linking + policy + admin, with the real hub and edge,
fake WhatsApp Graph and Telegram Bot APIs, and real audio. Only the model is stubbed."""

import hashlib
import hmac
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import types
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import support  # noqa: E402

SP = support.workdir()
FIX = support.make_fixtures(SP)
HUB_PORT = 18401
HUB = f"http://127.0.0.1:{HUB_PORT}"
META_SECRET = "meta-secret"
QTOK = "queue-token"
WA_TOKEN = "wa-token"
TG_TOKEN = "tg-token"
TG_SECRET = hashlib.sha256(f"eliezer-webhook:{TG_TOKEN}".encode()).hexdigest()
ADMIN = "999"
BOT_NUMBER = "15550100000"

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)


def free_port():
    s = socket.socket(); s.bind(("", 0)); p = s.getsockname()[1]; s.close(); return p


LOCK = threading.Lock()
MEDIA = {}                 # id -> (path, mime)   (WhatsApp media ids and Telegram file ids)
WA_SENT, TG_SENT, TG_CALLS = [], [], []
_message_ids = iter(range(5000, 10 ** 9))
TG_FORBIDDEN = set()       # chat ids answering 403
FILE_FAIL = {}             # file_id -> status codes its downloads return first, in order


class Base(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def _file(self, mid):
        path, mime = MEDIA[mid]
        data = open(path, "rb").read()
        self.send_response(200); self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)


class Graph(Base):
    def do_GET(self):
        if self.headers.get("Authorization") != f"Bearer {WA_TOKEN}":
            return self._json(401, {})
        path = urllib.parse.urlparse(self.path)
        parts = path.path.strip("/").split("/")
        if parts == ["PNID"]:
            return self._json(200, {"display_phone_number": "+1 555-010-0000", "id": "PNID"})
        if parts[0] == "dl":
            return self._file(parts[1])
        if parts[0] in MEDIA:
            return self._json(200, {"url": f"http://127.0.0.1:{self.server.server_port}/dl/{parts[0]}",
                                    "mime_type": MEDIA[parts[0]][1]})
        self._json(404, {})

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with LOCK:
            WA_SENT.append(data)
        self._json(200, {"messages": [{"id": "wamid.out"}]})


class Telegram(Base):
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        if parts[0] == "file" and parts[1] == f"bot{TG_TOKEN}":
            with LOCK:
                pending = FILE_FAIL.get(parts[2])
                code = pending.pop(0) if pending else None
            if code:
                return self._json(code, {"ok": False})
            return self._file(parts[2])
        self._json(404, {"ok": False})

    def do_POST(self):
        parts = self.path.strip("/").split("/")
        if parts[0] != f"bot{TG_TOKEN}":
            return self._json(401, {"ok": False, "description": "Unauthorized"})
        method = parts[1]
        data = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0) or b"{}") or b"{}")
        with LOCK:
            TG_CALLS.append((method, data))
            if method == "sendMessage":
                if str(data["chat_id"]) in TG_FORBIDDEN:
                    return self._json(403, {"ok": False, "error_code": 403,
                                            "description": "Forbidden: bot was blocked by the user"})
                data["_id"] = next(_message_ids)
                TG_SENT.append(data)
        if method == "getMe":
            return self._json(200, {"ok": True, "result": {"id": 1, "username": "EliezerTestBot"}})
        if method == "getFile":
            return self._json(200, {"ok": True, "result": {"file_path": data["file_id"]}})
        self._json(200, {"ok": True, "result": {"message_id": data.get("_id", 1)} if method == "sendMessage" else True})


def tg_to(chat):
    with LOCK:
        return [m for m in TG_SENT if str(m["chat_id"]) == str(chat)]


NOTICE = "שלום,\n\nוואטסאפ מתחילה"


def wa_texts(to, notices=False):
    """WhatsApp texts to a number; the pricing notice only when asked for."""
    with LOCK:
        return [m for m in WA_SENT if m.get("type") == "text" and m.get("to") == to
                and m["text"]["body"].startswith(NOTICE) == notices]


DROPPED = "שלום,\n\nבגלל שינוי המחירים בוואטסאפ"


def all_wa_texts(to):
    with LOCK:
        return [m for m in WA_SENT if m.get("type") == "text" and m.get("to") == to]


def wa_receipts(mid):
    with LOCK:
        return [m for m in WA_SENT if m.get("status") == "read" and m.get("message_id") == mid]


def wa_reactions(mid=None):
    with LOCK:
        return [m for m in WA_SENT if m.get("type") == "reaction"
                and (mid is None or m.get("reaction", {}).get("message_id") == mid)]


def tg_calls(method, chat=None):
    with LOCK:
        return [d for m, d in TG_CALLS if m == method and (chat is None or str(d.get("chat_id")) == str(chat))]


# ---------------------------------------------------------------- hub I/O
def http(method, path, body=None, headers=None, raw=None, timeout=30):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    h = {"Content-Type": "application/json"} if data is not None else {}
    h.update(headers or {})
    r = urllib.request.Request(HUB + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


_ids = iter(range(1000, 10 ** 9))


def wa(frm, mtype="audio", text=None, media="m-short"):
    mid = f"wamid.{next(_ids)}"
    m = {"from": frm, "id": mid, "timestamp": str(int(time.time())), "type": mtype}
    if mtype == "text":
        m["text"] = {"body": text}
    else:
        m["audio"] = {"id": media, "mime_type": "audio/ogg"}
    payload = {"object": "whatsapp_business_account", "entry": [{"id": "W", "changes": [{
        "field": "messages", "value": {"messaging_product": "whatsapp", "metadata": {"phone_number_id": "PNID"},
                                       "messages": [m]}}]}]}
    raw = json.dumps(payload).encode()
    sig = hmac.new(META_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    st, _ = http("POST", "/webhook/whatsapp", raw=raw, headers={"X-Hub-Signature-256": "sha256=" + sig})
    assert st == 200, st
    return mid


def tg(chat, text=None, voice=None, document=None, chat_type="private", update_id=None, secret=TG_SECRET,
       sender=None, reply_to=None):
    uid = update_id or next(_ids)
    msg = {"message_id": next(_ids), "date": int(time.time()), "chat": {"id": int(chat), "type": chat_type},
           "from": {"id": int(sender or chat), "first_name": "T"}}
    if reply_to:
        msg["reply_to_message"] = reply_to
    if text is not None:
        msg["text"] = text
    if voice:
        msg["voice"] = {"file_id": voice, "mime_type": "audio/ogg", "file_size": 11906, "duration": 3}
    if document:
        msg["document"] = document
    st, _ = http("POST", "/webhook/telegram", {"update_id": uid, "message": msg},
                 headers={"X-Telegram-Bot-Api-Secret-Token": secret})
    return st, msg["message_id"], uid


def tg_member(chat, status):
    http("POST", "/webhook/telegram", {"update_id": next(_ids), "my_chat_member": {
        "chat": {"id": int(chat), "type": "private"}, "date": int(time.time()),
        "new_chat_member": {"status": status, "user": {"id": 1}}}},
         headers={"X-Telegram-Bot-Api-Secret-Token": TG_SECRET})


def wait_for(cond, timeout=30, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return bool(cond())


def token_from(messages):
    for m in messages:
        found = re.search(r"link%20([0-9A-Z]{8})", json.dumps(m)) or re.search(r"link ([0-9A-Z]{8})", m.get("text", ""))
        if found:
            return found.group(1)
    return None


# ---------------------------------------------------------------- main
def main():
    MEDIA.update({"m-short": (os.path.join(FIX, "short.ogg"), "audio/ogg"),
                  "f-short": (os.path.join(FIX, "short.ogg"), "audio/ogg")})
    graph = ThreadingHTTPServer(("127.0.0.1", 0), Graph)
    tgapi = ThreadingHTTPServer(("127.0.0.1", 0), Telegram)
    for s in (graph, tgapi):
        threading.Thread(target=s.serve_forever, daemon=True).start()

    pg = free_port()
    subprocess.run(["docker", "rm", "-f", "eliezer-p3-pg"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "eliezer-p3-pg", "-e", "POSTGRES_PASSWORD=pw",
                    "-p", f"127.0.0.1:{pg}:5432", "postgres:16-alpine"], check=True, capture_output=True)
    wait_for(lambda: subprocess.run(["docker", "exec", "eliezer-p3-pg", "pg_isready", "-U", "postgres"],
                                    capture_output=True).returncode == 0, 30, 0.5)
    time.sleep(1)
    db = f"postgresql://postgres:pw@127.0.0.1:{pg}/postgres"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("POSTHOG", "WHATSAPP", "STATUS", "TELEGRAM"))}
    env.update(DATABASE_URL=db, INGEST_TOKEN="ingest", QUEUE_TOKEN=QTOK, META_APP_SECRET=META_SECRET,
               META_VERIFY_TOKEN="vt", WHATSAPP_API_TOKEN=WA_TOKEN, WHATSAPP_PHONE_NUMBER_ID="PNID",
               WHATSAPP_GRAPH_URL=f"http://127.0.0.1:{graph.server_port}",
               TELEGRAM_BOT_TOKEN=TG_TOKEN, TELEGRAM_API_URL=f"http://127.0.0.1:{tgapi.server_port}",
               TELEGRAM_WEBHOOK_URL=f"{HUB}/webhook/telegram", ADMIN_TG_CHAT_IDS=ADMIN,
               STATUS_USER_SALT="salt", NUDGE_INTERVAL="1000000", QUEUE_SWEEP_INTERVAL_SECONDS="1",
               LOG_LEVEL="DEBUG", PORT=str(HUB_PORT))
    hubdir = os.path.join(REPO, "status_site")
    py = sys.executable
    subprocess.run([py, "-c", "import app; app.init_db(); app.init_queue_db()"], cwd=hubdir, env=env, check=True)
    hub_log = open(os.path.join(SP, "hub3.log"), "w")
    hub = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                            "--port", str(HUB_PORT), "--workers", "1", "--no-access-log"],
                           cwd=hubdir, env=env, stdout=hub_log, stderr=subprocess.STDOUT)
    conn = psycopg.connect(db, autocommit=True)
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    try:
        wait_for(lambda: _up(), 20)
        start_edge()

        # ===== startup registration
        wait_for(lambda: tg_calls("setMyCommands"), 15)
        hooks = tg_calls("setWebhook")
        check("startup registers the Telegram webhook with its secret",
              hooks and hooks[-1]["url"] == f"{HUB}/webhook/telegram" and hooks[-1]["secret_token"] == TG_SECRET
              and set(hooks[-1]["allowed_updates"]) == {"message", "my_chat_member"}, hooks)
        check("startup publishes the command menu",
              {c["command"] for c in tg_calls("setMyCommands")[-1]["commands"]} == {"link", "unlink", "status", "transcribe"})

        st, _, _ = tg("111", text="/start", secret="wrong")
        check("Telegram delivery with a bad secret -> 403", st == 403, st)
        tg("112", text="hello", chat_type="group")
        time.sleep(1)
        check("group chats are ignored", not tg_to("112"))

        # ===== 1-4: Telegram-first link, then both directions deliver to Telegram
        tg("111", text="/start")
        wait_for(lambda: tg_to("111"))
        welcome = tg_to("111")[0]
        button = welcome.get("reply_markup", {}).get("inline_keyboard", [[{}]])[0][0].get("url", "")
        t1 = token_from([welcome])
        check("/start offers a wa.me button prefilled with a link code",
              t1 and button == f"https://wa.me/{BOT_NUMBER}?text=link%20{t1}", (button, t1))
        mid = wa("972500000001", "text", text=f"link {t1.lower()}")
        wait_for(lambda: len(tg_to("111")) >= 2)
        check("WhatsApp 'link <code>' links the number (Telegram told, masked)",
              "+972…001" in tg_to("111")[-1]["text"] and "מקושר" in tg_to("111")[-1]["text"], tg_to("111")[-1])
        time.sleep(1.5)
        wait_for(lambda: wa_reactions(mid), 10)
        check("...WhatsApp gets a thumbs up reaction and no confirmation text",
              wa_reactions(mid) and wa_reactions(mid)[0].get("reaction", {}).get("emoji") == "👍"
              and not wa_texts("972500000001"))
        check("...and the link message was marked read", wa_receipts(mid))

        n_wa = len(wa_texts("972500000001"))
        mid = wa("972500000001")
        wait_for(lambda: any(m["text"] == "hello world" for m in tg_to("111")), 30)
        check("a linked number's voice note is transcribed into Telegram",
              any(m["text"] == "hello world" and "reply_parameters" not in m for m in tg_to("111")))
        check("...not on WhatsApp", len(wa_texts("972500000001")) == n_wa)
        check("...though WhatsApp still gets its blue ticks", wa_receipts(mid))
        check("...and Telegram shows typing while it transcribes", tg_calls("sendChatAction", "111"))

        _, vmid, _ = tg("111", voice="f-short")
        wait_for(lambda: any(m.get("reply_parameters", {}).get("message_id") == vmid for m in tg_to("111")), 30)
        check("a voice note sent in Telegram is transcribed as a quoted reply",
              any(m.get("reply_parameters", {}).get("message_id") == vmid and m["text"] == "hello world"
                  for m in tg_to("111")))

        # ===== 5-6: Telegram-direct, unlinked; oversized file
        _, vmid, _ = tg("222", voice="f-short")
        wait_for(lambda: tg_to("222"), 30)
        check("an unlinked Telegram chat gets its own transcripts", tg_to("222") and tg_to("222")[0]["text"] == "hello world")
        tg("222", document={"file_id": "f-big", "file_size": 30 * 1024 * 1024, "mime_type": "audio/mpeg"})
        wait_for(lambda: len(tg_to("222")) >= 2)
        check("files over 20MB are refused up front", "20MB" in tg_to("222")[-1]["text"])

        check("transcripts delivered to Telegram carry no pricing notice",
              not any(m["text"].startswith(NOTICE) for m in tg_to("111") + tg_to("222")))
        # ===== 7-8: bad and expired codes
        mid = wa("972500000002", "text", text="link ZZZZZZZZ")
        wait_for(lambda: wa_receipts(mid))
        time.sleep(1)
        check("an unknown link code gets no WhatsApp reply (it would be billed)", not wa_texts("972500000002"))
        wait_for(lambda: wa_reactions(mid), 10)
        check("...but a (free) thumbs down on the link message",
              wa_reactions(mid) and wa_reactions(mid)[0].get("reaction", {}).get("emoji") == "👎", wa_reactions(mid))
        tg("333", text="/link")
        wait_for(lambda: tg_to("333"))
        t3 = token_from(tg_to("333"))
        q("UPDATE link_tokens SET expires_at = now() - interval '1 minute' WHERE token = %s RETURNING token", t3)
        check("the code offer says it lasts 15 minutes", "15 דקות" in tg_to("333")[-1]["text"], tg_to("333")[-1]["text"])
        mid3 = wa("972500000003", "text", text=f"link {t3}")
        wait_for(lambda: len(tg_to("333")) >= 2)
        check("an expired code is reported in the Telegram chat that asked for it",
              "פג" in tg_to("333")[-1]["text"] and not q("SELECT 1 FROM identities WHERE address = '972500000003'"))
        wait_for(lambda: wa_reactions(mid3), 10)
        check("...and gets a thumbs down on WhatsApp",
              wa_reactions(mid3) and wa_reactions(mid3)[0].get("reaction", {}).get("emoji") == "👎", wa_reactions(mid3))

        # ===== offers: a code's message loses its button once used, replaced or expired
        def edits_of(chat, message_id):
            return [d for d in tg_calls("editMessageText", chat) if d.get("message_id") == message_id]
        tg("901", text="/link")
        wait_for(lambda: tg_to("901"))
        first = tg_to("901")[-1]
        time.sleep(0.5)
        tg("901", text="/link")
        wait_for(lambda: len(tg_to("901")) >= 2)
        wait_for(lambda: edits_of("901", first["_id"]), 10)
        edit = (edits_of("901", first["_id"]) or [{}])[0]
        check("asking for a new code edits the old offer: replaced, button gone",
              "הוחלף" in edit.get("text", "") and "reply_markup" not in edit, edit)
        second = tg_to("901")[-1]
        time.sleep(0.5)
        wa("972500000091", "text", text=f"link {token_from([second])}")
        wait_for(lambda: edits_of("901", second["_id"]), 10)
        edit = (edits_of("901", second["_id"]) or [{}])[0]
        check("a used code's offer says it was used", "נוצל" in edit.get("text", "") and "reply_markup" not in edit, edit)
        tg("902", text="/link")
        wait_for(lambda: tg_to("902"))
        third = tg_to("902")[-1]
        time.sleep(0.5)
        q("UPDATE link_tokens SET expires_at = now() - interval '1 minute' WHERE token = %s RETURNING token",
          token_from([third]))
        wait_for(lambda: edits_of("902", third["_id"]), 15)
        edit = (edits_of("902", third["_id"]) or [{}])[0]
        check("an expired code's offer says so and tells how to get a new one",
              "פג" in edit.get("text", "") and "/link" in edit.get("text", "") and "reply_markup" not in edit, edit)
        check("...keeping the rest of the message", "אליעזר" in edit.get("text", ""), edit)

        # ===== 9: /unlink returns the number to WhatsApp
        tg("111", text="/unlink")
        wait_for(lambda: "בוטל" in tg_to("111")[-1]["text"])
        check("/unlink confirms, naming the number", "+972…001" in tg_to("111")[-1]["text"])
        n = len(wa_texts("972500000001"))
        wa("972500000001")
        wait_for(lambda: len(wa_texts("972500000001")) > n, 30)
        check("after /unlink, transcripts go back to WhatsApp", wa_texts("972500000001")[-1]["text"]["body"] == "hello world")

        check("...followed by the pricing notice, now that they go to WhatsApp",
              wait_for(lambda: len(wa_texts("972500000001", notices=True)) == 1, 10))
        # ===== 10: linking starts only from Telegram
        mid = wa("972500000004", "text", text="link")
        wait_for(lambda: wa_receipts(mid))
        time.sleep(1)
        check("WhatsApp 'link' alone starts nothing and gets no reply",
              not wa_texts("972500000004") and not q("SELECT 1 FROM link_tokens t JOIN identities i "
                                                     "ON i.user_id = t.user_id WHERE i.address = '972500000004'"))
        tg("444", text="/link")
        wait_for(lambda: tg_to("444"))
        t4 = token_from(tg_to("444"))
        tg("445", text=f"/start {t4}")
        wait_for(lambda: tg_to("445"))
        check("a code in /start does not link anything (Telegram-side codes are for WhatsApp)",
              "מקושר" not in tg_to("445")[-1]["text"] and not q("SELECT 1 FROM identities WHERE address = '445' "
                                                               "AND user_id IN (SELECT user_id FROM identities WHERE channel = 'whatsapp')"))
        n = len(wa_texts("972500000004"))
        mid4 = wa("972500000004", "text", text=f"link {t4}")
        wait_for(lambda: len(tg_to("444")) >= 2)
        time.sleep(1.5)
        check("the Telegram-first link works for this number", "+972…004" in tg_to("444")[-1]["text"])
        wait_for(lambda: wa_reactions(mid4), 10)
        check("...with a thumbs up reaction and no text reply on WhatsApp",
              wa_reactions(mid4) and wa_reactions(mid4)[0].get("reaction", {}).get("emoji") == "👍"
              and len(wa_texts("972500000004")) == n)

        # ===== 11: re-linking to another chat tells the old one
        tg("555", text="/link")
        wait_for(lambda: tg_to("555"))
        t5 = token_from(tg_to("555"))
        wa("972500000004", "text", text=f"link {t5}")
        wait_for(lambda: len(tg_to("444")) >= 2 and len(tg_to("555")) >= 2)
        check("re-linking moves the number and tells the chat it left",
              "אחר" in tg_to("444")[-1]["text"] and "מקושר" in tg_to("555")[-1]["text"])

        # ===== 12: blocked bot (membership update) -> unbound -> back to WhatsApp
        tg_member("555", "kicked")
        time.sleep(1)
        check("blocking the bot unbinds the chat", not q("SELECT 1 FROM identities WHERE address = '555'"))
        n = len(wa_texts("972500000004"))
        wa("972500000004")
        wait_for(lambda: len(wa_texts("972500000004")) > n, 30)
        check("...and its number falls back to WhatsApp replies",
              wa_texts("972500000004")[-1]["text"]["body"] == "hello world")

        # ===== 13: a 403 on send unbinds too
        tg("666", text="/link")
        wait_for(lambda: tg_to("666"))
        t6 = token_from(tg_to("666"))
        wa("972500000006", "text", text=f"link {t6}")
        wait_for(lambda: len(tg_to("666")) >= 2)
        TG_FORBIDDEN.add("666")
        wa("972500000006")
        wait_for(lambda: not q("SELECT 1 FROM identities WHERE channel = 'telegram' AND address = '666'"), 30)
        check("a 403 from Telegram unbinds the chat", not q("SELECT 1 FROM identities WHERE address = '666'"))

        # ===== 14: admin and the drop policy
        tg("111", text="/wa_policy drop")
        time.sleep(1)
        check("admin commands are ignored from non-admin chats",
              q("SELECT value FROM settings WHERE key = 'unlinked_wa_policy'")[0][0] == "reply")
        tg(ADMIN, text="/wa_policy drop")
        wait_for(lambda: tg_to(ADMIN))
        check("admin sets the policy", "policy: drop" in tg_to(ADMIN)[-1]["text"])
        before = q("SELECT dropped_unlinked FROM totals")[0][0]
        mid = wa("972500000007")
        wa("919812345678")
        wait_for(lambda: all_wa_texts("972500000007"), 10)
        told = all_wa_texts("972500000007")
        check("under drop, an unlinked number's first message gets one notice of where transcripts went",
              len(told) == 1 and told[0]["text"]["body"].startswith(DROPPED)
              and "status.eliezer.ivrit.ai" in told[0]["text"]["body"], told)
        check("...no transcript, and no ticks", not wa_receipts(mid))
        mid = wa("972500000007")
        mid_text = wa("972500000007", "text", text="hello?")
        time.sleep(3)
        check("...and never another: later messages get nothing at all",
              len(all_wa_texts("972500000007")) == 1 and not wa_receipts(mid), all_wa_texts("972500000007"))
        wait_for(lambda: wa_reactions(mid), 10)
        check("...a voice note while silent gets a ⚠️ reaction",
              wa_reactions(mid) and wa_reactions(mid)[0].get("reaction", {}).get("emoji") == "⚠️", wa_reactions(mid))
        check("...text message while silent gets no reaction", not wa_reactions(mid_text))
        check("...all counted as unlinked; the out-of-region number is ignored, but not as unlinked",
              q("SELECT dropped_unlinked FROM totals")[0][0] == before + 3)
        n = len(all_wa_texts("972500000001"))
        wa("972500000001")
        time.sleep(3)
        check("a number already sent the pricing notice is not told again under drop",
              len(all_wa_texts("972500000001")) == n, all_wa_texts("972500000001")[n:])
        told = q("SELECT count(*) FROM wa_told")[0][0]
        st, body = http("GET", "/api/stats")
        check("the dashboard counts the numbers told once", json.loads(body)["totals"]["wa_told"] == told and told >= 2,
              (json.loads(body)["totals"].get("wa_told"), told))
        tg(ADMIN, text="/wa_allow +972-50-000-0008 VIP")
        wait_for(lambda: len(tg_to(ADMIN)) >= 2)
        wa("972500000008")
        wait_for(lambda: wa_texts("972500000008"), 30)
        check("an allowlisted number keeps WhatsApp replies under drop",
              wa_texts("972500000008") and wa_texts("972500000008")[-1]["text"]["body"] == "hello world")
        time.sleep(2)
        check("an allowlisted WhatsApp number does not get the pricing notice",
              not wa_texts("972500000008", notices=True), wa_texts("972500000008", notices=True))
        tg("777", text="/link")
        wait_for(lambda: tg_to("777"))
        t7 = token_from(tg_to("777"))
        mid7 = wa("972500000009", "text", text=f"link {t7}")
        wait_for(lambda: len(tg_to("777")) >= 2)
        check("linking still works under drop", "מקושר" in tg_to("777")[-1]["text"])
        wait_for(lambda: wa_reactions(mid7), 10)
        check("...with thumbs up confirmation and no WhatsApp text reply, even while WhatsApp replies are off",
              wa_reactions(mid7) and wa_reactions(mid7)[0].get("reaction", {}).get("emoji") == "👍"
              and not wa_texts("972500000009"))
        wa("972500000009")
        wait_for(lambda: any(m["text"] == "hello world" for m in tg_to("777")), 30)
        check("...and its voice notes reach Telegram", any(m["text"] == "hello world" for m in tg_to("777")))
        tg(ADMIN, text="/whois 972500000009")
        wait_for(lambda: "telegram 777" in tg_to(ADMIN)[-1]["text"])
        check("/whois shows the identities", "telegram 777" in tg_to(ADMIN)[-1]["text"], tg_to(ADMIN)[-1]["text"])
        tg(ADMIN, text="/wa_policy reply")
        wait_for(lambda: "policy: reply" in tg_to(ADMIN)[-1]["text"])

        # ===== groups: transcribe audio as a reply to it, otherwise stay silent
        G = "-100200"
        _, gv, _ = tg(G, voice="f-short", chat_type="supergroup", sender="7001")
        wait_for(lambda: tg_to(G), 30)
        check("audio posted in a group is transcribed as a reply to it",
              tg_to(G) and tg_to(G)[0]["text"] == "hello world"
              and tg_to(G)[0].get("reply_parameters", {}).get("message_id") == gv, tg_to(G))
        check("...and the group sees the bot typing", tg_calls("sendChatAction", G))
        n = len(tg_to(G))
        tg(G, text="hello everyone", chat_type="supergroup", sender="7001")
        tg(G, text="/link", chat_type="supergroup", sender="7001")
        tg(G, text="/status", chat_type="supergroup", sender="7002")
        time.sleep(2)
        check("a group's conversation and commands get no answer", len(tg_to(G)) == n, tg_to(G)[n:])
        check("...and a group never becomes a linked user", not q("SELECT 1 FROM identities WHERE address = %s", G))
        voice_msg = {"message_id": 424242, "date": int(time.time()), "chat": {"id": int(G), "type": "supergroup"},
                     "voice": {"file_id": "f-short", "mime_type": "audio/ogg", "file_size": 11906}}
        tg(G, text="/transcribe@EliezerTestBot", chat_type="supergroup", sender="7002", reply_to=voice_msg)
        wait_for(lambda: len(tg_to(G)) > n, 30)
        check("/transcribe in reply to audio transcribes it, answering the audio",
              len(tg_to(G)) == n + 1 and tg_to(G)[-1].get("reply_parameters", {}).get("message_id") == 424242, tg_to(G)[n:])
        n = len(tg_to(G))
        tg(G, text="/transcribe@SomeOtherBot", chat_type="supergroup", sender="7002", reply_to=voice_msg)
        time.sleep(2)
        check("/transcribe addressed to another bot is ignored", len(tg_to(G)) == n)
        tg("222", text="/transcribe", reply_to={**voice_msg, "chat": {"id": 222, "type": "private"}, "message_id": 515151})
        wait_for(lambda: any(m.get("reply_parameters", {}).get("message_id") == 515151 for m in tg_to("222")), 30)
        check("/transcribe works in a private chat too",
              any(m.get("reply_parameters", {}).get("message_id") == 515151 for m in tg_to("222")))
        http("POST", "/webhook/telegram", {"update_id": next(_ids), "my_chat_member": {
            "chat": {"id": int(G), "type": "supergroup"}, "date": int(time.time()),
            "new_chat_member": {"status": "left", "user": {"id": 1}}}},
             headers={"X-Telegram-Bot-Api-Secret-Token": TG_SECRET})
        check("group transcripts are counted per person, not per group",
              q("SELECT count(*) FROM events WHERE user_hash = %s", hashlib.sha256(b"salttg:7001").hexdigest()[:16])[0][0] >= 1)
        check("the /transcribe menu entry is published",
              "transcribe" in {c["command"] for c in tg_calls("setMyCommands")[-1]["commands"]})

        # ===== group count and Telegram sent counter
        def group_member(chat, status, ctype="group"):
            http("POST", "/webhook/telegram", {"update_id": next(_ids), "my_chat_member": {
                "chat": {"id": int(chat), "type": ctype}, "date": int(time.time()),
                "new_chat_member": {"status": status, "user": {"id": 1}}}},
                 headers={"X-Telegram-Bot-Api-Secret-Token": TG_SECRET})
        active = lambda: {r[0] for r in q("SELECT chat_id FROM telegram_groups WHERE active")}
        group_member("-300", "member")
        tg("-400", text="hi all", chat_type="group", sender="7003")   # joined before tracking
        time.sleep(1)
        check("being added to a group, or hearing from one, counts it",
              {"-300", "-400"} <= active(), active())
        group_member("-300", "left")
        http("POST", "/webhook/telegram", {"update_id": next(_ids), "message": {
            "message_id": next(_ids), "date": int(time.time()), "chat": {"id": -400, "type": "group"},
            "from": {"id": 7003}, "migrate_to_chat_id": -1000400}},
             headers={"X-Telegram-Bot-Api-Secret-Token": TG_SECRET})
        time.sleep(1)
        check("leaving a group uncounts it", "-300" not in active())
        check("a group upgraded to a supergroup is counted once, under its new id",
              "-400" not in active() and "-1000400" in active(), active())
        s_ = json.loads(http("GET", "/api/stats")[1])
        check("dashboard reports the number of Telegram groups",
              s_["telegram_groups"] == len(active()), (s_["telegram_groups"], active()))

        # ===== reliability: a brief media failure costs seconds, not a lease
        MEDIA["f-flaky500"] = MEDIA["f-short"]
        MEDIA["f-flaky404"] = MEDIA["f-short"]
        FILE_FAIL["f-flaky500"] = [500]
        FILE_FAIL["f-flaky404"] = [404]
        answered = lambda chat, mid: any(m.get("reply_parameters", {}).get("message_id") == mid for m in tg_to(chat))
        t0 = time.time()
        _, v1, _ = tg("888", voice="f-flaky500")
        ok = wait_for(lambda: answered("888", v1), 20)
        took = time.time() - t0
        check("a platform 5xx on a media download is retried by the hub at once", ok and took < 8, f"{took:.1f}s")
        t0 = time.time()
        _, v2, _ = tg("889", voice="f-flaky404")
        ok = wait_for(lambda: answered("889", v2), 60)
        took = time.time() - t0
        check("a job an edge fails on is handed back and retried in seconds, not after its 15-minute lease",
              ok and 8 <= took < 40, f"{took:.1f}s")

        # ===== misc
        tg("222", text="/status")
        wait_for(lambda: "Eliezer Status" in tg_to("222")[-1]["text"])
        check("/status works in Telegram", "Eliezer Status" in tg_to("222")[-1]["text"])
        n = len(tg_to("222"))
        _, _, uid = tg("222", text="/help")
        tg("222", text="/help", update_id=uid)
        time.sleep(1.5)
        check("a re-delivered Telegram update is ignored", len(tg_to("222")) == n + 1)
        check("Telegram users are stored only as salted hashes",
              not q("SELECT 1 FROM events WHERE user_hash IN ('222', 'tg:222', '111')"))
        check("queue and outbox drained", q("SELECT count(*) FROM queue_messages")[0][0] == 0
              and wait_for(lambda: q("SELECT count(*) FROM outbox")[0][0] == 0, 10))
        wait_for(lambda: q("SELECT count(*) FROM outbox")[0][0] == 0, 10)
        s = json.loads(http("GET", "/api/stats")[1])
        with LOCK:
            delivered = len(TG_SENT)
        check("tg_sent counts every message delivered to Telegram", s["totals"]["tg_sent"] == delivered,
              (s["totals"]["tg_sent"], delivered))
        s = json.loads(http("GET", "/api/stats")[1])
        check("dashboard reports linked users and ignored-unlinked",
              s["linked_users"] >= 2 and s["totals"]["dropped_unlinked"] >= 1, (s["linked_users"], s["totals"]))

        EDGE.stop_event.set()
        t0 = time.time()
        hub.send_signal(15)
        hub.wait(timeout=30)
        check("hub exits promptly on SIGTERM", time.time() - t0 < 4, f"{time.time() - t0:.1f}s")
    finally:
        if hub.poll() is None:
            hub.kill(); hub.wait()
        hub_log.close()
        conn.close()
        subprocess.run(["docker", "rm", "-f", "eliezer-p3-pg"], capture_output=True)

    log = open(os.path.join(SP, "hub3.log")).read()
    check("hub logged no tracebacks", "Traceback" not in log)
    check("no setup failures logged", "setup failed" not in log)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    os._exit(1 if failed else 0)


def _up():
    try:
        return http("GET", "/favicon.ico")[0] == 200
    except OSError:
        return False


EDGE = None


def start_edge():
    global EDGE
    os.environ.update(QUEUE_API_URL=HUB, QUEUE_TOKEN=QTOK, INSTANCE_ID="edge-t",
                      RUNPOD_API_KEY="dummy", RUNPOD_ENDPOINT_ID="dummy",
                      STATUS_SITE_URL="", STATUS_INGEST_TOKEN="", POSTHOG_API_KEY="",
                      WHATSAPP_API_TOKEN="", APP_SQS_QUEUE="")
    os.environ.pop("PORT", None)
    os.chdir(SP)
    sys.path.insert(0, REPO)
    import whatsapp_bot

    async def fake_segments(self, audio_path):
        return [types.SimpleNamespace(text="hello world")]
    whatsapp_bot.WhatsAppBot._collect_transcription_segments = fake_segments
    EDGE = whatsapp_bot.WhatsAppBot(num_workers=2)
    threading.Thread(target=EDGE.run, daemon=True).start()


if __name__ == "__main__":
    main()
