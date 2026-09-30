"""Phase 4 end-to-end: the Notifier app as an output channel, with the real hub and edge,
fake WhatsApp Graph, Telegram Bot and Notifier APIs, and real audio. Only the model is
stubbed."""

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
HUB_PORT = 18501
HUB = f"http://127.0.0.1:{HUB_PORT}"
META_SECRET = "meta-secret"
QTOK = "queue-token"
WA_TOKEN = "wa-token"
TG_TOKEN = "tg-token"
TG_SECRET = hashlib.sha256(f"eliezer-webhook:{TG_TOKEN}".encode()).hexdigest()
NT_KEY = "nsrc_eliezer_" + "k" * 43

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)


def free_port():
    s = socket.socket(); s.bind(("", 0)); p = s.getsockname()[1]; s.close(); return p


LOCK = threading.Lock()
MEDIA = {}
WA_SENT, TG_SENT = [], []
# The fake Notifier: codes it has handed out, what it received, and how to misbehave.
CODES = {}                 # code -> "live" | "expired"
REDEEMS, NT_SENT = [], []
SUBSCRIPTIONS = {}         # subscription id -> subject
GONE = set()               # subscription ids answering 410
THROTTLE_ONCE = set()      # subscription ids answering 429 once


class Base(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, obj, headers=None):
        body = json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def _file(self, mid):
        path, mime = MEDIA[mid]
        data = open(path, "rb").read()
        self.send_response(200); self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def _body(self):
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0) or b"{}") or b"{}")


class Graph(Base):
    def do_GET(self):
        parts = urllib.parse.urlparse(self.path).path.strip("/").split("/")
        if parts == ["PNID"]:
            return self._json(200, {"display_phone_number": "+1 555-010-0000", "id": "PNID"})
        if parts[0] == "dl":
            return self._file(parts[1])
        if parts[0] in MEDIA:
            return self._json(200, {"url": f"http://127.0.0.1:{self.server.server_port}/dl/{parts[0]}",
                                    "mime_type": MEDIA[parts[0]][1]})
        self._json(404, {})

    def do_POST(self):
        data = self._body()
        with LOCK:
            WA_SENT.append(data)
        self._json(200, {"messages": [{"id": "wamid.out"}]})


class Telegram(Base):
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        if parts[0] == "file" and parts[1] == f"bot{TG_TOKEN}":
            return self._file(parts[2])
        self._json(404, {"ok": False})

    def do_POST(self):
        method = self.path.strip("/").split("/")[1]
        data = self._body()
        if method == "sendMessage":
            with LOCK:
                TG_SENT.append(data)
        if method == "getMe":
            return self._json(200, {"ok": True, "result": {"id": 1, "username": "EliezerTestBot"}})
        if method == "getFile":
            return self._json(200, {"ok": True, "result": {"file_path": data["file_id"]}})
        self._json(200, {"ok": True, "result": {"message_id": 1} if method == "sendMessage" else True})


class Notifier(Base):
    def do_POST(self):
        if self.headers.get("Authorization") != f"Bearer {NT_KEY}":
            return self._json(401, {"error": "invalid_key"})
        data = self._body()
        path = urllib.parse.urlparse(self.path).path
        with LOCK:
            if path == "/api/source/v1/links":
                REDEEMS.append(data)
                state = CODES.get(data["code"])
                if state is None:
                    return self._json(404, {"error": "unknown_code"})
                if state == "expired":
                    return self._json(410, {"error": "expired_code"})
                sub = "sub-" + data["code"].lower()
                SUBSCRIPTIONS[sub] = data["subject"]
                return self._json(201, {"subscription_id": sub})
            if path == "/api/source/v1/messages":
                sub = data["subscription_id"]
                if sub in GONE or sub not in SUBSCRIPTIONS:
                    return self._json(410, {"error": "unlinked"})
                if sub in THROTTLE_ONCE:
                    THROTTLE_ONCE.discard(sub)
                    return self._json(429, {"error": "rate_limited"}, {"Retry-After": "1"})
                NT_SENT.append(data)
                return self._json(202, {"id": "n", "devices": 1, "duplicate": False})
        self._json(404, {})


def nt_to(sub, kind=None):
    with LOCK:
        return [m for m in NT_SENT if m["subscription_id"] == sub and (kind is None or m.get("kind") == kind)]


def tg_to(chat):
    with LOCK:
        return [m for m in TG_SENT if str(m["chat_id"]) == str(chat)]


def wa_texts(to):
    with LOCK:
        return [m for m in WA_SENT if m.get("type") == "text" and m.get("to") == to]


def wa_receipts(mid):
    with LOCK:
        return [m for m in WA_SENT if m.get("status") == "read" and m.get("message_id") == mid]


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


def tg(chat, text=None, voice=None):
    msg = {"message_id": next(_ids), "date": int(time.time()), "chat": {"id": int(chat), "type": "private"},
           "from": {"id": int(chat), "first_name": "T"}}
    if text is not None:
        msg["text"] = text
    if voice:
        msg["voice"] = {"file_id": voice, "mime_type": "audio/ogg", "file_size": 11906, "duration": 3}
    return http("POST", "/webhook/telegram", {"update_id": next(_ids), "message": msg},
                headers={"X-Telegram-Bot-Api-Secret-Token": TG_SECRET})


def wait_for(cond, timeout=30, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return bool(cond())


# ---------------------------------------------------------------- main
def main():
    MEDIA.update({"m-short": (os.path.join(FIX, "short.ogg"), "audio/ogg"),
                  "f-short": (os.path.join(FIX, "short.ogg"), "audio/ogg")})
    graph = ThreadingHTTPServer(("127.0.0.1", 0), Graph)
    tgapi = ThreadingHTTPServer(("127.0.0.1", 0), Telegram)
    ntapi = ThreadingHTTPServer(("127.0.0.1", 0), Notifier)
    for s in (graph, tgapi, ntapi):
        threading.Thread(target=s.serve_forever, daemon=True).start()

    pg = free_port()
    subprocess.run(["docker", "rm", "-f", "eliezer-p4-pg"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "eliezer-p4-pg", "-e", "POSTGRES_PASSWORD=pw",
                    "-p", f"127.0.0.1:{pg}:5432", "postgres:16-alpine"], check=True, capture_output=True)
    wait_for(lambda: subprocess.run(["docker", "exec", "eliezer-p4-pg", "pg_isready", "-U", "postgres"],
                                    capture_output=True).returncode == 0, 30, 0.5)
    time.sleep(1)
    db = f"postgresql://postgres:pw@127.0.0.1:{pg}/postgres"
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("POSTHOG", "WHATSAPP", "STATUS", "TELEGRAM", "NOTIFIER"))}
    env.update(DATABASE_URL=db, QUEUE_TOKEN=QTOK, META_APP_SECRET=META_SECRET,
               META_VERIFY_TOKEN="vt", WHATSAPP_API_TOKEN=WA_TOKEN, WHATSAPP_PHONE_NUMBER_ID="PNID",
               WHATSAPP_GRAPH_URL=f"http://127.0.0.1:{graph.server_port}",
               TELEGRAM_BOT_TOKEN=TG_TOKEN, TELEGRAM_API_URL=f"http://127.0.0.1:{tgapi.server_port}",
               TELEGRAM_WEBHOOK_URL=f"{HUB}/webhook/telegram",
               NOTIFIER_URL=f"http://127.0.0.1:{ntapi.server_port}", NOTIFIER_SOURCE_KEY=NT_KEY,
               NOTIFIER_PUBLIC_URL="https://notifier.example",
               STATUS_USER_SALT="salt", NUDGE_INTERVAL="1", QUEUE_SWEEP_INTERVAL_SECONDS="1",
               LOG_LEVEL="DEBUG", PORT=str(HUB_PORT))
    hubdir = os.path.join(REPO, "status_site")
    subprocess.run([sys.executable, "-c", "import app; app.init_db(); app.init_queue_db()"],
                   cwd=hubdir, env=env, check=True)
    hub_log = open(os.path.join(SP, "hub4.log"), "w")
    hub = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                            "--port", str(HUB_PORT), "--workers", "1", "--no-access-log"],
                           cwd=hubdir, env=env, stdout=hub_log, stderr=subprocess.STDOUT)
    conn = psycopg.connect(db, autocommit=True)
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    try:
        wait_for(lambda: _up(), 20)
        start_edge()

        # ===== linking from WhatsApp
        num = "972500000001"
        CODES["ABCDEFGHJK"] = "live"
        mid = wa(num, "text", text="link abcde-fghjk")
        wait_for(lambda: nt_to("sub-abcdefghjk", "welcome"))
        with LOCK:
            redeem = REDEEMS[-1] if REDEEMS else {}
        check("WhatsApp 'link <notifier code>' is redeemed at the Notifier, canonical",
              redeem.get("code") == "ABCDEFGHJK", redeem)
        check("...with a salted subject: the number itself never leaves",
              redeem.get("subject") and num not in redeem["subject"] and len(redeem["subject"]) == 64, redeem)
        check("...and a masked label for the app", redeem.get("label") == "WhatsApp +972…001", redeem)
        check("the Notifier becomes an output of the number's user",
              q("SELECT deliver FROM identities WHERE channel = 'notifier' AND address = 'sub-abcdefghjk'")
              == [(True,)])
        check("...the number itself stops getting replies",
              q("SELECT deliver FROM identities WHERE channel = 'whatsapp' AND address = %s", num)
              == [(False,)])
        welcome = nt_to("sub-abcdefghjk", "welcome")[0]
        check("a welcome notification confirms it, in Hebrew", "אליעזר מקושר" in welcome["body"], welcome)
        time.sleep(1)
        check("...WhatsApp gets no text at all (it would cost)", not wa_texts(num), wa_texts(num))
        check("...just the free read receipt", wa_receipts(mid))

        # ===== transcripts go to the Notifier, not WhatsApp
        wa(num)
        wait_for(lambda: nt_to("sub-abcdefghjk", "transcript"), 40)
        sent = nt_to("sub-abcdefghjk", "transcript")
        check("a voice note on WhatsApp is transcribed into the Notifier",
              sent and sent[0]["body"] == "hello world", sent)
        check("...marked as a transcript with its length",
              sent and sent[0].get("subtitle", "").startswith("תמלול הקלטה"), sent)
        check("...with a dedupe key, so a retried send lands once",
              sent and sent[0].get("dedupe_key", "").startswith("outbox:"), sent)
        time.sleep(1)
        check("...no nudge rides along as a separate notification (NUDGE_INTERVAL=1)",
              len(nt_to("sub-abcdefghjk", "transcript")) == 1 and len(nt_to("sub-abcdefghjk")) == 2,
              nt_to("sub-abcdefghjk"))
        check("...and still nothing on WhatsApp", not wa_texts(num), wa_texts(num))

        # ===== a code that will never work
        num2 = "972500000002"
        CODES["ZZZZZZZZZZ"] = "expired"
        wa(num2, "text", text="link ZZZZZ-ZZZZZ")
        wait_for(lambda: any(r["code"] == "ZZZZZZZZZZ" for r in REDEEMS))
        time.sleep(1.5)
        check("an expired code from WhatsApp gets no WhatsApp answer", not wa_texts(num2), wa_texts(num2))
        check("...and links nothing", not q("SELECT 1 FROM identities WHERE channel = 'notifier' AND address LIKE 'sub-zz%%'"))

        # ===== from Telegram, by deep link
        CODES["TELEGRAM01"] = "live"
        tg("222", text="/start link-TELEGRAM01")
        wait_for(lambda: tg_to("222"))
        check("a t.me deep link links the Notifier to the chat, confirmed there",
              tg_to("222") and "Notifier" in tg_to("222")[-1]["text"], tg_to("222"))
        check("...labelled as Telegram", REDEEMS[-1]["label"].startswith("Telegram"), REDEEMS[-1])
        n_tg = len(tg_to("222"))
        tg("222", voice="f-short")
        hello = lambda chat, n: any(m["text"] == "hello world" for m in tg_to(chat)[n:])
        wait_for(lambda: nt_to("sub-telegram01", "transcript") and hello("222", n_tg), 40)
        check("a voice note in Telegram reaches both the chat and the Notifier",
              nt_to("sub-telegram01", "transcript") and hello("222", n_tg),
              (nt_to("sub-telegram01"), tg_to("222")[n_tg:]))
        CODES["EXPIRED002"] = "expired"
        n_tg = len(tg_to("222"))
        tg("222", text="/link EXPIR-ED002")
        wait_for(lambda: len(tg_to("222")) > n_tg)
        check("an expired code from Telegram is answered there (it is free)",
              "קוד" in tg_to("222")[-1]["text"], tg_to("222")[-1:])

        # ===== linking the same number to Telegram keeps the Notifier
        tg("333", text="/start")
        wait_for(lambda: tg_to("333"))
        code = re.search(r"link%20([0-9A-Z]{8})", json.dumps(tg_to("333")[0])).group(1)
        wa(num, "text", text=f"link {code}")
        wait_for(lambda: len(tg_to("333")) >= 2)
        before, n_tg = len(nt_to("sub-abcdefghjk", "transcript")), len(tg_to("333"))
        wa(num)
        wait_for(lambda: len(nt_to("sub-abcdefghjk", "transcript")) > before and hello("333", n_tg), 40)
        check("moving the number to a Telegram chat brings its Notifier along: both get transcripts",
              len(nt_to("sub-abcdefghjk", "transcript")) > before and hello("333", n_tg),
              (len(nt_to("sub-abcdefghjk", "transcript")), tg_to("333")[n_tg:]))

        # ===== /unlink in that chat leaves the Notifier linked
        tg("333", text="/unlink")
        wait_for(lambda: any("נותק" in m["text"] for m in tg_to("333")))
        check("/unlink with a Notifier linked detaches just the chat",
              not q("SELECT 1 FROM identities WHERE channel = 'telegram' AND address = '333'")
              and q("SELECT 1 FROM identities WHERE channel = 'whatsapp' AND address = %s", num)
              and q("SELECT 1 FROM identities WHERE channel = 'notifier' AND address = 'sub-abcdefghjk'"))

        # ===== the Notifier throttles once: retried, delivered
        THROTTLE_ONCE.add("sub-abcdefghjk")
        before = len(nt_to("sub-abcdefghjk", "transcript"))
        wa(num)
        wait_for(lambda: len(nt_to("sub-abcdefghjk", "transcript")) > before, 40)
        check("a 429 from the Notifier is retried until delivered",
              len(nt_to("sub-abcdefghjk", "transcript")) == before + 1)

        # ===== unlinked in the app: 410 unbinds
        GONE.add("sub-abcdefghjk")
        wa(num)
        wait_for(lambda: not q("SELECT 1 FROM identities WHERE channel = 'notifier' AND address = 'sub-abcdefghjk'"), 40)
        check("a 410 (unlinked in the app) unbinds the Notifier identity",
              not q("SELECT 1 FROM identities WHERE channel = 'notifier' AND address = 'sub-abcdefghjk'"))

        # ===== dashboard
        st, body = http("GET", "/api/stats")
        s = json.loads(body)
        check("dashboard counts Notifier sends and links, and knows the app's address",
              s["totals"]["nt_sent"] >= 3 and s["linked_notifier"] >= 1
              and s["notifier_url"] == "https://notifier.example",
              (s["totals"].get("nt_sent"), s.get("linked_notifier"), s.get("notifier_url")))

        EDGE.stop_event.set()
        hub.send_signal(15)
        hub.wait(timeout=30)
    finally:
        if hub.poll() is None:
            hub.kill(); hub.wait()
        hub_log.close()
        conn.close()
        subprocess.run(["docker", "rm", "-f", "eliezer-p4-pg"], capture_output=True)

    log = open(os.path.join(SP, "hub4.log")).read()
    check("hub logged no tracebacks", "Traceback" not in log)
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
    os.environ.update(QUEUE_API_URL=HUB, QUEUE_TOKEN=QTOK, INSTANCE_ID="edge-n",
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
