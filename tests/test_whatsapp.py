"""Phase 2 end-to-end: real hub + real edge, a fake WhatsApp Graph API, real audio.
Only the model call is stubbed. Run from the repo root; see tests/README.md."""

import asyncio
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import threading
import time
import types
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import support  # noqa: E402

SP = support.workdir()
FIX = support.make_fixtures(SP)
HUB_PORT = 18301
HUB = f"http://127.0.0.1:{HUB_PORT}"
SECRET = "meta-secret"
QTOK = "queue-token"
WA_TOKEN = "wa-token"

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)


def free_port():
    s = socket.socket(); s.bind(("", 0)); p = s.getsockname()[1]; s.close(); return p


# ---------------------------------------------------------------- fake Graph API
MEDIA = {}          # media_id -> (path, mime)
SENT = []           # recorded POSTs to /PNID/messages
SENT_LOCK = threading.Lock()
FAIL_ONCE = {}      # (to, marker) -> remaining failures; marker matched against text body
PERMANENT = {}      # to -> error code (400)


class Graph(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def _authed(self):
        return self.headers.get("Authorization") == f"Bearer {WA_TOKEN}"

    def do_GET(self):
        if not self._authed():
            return self._json(401, {"error": "bad token"})
        parts = self.path.strip("/").split("/")
        if parts[0] == "dl" and parts[1] in MEDIA:
            path, mime = MEDIA[parts[1]]
            data = open(path, "rb").read()
            self.send_response(200); self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
            return
        if len(parts) == 1 and parts[0] in MEDIA:
            return self._json(200, {"url": f"http://127.0.0.1:{self.server.server_port}/dl/{parts[0]}",
                                    "mime_type": MEDIA[parts[0]][1]})
        self._json(404, {"error": "no such media"})

    def do_POST(self):
        if not self._authed():
            return self._json(401, {"error": "bad token"})
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        to = data.get("to")
        text = (data.get("text") or {}).get("body", "")
        with SENT_LOCK:
            if to in PERMANENT:
                return self._json(400, {"error": {"code": PERMANENT[to], "message": "undeliverable"}})
            for (fto, marker), left in list(FAIL_ONCE.items()):
                if fto == to and marker in text and left > 0:
                    FAIL_ONCE[(fto, marker)] = left - 1
                    return self._json(500, {"error": {"code": 1, "message": "transient"}})
            SENT.append(data)
        self._json(200, {"messages": [{"id": "wamid.out"}]})


NOTICE = "שלום,\n\nוואטסאפ מתחילה"


def all_texts_to(to):
    with SENT_LOCK:
        return [d for d in SENT if d.get("type") == "text" and d.get("to") == to]


def texts_to(to):
    """Replies other than the pricing notice (checked on its own)."""
    return [d for d in all_texts_to(to) if not d["text"]["body"].startswith(NOTICE)]


def notices_to(to):
    return [d for d in all_texts_to(to) if d["text"]["body"].startswith(NOTICE)]


def receipts_for(mid):
    with SENT_LOCK:
        return [d for d in SENT if d.get("status") == "read" and d.get("message_id") == mid]


# ---------------------------------------------------------------- helpers
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


def edge_headers(inst="manual", protocol=2):
    h = {"Authorization": f"Bearer {QTOK}", "X-Instance-Id": inst}
    if protocol == 2:
        h["X-Edge-Protocol"] = "2"
    return h


def wa(frm, mid, mtype, **kw):
    m = {"from": frm, "id": mid, "timestamp": str(int(time.time())), "type": mtype}
    if mtype == "text":
        m["text"] = {"body": kw["text"]}
    elif mtype in ("audio", "document"):
        m[mtype] = {"id": kw["media"], "mime_type": MEDIA[kw["media"]][1]}
    elif mtype == "sticker":
        m["sticker"] = {"id": "stk"}
    return m


def post(*messages):
    payload = {"object": "whatsapp_business_account", "entry": [{"id": "WABA", "changes": [{
        "field": "messages", "value": {"messaging_product": "whatsapp",
                                       "metadata": {"phone_number_id": "PNID"},
                                       "messages": list(messages)}}]}]}
    raw = json.dumps(payload).encode()
    sig = hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return http("POST", "/webhook/whatsapp", raw=raw, headers={"X-Hub-Signature-256": "sha256=" + sig})


def wait_for(cond, timeout=40, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return cond()


# ---------------------------------------------------------------- main
def main():
    for mid, (fn, mime) in {"m-short": ("short.ogg", "audio/ogg"), "m-medium": ("medium.ogg", "audio/ogg"),
                             "m-long": ("long.ogg", "audio/ogg"), "m-mp3": ("song.mp3", "audio/mpeg"),
                             "m-garbage": ("garbage.bin", "application/pdf")}.items():
        MEDIA[mid] = (os.path.join(FIX, fn), mime)
    graph = ThreadingHTTPServer(("127.0.0.1", 0), Graph)
    threading.Thread(target=graph.serve_forever, daemon=True).start()
    graph_url = f"http://127.0.0.1:{graph.server_port}"

    pg = free_port()
    subprocess.run(["docker", "rm", "-f", "eliezer-p2-pg"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "eliezer-p2-pg", "-e", "POSTGRES_PASSWORD=pw",
                    "-p", f"127.0.0.1:{pg}:5432", "postgres:16-alpine"], check=True, capture_output=True)
    wait_for(lambda: subprocess.run(["docker", "exec", "eliezer-p2-pg", "pg_isready", "-U", "postgres"],
                                    capture_output=True).returncode == 0, 30, 0.5)
    time.sleep(1)
    db = f"postgresql://postgres:pw@127.0.0.1:{pg}/postgres"

    env = {k: v for k, v in os.environ.items() if not k.startswith(("POSTHOG", "WHATSAPP", "STATUS"))}
    env.update(DATABASE_URL=db, INGEST_TOKEN="ingest", QUEUE_TOKEN=QTOK, META_APP_SECRET=SECRET,
               META_VERIFY_TOKEN="vt", WHATSAPP_API_TOKEN=WA_TOKEN, WHATSAPP_PHONE_NUMBER_ID="PNID",
               WHATSAPP_GRAPH_URL=graph_url, STATUS_USER_SALT="salt", NUDGE_INTERVAL="1000000",
               USER_MAX_MESSAGES_PER_HOUR="3", QUEUE_VISIBILITY_TIMEOUT="3", QUEUE_MAX_RECEIVES="3",
               QUEUE_SWEEP_INTERVAL_SECONDS="1", LOG_LEVEL="DEBUG", PORT=str(HUB_PORT))
    hubdir = os.path.join(REPO, "status_site")
    py = sys.executable
    subprocess.run([py, "-c", "import app; app.init_db(); app.init_queue_db()"], cwd=hubdir, env=env, check=True)
    hub_log = open(os.path.join(SP, "hub2.log"), "w")
    hub = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                            "--port", str(HUB_PORT), "--workers", "1", "--no-access-log"],
                           cwd=hubdir, env=env, stdout=hub_log, stderr=subprocess.STDOUT)
    conn = psycopg.connect(db, autocommit=True)
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    bot = None
    try:
        wait_for(lambda: _up(), 20)

        # ===== K: protocol 2 by hand - media proxy, admit, complete, idempotency, stale
        post(wa("972500000011", "k1", "audio", media="m-short"))
        st, body = http("POST", "/queue/lease", {"max": 1, "wait": 2, "uptime_seconds": 12}, edge_headers())
        jobs = json.loads(body)["jobs"]
        check("v2 lease returns a job, not the raw body",
              len(jobs) == 1 and jobs[0]["kind"] == "audio" and "body" not in jobs[0], jobs)
        h = jobs[0]["handle"]
        st, media = http("GET", f"/queue/media/{h}", headers=edge_headers())
        check("media streams through the hub byte-for-byte",
              st == 200 and media == open(MEDIA["m-short"][0], "rb").read(), (st, len(media)))
        st, _ = http("GET", "/queue/media/not-a-handle", headers=edge_headers())
        check("media for an unknown handle -> 404", st == 404, st)
        st, body = http("POST", "/queue/admit", {"handle": h, "duration": 3.0}, edge_headers())
        check("admit ok for a 3s file", st == 200 and json.loads(body)["ok"] is True, body)
        st1, _ = http("POST", "/queue/complete", {"handle": h, "text": "manual transcript",
                                                   "transcription_seconds": 0.5, "duration": 3.0}, edge_headers())
        st2, _ = http("POST", "/queue/complete", {"handle": h, "text": "manual transcript"}, edge_headers())
        check("complete, and a retry of it, both succeed", st1 == 200 and st2 == 200, (st1, st2))
        wait_for(lambda: texts_to("972500000011"), 10)
        time.sleep(1.5)
        replies = texts_to("972500000011")
        check("exactly one reply, quoting the voice note",
              len(replies) == 1 and replies[0]["text"]["body"] == "manual transcript"
              and replies[0].get("context", {}).get("message_id") == "k1", replies)
        check("read receipt at ingest, typing indicator at admit",
              len(receipts_for("k1")) == 2 and any("typing_indicator" in r for r in receipts_for("k1")),
              receipts_for("k1"))
        inst = q("SELECT uptime_seconds, last_ip FROM instances WHERE instance_id = 'manual'")
        check("a v2 lease is the edge's heartbeat", inst and inst[0][0] == 12 and inst[0][1] == "127.0.0.1", inst)

        post(wa("972500000012", "k2", "audio", media="m-short"))
        old = json.loads(http("POST", "/queue/lease", {"max": 1, "wait": 2}, edge_headers())[1])["jobs"][0]["handle"]
        time.sleep(3.3)
        new = json.loads(http("POST", "/queue/lease", {"max": 1, "wait": 2}, edge_headers("other"))[1])["jobs"]
        check("lapsed lease is re-leased with a new handle", new and new[0]["handle"] != old)
        st, _ = http("POST", "/queue/complete", {"handle": old, "text": "late"}, edge_headers())
        check("complete with the stale handle -> 409", st == 409, st)
        st, _ = http("POST", "/queue/complete", {"handle": new[0]["handle"], "text": "on time"}, edge_headers("other"))
        wait_for(lambda: texts_to("972500000012"), 10)
        time.sleep(1)
        check("only the current lease's transcript is sent",
              [r["text"]["body"] for r in texts_to("972500000012")] == ["on time"], texts_to("972500000012"))

        # ===== the real edge
        start_edge()
        from whatsapp_bot import WhatsAppBot
        global EDGE
        EDGE = WhatsAppBot(num_workers=2)
        threading.Thread(target=EDGE.run, daemon=True).start()

        FAIL_ONCE[("972500000008", "[2/2]")] = 1   # second part fails once
        FAIL_ONCE[("972500000009", "hello")] = 1   # whole reply fails once
        PERMANENT["972500000010"] = 131026        # undeliverable
        post(wa("972500000001", "a1", "audio", media="m-short"))
        post(wa("972500000002", "b1", "text", text="/status"), wa("972500000002", "b2", "text", text="hi"))
        post(wa("972500000003", "c1", "sticker"))
        post(wa("919812345678", "d1", "audio", media="m-short"))
        post(wa("972500000004", "e1", "audio", media="m-long"))
        post(*[wa("972500000005", f"f{i}", "audio", media="m-short") for i in range(4)])
        post(wa("972500000006", "g1", "document", media="m-mp3"), wa("972500000007", "g2", "document", media="m-garbage"))
        post(wa("972500000008", "h1", "audio", media="m-medium"))
        post(wa("972500000009", "i1", "audio", media="m-short"))
        post(wa("972500000010", "j1", "audio", media="m-short"))
        st, _ = post(wa("972500000001", "a1", "audio", media="m-short"))  # Meta re-delivery

        ok = wait_for(lambda: len(texts_to("972500000005")) >= 3 and texts_to("972500000009")
                      and len(texts_to("972500000008")) >= 2 and texts_to("972500000006") and texts_to("972500000001")
                      and q("SELECT count(*) FROM queue_messages")[0][0] == 0, 60)
        time.sleep(2)
        check("all expected transcripts arrived and every job was closed", ok)

        a = texts_to("972500000001")
        check("voice note -> one transcript, quoted (re-delivery ignored)",
              len(a) == 1 and a[0]["text"]["body"] == "hello world" and a[0]["context"]["message_id"] == "a1", a)
        check("/status and stray text get no WhatsApp reply (it would be billed)", not all_texts_to("972500000002"))
        check("text is still marked read", receipts_for("b1") and receipts_for("b2"))
        check("sticker: no reply, no receipt", not texts_to("972500000003") and not receipts_for("c1"))
        check("non-allowed region: ignored - no reply, no receipt, nothing queued",
              not all_texts_to("919812345678") and not receipts_for("d1"))
        check("over 10 minutes: refused at admit without a WhatsApp message", not all_texts_to("972500000004"))
        f = [r["text"]["body"] for r in texts_to("972500000005")]
        check("rate limit is fleet-wide: 3 transcripts, and the 4th refused silently",
              f == ["hello world"] * 3, f)
        check("mp3 document converted and transcribed",
              [r["text"]["body"] for r in texts_to("972500000006")] == ["hello world"], texts_to("972500000006"))
        check("non-audio document: no WhatsApp reply", not all_texts_to("972500000007"))
        h8 = texts_to("972500000008")
        firsts = [x for x in h8 if x["text"]["body"].startswith("[1/2]")]
        seconds = [x for x in h8 if x["text"]["body"].startswith("[2/2]")]
        check("long transcript split in two, first part quoted",
              firsts and seconds and firsts[0].get("context", {}).get("message_id") == "h1"
              and "context" not in seconds[0], [x["text"]["body"][:6] for x in h8])
        check("a failed part resumes: part 1 sent once, part 2 once after retry",
              len(firsts) == 1 and len(seconds) == 1, (len(firsts), len(seconds)))
        check("transient send failure retried and delivered", len(texts_to("972500000009")) == 1)
        wait_for(lambda: not q("SELECT 1 FROM outbox WHERE address = '972500000010'"), 10)
        check("permanently undeliverable reply dropped, not retried",
              not q("SELECT 1 FROM outbox WHERE address = '972500000010'") and not texts_to("972500000010"))
        seq = [d["text"]["body"] for d in all_texts_to("972500000001")]
        check("each WhatsApp transcript is followed by the pricing notice",
              len(seq) == 2 and seq[0] == "hello world" and seq[1].startswith(NOTICE) and "status.eliezer.ivrit.ai" in seq[1], seq)
        check("...once per transcript (3 transcripts, 3 notices; none for the refusal)",
              len(notices_to("972500000005")) == 3, len(notices_to("972500000005")))
        check("...and never after a refusal or a non-audio reply",
              not notices_to("972500000004") and not notices_to("972500000007") and not notices_to("919812345678")
              and not notices_to("972500000002"))
        check("...and it is not sent unquoted-first: it follows the long transcript's last part",
              all_texts_to("972500000008")[-1]["text"]["body"].startswith(NOTICE)
              and "context" not in all_texts_to("972500000008")[-1])
        check("queue drained", q("SELECT count(*) FROM queue_messages")[0][0] == 0)
        check("outbox drained", q("SELECT count(*) FROM outbox")[0][0] == 0)

        # ===== stats
        with SENT_LOCK:
            wa_texts = sum(1 for d in SENT if d.get("type") == "text")
        s = json.loads(http("GET", "/api/stats")[1])
        check("wa_sent counts every WhatsApp text sent", s["totals"]["wa_sent"] == wa_texts,
              (s["totals"]["wa_sent"], wa_texts))
        by_inst = dict(q("SELECT instance_id, count(*) FROM events WHERE kind = 'message' GROUP BY 1"))
        check("media messages counted on the edge, text/other on the hub",
              by_inst.get("hub") == 3 and by_inst.get("edge-t", 0) >= 11, by_inst)
        tr = q("SELECT count(*), round(sum(duration_seconds)) FROM events WHERE kind = 'transcription'")
        check("transcriptions recorded with durations (2 manual + 8 edge, 28s audio)", tr[0][0] == 10 and tr[0][1] == 28, tr)
        check("dashboard queue depth is the hub's live depth", s["queue_depth"] == 0, s["queue_depth"])
        hashes = q("SELECT DISTINCT user_hash FROM events WHERE user_hash IS NOT NULL")
        check("users stored only as salted hashes", all(len(h[0]) == 16 for h in hashes) and
              not q("SELECT 1 FROM events WHERE user_hash LIKE '9725%%'"))

        # ===== outbox expiry
        q("INSERT INTO outbox (channel, address, action, payload, sent_at, next_attempt_at) VALUES "
          "('whatsapp', '972599999999', 'text', '{\"parts\": []}', now() - interval '3 hours 1 minute', now() + interval '1 hour') RETURNING id")
        time.sleep(1.8)
        check("reply past max age swept", not q("SELECT 1 FROM outbox WHERE address = '972599999999'"))

        # ===== SIGTERM with the edge long-polling and senders running
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
        subprocess.run(["docker", "rm", "-f", "eliezer-p2-pg"], capture_output=True)
        graph.shutdown()

    log = open(os.path.join(SP, "hub2.log")).read()
    check("hub logged no tracebacks", "Traceback" not in log)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    os._exit(1 if failed else 0)


def _up():
    try:
        return http("GET", "/favicon.ico")[0] == 200
    except OSError:
        return False


def start_edge():
    # The edge calls load_dotenv(), which fills any *unset* variable from the repo's real
    # .env; everything it could pick up is set here explicitly.
    os.environ.update(QUEUE_API_URL=HUB, QUEUE_TOKEN=QTOK, INSTANCE_ID="edge-t",
                      RUNPOD_API_KEY="dummy", RUNPOD_ENDPOINT_ID="dummy",
                      STATUS_SITE_URL="", STATUS_INGEST_TOKEN="", POSTHOG_API_KEY="",
                      WHATSAPP_API_TOKEN="", APP_SQS_QUEUE="")
    os.environ.pop("PORT", None)
    os.chdir(SP)
    if os.path.exists("whatsapp_bot.log"):
        os.unlink("whatsapp_bot.log")
    sys.path.insert(0, REPO)
    import whatsapp_bot

    async def fake_segments(self, audio_path):
        # 5s fixture -> a transcript long enough to need two WhatsApp messages.
        long = abs(self.check_audio_duration(audio_path) - 5.0) < 0.5
        return [types.SimpleNamespace(text=("א" * 4500) if long else "hello world")]
    whatsapp_bot.WhatsAppBot._collect_transcription_segments = fake_segments


if __name__ == "__main__":
    main()
