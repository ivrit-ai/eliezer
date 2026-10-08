"""The ivrit.ai app as a source, end to end: Google ID tokens (signed here by a stand-in
for Google's keys), uploads, the real edge transcribing them with only the model
stubbed, refusals, per-account limits, and the app's cross-origin access. Run from the
repo root; see tests/README.md."""

import json
import os
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

import jwt
import psycopg
from cryptography.hazmat.primitives.asymmetric import rsa

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import support  # noqa: E402

SP = support.workdir()
HUB_PORT = 18501
HUB = f"http://127.0.0.1:{HUB_PORT}"
QTOK = "queue-token"
CLIENT_ID = "test-app.apps.googleusercontent.com"
APP_ORIGIN = "https://app.example.test"
PER_HOUR = 4

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))


def free_port():
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def http(method, path, raw=None, headers=None, timeout=30):
    r = urllib.request.Request(HUB + path, data=raw, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


# --- a stand-in for Google: one RSA key, its JWKS served locally, tokens minted here

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
KID = "test-kid"


class Certs(BaseHTTPRequestHandler):
    def do_GET(self):
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(KEY.public_key()))
        jwk.update(kid=KID, use="sig", alg="RS256")
        body = json.dumps({"keys": [jwk]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def token(sub, aud=CLIENT_ID, iss="https://accounts.google.com", ttl=3600, key=None, verified=True):
    now = int(time.time())
    claims = {"iss": iss, "aud": aud, "sub": sub, "email": f"{sub}@example.com", "email_verified": verified,
              "iat": now, "exp": now + ttl}
    return jwt.encode(claims, key or KEY, algorithm="RS256", headers={"kid": KID})


def auth(sub, **kw):
    return {"Authorization": f"Bearer {token(sub, **kw)}"}


def upload(sub, path, mime, name=None, headers=None):
    with open(path, "rb") as f:
        data = f.read()
    h = {"Content-Type": mime, "X-Filename": urllib.parse.quote(name or os.path.basename(path))}
    h.update(auth(sub) if headers is None else headers)
    st, body, _ = http("POST", "/app/v1/jobs", raw=data, headers=h)
    return st, (json.loads(body) if body.startswith(b"{") else body)


def job(sub, job_id):
    st, body, _ = http("GET", f"/app/v1/jobs/{job_id}", headers=auth(sub))
    return st, (json.loads(body) if st == 200 else None)


def wait_for(fn, seconds):
    end = time.time() + seconds
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(0.3)
    return fn()


def finished(sub, job_id):
    return wait_for(lambda: (lambda r: r[1] if r[0] == 200 and r[1]["status"] != "queued" else None)(job(sub, job_id)), 60)


def start_edge():
    # The edge calls load_dotenv(), which fills any *unset* variable from the repo's real
    # .env; everything it could pick up is set here explicitly.
    os.environ.update(QUEUE_API_URL=HUB, QUEUE_TOKEN=QTOK, INSTANCE_ID="edge-app",
                      RUNPOD_API_KEY="dummy", RUNPOD_ENDPOINT_ID="dummy",
                      STATUS_SITE_URL="", STATUS_INGEST_TOKEN="", POSTHOG_API_KEY="",
                      WHATSAPP_API_TOKEN="", APP_SQS_QUEUE="")
    os.environ.pop("PORT", None)
    os.chdir(SP)
    sys.path.insert(0, REPO)
    import whatsapp_bot

    async def fake_segments(self, audio_path):
        return [types.SimpleNamespace(text="שלום עולם")]
    whatsapp_bot.WhatsAppBot._collect_transcription_segments = fake_segments
    bot = whatsapp_bot.WhatsAppBot(num_workers=2)
    threading.Thread(target=bot.run, daemon=True).start()


def main():
    fixtures = support.make_fixtures(SP)
    certs = ThreadingHTTPServer(("127.0.0.1", 0), Certs)
    threading.Thread(target=certs.serve_forever, daemon=True).start()

    pg_port = free_port()
    subprocess.run(["docker", "rm", "-f", "eliezer-app-pg"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "eliezer-app-pg", "-e", "POSTGRES_PASSWORD=pw",
                    "-p", f"127.0.0.1:{pg_port}:5432", "postgres:16-alpine"], check=True, capture_output=True)
    for _ in range(60):
        if subprocess.run(["docker", "exec", "eliezer-app-pg", "pg_isready", "-U", "postgres"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(0.5)
    time.sleep(1)
    db = f"postgresql://postgres:pw@127.0.0.1:{pg_port}/postgres"

    env = dict(os.environ, DATABASE_URL=db, QUEUE_TOKEN=QTOK, LOG_LEVEL="DEBUG", PORT=str(HUB_PORT),
               WHATSAPP_GRAPH_URL="http://127.0.0.1:9", TELEGRAM_API_URL="http://127.0.0.1:9",
               APP_GOOGLE_CLIENT_IDS=CLIENT_ID,
               APP_GOOGLE_JWKS_URL=f"http://127.0.0.1:{certs.server_address[1]}/certs",
               APP_ORIGINS=APP_ORIGIN, USER_MAX_MESSAGES_PER_HOUR=str(PER_HOUR),
               APP_TOKEN_ISSUER=APP_ORIGIN,
               APP_TOKEN_JWKS_URL=f"http://127.0.0.1:{certs.server_address[1]}/certs",
               QUEUE_SWEEP_INTERVAL_SECONDS="1")
    hubdir = os.path.join(REPO, "status_site")
    subprocess.run([sys.executable, "-c", "import app; app.init_db(); app.init_queue_db()"],
                   cwd=hubdir, env=env, check=True)
    hub_log = open(os.path.join(SP, "hub.log"), "w")
    hub = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                            "--port", str(HUB_PORT), "--workers", "1", "--no-access-log"],
                           cwd=hubdir, env=env, stdout=hub_log, stderr=subprocess.STDOUT)
    conn = psycopg.connect(db, autocommit=True)
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    short, mp3 = os.path.join(fixtures, "short.ogg"), os.path.join(fixtures, "song.mp3")
    try:
        wait_for(lambda: _up(), 15)

        # ===== who may ask
        st, _ = upload("alice", short, "audio/ogg", headers={})
        check("no token -> 401", st == 401, st)
        st, _ = upload("alice", short, "audio/ogg", headers=auth("alice", aud="someone-else"))
        check("token for another app -> 401", st == 401, st)
        st, _ = upload("alice", short, "audio/ogg", headers=auth("alice", iss="https://evil.example"))
        check("token from another issuer -> 401", st == 401, st)
        st, _ = upload("alice", short, "audio/ogg", headers=auth("alice", ttl=-600))
        check("expired token -> 401", st == 401, st)
        forged = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        st, _ = upload("alice", short, "audio/ogg", headers=auth("alice", key=forged))
        check("token signed by another key -> 401", st == 401, st)
        st, _ = upload("alice", short, "audio/ogg", headers=auth("alice", verified=False))
        check("unverified email -> 401", st == 401, st)

        # ===== what may be sent
        st, _, _ = http("POST", "/app/v1/jobs", raw=b"hello", headers={**auth("alice"), "Content-Type": "text/plain"})
        check("not audio -> 415", st == 415, st)
        # Refused on the declared length, before the body is read: the hub may answer
        # 413 or close the connection while the client is still sending.
        try:
            st, _, _ = http("POST", "/app/v1/jobs", raw=b"0" * (20 * 1024 * 1024 + 1),
                            headers={**auth("alice"), "Content-Type": "audio/ogg"})
        except urllib.error.URLError as e:
            st = "reset" if isinstance(e.reason, (ConnectionResetError, BrokenPipeError)) else e
        check("over 20 MB -> refused", st in (413, "reset"), st)

        # ===== queued until an edge comes; at most three waiting per account
        ids = []
        for i in range(3):
            st, body = upload("alice", short, "audio/ogg", name=f"הקלטה {i}.ogg")
            check(f"upload {i + 1} accepted (202)", st == 202 and body.get("status") == "queued", (st, body))
            ids.append(body["job_id"])
        st, _ = upload("alice", short, "audio/ogg")
        check("a fourth waiting upload -> 429", st == 429, st)
        st, view = job("alice", ids[0])
        check("a job reads back queued, with its name",
              st == 200 and view["status"] == "queued" and view["filename"] == "הקלטה 0.ogg", view)
        st, _ = job("bob", ids[0])
        check("another account cannot see it (404)", st == 404, st)
        check("the queue holds them as app jobs",
              q("SELECT count(*) FROM queue_messages WHERE source = 'app'")[0][0] == 3)

        # ===== the real edge
        start_edge()
        done = [finished("alice", i) for i in ids]
        check("all three transcribed", all(d and d["status"] == "done" and d["text"] == "שלום עולם" for d in done), done)
        check("with their duration", all(d and 2.5 < d.get("duration", 0) < 3.5 for d in done), done)
        check("the audio is gone once done",
              q("SELECT count(*) FROM app_jobs WHERE audio IS NOT NULL")[0][0] == 0)
        check("nothing went to the outbox", q("SELECT count(*) FROM outbox")[0][0] == 0)

        st, body = upload("alice", mp3, "audio/mpeg")
        d = finished("alice", body["job_id"])
        check("an mp3 is converted and transcribed", d and d["status"] == "done", d)

        st, body = upload("alice", os.path.join(fixtures, "long.ogg"), "audio/ogg")
        d = finished("alice", body["job_id"])
        check("over 10 minutes -> failed, too_long", d and d["status"] == "failed" and d["error"] == "too_long", d)

        st, body = upload("alice", os.path.join(fixtures, "garbage.bin"), "audio/mpeg")
        d = finished("alice", body["job_id"])
        check("not audio at all -> failed", d and d["status"] == "failed" and d["error"] in ("unsupported", "duration_failed"), d)

        # Four admitted this hour (three clips and the mp3): the account's limit.
        st, body = upload("alice", short, "audio/ogg")
        d = finished("alice", body["job_id"])
        check("past the hourly limit -> rate_limited, with a wait",
              d and d["error"] == "rate_limited" and d.get("wait_minutes", 0) >= 1, d)
        st, body = upload("bob", short, "audio/ogg")
        d = finished("bob", body["job_id"])
        check("another account has its own limit", d and d["status"] == "done", d)

        # ===== a re-sent upload (the app died before it saw the answer) is the same job
        h = {**auth("carol"), "X-Upload-Id": "upload-0001-abcd", "X-Origin": "whatsapp"}
        st, first = upload("carol", short, "audio/ogg", headers=h)
        st2, again = upload("carol", short, "audio/ogg", headers=h)
        check("first send of an upload -> 202", st == 202, (st, first))
        check("a re-send -> 200, the same job", st2 == 200 and again["job_id"] == first["job_id"], (st2, again))
        check("only one job made", q("SELECT count(*) FROM app_jobs WHERE google_sub = 'carol'")[0][0] == 1)
        d = finished("carol", first["job_id"])
        check("the job keeps its upload id and origin",
              d and d.get("upload_id") == "upload-0001-abcd" and d.get("origin") == "whatsapp", d)
        st, _ = upload("carol", short, "audio/ogg", headers={**auth("carol"), "X-Upload-Id": "x"})
        check("a malformed upload id -> 400", st == 400, st)
        st, other = upload("dave", short, "audio/ogg", headers={**auth("dave"), "X-Upload-Id": "upload-0001-abcd"})
        check("the same upload id from another account is its own job",
              st == 202 and other["job_id"] != first["job_id"], (st, other))

        # ===== the app's own session, for the same account
        session = lambda **kw: auth("carol", iss=APP_ORIGIN, aud="ivrit-app", **kw)
        st, body, _ = http("GET", "/app/v1/jobs", headers=session())
        check("the app's session sees the account's jobs",
              st == 200 and [j["job_id"] for j in json.loads(body)["jobs"]] == [first["job_id"]], (st, body[:200]))
        st, _, _ = http("GET", "/app/v1/jobs", headers=auth("carol", iss=APP_ORIGIN, aud="someone-else"))
        check("a session meant for others -> 401", st == 401, st)
        st, _, _ = http("GET", "/app/v1/jobs", headers=session(ttl=-600))
        check("an expired session -> 401", st == 401, st)

        st, body, _ = http("GET", "/app/v1/jobs", headers=auth("alice"))
        listing = json.loads(body)["jobs"] if st == 200 else []
        check("the account's jobs, newest first", len(listing) == 7 and listing[0]["error"] == "rate_limited",
              [j.get("error") or j["status"] for j in listing])

        # ===== the app's pages call this from their own origin
        st, _, h = http("OPTIONS", "/app/v1/jobs", headers={
            "Origin": APP_ORIGIN, "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization,content-type,x-filename"})
        check("preflight from the app's origin allowed",
              st == 200 and h.get("access-control-allow-origin") == APP_ORIGIN, (st, h))
        st, _, h = http("GET", "/app/v1/jobs", headers={**auth("alice"), "Origin": "https://evil.example"})
        check("no CORS grant for other origins", "access-control-allow-origin" not in {k.lower() for k in h}, h)
    finally:
        failed = [n for n, ok in results if not ok]
        print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; logs in {SP}" if failed else ""))
        hub.terminate()
        certs.shutdown()
        subprocess.run(["docker", "rm", "-f", "eliezer-app-pg"], capture_output=True)
    os._exit(1 if failed else 0)


def _up():
    try:
        return http("GET", "/favicon.ico")[0] == 200
    except OSError:
        return False


if __name__ == "__main__":
    main()
