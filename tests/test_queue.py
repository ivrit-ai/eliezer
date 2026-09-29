"""Phase 1 end-to-end: the hub (webhooks + queue) and the real edge dispatcher/workers,
against a throwaway Postgres. Run from the repo root; see tests/README.md."""

import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import support  # noqa: E402

SP = support.workdir()
HUB_PORT = 18201
HUB = f"http://127.0.0.1:{HUB_PORT}"
SECRET = "meta-app-secret"
VERIFY = "verify-me"
QUEUE_TOKEN = "shared-queue-token"
MAX_AGE = 10800

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


def signed(payload):
    raw = json.dumps(payload).encode()
    sig = hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return raw, {"X-Hub-Signature-256": "sha256=" + sig}


def wa_msg(mid, ts=None, frm="972500000001"):
    return {"from": frm, "id": mid, "timestamp": str(int(ts or time.time())),
            "type": "audio", "audio": {"id": "media-" + mid, "mime_type": "audio/ogg"}}


def wa_payload(messages=None, statuses=None):
    value = {"messaging_product": "whatsapp",
             "metadata": {"display_phone_number": "15550000000", "phone_number_id": "PNID"},
             "contacts": [{"profile": {"name": "Tester"}, "wa_id": "972500000001"}]}
    if messages is not None:
        value["messages"] = messages
    if statuses is not None:
        value["statuses"] = statuses
    return {"object": "whatsapp_business_account",
            "entry": [{"id": "WABA", "changes": [{"field": "messages", "value": value}]}]}


def post_webhook(payload):
    raw, h = signed(payload)
    return http("POST", "/webhook/whatsapp", headers=h, raw=raw)


def edge(tok=QUEUE_TOKEN, inst="edge-a"):
    h = {"Authorization": "Bearer " + tok, "X-Edge-Protocol": "2"}
    if inst:
        h["X-Instance-Id"] = inst
    return h


def main():
    pg_port = free_port()
    subprocess.run(["docker", "rm", "-f", "eliezer-p1-pg"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "eliezer-p1-pg", "-e", "POSTGRES_PASSWORD=pw",
                    "-p", f"127.0.0.1:{pg_port}:5432", "postgres:16-alpine"], check=True, capture_output=True)
    for _ in range(60):
        if subprocess.run(["docker", "exec", "eliezer-p1-pg", "pg_isready", "-U", "postgres"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(0.5)
    time.sleep(1)
    db = f"postgresql://postgres:pw@127.0.0.1:{pg_port}/postgres"

    env = dict(os.environ, DATABASE_URL=db, INGEST_TOKEN="ingest", QUEUE_TOKEN=QUEUE_TOKEN,
               META_APP_SECRET=SECRET, META_VERIFY_TOKEN=VERIFY, QUEUE_VISIBILITY_TIMEOUT="2",
               QUEUE_MAX_RECEIVES="2", QUEUE_SWEEP_INTERVAL_SECONDS="1", LOG_LEVEL="DEBUG",
               PORT=str(HUB_PORT), WHATSAPP_GRAPH_URL="http://127.0.0.1:9")
    py = sys.executable
    hubdir = os.path.join(REPO, "status_site")
    subprocess.run([py, "-c", "import app; app.init_db(); app.init_queue_db()"], cwd=hubdir, env=env, check=True)
    hub_log = open(os.path.join(SP, "hub.log"), "w")
    hub = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                            "--port", str(HUB_PORT), "--workers", "1", "--no-access-log"],
                           cwd=hubdir, env=env, stdout=hub_log, stderr=subprocess.STDOUT)
    conn = psycopg.connect(db, autocommit=True)
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    try:
        for _ in range(50):
            try:
                http("GET", "/favicon.ico")
                break
            except OSError:
                time.sleep(0.2)

        # --- webhook registration handshake
        st, body = http("GET", f"/webhook/whatsapp?hub.mode=subscribe&hub.verify_token={VERIFY}&hub.challenge=4242")
        check("GET verify with right token echoes challenge", st == 200 and body == b"4242", (st, body))
        st, _ = http("GET", "/webhook/whatsapp?hub.mode=subscribe&hub.verify_token=wrong&hub.challenge=1")
        check("GET verify with wrong token -> 403", st == 403, st)
        st, _ = http("GET", "/webhook/nosuch?hub.mode=subscribe")
        check("unknown source -> 404", st == 404, st)

        # --- signature
        raw = json.dumps(wa_payload([wa_msg("m-unsigned")])).encode()
        st, _ = http("POST", "/webhook/whatsapp", raw=raw)
        check("unsigned delivery -> 403", st == 403, st)
        st, _ = http("POST", "/webhook/whatsapp", raw=raw, headers={"X-Hub-Signature-256": "sha256=" + "0" * 64})
        check("bad signature -> 403", st == 403, st)
        check("rejected deliveries queue nothing", q("SELECT count(*) FROM queue_messages")[0][0] == 0)

        st, _ = post_webhook(wa_payload([wa_msg("m1")]))
        check("signed delivery -> 200", st == 200, st)
        check("one row queued", q("SELECT count(*) FROM queue_messages")[0][0] == 1)
        st, _ = post_webhook(wa_payload([wa_msg("m1")]))
        check("re-delivered message is deduplicated", st == 200 and q("SELECT count(*) FROM queue_messages")[0][0] == 1)

        # --- batching: one delivery, three messages -> three single-message rows
        post_webhook(wa_payload([wa_msg("b1"), wa_msg("b2"), wa_msg("b3")]))
        rows = q("SELECT body FROM queue_messages WHERE body LIKE '%%\"b_\"%%' ORDER BY id")
        bodies = [json.loads(r[0]) for r in rows]
        ids = [b["entry"][0]["changes"][0]["value"]["messages"][0]["id"] for b in bodies]
        check("batched delivery split into one row per message", ids == ["b1", "b2", "b3"], ids)
        v = bodies[0]["entry"][0]["changes"][0]["value"]
        check("each row keeps metadata/contacts and exactly one message",
              len(v["messages"]) == 1 and v["metadata"]["phone_number_id"] == "PNID" and v["contacts"], v)

        # --- statuses-only delivery (receipts for our own replies)
        before = q("SELECT count(*) FROM queue_messages")[0][0]
        st, _ = post_webhook(wa_payload(statuses=[{"id": "wamid.s", "status": "delivered"}]))
        check("status-only delivery -> 200, nothing queued",
              st == 200 and q("SELECT count(*) FROM queue_messages")[0][0] == before)

        # --- already past max age at ingest
        dropped0 = q("SELECT dropped FROM totals")[0][0]
        post_webhook(wa_payload([wa_msg("stale", ts=time.time() - MAX_AGE - 60)]))
        check("message older than max age is not queued",
              q("SELECT count(*) FROM queue_messages WHERE body LIKE '%%\"stale\"%%'")[0][0] == 0)
        check("...and is counted as dropped", q("SELECT dropped FROM totals")[0][0] == dropped0 + 1)
        sent_at = q("SELECT extract(epoch FROM sent_at) FROM queue_messages WHERE body LIKE '%%\"m1\"%%'")[0][0]
        check("sent_at is the platform timestamp", abs(float(sent_at) - time.time()) < 30, sent_at)

        # --- edge auth
        st, _ = http("POST", "/queue/lease", {"max": 1})
        check("lease without token -> 401", st == 401, st)
        st, _ = http("POST", "/queue/lease", {"max": 1}, edge("nope"))
        check("lease with unknown token -> 401", st == 401, st)
        st, body = http("GET", "/queue/depth", headers=edge())
        check("depth reports 4 waiting", st == 200 and json.loads(body)["depth"] == 4, body)

        # --- overflow threshold: only what sits above min_depth
        st, body = http("POST", "/queue/lease", {"max": 10, "wait": 0, "min_depth": 3}, edge())
        got = json.loads(body)["jobs"]
        check("min_depth=3 with 4 waiting leases exactly 1", len(got) == 1, len(got))
        st, body = http("POST", "/queue/lease", {"max": 10, "wait": 0, "min_depth": 10}, edge())
        check("min_depth above depth leases nothing", json.loads(body)["jobs"] == [])
        m = got[0]
        check("lease returns a job: handle/kind/sent_at, no raw body",
              m["kind"] == "audio" and m["handle"] and "body" not in m and m["sent_at"] > 0, m)
        leased_by = q("SELECT leased_by FROM queue_messages WHERE receipt_handle = %s", m["handle"])
        check("lease is attributed to the edge", leased_by == [("edge-a",)], leased_by)
        st, body = http("POST", "/queue/lease", {"max": 10, "wait": 0}, edge(inst="edge-b"))
        rest = json.loads(body)["jobs"]
        check("leased message is invisible to another edge", m["handle"] not in [r["handle"] for r in rest] and len(rest) == 3)
        for r in rest + [m]:
            http("POST", "/queue/complete", {"handle": r["handle"], "text": "t"}, edge())
        post_webhook(wa_payload([wa_msg("anon")]))
        anon = json.loads(http("POST", "/queue/lease", {"max": 1, "wait": 0}, edge(inst=None))[1])["jobs"]
        check("valid token without X-Instance-Id is accepted as \"unknown\"",
              len(anon) == 1 and q("SELECT leased_by FROM queue_messages WHERE receipt_handle = %s", anon[0]["handle"]) == [("unknown",)])
        rest += anon
        for r in anon:
            http("POST", "/queue/complete", {"handle": r["handle"], "text": "t"}, edge())
        check("acks delete the rows", q("SELECT count(*) FROM queue_messages")[0][0] == 0)
        st, _ = http("POST", "/queue/complete", {"handle": m["handle"], "text": "t"}, edge())
        check("repeating a completion is a harmless no-op", st == 200, st)

        # --- poison: leased MAX_RECEIVES (2) times without ack -> dropped by the sweeper
        post_webhook(wa_payload([wa_msg("poison")]))
        dropped1 = q("SELECT dropped FROM totals")[0][0]
        a1 = json.loads(http("POST", "/queue/lease", {"max": 1, "wait": 0}, edge())[1])["jobs"]
        time.sleep(2.3)
        a2 = json.loads(http("POST", "/queue/lease", {"max": 1, "wait": 0}, edge())[1])["jobs"]
        check("unacked message is redelivered after the visibility timeout", len(a1) == 1 and len(a2) == 1)
        time.sleep(2.3)
        a3 = json.loads(http("POST", "/queue/lease", {"max": 1, "wait": 0}, edge())[1])["jobs"]
        check("never leased more than MAX_RECEIVES times", a3 == [], a3)
        time.sleep(1.5)
        check("poison message swept and counted",
              q("SELECT count(*) FROM queue_messages")[0][0] == 0 and q("SELECT dropped FROM totals")[0][0] == dropped1 + 1)

        # --- sweeper expires rows by age with no edge polling
        q("INSERT INTO queue_messages (source, body, sent_at) VALUES ('whatsapp', '{}', now() - interval '3 hours 1 minute') RETURNING id")
        time.sleep(1.8)
        check("row past max age swept without any lease", q("SELECT count(*) FROM queue_messages")[0][0] == 0)
        q("INSERT INTO ingest_seen (key, seen_at) VALUES ('whatsapp:old', now() - interval '4 hours') RETURNING key")
        time.sleep(1.8)
        check("dedup ids pruned at max age", q("SELECT count(*) FROM ingest_seen WHERE key = 'whatsapp:old'")[0][0] == 0)

        # --- long polls hold no threads: 50 idle waiters, dashboard still instant
        waiters, got_msgs = [], []
        def waiter():
            st, body = http("POST", "/queue/lease", {"max": 1, "wait": 8}, edge(), timeout=30)
            msgs = json.loads(body)["jobs"]
            for m in msgs:  # ack at once, as an edge does, so it can't resurface
                http("POST", "/queue/complete", {"handle": m["handle"], "text": "t"}, edge())
            got_msgs.extend(msgs)
        for _ in range(50):
            t = threading.Thread(target=waiter)
            t.start()
            waiters.append(t)
        time.sleep(1.0)
        t0 = time.time()
        st, body = http("GET", "/api/stats")
        stats_s = time.time() - t0
        check("/api/stats answers while 50 long-polls wait", st == 200 and stats_s < 1.0, f"{stats_s:.2f}s")
        check("/api/stats exposes totals.dropped", "dropped" in json.loads(body)["totals"])
        post_webhook(wa_payload([wa_msg("wakeup")]))
        for t in waiters:
            t.join()
        check("a queued message wakes exactly one waiter", len(got_msgs) == 1, len(got_msgs))

        # --- the real edge: dispatcher + workers + HttpQueueClient against the hub
        # (the edge itself is exercised end to end by test_whatsapp.py)

        # --- /queue/edges: instance -> reporting IP, token-protected
        http("POST", "/queue/lease", {"max": 1, "wait": 0, "uptime_seconds": 7}, edge(inst="ip-probe"))
        st_noauth, _ = http("GET", "/queue/edges")
        st, body = http("GET", "/queue/edges", headers=edge())
        found = {e["instance_id"]: e["ip"] for e in json.loads(body)["edges"]}
        check("/queue/edges requires the queue token", st_noauth == 401, st_noauth)
        check("/queue/edges maps instance to its reporting IP", found.get("ip-probe") == "127.0.0.1", found)
        st, body = http("GET", "/api/stats")
        check("public /api/stats does not expose IPs", b"127.0.0.1" not in body and b'"ip"' not in body)

        # --- SIGTERM with idle long-polls open (as on every redeploy) must not hang
        post_webhook(wa_payload([wa_msg("survivor")]))
        http("POST", "/queue/lease", {"max": 1, "wait": 0}, edge())  # leased, never acked
        idle = []
        def idle_poll():
            try:
                idle.append(http("POST", "/queue/lease", {"max": 1, "wait": 20}, edge(), timeout=30))
            except OSError as e:
                idle.append(("error", str(e)))
        pollers = [threading.Thread(target=idle_poll) for _ in range(5)]
        for t in pollers:
            t.start()
        time.sleep(1.0)
        t0 = time.time()
        hub.send_signal(15)
        hub.wait(timeout=30)
        took = time.time() - t0
        for t in pollers:
            t.join()
        check("hub exits promptly on SIGTERM despite open long-polls", took < 3.0, f"{took:.1f}s")
        check("open long-polls get a clean empty answer, not an error",
              all(r[0] == 200 and json.loads(r[1])["jobs"] == [] for r in idle), idle)
        check("an unacked lease survives the restart in the DB",
              q("SELECT count(*) FROM queue_messages WHERE body LIKE '%%survivor%%'")[0][0] == 1)
    finally:
        if hub.poll() is None:
            hub.kill()
            hub.wait()
        hub_log.close()
        conn.close()
        subprocess.run(["docker", "rm", "-f", "eliezer-p1-pg"], capture_output=True)

    log = open(os.path.join(SP, "hub.log")).read()
    check("hub logged no tracebacks", "Traceback" not in log)
    check("hub logged the poison drop", "dropping message id=" in log)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
