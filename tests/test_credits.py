"""Credits: the hub scheduling the ivrit.ai app's file transcriptions - lanes, the
shared RunPod pool (with an overflow edge drawing on it too), per-owner caps, the
long-file share, heartbeats, lapses that reattach, cancel, byok pools, the weekly
quota with its refunds, and the sweeper. Against a throwaway Postgres; see
tests/README.md."""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import support  # noqa: E402

SP = support.workdir()
TOKEN = "app-server-token"
QUEUE_TOKEN = "queue-token"
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


HUB = None


def http(method, path, body=None, token=TOKEN, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json"} if data is not None else {}
    if token:
        h["Authorization"] = "Bearer " + token
    r = urllib.request.Request(HUB + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw


def register(job_id, owner, duration, **extra):
    return http("POST", "/credits/jobs", {"job_id": job_id, "owner": owner, "duration": duration, **extra})


def wait(n=10, secs=0, byok=False):
    st, body = http("POST", "/credits/wait", {"max": n, "wait": secs, "byok": byok})
    assert st == 200, (st, body)
    return {g["job_id"]: g for g in body["grants"]}


def wa_body(mid):
    value = {"messaging_product": "whatsapp",
             "metadata": {"display_phone_number": "15550000000", "phone_number_id": "PNID"},
             "contacts": [{"profile": {"name": "T"}, "wa_id": "972500000001"}],
             "messages": [{"from": "972500000001", "id": mid, "timestamp": str(int(time.time())),
                           "type": "audio", "audio": {"id": "media-" + mid, "mime_type": "audio/ogg"}}]}
    return json.dumps({"object": "whatsapp_business_account",
                       "entry": [{"id": "WABA", "changes": [{"field": "messages", "value": value}]}]})


def main():
    global HUB
    pg_port, hub_port = free_port(), free_port()
    HUB = f"http://127.0.0.1:{hub_port}"
    subprocess.run(["docker", "rm", "-f", "eliezer-credits-pg"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "eliezer-credits-pg", "-e", "POSTGRES_PASSWORD=pw",
                    "-p", f"127.0.0.1:{pg_port}:5432", "postgres:16-alpine"], check=True, capture_output=True)
    for _ in range(60):
        if subprocess.run(["docker", "exec", "eliezer-credits-pg", "pg_isready", "-U", "postgres"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(0.5)
    time.sleep(1)
    dburl = f"postgresql://postgres:pw@127.0.0.1:{pg_port}/postgres"
    env = dict(os.environ, DATABASE_URL=dburl, QUEUE_TOKEN=QUEUE_TOKEN, APP_SERVER_TOKEN=TOKEN,
               CREDIT_POOLS="runpod=3", FILE_LONG_SHARE="0.5", CREDIT_LEASE_SECONDS="4",
               QUEUE_SWEEP_INTERVAL_SECONDS="1", FILE_QUOTA_MINUTES_PER_WEEK="60",
               PORT=str(hub_port), WHATSAPP_GRAPH_URL="http://127.0.0.1:9", TELEGRAM_API_URL="http://127.0.0.1:9")
    hubdir = os.path.join(REPO, "status_site")
    subprocess.run([sys.executable, "-c", "import app; app.init_db(); app.init_queue_db()"],
                   cwd=hubdir, env=env, check=True)

    def start_hub():
        return subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                                 "--port", str(hub_port), "--workers", "1", "--no-access-log"],
                                cwd=hubdir, env=env, stdout=open(os.path.join(SP, "hub.log"), "a"),
                                stderr=subprocess.STDOUT)

    hub = start_hub()
    conn = psycopg.connect(dburl, autocommit=True)
    q = lambda sql, *a: conn.execute(sql, a).fetchall()
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(HUB + "/favicon.ico", timeout=2)
                break
            except OSError:
                time.sleep(0.2)

        # --- auth and registration
        st, _ = http("POST", "/credits/jobs", {"job_id": "x" * 10}, token=None)
        check("no token -> 401", st == 401, st)
        st, _ = http("POST", "/credits/jobs", {"job_id": "x" * 10}, token=QUEUE_TOKEN)
        check("the edges' token is not the app server's -> 401", st == 401, st)
        st, _ = register("bad id!", "g:a", 60)
        check("bad job id -> 400", st == 400, st)

        # --- the weekly quota (60 minutes here): charged at registration, refunded once
        st, body = register("quota-too-big", "g:q", 3700)
        check("longer than the whole weekly quota -> 402, never fits",
              st == 402 and body["detail"]["wait_seconds"] is None, (st, body))
        st, body = register("quota-1", "g:q", 3000)
        check("within quota -> 201", st == 201 and body["lane"] == "file_long", (st, body))
        st, body = http("GET", "/credits/quota?owner=g:q")
        check("quota shows what is left", abs(body["remaining_seconds"] - 600) < 5, body)
        st, body = register("quota-2", "g:q", 1000)
        check("over what is left -> 402 with a wait",
              st == 402 and body["detail"]["wait_seconds"] > 0 and body["detail"]["remaining_seconds"] < 700,
              (st, body))
        st, body = register("quota-1", "g:q", 3000)
        check("re-registering is the same job (200), not a second charge", st == 200, (st, body))
        st, body = http("GET", "/credits/quota?owner=g:q")
        check("...and charged nothing more", abs(body["remaining_seconds"] - 600) < 5, body)
        st, body = http("POST", "/credits/cancel", {"job_id": "quota-1", "owner": "g:q"})
        check("cancelling a waiting job -> cancelled", body == {"state": "cancelled"}, body)
        st, body = http("GET", "/credits/quota?owner=g:q")
        check("...and refunds it", body["remaining_seconds"] > 3590, body)
        st, body = http("POST", "/credits/cancel", {"job_id": "quota-1", "owner": "g:q"})
        check("cancelling again is harmless", body == {"state": "gone"}, body)
        st, body = http("GET", "/credits/quota?owner=g:q")
        check("...and refunds nothing more", body["remaining_seconds"] <= 3600.5, body)
        st, body = register("byok-free", "g:q", 5000, byok=True)
        check("a job on the user's own endpoint is not charged", st == 201 and body["lane"] == "byok", (st, body))
        http("POST", "/credits/cancel", {"job_id": "byok-free", "owner": "g:q"})

        # --- lanes, pool capacity (3), per-owner caps, the long share (1 of 3)
        for job_id, owner, secs in [("a-1", "g:a", 60), ("a-2", "g:a", 60), ("b-1", "g:b", 3000),
                                    ("b-2", "g:b", 3000), ("c-1", "g:c", 3000), ("d-1", "g:d", 60)]:
            # The long ones are not charged: they would not fit the hour of quota.
            st, body = register(job_id, owner, secs, charge=secs < 1000)
            assert st == 201, (job_id, st, body)
        st, body = http("GET", "/credits/jobs?owner=g:d")
        d1 = body["jobs"][0]
        check("a waiting job has a queue position and an estimate",
              d1["status"] == "queued" and d1["position"] >= 1 and d1["eta_seconds"] > 0, d1)
        g = wait()
        check("first grants: both owners' first short files, and one long file",
              set(g) == {"a-1", "d-1", "b-1"}, sorted(g))
        check("a grant says what to do", g["a-1"]["spec"]["max_running"] == 1 and g["a-1"]["attempt"] == 1, g["a-1"])
        check("pool full: nothing more", wait() == {})
        st, body = http("GET", "/credits/jobs?ids=a-1,a-2")
        states = {j["job_id"]: j["status"] for j in body["jobs"]}
        check("granted jobs show as running", states == {"a-1": "running", "a-2": "queued"}, states)

        st, body = http("POST", "/credits/progress", {"handle": g["a-1"]["handle"], "stage": "transcribing",
                                                      "percent": 40, "eta_s": 30})
        check("heartbeat -> no cancel", st == 200 and body == {"cancel": False}, (st, body))
        st, body = http("GET", "/credits/jobs?ids=a-1")
        check("progress is visible to the app", body["jobs"][0]["progress"]["percent"] == 40
              and body["jobs"][0]["eta_seconds"] == 30, body)
        st, _ = http("POST", "/credits/done", {"handle": g["a-1"]["handle"], "status": "done", "duration": 61})
        check("done -> 200", st == 200)
        st, _ = http("POST", "/credits/done", {"handle": g["a-1"]["handle"], "status": "done"})
        check("done again (a retry) -> still 200", st == 200)
        g2 = wait()
        check("the owner's next file gets the freed slot", set(g2) == {"a-2"}, sorted(g2))

        # --- an overflow edge leasing voice messages for RunPod shares the pool
        for mid in ("wa-1", "wa-2"):
            conn.execute("INSERT INTO queue_messages (source, body, sent_at, expires_at) "
                         "VALUES ('whatsapp', %s, now(), now() + interval '3 hours');", (wa_body(mid),))
        st, body = http("POST", "/queue/lease", {"max": 5, "wait": 0, "pool": "runpod"}, token=QUEUE_TOKEN)
        check("the edge gets nothing while the pool is full", body["jobs"] == [], body)
        http("POST", "/credits/done", {"handle": g["d-1"]["handle"], "status": "done"})
        st, body = http("POST", "/queue/lease", {"max": 5, "wait": 0, "pool": "runpod"}, token=QUEUE_TOKEN)
        edge_jobs = body["jobs"]
        check("...and exactly the one freed slot after", len(edge_jobs) == 1, body)
        st, body = http("POST", "/queue/lease", {"max": 5, "wait": 0}, token=QUEUE_TOKEN)
        check("an edge with its own GPUs (no pool) is not limited by it", len(body["jobs"]) == 1, body)
        http("POST", "/queue/release", {"handle": body["jobs"][0]["handle"]}, token=QUEUE_TOKEN)
        check("credits: none while the edge holds the last slot", wait() == {})

        # admit is charged once, however often the edge retries it
        h = edge_jobs[0]["handle"]
        for _ in range(3):
            st, body = http("POST", "/queue/admit", {"handle": h, "duration": 30}, token=QUEUE_TOKEN)
        check("a retried admit is still ok", st == 200 and body == {"ok": True}, (st, body))
        level = q("SELECT level FROM quota_buckets WHERE user_key = '972500000001' AND kind = 'msgs_hour'")
        check("...and charged the hourly bucket once", level and 8.9 < level[0][0] < 9.1, level)
        http("POST", "/queue/release", {"handle": h}, token=QUEUE_TOKEN)

        # Released: the slot is free again - for a long file? Not while one long runs.
        g3 = wait()
        check("long files never take more than their share", g3 == {}, sorted(g3))
        http("POST", "/credits/done", {"handle": g["b-1"]["handle"], "status": "done"})
        g4 = wait()
        check("the long slot goes round to the other owner", set(g4) == {"b-2"} or set(g4) == {"c-1"}, sorted(g4))
        long_job = next(iter(g4))

        # --- a holder that dies: the credit lapses, and is granted again with where it ran
        st, _ = http("POST", "/credits/progress", {"handle": g4[long_job]["handle"],
                                                   "backend_ref": {"runpod_job": "rp-123"}})
        time.sleep(5)
        st, _ = http("POST", "/credits/progress", {"handle": g4[long_job]["handle"]})
        check("a lapsed credit's heartbeat -> 409", st == 409, st)
        g5 = wait()
        check("the lapsed job is granted again, with its backend ref and attempt 2",
              long_job in g5 and g5[long_job]["backend_ref"] == {"runpod_job": "rp-123"}
              and g5[long_job]["attempt"] == 2, g5)
        st, _ = http("POST", "/credits/done", {"handle": g4[long_job]["handle"], "status": "done"})
        check("the old holder's done -> 409", st == 409, st)
        for job_id, grant in g5.items():
            if job_id != long_job:
                http("POST", "/credits/release", {"handle": grant["handle"]})
        handle = g5[long_job]["handle"]

        # --- cancel while running reaches the holder through its heartbeat
        owner = "g:b" if long_job == "b-2" else "g:c"
        st, body = http("POST", "/credits/cancel", {"job_id": long_job, "owner": owner})
        check("cancel a running job -> stopping", body == {"state": "stopping"}, body)
        st, body = http("POST", "/credits/progress", {"handle": handle})
        check("...its next heartbeat says cancel", body == {"cancel": True}, body)
        http("POST", "/credits/done", {"handle": handle, "status": "failed", "error": "cancelled"})
        st, body = http("GET", f"/credits/jobs?ids={long_job}")
        check("...and it ends cancelled", body["jobs"][0]["status"] == "failed"
              and body["jobs"][0]["error"] == "cancelled", body)

        # --- a user's own endpoint: its own pool, sized by the job, not the shared one
        for i in range(3):
            register(f"e-{i}", "g:e", 4000, byok=True, max_running=2)
        g6 = wait(byok=True)
        mine = {k for k in g6 if k.startswith("e-")}
        check("byok: as many at once as the user's endpoint takes", mine == {"e-0", "e-1"}, sorted(g6))
        check("byok credits draw on the owner's pool",
              q("SELECT DISTINCT pool FROM queue_messages WHERE owner = 'g:e' AND receipt_handle IS NOT NULL")
              == [("byok:g:e",)])
        check("byok is not granted to a holder that does not ask for it", "e-2" not in wait(byok=False))

        # --- the sweeper: a waiting job past its age fails and is refunded; a held one is spared
        st, _ = register("old-1", "g:f", 600)
        conn.execute("UPDATE queue_messages SET expires_at = now() - interval '1 second' "
                     "WHERE job_id IN ('old-1', 'e-0');")
        before = http("GET", "/credits/quota?owner=g:f")[1]["remaining_seconds"]
        time.sleep(2.5)
        st, body = http("GET", "/credits/jobs?ids=old-1")
        check("a job that waited too long fails as expired", body["jobs"][0]["status"] == "failed"
              and body["jobs"][0]["error"] == "expired", body)
        after = http("GET", "/credits/quota?owner=g:f")[1]["remaining_seconds"]
        check("...and its quota comes back", after > before + 500, (before, after))
        st, _ = http("POST", "/credits/progress", {"handle": g6["e-0"]["handle"]})
        check("a job past its age that is still held is not swept", st == 200, st)
        st, _ = http("POST", "/credits/done", {"handle": g6["e-0"]["handle"], "status": "done"})
        check("...and its result is accepted", st == 200, st)

        # --- buckets live in Postgres, so a restart forgets nothing
        hub.terminate()
        hub.wait(timeout=20)
        hub = start_hub()
        for _ in range(50):
            try:
                urllib.request.urlopen(HUB + "/favicon.ico", timeout=2)
                break
            except OSError:
                time.sleep(0.2)
        st, body = http("GET", "/credits/quota?owner=g:a")
        check("after a restart the weekly quota still shows what was used",
              body["remaining_seconds"] < 3600 - 100, body)
        st, body = http("GET", "/credits/status")
        check("status shows pools and lanes", body["pools"]["runpod"]["capacity"] == 3 and "byok" in body["lanes"], body)

        # --- long-poll wakes when a credit frees, not on a timer
        held = wait(byok=True)
        for grant in held.values():
            http("POST", "/credits/release", {"handle": grant["handle"]})
        st, body = http("GET", "/credits/status")
        started = time.monotonic()
        import threading
        got = {}

        def waiter():
            got.update(wait(secs=10, byok=True))
        t = threading.Thread(target=waiter)
        # Fill everything first, then wait, then free one.
        everything = wait(byok=True)
        t.start()
        time.sleep(0.5)
        register("late-1", "g:z", 60)
        t.join(timeout=12)
        check("a waiting holder is woken by new work", "late-1" in got and time.monotonic() - started < 6,
              (sorted(got), time.monotonic() - started))
        for grant in list(everything.values()) + list(got.values()):
            http("POST", "/credits/release", {"handle": grant["handle"]})
    finally:
        hub.terminate()
        conn.close()
        subprocess.run(["docker", "rm", "-f", "eliezer-credits-pg"], capture_output=True)

    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    if failed:
        print("hub log:", os.path.join(SP, "hub.log"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
