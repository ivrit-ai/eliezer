"""Credits: the hub schedules the ivrit.ai app's file transcriptions, and the app's
server sends each file when the hub grants it a slot.

The files stay with the app's server (the hub stores no media). It registers a job -
owner, length, whether it runs on the shared RunPod endpoint or on the user's own - and
long-polls /credits/wait. The hub grants credits in a fair order: lane first (short
files before long ones, and long ones only up to a share of the pool, so they never
take every worker), then round-robin between owners, then oldest first, never more at
once than a pool has capacity or an owner may run. Holding a credit, the server submits
the file to RunPod, reports progress (/credits/progress, which keeps the credit alive
and is how a cancel reaches it), and returns the credit with the outcome
(/credits/done). If the server dies holding one, the credit lapses and is granted again
with the backend's job id, so the job is picked up where it runs instead of restarted.

The "runpod" pool is shared with the edges: an overflow edge leasing voice messages
for RunPod names it too (queue_api._try_lease), so both never overfill the endpoint.
A user's own endpoint is a pool of its own, "byok:<owner>", sized by the job.

Quota: a job charged to the shared endpoint is charged at registration, against the
owner's weekly bucket (limits.py), and refunded once if it ends with no transcript.
"""

import hmac
import json
import logging
import math
import os
import re
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

import db
import limits
import queue_api

log = logging.getLogger(__name__)

# The app's server - the only holder of credits - authenticates with this.
APP_SERVER_TOKEN = os.environ.get("APP_SERVER_TOKEN", "")

SHORT, LONG, BYOK = "file_short", "file_long", "byok"
LANES = (SHORT, LONG, BYOK)
RUNPOD = "runpod"

# Files up to this long go in the short lane (transcribe.ivrit.ai's threshold).
SHORT_SECONDS = float(os.environ.get("FILE_SHORT_SECONDS", "1200"))
# The share of the RunPod pool long files may hold at once (at least one slot).
LONG_SHARE = float(os.environ.get("FILE_LONG_SHARE", "0.7"))
# How long a file may wait for a slot before it is given up on.
FILE_MAX_AGE = int(os.environ.get("FILE_MAX_AGE_SECONDS", str(24 * 3600)))
# A credit lapses this long after its last heartbeat.
CREDIT_LEASE = int(os.environ.get("CREDIT_LEASE_SECONDS", "300"))
# A job running longer than this is told to stop (RunPod's own limit is lower).
MAX_RUN = int(os.environ.get("FILE_MAX_RUN_SECONDS", str(8 * 3600)))
# Jobs one owner may have waiting; more is a script, not a person.
MAX_QUEUED = int(os.environ.get("FILE_MAX_QUEUED_PER_OWNER", "100"))
# Seconds of audio per second of work, until there are finished jobs to measure.
DEFAULT_SPEED = float(os.environ.get("FILE_SPEED", "15"))
# The RunPod pool's capacity when CREDIT_POOLS does not set it: what
# transcribe.ivrit.ai ran at once.
DEFAULT_RUNPOD_CAPACITY = 2
# Finished jobs are kept this long, for the app to read and for measuring speed.
KEEP_SECONDS = 7 * 24 * 3600

JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
OWNER = re.compile(r"^[A-Za-z0-9_:.@+-]{1,200}$")


def init_db(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS credit_pools (
          name     TEXT PRIMARY KEY,
          capacity INT NOT NULL
        );
        """
    )
    # CREDIT_POOLS="runpod=8": capacities set by configuration, applied on every boot.
    for item in filter(None, (p.strip() for p in os.environ.get("CREDIT_POOLS", "").split(","))):
        name, _, capacity = item.partition("=")
        cur.execute(
            "INSERT INTO credit_pools (name, capacity) VALUES (%s, %s) "
            "ON CONFLICT (name) DO UPDATE SET capacity = EXCLUDED.capacity;",
            (name.strip(), int(capacity)),
        )
    # Every file job, from registration until a week after it ends: what the app's
    # server reads back, and what measures how fast files go through.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS credit_jobs (
          job_id      TEXT PRIMARY KEY,
          owner       TEXT NOT NULL,
          lane        TEXT NOT NULL,
          status      TEXT NOT NULL DEFAULT 'queued',
          error       TEXT,
          duration    DOUBLE PRECISION NOT NULL,
          charged     BOOLEAN NOT NULL DEFAULT false,
          progress    JSONB,
          handle      TEXT,
          created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
          granted_at  TIMESTAMPTZ,
          finished_at TIMESTAMPTZ
        );
        """
    )
    # What the app's server said about the job (file name, language...), handed back
    # with its grants and its job list, so a restarted server knows its jobs.
    cur.execute("ALTER TABLE credit_jobs ADD COLUMN IF NOT EXISTS spec JSONB;")
    cur.execute("CREATE INDEX IF NOT EXISTS credit_jobs_owner_idx ON credit_jobs (owner, created_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS credit_jobs_handle_idx ON credit_jobs (handle) WHERE handle IS NOT NULL;")


# --- the sweeper's side (queue_api.sweep, inside its transaction)

def dropped(cur, rows):
    """File jobs the sweeper dropped: waited too long, or failed every time it was granted."""
    for r in rows:
        _finish(cur, r["job_id"], "failed", "expired" if r["expired"] else "failed_repeatedly")


def sweep(cur):
    # Cancelled while held, and the holder never came back to stop it.
    cur.execute(
        f"DELETE FROM queue_messages WHERE cancel_requested AND lane = ANY(%s) AND NOT ({queue_api.LIVE}) "
        f"RETURNING job_id;",
        (list(LANES),),
    )
    for r in cur.fetchall():
        _finish(cur, r["job_id"], "failed", "cancelled")
    cur.execute(
        "DELETE FROM credit_jobs WHERE finished_at < now() - make_interval(secs => %s::double precision);",
        (KEEP_SECONDS,),
    )


def _finish(cur, job_id, status, error, duration=None, handle=None):
    cur.execute(
        "UPDATE credit_jobs SET status = %s, error = %s, duration = coalesce(%s, duration), "
        "handle = coalesce(%s, handle), finished_at = now() "
        "WHERE job_id = %s AND finished_at IS NULL RETURNING charged;",
        (status, error, duration, handle, job_id),
    )
    row = cur.fetchone()
    # Nothing came of it: give the time back.
    if row and row["charged"] and status != "done":
        limits.refund_file(cur, job_id)


# --- granting

def _capacity(cur):
    capacity = queue_api.pool_capacity(cur, RUNPOD)
    return DEFAULT_RUNPOD_CAPACITY if capacity is None else capacity


def _grant(cur, lanes, pool, n, holder):
    """Grant up to n credits for jobs in these lanes, from this pool (or each owner's
    own pool, for byok). The caller holds the pool's lock."""
    params = queue_api._params(lanes=list(lanes))
    cur.execute(
        f"""
        WITH live AS (
          SELECT owner, count(*) AS n FROM queue_messages
          WHERE lane = ANY(%(lanes)s) AND {queue_api.LIVE} GROUP BY owner
        ),
        ranked AS (
          SELECT q.id, q.lane, q.owner,
                 coalesce(l.n, 0) + row_number() OVER (PARTITION BY q.owner ORDER BY q.id) AS slot,
                 coalesce((q.spec->>'max_running')::int, 1) AS cap
          FROM queue_messages q LEFT JOIN live l ON l.owner = q.owner
          WHERE {queue_api.LEASABLE} AND q.lane = ANY(%(lanes)s) AND NOT q.cancel_requested
        )
        SELECT id, lane, owner FROM ranked WHERE slot <= cap
        ORDER BY array_position(%(lanes)s, lane), slot, id
        LIMIT 500;
        """,
        params,
    )
    candidates = cur.fetchall()
    if not candidates:
        return []
    chosen = []
    if pool:
        capacity = _capacity(cur)
        free = capacity - queue_api.pool_in_use(cur, pool)
        cur.execute(f"SELECT count(*) AS n FROM queue_messages WHERE pool = %s AND lane = %s AND {queue_api.LIVE};",
                    (pool, LONG))
        long_free = max(1, math.floor(capacity * LONG_SHARE)) - cur.fetchone()["n"]
        for c in candidates:
            if len(chosen) >= min(n, free):
                break
            if c["lane"] == LONG:
                if long_free <= 0:
                    continue
                long_free -= 1
            chosen.append((c["id"], pool))
    else:
        chosen = [(c["id"], f"byok:{c['owner']}") for c in candidates[:n]]
    if not chosen:
        return []
    granted = []
    for job_row_id, job_pool in chosen:
        cur.execute(
            f"""
            UPDATE queue_messages SET
              visible_at     = now() + make_interval(secs => %(lease)s::double precision),
              receive_count  = receive_count + 1,
              receipt_handle = gen_random_uuid()::text,
              leased_by      = %(holder)s,
              pool           = %(pool)s,
              granted_at     = coalesce(granted_at, now())
            WHERE id = %(id)s AND {queue_api.LEASABLE}
            RETURNING receipt_handle AS handle, job_id, owner, lane, spec, backend_ref, receive_count;
            """,
            queue_api._params(lease=CREDIT_LEASE, holder=holder, pool=job_pool, id=job_row_id),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE credit_jobs SET status = 'running', granted_at = coalesce(granted_at, now()) "
                "WHERE job_id = %s;",
                (row["job_id"],),
            )
            granted.append(row)
    return granted


def _try_wait(n, byok, holder):
    with db.pool().connection() as conn, conn.cursor() as cur:
        granted = []
        queue_api.lock_pool(cur, RUNPOD)
        granted += _grant(cur, (SHORT, LONG), RUNPOD, n, holder)
        if byok and len(granted) < n:
            queue_api.lock_pool(cur, BYOK)
            granted += _grant(cur, (BYOK,), None, n - len(granted), holder)
        conn.commit()
    return [
        {
            "handle": g["handle"],
            "job_id": g["job_id"],
            "owner": g["owner"],
            "lane": g["lane"],
            "spec": g["spec"] or {},
            "backend_ref": g["backend_ref"],
            "attempt": g["receive_count"],
        }
        for g in granted
    ]


# --- the app server's API

def require_holder(request: Request):
    header = request.headers.get("Authorization", "")
    token = header[len("Bearer "):] if header.startswith("Bearer ") else ""
    if not APP_SERVER_TOKEN or not hmac.compare_digest(token.encode(), APP_SERVER_TOKEN.encode()):
        raise HTTPException(status_code=401, detail="unauthorized")
    return request.headers.get("X-Instance-Id") or "app"


credits_api = APIRouter(prefix="/credits")

VIEW = ("job_id, owner, lane, status, error, duration, progress, spec, "
        "extract(epoch FROM created_at)::float8 AS created_at, "
        "extract(epoch FROM granted_at)::float8 AS granted_at, "
        "extract(epoch FROM finished_at)::float8 AS finished_at")


def _speed(cur):
    """Seconds of audio per second of work, over the last day's finished files."""
    cur.execute(
        "SELECT sum(duration) AS audio, sum(extract(epoch FROM finished_at - granted_at)) AS work "
        "FROM credit_jobs WHERE status = 'done' AND lane <> 'byok' "
        "AND finished_at > now() - interval '1 day' AND granted_at IS NOT NULL;"
    )
    row = cur.fetchone()
    if row["audio"] and row["work"] and row["work"] > 0:
        return max(1.0, float(row["audio"]) / float(row["work"]))
    return DEFAULT_SPEED


def _with_eta(cur, jobs):
    """Add queue position and an estimate of seconds to finish, for jobs still waiting
    or running on the shared endpoint."""
    waiting = [j for j in jobs if j["status"] == "queued" and j["lane"] != BYOK]
    running = [j for j in jobs if j["status"] == "running"]
    if not (waiting or running):
        return jobs
    speed = _speed(cur)
    for j in running:
        p = j.get("progress") or {}
        if p.get("eta_s") is not None:
            j["eta_seconds"] = p["eta_s"]
        elif p.get("percent"):
            elapsed = time.time() - j.get("granted_at", time.time())
            j["eta_seconds"] = elapsed * (100 - p["percent"]) / max(p["percent"], 1)
        else:
            j["eta_seconds"] = j["duration"] / speed
    if not waiting:
        return jobs
    capacity = max(1, _capacity(cur))
    # What runs now, and how much of it is left.
    cur.execute(
        f"SELECT q.progress, c.duration, extract(epoch FROM now() - q.granted_at)::float8 AS elapsed "
        f"FROM queue_messages q JOIN credit_jobs c USING (job_id) "
        f"WHERE q.pool = %s AND {queue_api.LIVE};",
        (RUNPOD,),
    )
    busy = 0.0
    for r in cur.fetchall():
        p = r["progress"] or {}
        busy += p["eta_s"] if p.get("eta_s") is not None else max(0.0, r["duration"] / speed - (r["elapsed"] or 0))
    # What waits, in the order it would be granted (approximately: lane, then age).
    cur.execute(
        f"SELECT q.job_id, q.lane, c.duration FROM queue_messages q JOIN credit_jobs c USING (job_id) "
        f"WHERE {queue_api.LEASABLE} AND q.lane IN (%(short)s, %(long)s) "
        f"ORDER BY array_position(ARRAY[%(short)s, %(long)s], q.lane), q.id;",
        queue_api._params(short=SHORT, long=LONG),
    )
    ahead, position = busy, {}
    for i, r in enumerate(cur.fetchall()):
        position[r["job_id"]] = (i + 1, ahead / capacity + r["duration"] / speed)
        ahead += r["duration"] / speed
    for j in waiting:
        if j["job_id"] in position:
            j["position"], j["eta_seconds"] = position[j["job_id"]]
    return jobs


def _view(row):
    out = {k: row[k] for k in ("job_id", "owner", "lane", "status", "duration", "created_at")}
    for k in ("error", "progress", "granted_at", "finished_at", "spec"):
        if row[k] is not None:
            out[k] = row[k]
    return out


def _register(payload):
    job_id, owner = payload.get("job_id"), payload.get("owner")
    if not (isinstance(job_id, str) and JOB_ID.match(job_id)):
        raise HTTPException(status_code=400, detail="bad job_id")
    if not (isinstance(owner, str) and OWNER.match(owner)):
        raise HTTPException(status_code=400, detail="bad owner")
    try:
        duration = float(payload.get("duration"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="duration required")
    if not (0 < duration < 100 * 3600):
        raise HTTPException(status_code=400, detail="bad duration")
    byok = bool(payload.get("byok"))
    lane = BYOK if byok else (SHORT if duration <= SHORT_SECONDS else LONG)
    max_running = max(1, min(int(payload.get("max_running") or 1), 50))
    charge = bool(payload.get("charge", True)) and not byok
    spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}
    spec = {**spec, "max_running": max_running}

    with db.pool().connection() as conn, conn.cursor() as cur:
        # One owner at a time, so two registrations cannot both pass the counts.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (f"credits:{owner}",))
        cur.execute(f"SELECT {VIEW} FROM credit_jobs WHERE job_id = %s;", (job_id,))
        existing = cur.fetchone()
        if existing:
            if existing["owner"] != owner:
                raise HTTPException(status_code=409, detail="job_id taken")
            return _with_eta(cur, [_view(existing)])[0], False
        cur.execute(
            "SELECT count(*) AS n FROM credit_jobs WHERE owner = %s AND status = 'queued';", (owner,)
        )
        if cur.fetchone()["n"] >= MAX_QUEUED:
            raise HTTPException(status_code=429, detail="too many queued")
        if charge:
            ok, bucket = limits.charge_file(cur, job_id, owner, duration)
            if not ok:
                wait = bucket.wait_seconds(duration)
                raise HTTPException(status_code=402, detail={
                    "error": "quota",
                    "remaining_seconds": bucket.level,
                    "wait_seconds": None if math.isinf(wait) else wait,
                })
        cur.execute(
            "INSERT INTO credit_jobs (job_id, owner, lane, duration, charged, spec) VALUES (%s, %s, %s, %s, %s, %s);",
            (job_id, owner, lane, duration, charge, json.dumps(spec)),
        )
        queue_api.enqueue(cur, "files", json.dumps({"job_id": job_id}), time.time(), job_id,
                          lane=lane, owner=owner, spec=spec, max_age=FILE_MAX_AGE)
        conn.commit()
        cur.execute(f"SELECT {VIEW} FROM credit_jobs WHERE job_id = %s;", (job_id,))
        return _with_eta(cur, [_view(cur.fetchone())])[0], True


@credits_api.post("/jobs")
async def register(request: Request, holder: str = Depends(require_holder)):
    """A file ready to transcribe: {job_id, owner, duration, byok?, max_running?,
    charge?, spec?}. Answers 201 with the job (position, eta_seconds); 200 if this
    job_id was already registered; 402 {error: quota, remaining_seconds,
    wait_seconds} when the owner's weekly quota lacks the time; 429 when too many wait."""
    payload = await queue_api._json_body(request)
    view, created = await run_in_threadpool(_register, payload)
    return JSONResponse(view, status_code=201 if created else 200)


@credits_api.post("/wait")
async def wait(request: Request, holder: str = Depends(require_holder)):
    """Long-poll for credits: {max, wait, byok}. Each grant is a job the caller may
    start now, with its handle, spec, and backend_ref if a previous holder had
    submitted it already."""
    payload = await queue_api._json_body(request)
    n = max(1, min(int(payload.get("max", 1)), queue_api.MAX_BATCH))
    secs = max(0, min(int(payload.get("wait", 0)), queue_api.MAX_WAIT_SECONDS))
    byok = bool(payload.get("byok", True))
    grants = await queue_api.long_poll(lambda: _try_wait(n, byok, holder), secs)
    return {"grants": grants or []}


def _progress(handle, progress, backend_ref):
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE queue_messages SET
              visible_at  = now() + make_interval(secs => %s::double precision),
              progress    = coalesce(%s, progress),
              backend_ref = coalesce(%s, backend_ref)
            WHERE receipt_handle = %s AND {queue_api.LIVE} AND lane = ANY(%s)
            RETURNING job_id, cancel_requested,
                      granted_at < now() - make_interval(secs => %s::double precision) AS overrun;
            """,
            (CREDIT_LEASE, json.dumps(progress) if progress else None,
             json.dumps(backend_ref) if backend_ref else None, handle, list(LANES), MAX_RUN),
        )
        row = cur.fetchone()
        if row and progress:
            cur.execute("UPDATE credit_jobs SET progress = %s WHERE job_id = %s;",
                        (json.dumps(progress), row["job_id"]))
        conn.commit()
    return row


@credits_api.post("/progress")
async def progress(request: Request, holder: str = Depends(require_holder)):
    """Heartbeat for a held credit: {handle, stage?, percent?, eta_s?, backend_ref?}.
    Keeps the credit for another CREDIT_LEASE seconds. Answers {cancel}: true when the
    owner cancelled, or the job has run too long. 409 if the credit was lost."""
    payload = await queue_api._json_body(request)
    handle = payload.get("handle")
    if not handle:
        raise HTTPException(status_code=400, detail="handle required")
    report = {k: payload[k] for k in ("stage", "percent", "eta_s") if payload.get(k) is not None}
    backend_ref = payload.get("backend_ref") if isinstance(payload.get("backend_ref"), dict) else None
    row = await run_in_threadpool(_progress, handle, report or None, backend_ref)
    if row is None:
        raise HTTPException(status_code=409, detail="credit no longer held")
    return {"cancel": bool(row["cancel_requested"] or row["overrun"])}


def _done(handle, status, error, duration):
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, job_id FROM queue_messages WHERE receipt_handle = %s AND lane = ANY(%s) FOR UPDATE;",
            (handle, list(LANES)),
        )
        row = cur.fetchone()
        if row is None:
            # Already returned (a retry), or lost to another grant.
            cur.execute("SELECT 1 FROM credit_jobs WHERE handle = %s;", (handle,))
            return cur.fetchone() is not None
        _finish(cur, row["job_id"], status, error, duration, handle)
        cur.execute("DELETE FROM queue_messages WHERE id = %s;", (row["id"],))
        queue_api.notify(cur)
        conn.commit()
    return True


@credits_api.post("/done")
async def done(request: Request, holder: str = Depends(require_holder)):
    """Return a credit with the job's outcome: {handle, status: done|failed, error?,
    duration?}. A failed job's quota is refunded. Safe to retry."""
    payload = await queue_api._json_body(request)
    handle, status = payload.get("handle"), payload.get("status")
    if not handle or status not in ("done", "failed"):
        raise HTTPException(status_code=400, detail="handle and status required")
    duration = payload.get("duration")
    ok = await run_in_threadpool(
        _done, handle, status, payload.get("error") if status == "failed" else None,
        float(duration) if duration is not None else None)
    if not ok:
        raise HTTPException(status_code=409, detail="credit no longer held")
    return {"ok": True}


def _release(handle):
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE queue_messages SET visible_at = now(), pool = NULL, "
            "receive_count = greatest(receive_count - 1, 0) "
            "WHERE receipt_handle = %s AND lane = ANY(%s);",
            (handle, list(LANES)),
        )
        queue_api.notify(cur)
        conn.commit()


@credits_api.post("/release")
async def release(request: Request, holder: str = Depends(require_holder)):
    """Give a credit back unused (the server is shutting down before it started the
    job): the job waits again at once, and the grant does not count as an attempt."""
    payload = await queue_api._json_body(request)
    if not payload.get("handle"):
        raise HTTPException(status_code=400, detail="handle required")
    await run_in_threadpool(_release, payload["handle"])
    return {"ok": True}


def _cancel(job_id, owner):
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT id, {queue_api.LIVE} AS held FROM queue_messages "
            f"WHERE job_id = %s AND owner = %s AND lane = ANY(%s) FOR UPDATE;",
            (job_id, owner, list(LANES)),
        )
        row = cur.fetchone()
        if row is None:
            return "gone"
        if row["held"]:
            # Its holder stops it and returns the credit on its next heartbeat.
            cur.execute("UPDATE queue_messages SET cancel_requested = true WHERE id = %s;", (row["id"],))
            state = "stopping"
        else:
            _finish(cur, job_id, "failed", "cancelled")
            cur.execute("DELETE FROM queue_messages WHERE id = %s;", (row["id"],))
            state = "cancelled"
        conn.commit()
    return state


@credits_api.post("/cancel")
async def cancel(request: Request, holder: str = Depends(require_holder)):
    """{job_id, owner}: a waiting job is cancelled (and refunded) at once; a running one
    is told to stop through its holder's next heartbeat."""
    payload = await queue_api._json_body(request)
    if not payload.get("job_id") or not payload.get("owner"):
        raise HTTPException(status_code=400, detail="job_id and owner required")
    return {"state": await run_in_threadpool(_cancel, payload["job_id"], payload["owner"])}


def _jobs(owner, ids):
    with db.pool().connection() as conn, conn.cursor() as cur:
        if ids:
            cur.execute(f"SELECT {VIEW} FROM credit_jobs WHERE job_id = ANY(%s);", (ids,))
        else:
            cur.execute(f"SELECT {VIEW} FROM credit_jobs WHERE owner = %s ORDER BY created_at DESC LIMIT 200;",
                        (owner,))
        return _with_eta(cur, [_view(r) for r in cur.fetchall()])


@credits_api.get("/jobs")
async def jobs(owner: str = "", ids: str = "", holder: str = Depends(require_holder)):
    """An owner's jobs (?owner=), or specific ones (?ids=a,b), with queue position and
    eta_seconds for those waiting or running."""
    id_list = [i for i in ids.split(",") if i][:500]
    if not owner and not id_list:
        raise HTTPException(status_code=400, detail="owner or ids required")
    return {"jobs": await run_in_threadpool(_jobs, owner, id_list)}


def _quota(owner):
    with db.pool().connection() as conn, conn.cursor() as cur:
        bucket = limits.file_quota(cur, owner)
    return {"remaining_seconds": bucket.level, "cap_seconds": bucket.cap, "refill_per_second": bucket.rate}


@credits_api.get("/quota")
async def quota(owner: str, holder: str = Depends(require_holder)):
    return await run_in_threadpool(_quota, owner)


def _status():
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT name, capacity FROM credit_pools ORDER BY name;")
        pools = {r["name"]: {"capacity": r["capacity"], "in_use": queue_api.pool_in_use(cur, r["name"])}
                 for r in cur.fetchall()}
        cur.execute(
            f"SELECT lane, count(*) FILTER (WHERE {queue_api.LIVE}) AS running, "
            f"count(*) FILTER (WHERE {queue_api.LEASABLE}) AS waiting "
            f"FROM queue_messages GROUP BY lane;",
            queue_api._params(),
        )
        lanes = {r["lane"]: {"running": r["running"], "waiting": r["waiting"]} for r in cur.fetchall()}
        return {"pools": pools, "lanes": lanes, "speed": _speed(cur)}


@credits_api.get("/status")
async def status(holder: str = Depends(require_holder)):
    """Pools and lanes at a glance, for the app's Stats page."""
    return await run_in_threadpool(_status)
