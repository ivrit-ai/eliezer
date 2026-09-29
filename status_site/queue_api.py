"""Postgres-backed message queue with an HTTP API for the transcription edges.

Replaces the AWS SQS queue the edges used to poll. One table, one FOR UPDATE SKIP
LOCKED statement, real visibility-timeout semantics. Webhooks (ingest.py) feed it.

An edge leases a job, fetches its media through the hub (/queue/media), reports the
duration (/queue/admit), and hands back the transcript (/queue/complete). The hub does
all platform I/O, limits, replies and statistics; edges hold no platform credentials.

Messages are never kept: a message not handled within QUEUE_MAX_AGE_SECONDS of being
*sent* is dropped - at ingest if it already is that old, otherwise by the sweeper.
"""

import asyncio
import hmac
import json
import logging
import os
import signal
import threading
import time

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from psycopg.rows import dict_row
from starlette.background import BackgroundTask

import analytics
import db
import identity
import limits
import messages
import outbox
import stats
import telegram
import whatsapp

log = logging.getLogger(__name__)

# One token shared by every edge, so adding an edge needs no change here. Edges name
# themselves in X-Instance-Id; that is self-reported, and used only to attribute leases.
QUEUE_TOKEN = os.environ.get("QUEUE_TOKEN", "")
# Longer than a job can sit behind a busy edge's transcription semaphore, so a message
# being worked on is not handed to a second edge.
VISIBILITY_TIMEOUT = int(os.environ.get("QUEUE_VISIBILITY_TIMEOUT", "900"))
# A message is leased at most this many times; after the last lease lapses unacked, it is
# dropped as poison.
MAX_RECEIVES = int(os.environ.get("QUEUE_MAX_RECEIVES", "5"))
# Measured from the platform's send timestamp, not from enqueue, so a webhook the
# platform re-delivers after an outage still ages from when the user sent it.
MAX_AGE_SECONDS = int(os.environ.get("QUEUE_MAX_AGE_SECONDS", "10800"))
SWEEP_INTERVAL_SECONDS = int(os.environ.get("QUEUE_SWEEP_INTERVAL_SECONDS", "60"))

MAX_WAIT_SECONDS = 20
MAX_BATCH = 100

# Platform adapters by queue source: parse(body), target(parsed), open_media(media).
SOURCES = {"whatsapp": whatsapp, "telegram": telegram}

# A message an edge may take right now. Shared by lease, depth and the overflow
# threshold, so all three agree on what "waiting" means.
LEASABLE = """
    visible_at <= now()
    AND receive_count < %(max)s
    AND sent_at >= now() - make_interval(secs => %(age)s::double precision)
"""

queue_api = APIRouter()

# Set on SIGTERM/SIGINT. The server's graceful shutdown waits for open requests, and an
# idle long-poll can hold one for MAX_WAIT_SECONDS, so on a redeploy the platform would
# kill the process mid-request - possibly just after a lease committed, leaving that
# message invisible for the whole visibility timeout. Instead, waiting polls return
# empty at once and no new lease is taken.
_shutting_down = threading.Event()


def install_shutdown_hook():
    """Chain in front of the server's own signal handlers. Must run on the main thread
    after the server has installed them (i.e. from the lifespan startup)."""
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous = signal.getsignal(sig)

        def handler(signum, frame, previous=previous):
            _shutting_down.set()
            if callable(previous):
                previous(signum, frame)

        signal.signal(sig, handler)


def _params(**extra):
    return {"max": MAX_RECEIVES, "age": MAX_AGE_SECONDS, **extra}


def init_queue_db():
    # A plain connection, not the shared pool: this runs in launch.sh's one-shot process,
    # where a pool left open makes interpreter exit wait 5s on its worker threads.
    with psycopg.connect(db.DATABASE_URL, row_factory=dict_row) as conn, conn.cursor() as cur:
        # Leases mint receipt handles with gen_random_uuid() (built in from Postgres
        # 13). Fail the boot here, where the platform keeps the old container
        # running, rather than on the first lease.
        cur.execute("SELECT gen_random_uuid();")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_messages (
              id             BIGSERIAL PRIMARY KEY,
              source         TEXT NOT NULL,
              body           TEXT NOT NULL,
              sent_at        TIMESTAMPTZ NOT NULL,
              receipt_handle TEXT,
              leased_by      TEXT,
              visible_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
              receive_count  INT NOT NULL DEFAULT 0,
              created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        # job_id ties a message's analytics events together. targets overrides where
        # replies go (default: back to the sender); admission holds what /queue/admit
        # decided, for /queue/complete to report.
        cur.execute("ALTER TABLE queue_messages ADD COLUMN IF NOT EXISTS job_id TEXT;")
        cur.execute("ALTER TABLE queue_messages ADD COLUMN IF NOT EXISTS targets JSONB;")
        cur.execute("ALTER TABLE queue_messages ADD COLUMN IF NOT EXISTS admission JSONB;")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS queue_ready_idx "
            "ON queue_messages (visible_at, id);"
        )
        # Leases are looked up by handle on every edge call; without this it seq-scans,
        # which only hurts once a backlog has built up - exactly when it needs to be cheap.
        cur.execute(
            "CREATE INDEX IF NOT EXISTS queue_handle_idx "
            "ON queue_messages (receipt_handle) WHERE receipt_handle IS NOT NULL;"
        )
        # Platform message ids already accepted, so a re-delivered webhook is not
        # queued twice. Ids only - no content - and pruned with the same max age.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ingest_seen (
              key     TEXT PRIMARY KEY,
              seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        outbox.init_outbox_db(cur)
        identity.init_identity_db(cur)
        conn.commit()


# --- ingest side (called by ingest.py inside its transaction)

def first_delivery(cur, source, message_id):
    """True the first time a platform message id is seen; False for a re-delivery."""
    cur.execute(
        "INSERT INTO ingest_seen (key) VALUES (%s) ON CONFLICT DO NOTHING;",
        (f"{source}:{message_id}",),
    )
    return cur.rowcount == 1


def is_stale(sent_at):
    return sent_at < time.time() - MAX_AGE_SECONDS


def count_dropped(cur, n):
    if n:
        cur.execute("UPDATE totals SET dropped = dropped + %s WHERE id = 1;", (n,))


def enqueue(cur, source, body, sent_at, job_id, targets=None):
    cur.execute(
        "INSERT INTO queue_messages (source, body, sent_at, job_id, targets) "
        "VALUES (%s, %s, to_timestamp(%s), %s, %s);",
        (source, body, sent_at, job_id, json.dumps(targets) if targets else None),
    )


def queue_depth():
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS n FROM queue_messages WHERE {LEASABLE};", _params())
        return int(cur.fetchone()["n"])


# --- sweeper

def sweep():
    """Drop what will never be delivered: messages past the max age, poison messages
    whose final lease lapsed without an ack, and replies still unsent past the max age.
    Runs on a timer, so nothing outlives the max age even while no edge is polling."""
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM queue_messages
            WHERE sent_at < now() - make_interval(secs => %(age)s::double precision)
               OR (receive_count >= %(max)s AND visible_at <= now())
            RETURNING id, source, receive_count,
                      extract(epoch FROM now() - sent_at)::int AS age_seconds;
            """,
            _params(),
        )
        dropped = cur.fetchall()
        expired_replies = outbox.sweep_expired(cur, MAX_AGE_SECONDS)
        identity.sweep_tokens(cur)
        cur.execute(
            "DELETE FROM ingest_seen "
            "WHERE seen_at < now() - make_interval(secs => %s::double precision);",
            (MAX_AGE_SECONDS,),
        )
        count_dropped(cur, len(dropped) + expired_replies)
        conn.commit()
    if expired_replies:
        log.warning("dropping %s unsent repl(ies) past the %ss max age", expired_replies, MAX_AGE_SECONDS)
    if not dropped:
        return
    stale = [r for r in dropped if r["age_seconds"] >= MAX_AGE_SECONDS]
    poison = [r for r in dropped if r["age_seconds"] < MAX_AGE_SECONDS]
    # One line per poison message: it should be rare, the body is gone, and the log is the
    # only forensic record left. An age sweep can cover thousands of rows at once, so it
    # gets a summary instead.
    for row in poison:
        log.warning(
            "dropping message id=%s source=%s after %s receives",
            row["id"], row["source"], row["receive_count"],
        )
    if stale:
        log.warning(
            "dropping %s message(s) past the %ss max age (oldest %ss)",
            len(stale), MAX_AGE_SECONDS, max(r["age_seconds"] for r in stale),
        )


def run_sweeper(stop):
    while not stop.wait(SWEEP_INTERVAL_SECONDS):
        try:
            sweep()
        except Exception:
            log.exception("queue sweep failed")


def start_sweeper():
    stop = threading.Event()
    threading.Thread(target=run_sweeper, args=(stop,), name="QueueSweeper", daemon=True).start()
    return stop


# --- edge API

def require_edge(request: Request):
    """Check the shared bearer token; return the calling edge's self-reported name."""
    header = request.headers.get("Authorization", "")
    token = header[len("Bearer "):] if header.startswith("Bearer ") else ""
    if not QUEUE_TOKEN or not hmac.compare_digest(token.encode(), QUEUE_TOKEN.encode()):
        raise HTTPException(status_code=401, detail="unauthorized")
    return request.headers.get("X-Instance-Id") or "unknown"


async def _json_body(request):
    try:
        body = json.loads(await request.body())
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _try_lease(n, min_depth, edge):
    with db.pool().connection() as conn, conn.cursor() as cur:
        if min_depth:
            cur.execute(f"SELECT count(*) AS n FROM queue_messages WHERE {LEASABLE};", _params())
            n = min(n, int(cur.fetchone()["n"]) - min_depth)
            if n <= 0:
                return []
        # MATERIALIZED is load-bearing: as a plain IN (SELECT ... LIMIT n) the planner is
        # free to run the pick as a per-row subplan, which re-applies the LIMIT on every
        # outer row and leases the whole queue instead of n.
        cur.execute(
            f"""
            WITH picked AS MATERIALIZED (
              SELECT id FROM queue_messages
              WHERE {LEASABLE}
              ORDER BY id
              FOR UPDATE SKIP LOCKED
              LIMIT %(n)s
            )
            UPDATE queue_messages q SET
              visible_at     = now() + make_interval(secs => %(vis)s::double precision),
              receive_count  = receive_count + 1,
              receipt_handle = gen_random_uuid()::text,
              leased_by      = %(edge)s
            FROM picked p
            WHERE q.id = p.id
            RETURNING q.id, q.source, q.body, q.receipt_handle, q.receive_count, q.job_id,
                      extract(epoch FROM q.sent_at)::float8 AS sent_at;
            """,
            _params(n=n, vis=VISIBILITY_TIMEOUT, edge=edge),
        )
        rows = cur.fetchall()
        conn.commit()
    return rows


def _jobs_for(rows, edge, uptime, client_ip):
    """Describe leased rows as jobs, record the edge's heartbeat, and count each message
    once, on its first lease."""
    jobs, events, after = [], [], []
    for r in rows:
        parsed = SOURCES[r["source"]].parse(r["body"])
        jobs.append({
            "handle": r["receipt_handle"],
            "kind": parsed["kind"],
            "mime_type": (parsed["media"] or {}).get("mime_type"),
            "sent_at": r["sent_at"],
        })
        if r["receive_count"] == 1:
            events.append({"kind": "message", "ts": time.time(), "message_type": parsed["type"],
                           "user_hash": stats.user_hash(parsed["user_key"])})
            after.append((parsed["user_key"], {"user": parsed["user_key"], "type": parsed["type"],
                                                "job_id": r["job_id"]}))
    with db.pool().connection() as conn, conn.cursor() as cur:
        stats.heartbeat(cur, edge, uptime, None, client_ip)
        minutes = stats.write_events(cur, edge, events)
        conn.commit()
        stats.cache_messages(cur, edge, minutes)
    for user, props in after:
        analytics.capture(user, "message-received", props, edge)
    return jobs


@queue_api.post("/queue/lease")
async def lease(request: Request, edge: str = Depends(require_edge)):
    payload = await _json_body(request)
    n = max(1, min(int(payload.get("max", 1)), MAX_BATCH))
    wait = max(0, min(int(payload.get("wait", 0)), MAX_WAIT_SECONDS))
    min_depth = max(0, int(payload.get("min_depth", 0)))

    # Long poll without holding a thread: each attempt runs on the threadpool, and the
    # wait between attempts is an await, so idle edges cost nothing.
    deadline = time.monotonic() + wait
    backoff = 0.2
    rows = []
    while not _shutting_down.is_set():
        rows = await run_in_threadpool(_try_lease, n, min_depth, edge)
        if rows or time.monotonic() >= deadline:
            break
        await asyncio.sleep(min(backoff, max(0.0, deadline - time.monotonic())))
        backoff = min(backoff * 2.5, 1.0)

    log.debug("lease edge=%s max=%s wait=%s min_depth=%s -> %s", edge, n, wait, min_depth, len(rows))
    client_ip = request.client.host if request.client else None
    uptime = float(payload.get("uptime_seconds") or 0)
    jobs = await run_in_threadpool(_jobs_for, rows, edge, uptime, client_ip)
    return {"jobs": jobs}


def _leased(cur, handle, lock=False):
    """The job behind a receipt handle, or None if that lease no longer holds it."""
    cur.execute(
        "SELECT id, source, body, job_id, targets, admission, "
        "extract(epoch FROM sent_at)::float8 AS sent_at "
        "FROM queue_messages WHERE receipt_handle = %s" + (" FOR UPDATE;" if lock else ";"),
        (handle,),
    )
    row = cur.fetchone()
    if row is None:
        return None, None, None
    adapter = SOURCES[row["source"]]
    parsed = adapter.parse(row["body"])
    return row, parsed, row["targets"] or [adapter.target(parsed)]


def _media_ref(handle):
    with db.pool().connection() as conn, conn.cursor() as cur:
        row, parsed, _ = _leased(cur, handle)
        # Only while the lease is live: an expired handle must not keep a download open.
        cur.execute(
            "SELECT visible_at > now() AS live FROM queue_messages WHERE receipt_handle = %s;",
            (handle,),
        )
        live = cur.fetchone()
    if row is None or not live or not live["live"] or not parsed["media"]:
        return None, None
    return row["source"], parsed["media"]


@queue_api.get("/queue/media/{handle}")
async def media(handle: str, edge: str = Depends(require_edge)):
    source, ref = await run_in_threadpool(_media_ref, handle)
    if ref is None:
        raise HTTPException(status_code=404, detail="no such lease")
    try:
        response, content_type = await SOURCES[source].open_media(ref)
    except Exception as e:
        log.warning("media for lease %s (%s) unavailable: %s", handle, source, e)
        raise HTTPException(status_code=502, detail="media unavailable")
    return StreamingResponse(
        response.aiter_bytes(), media_type=content_type, background=BackgroundTask(response.aclose)
    )


def _close_with_notice(cur, row, targets, text):
    for target in targets:
        outbox.enqueue_text(cur, target, text, row["sent_at"])
    cur.execute("DELETE FROM queue_messages WHERE id = %s;", (row["id"],))


def _admit(handle, duration, edge):
    """Decide whether a job may be transcribed. On refusal the hub tells the user why
    and closes the job; the edge just stops."""
    after = []
    with db.pool().connection() as conn, conn.cursor() as cur:
        row, parsed, targets = _leased(cur, handle, lock=True)
        if row is None:
            return None
        user = parsed["user_key"]
        notice = None
        if duration is None:
            notice = messages.DURATION_FAILED
        elif duration > limits.MAX_AUDIO_SECONDS:
            notice = messages.TOO_LONG
        else:
            allowed, has_resources_left, bucket = limits.admit(user, duration)
            if not allowed:
                notice = messages.rate_limited(bucket.minutes_until_allowed(duration))
                after.append(("rate-limit-hit", {
                    "user": user,
                    "messages_remaining": bucket.messages_remaining,
                    "seconds_remaining": bucket.seconds_remaining,
                    "requested_duration": duration,
                    "job_id": row["job_id"],
                }))
        if notice:
            _close_with_notice(cur, row, targets, notice)
            ok = False
        else:
            cur.execute(
                "UPDATE queue_messages SET admission = %s WHERE id = %s;",
                (json.dumps({"duration": duration, "has_resources_left": has_resources_left}), row["id"]),
            )
            for target in targets:
                outbox.enqueue_receipt(cur, target, typing=True, sent_at=row["sent_at"])
            ok = True
        conn.commit()
    for event, props in after:
        analytics.capture(user, event, props, edge)
    return ok


@queue_api.post("/queue/admit")
async def admit(request: Request, edge: str = Depends(require_edge)):
    payload = await _json_body(request)
    handle = payload.get("handle")
    if not handle:
        raise HTTPException(status_code=400, detail="handle required")
    duration = payload.get("duration")
    ok = await run_in_threadpool(_admit, handle, None if duration is None else float(duration), edge)
    if ok is None:
        raise HTTPException(status_code=409, detail="lease no longer held")
    return {"ok": ok}


def _complete(handle, text, error, transcription_seconds, duration, edge):
    """Turn a finished job into its reply, atomically: the outbox rows and the job's
    removal commit together, so a transcript is never lost between edge and user.
    Returns False if this lease no longer holds the job (another edge has it, or it
    expired) - unless this very call already succeeded, which makes retries safe."""
    after = []
    with db.pool().connection() as conn, conn.cursor() as cur:
        row, parsed, targets = _leased(cur, handle, lock=True)
        if row is None:
            cur.execute("SELECT 1 FROM outbox WHERE source_handle = %s;", (handle,))
            return cur.fetchone() is not None
        user = parsed["user_key"]
        transcribed = error is None
        reply = text if transcribed else messages.ONLY_RECORDINGS
        nudge = messages.maybe_nudge() if transcribed else None
        for i, target in enumerate(targets):
            # Users still getting transcripts on WhatsApp are told WhatsApp now charges
            # for these messages, after each one.
            notice = messages.WA_PRICING_NOTICE if transcribed and target["channel"] == "whatsapp" else None
            outbox.enqueue_text(
                cur, target, reply, row["sent_at"], extra=[notice, nudge],
                source_handle=handle if i == 0 else f"{handle}#{i}",
            )
        minutes = []
        if transcribed:
            admission = row["admission"] or {}
            audio_seconds = admission.get("duration") or duration or 0
            minutes = stats.write_events(cur, edge, [{
                "kind": "transcription", "ts": time.time(), "duration_seconds": audio_seconds,
                "message_type": "audio", "user_hash": stats.user_hash(user),
            }])
            after.append(("transcribe-done", {
                "user": user,
                "audio_duration_seconds": audio_seconds,
                "transcription_seconds": transcription_seconds,
                "has_resources_left": admission.get("has_resources_left"),
                "job_id": row["job_id"],
            }))
        cur.execute("DELETE FROM queue_messages WHERE id = %s;", (row["id"],))
        conn.commit()
        stats.cache_messages(cur, edge, minutes)
    for event, props in after:
        analytics.capture(user, event, props, edge)
    return True


@queue_api.post("/queue/complete")
async def complete(request: Request, edge: str = Depends(require_edge)):
    payload = await _json_body(request)
    handle = payload.get("handle")
    if not handle:
        raise HTTPException(status_code=400, detail="handle required")
    error = payload.get("error")
    text = payload.get("text")
    if error is None and not isinstance(text, str):
        raise HTTPException(status_code=400, detail="text or error required")
    done = await run_in_threadpool(
        _complete, handle, text, error, payload.get("transcription_seconds"),
        payload.get("duration"), edge,
    )
    if not done:
        raise HTTPException(status_code=409, detail="lease no longer held")
    log.debug("complete edge=%s handle=%s error=%s", edge, handle, error)
    return {"ok": True}


@queue_api.get("/queue/depth")
def depth(edge: str = Depends(require_edge)):
    return {"depth": queue_depth()}
