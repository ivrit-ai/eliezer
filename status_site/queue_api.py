"""Postgres-backed message queue with an HTTP lease/ack API for the transcription edges.

Replaces the AWS SQS queue the edges used to poll. One table, one FOR UPDATE SKIP
LOCKED statement, real visibility-timeout semantics. Webhooks (ingest.py) feed it.

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

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
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

# A message an edge may take right now. Shared by lease, depth and the overflow
# threshold, so all three agree on what "waiting" means.
LEASABLE = """
    visible_at <= now()
    AND receive_count < %(max)s
    AND sent_at >= now() - make_interval(secs => %(age)s::double precision)
"""

queue_api = APIRouter()

_pool = None

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


def pool():
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            DATABASE_URL,
            min_size=1,
            max_size=8,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _pool


def _params(**extra):
    return {"max": MAX_RECEIVES, "age": MAX_AGE_SECONDS, **extra}


def init_queue_db():
    with ConnectionPool(DATABASE_URL, min_size=1, max_size=1, open=True) as p:
        with p.connection() as conn, conn.cursor() as cur:
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
            cur.execute(
                "CREATE INDEX IF NOT EXISTS queue_ready_idx "
                "ON queue_messages (visible_at, id);"
            )
            # ack() looks messages up by handle; without this it seq-scans, which only
            # hurts once a backlog has built up - exactly when acks need to be cheap.
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
            cur.execute(
                "ALTER TABLE totals ADD COLUMN IF NOT EXISTS dropped BIGINT NOT NULL DEFAULT 0;"
            )
            conn.commit()


def ingest(source, items):
    """Queue webhook messages. items is [(message_id, sent_at_epoch, body)]; returns
    (queued, duplicates, stale). One transaction, so a webhook the platform retries
    after a failure is either fully queued or not at all."""
    queued = duplicates = stale = 0
    cutoff = time.time() - MAX_AGE_SECONDS
    with pool().connection() as conn, conn.cursor() as cur:
        for message_id, sent_at, body in items:
            if sent_at < cutoff:
                stale += 1
                continue
            cur.execute(
                "INSERT INTO ingest_seen (key) VALUES (%s) ON CONFLICT DO NOTHING;",
                (f"{source}:{message_id}",),
            )
            if cur.rowcount == 0:
                duplicates += 1
                continue
            cur.execute(
                "INSERT INTO queue_messages (source, body, sent_at) "
                "VALUES (%s, %s, to_timestamp(%s));",
                (source, body, sent_at),
            )
            queued += 1
        if stale:
            cur.execute("UPDATE totals SET dropped = dropped + %s WHERE id = 1;", (stale,))
        conn.commit()
    return queued, duplicates, stale


def queue_depth():
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS n FROM queue_messages WHERE {LEASABLE};", _params())
        return int(cur.fetchone()["n"])


def sweep():
    """Drop what will never be delivered: messages past the max age, and poison messages
    whose final lease lapsed without an ack. Runs on a timer, so nothing outlives the
    max age even while no edge is polling."""
    with pool().connection() as conn, conn.cursor() as cur:
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
        cur.execute(
            "DELETE FROM ingest_seen "
            "WHERE seen_at < now() - make_interval(secs => %s::double precision);",
            (MAX_AGE_SECONDS,),
        )
        if dropped:
            cur.execute(
                "UPDATE totals SET dropped = dropped + %s WHERE id = 1;", (len(dropped),)
            )
        conn.commit()
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


def _try_lease(n, min_depth, edge):
    with pool().connection() as conn, conn.cursor() as cur:
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
            RETURNING q.id, q.source, q.body, q.receipt_handle,
                      extract(epoch FROM q.sent_at)::float8 AS sent_at;
            """,
            _params(n=n, vis=VISIBILITY_TIMEOUT, edge=edge),
        )
        rows = cur.fetchall()
        conn.commit()
    return rows


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
    return {
        "messages": [
            {
                "handle": r["receipt_handle"],
                "source": r["source"],
                "body": r["body"],
                "sent_at": r["sent_at"],
            }
            for r in rows
        ]
    }


def _ack(handle):
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM queue_messages WHERE receipt_handle = %s;", (handle,))
        deleted = cur.rowcount
        conn.commit()
    return deleted


@queue_api.post("/queue/ack")
async def ack(request: Request, edge: str = Depends(require_edge)):
    payload = await _json_body(request)
    handle = payload.get("handle")
    if not handle:
        raise HTTPException(status_code=400, detail="handle required")
    deleted = await run_in_threadpool(_ack, handle)
    log.debug("ack edge=%s handle=%s deleted=%s", edge, handle, deleted)
    return {"ok": True}


@queue_api.get("/queue/depth")
def depth(edge: str = Depends(require_edge)):
    return {"depth": queue_depth()}
