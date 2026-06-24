import os
import random
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
STALE_SECONDS = int(os.environ.get("STALE_SECONDS", "90"))

# Events older than this are pruned; long enough to cover the 24h window with margin.
EVENT_RETENTION_SECONDS = 25 * 3600

# In-process per-minute message-count cache, so /api/stats can serve the 24h graph
# without re-aggregating the events table on every poll. Keyed by minute-bucket epoch
# (int seconds, floored to the minute) -> {instance_id: count}. The single site process
# is the sole writer for all instances' ingests, so this stays consistent across the
# fleet. waitress is multithreaded, so every access is guarded by _series_lock.
SERIES_MINUTES = 24 * 60  # graph window: 1440 one-minute buckets

_series_lock = threading.Lock()
_series_counts = defaultdict(lambda: defaultdict(int))
_series_loaded = False


def _minute_epoch(ts):
    """Floor a datetime to its minute as an int epoch (seconds)."""
    return int(ts.timestamp()) // 60 * 60


def _ensure_series_loaded(cur):
    """One-time backfill of the cache from the DB (cold start / process restart)."""
    global _series_loaded
    if _series_loaded:
        return
    cur.execute(
        """
        SELECT instance_id,
               extract(epoch FROM date_trunc('minute', ts))::bigint AS m,
               count(*) AS cnt
        FROM events
        WHERE kind = 'message' AND ts > now() - interval '24 hours'
        GROUP BY instance_id, date_trunc('minute', ts);
        """
    )
    for row in cur.fetchall():
        _series_counts[int(row["m"])][row["instance_id"]] = int(row["cnt"])
    _series_loaded = True


def _prune_series(now_epoch):
    cutoff = now_epoch - SERIES_MINUTES * 60
    for m in [m for m in _series_counts if m < cutoff]:
        del _series_counts[m]


def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS totals (
              id INT PRIMARY KEY DEFAULT 1,
              messages BIGINT NOT NULL DEFAULT 0,
              transcriptions BIGINT NOT NULL DEFAULT 0,
              duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
              CHECK (id = 1)
            );
            """
        )
        cur.execute("INSERT INTO totals (id) VALUES (1) ON CONFLICT DO NOTHING;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL,
              instance_id TEXT NOT NULL,
              kind TEXT NOT NULL,
              duration_seconds DOUBLE PRECISION,
              user_hash TEXT,
              message_type TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS events_ts_idx ON events (ts);")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS instances (
              instance_id TEXT PRIMARY KEY,
              last_seen TIMESTAMPTZ NOT NULL,
              uptime_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
              queue_depth INT
            );
            """
        )
        conn.commit()


def reset_db():
    """Wipe all stats back to zero. Gated behind the RESET_DB env flag in install.sh so it
    only runs on an explicitly-flagged deploy (used to clear test data)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM events;")
        cur.execute("DELETE FROM instances;")
        cur.execute("UPDATE totals SET messages = 0, transcriptions = 0, duration_seconds = 0 WHERE id = 1;")
        conn.commit()


def _event_ts(raw):
    """Accept a unix epoch (int/float) or ISO string; default to now."""
    if raw is None:
        return datetime.now(timezone.utc)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return datetime.now(timezone.utc)


@app.post("/api/events")
def ingest():
    if not INGEST_TOKEN or request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    instance_id = body.get("instance_id")
    if not instance_id:
        return jsonify({"error": "instance_id required"}), 400

    uptime_seconds = float(body.get("uptime_seconds") or 0)
    queue_depth = body.get("queue_depth")
    events = body.get("events") or []

    messages_delta = 0
    transcriptions_delta = 0
    duration_delta = 0.0
    message_minutes = []

    with connect() as conn, conn.cursor() as cur:
        for ev in events:
            kind = ev.get("kind")
            if kind not in ("message", "transcription"):
                continue
            ts = _event_ts(ev.get("ts"))
            duration = ev.get("duration_seconds")
            cur.execute(
                """
                INSERT INTO events (ts, instance_id, kind, duration_seconds, user_hash, message_type)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (ts, instance_id, kind, duration, ev.get("user_hash"), ev.get("message_type")),
            )
            if kind == "message":
                messages_delta += 1
                message_minutes.append(_minute_epoch(ts))
            else:
                transcriptions_delta += 1
                duration_delta += float(duration or 0)

        if messages_delta or transcriptions_delta or duration_delta:
            cur.execute(
                """
                UPDATE totals
                   SET messages = messages + %s,
                       transcriptions = transcriptions + %s,
                       duration_seconds = duration_seconds + %s
                 WHERE id = 1;
                """,
                (messages_delta, transcriptions_delta, duration_delta),
            )

        cur.execute(
            """
            INSERT INTO instances (instance_id, last_seen, uptime_seconds, queue_depth)
            VALUES (%s, now(), %s, %s)
            ON CONFLICT (instance_id) DO UPDATE
              SET last_seen = now(),
                  uptime_seconds = EXCLUDED.uptime_seconds,
                  queue_depth = EXCLUDED.queue_depth;
            """,
            (instance_id, uptime_seconds, queue_depth),
        )

        # Opportunistic prune so events don't grow without bound.
        if random.random() < 0.02:
            cur.execute(
                "DELETE FROM events WHERE ts < now() - make_interval(secs => %s);",
                (EVENT_RETENTION_SECONDS,),
            )

        conn.commit()

        if message_minutes:
            with _series_lock:
                _ensure_series_loaded(cur)
                for m in message_minutes:
                    _series_counts[m][instance_id] += 1

    return jsonify({"ok": True, "ingested": len(events)})


@app.get("/api/stats")
def stats():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT messages, transcriptions, duration_seconds FROM totals WHERE id = 1;")
        totals = cur.fetchone() or {"messages": 0, "transcriptions": 0, "duration_seconds": 0}

        cur.execute(
            """
            SELECT
              count(*) FILTER (WHERE ts > now() - interval '1 minute')  AS m1,
              count(*) FILTER (WHERE ts > now() - interval '5 minutes') AS m5,
              count(*) FILTER (WHERE ts > now() - interval '1 hour')    AS m60,
              count(*) FILTER (WHERE ts > now() - interval '24 hours')  AS m1440
            FROM events
            WHERE kind = 'message';
            """
        )
        msg = cur.fetchone()

        cur.execute(
            """
            SELECT
              count(*) AS n,
              avg(duration_seconds) AS avg_d,
              percentile_cont(0.5)  WITHIN GROUP (ORDER BY duration_seconds) AS median_d,
              percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_d
            FROM events
            WHERE kind = 'transcription' AND ts > now() - interval '1 hour';
            """
        )
        tr = cur.fetchone()

        cur.execute(
            """
            SELECT count(DISTINCT user_hash) AS u
            FROM events
            WHERE user_hash IS NOT NULL AND ts > now() - interval '24 hours';
            """
        )
        unique_users_24h = cur.fetchone()["u"]

        cur.execute(
            """
            SELECT instance_id, uptime_seconds, queue_depth,
                   extract(epoch FROM (now() - last_seen)) AS age_seconds
            FROM instances
            WHERE last_seen > now() - make_interval(secs => %s)
            ORDER BY instance_id;
            """,
            (STALE_SECONDS,),
        )
        instances = cur.fetchall()

        # Per-minute message counts over the last 24h, served from the in-process cache
        # (zero-filled for a continuous x-axis). The cache is backfilled from the DB once
        # on cold start and kept current by ingest, so no aggregation runs per poll.
        with _series_lock:
            _ensure_series_loaded(cur)
            now_epoch = int(time.time()) // 60 * 60
            _prune_series(now_epoch)
            minutes = [now_epoch - (SERIES_MINUTES - 1 - i) * 60 for i in range(SERIES_MINUTES)]
            series = [
                {"t": m, "count": sum(_series_counts.get(m, {}).values())}
                for m in minutes
            ]
            host_ids = sorted({iid for c in _series_counts.values() for iid in c})
            series_by_instance = [
                {
                    "instance_id": iid,
                    "counts": [_series_counts.get(m, {}).get(iid, 0) for m in minutes],
                }
                for iid in host_ids
            ]

    latest_queue_depth = None
    for inst in instances:
        if inst["queue_depth"] is not None:
            latest_queue_depth = inst["queue_depth"]
            break

    return jsonify(
        {
            "totals": {
                "messages": totals["messages"],
                "transcriptions": totals["transcriptions"],
                "duration_seconds": totals["duration_seconds"],
            },
            "messages": {
                "last_1m": msg["m1"],
                "last_5m": msg["m5"],
                "last_1h": msg["m60"],
                "last_24h": msg["m1440"],
                "per_min_1h": round((msg["m60"] or 0) / 60.0, 2),
            },
            "transcriptions_1h": {
                "count": tr["n"],
                "avg_duration": tr["avg_d"],
                "median_duration": tr["median_d"],
                "p95_duration": tr["p95_d"],
            },
            "unique_users_24h": unique_users_24h,
            "queue_depth": latest_queue_depth,
            "instances": [
                {
                    "instance_id": i["instance_id"],
                    "uptime_seconds": i["uptime_seconds"],
                    "queue_depth": i["queue_depth"],
                    "age_seconds": round(i["age_seconds"], 1),
                }
                for i in instances
            ],
            "live_instances": len(instances),
            "messages_series": series,
            "messages_series_by_instance": series_by_instance,
            "generated_at": time.time(),
        }
    )


@app.get("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
