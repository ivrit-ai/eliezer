"""The ivrit.ai Android app as a source: audio shared into the app, sent straight here
under the user's Google account, transcribed like any voice message, and fetched back
by the app.

Unlike WhatsApp and Telegram nothing is pushed back: the app asks for its jobs. So
"app" is a queue source (parse, target, open_media, like whatsapp.py) whose replies
are written into app_jobs rather than the outbox; queue_api.py hands them over through
refuse() and finish().

Who is asking is the Google account: the app signs in with Google on the phone and
sends the ID token Google issued it. Limits are per account (user key app:<sub>), with
the same 10-minute cap and hourly buckets as everyone else.
"""

import json
import logging
import os
import re
import time
import urllib.parse
import uuid

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

import db

log = logging.getLogger(__name__)

# The app's OAuth client ids: an ID token must be issued to one of them. Unset means
# the app's API is off (503), never open.
GOOGLE_CLIENT_IDS = [c.strip() for c in os.environ.get("APP_GOOGLE_CLIENT_IDS", "").split(",") if c.strip()]
GOOGLE_JWKS_URL = os.environ.get("APP_GOOGLE_JWKS_URL", "https://www.googleapis.com/oauth2/v3/certs")
GOOGLE_ISSUERS = ["accounts.google.com", "https://accounts.google.com"]

# Where the app's pages are served from, for CORS (app.py).
APP_ORIGINS = [o.strip() for o in os.environ.get("APP_ORIGINS", "https://app.ivrit.ai").split(",") if o.strip()]

# As for Telegram: a 10-minute voice note is far smaller, and the bytes sit in
# Postgres until a worker has them.
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
# Jobs one account may have waiting at once; more is someone looping, not a person.
MAX_PENDING = 3
# How long results stay for the app to fetch. The app keeps its own copy.
KEEP_SECONDS = 3 * 24 * 3600

# Formats a worker transcribes as they are (a WhatsApp voice note is Ogg Opus);
# anything else, video included, is converted first.
DIRECT_MIME = {"audio/ogg", "audio/opus"}


def init_db(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_jobs (
          job_id       TEXT PRIMARY KEY,
          google_sub   TEXT NOT NULL,
          status       TEXT NOT NULL DEFAULT 'queued',
          filename     TEXT,
          mime         TEXT NOT NULL,
          audio        BYTEA,
          text         TEXT,
          error        TEXT,
          wait_minutes INT,
          duration     DOUBLE PRECISION,
          handle       TEXT,
          created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
          finished_at  TIMESTAMPTZ
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS app_jobs_user_idx ON app_jobs (google_sub, created_at DESC);")
    # upload_id: the app's own id for an upload, so a re-send (the app died
    # before it saw the answer) finds the job instead of making a second one.
    # origin: where the recording was shared from ("whatsapp"), for the app.
    cur.execute("ALTER TABLE app_jobs ADD COLUMN IF NOT EXISTS upload_id TEXT;")
    cur.execute("ALTER TABLE app_jobs ADD COLUMN IF NOT EXISTS origin TEXT;")
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS app_jobs_upload_idx ON app_jobs (google_sub, upload_id) "
        "WHERE upload_id IS NOT NULL;"
    )


# --- the queue source (see channel.py)

def parse(body):
    b = json.loads(body)
    return {
        "sender": None,
        "user_key": f"app:{b['sub']}",
        "message_id": b["job_id"],
        "type": "audio",
        "kind": "audio" if b["mime"] in DIRECT_MIME else "document",
        "text": None,
        "media": {"job_id": b["job_id"], "mime_type": b["mime"]},
    }


def target(parsed):
    return {"channel": "app", "address": parsed["message_id"]}


class _Stored:
    """The uploaded bytes, shaped like the httpx response the platform adapters return."""

    def __init__(self, data):
        self.data = data

    async def aiter_bytes(self):
        for i in range(0, len(self.data), 64 * 1024):
            yield self.data[i:i + 64 * 1024]

    async def aclose(self):
        pass


def _audio(job_id):
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT audio, mime FROM app_jobs WHERE job_id = %s AND audio IS NOT NULL;", (job_id,))
        return cur.fetchone()


async def open_media(media):
    row = await run_in_threadpool(_audio, media["job_id"])
    if row is None:
        raise LookupError("app upload gone")
    return _Stored(bytes(row["audio"])), row["mime"]


# --- replies, from queue_api.py inside its transaction

def refuse(cur, parsed, code, wait_minutes=None):
    """The job was not admitted (too long, rate limited, unmeasurable)."""
    cur.execute(
        "UPDATE app_jobs SET status = 'failed', error = %s, wait_minutes = %s, audio = NULL, "
        "finished_at = now() WHERE job_id = %s;",
        (code, wait_minutes, parsed["message_id"]),
    )


def finish(cur, parsed, handle, text, error, duration):
    """The job is done: its transcript, or why there is none. The handle makes a retried
    /queue/complete recognisable once the queue row is gone (see already_finished)."""
    cur.execute(
        "UPDATE app_jobs SET status = %s, text = %s, error = %s, duration = %s, handle = %s, "
        "audio = NULL, finished_at = now() WHERE job_id = %s;",
        ("done" if error is None else "failed", text, error, duration, handle, parsed["message_id"]),
    )


def already_finished(cur, handle):
    cur.execute("SELECT 1 FROM app_jobs WHERE handle = %s;", (handle,))
    return cur.fetchone() is not None


def expire(cur, job_ids):
    """Queue rows the sweeper dropped (too old, or failing every time): say so."""
    if job_ids:
        cur.execute(
            "UPDATE app_jobs SET status = 'failed', error = 'expired', audio = NULL, finished_at = now() "
            "WHERE job_id = ANY(%s) AND status = 'queued';",
            (list(job_ids),),
        )


def sweep(cur):
    cur.execute(
        "DELETE FROM app_jobs WHERE created_at < now() - make_interval(secs => %s::double precision);",
        (KEEP_SECONDS,),
    )


# --- the app's API

_jwks = None


def _signing_key(token):
    global _jwks
    if _jwks is None:
        # Google rotates its keys; the client caches them and refetches on an unknown kid.
        _jwks = jwt.PyJWKClient(GOOGLE_JWKS_URL, cache_keys=True, lifespan=3600)
    return _jwks.get_signing_key_from_jwt(token).key


def google_account(request: Request):
    """The Google account behind the request's ID token, or 401. Sync, so FastAPI runs
    it on the threadpool: fetching Google's keys is a blocking call."""
    if not GOOGLE_CLIENT_IDS:
        raise HTTPException(status_code=503, detail="app sign-in not configured")
    header = request.headers.get("Authorization", "")
    token = header[len("Bearer "):] if header.startswith("Bearer ") else ""
    if not token:
        raise HTTPException(status_code=401, detail="sign-in required")
    try:
        claims = jwt.decode(
            token,
            _signing_key(token),
            algorithms=["RS256"],
            audience=GOOGLE_CLIENT_IDS,
            issuer=GOOGLE_ISSUERS,
            options={"require": ["exp", "iat", "sub", "aud", "iss"]},
            leeway=60,
        )
    except Exception as e:
        log.info("app token rejected: %s", e)
        raise HTTPException(status_code=401, detail="sign-in required")
    if claims.get("email") and not claims.get("email_verified"):
        raise HTTPException(status_code=401, detail="unverified email")
    return {"sub": str(claims["sub"]), "email": claims.get("email")}


app_api = APIRouter(prefix="/app/v1")


def _view(row):
    out = {
        "job_id": row["job_id"],
        "status": row["status"],
        "filename": row["filename"],
        "created_at": row["created_at"].timestamp(),
    }
    if row["upload_id"]:
        out["upload_id"] = row["upload_id"]
    if row["origin"]:
        out["origin"] = row["origin"]
    if row["status"] == "done":
        out["text"] = row["text"]
    if row["error"]:
        out["error"] = row["error"]
    if row["wait_minutes"] is not None:
        out["wait_minutes"] = row["wait_minutes"]
    if row["duration"] is not None:
        out["duration"] = row["duration"]
    if row["finished_at"] is not None:
        out["finished_at"] = row["finished_at"].timestamp()
    return out


VIEW_COLUMNS = "job_id, status, filename, text, error, wait_minutes, duration, created_at, finished_at, upload_id, origin"


def _submit(account, data, mime, filename, upload_id, origin):
    """The job for this upload: (row, created). None if too many are waiting."""
    # Imported here: queue_api imports this module for its SOURCES.
    import queue_api

    job_id = uuid.uuid4().hex
    with db.pool().connection() as conn, conn.cursor() as cur:
        # One account at a time, so concurrent uploads cannot both pass the count,
        # nor two sends of one upload both make a job.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (f"app:{account['sub']}",))
        if upload_id:
            cur.execute(
                f"SELECT {VIEW_COLUMNS} FROM app_jobs WHERE google_sub = %s AND upload_id = %s;",
                (account["sub"], upload_id),
            )
            existing = cur.fetchone()
            if existing:
                return existing, False
        cur.execute(
            "SELECT count(*) AS n FROM app_jobs WHERE google_sub = %s AND status = 'queued';",
            (account["sub"],),
        )
        if cur.fetchone()["n"] >= MAX_PENDING:
            return None
        cur.execute(
            "INSERT INTO app_jobs (job_id, google_sub, filename, mime, audio, upload_id, origin) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING {VIEW_COLUMNS};",
            (job_id, account["sub"], filename, mime, data, upload_id, origin),
        )
        row = cur.fetchone()
        body = json.dumps({"job_id": job_id, "sub": account["sub"], "mime": mime})
        queue_api.enqueue(cur, "app", body, time.time(), job_id)
        conn.commit()
    return row, True


UPLOAD_ID = re.compile(r"^[A-Za-z0-9-]{8,64}$")
ORIGINS = {"whatsapp"}


@app_api.post("/jobs")
async def submit(request: Request, account: dict = Depends(google_account)):
    """A clip to transcribe: the file itself as the body, its type as Content-Type,
    its name (URL-encoded) in X-Filename, and optionally the app's own id for this
    upload in X-Upload-Id and where it was shared from in X-Origin. Answers 202 with
    the job; a re-send of an upload already received, 200 with that same job."""
    mime = (request.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if not (mime.startswith("audio/") or mime.startswith("video/")):
        raise HTTPException(status_code=415, detail="audio or video only")
    length = int(request.headers.get("Content-Length") or 0)
    if length > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="too large")
    data = bytearray()
    async for chunk in request.stream():
        data += chunk
        if len(data) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="too large")
    if not data:
        raise HTTPException(status_code=400, detail="empty")
    filename = urllib.parse.unquote(request.headers.get("X-Filename", ""))[:200] or None
    upload_id = request.headers.get("X-Upload-Id") or None
    if upload_id and not UPLOAD_ID.match(upload_id):
        raise HTTPException(status_code=400, detail="bad upload id")
    origin = request.headers.get("X-Origin") if request.headers.get("X-Origin") in ORIGINS else None
    result = await run_in_threadpool(_submit, account, bytes(data), mime, filename, upload_id, origin)
    if result is None:
        raise HTTPException(status_code=429, detail="too many pending")
    row, created = result
    return JSONResponse(_view(row), status_code=202 if created else 200)


def _jobs(sub, job_id=None):
    with db.pool().connection() as conn, conn.cursor() as cur:
        if job_id:
            cur.execute(f"SELECT {VIEW_COLUMNS} FROM app_jobs WHERE job_id = %s AND google_sub = %s;", (job_id, sub))
        else:
            cur.execute(
                f"SELECT {VIEW_COLUMNS} FROM app_jobs WHERE google_sub = %s ORDER BY created_at DESC LIMIT 50;",
                (sub,),
            )
        return cur.fetchall()


@app_api.get("/jobs")
async def jobs(account: dict = Depends(google_account)):
    """This account's jobs from the last three days, newest first."""
    rows = await run_in_threadpool(_jobs, account["sub"])
    return {"jobs": [_view(r) for r in rows]}


@app_api.get("/jobs/{job_id}")
async def job(job_id: str, account: dict = Depends(google_account)):
    rows = await run_in_threadpool(_jobs, account["sub"], job_id)
    if not rows:
        raise HTTPException(status_code=404, detail="no such job")
    return _view(rows[0])
