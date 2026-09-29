"""Platform webhooks: verify each delivery, then route every message in it.

Media goes to the queue for an edge to transcribe. Everything else - commands, stray
text, other message types, refusals - the hub answers itself through the outbox, so an
edge only ever sees work that needs a GPU.
"""

import json
import logging
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse

import analytics
import db
import limits
import messages
import outbox
import queue_api
import stats
import whatsapp

log = logging.getLogger(__name__)

# Messages the hub handled itself are attributed to this instance in the statistics.
HUB = "hub"

SOURCES = {"whatsapp": whatsapp}

webhooks = APIRouter()


@webhooks.get("/webhook/{source}")
def webhook_verify(source: str, request: Request):
    adapter = SOURCES.get(source)
    if adapter is None:
        return JSONResponse({"error": "unknown source"}, status_code=404)
    challenge = adapter.verify_registration(request.query_params)
    if challenge is None:
        log.warning("webhook %s registration rejected: bad mode or verify token", source)
        return JSONResponse({"error": "forbidden"}, status_code=403)
    log.debug("webhook %s registration verified", source)
    return PlainTextResponse(challenge)


@webhooks.post("/webhook/{source}")
async def webhook_receive(source: str, request: Request):
    adapter = SOURCES.get(source)
    if adapter is None:
        return JSONResponse({"error": "unknown source"}, status_code=404)
    # The signature covers the exact bytes the platform sent, so verify before parsing.
    raw = await request.body()
    if not adapter.verify_payload(raw, request.headers):
        log.warning(
            "webhook %s REJECTED: signature mismatch (%s bytes) - check META_APP_SECRET",
            source, len(raw),
        )
        return JSONResponse({"error": "forbidden"}, status_code=403)
    try:
        payload = json.loads(raw)
    except ValueError:
        # Signed but unparseable: retrying would not help, so accept and drop it.
        log.warning("webhook %s: signed payload is not JSON (%s bytes)", source, len(raw))
        return {"ok": True}

    # A DB failure raises and returns 500, so the platform retries; the dedup table
    # makes that retry safe.
    summary = await run_in_threadpool(_route, source, list(adapter.split(payload)))
    log.debug("webhook %s: %s", source, summary)
    return {"ok": True}


def _route(source, items):
    """Handle one delivery's messages in a single transaction: all of it lands, or
    none of it and the platform retries."""
    adapter = SOURCES[source]
    counts = {"queued": 0, "answered": 0, "ignored": 0, "duplicate": 0, "stale": 0}
    events, after = [], []
    with db.pool().connection() as conn, conn.cursor() as cur:
        for message_id, sent_at, body in items:
            if queue_api.is_stale(sent_at):
                counts["stale"] += 1
                continue
            if not queue_api.first_delivery(cur, source, message_id):
                counts["duplicate"] += 1
                continue
            parsed = adapter.parse(body)
            target = adapter.target(parsed)
            user = parsed["sender"]

            if source == "whatsapp" and not limits.is_allowed_region(user):
                outbox.enqueue_text(cur, target, messages.REJECTED_REGION, sent_at)
                counts["answered"] += 1
                continue

            job_id = str(uuid.uuid4())
            if parsed["kind"]:
                # Counted when an edge first leases it, like the edges always did.
                queue_api.enqueue(cur, source, body, sent_at, job_id)
                outbox.enqueue_receipt(cur, target, typing=False, sent_at=sent_at)
                counts["queued"] += 1
                continue

            events.append({"kind": "message", "ts": time.time(), "message_type": parsed["type"],
                           "user_hash": stats.user_hash(user)})
            after.append((user, {"user": user, "type": parsed["type"], "job_id": job_id}))
            if parsed["type"] != "text":
                # Stickers, images, reactions...: counted, otherwise left alone.
                counts["ignored"] += 1
                continue
            outbox.enqueue_receipt(cur, target, typing=False, sent_at=sent_at)
            outbox.enqueue_text(cur, target, _answer(parsed["text"]), sent_at)
            counts["answered"] += 1

        queue_api.count_dropped(cur, counts["stale"])
        minutes = stats.write_events(cur, HUB, events)
        conn.commit()
        stats.cache_messages(cur, HUB, minutes)
    for user, props in after:
        analytics.capture(user, "message-received", props, HUB)
    return counts


def _answer(text):
    if text == "/status":
        return messages.status_text(stats.compute_stats(queue_api.queue_depth()))
    if text == "/detailed-status":
        return messages.detailed_status_text(stats.compute_stats(queue_api.queue_depth()))
    return messages.ONLY_RECORDINGS
