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
import control
import db
import identity
import limits
import messages
import outbox
import queue_api
import stats
import telegram
import whatsapp

log = logging.getLogger(__name__)

# Messages the hub handled itself are attributed to this instance in the statistics.
HUB = "hub"

# The bot's statuses that mean it is in a group ("left" and "kicked" mean it is not).
GROUP_PRESENT = ("member", "administrator", "restricted")

SOURCES = {"whatsapp": whatsapp, "telegram": telegram}

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
            "webhook %s REJECTED: signature mismatch (%s bytes) - check the channel's secret",
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
    counts = {"queued": 0, "answered": 0, "ignored": 0, "unlinked": 0, "duplicate": 0, "stale": 0}
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
            if parsed["type"] == "membership":
                if parsed.get("group"):
                    stats.set_telegram_group(cur, parsed["sender"], parsed["member_status"] in GROUP_PRESENT)
                else:
                    control.handle_membership(cur, parsed)
                continue
            if source == "telegram" and parsed.get("group"):
                # Any message proves the bot is there - which also picks up groups it
                # joined before their membership was tracked.
                if parsed.get("migrate_to"):
                    stats.set_telegram_group(cur, parsed["sender"], False)
                    stats.set_telegram_group(cur, parsed["migrate_to"], True)
                else:
                    stats.set_telegram_group(cur, parsed["sender"], True)
            default = adapter.target(parsed)
            user_key = parsed["user_key"]

            if source == "whatsapp" and not limits.is_allowed_region(parsed["sender"]):
                # Outside the served regions: ignored (a refusal would be a billed
                # WhatsApp message). Allowlisted numbers are an admin's call.
                if identity.user_of(cur, "whatsapp", parsed["sender"]) is None:
                    counts["ignored"] += 1
                    continue

            text = parsed["text"]
            if source == "whatsapp" and control.is_whatsapp_link(text):
                # Linking must work for exactly the people WhatsApp is being turned off
                # for, so it comes before any decision about where replies go.
                outbox.enqueue_receipt(cur, default, typing=False, sent_at=sent_at)
                control.handle_whatsapp_link(cur, parsed, sent_at)
                counts["answered"] += 1
                continue

            if source == "telegram" and parsed.get("group") and not parsed["kind"]:
                # In a group the bot transcribes audio and is otherwise silent: no
                # commands, no linking, no answers to conversation.
                counts["ignored"] += 1
                continue

            if source == "telegram" and parsed["type"] == "text":
                events.append(_message_event(parsed))
                after.append((user_key, {"user": user_key, "type": "text", "job_id": str(uuid.uuid4())}))
                control.handle_telegram_text(cur, parsed, sent_at)
                counts["answered"] += 1
                continue

            targets = identity.resolve_targets(cur, source, parsed, default)
            if not targets:
                # An unlinked WhatsApp user while WhatsApp replies are off: no reply, and
                # no read receipt either, which would read as "seen and ignored".
                counts["unlinked"] += 1
                continue
            if source == "whatsapp" and (parsed["kind"] or parsed["type"] == "text"):
                # Blue ticks cost nothing, and tell the user their message arrived even
                # when the reply goes elsewhere.
                outbox.enqueue_receipt(cur, default, typing=False, sent_at=sent_at)

            job_id = str(uuid.uuid4())
            if parsed["kind"]:
                size = (parsed["media"] or {}).get("size")
                if source == "telegram" and size and size > telegram.MAX_FILE_BYTES:
                    for target in targets:
                        outbox.enqueue_text(cur, target, messages.TOO_LARGE, sent_at)
                    counts["answered"] += 1
                    continue
                # Counted when an edge first leases it, like the edges always did.
                queue_api.enqueue(cur, source, body, sent_at, job_id, targets)
                counts["queued"] += 1
                continue

            events.append(_message_event(parsed))
            after.append((user_key, {"user": user_key, "type": parsed["type"], "job_id": job_id}))
            if parsed["type"] != "text":
                # Stickers, images, reactions...: counted, otherwise left alone.
                counts["ignored"] += 1
                continue
            for target in targets:
                outbox.enqueue_text(cur, target, _answer(text), sent_at)
            counts["answered"] += 1

        queue_api.count_dropped(cur, counts["stale"])
        if counts["unlinked"]:
            cur.execute("UPDATE totals SET dropped_unlinked = dropped_unlinked + %s WHERE id = 1;",
                        (counts["unlinked"],))
        minutes = stats.write_events(cur, HUB, events)
        conn.commit()
        stats.cache_messages(cur, HUB, minutes)
    for user, props in after:
        analytics.capture(user, "message-received", props, HUB)
    return counts


def _message_event(parsed):
    return {"kind": "message", "ts": time.time(), "message_type": parsed["type"],
            "user_hash": stats.user_hash(parsed["user_key"])}


def _answer(text):
    if text == "/status":
        return messages.status_text(stats.compute_stats(queue_api.queue_depth()))
    if text == "/detailed-status":
        return messages.detailed_status_text(stats.compute_stats(queue_api.queue_depth()))
    return messages.ONLY_RECORDINGS
