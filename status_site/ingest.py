"""Platform webhooks: verify each delivery, split it into messages, and queue them."""

import hashlib
import hmac
import json
import logging
import os
import time

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse

import queue_api

log = logging.getLogger(__name__)

META_APP_SECRET = os.environ.get("META_APP_SECRET", "")
META_VERIFY_TOKEN = os.environ.get("META_VERIFY_TOKEN", "")

webhooks = APIRouter()


class WhatsAppWebhook:
    def verify_registration(self, args):
        if args.get("hub.mode") != "subscribe":
            return None
        token = args.get("hub.verify_token") or ""
        if not META_VERIFY_TOKEN or not hmac.compare_digest(token, META_VERIFY_TOKEN):
            return None
        return args.get("hub.challenge")

    def verify_payload(self, raw, headers):
        received = headers.get("X-Hub-Signature-256", "")
        if not META_APP_SECRET or not received.startswith("sha256="):
            return False
        expected = hmac.new(META_APP_SECRET.encode(), raw, hashlib.sha256).hexdigest()
        return hmac.compare_digest(received[len("sha256="):], expected)

    def split(self, payload):
        """Yield (message_id, sent_at, body) per message. Meta may batch several messages
        into one delivery; each becomes its own queue row, shaped as a single-message
        webhook so the edge reads it exactly as before. Status updates (sent/delivered/
        read receipts for our own replies) carry no message and are skipped."""
        for entry in payload.get("entry") or []:
            for change in entry.get("changes") or []:
                value = change.get("value") or {}
                context = {k: v for k, v in value.items() if k not in ("messages", "statuses")}
                for message in value.get("messages") or []:
                    body = {
                        "object": payload.get("object"),
                        "entry": [{
                            "id": entry.get("id"),
                            "changes": [{
                                "field": change.get("field"),
                                "value": {**context, "messages": [message]},
                            }],
                        }],
                    }
                    try:
                        sent_at = float(message["timestamp"])
                    except (KeyError, TypeError, ValueError):
                        sent_at = time.time()
                    yield message.get("id") or "", sent_at, json.dumps(body)


SOURCES = {"whatsapp": WhatsAppWebhook()}


@webhooks.get("/webhook/{source}")
def webhook_verify(source: str, request: Request):
    handler = SOURCES.get(source)
    if handler is None:
        return JSONResponse({"error": "unknown source"}, status_code=404)
    challenge = handler.verify_registration(request.query_params)
    if challenge is None:
        log.warning("webhook %s registration rejected: bad mode or verify token", source)
        return JSONResponse({"error": "forbidden"}, status_code=403)
    log.debug("webhook %s registration verified", source)
    return PlainTextResponse(challenge)


@webhooks.post("/webhook/{source}")
async def webhook_receive(source: str, request: Request):
    handler = SOURCES.get(source)
    if handler is None:
        return JSONResponse({"error": "unknown source"}, status_code=404)
    # The signature covers the exact bytes Meta sent, so verify before parsing.
    raw = await request.body()
    if not handler.verify_payload(raw, request.headers):
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

    items = list(handler.split(payload))
    # A DB failure raises and returns 500, so the platform retries; the dedup table
    # makes that retry safe.
    queued, duplicates, stale = await run_in_threadpool(queue_api.ingest, source, items)
    log.debug(
        "webhook %s: %s message(s) -> queued %s, duplicate %s, stale %s",
        source, len(items), queued, duplicates, stale,
    )
    return {"ok": True}
