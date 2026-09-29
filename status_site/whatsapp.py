"""WhatsApp channel: webhook verification and parsing, sending, and media download.

Everything that knows the WhatsApp Cloud API lives here, so the queue, outbox and
routing deal only in channel-neutral messages and targets.
"""

import hashlib
import hmac
import json
import logging
import os
import time

import httpx
import requests

log = logging.getLogger(__name__)

META_APP_SECRET = os.environ.get("META_APP_SECRET", "")
META_VERIFY_TOKEN = os.environ.get("META_VERIFY_TOKEN", "")
API_TOKEN = os.environ.get("WHATSAPP_API_TOKEN", "")
PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "")
GRAPH_URL = os.environ.get("WHATSAPP_GRAPH_URL", "https://graph.facebook.com/v22.0").rstrip("/")

# WhatsApp's limit is 4096; keep some distance from it.
MAX_TEXT_LENGTH = 4000
REQUEST_TIMEOUT = (10, 30)
MEDIA_TIMEOUT = httpx.Timeout(120.0, connect=10.0)

# Cloud API errors worth retrying even when they arrive as HTTP 400: rate limits and
# transient server-side failures.
RETRYABLE_ERROR_CODES = {4, 80007, 130429, 131000, 131016, 131048, 131056, 133004}


class SendError(Exception):
    def __init__(self, message, retryable, retry_after=None):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


# --- webhooks

def verify_registration(args):
    if args.get("hub.mode") != "subscribe":
        return None
    token = args.get("hub.verify_token") or ""
    if not META_VERIFY_TOKEN or not hmac.compare_digest(token, META_VERIFY_TOKEN):
        return None
    return args.get("hub.challenge")


def verify_payload(raw, headers):
    received = headers.get("X-Hub-Signature-256", "")
    if not META_APP_SECRET or not received.startswith("sha256="):
        return False
    expected = hmac.new(META_APP_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(received[len("sha256="):], expected)


def split(payload):
    """Yield (message_id, sent_at, body) per message. Meta may batch several messages
    into one delivery; each becomes its own single-message webhook body, which is what
    the queue stores. Status updates (sent/delivered/read receipts for our own replies)
    carry no message and are skipped."""
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


def parse(body):
    """The parts of a single-message webhook body the hub acts on."""
    if isinstance(body, str):
        body = json.loads(body)
    message = body["entry"][0]["changes"][0]["value"]["messages"][0]
    mtype = message.get("type")
    media = None
    if mtype in ("audio", "document"):
        m = message.get(mtype) or {}
        media = {"id": m.get("id"), "mime_type": m.get("mime_type")}
    return {
        "sender": message.get("from"),
        "message_id": message.get("id"),
        "type": mtype,
        "kind": mtype if media else None,  # what the edge must do: audio, or document to convert
        "text": (message.get("text") or {}).get("body", "").strip() if mtype == "text" else None,
        "media": media,
    }


def target(parsed):
    """Where replies to this message go by default: back to the sender, quoting it."""
    return {"channel": "whatsapp", "address": parsed["sender"], "quote": parsed["message_id"]}


# --- sending

def _post(data):
    try:
        response = requests.post(
            f"{GRAPH_URL}/{PHONE_NUMBER_ID}/messages",
            headers={"Authorization": f"Bearer {API_TOKEN}"},
            json=data,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        raise SendError(f"network error: {e}", retryable=True)
    if response.ok:
        return response.json()
    code = None
    try:
        code = response.json().get("error", {}).get("code")
    except ValueError:
        pass
    retryable = (response.status_code == 429 or response.status_code >= 500
                 or code in RETRYABLE_ERROR_CODES)
    retry_after = response.headers.get("Retry-After")
    raise SendError(
        f"HTTP {response.status_code} code={code}: {response.text[:300]}",
        retryable=retryable,
        retry_after=float(retry_after) if retry_after and retry_after.isdigit() else None,
    )


def send_text(address, text, quote=None):
    data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": address,
        "type": "text",
        "text": {"body": text},
    }
    if quote:
        data["context"] = {"message_id": quote}
    return _post(data)


def send_receipt(address, message_id, typing=False):
    """Blue ticks; with typing=True also shows the typing indicator. Status updates,
    not messages, so WhatsApp does not bill for them."""
    data = {"messaging_product": "whatsapp", "status": "read", "message_id": message_id}
    if typing:
        data["typing_indicator"] = {"type": "text"}
    return _post(data)


# --- media

_http = None


def _client():
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=MEDIA_TIMEOUT)
    return _http


async def open_media(media):
    """Resolve a media id and open the download as a stream. Returns (response,
    content_type); the caller streams response.aiter_bytes() and must aclose() it."""
    auth = {"Authorization": f"Bearer {API_TOKEN}"}
    info = await _client().get(f"{GRAPH_URL}/{media['id']}", headers=auth)
    info.raise_for_status()
    info = info.json()
    response = await _client().send(
        _client().build_request("GET", info["url"], headers=auth), stream=True
    )
    if response.status_code >= 400:
        await response.aclose()
        response.raise_for_status()
    return response, info.get("mime_type") or media.get("mime_type") or "application/octet-stream"


async def close():
    global _http
    if _http is not None:
        await _http.aclose()
        _http = None
