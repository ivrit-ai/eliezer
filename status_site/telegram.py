"""Telegram channel: webhook verification and parsing, sending, media download, and
registering the bot's webhook and commands.

The only setting is TELEGRAM_BOT_TOKEN; the webhook secret is derived from it and the
bot's username is read from the Bot API at startup.
"""

import hashlib
import hmac
import json
import logging
import os
import re
import time

import httpx
import requests

from channel import SendError

log = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
API_URL = os.environ.get("TELEGRAM_API_URL", "https://api.telegram.org").rstrip("/")
# Telegram echoes this in a header on every webhook call. Derived from the token, so it
# needs no configuration and changes whenever the token is rotated.
WEBHOOK_SECRET = hashlib.sha256(f"eliezer-webhook:{BOT_TOKEN}".encode()).hexdigest() if BOT_TOKEN else ""

# Telegram's limit is 4096; keep some distance from it.
MAX_TEXT_LENGTH = 4000
# The Bot API will not hand bots files larger than this.
MAX_FILE_BYTES = 20 * 1024 * 1024
REQUEST_TIMEOUT = (10, 30)
MEDIA_TIMEOUT = httpx.Timeout(120.0, connect=10.0)

COMMANDS = [
    {"command": "link", "description": "קישור לוואטסאפ"},
    {"command": "unlink", "description": "ביטול הקישור לוואטסאפ"},
    {"command": "status", "description": "סטטוס השירות"},
    {"command": "transcribe", "description": "בתשובה להקלטה: תמלול שלה"},
]

_username = None


def username():
    """The bot's @username, for t.me links; None until startup has looked it up."""
    return _username


# --- webhooks

def verify_registration(args):
    # Telegram has no subscription handshake: the webhook is set by calling setWebhook.
    return None


def verify_payload(raw, headers):
    received = headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    return bool(WEBHOOK_SECRET) and hmac.compare_digest(received, WEBHOOK_SECRET)


# Groups the bot is in: it transcribes audio posted there, replying to it, and otherwise
# stays quiet. Automatic only with the bot's group privacy mode off in BotFather; with it
# on, Telegram delivers only commands, so "/transcribe" as a reply to audio is the way in.
GROUP_TYPES = ("group", "supergroup")
TRANSCRIBE = re.compile(r"^/transcribe(@\w+)?$", re.IGNORECASE)

MEDIA_FIELDS = (("voice", "audio"), ("audio", "audio"),
                ("video_note", "document"), ("video", "document"), ("document", "document"))


def split(update):
    """Yield (update_id, sent_at, body) for the one update in a delivery: a message in a
    private chat or group, or a private chat's membership change (the user blocked or
    restarted the bot). Being added to or removed from a group needs no action."""
    message = update.get("message")
    member = update.get("my_chat_member")
    if message:
        if (message.get("chat") or {}).get("type") not in ("private",) + GROUP_TYPES:
            return
        event = message
    elif member and (member.get("chat") or {}).get("type") == "private":
        event = member
    else:
        return
    yield str(update.get("update_id", "")), float(event.get("date") or time.time()), json.dumps(update)


def _media_of(message):
    for field, kind in MEDIA_FIELDS:
        if field in message:
            m = message[field]
            return {"id": m.get("file_id"), "mime_type": m.get("mime_type"), "size": m.get("file_size")}, kind, field
    return None, None, None


def parse(body):
    """The parts of an update the hub acts on."""
    if isinstance(body, str):
        body = json.loads(body)
    if "my_chat_member" in body:
        member = body["my_chat_member"]
        chat_id = str(member["chat"]["id"])
        return {"sender": chat_id, "user_key": f"tg:{chat_id}", "message_id": None,
                "type": "membership", "kind": None, "text": None, "media": None,
                "member_status": (member.get("new_chat_member") or {}).get("status")}
    message = body["message"]
    chat_id = str(message["chat"]["id"])
    # In a group, replies go to the group but limits and statistics follow the person.
    person = str((message.get("from") or {}).get("id") or chat_id)
    media, kind, mtype = _media_of(message)
    quote = message.get("message_id")
    text = message["text"].strip() if "text" in message else None
    original = message.get("reply_to_message")
    command = TRANSCRIBE.match(text) if text is not None else None
    # "/transcribe@SomeOtherBot" is another bot's business.
    if command and command.group(1) and _username and command.group(1)[1:].lower() != _username.lower():
        command = None
    if command and original:
        # "/transcribe" in reply to audio: transcribe that audio, answering it.
        media, kind, mtype = _media_of(original)
        if media:
            quote, text = original.get("message_id"), None
    if media is None:
        mtype = "text" if text is not None else "other"
    return {
        "sender": chat_id,
        "user_key": f"tg:{person}",
        "message_id": quote,
        "type": mtype,
        "kind": kind,
        "text": text if mtype == "text" else None,
        "media": media,
        "group": message["chat"].get("type") in GROUP_TYPES,
    }


def target(parsed):
    return {"channel": "telegram", "address": parsed["sender"], "quote": parsed["message_id"]}


# --- sending

def _call(method, payload):
    try:
        response = requests.post(f"{API_URL}/bot{BOT_TOKEN}/{method}", json=payload, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        raise SendError(f"network error: {e}", retryable=True)
    try:
        result = response.json()
    except ValueError:
        result = {}
    if response.ok and result.get("ok"):
        return result.get("result")
    description = result.get("description", response.text[:300])
    retry_after = (result.get("parameters") or {}).get("retry_after")
    # 403: blocked by the user or account deleted; 400 "chat not found": never started.
    gone = response.status_code == 403 or "chat not found" in description.lower()
    raise SendError(
        f"{method}: HTTP {response.status_code}: {description}",
        retryable=response.status_code == 429 or response.status_code >= 500,
        retry_after=float(retry_after) if retry_after else None,
        gone=gone,
    )


def send_text(address, text, quote=None, buttons=None):
    payload = {"chat_id": address, "text": text, "link_preview_options": {"is_disabled": True}}
    if quote:
        payload["reply_parameters"] = {"message_id": quote, "allow_sending_without_reply": True}
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": [[{"text": b["text"], "url": b["url"]}] for b in buttons]}
    return _call("sendMessage", payload)


def send_receipt(address, message_id, typing=False):
    # Telegram shows no read receipts to users; only the typing indicator is worth sending.
    if typing:
        return _call("sendChatAction", {"chat_id": address, "action": "typing"})
    return None


# --- media

_http = None


def _client():
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=MEDIA_TIMEOUT)
    return _http


async def open_media(media):
    """Resolve a file_id and open the download as a stream. Returns (response,
    content_type); the caller streams response.aiter_bytes() and must aclose() it."""
    info = await _client().post(f"{API_URL}/bot{BOT_TOKEN}/getFile", json={"file_id": media["id"]})
    info.raise_for_status()
    path = info.json()["result"]["file_path"]
    response = await _client().send(
        _client().build_request("GET", f"{API_URL}/file/bot{BOT_TOKEN}/{path}"), stream=True
    )
    if response.status_code >= 400:
        await response.aclose()
        response.raise_for_status()
    return response, media.get("mime_type") or "application/octet-stream"


async def close():
    global _http
    if _http is not None:
        await _http.aclose()
        _http = None


# --- registration

def setup(webhook_url):
    """Point the bot at this site and publish its command menu. Idempotent; run at
    every startup, so a rotated token or a new URL takes effect on the next deploy."""
    global _username
    if not BOT_TOKEN:
        return
    me = _call("getMe", {})
    _username = me.get("username")
    _call("setWebhook", {
        "url": webhook_url,
        "secret_token": WEBHOOK_SECRET,
        "allowed_updates": ["message", "my_chat_member"],
    })
    _call("setMyCommands", {"commands": COMMANDS})
    log.debug("telegram: @%s receives updates at %s", _username, webhook_url)
