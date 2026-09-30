"""Notifier channel: transcripts as push notifications, through a Notifier instance
where Eliezer is registered as a source.

Unlike WhatsApp and Telegram this is an output only - nobody sends Eliezer anything
through it. A user links it from the Notifier app: the app shows a code, the user sends
"link <code>" to Eliezer (on WhatsApp or Telegram), and Eliezer redeems the code here for
a subscription id, which is the identity's address from then on. Nothing sent this way
is billed by anyone.

Settings: NOTIFIER_URL (the instance) and NOTIFIER_SOURCE_KEY (Eliezer's source key,
from the instance's admin page). Without both, the channel is off: notifier codes are
not recognized and nothing is sent.
"""

import hashlib
import logging
import os

import requests

from channel import SendError

log = logging.getLogger(__name__)

URL = os.environ.get("NOTIFIER_URL", "").rstrip("/")
KEY = os.environ.get("NOTIFIER_SOURCE_KEY", "")
# Where people open the app; the same instance unless it sits behind another name.
PUBLIC_URL = os.environ.get("NOTIFIER_PUBLIC_URL", URL).rstrip("/")
SALT = os.environ.get("STATUS_USER_SALT", "")

# The whole transcript in one notification: the app shows it in full, and a long voice
# note split into numbered parts would arrive as several notifications.
MAX_TEXT_LENGTH = 16000
REQUEST_TIMEOUT = (10, 30)


def enabled():
    return bool(URL and KEY)


def subject(channel, address):
    """Who redeemed a code, as the Notifier sees it: stable, so relinking from the same
    number finds the same subscription, and salted, so the number never leaves here."""
    return hashlib.sha256(f"{SALT}:{channel}:{address}".encode()).hexdigest()


def _post(path, payload):
    try:
        return requests.post(f"{URL}{path}", json=payload, timeout=REQUEST_TIMEOUT,
                             headers={"Authorization": f"Bearer {KEY}"})
    except requests.RequestException as e:
        raise SendError(f"network error: {e}", retryable=True)


def _retry_after(response):
    try:
        return float(response.headers.get("Retry-After", ""))
    except ValueError:
        return None


def _error(response, what):
    try:
        detail = response.json().get("error", "")
    except ValueError:
        detail = response.text[:200]
    return SendError(
        f"{what}: HTTP {response.status_code}: {detail}",
        retryable=response.status_code == 429 or response.status_code >= 500,
        retry_after=_retry_after(response),
        # The user unlinked in the app, or deleted their account.
        gone=response.status_code == 410,
    )


def send_text(address, text, quote=None, buttons=None, meta=None, dedupe_key=None):
    """quote and buttons have no meaning here; meta is {kind, subtitle, lang}, and
    dedupe_key makes a retried send land once."""
    payload = {"subscription_id": address, "body": text, "lang": "he"}
    for field in ("kind", "subtitle", "lang", "title"):
        if meta and meta.get(field):
            payload[field] = meta[field]
    if dedupe_key:
        payload["dedupe_key"] = dedupe_key
    response = _post("/api/source/v1/messages", payload)
    if response.status_code != 202:
        raise _error(response, "messages")


def send_receipt(address, message_id, typing=False):
    return None


# What a redeem answer means for the user; anything else is an error to retry or log.
REDEEM_OUTCOMES = {404: "unknown", 409: "rejected", 410: "expired"}


def redeem(code, subject_id, label):
    """('ok', subscription_id), or (outcome, None) for a code that will never work.
    Raises SendError for failures worth retrying."""
    response = _post("/api/source/v1/links", {"code": code, "subject": subject_id, "label": label})
    if response.status_code in (200, 201):
        return "ok", response.json()["subscription_id"]
    if response.status_code in REDEEM_OUTCOMES:
        return REDEEM_OUTCOMES[response.status_code], None
    raise _error(response, "links")
