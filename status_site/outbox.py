"""Everything the service sends - replies, notices, read receipts - goes through this
table and is delivered by a few background threads.

A reply is committed here in the same transaction that closes the job it answers, so a
finished transcription cannot be lost between the edge and the user: if a send fails it
is retried, and if the process dies mid-reply the next sender resumes at the next part.
The one exception is an explicit drop: rows past the max age expire like queue messages.
"""

import json
import logging
import os
import threading
import time

import db
import identity
import notifier
import telegram
import whatsapp
from channel import SendError

log = logging.getLogger(__name__)

# Channel adapters (see channel.py for what each provides).
CHANNELS = {"whatsapp": whatsapp, "telegram": telegram, "notifier": notifier}
# Messages delivered per channel, for the dashboard. WhatsApp's is the one it bills.
SENT_COUNTERS = {"whatsapp": "wa_sent", "telegram": "tg_sent", "notifier": "nt_sent"}

SENDER_THREADS = int(os.environ.get("OUTBOX_SENDERS", "4"))
# A claimed row is invisible to other senders for this long; a sender that dies mid-send
# releases it by timing out. Longest send is a multi-part reply, well under this.
CLAIM_SECONDS = 120
MAX_BACKOFF_SECONDS = 300
PART_INTERVAL_SECONDS = 0.1  # between parts of a long reply, as before

_wakeup = threading.Event()


def init_outbox_db(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS outbox (
          id              BIGSERIAL PRIMARY KEY,
          channel         TEXT NOT NULL,
          address         TEXT NOT NULL,
          action          TEXT NOT NULL,
          payload         JSONB NOT NULL,
          parts_sent      INT NOT NULL DEFAULT 0,
          sent_at         TIMESTAMPTZ NOT NULL,
          source_handle   TEXT UNIQUE,
          attempts        INT NOT NULL DEFAULT 0,
          next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          claimed_until   TIMESTAMPTZ,
          created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS outbox_due_idx ON outbox (next_attempt_at, id);")


def split_text(channel, text, quote):
    """One reply as sendable parts: chunks within the channel's length limit, numbered
    when there is more than one; only the first quotes the message it answers."""
    limit = CHANNELS[channel].MAX_TEXT_LENGTH
    if len(text) <= limit:
        return [{"text": text, "quote": quote}]
    chunks = [text[i:i + limit] for i in range(0, len(text), limit)]
    return [
        {"text": f"[{i + 1}/{len(chunks)}]\n{chunk}", "quote": quote if i == 0 else None}
        for i, chunk in enumerate(chunks)
    ]


def enqueue_text(cur, target, text, sent_at, extra=(), source_handle=None, buttons=None,
                 transcript=False, meta=None):
    """Queue a reply to target, in the caller's transaction. extra are further messages
    sent after it, unquoted (the nudge); buttons are links shown under the reply, where
    the channel supports them; meta describes it for channels that show more than text
    (the Notifier: kind, subtitle). sent_at is when the user sent what this answers.

    WhatsApp bills every message, so it gets transcripts (transcript=True) and nothing
    else: notices, refusals and command answers are dropped there, and still go out on
    every other channel."""
    if target["channel"] == "whatsapp" and not transcript:
        return
    if target["channel"] == "notifier":
        if not notifier.enabled():
            return
        # Each extra would be a notification of its own.
        extra = ()
    parts = split_text(target["channel"], text, target.get("quote"))
    if buttons:
        parts[-1]["buttons"] = buttons
    if meta:
        for part in parts:
            part["meta"] = meta
    parts += [{"text": e, "quote": None} for e in extra if e]
    cur.execute(
        """
        INSERT INTO outbox (channel, address, action, payload, sent_at, source_handle)
        VALUES (%s, %s, 'text', %s, to_timestamp(%s), %s);
        """,
        (target["channel"], target["address"], json.dumps({"parts": parts}), sent_at, source_handle),
    )
    _wakeup.set()


def enqueue_link(cur, code, owner, sent_at):
    """Queue redeeming a Notifier link code that owner (a WhatsApp number or Telegram
    chat) sent us. A network call, so it goes through the senders and their retries
    rather than holding up the webhook."""
    cur.execute(
        """
        INSERT INTO outbox (channel, address, action, payload, sent_at)
        VALUES ('notifier', %s, 'link', %s, to_timestamp(%s));
        """,
        (f"{owner['channel']}:{owner['address']}", json.dumps({"code": code, "owner": owner}), sent_at),
    )
    _wakeup.set()


def enqueue_receipt(cur, target, typing, sent_at):
    """Queue a read receipt (with typing=True, also a typing indicator) for the message
    target quotes. WhatsApp receipts need that message; a Telegram typing indicator
    does not."""
    if target["channel"] == "whatsapp" and not target.get("quote"):
        return
    if target["channel"] != "whatsapp" and not typing:
        return
    cur.execute(
        """
        INSERT INTO outbox (channel, address, action, payload, sent_at)
        VALUES (%s, %s, %s, %s, to_timestamp(%s));
        """,
        (target["channel"], target["address"], "typing" if typing else "read",
         json.dumps({"message_id": target["quote"]}), sent_at),
    )
    _wakeup.set()


def sweep_expired(cur, max_age_seconds):
    """Drop replies still unsent past the max age. Returns how many."""
    cur.execute(
        "DELETE FROM outbox WHERE sent_at < now() - make_interval(secs => %s::double precision);",
        (max_age_seconds,),
    )
    return cur.rowcount


def _claim():
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH picked AS MATERIALIZED (
              SELECT id FROM outbox
              WHERE next_attempt_at <= now()
                AND (claimed_until IS NULL OR claimed_until < now())
              ORDER BY id
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            UPDATE outbox o SET
              claimed_until = now() + make_interval(secs => %s::double precision),
              attempts = attempts + 1
            FROM picked p
            WHERE o.id = p.id
            RETURNING o.id, o.channel, o.address, o.action, o.payload, o.parts_sent, o.attempts;
            """,
            (CLAIM_SECONDS,),
        )
        row = cur.fetchone()
        conn.commit()
    return row


def _execute(sql, params):
    with db.pool().connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()


def _deliver(row):
    adapter = CHANNELS[row["channel"]]
    if row["action"] == "link":
        _redeem(row)
        return
    if row["action"] != "text":
        adapter.send_receipt(row["address"], row["payload"]["message_id"], row["action"] == "typing")
        return
    parts = row["payload"]["parts"]
    for i in range(row["parts_sent"], len(parts)):
        if i > row["parts_sent"]:
            time.sleep(PART_INTERVAL_SECONDS)
        part = parts[i]
        if row["channel"] == "notifier":
            # The row id and part number make a retry of this very part land once.
            adapter.send_text(row["address"], part["text"], part.get("quote"), part.get("buttons"),
                              meta=part.get("meta"), dedupe_key=f"outbox:{row['id']}:{i}")
        else:
            adapter.send_text(row["address"], part["text"], part.get("quote"), part.get("buttons"))
        # Record each part as it goes, so a retry after a failure resumes here.
        _execute("UPDATE outbox SET parts_sent = %s WHERE id = %s;", (i + 1, row["id"]))
        counter = SENT_COUNTERS.get(row["channel"])
        if counter:
            _execute(f"UPDATE totals SET {counter} = {counter} + 1 WHERE id = 1;", ())


def _redeem(row):
    """Trade a link code for a subscription, then bind it. Transient failures raise and
    are retried like any send; a code that will never work ends here."""
    import control  # control enqueues into the outbox, so it cannot be imported at the top

    owner = row["payload"]["owner"]
    label = control.notifier_label(owner)
    status, subscription_id = notifier.redeem(
        row["payload"]["code"], notifier.subject(owner["channel"], owner["address"]), label)
    with db.pool().connection() as conn, conn.cursor() as cur:
        control.notifier_redeemed(cur, owner, status, subscription_id, time.time())
        conn.commit()


def _unbind(channel, address):
    with db.pool().connection() as conn, conn.cursor() as cur:
        identity.detach(cur, channel, address)
        conn.commit()


def _send_one(row):
    try:
        _deliver(row)
    except SendError as e:
        if e.gone:
            # Blocked, deleted, never started: this identity is dead. Unbind it, so
            # replies for its user fall back to wherever else they may go.
            log.debug("outbox %s: %s:%s is unreachable (%s); unbinding it",
                     row["id"], row["channel"], row["address"], e)
            _unbind(row["channel"], row["address"])
            _execute("DELETE FROM outbox WHERE channel = %s AND address = %s;",
                     (row["channel"], row["address"]))
            return
        if row["action"] in ("text", "link") and e.retryable:
            delay = e.retry_after or min(5 * 2 ** (row["attempts"] - 1), MAX_BACKOFF_SECONDS)
            log.debug("outbox %s to %s:%s failed (%s); retry in %ss",
                      row["id"], row["channel"], row["address"], e, delay)
            _execute(
                "UPDATE outbox SET claimed_until = NULL, "
                "next_attempt_at = now() + make_interval(secs => %s::double precision) WHERE id = %s;",
                (delay, row["id"]),
            )
            return
        # A receipt is only worth sending now; a permanent error will not go away.
        level = logging.WARNING if row["action"] == "text" else logging.DEBUG
        log.log(level, "outbox %s %s to %s:%s dropped: %s",
                row["id"], row["action"], row["channel"], row["address"], e)
    except Exception:
        log.exception("outbox %s: unexpected send failure; retrying later", row["id"])
        _execute(
            "UPDATE outbox SET claimed_until = NULL, "
            "next_attempt_at = now() + interval '30 seconds' WHERE id = %s;",
            (row["id"],),
        )
        return
    _execute("DELETE FROM outbox WHERE id = %s;", (row["id"],))


def _run(stop):
    while not stop.is_set():
        try:
            row = _claim()
        except Exception:
            log.exception("outbox claim failed")
            stop.wait(5)
            continue
        if row is None:
            # Enqueuers set this; the timeout covers rows whose backoff has come due.
            _wakeup.wait(1.0)
            _wakeup.clear()
            continue
        _send_one(row)


def start_senders():
    stop = threading.Event()
    for i in range(SENDER_THREADS):
        threading.Thread(target=_run, args=(stop,), name=f"OutboxSender-{i}", daemon=True).start()
    return stop
