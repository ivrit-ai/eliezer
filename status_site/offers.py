"""Telegram link-code offers: the bot's messages carrying a code and its button.

A code lasts 15 minutes, but the message stays in the chat for good, and people come
back days later and tap a button whose code is long gone. So once a code is used,
replaced by a newer one, or expired, its message is edited: the button goes, and the
code line gives way to a line saying what happened. Only offers sent since this was
added are known; older ones are left as they are.
"""

import identity
import messages
import outbox

STATUS = {
    "used": messages.OFFER_USED,
    "replaced": messages.OFFER_REPLACED,
    "expired": messages.OFFER_EXPIRED,
}


def _retire(cur, rows, reason, sent_at):
    for row in rows:
        outbox.enqueue_edit(cur, {"channel": "telegram", "address": row["chat_id"]}, row["message_id"],
                            f"{row['base_text']}\n\n{STATUS[reason]}", sent_at)
        cur.execute("DELETE FROM link_offers WHERE chat_id = %s AND message_id = %s;",
                    (row["chat_id"], row["message_id"]))


def retire(cur, tokens, reason, sent_at):
    """Edit the offers carrying these tokens to say the code was used or replaced."""
    if not tokens:
        return
    cur.execute(
        "SELECT chat_id, message_id, base_text FROM link_offers WHERE token = ANY(%s) FOR UPDATE SKIP LOCKED;",
        (list(tokens),),
    )
    _retire(cur, cur.fetchall(), reason, sent_at)


def retire_expired(cur, sent_at):
    """Offers whose code has expired, or is gone without having been retired (deleted
    by a newer code before its own offer was even recorded): say it expired."""
    cur.execute(
        """
        SELECT o.chat_id, o.message_id, o.base_text
          FROM link_offers o
          LEFT JOIN link_tokens t ON t.token = o.token
         WHERE t.token IS NULL OR t.expires_at < now()
         LIMIT 500
           FOR UPDATE OF o SKIP LOCKED;
        """
    )
    _retire(cur, cur.fetchall(), "expired", sent_at)


record = identity.record_offer
