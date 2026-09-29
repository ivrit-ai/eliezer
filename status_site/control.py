"""The control plane: commands, linking and admin, answered by the hub itself.

Linking starts in Telegram and ends with WhatsApp numbers joining the Telegram chat's
user and stopping WhatsApp replies (the linked channel replaces WhatsApp): /link gives a
wa.me button whose prefilled text is "link <code>", and sending that from WhatsApp links
the number. There is no way to start from WhatsApp, where every reply costs. Codes are
single-use and expire after 15 minutes.
"""

import os
import re

import identity
import limits
import messages
import outbox
import queue_api
import stats
import whatsapp

ADMIN_CHAT_IDS = {c.strip() for c in os.environ.get("ADMIN_TG_CHAT_IDS", "").split(",") if c.strip()}

LINK_WITH_CODE = re.compile(r"^\s*link\s+([0-9a-z]{8})\s*$", re.IGNORECASE)


def _tg(chat_id, quote=None):
    return {"channel": "telegram", "address": str(chat_id), "quote": quote}


def _wa(number, quote=None):
    return {"channel": "whatsapp", "address": str(number), "quote": quote}


def _numbers(cur, user_id):
    return [i["address"] for i in identity.identities_of(cur, user_id) if i["channel"] == "whatsapp"]


def _chats(cur, user_id):
    return [i["address"] for i in identity.identities_of(cur, user_id) if i["channel"] == "telegram"]


def _link_whatsapp(cur, user_id, number, sent_at):
    """Move a WhatsApp number into user_id (a Telegram chat's user), and tell everyone
    concerned. The caller has checked the region."""
    previous = identity.user_of(cur, "whatsapp", number)
    previous_chats = _chats(cur, previous["id"]) if previous and previous["id"] != user_id else []
    identity.attach(cur, user_id, "whatsapp", number, deliver=False)
    masked = identity.mask("whatsapp", number)
    for chat in previous_chats:
        outbox.enqueue_text(cur, _tg(chat), messages.tg_moved_away(masked), sent_at)
    # Confirmed on Telegram only: a WhatsApp message would cost, and the user is looking
    # at Telegram anyway.
    for chat in _chats(cur, user_id):
        outbox.enqueue_text(cur, _tg(chat), messages.tg_linked(masked), sent_at)


# --- WhatsApp

def is_whatsapp_link(text):
    return bool(text and LINK_WITH_CODE.match(text))


def handle_whatsapp_link(cur, parsed, sent_at):
    number = parsed["sender"]
    user = identity.user_of(cur, "whatsapp", number)
    can_reply = identity.wa_permitted(cur, user)
    here = _wa(number, parsed["message_id"])
    status, user_id = identity.consume_token(cur, LINK_WITH_CODE.match(parsed["text"]).group(1))
    if status == "ok" and not limits.is_allowed_region(number):
        if can_reply:
            outbox.enqueue_text(cur, here, messages.REJECTED_REGION, sent_at)
        return
    if status == "ok":
        _link_whatsapp(cur, user_id, number, sent_at)
    elif status == "expired":
        for chat in _chats(cur, user_id):
            outbox.enqueue_text(cur, _tg(chat), messages.TG_LINK_EXPIRED, sent_at)
    elif can_reply:
        outbox.enqueue_text(cur, here, messages.WA_LINK_UNKNOWN, sent_at)


# --- Telegram

def _offer_link(cur, chat, user_id, sent_at, quote=None):
    token = identity.mint_token(cur, user_id)
    numbers = _numbers(cur, user_id)
    text = (messages.tg_already_linked([identity.mask("whatsapp", n) for n in numbers])
            if numbers else messages.TG_WELCOME)
    buttons = None
    if whatsapp.display_number():
        url = f"https://wa.me/{whatsapp.display_number()}?text=link%20{token}"
        buttons = [{"text": messages.LINK_BUTTON, "url": url}]
    outbox.enqueue_text(cur, _tg(chat, quote), text + messages.tg_link_code_hint(token), sent_at,
                        buttons=buttons)


def handle_telegram_text(cur, parsed, sent_at):
    chat = parsed["sender"]
    text = parsed["text"] or ""
    command, _, arg = text.partition(" ")
    command = command.split("@", 1)[0].lower()  # "/link@EliezerBot" in some clients
    arg = arg.strip()
    here = _tg(chat, parsed["message_id"])

    if command in ADMIN_COMMANDS and chat in ADMIN_CHAT_IDS:
        outbox.enqueue_text(cur, here, ADMIN_COMMANDS[command](cur, arg), sent_at)
        return
    if command == "/unlink":
        user = identity.user_of(cur, "telegram", chat)
        numbers = _numbers(cur, user["id"]) if user else []
        for number in numbers:
            identity.detach(cur, "whatsapp", number)
        reply = (messages.tg_unlinked([identity.mask("whatsapp", n) for n in numbers])
                 if numbers else messages.TG_NOTHING_TO_UNLINK)
        outbox.enqueue_text(cur, here, reply, sent_at)
        return
    if command == "/status":
        outbox.enqueue_text(cur, here, messages.status_text(stats.compute_stats(queue_api.queue_depth())), sent_at)
        return
    if command == "/detailed-status":
        outbox.enqueue_text(cur, here, messages.detailed_status_text(stats.compute_stats(queue_api.queue_depth())), sent_at)
        return
    if command == "/whoami":
        outbox.enqueue_text(cur, here, f"chat id: {chat}", sent_at)
        return
    if command == "/help":
        outbox.enqueue_text(cur, here, messages.TG_HELP, sent_at)
        return

    user = identity.user_of(cur, "telegram", chat)
    if command in ("/start", "/link") or not (user and _numbers(cur, user["id"])):
        user_id = identity.ensure_user(cur, "telegram", chat, deliver=True)
        _offer_link(cur, chat, user_id, sent_at, quote=None if command.startswith("/") else parsed["message_id"])
        return
    outbox.enqueue_text(cur, here, messages.TG_HELP, sent_at)


def handle_membership(cur, parsed):
    """The user blocked the bot (or deleted the chat): unbind it."""
    if parsed.get("member_status") in ("kicked", "left"):
        identity.detach(cur, "telegram", parsed["sender"])


# --- admin (Telegram chats listed in ADMIN_TG_CHAT_IDS)

def _number(arg):
    return "".join(c for c in arg if c.isdigit())


def _wa_allow(cur, arg):
    number, _, note = arg.partition(" ")
    number = _number(number)
    if not number:
        return "usage: /wa_allow <number> [note]"
    identity.set_wa_allowed(cur, number, True, note.strip() or None)
    return f"{identity.mask('whatsapp', number)} keeps WhatsApp replies whatever the policy."


def _wa_revoke(cur, arg):
    number = _number(arg)
    if not number:
        return "usage: /wa_revoke <number>"
    if not identity.set_wa_allowed(cur, number, False):
        return f"{identity.mask('whatsapp', number)} was not allowlisted."
    return f"{identity.mask('whatsapp', number)} now follows the policy ({identity.policy(cur)})."


def _wa_policy(cur, arg):
    arg = arg.lower()
    if arg:
        if arg not in identity.POLICIES:
            return "usage: /wa_policy [reply|drop]"
        identity.set_policy(cur, arg)
    current = identity.policy(cur)
    meaning = ("unlinked WhatsApp users get replies on WhatsApp" if current == "reply"
               else "unlinked WhatsApp users are ignored, unless allowlisted")
    return f"policy: {current} ({meaning})"


def _whois(cur, arg):
    arg = arg.strip()
    if not arg:
        return "usage: /whois <number|chat id>"
    for channel, address in (("whatsapp", _number(arg)), ("telegram", arg)):
        info = identity.describe(cur, channel, address)
        if info:
            lines = [f"user {info['user_id']}" + (" (WhatsApp allowlisted)" if info["wa_replies_allowed"] else "")
                     + (f": {info['note']}" if info["note"] else "")]
            lines += [f"  {i['channel']} {i['address']}" + ("" if i["deliver"] else " (no delivery)")
                      for i in info["identities"]]
            return "\n".join(lines)
    return "not linked or allowlisted: an anonymous sender"


def _edges(cur, arg):
    s = stats.compute_stats(queue_api.queue_depth())
    lines = [f"queue depth {s['queue_depth']}, linked users {identity.linked_users(cur)}"]
    lines += [f"{i['instance_id']}: up {int(i['uptime_seconds'] // 3600)}h, seen {int(i['age_seconds'])}s ago"
              for i in s["instances"]]
    return "\n".join(lines)


ADMIN_COMMANDS = {
    "/wa_allow": _wa_allow,
    "/wa_revoke": _wa_revoke,
    "/wa_policy": _wa_policy,
    "/whois": _whois,
    "/edges": _edges,
}
