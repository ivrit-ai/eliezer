"""The control plane: commands, linking and admin, answered by the hub itself.

Linking starts in Telegram and ends with WhatsApp numbers joining the Telegram chat's
user and stopping WhatsApp replies (the linked channel replaces WhatsApp): /link gives a
wa.me button whose prefilled text is "link <code>", and sending that from WhatsApp links
the number. There is no way to start from WhatsApp, where every reply costs. Codes are
single-use and expire after 15 minutes.

The Notifier app links the other way round: it shows the code, and the user sends
"link <code>" from WhatsApp (or Telegram). Its codes are ten characters to Telegram's
eight, which is how the two are told apart. The outcome shows in the app. Either way, a
link message from WhatsApp gets an emoji reaction (free): 👍 when it linked, 👎 when it
did not (unknown, expired or used code), so the sender is never left guessing.
"""

import os
import re

import identity
import limits
import messages
import notifier
import offers
import outbox
import queue_api
import stats
import whatsapp

ADMIN_CHAT_IDS = {c.strip() for c in os.environ.get("ADMIN_TG_CHAT_IDS", "").split(",") if c.strip()}

TELEGRAM_CODE = re.compile(r"^\s*link\s+([0-9a-z]{8})\s*$", re.IGNORECASE)
LINK_WITH_CODE = TELEGRAM_CODE  # backwards-compatibility alias
# "link ABCDE-FGHJK" as typed or prefilled, "/link ABCDE-FGHJK" in Telegram, and
# "link-ABCDEFGHJK" from a t.me/...?start= deep link.
NOTIFIER_CODE = re.compile(r"^\s*/?link[\s-]+([0-9a-z]{5})-?([0-9a-z]{5})\s*$", re.IGNORECASE)


def _tg(chat_id, quote=None):
    return {"channel": "telegram", "address": str(chat_id), "quote": quote}


def _numbers(cur, user_id):
    return [i["address"] for i in identity.identities_of(cur, user_id) if i["channel"] == "whatsapp"]


def _chats(cur, user_id):
    return [i["address"] for i in identity.identities_of(cur, user_id) if i["channel"] == "telegram"]


def _link_whatsapp_to_telegram(cur, user_id, number, sent_at, message_id=None):
    """Move a WhatsApp number into user_id (a Telegram chat's user), and tell everyone
    concerned. The caller has checked the region."""
    previous = identity.user_of(cur, "whatsapp", number)
    moved = previous and previous["id"] != user_id
    previous_chats = _chats(cur, previous["id"]) if moved else []
    identity.attach(cur, user_id, "whatsapp", number, deliver=False)
    if moved and not _numbers(cur, previous["id"]):
        # A Notifier linked through this number follows it: it was linked to receive
        # this number's transcripts, and would otherwise be left on a user nothing
        # sends to any more.
        for ident in identity.identities_of(cur, previous["id"]):
            if ident["channel"] == "notifier":
                identity.attach(cur, user_id, "notifier", ident["address"], deliver=True)
    masked = identity.mask("whatsapp", number)
    for chat in previous_chats:
        outbox.enqueue_text(cur, _tg(chat), messages.tg_moved_away(masked), sent_at)
    for chat in _chats(cur, user_id):
        outbox.enqueue_text(cur, _tg(chat), messages.tg_linked(masked), sent_at)
    if message_id:
        _react_whatsapp(cur, number, message_id, True, sent_at)


_link_whatsapp = _link_whatsapp_to_telegram  # backwards-compatibility alias


# --- Notifier

def notifier_code(text):
    """The Notifier link code in text, canonical, or None."""
    if not (text and notifier.enabled()):
        return None
    match = NOTIFIER_CODE.match(text)
    return (match.group(1) + match.group(2)).upper() if match else None


def is_notifier_link(text):
    return notifier_code(text) is not None


def handle_notifier_link(cur, channel, address, code, sent_at, message_id=None):
    """Redeeming takes a call to the Notifier, so it is queued; notifier_redeemed
    finishes the job."""
    outbox.enqueue_link(cur, code, {"channel": channel, "address": str(address), "message_id": message_id}, sent_at)


def notifier_label(owner):
    """How the app shows this link: which number or chat it came from, masked."""
    name = "WhatsApp" if owner["channel"] == "whatsapp" else "Telegram"
    return f"{name} {identity.mask(owner['channel'], owner['address'])}"


def notifier_redeemed(cur, owner, status, subscription_id, sent_at):
    """The Notifier's answer to a link code owner sent. On success the subscription
    joins owner's user as an output; from WhatsApp, the number also stops getting
    replies there, which is the point."""
    channel, address = owner["channel"], owner["address"]
    if status != "ok":
        # The app already says what went wrong. Telegram costs nothing, so it hears in
        # words; WhatsApp gets a (free) thumbs down on the link message.
        if channel == "telegram":
            outbox.enqueue_text(cur, _tg(address), messages.TG_NOTIFIER_CODE_FAILED, sent_at)
        elif owner.get("message_id"):
            _react_whatsapp(cur, address, owner["message_id"], False, sent_at)
        return
    user_id = identity.ensure_user(cur, channel, address, deliver=channel == "telegram")
    if channel == "whatsapp":
        identity.attach(cur, user_id, "whatsapp", address, deliver=False)
    identity.attach(cur, user_id, "notifier", subscription_id, deliver=True)
    masked = identity.mask(channel, address)
    outbox.enqueue_text(cur, {"channel": "notifier", "address": subscription_id, "quote": None},
                        messages.notifier_welcome(channel, masked), sent_at, meta={"kind": "welcome"})
    for chat in _chats(cur, user_id):
        outbox.enqueue_text(cur, _tg(chat), messages.TG_NOTIFIER_LINKED, sent_at)
    if channel == "whatsapp" and owner.get("message_id"):
        _react_whatsapp(cur, address, owner["message_id"], True, sent_at)


def _react_whatsapp(cur, number, message_id, linked, sent_at):
    """Answer a link message on WhatsApp with 👍 (linked) or 👎 (not): reactions are free,
    where any text reply would be billed."""
    if not message_id:
        return
    outbox.enqueue_reaction(cur, {"channel": "whatsapp", "address": str(number), "quote": message_id},
                            "👍" if linked else "👎", sent_at)


def _outputs(cur, user_id, channel):
    return [i["address"] for i in identity.identities_of(cur, user_id) if i["channel"] == channel]


# --- Telegram linking (from WhatsApp)

def telegram_code(text):
    """The 8-character Telegram link code in text, canonical, or None."""
    if not text:
        return None
    match = TELEGRAM_CODE.match(text)
    return match.group(1).upper() if match else None


def is_telegram_link(text):
    return telegram_code(text) is not None


def handle_telegram_link(cur, parsed, sent_at):
    """"link <code>" from WhatsApp to link with a Telegram chat. Outcomes are reported
    in Telegram, where the code came from (when it is known), and on WhatsApp as a
    reaction on the link message: 👍 linked, 👎 not."""
    number = parsed["sender"]
    code = telegram_code(parsed["text"])
    status, user_id = identity.consume_token(cur, code)
    if status == "ok" and limits.is_allowed_region(number):
        _link_whatsapp_to_telegram(cur, user_id, number, sent_at, message_id=parsed.get("message_id"))
        offers.retire(cur, [code], "used", sent_at)
        return
    if status == "expired":
        for chat in _chats(cur, user_id):
            outbox.enqueue_text(cur, _tg(chat), messages.TG_LINK_EXPIRED, sent_at)
    _react_whatsapp(cur, number, parsed.get("message_id"), False, sent_at)


# Backwards-compatibility aliases
is_whatsapp_link = is_telegram_link
handle_whatsapp_link = handle_telegram_link


# --- Telegram

def _offer_link(cur, chat, user_id, sent_at, quote=None):
    # Minting cancels the user's earlier codes, so their messages say so.
    offers.retire(cur, identity.live_tokens(cur, user_id), "replaced", sent_at)
    token = identity.mint_token(cur, user_id)
    numbers = _numbers(cur, user_id)
    text = (messages.tg_already_linked([identity.mask("whatsapp", n) for n in numbers])
            if numbers else messages.TG_WELCOME)
    buttons = None
    if whatsapp.display_number():
        url = f"https://wa.me/{whatsapp.display_number()}?text=link%20{token}"
        buttons = [{"text": messages.LINK_BUTTON, "url": url}]
    outbox.enqueue_text(cur, _tg(chat, quote), text + messages.tg_link_code_hint(token), sent_at,
                        buttons=buttons, offer={"token": token, "base_text": text})


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
    code = notifier_code(text) or (notifier_code(arg) if command == "/start" else None)
    if code:
        handle_notifier_link(cur, "telegram", chat, code, sent_at)
        return
    if command == "/unlink":
        user = identity.user_of(cur, "telegram", chat)
        if user and _outputs(cur, user["id"], "notifier"):
            # The numbers keep delivering to the Notifier; only this chat leaves.
            identity.detach(cur, "telegram", chat)
            outbox.enqueue_text(cur, here, messages.TG_CHAT_UNLINKED, sent_at)
            return
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
