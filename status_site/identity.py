"""Who is who across channels, and where their replies go.

A user is a set of identities - a WhatsApp number, a Telegram chat, a Notifier app - joined
by linking. Only people who link or are allowlisted get a user; everyone else stays an
anonymous sender. Each identity has a deliver flag: replies go to every identity with it
set, and WhatsApp additionally needs permission (the global policy, or the allowlist),
because WhatsApp messages are the ones that cost.
"""

import secrets

# Crockford base32: no I, L, O or U, so a code survives being read aloud or retyped.
TOKEN_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
TOKEN_LENGTH = 8
TOKEN_TTL_SECONDS = 15 * 60
# Expired tokens are kept a while longer, to tell a late user "expired" rather than "wrong".
TOKEN_GRACE_SECONDS = 60 * 60

POLICY_KEY = "unlinked_wa_policy"
POLICIES = ("reply", "drop")

# Channels whose identity is a real user-facing output (as opposed to WhatsApp, the input
# channel users are being moved off).
OUTPUT_CHANNELS = ("telegram", "notifier")


def init_identity_db(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          id                 BIGSERIAL PRIMARY KEY,
          wa_replies_allowed BOOLEAN NOT NULL DEFAULT false,
          note               TEXT,
          created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS identities (
          channel    TEXT NOT NULL,
          address    TEXT NOT NULL,
          user_id    BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
          deliver    BOOLEAN NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (channel, address)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS identities_user_idx ON identities (user_id);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS link_tokens (
          token      TEXT PRIMARY KEY,
          user_id    BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
          expires_at TIMESTAMPTZ NOT NULL
        );
        """
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
    )
    # WhatsApp numbers already told where transcripts went: each is told once, since
    # every WhatsApp message is billed.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wa_told (
          address  TEXT PRIMARY KEY,
          told_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    # The dashboard's running count starts from whatever is already recorded.
    cur.execute("UPDATE totals SET wa_told = (SELECT count(*) FROM wa_told) WHERE id = 1;")
    # Until an admin says otherwise, WhatsApp works as it always has.
    cur.execute(
        "INSERT INTO settings (key, value) VALUES (%s, 'reply') ON CONFLICT DO NOTHING;",
        (POLICY_KEY,),
    )


# --- policy

def policy(cur):
    cur.execute("SELECT value FROM settings WHERE key = %s;", (POLICY_KEY,))
    row = cur.fetchone()
    return row["value"] if row else "reply"


def set_policy(cur, value):
    cur.execute(
        "INSERT INTO settings (key, value) VALUES (%s, %s) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;",
        (POLICY_KEY, value),
    )


# --- lookups

def user_of(cur, channel, address):
    cur.execute(
        "SELECT u.id, u.wa_replies_allowed, u.note FROM identities i JOIN users u ON u.id = i.user_id "
        "WHERE i.channel = %s AND i.address = %s;",
        (channel, str(address)),
    )
    return cur.fetchone()


def identities_of(cur, user_id):
    cur.execute(
        "SELECT channel, address, deliver FROM identities WHERE user_id = %s ORDER BY created_at;",
        (user_id,),
    )
    return cur.fetchall()


def wa_permitted(cur, user):
    return policy(cur) == "reply" or bool(user and user["wa_replies_allowed"])


def resolve_targets(cur, channel, parsed, default_target):
    """Where replies to this message go. Empty means nowhere: drop it."""
    user = user_of(cur, channel, parsed["sender"])
    if user is None:
        if channel == "whatsapp":
            return [default_target] if policy(cur) == "reply" else []
        return [default_target]
    permitted = wa_permitted(cur, user)
    targets = []
    for ident in identities_of(cur, user["id"]):
        if not ident["deliver"] or (ident["channel"] == "whatsapp" and not permitted):
            continue
        # Quote the message only where it was sent; elsewhere there is nothing to quote.
        same = ident["channel"] == channel and ident["address"] == str(parsed["sender"])
        targets.append({"channel": ident["channel"], "address": ident["address"],
                        "quote": parsed["message_id"] if same else None})
    if not targets and (channel != "whatsapp" or permitted):
        # A chat always hears back about what it sent itself - including a WhatsApp
        # number whose linked chat has since gone (blocked, deleted), while WhatsApp
        # replies are allowed for it.
        targets = [default_target]
    return targets


# --- users and linking

def _drop_orphans(cur):
    cur.execute(
        "DELETE FROM users u WHERE NOT EXISTS (SELECT 1 FROM identities i WHERE i.user_id = u.id);"
    )


def ensure_user(cur, channel, address, deliver):
    """The user behind an identity, creating both if needed."""
    user = user_of(cur, channel, address)
    if user:
        return user["id"]
    cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id;")
    user_id = cur.fetchone()["id"]
    cur.execute(
        "INSERT INTO identities (channel, address, user_id, deliver) VALUES (%s, %s, %s, %s);",
        (channel, str(address), user_id, deliver),
    )
    return user_id


def attach(cur, user_id, channel, address, deliver):
    """Move an identity to a user. Returns the user it belonged to before, if another."""
    previous = user_of(cur, channel, address)
    cur.execute(
        "INSERT INTO identities (channel, address, user_id, deliver) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (channel, address) DO UPDATE SET user_id = EXCLUDED.user_id, deliver = EXCLUDED.deliver;",
        (channel, str(address), user_id, deliver),
    )
    _drop_orphans(cur)
    if previous and previous["id"] != user_id:
        return previous["id"]
    return None


def detach(cur, channel, address):
    """Remove an identity; its user goes too if nothing else is left."""
    cur.execute("DELETE FROM identities WHERE channel = %s AND address = %s;", (channel, str(address)))
    removed = cur.rowcount
    _drop_orphans(cur)
    return removed


def mint_token(cur, user_id):
    """A fresh link code for this user; any earlier one stops working."""
    cur.execute("DELETE FROM link_tokens WHERE user_id = %s;", (user_id,))
    token = "".join(secrets.choice(TOKEN_ALPHABET) for _ in range(TOKEN_LENGTH))
    cur.execute(
        "INSERT INTO link_tokens (token, user_id, expires_at) "
        "VALUES (%s, %s, now() + make_interval(secs => %s::double precision));",
        (token, user_id, TOKEN_TTL_SECONDS),
    )
    return token


def consume_token(cur, token):
    """('ok', user_id) once; ('expired', user_id) past its TTL; ('unknown', None)."""
    cur.execute(
        "SELECT user_id, expires_at < now() AS expired FROM link_tokens WHERE token = %s FOR UPDATE;",
        (token.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return "unknown", None
    if row["expired"]:
        return "expired", row["user_id"]
    cur.execute("DELETE FROM link_tokens WHERE token = %s;", (token.upper(),))
    return "ok", row["user_id"]


def sweep_tokens(cur):
    cur.execute(
        "DELETE FROM link_tokens WHERE expires_at < now() - make_interval(secs => %s::double precision);",
        (TOKEN_GRACE_SECONDS,),
    )


def mark_wa_told(cur, number):
    """Record that number has been told where to get transcripts now. True only the
    first time, so the caller tells it exactly once."""
    cur.execute(
        "INSERT INTO wa_told (address) VALUES (%s) ON CONFLICT DO NOTHING;", (str(number),)
    )
    if cur.rowcount != 1:
        return False
    cur.execute("UPDATE totals SET wa_told = wa_told + 1 WHERE id = 1;")
    return True


def mask(channel, address):
    """Enough of an address to recognize it, not enough to use it."""
    address = str(address)
    if channel == "whatsapp":
        return f"+{address[:3]}…{address[-3:]}"
    return f"…{address[-4:]}"


# --- admin

def set_wa_allowed(cur, number, allowed, note=None):
    """Allowlist a WhatsApp number (it keeps WhatsApp replies whatever the policy), or
    take it off. Returns False when revoking a number that was not allowlisted."""
    if allowed:
        user_id = ensure_user(cur, "whatsapp", number, deliver=True)
        cur.execute(
            "UPDATE users SET wa_replies_allowed = true, note = COALESCE(%s, note) WHERE id = %s;",
            (note, user_id),
        )
        return True
    user = user_of(cur, "whatsapp", number)
    if user is None or not user["wa_replies_allowed"]:
        return False
    cur.execute("UPDATE users SET wa_replies_allowed = false WHERE id = %s;", (user["id"],))
    if len(identities_of(cur, user["id"])) == 1:
        # Only ever existed to be allowlisted: back to an anonymous sender.
        detach(cur, "whatsapp", number)
    return True


def describe(cur, channel, address):
    user = user_of(cur, channel, address)
    if user is None:
        return None
    return {
        "user_id": user["id"],
        "wa_replies_allowed": user["wa_replies_allowed"],
        "note": user["note"],
        "identities": [dict(i) for i in identities_of(cur, user["id"])],
    }


def linked_users(cur):
    cur.execute(
        "SELECT count(DISTINCT user_id) AS n FROM identities WHERE channel = ANY(%s);",
        (list(OUTPUT_CHANNELS),),
    )
    return int(cur.fetchone()["n"])
