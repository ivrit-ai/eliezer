"""Who may use the service, and how much: region gate, audio length cap and per-user
rate limits. Held by the hub, so the limits apply fleet-wide; when each edge kept its
own buckets, a user's effective limit grew with the fleet."""

import os
import time

import phonenumbers
from phonenumbers import region_code_for_number

MAX_AUDIO_SECONDS = 600
USER_MAX_MESSAGES_PER_HOUR = float(os.environ.get("USER_MAX_MESSAGES_PER_HOUR", "10"))
USER_MAX_MINUTES_PER_HOUR = float(os.environ.get("USER_MAX_MINUTES_PER_HOUR", "20"))

ALLOWED_REGIONS = {
    # North America
    'US', 'CA',
    # Europe (EU countries and other European countries)
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU',
    'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES',
    'SE', 'GB', 'IS', 'LI', 'NO', 'CH', 'AL', 'AD', 'BA', 'BY', 'FO', 'GI', 'VA',
    'IM', 'JE', 'XK', 'MK', 'MD', 'MC', 'ME', 'RU', 'SM', 'RS', 'SJ', 'TR', 'UA',
    # Israel
    'IL',
}


def is_allowed_region(phone_number):
    """Whether a WhatsApp number (digits, no '+') is from Israel, Europe or North America."""
    try:
        return region_code_for_number(phonenumbers.parse("+" + phone_number)) in ALLOWED_REGIONS
    except phonenumbers.phonenumberutil.NumberParseException:
        return False


# --- buckets, in Postgres
#
# A bucket fills at `rate` per second up to `cap`, and a transcription takes from it.
# Kept in Postgres so the limits survive a hub restart and hold across hub workers; a
# row is locked (FOR UPDATE) while it is checked and charged, so two edges admitting
# the same user's files at once cannot both slip under the limit, and the charge
# commits with whatever the caller decided - a retried admit finds that decision and is
# not charged again (see queue_api._admit).
#
# Kinds: "msgs_hour" and "secs_hour" for voice messages (Eliezer's hourly limits),
# "secs_week" for files transcribed in the app (transcribe.ivrit.ai's weekly quota).

HOURLY = {
    "msgs_hour": (USER_MAX_MESSAGES_PER_HOUR, USER_MAX_MESSAGES_PER_HOUR / 3600),
    "secs_hour": (USER_MAX_MINUTES_PER_HOUR * 60, USER_MAX_MINUTES_PER_HOUR * 60 / 3600),
}
FILE_MINUTES_PER_WEEK = float(os.environ.get("FILE_QUOTA_MINUTES_PER_WEEK", "420"))
FILE_REPLENISH_MINUTES_PER_DAY = float(os.environ.get("FILE_QUOTA_REPLENISH_MINUTES_PER_DAY", "60"))
WEEKLY = (FILE_MINUTES_PER_WEEK * 60, FILE_REPLENISH_MINUTES_PER_DAY * 60 / 86400)


def init_db(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS quota_buckets (
          user_key   TEXT NOT NULL,
          kind       TEXT NOT NULL,
          level      DOUBLE PRECISION NOT NULL,
          cap        DOUBLE PRECISION NOT NULL,
          rate       DOUBLE PRECISION NOT NULL,
          custom_cap BOOLEAN NOT NULL DEFAULT false,
          updated_at DOUBLE PRECISION NOT NULL,
          PRIMARY KEY (user_key, kind)
        );
        """
    )
    # What each job was charged, so a charge and its refund each happen once however
    # often the calls behind them are retried.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS quota_ledger (
          job_id   TEXT NOT NULL,
          kind     TEXT NOT NULL,
          user_key TEXT NOT NULL,
          amount   DOUBLE PRECISION NOT NULL,
          refunded BOOLEAN NOT NULL DEFAULT false,
          at       TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (job_id, kind)
        );
        """
    )


class Bucket:
    def __init__(self, user_key, kind, level, cap, rate, updated_at):
        self.user_key, self.kind, self.cap, self.rate = user_key, kind, cap, rate
        now = time.time()
        self.level = min(cap, level + rate * max(0.0, now - updated_at))
        self.updated_at = now

    def wait_seconds(self, amount):
        """Until `amount` is available: 0 now, inf if it never fits."""
        if amount > self.cap:
            return float("inf")
        if self.level >= amount:
            return 0.0
        return (amount - self.level) / self.rate if self.rate > 0 else float("inf")


def _lock(cur, user_key, kind, cap, rate):
    cur.execute(
        "INSERT INTO quota_buckets (user_key, kind, level, cap, rate, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
        (user_key, kind, cap, cap, rate, time.time()),
    )
    cur.execute(
        "SELECT level, cap, rate, custom_cap, updated_at FROM quota_buckets "
        "WHERE user_key = %s AND kind = %s FOR UPDATE;",
        (user_key, kind),
    )
    row = cur.fetchone()
    # The configured rate applies to everyone; a cap set for one user stays theirs.
    return Bucket(user_key, kind, row["level"], row["cap"] if row["custom_cap"] else cap, rate, row["updated_at"])


def _peek(cur, user_key, kind, cap, rate):
    cur.execute(
        "SELECT level, cap, custom_cap, updated_at FROM quota_buckets WHERE user_key = %s AND kind = %s;",
        (user_key, kind),
    )
    row = cur.fetchone()
    if row is None:
        return Bucket(user_key, kind, cap, cap, rate, time.time())
    return Bucket(user_key, kind, row["level"], row["cap"] if row["custom_cap"] else cap, rate, row["updated_at"])


def _save(cur, bucket):
    cur.execute(
        "UPDATE quota_buckets SET level = %s, cap = %s, rate = %s, updated_at = %s "
        "WHERE user_key = %s AND kind = %s;",
        (bucket.level, bucket.cap, bucket.rate, bucket.updated_at, bucket.user_key, bucket.kind),
    )


class Refusal:
    """Why a voice message was refused, for the user's notice and for analytics."""

    def __init__(self, messages, seconds):
        self.messages_remaining = messages.level
        self.seconds_remaining = seconds.level
        self._messages, self._seconds = messages, seconds

    def minutes_until_allowed(self, duration_seconds):
        if self._messages.level < 1:
            wait = self._messages.wait_seconds(1)
        else:
            wait = self._seconds.wait_seconds(duration_seconds)
        return max(1, int(min(wait, 10 ** 6) / 60))


def admit(cur, user, duration_seconds):
    """Charge a voice message of this length to the user's hourly buckets, inside the
    caller's transaction. Returns (allowed, has_resources_left, refusal)."""
    messages = _lock(cur, user, "msgs_hour", *HOURLY["msgs_hour"])
    seconds = _lock(cur, user, "secs_hour", *HOURLY["secs_hour"])
    if messages.level < 1 or seconds.level < duration_seconds:
        return False, None, Refusal(messages, seconds)
    messages.level -= 1
    seconds.level -= duration_seconds
    _save(cur, messages)
    _save(cur, seconds)
    return True, messages.level > 0 and seconds.level > 0, None


# --- the weekly quota for files

def file_quota(cur, user):
    """The user's weekly bucket as it stands (read only)."""
    return _peek(cur, user, "secs_week", *WEEKLY)


def charge_file(cur, job_id, user, seconds):
    """Charge a file job once. Returns (ok, bucket): not ok, nothing charged, when the
    bucket lacks the time. A repeat of a charge already made is ok and charges nothing."""
    bucket = _lock(cur, user, "secs_week", *WEEKLY)
    cur.execute("SELECT 1 FROM quota_ledger WHERE job_id = %s AND kind = 'secs_week';", (job_id,))
    if cur.fetchone():
        return True, bucket
    if bucket.level < seconds:
        return False, bucket
    bucket.level -= seconds
    _save(cur, bucket)
    cur.execute(
        "INSERT INTO quota_ledger (job_id, kind, user_key, amount) VALUES (%s, 'secs_week', %s, %s);",
        (job_id, user, seconds),
    )
    return True, bucket


def refund_file(cur, job_id):
    """Give back what a job was charged, once: it produced nothing."""
    cur.execute(
        "UPDATE quota_ledger SET refunded = true WHERE job_id = %s AND kind = 'secs_week' "
        "AND NOT refunded RETURNING user_key, amount;",
        (job_id,),
    )
    row = cur.fetchone()
    if row is None:
        return 0.0
    bucket = _lock(cur, row["user_key"], "secs_week", *WEEKLY)
    # Capped, so a refund after the bucket refilled cannot lift anyone above their cap.
    bucket.level = min(bucket.cap, bucket.level + row["amount"])
    _save(cur, bucket)
    return row["amount"]


def sweep(cur):
    """Forget buckets that have refilled - indistinguishable from new ones - and ledger
    entries too old to be refunded."""
    cur.execute(
        "DELETE FROM quota_buckets WHERE NOT custom_cap "
        "AND level + rate * (extract(epoch FROM now()) - updated_at) >= cap;"
    )
    cur.execute("DELETE FROM quota_ledger WHERE at < now() - interval '30 days';")
