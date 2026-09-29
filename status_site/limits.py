"""Who may use the service, and how much: region gate, audio length cap and per-user
rate limits. Held here, in the one hub process, so the limits apply fleet-wide; when
each edge kept its own buckets, a user's effective limit grew with the fleet."""

import os
import threading
import time

import phonenumbers
from phonenumbers import region_code_for_number

MAX_AUDIO_SECONDS = 600
USER_MAX_MESSAGES_PER_HOUR = float(os.environ.get("USER_MAX_MESSAGES_PER_HOUR", "10"))
USER_MAX_MINUTES_PER_HOUR = float(os.environ.get("USER_MAX_MINUTES_PER_HOUR", "20"))
BUCKET_CLEANUP_EVERY = 50  # admissions between sweeps of full (idle) buckets

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


class LeakyBucket:
    def __init__(self, max_messages_per_hour, max_minutes_per_hour):
        self.max_messages = max_messages_per_hour
        self.max_minutes = max_minutes_per_hour * 60  # Convert to seconds
        self.messages_remaining = max_messages_per_hour
        self.seconds_remaining = max_minutes_per_hour * 60
        self.last_update = time.time()

        # Calculate fill rates (per second)
        self.message_fill_rate = max_messages_per_hour / 3600
        self.time_fill_rate = self.max_minutes / 3600

    def update(self):
        """Update bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        self.last_update = now

        # Add resources based on fill rate
        self.messages_remaining = min(self.max_messages, self.messages_remaining + self.message_fill_rate * elapsed)
        self.seconds_remaining = min(self.max_minutes, self.seconds_remaining + self.time_fill_rate * elapsed)

    def can_transcribe(self, duration_seconds):
        """Check if transcription is allowed."""
        self.update()
        return self.messages_remaining >= 1 and self.seconds_remaining >= duration_seconds

    def consume(self, duration_seconds):
        """Consume resources for transcription."""
        self.update()
        self.messages_remaining -= 1
        self.seconds_remaining -= duration_seconds
        return self.messages_remaining > 0 and self.seconds_remaining > 0

    def is_full(self):
        """Check if the bucket is full (or nearly full)."""
        self.update()
        return (self.messages_remaining >= self.max_messages * 0.95 and
                self.seconds_remaining >= self.max_minutes * 0.95)

    def minutes_until_allowed(self, duration_seconds):
        """Rough wait before a transcription of this length would be allowed."""
        if self.messages_remaining < 1:
            remaining_time = 1 / USER_MAX_MESSAGES_PER_HOUR
        else:
            # Must be time limit that's causing the issue
            remaining_time = (duration_seconds - self.seconds_remaining) / (USER_MAX_MINUTES_PER_HOUR * 60)
        return max(1, int(remaining_time * 60))


_buckets = {}
_buckets_lock = threading.Lock()
_admissions = 0


def admit(user, duration_seconds):
    """Charge a transcription of this length to the user's bucket.

    Returns (allowed, has_resources_left, bucket). Check and charge happen under one lock,
    so two edges admitting the same user's files at once cannot both slip under the limit.
    """
    global _admissions
    with _buckets_lock:
        bucket = _buckets.get(user)
        if bucket is None:
            bucket = _buckets[user] = LeakyBucket(USER_MAX_MESSAGES_PER_HOUR, USER_MAX_MINUTES_PER_HOUR)
        if not bucket.can_transcribe(duration_seconds):
            return False, None, bucket
        has_resources_left = bucket.consume(duration_seconds)
        _admissions += 1
        if _admissions % BUCKET_CLEANUP_EVERY == 0:
            # A full bucket is indistinguishable from a new one; drop them to bound memory.
            for key in [k for k, b in _buckets.items() if b.is_full()]:
                del _buckets[key]
        return True, has_resources_left, bucket
