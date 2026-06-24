import os
import hashlib
import logging
import queue
import threading
import time

import requests

logger = logging.getLogger('whatsapp_bot')

# (connect_timeout, read_timeout) for POSTs to the status site.
REQUEST_TIMEOUT = (5, 10)
SEND_INTERVAL = 5          # seconds between queue drains / POSTs
HEARTBEAT_INTERVAL = 30    # max seconds between sends, so idle instances stay "live"
QUEUE_MAXSIZE = 10000
MAX_BATCH = 500            # cap events per POST so a backlog doesn't build a giant body


class StatusReporter:
    """Best-effort, fire-and-forget reporter that POSTs per-event metadata to the
    Eliezer Status site. Opt-in: disabled (no-op) unless STATUS_SITE_URL is set, so
    the bot behaves exactly as before for users who don't run the site."""

    def __init__(self, instance_id, stop_event, queue_depth_fn=None):
        self.instance_id = instance_id
        self.stop_event = stop_event
        self.queue_depth_fn = queue_depth_fn

        self.site_url = os.environ.get("STATUS_SITE_URL", "").rstrip("/")
        self.token = os.environ.get("STATUS_INGEST_TOKEN", "")
        self.salt = os.environ.get("STATUS_USER_SALT", "")
        self.enabled = bool(self.site_url)

        self.start_time = time.time()
        self._queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

    def user_hash(self, number):
        """Salted SHA-256 of the sender's number. Shared salt across instances yields
        consistent hashes so the site can count distinct users without storing PII."""
        if not number:
            return None
        return hashlib.sha256((self.salt + str(number)).encode()).hexdigest()[:16]

    def _enqueue(self, event):
        if not self.enabled:
            return
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            # Never block the transcription path; drop on overflow.
            logger.debug("StatusReporter queue full; dropping event")

    def record_message(self, message_type, user_hash):
        self._enqueue({
            "kind": "message",
            "ts": time.time(),
            "message_type": message_type,
            "user_hash": user_hash,
        })

    def record_transcription(self, duration, message_type, user_hash):
        self._enqueue({
            "kind": "transcription",
            "ts": time.time(),
            "duration_seconds": duration,
            "message_type": message_type,
            "user_hash": user_hash,
        })

    def _drain(self):
        events = []
        while len(events) < MAX_BATCH:
            try:
                events.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return events

    def _queue_depth(self):
        if not self.queue_depth_fn:
            return None
        try:
            return self.queue_depth_fn()
        except Exception:
            logger.debug("StatusReporter queue_depth_fn failed", exc_info=True)
            return None

    def _post(self, events):
        payload = {
            "instance_id": self.instance_id,
            "uptime_seconds": time.time() - self.start_time,
            "queue_depth": self._queue_depth(),
            "events": events,
        }
        resp = requests.post(
            f"{self.site_url}/api/events",
            json=payload,
            headers={"X-Ingest-Token": self.token},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

    def run(self):
        if not self.enabled:
            return
        logger.debug("StatusReporter started, posting to %s", self.site_url)
        last_send = 0.0
        while not self.stop_event.is_set():
            self.stop_event.wait(SEND_INTERVAL)
            events = self._drain()
            now = time.time()
            # Send when we have events, or on heartbeat so idle instances stay live.
            if not events and (now - last_send) < HEARTBEAT_INTERVAL:
                continue
            try:
                self._post(events)
                last_send = now
            except Exception:
                logger.debug("StatusReporter POST failed", exc_info=True)
                # Re-queue drained events if there's room, else drop.
                for ev in events:
                    try:
                        self._queue.put_nowait(ev)
                    except queue.Full:
                        break
