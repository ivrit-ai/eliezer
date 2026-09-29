"""The edge's side of the status site's queue API.

The edge leases a job, fetches its media through the site, reports the audio's duration
for admission, and hands back the transcript. Everything user-facing - replies, limits,
statistics - happens on the site, so this is the edge's only connection to anything.
"""

import os
import socket
import time

import requests

# (connect, read) seconds. Media gets longer to read: it is a whole audio file.
REQUEST_TIMEOUT = (5, 30)
MEDIA_TIMEOUT = (10, 120)
RETRIES = 3


class StaleJob(Exception):
    """This lease no longer holds the job - it expired and another edge took it, or it
    was dropped. Nothing to do but let it go."""


class QueueClient:
    def __init__(self, base_url, token, instance_id):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"
        # Lets the site attribute leases to this edge.
        self.session.headers["X-Instance-Id"] = instance_id
        self.session.headers["X-Edge-Protocol"] = "2"
        self.started = time.time()

    def _post(self, path, payload, timeout=REQUEST_TIMEOUT, retries=RETRIES):
        # A lost admit or complete costs a whole re-transcription once the lease times
        # out, so transient failures are worth a few attempts. Both are safe to repeat.
        for attempt in range(retries):
            try:
                r = self.session.post(f"{self.base_url}{path}", json=payload, timeout=timeout)
                if r.status_code == 409:
                    raise StaleJob(r.text)
                r.raise_for_status()
                return r.json()
            except (requests.ConnectionError, requests.Timeout):
                if attempt == retries - 1:
                    raise
                time.sleep(0.5 * (attempt + 1))
            except requests.HTTPError as e:
                if e.response is None or e.response.status_code < 500 or attempt == retries - 1:
                    raise
                time.sleep(0.5 * (attempt + 1))

    def lease(self, max_jobs, wait_seconds, min_depth=0):
        """Up to max_jobs jobs, long-polling up to wait_seconds. Doubles as this edge's
        heartbeat, so it carries the uptime."""
        return self._post(
            "/queue/lease",
            {"max": max_jobs, "wait": wait_seconds, "min_depth": min_depth,
             "uptime_seconds": time.time() - self.started},
            timeout=(5, wait_seconds + 15),
            retries=1,
        )["jobs"]

    def fetch_media(self, handle, path):
        """Download the job's media to path."""
        with self.session.get(f"{self.base_url}/queue/media/{handle}", stream=True,
                              timeout=MEDIA_TIMEOUT) as r:
            if r.status_code == 404:
                raise StaleJob(r.text)
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    f.write(chunk)

    def admit(self, handle, duration):
        """Whether to transcribe. On False the site has closed the job (and told the user
        why, on channels where that costs nothing)."""
        return self._post("/queue/admit", {"handle": handle, "duration": duration})["ok"]

    def release(self, handle):
        """Give back a job this edge failed on, so it is retried soon instead of when the
        lease would lapse."""
        self._post("/queue/release", {"handle": handle}, retries=1)

    def complete(self, handle, text=None, error=None, transcription_seconds=None, duration=None):
        """Hand back the transcript (or the reason there is none); the site replies."""
        self._post("/queue/complete", {
            "handle": handle, "text": text, "error": error,
            "transcription_seconds": transcription_seconds, "duration": duration,
        })


def make_queue_client():
    url = os.getenv("QUEUE_API_URL")
    if not url:
        raise RuntimeError("QUEUE_API_URL is not set: the edge needs the status site's queue")
    return QueueClient(
        url,
        os.getenv("QUEUE_TOKEN", ""),
        os.getenv("INSTANCE_ID") or socket.gethostname(),
    )
