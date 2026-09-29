"""Queue transport seam.

Two interchangeable backends: the status site's HTTP queue API, and AWS SQS.
Both normalize messages to {"body": str, "handle": str, "source": str}.

SQS remains only for the cutover, so each edge can move to the status site
independently by setting QUEUE_API_URL; it goes once the SQS queue is retired.
"""

import os
import time

import boto3
import requests


class SqsQueueClient:
    def __init__(self, queue_url=None):
        self.sqs = boto3.client("sqs")
        self.queue_url = queue_url or os.getenv("APP_SQS_QUEUE")

    def depth(self):
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        return int(attrs["Attributes"]["ApproximateNumberOfMessages"])

    def lease(self, max_messages, wait_seconds, min_depth=0):
        if min_depth:
            max_messages = min(max_messages, self.depth() - min_depth)
            if max_messages < 1:
                # No long poll to block on, so honor the caller's wait here instead —
                # otherwise the dispatcher spins on get_queue_attributes.
                time.sleep(wait_seconds)
                return []
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=wait_seconds,
        )
        return [
            {"body": m["Body"], "handle": m["ReceiptHandle"], "source": "whatsapp"}
            for m in response.get("Messages", [])
        ]

    def ack(self, handle):
        self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=handle)


class HttpQueueClient:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"

    def depth(self):
        r = self.session.get(f"{self.base_url}/queue/depth", timeout=(5, 15))
        r.raise_for_status()
        return int(r.json()["depth"])

    def lease(self, max_messages, wait_seconds, min_depth=0):
        r = self.session.post(
            f"{self.base_url}/queue/lease",
            json={"max": max_messages, "wait": wait_seconds, "min_depth": min_depth},
            timeout=(5, wait_seconds + 15),
        )
        r.raise_for_status()
        return r.json()["messages"]

    def ack(self, handle):
        # boto3 retried this for us; requests does not. A lost ack means the message comes
        # back after the visibility timeout and the user gets a second transcription of the
        # same voice note, so it is worth a few attempts. Acking twice is a no-op.
        for attempt in range(3):
            try:
                r = self.session.post(
                    f"{self.base_url}/queue/ack", json={"handle": handle}, timeout=(5, 15)
                )
                r.raise_for_status()
                return
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(0.5 * (attempt + 1))


def make_queue_client():
    url = os.getenv("QUEUE_API_URL")
    if url:
        return HttpQueueClient(url, os.getenv("QUEUE_TOKEN", ""))
    return SqsQueueClient()
