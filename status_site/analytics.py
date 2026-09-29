"""Product analytics (PostHog), captured by the hub on behalf of the whole fleet."""

import os

import posthog

_client = None
if os.environ.get("POSTHOG_API_KEY"):
    _client = posthog.Posthog(
        project_api_key=os.environ["POSTHOG_API_KEY"], host="https://us.i.posthog.com"
    )


def capture(distinct_id, event, props=None, instance_id=None):
    """Same event shape the edges used to send; instance_id names the edge that did the
    work, or "hub" for what the hub handled itself."""
    if not _client:
        return
    props = dict(props or {})
    props["source"] = "eliezer.ivrit.ai"
    props["instance_id"] = instance_id or "unknown"
    _client.capture(distinct_id=distinct_id, event=event, properties=props)
