"""What every channel adapter shares.

An adapter module (whatsapp.py, telegram.py) provides the following; notifier.py is
output-only, and provides just the replies part (its send_text also takes meta and
dedupe_key):
  webhooks: verify_registration(args), verify_payload(raw, headers), split(payload), parse(body)
  replies:  target(parsed), MAX_TEXT_LENGTH, send_text(address, text, quote, buttons),
            send_receipt(address, message_id, typing), send_reaction(address, message_id, emoji)
  media:    open_media(media)
and raises SendError from its send functions.
"""


class SendError(Exception):
    def __init__(self, message, retryable, retry_after=None, gone=False):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after
        # The recipient can never be reached here again (blocked the bot, deleted the
        # account): drop the binding rather than keep sending into the void.
        self.gone = gone
