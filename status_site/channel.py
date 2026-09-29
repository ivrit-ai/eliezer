"""What every channel adapter shares.

An adapter module (whatsapp.py, telegram.py) provides:
  webhooks: verify_registration(args), verify_payload(raw, headers), split(payload), parse(body)
  replies:  target(parsed), MAX_TEXT_LENGTH, send_text(address, text, quote, buttons),
            send_receipt(address, message_id, typing)
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
