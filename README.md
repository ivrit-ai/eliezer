# WhatsApp Bot

A simple bot that processes WhatsApp messages from an SQS queue and responds to them.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Copy the environment template and fill in your values:
```bash
cp .env.example .env
```

3. Edit the `.env` file with your credentials:
- `APP_SQS_QUEUE`: Your SQS queue URL
- `WHATSAPP_API_TOKEN`: Your WhatsApp Business API token
- `WHATSAPP_PHONE_NUMBER_ID`: Your WhatsApp phone number ID

## Usage

Run the bot:
```bash
python whatsapp_bot.py
```

The bot will:
1. Listen for messages in the specified SQS queue
2. Mark received messages as read
3. Reply with a transcription of audio messages.
4. Delete processed messages from the queue

## Optional: Text-to-Speech (Hebrew text -> voice note)

Off by default. When enabled, a plain TEXT message is answered with a synthesized
voice note (via the BlueTTS Hugging Face Space), alongside the existing speech-to-text
transcription. It needs `ffmpeg` on the host (the transcription paths already use it) and
`gradio_client` (in `requirements.txt`). Set in `.env`:

- `TTS_ENABLED`: `1` to enable (default `0` = off).
- `HF_TOKEN`: a Hugging Face token. Recommended: it raises the Space config-fetch rate limit.
- `HF_TTS_SPACE`: the Space id (default `notmax123/BlueV2`).
- `TTS_VOICE`: the voice name (default `Male1`).
- `TTS_MAX_CHARS`: how long ONE voice note is, in characters (default `2800`, ~3.5 min of speech). Long text is split into several notes at sentence/paragraph bounds.
- `TTS_MAX_CHUNKS`: the most voice notes made for one message (default `5`). Past `TTS_MAX_CHARS * TTS_MAX_CHUNKS` (~14,000 chars) the text is refused with a short reply.

**Languages:** only Hebrew and English are routed today. The bot detects the script of the
text: Hebrew or Latin letters are read aloud (`he` / `en`, mixed Hebrew+English reads as
Hebrew), while text with no readable words (emoji / numbers only) or a script we do not read
gets a short honest reply instead. As a result **Russian/Cyrillic is rejected outright**, and
**German/Spanish/Italian collapse to English** (their Latin letters make the language `en`).
Real per-language TTS support is a future enhancement.

The Space is paid but has a fixed number of concurrent slots. To keep bursts from queueing,
overflow work can spill to a dedicated RunPod GPU endpoint. This is optional: with the RunPod
variables unset, the bot uses the Space only (exactly as before).

- `SYNTH_BACKEND`: `hf` (default) uses the Space first and spills to RunPod when full; `runpod` sends every synth straight to RunPod.
- `TTS_RUNPOD_ENDPOINT_ID`: the RunPod Serverless endpoint id for TTS (SEPARATE from the speech-to-text `RUNPOD_ENDPOINT_ID`).
- `RUNPOD_API_KEY`: the RunPod API key (shared with the speech-to-text endpoint).
- `TTS_HF_CONCURRENCY`: how many syntheses may hit the Space at once before overflowing (default `6`).
- `TTS_OVERFLOW_WAIT`: seconds to wait for a free Space slot before spilling to RunPod (default `4`).
- `TTS_RUNPOD_CONCURRENCY`: max simultaneous RunPod synths (default `8`).
- `TTS_RUNPOD_DAILY_MAX`: max RunPod synths per day, resets at local midnight; `0` = no cap (default `500`).
- `TTS_SEND_GAP`: seconds between the several voice-note sends of one long message, so rapid replies do not burst the WhatsApp API into throttling (default `0.35`).
- `TTS_SEND_RETRIES`: how many times one voice-note send is retried on a transient failure (timeout / 429 / 5xx); a 4xx is never retried (default `3`).

TTS runs under its own concurrency limit, so it never slows down or starves the transcription
workers. A repeated queue delivery of the same message is de-duplicated so a user never gets two
voice notes for one text. A first-time user gets a one-line welcome; every request gets a rotated
"one moment" reply before the voice note (a part-count reply when the text is split into several).
Rate limiting is work-based: one message-token is charged per voice note actually sent, so a long
multi-note message costs proportionally more. If synthesis is busy or fails, the user gets a short
"try again shortly" message, and the transcription path is untouched either way.

## Error Handling

The bot includes error handling for:
- SQS connection issues
- WhatsApp API errors
- Message processing errors

Errors are logged to the console but won't stop the bot from running.
