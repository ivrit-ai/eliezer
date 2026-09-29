# WhatsApp Bot

A simple bot that processes WhatsApp messages from a message queue and responds to them.
The queue is the Eliezer Status site's (`status_site/`), which receives the WhatsApp
webhook itself; AWS SQS remains supported only until every instance has moved over.

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
- `QUEUE_API_URL`: The status site's URL (unset: fall back to SQS via `APP_SQS_QUEUE`)
- `QUEUE_TOKEN`: This instance's token, from the site's `EDGE_TOKENS`
- `WHATSAPP_API_TOKEN`: Your WhatsApp Business API token
- `WHATSAPP_PHONE_NUMBER_ID`: Your WhatsApp phone number ID

## Usage

Run the bot:
```bash
python whatsapp_bot.py
```

The bot will:
1. Lease messages from the queue
2. Mark received messages as read
3. Reply with a transcription of audio messages.
4. Acknowledge processed messages, removing them from the queue

## Error Handling

The bot includes error handling for:
- Queue connection issues
- WhatsApp API errors
- Message processing errors

Errors are logged to the console but won't stop the bot from running.
