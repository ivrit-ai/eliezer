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

## Error Handling

The bot includes error handling for:
- SQS connection issues
- WhatsApp API errors
- Message processing errors

Errors are logged to the console but won't stop the bot from running.
