#!/bin/bash
# RunPod transcription mode, engaged only when the queue backlog exceeds 50.
exec python3 -u whatsapp_bot.py --overflow-handler 50 --num-workers 10
