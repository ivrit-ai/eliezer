# Eliezer

Transcribes WhatsApp voice notes. Two parts:

- **The status site** (`status_site/`, deployed as `eliezer-status` on xhostd) is the hub.
  It receives the WhatsApp webhook, queues voice notes, answers everything that isn't a
  transcription itself, and sends every reply. It holds all platform credentials and the
  per-user limits, and serves the fleet dashboard at https://status.eliezer.ivrit.ai.
- **Edges** (`whatsapp_bot.py`) only transcribe: they lease jobs from the hub, fetch the
  audio through it, and hand back the text. Run as many as needed, anywhere.

## Running an edge

```bash
pip install -r requirements.txt   # plus ffmpeg/ffprobe on the PATH
cp .env.example .env              # fill in the queue URL and token
python whatsapp_bot.py --local    # faster-whisper on this machine's GPU
python whatsapp_bot.py            # or transcribe on RunPod (needs RUNPOD_* in .env)
```

`--num-workers N` sets concurrent transcriptions (default 1 with `--local`, else 10).
`--overflow-handler N` makes the edge take work only while more than N jobs are waiting.
