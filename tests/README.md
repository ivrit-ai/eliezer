# End-to-end tests

Each suite starts a throwaway Postgres in Docker and the real hub (`status_site/`)
under uvicorn. The WhatsApp and Telegram suites also run a real edge (`whatsapp_bot.py`)
with only the model call stubbed, against fake WhatsApp Graph and Telegram Bot APIs,
using real audio generated with ffmpeg.

| Suite | Covers |
|---|---|
| `test_queue.py` | Queue mechanics: signed webhooks, dedup, batch splitting, expiry and the sweeper, poison messages, the overflow threshold, edge auth, long-polls, prompt SIGTERM. |
| `test_whatsapp.py` | The WhatsApp path end to end: media through the hub, admission (10-minute cap, fleet-wide rate limits), transcripts-only WhatsApp replies, the pricing notice, long replies, send retries and resumption, safe completion retries, statistics. |
| `test_telegram.py` | Telegram, linking and admin: link flow, delivery to Telegram, the reply policy and allowlist, unlink and blocked bots, groups and `/transcribe`, group and sent-message counters, recovery from media failures. |

## Running

Needs Docker, `ffmpeg`/`ffprobe`, and the hub's and edge's dependencies:

```bash
pip install -r status_site/requirements.txt -r requirements.txt
tests/run_all.sh          # or: python tests/test_telegram.py
```

Each prints one PASS/FAIL line per check and exits non-zero on any failure. Logs go to
a temporary directory (`eliezer-test-*`), which is left behind for inspection.
