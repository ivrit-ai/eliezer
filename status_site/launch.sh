#!/bin/bash
set -e
# Schema init runs here, not in install.sh: install.sh is a build step with no
# DATABASE_URL. Both calls are idempotent, so re-running them on every boot is fine.
python3 -c "import app; app.init_db(); app.init_queue_db()"
# One worker: the per-minute series cache and the queue sweeper live in this process.
# Access logs off, as under waitress: the edges post and lease continuously.
exec uvicorn app:app --host 0.0.0.0 --port "$PORT" --workers 1 \
  --proxy-headers --forwarded-allow-ips='*' --no-access-log
