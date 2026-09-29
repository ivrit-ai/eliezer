#!/bin/bash
set -e
python3 -c "import app; app.init_db()"
# One worker: the per-minute series cache lives in this process, so a second worker
# would serve a partial graph. Access logs off, as under waitress: the edges post
# every few seconds and the dashboard polls every 5s.
exec uvicorn app:app --host 0.0.0.0 --port "$PORT" --workers 1 \
  --proxy-headers --forwarded-allow-ips='*' --no-access-log
