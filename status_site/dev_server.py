"""Run the status page locally with mock statistics: no database, no credentials.

    pip install fastapi uvicorn
    python status_site/dev_server.py        # http://localhost:8000 (PORT=... to change)

Serves the real index.html and the real /api/faq; only /api/stats is made up.
"""

import math
import os
import random
import time

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

from faq import FAQ

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


def _series(n, step, base):
    now = int(time.time()) // step * step
    return [
        {"t": now - (n - 1 - i) * step, "count": max(0, int(base * (1 + math.sin(i / 6)) + random.randint(0, 5)))}
        for i in range(n)
    ]


@app.get("/api/stats")
def stats():
    minutes, hours = _series(60, 60, 12), _series(168, 3600, 600)
    return JSONResponse({
        "totals": {
            "messages": 1234567, "transcriptions": 1200345, "duration_seconds": 8765432,
            "dropped": 321, "wa_sent": 900000, "tg_sent": 200000, "nt_sent": 100000,
            "wa_told": 4321, "dropped_unlinked": 6543,
        },
        "messages": {"last_1m": 14, "last_5m": 70, "last_1h": 800, "last_24h": 19000, "per_min_1h": 13.3},
        "transcriptions_1h": {"count": 790, "avg_duration": 6.2, "median_duration": 5.1, "p95_duration": 14.8},
        "unique_users_24h": 8123, "linked_users": 5400, "linked_telegram": 3000, "linked_notifier": 2400,
        "notifier_url": None, "telegram_groups": 42, "queue_depth": 3,
        "instances": [
            {"instance_id": "edge-1", "uptime_seconds": 86400, "age_seconds": 2.1},
            {"instance_id": "edge-2", "uptime_seconds": 3600, "age_seconds": 4.5},
        ],
        "live_instances": 2,
        "messages_series_1h": minutes,
        "messages_series_1h_by_instance": [{"instance_id": "edge-1", "counts": [m["count"] for m in minutes]}],
        "messages_series_1w": hours,
        "messages_series_1w_by_instance": [{"instance_id": "edge-1", "counts": [h["count"] for h in hours]}],
    })


@app.get("/api/faq")
def api_faq():
    return JSONResponse(FAQ)


@app.get("/")
def index():
    return FileResponse(os.path.join(BASE_DIR, "templates", "index.html"))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
