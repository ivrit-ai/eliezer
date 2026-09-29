import base64
import json
import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

import outbox
import stats
import telegram
import whatsapp
from ingest import webhooks
from queue_api import (
    init_queue_db, install_shutdown_hook, queue_api, queue_depth, require_edge, start_sweeper,
)
from stats import init_db, reset_db  # noqa: F401 - launch.sh calls app.init_db()

# 32x32 ivrit.ai favicon (PNG), embedded so it can be served without a binary asset.
FAVICON_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAEyUlEQVR42q2WzWsTXRTGfzOTJmnzoVBo"
    "SlVKoXZnuzBQrBUEQbpRuhDq0n/Bf8CtC9e6dFXElQoKL7qwFUUQJVFcuDCUtEVs0khjSm2TmXufd6EZ"
    "GtukNnogZDhz5pznfNznHgAkub/+X+mnBGojxhgdJEEQ7GfX9Pnfr1gegMsBYowB4NGjR+TzeVzXJQiC"
    "fW0eP37M/fv38TwP13VD/YHSrgLWWllrZYzRyMiI+vv79fr16zDL36syMTEhQNeuXdPy8vLvFdu3Ah0B"
    "NIPcvXtXrusKUCKR0KtXr0LnTZv5+XkBisViApTJZLSwsLAbRHctAHjz5g3WWuLxOFtbW8zNzVGtVnEc"
    "B0lIolwuk0qlqNfrRKNRSqUSs7OzfP36FcdxOrejUwuMMdre3talS5daMrxz544kyfd9WWslSYVCQdls"
    "tsVufn5ektRoNA7fgiYISarVahoeHpbrunIcRzMzMy2z4Pu+JGlpaUmJREKe58lxnBBoOwAHtsBxHHzf"
    "J5VKcf36day1SOLdu3d8//4dz/OQRCQSIQgCRkZGyGazGGOQRCwW6+j/j2agGeTKlSv09fUBUKlUyOVy"
    "AFhrQ7CSSKfT4bdHjhz5ewCu6yKJY8eOcebMmVC/uLjYbGEIwHEcGo1GaDMwMBC+6xpAM0tJXLx4MdS9"
    "ePEiBLhbdnZ2AIhGowwNDf0bAM3szp07F+ry+TyVSgXXdbHWhkA2NzcByGQy/w5A0/mpU6cYHBwEoFar"
    "8fbt25Y58H2fWq0GwOjoKL29vS3g/qoCxhiSySTZbDbUP3/+vGUOqtUqGxsbAJw+fboF3F8B2B3k/Pnz"
    "ewaxmWGpVKJarQJw9uzZjuU/NIBmkMuXL9PT04PjOHz8+JFisYjn/SS2paUljDGk0+nwxLQrf1cAjDGc"
    "PHmSq1evIol6vc7i4iLWWowxvH//HsdxmJ6eJpPJYK3tWAG6WUiMMSoWi0omkwI0NzcXvp+enhage/fu"
    "nVB0V9fx7g3H9335vh8+S9KDBw8Ui8WUyWS0urqq27dvy/M8DQ8Pa2trK9wn/grALgctUqlUlM/nNTk5"
    "Kdd11d/fL0CAHj58uGdpaQcg8ifH7+XLlxQKBVZWVvj8+TOFQoFisUipVArtvn37xvHjx7l58yazs7NY"
    "a8PB7GolC4JA1lrlcrkws3a/iYkJ3bp1S+vr650W18NXwHEcbty4sUefTqcZHx/nwoULzMzMMDk5GU66"
    "MebPM28HIAgCIpEIz54948mTJwwNDTE2NkY2m2VqaopsNsuJEyf2fON53qGCtwXQJI7BwUHy+Tyjo6Mk"
    "k8k9rGiMwXEcXNclEonQjXQEMD4+3nId7w7oOM6ebJtUvXs5OYiKI4dhwU6Uul+gAxmwE4B6vc7Tp09"
    "Jp9PE43FisRjWWlKpFNZa1tfX6evrY3t7G9d1mZqaYmFhAUnE4/HwGo5Go4yNjbXdDdum1Gg0MMZw9OhR"
    "EokEpVKJXC5HsVgM9761tTU2Nzfp7e3ly5cvrK6u0tPTw8DAAOVymQ8fPvDp0ycikUjHK3lfHmhyviTV"
    "63UtLy+rUqmo0WiEB3ttbU0bGxv68eOHqtWqarVayAMrKysql8va2dlp6v4tFXch3VPx75vvflP/J/b7"
    "yf/Gfn489swlzwAAAABJRU5ErkJggg=="
)

# Nothing configures logging otherwise, so only WARNING+ would reach the runtime log
# via logging's last-resort handler. Diagnostics stay at DEBUG; set LOG_LEVEL=DEBUG to
# see them during a cutover, when knowing which payloads arrived is the whole game.
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
# httpx logs every request at INFO - one line per voice note fetched, media URL included.
logging.getLogger("httpx").setLevel(logging.WARNING)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


REQUIRED_SETTINGS = (
    "QUEUE_TOKEN", "META_APP_SECRET", "META_VERIFY_TOKEN", "WHATSAPP_API_TOKEN",
    "WHATSAPP_PHONE_NUMBER_ID", "STATUS_USER_SALT", "POSTHOG_API_KEY", "TELEGRAM_BOT_TOKEN",
)

# Where Telegram delivers updates; the public name, not whichever host serves this.
TELEGRAM_WEBHOOK_URL = os.environ.get(
    "TELEGRAM_WEBHOOK_URL", "https://status.eliezer.ivrit.ai/webhook/telegram"
)


def _register_channels(stop):
    """Look up the bot's WhatsApp number and point Telegram at this site, retrying until
    both work: links in the link flow need them, but serving must not wait for them."""
    pending = {"whatsapp": whatsapp.setup, "telegram": lambda: telegram.setup(TELEGRAM_WEBHOOK_URL)}
    delay = 5
    while pending and not stop.is_set():
        for name, setup in list(pending.items()):
            try:
                setup()
                del pending[name]
            except Exception as e:
                logging.getLogger(__name__).warning("%s setup failed, retrying: %s", name, e)
        if pending:
            stop.wait(delay)
            delay = min(delay * 2, 300)


@asynccontextmanager
async def lifespan(app):
    missing = [k for k in REQUIRED_SETTINGS if not os.environ.get(k)]
    if missing:
        # Say so once, loudly, rather than failing quietly on every message.
        logging.getLogger(__name__).warning("missing settings: %s", ", ".join(missing))
    install_shutdown_hook()
    stop_sweeper = start_sweeper()
    stop_senders = outbox.start_senders()
    stop_setup = threading.Event()
    threading.Thread(target=_register_channels, args=(stop_setup,), name="ChannelSetup", daemon=True).start()
    yield
    stop_setup.set()
    stop_senders.set()
    stop_sweeper.set()
    await whatsapp.close()
    await telegram.close()


# No generated docs: the queue and webhook endpoints are not meant to be browsable.
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
app.include_router(queue_api)
app.include_router(webhooks)

INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")


@app.post("/api/events")
async def ingest(request: Request):
    """Events and heartbeats from edges that still report their own statistics."""
    if not INGEST_TOKEN or request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    # Like Flask's get_json(silent=True): a body that isn't a JSON object reads as empty.
    try:
        body = json.loads(await request.body())
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = {}
    # uvicorn runs with --proxy-headers, so this is the caller's address, not the proxy's.
    client_ip = request.client.host if request.client else None
    ingested = await run_in_threadpool(stats.ingest_events, body, client_ip)
    if ingested is None:
        return JSONResponse({"error": "instance_id required"}, status_code=400)
    return {"ok": True, "ingested": ingested}


@app.get("/api/stats")
def api_stats():
    return JSONResponse(stats.compute_stats(queue_depth()))


@app.get("/queue/edges")
def edges(caller: str = Depends(require_edge)):
    """Every instance seen in the last day, with the address it reports from. Behind the
    queue token: which machines run the fleet is operator information, unlike /api/stats."""
    with stats.connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT instance_id, last_ip, uptime_seconds,
                   extract(epoch FROM (now() - last_seen)) AS age_seconds
            FROM instances
            WHERE last_seen > now() - interval '24 hours'
            ORDER BY instance_id;
            """
        )
        rows = cur.fetchall()
    return {
        "edges": [
            {
                "instance_id": r["instance_id"],
                "ip": r["last_ip"],
                "uptime_seconds": r["uptime_seconds"],
                "age_seconds": round(float(r["age_seconds"]), 1),
            }
            for r in rows
        ]
    }


# HEAD is listed explicitly: Flask answered it on every GET route, Starlette does not.
@app.api_route("/favicon.ico", methods=["GET", "HEAD"])
def favicon():
    return Response(FAVICON_PNG, media_type="image/png")


@app.api_route("/", methods=["GET", "HEAD"])
def index():
    # The page is static HTML that fetches /api/stats client-side; nothing to render.
    return FileResponse(os.path.join(BASE_DIR, "templates", "index.html"))


if __name__ == "__main__":
    import uvicorn

    init_db()
    init_queue_db()
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
