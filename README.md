# Eliezer

Transcribes WhatsApp voice notes, and Telegram voice notes and videos (a video also gets
its subtitles back, as an `.srt` after the transcript). Two parts:

- **The status site** (`status_site/`, deployed on xhostd) is the hub.
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

## Running a render worker (optional)

A video sent on Telegram gets its transcript and an `.srt`. With `RENDER_ENABLED=1` on
the hub, it instead gets its transcript and then the video back with the subtitles
burned in, made by a render worker: a separate, CPU-bound
process that leases the hub's `render` lane (transcription edges never see it), so
rendering never slows transcription. If no render worker delivers within
`RENDER_MAX_AGE_SECONDS` (default 30 minutes), or one gives up on the video, the user
gets the `.srt` after all.

```bash
# same .env as an edge (queue URL and token); needs ffmpeg with libass and a font
# with Hebrew - on Debian/Ubuntu: apt install ffmpeg fonts-noto-core
python render_worker.py
```

Output is at most 720p and under the Bot API's 50MB upload limit (compressed further
when needed). `SUB_FONT`, `RENDER_MAX_HEIGHT` and `RENDER_MAX_OUTPUT_MB` tune it.

Hard caps, so raising `TELEGRAM_MAX_FILE_MB` (the hub's refusal size, 20MB by default -
the public Bot API's limit) never means rendering anything that arrives: the hub queues
no render job for a video over `RENDER_MAX_INPUT_MB` (default 50), and the worker gives
up on one that downloads larger than that, runs longer than `RENDER_MAX_SECONDS`
(default 600), or takes longer than `RENDER_BUDGET_SECONDS` (default 1800) to encode.
Those get the `.srt` instead. The hub leases a render job for `RENDER_LEASE_SECONDS`
(default 3600, more than the budget plus the download and upload), so no second worker
takes a video that is still rendering.

## The status page FAQ

The "שאלות נפוצות" section at the bottom of the status page comes from
`status_site/faq.py`. Edit the `FAQ_TEXT` block there (a `q:` line, an `a:` line,
`---` between entries) and redeploy. A question with an empty `a:` is not shown until
someone writes the answer.

To work on the page locally with mock statistics (no database or credentials):

```bash
pip install fastapi uvicorn
python status_site/dev_server.py   # http://127.0.0.1:8000
```

## Deploying the status site to xhostd

The status site (`status_site/`) runs on xhostd as a standalone app. Within this repository, the site's files reside in the `status_site/` directory, while the xhostd app repository expects those files directly at its repository root (so `status_site/install.sh` and `status_site/launch.sh` serve as the root build and launch scripts).

To publish updates:

1. **Commit and push** your changes to this repository.
2. **Push the `status_site` subtree** to the xhostd git remote:
   ```bash
   git subtree push --prefix=status_site <xhostd-git-remote-url> master
   ```
   *(Alternatively, pull or clone the xhostd app repository, copy the updated files from `status_site/` into it, commit, and push to its `master` branch.)*
3. **Trigger the deployment** on xhostd:
   - Using the xhostd MCP / API: call `deploy(app_name="<app-name>", channel="prod", ref="master")`
   - Or trigger a deploy of `master` via the xhostd console.
4. **Verify the build and runtime logs** to confirm the container started and health checks pass.

## The Communicator app

Besides Telegram, the hub can deliver transcripts as push notifications through
[Communicator](https://communicator.ivrit.ai) (a Notifier instance), where Eliezer is
registered as a source. A user links it from the app, which shows a code: sending
`link <code>` to Eliezer on WhatsApp links it, and from then on their transcripts go to
the app instead of WhatsApp. The hub needs:

- `NOTIFIER_URL`: the instance, e.g. `https://communicator.ivrit.ai`.
- `NOTIFIER_SOURCE_KEY`: Eliezer's source key, from the instance's admin page.
- `NOTIFIER_PUBLIC_URL` (optional): where the status page sends people to open the app,
  if not `NOTIFIER_URL`.

Without the first two the channel is off.

## The ivrit.ai app

The ivrit.ai Android app (`ai.ivrit.app`) sends audio shared into it straight to the
hub, under the user's Google account, and fetches the result (`status_site/ivrit_app.py`):

- `POST /app/v1/jobs`: the file as the body (`Content-Type` audio/* or video/*, name in
  `X-Filename`), with `Authorization: Bearer <Google ID token>`. Answers 202 `{job_id}`.
- `GET /app/v1/jobs/{job_id}` and `GET /app/v1/jobs`: `queued`, `done` with `text`, or
  `failed` with `error` (`too_long`, `rate_limited` with `wait_minutes`,
  `duration_failed`, `unsupported`, `expired`). Kept three days.

Uploads are queued as source `app` and transcribed by the edges like any voice message,
under the same 10-minute cap and hourly limits, counted per Google account. Results go
to the app only, never to WhatsApp or Telegram. The hub needs:

- `APP_GOOGLE_CLIENT_IDS`: the app's OAuth client id(s) an ID token must be issued to.
  Without it the app's API answers 503.
- `APP_ORIGINS` (optional): where the app's pages are served from, for CORS; default
  `https://app.ivrit.ai`.
