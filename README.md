# Eliezer

Transcribes WhatsApp voice notes. Two parts:

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
