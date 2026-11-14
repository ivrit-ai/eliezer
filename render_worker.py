"""Render worker: burns a video's subtitles into it, so the user gets back a video they
can watch (and forward) with the transcript on screen.

Leases jobs from the status site's "render" lane - queued after a video's transcript,
when the site runs with RENDER_ENABLED - fetches the video through the site like an
edge does, re-encodes it with the site's subtitles drawn in (ffmpeg + libass; needs a
font with Hebrew, e.g. Debian's fonts-noto-core), and hands the mp4 back. The site
sends it. CPU-bound and separate from the transcription edges, so a busy render queue
never slows transcription. A video it gives up on (or that no worker gets to in time)
is answered with the .srt instead.

    python render_worker.py              # QUEUE_API_URL and QUEUE_TOKEN as for an edge
"""

import argparse
import json
import logging
import os
import subprocess
import tempfile
import time

from dotenv import load_dotenv

from health import start_health_server
from queue_client import MediaTooLarge, StaleJob, make_queue_client

load_dotenv()

log = logging.getLogger("render_worker")

RENDER = "render"
SUB_FONT = os.getenv("SUB_FONT", "Noto Sans Hebrew")
MAX_HEIGHT = int(os.getenv("RENDER_MAX_HEIGHT", "720"))
# Hard caps on what this worker takes on, whatever the site lets through.
MAX_INPUT_BYTES = int(float(os.getenv("RENDER_MAX_INPUT_MB", "50")) * 1024 * 1024)
MAX_SECONDS = float(os.getenv("RENDER_MAX_SECONDS", "600"))
# Under the site's 50MB (the Bot API's upload limit for bots), with room to spare.
MAX_OUTPUT_BYTES = int(float(os.getenv("RENDER_MAX_OUTPUT_MB", "48")) * 1024 * 1024)
# All of a video's encoding (the burn-in, and compressing if needed) must finish within
# this, or the worker gives up on it. Keep it well under the hub's RENDER_LEASE_SECONDS
# (an hour by default), which also has to cover the download and the upload.
BUDGET_SECONDS = float(os.getenv("RENDER_BUDGET_SECONDS", "1800"))
PROBE_TIMEOUT = 60
# ffmpeg draws SRT on a 384x288 ASS canvas: font sizes are in its units.
ASS_PLAY_RES_Y = 288
# Average Hebrew glyph width, in ems, for sizing text to the frame.
EM_PER_CHAR = 0.40
# Characters a subtitle line holds before libass wraps it: fewer on a portrait (phone)
# video, so the text can be large there too.
LINE_CHARS = 42
PORTRAIT_LINE_CHARS = 22


class RenderError(Exception):
    """This video will never render (no picture, undecodable, too long or too large)."""


def _run(cmd, cwd=None, deadline=None):
    """Run an ffmpeg tool; past the deadline (time.monotonic()), give up on the video."""
    timeout = PROBE_TIMEOUT if deadline is None else deadline - time.monotonic()
    if timeout <= 0:
        raise RenderError(f"over the {BUDGET_SECONDS:g}s time budget")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RenderError(f"{cmd[0]} ran out of time")
    if result.returncode != 0:
        raise RenderError(f"{cmd[0]}: {result.stderr.strip()[-300:]}")
    return result.stdout


def probe(path):
    data = json.loads(_run(["ffprobe", "-v", "error", "-show_entries",
                            "format=duration:stream=codec_type,width,height:stream_side_data=rotation",
                            "-of", "json", path]))
    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    if video is None:
        raise RenderError("no video stream")
    width, height = video.get("width") or 0, video.get("height") or 0
    for side in video.get("side_data_list") or []:
        if abs(int(side.get("rotation") or 0)) in (90, 270):
            width, height = height, width
    return {"duration": float(data.get("format", {}).get("duration") or 0),
            "width": width, "height": height,
            "has_audio": any(s.get("codec_type") == "audio" for s in streams)}


def output_size(width, height):
    """Even dimensions, at most MAX_HEIGHT high."""
    if height > MAX_HEIGHT:
        width, height = width * MAX_HEIGHT / height, MAX_HEIGHT
    return int(width) // 2 * 2, int(height) // 2 * 2


def longest_cue(srt_text):
    """Characters in the SRT's longest cue (a cue is one line, between RLM marks)."""
    return max((len(line.strip("‏ ")) for line in srt_text.splitlines()
                if line.strip() and "-->" not in line and not line.isdigit()), default=0)


def subtitle_style(width, height, longest=0):
    """Text as large as fits: a line of LINE_CHARS (PORTRAIT_LINE_CHARS on a portrait
    video) spans most of the frame's width; libass wraps longer cues. Smaller when
    needed, so even the longest cue takes at most two lines."""
    chars = max(PORTRAIT_LINE_CHARS if height > width else LINE_CHARS, longest // 2 + 3)
    px = min(0.92 * width / (chars * EM_PER_CHAR), height / 12)
    size = max(6, round(px * ASS_PLAY_RES_Y / height, 1))
    return (f"FontName={SUB_FONT},FontSize={size},PrimaryColour=&H00FFFFFF,BackColour=&H80000000,"
            "BorderStyle=3,Outline=1,Shadow=0,MarginV=16,Alignment=2")


def burn(src, srt_name, dst, workdir, info, deadline, longest=0):
    """Re-encode with the subtitles drawn in. The .srt is named relative to workdir:
    the subtitles filter's own escaping rules make absolute paths fragile."""
    w, h = output_size(info["width"], info["height"])
    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", os.path.abspath(src),
           "-vf", f"scale={w}:{h},subtitles={srt_name}:force_style='{subtitle_style(w, h, longest)}'",
           "-c:v", "libx264", "-crf", "26", "-preset", "veryfast", "-pix_fmt", "yuv420p"]
    cmd += ["-c:a", "aac", "-b:a", "96k"] if info["has_audio"] else ["-an"]
    _run(cmd + ["-movflags", "+faststart", os.path.abspath(dst)], cwd=workdir, deadline=deadline)


def compress(src, dst, target_bytes, workdir, deadline):
    """Two-pass x264 aimed at target_bytes, down to 480p when the bitrate gets low."""
    info = probe(src)
    video_kbps = int(target_bytes * 8 / 1000 / max(info["duration"], 1) - 96)
    if video_kbps < 150:
        raise RenderError("too long to fit the upload limit")
    w, h = info["width"], info["height"]
    if video_kbps < 800 and h > 480:
        w, h = int(w * 480 / h) // 2 * 2, 480
    common = ["-vf", f"scale={w}:{h}", "-c:v", "libx264", "-b:v", f"{video_kbps}k", "-preset", "veryfast",
              "-pix_fmt", "yuv420p", "-passlogfile", os.path.join(workdir, "pass")]
    _run(["ffmpeg", "-y", "-loglevel", "error", "-i", src, *common, "-pass", "1", "-an", "-f", "mp4", os.devnull],
         deadline=deadline)
    _run(["ffmpeg", "-y", "-loglevel", "error", "-i", src, *common, "-pass", "2",
          "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", dst], deadline=deadline)


def render(src, srt_text, workdir):
    """The subtitled mp4 for src, and its (width, height, duration)."""
    deadline = time.monotonic() + BUDGET_SECONDS
    info = probe(src)
    if info["duration"] > MAX_SECONDS:
        raise RenderError(f"longer than {MAX_SECONDS:g}s")
    with open(os.path.join(workdir, "subs.srt"), "w", encoding="utf-8") as f:
        f.write(srt_text)
    out = os.path.join(workdir, "out.mp4")
    burn(src, "subs.srt", out, workdir, info, deadline, longest_cue(srt_text))
    if os.path.getsize(out) > MAX_OUTPUT_BYTES:
        small = os.path.join(workdir, "small.mp4")
        compress(out, small, MAX_OUTPUT_BYTES * 0.95, workdir, deadline)
        out = small
        if os.path.getsize(out) > MAX_OUTPUT_BYTES:
            raise RenderError("still too large after compression")
    final = probe(out)
    return out, final["width"], final["height"], int(round(final["duration"]))


def process(queue, job):
    handle = job["handle"]
    label = handle[:8]
    with tempfile.TemporaryDirectory(prefix="render-") as workdir:
        src = os.path.join(workdir, "input")
        try:
            queue.fetch_media(handle, src, max_bytes=MAX_INPUT_BYTES)
            started = time.time()
            out, w, h, duration = render(src, job.get("srt") or "", workdir)
        except (RenderError, MediaTooLarge) as e:
            # It will not work next time either: give up, and the site sends the .srt.
            log.info("job %s will not render: %s", label, e)
            queue.render_failed(handle, str(e)[:200])
            return
        queue.rendered(handle, out, w, h, duration)
        log.info("job %s rendered in %.1fs (%dx%d, %ds, %.1f MB)", label, time.time() - started,
                 w, h, duration, os.path.getsize(out) / 1024 / 1024)


def run(queue, stop=None):
    log.info("render worker started")
    while not (stop and stop.is_set()):
        try:
            jobs = queue.lease(1, 20, lane=RENDER)
        except Exception as e:
            log.warning("lease failed: %s", e)
            time.sleep(5)
            continue
        for job in jobs:
            try:
                process(queue, job)
            except StaleJob:
                log.info("job %s is no longer ours; dropping it", job["handle"][:8])
            except Exception as e:
                # Hand it back: retried soon, here or by another worker, up to the
                # site's receive limit (after which the site sends the .srt).
                log.warning("job %s failed: %s", job["handle"][:8], e)
                try:
                    queue.release(job["handle"])
                except Exception:
                    pass


if __name__ == "__main__":
    argparse.ArgumentParser(description="Eliezer render worker: burns subtitles into videos").parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
    # Bound first, so xhost's startup check never races the worker (as for the edge).
    if os.getenv("PORT"):
        start_health_server(int(os.getenv("PORT")))
    run(make_queue_client())
