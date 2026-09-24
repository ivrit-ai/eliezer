"""Hebrew text -> a WhatsApp voice note (OGG/Opus).

BlueTTS runs on a paid Hugging Face Space; we call it via gradio_client and ffmpeg-transcode
the result to Opus. Config from env:

    HF_TTS_SPACE   the Space to call     (default: notmax123/BlueV2)
    HF_TTS_API     the Space's api name  (default: /synthesize_text_stream)
    HF_TOKEN       a Hugging Face token  (recommended - see connect())
"""
import os
import re
import subprocess
import threading
import time

from gradio_client import Client

# Which Space to call, and the name of its synthesis endpoint.
SPACE = os.environ.get("HF_TTS_SPACE", "notmax123/BlueV2")
API_NAME = os.environ.get("HF_TTS_API", "/synthesize_text_stream")

# The model expects Hebrew. If we hand it plain English it mispronounces the words, so we
# wrap any run of English letters in <en>...</en> tags, which route those words to espeak.
_EN_RUN = re.compile(r"[A-Za-z][A-Za-z0-9 .,'&/\-]*[A-Za-z0-9]|[A-Za-z]")


def _tag_english(text):
    return _EN_RUN.sub(lambda m: f"<en>{m.group(0).strip()}</en>", text)


# How long we let ffmpeg run before giving up.
FFMPEG_TIMEOUT = float(os.environ.get("TTS_FFMPEG_TIMEOUT", "120"))

# How long we wait for one synthesis to finish. The Space can be slow or busy, so this
# stops a single request from tying up a worker thread forever.
SYNTH_TIMEOUT = float(os.environ.get("TTS_SYNTH_TIMEOUT", "120"))

# WhatsApp voice-note codec: Opus 32k, mono, 48kHz (WhatsApp only plays OGG/Opus mono).
_FFMPEG_OPUS = ["-c:a", "libopus", "-application", "audio", "-b:a", "32k", "-ar", "48000", "-ac", "1"]


class SynthError(Exception):
    pass


def _rm(path):
    """Delete a file if it exists. Used to clean up a half-written temp file."""
    try:
        if path and os.path.exists(path):
            os.unlink(path)
    except OSError:
        pass


def connect(attempts=3, delay=5.0):
    """Open a connection to the Space, retrying if it is briefly unavailable.

    Building a Client fetches the Space's config over the Hugging Face API. Without a
    token that request has a low rate limit, and once we hit it every synthesis fails, so
    pass a token if you have one. download_files=False means the audio comes back as a
    URL instead of being downloaded here; ffmpeg fetches it directly.
    """
    token = (os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
             or os.environ.get("HUGGINGFACE_TOKEN") or None)
    last = None
    for i in range(attempts):
        try:
            return Client(SPACE, token=token, verbose=False, download_files=False)
        except Exception as e:
            last = e
            if i < attempts - 1:
                time.sleep(delay * (3 ** i))  # wait 5s, then 15s, so we don't add to a rate limit
    raise SynthError(f"could not connect to {SPACE} after {attempts} tries: {last!r}")


# We keep and reuse a single client. Building a new one for every message re-fetches the
# Space config and quickly hits the rate limit, which used to make most syntheses fail.
_CLIENT = None
_CLIENT_LOCK = threading.Lock()
_LAST_CONNECT_FAIL = 0.0
_CONNECT_COOLDOWN = 90.0  # after a failed connect, don't try again for this many seconds


def get_client(force_new=False):
    """Return the shared client, creating it the first time it is needed.

    Connecting happens INSIDE the lock so a cold-start burst can't all call connect() at
    once and trip the Hugging Face config-API rate limit (429) - the exact thing this
    shared client exists to prevent.
    """
    global _CLIENT, _LAST_CONNECT_FAIL
    with _CLIENT_LOCK:
        if _CLIENT is not None and not force_new:
            return _CLIENT
        if time.time() - _LAST_CONNECT_FAIL < _CONNECT_COOLDOWN:
            raise SynthError("skipping connect: a recent one failed, waiting out the cooldown")
        try:
            _CLIENT = connect()
            return _CLIENT
        except Exception:
            _LAST_CONNECT_FAIL = time.time()
            raise


def _to_ogg(src, out_path):
    """Convert the Space's audio (a URL) into one WhatsApp voice-note file with ffmpeg.

    Handles a plain WAV and an HLS playlist (.m3u8); on any failure we delete the
    half-written output before raising.
    """
    # `file` is needed because RunPod can hand back a LOCAL .wav path to transcode (the HF
    # path is always a remote URL). Keep the network protocols too for the HF/HLS case.
    cmd = ["ffmpeg", "-y", "-protocol_whitelist", "file,http,https,tcp,tls,crypto"]
    if ".m3u8" in src.lower():
        cmd += ["-allowed_extensions", "ALL"]  # needed for the HLS playlist; harmless to skip for a WAV
    cmd += ["-i", src, *_FFMPEG_OPUS, out_path]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT)
    except subprocess.TimeoutExpired:
        _rm(out_path)
        raise SynthError(f"ffmpeg timed out after {FFMPEG_TIMEOUT}s") from None
    if r.returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        _rm(out_path)
        raise SynthError("ffmpeg could not make the OGG file: " + (r.stderr or "")[-400:])
    return out_path


def synthesize(text, out_path, voice="Male1", lang="he", steps=16, speed=0.95,
               client=None, attempts=4, delay=5.0):
    """Turn `text` into a voice-note OGG at `out_path` and return that path.

    Retries because the Space is sometimes slow or flaky, with a per-try time limit
    (SYNTH_TIMEOUT) so one call can't hang. BlueTTS chunks long text itself, so we do not.
    """
    if not text or not text.strip():
        raise SynthError("empty text")
    if lang == "he":
        text = _tag_english(text)  # keep any English words readable inside the Hebrew
    c = client or get_client()
    last = None
    for i in range(attempts):
        try:
            # On the last try, get a fresh client in case the shared one has gone stale.
            if i and i == attempts - 1 and client is None:
                c = get_client(force_new=True)
            # We use submit(...).result(timeout=...) rather than predict(...) so we can put
            # a time limit on the call. Without it, a busy Space could block for minutes.
            job = c.submit(text, voice, lang, steps, speed, None, api_name=API_NAME)
            res = job.result(timeout=SYNTH_TIMEOUT)
            audio = res[0] if isinstance(res, (list, tuple)) else res
            src = (audio.get("url") or audio.get("path")) if isinstance(audio, dict) else audio
            if not src:
                raise SynthError(f"no audio in the Space's response: {res!r}")
            return _to_ogg(str(src), out_path)
        except Exception as e:  # slow Space, timeout, or an ffmpeg blip: try again
            last = e
            time.sleep(delay)
    raise SynthError(f"synthesis failed after {attempts} tries: {last!r}")
