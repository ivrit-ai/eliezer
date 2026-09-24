"""RunPod Serverless synth backend -- a drop-in replacement for tts.synthesize.

    POST https://api.runpod.ai/v2/{ENDPOINT_ID}/runsync
    Authorization: Bearer <RUNPOD_API_KEY>
    {"input": {"prompt": "...", "voice": "Male1", "lang": "he", "steps": 16}}

Same contract as tts.synthesize(text, out_path, voice=, lang=) -> OGG path, so the
capacity router in tts.py can spill a synth here with nothing else changing.

This uses a SEPARATE TTS endpoint (TTS_RUNPOD_ENDPOINT_ID), distinct from Eliezer's
speech-to-text RunPod endpoint (RUNPOD_ENDPOINT_ID). If it is not configured, tts.py
never calls this and the bot runs on the HF Space only.

Design notes (lessons already paid for on the HF path):
- ONE shared, connection-pooled HTTP session, built once and reused. Building a client per
  request is what rate-limited us on Hugging Face and failed most live synths.
- `<en>...</en>` tagging for English inside Hebrew is reused from tts (with lang=he the
  Hebrew G2P garbles untagged English).
- The handler's audio output shape is discovered at runtime (base64 or URL) -- see _extract_audio.
"""
import base64
import binascii
import logging
import os
import tempfile
import threading
import time

import requests

from tts import SynthError, _tag_english, _to_ogg

logger = logging.getLogger(__name__)

# Rough $/GPU-second for this endpoint's cards (A4000 $0.17/hr, A4500 $0.19/hr list).
# Only used to turn billed seconds into a number worth reading in the logs.
GPU_SEC_USD = float(os.getenv("RUNPOD_GPU_SEC_USD", "0.00005"))
_totals = {"jobs": 0, "billed_s": 0.0, "delay_s": 0.0, "audio_s": 0.0, "cold": 0}

API_BASE = os.getenv("RUNPOD_API_BASE", "https://api.runpod.ai/v2")
# SEPARATE TTS endpoint -- distinct from Eliezer's speech-to-text RUNPOD_ENDPOINT_ID.
ENDPOINT_ID = os.getenv("TTS_RUNPOD_ENDPOINT_ID", "")
API_KEY = os.getenv("RUNPOD_API_KEY", "")
# /runsync holds the connection until the job finishes. Our synths are ~4s; allow headroom
# for a cold worker + a long paragraph, but stay well under the bot's own request budget.
TIMEOUT = float(os.getenv("RUNPOD_TIMEOUT", "180"))

_SESSION = None
_LOCK = threading.Lock()


def _log_metrics(body, chars):
    # Log what every job actually cost and where the time went, so the endpoint can be
    # tuned later from real data instead of guesses. Also keeps a running total per process.
    o = body.get("output") or {}
    delay = (body.get("delayTime") or 0) / 1000.0
    billed = (body.get("executionTime") or 0) / 1000.0
    audio = float(o.get("duration_s") or 0)
    cold = delay > 3.0          # warm dispatch measured ~0.1s; >3s means a cold worker
    _totals["jobs"] += 1
    _totals["billed_s"] += billed
    _totals["delay_s"] += delay
    _totals["audio_s"] += audio
    _totals["cold"] += int(cold)
    logger.info(
        f"runpod job chars={chars} delay={delay:.2f}s{' COLD' if cold else ''} "
        f"billed={billed:.2f}s infer={float(o.get('inference_s') or 0):.2f}s "
        f"encode={float(o.get('encode_s') or 0):.2f}s audio={audio:.1f}s "
        f"rtf={float(o.get('rtf') or 0):.3f} cost=${billed * GPU_SEC_USD:.5f} | "
        f"totals jobs={_totals['jobs']} cold={_totals['cold']} "
        f"billed={_totals['billed_s']:.1f}s audio={_totals['audio_s']:.0f}s "
        f"cost=${_totals['billed_s'] * GPU_SEC_USD:.4f}")


def get_client():
    """One shared, pooled HTTP session, reused across calls (never build one per request).

    Built INSIDE the lock so a cold-start burst makes exactly one session, not many.
    """
    global _SESSION
    with _LOCK:
        if _SESSION is None:
            if not (ENDPOINT_ID and API_KEY):
                raise SynthError("TTS_RUNPOD_ENDPOINT_ID / RUNPOD_API_KEY not configured")
            s = requests.Session()
            s.headers.update({"Authorization": f"Bearer {API_KEY}",
                              "Content-Type": "application/json"})
            # pool sized to the bot's GPU concurrency cap so bursts reuse connections
            adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16)
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            _SESSION = s
        return _SESSION


def _extract_audio(output):
    # Return (raw_bytes, url) from the handler's `output`, whatever shape it uses.
    # Handles: a bare base64 string, a URL string, or a dict under common key names.
    if output is None:
        return None, None
    if isinstance(output, list) and output:
        output = output[0]
    if isinstance(output, str):
        if output.startswith(("http://", "https://")):
            return None, output
        return _b64(output), None
    if isinstance(output, dict):
        for k in ("audio_base64", "audio_b64", "base64", "audio", "wav", "data", "output"):
            v = output.get(k)
            if isinstance(v, str) and v:
                if v.startswith(("http://", "https://")):
                    return None, v
                b = _b64(v)
                if b:
                    return b, None
        for k in ("url", "audio_url", "file_url", "s3_url"):
            v = output.get(k)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                return None, v
    return None, None


def _b64(s):
    if s.startswith("data:"):          # data:audio/wav;base64,....
        s = s.split(",", 1)[-1]
    try:
        raw = base64.b64decode(s, validate=False)
    except (binascii.Error, ValueError):
        return None
    return raw if len(raw) > 128 else None   # too small to be audio


def synthesize(text, out_path, voice="Male1", lang="he", steps=16, speed=0.95,
               attempts=3, delay=3.0):
    """Synthesize `text` -> WhatsApp voice-note OGG at `out_path`. Raises SynthError.

    Safe to call concurrently (shared pooled session; RunPod scales workers).
    """
    if not text or not text.strip():
        raise SynthError("empty text")
    if lang == "he":
        text = _tag_english(text)   # keep English intelligible inside Hebrew

    payload = {"input": {"prompt": text, "voice": voice, "lang": lang,
                         "steps": steps, "speed": speed}}
    url = f"{API_BASE}/{ENDPOINT_ID}/runsync"
    c = get_client()
    last = None
    for i in range(attempts):
        try:
            r = c.post(url, json=payload, timeout=TIMEOUT)
            if r.status_code >= 500 or r.status_code == 429:
                raise SynthError(f"runpod HTTP {r.status_code}: {r.text[:200]}")
            if r.status_code >= 400:   # 4xx = our fault; do not retry
                raise SynthError(f"runpod HTTP {r.status_code} (not retrying): {r.text[:300]}")
            body = r.json()
            status = str(body.get("status") or "").upper()
            if status in ("FAILED", "CANCELLED", "TIMED_OUT"):
                raise SynthError(f"runpod job {status}: {str(body.get('error'))[:200]}")
            _log_metrics(body, len(text))
            out = body.get("output")
            raw, audio_url = _extract_audio(out)
            if not raw and not audio_url:
                raise SynthError(f"no audio in runpod output: {str(body)[:300]}")
            # The worker now returns OGG/Opus already. Re-running ffmpeg would decode and
            # RE-ENCODE a lossy stream -- audible quality loss for no benefit -- so write
            # it straight out. Only transcode when the worker hands us WAV.
            fmt = str((out or {}).get("format", "")).lower() if isinstance(out, dict) else ""
            if raw and fmt in ("ogg", "opus", "oga"):
                with open(out_path, "wb") as f:
                    f.write(raw)
                if os.path.getsize(out_path) == 0:
                    raise SynthError("runpod returned an empty ogg")
                return out_path
            if raw:
                src = os.path.join(tempfile.gettempdir(),
                                   f"rp_{os.path.basename(out_path)}.wav")
                with open(src, "wb") as f:
                    f.write(raw)
                try:
                    return _to_ogg(src, out_path)
                finally:
                    try:
                        os.remove(src)
                    except OSError:
                        pass
            return _to_ogg(audio_url, out_path)   # ffmpeg fetches the URL directly
        except SynthError as e:
            if "not retrying" in str(e):
                raise
            last = e
        except Exception as e:  # network/timeouts -> retry
            last = e
        if i < attempts - 1:
            time.sleep(delay * (2 ** i))    # 3s, 6s
    logger.error(f"runpod synth failed after {attempts} tries: {last!r}")
    print(f"runpod synth failed after {attempts} tries: {last!r}")
    raise SynthError(f"runpod synth failed after {attempts} tries: {last!r}")
