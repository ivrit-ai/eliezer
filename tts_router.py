"""Capacity routing for a synth: HF Space first, spill overflow to RunPod.

Picks the backend by how much capacity is free, not by config: it tries the paid HF
Space (tts.synthesize) and only spills to the paid RunPod GPU when the Space is full or
errors. With RunPod unconfigured this degrades to plain Space synth.
"""
import os
import threading
import time
import logging
from datetime import datetime

import tts

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Capacity routing: HF Space first, spill overflow to RunPod.
# ---------------------------------------------------------------------------
# The Space is paid but has a fixed number of concurrent slots. RunPod is also paid but
# scales on demand. So we prefer the Space, and only spill a synth to RunPod when the
# Space is FULL or errors. Under light traffic RunPod is never touched; under a burst the
# user does not have to wait. With RunPod unconfigured this degrades to Space-only.

SYNTH_BACKEND = os.environ.get("SYNTH_BACKEND", "hf").lower()

# How long a synth waits for a free Space slot before spilling to RunPod.
OVERFLOW_WAIT = float(os.environ.get("TTS_OVERFLOW_WAIT", "4"))

# How many syntheses may hit the paid Space at once. A router permit is held for the whole
# synth; when they are all taken, new work overflows to RunPod instead of queueing.
HF_CONCURRENCY = int(os.environ.get("TTS_HF_CONCURRENCY", "6"))

# Safety valve on the PAID RunPod GPU: cap both simultaneous calls and total calls per day
# so a burst (or a bug) can never spin up unbounded paid workers and drain the balance.
RUNPOD_CONCURRENCY = int(os.environ.get("TTS_RUNPOD_CONCURRENCY", "8"))
RUNPOD_DAILY_MAX = int(os.environ.get("TTS_RUNPOD_DAILY_MAX", "500"))  # 0 = no daily cap

_hf_sema = threading.Semaphore(max(1, HF_CONCURRENCY))
_rp_sema = threading.Semaphore(max(1, RUNPOD_CONCURRENCY))
_rp_lock = threading.Lock()
_rp_day = {"day": "", "count": 0}  # RunPod calls made today (daily budget kill-switch)

# runpod_client imports names from tts, so importing it here (not in tts) keeps the graph
# acyclic: tts <- runpod_client <- tts_router.
try:
    import runpod_client as _runpod
except Exception as e:
    _runpod = None
    logger.error(f"RunPod overflow client unavailable, staying Space-only: {e}")
    print(f"RunPod overflow client unavailable, staying Space-only: {e}")


class RunpodBudgetError(Exception):
    """RunPod's daily/concurrency budget is spent - refuse the paid call rather than overspend."""


def _runpod_ready():
    # RunPod overflow is available only when its client imported AND a separate TTS endpoint
    # + key are set (distinct from Eliezer's speech-to-text RUNPOD_ENDPOINT_ID).
    return bool(_runpod and os.environ.get("TTS_RUNPOD_ENDPOINT_ID")
                and os.environ.get("RUNPOD_API_KEY"))


def _rp_budget_ok():
    # True if we may make another RunPod call today. Resets at local midnight.
    if RUNPOD_DAILY_MAX <= 0:
        return True
    today = datetime.now().strftime("%Y-%m-%d")
    with _rp_lock:
        if _rp_day["day"] != today:
            _rp_day["day"], _rp_day["count"] = today, 0
        return _rp_day["count"] < RUNPOD_DAILY_MAX


def _run_on_runpod(text, out_path, voice, lang):
    # Synthesize one request on PAID RunPod, bounded by a concurrency cap and a daily budget.
    # Raises RunpodBudgetError when either cap is hit so the caller can fall back or fail.
    if not _rp_budget_ok():
        raise RunpodBudgetError(f"runpod daily cap {RUNPOD_DAILY_MAX} reached")
    if not _rp_sema.acquire(timeout=OVERFLOW_WAIT):  # never more than N paid workers at once
        raise RunpodBudgetError(f"runpod concurrency cap {RUNPOD_CONCURRENCY} reached")
    try:
        with _rp_lock:
            _rp_day["count"] += 1
        _runpod.synthesize(text, out_path, voice=voice, lang=lang)
        return "runpod"
    finally:
        _rp_sema.release()


def synthesize_routed(text, out_path, voice="Male1", lang="he"):
    """Synthesize one request, picking the backend by CAPACITY rather than config.

    Space first: wait up to OVERFLOW_WAIT for a free slot, else spill to RunPod. If the
    Space errors, retry once on RunPod. With RunPod unconfigured this is plain Space synth.
    """
    # Dedicated RunPod mode: skip the Space entirely (the budget/concurrency caps still apply).
    if SYNTH_BACKEND == "runpod" and _runpod_ready():
        return _run_on_runpod(text, out_path, voice, lang)

    got_slot = _hf_sema.acquire(timeout=OVERFLOW_WAIT)
    if not got_slot:
        # The Space is full. Spill this synth to RunPod rather than making the user wait.
        if _runpod_ready():
            try:
                logger.info("HF Space at capacity, overflowing this synth to RunPod")
                return _run_on_runpod(text, out_path, voice, lang)
            except RunpodBudgetError as e:
                logger.warning(f"RunPod budget blocked overflow ({e}), waiting for a Space slot")
        # No overflow available (or budget blocked): wait for a Space slot, but BOUNDED, so a
        # wedged Space can't pin this worker thread (and its outer TTS permit) forever.
        if not _hf_sema.acquire(timeout=tts.SYNTH_TIMEOUT):
            raise tts.SynthError("no Space slot free and RunPod unavailable")
        got_slot = True
    try:
        tts.synthesize(text, out_path, voice=voice, lang=lang)
        return "hf"
    except Exception as e:
        if not _runpod_ready():
            raise
        try:
            logger.warning(f"HF Space synth failed ({type(e).__name__}), falling back to RunPod")
            return _run_on_runpod(text, out_path, voice, lang)
        except RunpodBudgetError:
            logger.warning("HF Space failed AND RunPod budget blocked, giving up on this synth")
            raise e
    finally:
        if got_slot:
            _hf_sema.release()
