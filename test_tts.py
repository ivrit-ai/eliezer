"""Unit tests for the TTS capacity routing (HF Space -> RunPod overflow) and budget caps.

No live services: gradio_client is stubbed, the HF synth and the RunPod client are faked, so
we can assert the routing decisions the same way the original TTS bot's overflow logic behaves.
Run: py test_tts.py
"""
import os
import sys
import time
import types
import tempfile
import threading

# Stub gradio_client so `import tts` works without the real package (we fake the synth anyway).
_stub = types.ModuleType("gradio_client")
_stub.Client = object
sys.modules["gradio_client"] = _stub
# A separate TTS RunPod endpoint must look configured for _runpod_ready() to pass.
os.environ.setdefault("TTS_RUNPOD_ENDPOINT_ID", "test-endpoint")
os.environ.setdefault("RUNPOD_API_KEY", "test-key")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tts  # noqa: E402
import tts_router  # noqa: E402


class FakeRunpod:
    def __init__(self):
        self.calls = 0

    def synthesize(self, text, out_path, voice="Male1", lang="he"):
        self.calls += 1
        with open(out_path, "wb") as f:
            f.write(b"ogg")
        return out_path


def _fake_hf(text, out_path, voice="Male1", lang="he"):
    _fake_hf.calls += 1
    with open(out_path, "wb") as f:
        f.write(b"ogg")
    return out_path


def _tmp():
    return os.path.join(tempfile.gettempdir(), f"tts_test_{os.getpid()}_{time.time_ns()}.ogg")


def reset(runpod_ready=True):
    """Fresh semaphores + fakes before each test."""
    tts_router.SYNTH_BACKEND = "hf"
    tts_router.OVERFLOW_WAIT = 0.2
    tts_router.RUNPOD_DAILY_MAX = 500
    tts_router._hf_sema = threading.Semaphore(tts_router.HF_CONCURRENCY)
    tts_router._rp_sema = threading.Semaphore(tts_router.RUNPOD_CONCURRENCY)
    tts_router._rp_day = {"day": "", "count": 0}
    tts_router._runpod = FakeRunpod()
    tts_router._runpod_ready = lambda: runpod_ready
    tts.synthesize = _fake_hf
    _fake_hf.calls = 0


RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))


# 1. Free Space slot -> HF, RunPod untouched.
reset()
b = tts_router.synthesize_routed("שלום עולם", _tmp())
check("hf_when_slot_free", b == "hf" and _fake_hf.calls == 1 and tts_router._runpod.calls == 0)

# 2. Space full -> spill to RunPod (the whole point).
reset()
for _ in range(tts_router.HF_CONCURRENCY):
    tts_router._hf_sema.acquire()
b = tts_router.synthesize_routed("שלום עולם", _tmp())
check("spill_to_runpod_when_full", b == "runpod" and _fake_hf.calls == 0 and tts_router._runpod.calls == 1)

# 3. HF errors -> fall back to RunPod.
reset()
def _boom(*a, **k):
    raise tts.SynthError("hf down")
tts.synthesize = _boom
b = tts_router.synthesize_routed("שלום עולם", _tmp())
check("hf_error_falls_back_to_runpod", b == "runpod" and tts_router._runpod.calls == 1)

# 4. SYNTH_BACKEND=runpod skips the Space entirely.
reset()
tts_router.SYNTH_BACKEND = "runpod"
b = tts_router.synthesize_routed("שלום עולם", _tmp())
check("backend_runpod_skips_hf", b == "runpod" and _fake_hf.calls == 0 and tts_router._runpod.calls == 1)

# 5. Space full + RunPod daily cap hit -> wait for a Space slot (no overflow).
reset()
tts_router.RUNPOD_DAILY_MAX = 1
tts_router._rp_day = {"day": tts_router.datetime.now().strftime("%Y-%m-%d"), "count": 1}  # already at cap
for _ in range(tts_router.HF_CONCURRENCY):
    tts_router._hf_sema.acquire()
threading.Timer(0.1, tts_router._hf_sema.release).start()  # free one slot shortly
b = tts_router.synthesize_routed("שלום עולם", _tmp())
check("budget_blocked_waits_for_hf", b == "hf" and tts_router._runpod.calls == 0)

# 6. Dedicated RunPod mode + daily cap hit -> RunpodBudgetError, nothing synthesized.
reset()
tts_router.SYNTH_BACKEND = "runpod"
tts_router.RUNPOD_DAILY_MAX = 1
tts_router._rp_day = {"day": tts_router.datetime.now().strftime("%Y-%m-%d"), "count": 1}
try:
    tts_router.synthesize_routed("שלום עולם", _tmp())
    ok = False
except tts_router.RunpodBudgetError:
    ok = True
check("runpod_daily_cap_blocks", ok and tts_router._runpod.calls == 0)

# 7. HF semaphore is not leaked on success or on the error path.
reset()
before = tts_router._hf_sema._value
tts_router.synthesize_routed("שלום עולם", _tmp())          # success path
tts.synthesize = _boom
tts_router._runpod_ready = lambda: False                    # force a re-raise (no RunPod)
try:
    tts_router.synthesize_routed("שלום עולם", _tmp())
except tts.SynthError:
    pass
check("no_hf_semaphore_leak", tts_router._hf_sema._value == before)

ok_all = all(c for _, c in RESULTS)
for name, c in RESULTS:
    print(f"[{'OK  ' if c else 'FAIL'}] {name}")
print("ALL PASS" if ok_all else "SOME FAILED")
sys.exit(0 if ok_all else 1)
