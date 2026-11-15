"""Subtitles (SRT) from the timed segments an edge hands back with a video's transcript.

One cue per segment, as the model cut them - as the ivrit.ai app exports them
(ivrit-app: web/transcription/transcript.js, srt()) - except that a long segment is
split into shorter cues. Players and the render worker wrap lines. Each cue is wrapped in right-to-left marks, so punctuation and
numbers at either end of a Hebrew line stay where a reader expects them.
"""

import math

RLM = "‏"

# Longer segments are split into cues of about this many characters (the usual
# subtitle line), so no cue fills the screen.
MAX_CUE_CHARS = 42

# What /queue/complete accepts: a 10-minute clip is a few hundred segments.
MAX_SEGMENTS = 5000


def _number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x >= 0


def clean(raw):
    """The segments an edge sent ([{start, end, text}]), validated, or None when there
    are none or they are malformed. Never raises: bad timings must not cost the user
    their transcript."""
    if not isinstance(raw, list) or not raw or len(raw) > MAX_SEGMENTS:
        return None
    segments = []
    for s in raw:
        if not (isinstance(s, dict) and _number(s.get("start")) and _number(s.get("end"))
                and isinstance(s.get("text"), str)):
            return None
        start = float(s["start"])
        segments.append({"start": start, "end": max(start, float(s["end"])), "text": s["text"]})
    return segments


def timestamp(seconds):
    ms = max(0, int(round(seconds * 1000)))
    h, ms = divmod(ms, 3_600_000)
    m, ms = divmod(ms, 60_000)
    s, ms = divmod(ms, 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def _pieces(text):
    """text as near-equal parts of at most about MAX_CUE_CHARS, split at spaces:
    [(piece, offset)], offset being where the piece starts in text."""
    parts = math.ceil(len(text) / MAX_CUE_CHARS)
    pieces, current, offset = [], [], 0
    for word in text.split(" "):
        current.append(word)
        if len(pieces) < parts - 1 and len(" ".join(current)) >= len(text) / parts:
            pieces.append((" ".join(current), offset))
            offset += len(pieces[-1][0]) + 1
            current = []
    if current:
        pieces.append((" ".join(current), offset))
    return pieces


def render(segments):
    """SRT text for clean()ed segments; '' when there is nothing to show. A long
    segment becomes several cues, sharing its time by their lengths."""
    blocks = []
    for s in segments:
        text = " ".join(s["text"].split())
        span = s["end"] - s["start"]
        for piece, offset in _pieces(text) if text else []:
            start = s["start"] + span * offset / len(text)
            end = s["start"] + span * (offset + len(piece)) / len(text)
            blocks.append(f"{len(blocks) + 1}\n{timestamp(start)} --> {timestamp(end)}\n{RLM}{piece}{RLM}\n")
    return "\n".join(blocks)
