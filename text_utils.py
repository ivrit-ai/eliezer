"""Text helpers for the text-to-speech path: language detection + chunking.

Kept separate from whatsapp_bot.py so it imports nothing heavy and can be unit-tested on its own.

Language routing note: only Hebrew and English are routed today. BlueTTS reads Hebrew and
Latin script, so detect_lang returns "he" or "en" and rejects everything else. That means
Russian/Cyrillic is rejected outright, and German/Spanish/Italian collapse to English
(their Latin letters make lang="en"). Real per-language TTS support is a future enhancement.
"""
import re

# How long ONE voice note is (chars), and the most voice notes we make for one message.
# MAX_CHUNKS * MAX_CHARS ~ 14,000 chars (~17 minutes of audio) is the whole-message ceiling;
# a longer text is refused rather than read aloud.
MAX_CHARS = 2800
MAX_CHUNKS = 5

# BlueTTS reads Hebrew + Latin script only.
HEB_RE = re.compile(r"[֐-׿]")            # Hebrew block
LAT_RE = re.compile(r"[A-Za-z]")                   # Latin letters
LETTER_RE = re.compile(r"[^\W\d_]", re.UNICODE)    # any Unicode letter (excludes digits/symbols)


def has_letters(text):
    """True if the text has at least one real letter (not just emoji / digits / symbols)."""
    return bool(LETTER_RE.search(text or ""))


def detect_lang(text):
    """Return 'he' or 'en' for text BlueTTS can read, else None.

    None means the text has NO letters at all (emoji / numbers / symbols only) OR only
    letters in a script we don't route (Russian/Cyrillic, Arabic, CJK, ...). Mixed
    Hebrew+English maps to 'he'.
    """
    s = (text or "").strip()
    has_heb = bool(HEB_RE.search(s))
    has_lat = bool(LAT_RE.search(s))
    if not (has_heb or has_lat):
        return None
    return "he" if has_heb else "en"


def split_for_whatsapp(text, limit=MAX_CHARS):
    """Split into <=limit-char chunks at paragraph -> sentence -> whitespace bounds."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    chunks = []
    remaining = text
    while len(remaining) > limit:
        window = remaining[:limit]
        cut = _best_cut(window)
        chunk = remaining[:cut].rstrip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _best_cut(window):
    """Pick the nicest split point inside window: last paragraph break, else last sentence
    end, else last space, else a hard cut at the limit."""
    for sep in ("\n\n", "\n"):
        idx = window.rfind(sep)
        if idx > 0:
            return idx + len(sep)
    m = None
    for match in re.finditer(r"[.!?׃]\s", window):  # . ! ? and the Hebrew sof pasuq
        m = match
    if m is not None and m.end() > 0:
        return m.end()
    idx = window.rfind(" ")
    if idx > 0:
        return idx + 1
    return len(window)
