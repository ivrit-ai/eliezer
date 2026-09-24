"""Unit tests for the TTS language gate + long-text chunking (text_utils).

No live services and no heavy deps: text_utils imports only `re`, so these run standalone.
Run: py test_tts_lang.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import text_utils  # noqa: E402


RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))


# --- Language detection (faithful to the BlueTTS bot's _voiceable_lang) ---

# Hebrew -> he
check("hebrew_detects_he", text_utils.detect_lang("שלום עולם") == "he")
# English -> en
check("english_detects_en", text_utils.detect_lang("hello world") == "en")
# Mixed Hebrew + English -> he
check("mixed_heb_en_is_he", text_utils.detect_lang("שלום hello") == "he")
# Cyrillic (Russian) -> rejected (None): a script we don't route
check("cyrillic_rejected", text_utils.detect_lang("Привет мир") is None)
# Numbers / symbols only -> rejected (no letters)
check("numbers_only_rejected", text_utils.detect_lang("12345 !!! ???") is None)
# Emoji only -> rejected (no letters)
check("emoji_only_rejected", text_utils.detect_lang("😀🎉🎧") is None)
# has_letters distinguishes emoji/number-only from real words
check("has_letters_false_on_emoji", text_utils.has_letters("😀🎉") is False)
check("has_letters_true_on_word", text_utils.has_letters("שלום") is True)


# --- Chunking (split_for_whatsapp) ---

# Short text -> a single chunk, unchanged.
check("short_text_one_chunk", text_utils.split_for_whatsapp("שלום עולם") == ["שלום עולם"])

# A long text splits into N>1 chunks, each within the per-note limit.
sentence = "שלום עולם. "  # 11 chars, ends on a sentence boundary
long_text = sentence * 280  # ~3080 chars -> more than one MAX_CHARS chunk
long_chunks = text_utils.split_for_whatsapp(long_text, text_utils.MAX_CHARS)
check("long_text_splits_into_many", len(long_chunks) > 1)
check("every_chunk_within_limit", all(len(c) <= text_utils.MAX_CHARS for c in long_chunks))

# A too-long text yields more chunks than MAX_CHUNKS -> the hook refuses it.
too_long = sentence * 1400  # ~15,400 chars -> > MAX_CHUNKS * MAX_CHARS (~14,000)
too_long_chunks = text_utils.split_for_whatsapp(too_long, text_utils.MAX_CHARS)
check("too_long_exceeds_max_chunks", len(too_long_chunks) > text_utils.MAX_CHUNKS)


ok_all = all(c for _, c in RESULTS)
for name, c in RESULTS:
    print(f"[{'OK  ' if c else 'FAIL'}] {name}")
print("ALL PASS" if ok_all else "SOME FAILED")
sys.exit(0 if ok_all else 1)
