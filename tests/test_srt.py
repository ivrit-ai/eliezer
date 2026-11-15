"""Subtitles from timed segments (status_site/srt.py): validation and SRT."""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "status_site"))
import srt  # noqa: E402

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)


def main():
    # ===== clean(): what the hub accepts from an edge
    check("clean() accepts well-formed segments", srt.clean([{"start": 0, "end": 2, "text": "שלום"}]))
    check("clean() ignores extra fields", srt.clean([{"start": 0, "end": 2, "text": "שלום", "words": []}]))
    for name, bad in [
        ("not a list", {"start": 0}),
        ("empty", []),
        ("missing times", [{"text": "x"}]),
        ("negative time", [{"start": -1, "end": 2, "text": "x"}]),
        ("bool time", [{"start": True, "end": 2, "text": "x"}]),
        ("non-finite time", [{"start": 0, "end": float("inf"), "text": "x"}]),
        ("text not a string", [{"start": 0, "end": 1, "text": 5}]),
        ("too many segments", [{"start": 0, "end": 1, "text": "x"}] * (srt.MAX_SEGMENTS + 1)),
    ]:
        check(f"clean() rejects malformed segments: {name}", srt.clean(bad) is None)
    check("an end before its start is clamped to the start",
          srt.clean([{"start": 3, "end": 1, "text": "x"}])[0]["end"] == 3.0)

    # ===== render
    check("timestamps are HH:MM:SS,mmm",
          srt.timestamp(0) == "00:00:00,000" and srt.timestamp(3725.5) == "01:02:05,500")
    out = srt.render(srt.clean([
        {"start": 0.42, "end": 1.26, "text": " מה שלומך? "},
        {"start": 2.0, "end": 3.5, "text": "   "},
        {"start": 4.0, "end": 9.0, "text": "טוב,\nתודה."},
    ]))
    lines = out.splitlines()
    check("one numbered cue per segment, with its own time range",
          lines[:2] == ["1", "00:00:00,420 --> 00:00:01,260"]
          and lines[4:6] == ["2", "00:00:04,000 --> 00:00:09,000"], lines)
    check("each cue is one line, wrapped in right-to-left marks",
          lines[2] == f"{srt.RLM}מה שלומך?{srt.RLM}" and lines[6] == f"{srt.RLM}טוב, תודה.{srt.RLM}", lines)
    check("blank segments are skipped, and numbering stays continuous", len(lines) == 7, lines)
    long_text = " ".join(f"מילה{i}" for i in range(20))
    out = srt.render(srt.clean([{"start": 10.0, "end": 20.0, "text": long_text}]))
    blocks = [b.splitlines() for b in out.split("\n\n")]
    pieces = [b[2].strip(srt.RLM) for b in blocks]
    check("a long segment is split at spaces into cues of about MAX_CUE_CHARS, nothing lost",
          len(blocks) == -(-len(long_text) // srt.MAX_CUE_CHARS) and " ".join(pieces) == long_text
          and all(len(p) <= srt.MAX_CUE_CHARS + 6 for p in pieces), pieces)
    check("...sharing the segment's time, in order and without overlap",
          blocks[0][1].startswith("00:00:10,000 --> ") and blocks[-1][1].endswith(" --> 00:00:20,000")
          and all(blocks[i][1][-12:] <= blocks[i + 1][1][:12] for i in range(len(blocks) - 1)), blocks)
    check("nothing to show renders nothing", srt.render(srt.clean([{"start": 0, "end": 1, "text": " "}])) == "")

    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
