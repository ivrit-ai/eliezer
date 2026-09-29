"""Shared setup for the end-to-end suites: a scratch directory, and real audio made
with ffmpeg so that ffprobe and document conversion run for real."""

import os
import subprocess
import tempfile

# name -> ffmpeg arguments producing it from a sine wave
AUDIO = {
    "short.ogg": ["-f", "lavfi", "-i", "sine=frequency=440:duration=3", "-c:a", "libopus", "-b:a", "24k"],
    # 5s: the suites' fake model returns a transcript long enough to need two messages
    "medium.ogg": ["-f", "lavfi", "-i", "sine=frequency=330:duration=5", "-c:a", "libopus", "-b:a", "24k"],
    # just over the 10-minute cap
    "long.ogg": ["-f", "lavfi", "-i", "sine=frequency=220:duration=601", "-c:a", "libopus", "-b:a", "6k"],
    "song.mp3": ["-f", "lavfi", "-i", "sine=frequency=550:duration=2", "-c:a", "libmp3lame", "-b:a", "32k"],
}


def workdir():
    """Where a run keeps its logs; the edge also writes its log to its working directory."""
    return tempfile.mkdtemp(prefix="eliezer-test-")


def make_fixtures(root):
    """Generate the audio fixtures (plus a non-audio file) under root/fixtures."""
    path = os.path.join(root, "fixtures")
    os.makedirs(path, exist_ok=True)
    for name, args in AUDIO.items():
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *args, os.path.join(path, name)], check=True)
    with open(os.path.join(path, "garbage.bin"), "wb") as f:
        f.write(os.urandom(4000))
    return path
