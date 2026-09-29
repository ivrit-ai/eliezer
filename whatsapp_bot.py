"""Eliezer transcription edge.

Leases jobs from the Eliezer Status site's queue, fetches each job's audio through the
site, reports its duration for admission, transcribes it (locally with faster-whisper,
or on RunPod) and hands the transcript back. The site does everything user-facing -
platform APIs, replies, limits, statistics - so the edge needs only the queue's URL and
token, plus RunPod credentials when not running --local.
"""

import os
import sys
import asyncio
import tempfile
import ivrit
from dotenv import load_dotenv
import subprocess
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import queue
import socket
import time
import argparse
from queue_client import StaleJob, make_queue_client

# Load environment variables
load_dotenv()

# Configure logging with a custom formatter that includes file name and line number
class FileLineFormatter(logging.Formatter):
    def format(self, record):
        # Get the relative path of the file
        filepath = record.pathname
        filename = os.path.basename(filepath)

        # Format the log message with file name, line number, thread name, and message
        return f"{filename:<20}:{record.lineno:<4} {self.formatTime(record)} [{record.threadName}] {record.levelname} - {record.getMessage()}"

# On xhost ($PORT is set) log to stdout, which the platform captures as the runtime log;
# the container disk is ephemeral and an unbounded log file would eventually fill it.
if os.getenv('PORT'):
    file_handler = logging.StreamHandler(sys.stdout)
else:
    file_handler = logging.FileHandler('whatsapp_bot.log')
file_handler.setFormatter(FileLineFormatter())

# Create a logger
logger = logging.getLogger('whatsapp_bot')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
# Prevent propagation to the root logger (which would output to console)
logger.propagate = False

# Forward ivrit-py logs into the same file (its module uses logging.getLogger('ivrit.audio')).
# Without this, ivrit's records propagate to the unconfigured root logger and are dropped.
ivrit_logger = logging.getLogger('ivrit')
ivrit_logger.setLevel(logging.INFO)
ivrit_logger.addHandler(file_handler)
ivrit_logger.propagate = False


def start_health_server(port):
    """Serve 200 on every path so the xhost platform health check passes.

    The bot is a worker with no web surface, but xhost fails a deploy if nothing is
    listening on $PORT or / returns non-2xx. Started before the bot boots so the check
    succeeds regardless of how long startup takes.
    """
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'ok')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    threading.Thread(target=server.serve_forever, name='Health', daemon=True).start()
    return server


SUBPROCESS_TIMEOUT = 30
MONITOR_INTERVAL = 60  # seconds between thread status reports
STALLED_THRESHOLD = 120  # seconds before a thread is flagged as possibly stuck
IDLE_PROBE_INTERVAL = 10  # seconds the dispatcher waits when there is nothing to pull


class WhatsAppBot:
    def __init__(self, num_workers, local=False, overflow_handler=None, debug=False):
        self.queue = make_queue_client()

        # Initialize transcription model
        if local:
            self.transcription_model = ivrit.load_model(
                engine='faster-whisper',
                model='ivrit-ai/whisper-large-v3-turbo-ct2',
            )
        else:
            self.transcription_model = ivrit.load_model(
                engine='runpod',
                model='ivrit-ai/whisper-large-v3-turbo-ct2',
                api_key=os.getenv('RUNPOD_API_KEY'),
                endpoint_id=os.getenv('RUNPOD_ENDPOINT_ID'),
                core_engine='faster-whisper',
            )

        # Thread control
        self.stop_event = threading.Event()
        self.worker_threads = []
        # num_workers is the number of concurrent transcriptions. We run twice as
        # many worker threads so I/O (queue, media download, ffmpeg) overlaps with
        # transcription, and gate the transcription step itself with a semaphore.
        self.num_workers = num_workers
        self.num_worker_threads = 2 * num_workers
        self.transcription_semaphore = threading.BoundedSemaphore(num_workers)
        self.overflow_handler = overflow_handler

        # In-process queue: the dispatcher thread fills it from the message queue,
        # workers drain it. At most one buffered job per worker thread: a leased job's
        # visibility timeout starts at lease time, so a deeper local backlog risks it
        # expiring here and being handed to another edge as well.
        self.job_queue = queue.Queue(maxsize=self.num_worker_threads)

        # Logger
        self.logger = logging.getLogger('whatsapp_bot')

        # Transcription counter and duration tracker, for the log
        self.transcription_counter = 0
        self.total_duration = 0
        self.counter_lock = threading.Lock()
        self.instance_id = os.getenv('INSTANCE_ID') or socket.gethostname()

        # Thread activity tracking for debugging
        self.worker_activity = {}
        self.activity_lock = threading.Lock()

        # Debug mode - verbose logging to file
        if debug:
            self.logger.setLevel(logging.DEBUG)
            logging.getLogger('ivrit').setLevel(logging.DEBUG)

    def transcribe_audio(self, audio_path):
        """Transcribe audio using the ivrit package (async path -> aiohttp)."""
        try:
            self.logger.debug(f"transcribe_audio: waiting for transcription slot for {audio_path}")
            with self.transcription_semaphore:
                self.logger.debug(f"transcribe_audio: starting transcription for {audio_path}")
                segments = asyncio.run(self._collect_transcription_segments(audio_path))
            self.logger.debug(f"transcribe_audio: completed for {audio_path} ({len(segments)} segments)")
            text_result = '\n'.join(segment.text.strip() for segment in segments)
            if text_result:
                return text_result
            return "לא הצלחתי להבין את ההודעה הקולית."
        except Exception as e:
            self.logger.error(f"Error transcribing audio: {e}")
            return "אירעה שגיאה בעיבוד ההודעה הקולית."

    async def _collect_transcription_segments(self, audio_path):
        """Consume ivrit's async transcription generator into a list of segments."""
        segments = []
        async for segment in self.transcription_model.transcribe_async(path=audio_path, language='he'):
            segments.append(segment)
        return segments

    def check_audio_duration(self, audio_path):
        """Get the duration of an audio file in seconds using ffprobe."""
        try:
            cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', audio_path]
            self.logger.debug(f"check_audio_duration: running ffprobe on {audio_path}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
            duration = float(result.stdout.strip())
            return duration
        except Exception as e:
            self.logger.error(f"Error checking audio duration: {str(e)}")
            return None

    def process_audio_message(self, audio_path):
        """Transcribe, turning failure and silence into a message for the user."""
        try:
            transcription = self.transcribe_audio(audio_path)
            if not transcription or transcription.strip() == "":
                self.logger.warning("Transcription returned empty text")
                return "התמלול לא החזיר טקסט. ייתכן שההקלטה שקטה מדי."
            return transcription
        except Exception as e:
            self.logger.error(f"Transcription failed: {str(e)}")
            return "אירעה שגיאה בתמלול ההקלטה."

    def convert_to_opus(self, input_path):
        """Convert a document to Opus (CBR, mono) via ffmpeg; None if it isn't audio."""
        temp_output = tempfile.NamedTemporaryFile(suffix='.opus', delete=False)
        temp_output.close()
        cmd = [
            'ffmpeg', '-y', '-loglevel', 'error',
            '-i', input_path,
            '-c:a', 'libopus',
            '-b:a', '32k',
            '-vbr', 'off',
            '-ac', '1',
            temp_output.name,
        ]
        self.logger.debug(f"convert_to_opus: running ffmpeg on {input_path}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            self.logger.info(f"Document is not convertible audio (ffmpeg rc={result.returncode}): {result.stderr.strip()[:200]}")
            os.unlink(temp_output.name)
            return None
        return temp_output.name

    def process_job(self, job):
        """Fetch, admit, transcribe, hand back. The site replies to the user."""
        handle = job['handle']
        label = handle[:8]
        temp_files = []
        try:
            self._set_activity(f"downloading job {label}")
            source = tempfile.NamedTemporaryFile(suffix='.ogg' if job['kind'] == 'audio' else '', delete=False)
            source.close()
            temp_files.append(source.name)
            self.queue.fetch_media(handle, source.name)

            audio_path = source.name
            if job['kind'] == 'document':
                self._set_activity(f"converting job {label}")
                audio_path = self.convert_to_opus(source.name)
                if audio_path is None:
                    self.queue.complete(handle, error='unsupported')
                    return
                temp_files.append(audio_path)

            duration = self.check_audio_duration(audio_path)
            self._set_activity(f"admitting job {label}")
            if not self.queue.admit(handle, duration):
                self.logger.info(f"Job {label} not admitted (duration {duration})")
                return

            self._set_activity(f"transcribing job {label}")
            started = time.time()
            text = self.process_audio_message(audio_path)
            transcription_seconds = time.time() - started

            self._set_activity(f"completing job {label}")
            self.queue.complete(handle, text=text, transcription_seconds=transcription_seconds, duration=duration)
            with self.counter_lock:
                self.transcription_counter += 1
                self.total_duration += duration
                count, total_minutes = self.transcription_counter, self.total_duration / 60
            self.logger.info(f"Completed transcription #{count} for job {label} ({duration:.1f}s audio, total {total_minutes:.1f} minutes)")
        except StaleJob:
            # Its lease lapsed and another edge has it, or it was dropped; either way
            # it is no longer ours to finish.
            self.logger.info(f"Job {label} is no longer ours; dropping it")
        finally:
            for path in temp_files:
                if os.path.exists(path):
                    os.unlink(path)

    def _set_activity(self, activity):
        """Update current thread's activity for monitoring."""
        thread_name = threading.current_thread().name
        with self.activity_lock:
            self.worker_activity[thread_name] = (activity, time.time())

    def _monitor_threads(self):
        """Periodically log thread activity to detect stuck workers."""
        threading.current_thread().name = "Monitor"
        self.logger.info("Monitor thread started")

        while not self.stop_event.is_set():
            self.stop_event.wait(MONITOR_INTERVAL)
            if self.stop_event.is_set():
                break

            with self.activity_lock:
                activities = dict(self.worker_activity)

            now = time.time()
            stuck_workers = []
            status_lines = []

            for worker, (activity, timestamp) in sorted(activities.items()):
                age = now - timestamp
                line = f"  {worker}: {activity} ({age:.0f}s ago)"
                status_lines.append(line)
                if age > STALLED_THRESHOLD:
                    stuck_workers.append((worker, activity, age))

            if status_lines:
                self.logger.info("Thread status:\n" + "\n".join(status_lines))

            if stuck_workers:
                for worker, activity, age in stuck_workers:
                    self.logger.warning(f"POSSIBLY STUCK: {worker} has been '{activity}' for {age:.0f}s")

    def dispatcher(self):
        """Single thread that reads the queue and feeds jobs to the worker pool."""
        threading.current_thread().name = "Dispatcher"
        self.logger.info("Dispatcher thread started")

        while not self.stop_event.is_set():
            try:
                free = self.job_queue.maxsize - self.job_queue.qsize()
                if free < 1:
                    self.stop_event.wait(IDLE_PROBE_INTERVAL)
                    continue

                self._set_activity("polling queue")
                # Pull up to one job per worker thread this cycle. In overflow mode the
                # server leases only what sits above the threshold, in the same call.
                target = min(free, self.num_worker_threads)
                jobs = self.queue.lease(
                    target, 20, min_depth=self.overflow_handler or 0)
                self.logger.debug(
                    f"Leased {len(jobs)} job(s), target {target}, "
                    f"overflow threshold {self.overflow_handler}")
                for job in jobs:
                    self.job_queue.put(job)  # guaranteed room -> won't block

            except Exception as e:
                self.logger.error(f"Error in dispatcher thread: {str(e)}")
                time.sleep(5)  # Wait a bit before retrying
                continue

    def worker(self, worker_id):
        """Worker thread function to process jobs handed off by the dispatcher."""
        thread_name = f"Worker-{worker_id}"
        # Set thread name
        threading.current_thread().name = thread_name

        self.logger.info("Starting worker thread")

        while not self.stop_event.is_set():
            try:
                self._set_activity("waiting for job")
                try:
                    job = self.job_queue.get(timeout=1)
                except queue.Empty:
                    continue

                try:
                    self.process_job(job)
                except Exception as e:
                    # Not completed: hand it back so it is retried shortly, on this edge
                    # or another, up to the site's receive limit. If even that fails,
                    # the lease lapses and it is retried then.
                    self.logger.error(f"Error processing job: {str(e)}")
                    try:
                        self.queue.release(job['handle'])
                    except Exception as release_error:
                        self.logger.debug(f"Could not release job: {release_error}")
                finally:
                    self.job_queue.task_done()

            except Exception as e:
                self.logger.error(f"Error in worker thread: {str(e)}")
                time.sleep(5)  # Wait a bit before retrying
                continue

    def run(self):
        """Start worker threads to poll the queue."""
        # Set main thread name
        threading.current_thread().name = "Main"

        self.logger.info(f"Starting transcription edge {self.instance_id} with {self.num_worker_threads} worker threads "
                         f"({self.num_workers} concurrent transcriptions)...")

        # Start worker threads
        for i in range(self.num_worker_threads):
            thread = threading.Thread(
                target=self.worker,
                args=(i,),
                name=f"Worker-{i}",
                daemon=True
            )
            self.worker_threads.append(thread)
            thread.start()
            self.logger.info(f"Started thread {thread.name}")

        # Start dispatcher thread (sole queue reader, feeds the worker pool)
        dispatcher_thread = threading.Thread(
            target=self.dispatcher,
            name="Dispatcher",
            daemon=True
        )
        dispatcher_thread.start()
        self.logger.info("Started dispatcher thread")

        # Start monitor thread
        monitor_thread = threading.Thread(
            target=self._monitor_threads,
            name="Monitor",
            daemon=True
        )
        monitor_thread.start()
        self.logger.info("Started monitor thread")

        try:
            # Keep the main thread alive
            while not self.stop_event.is_set():
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            self.stop_event.set()

            # Wait for all threads to finish
            for thread in self.worker_threads:
                thread.join()

            self.logger.info("Shutdown complete")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Eliezer transcription edge')
    parser.add_argument('--num-workers', type=int, default=None, help='Number of concurrent transcriptions (default: 10, or 1 in --local mode); the bot runs 2x this many worker threads to overlap I/O')
    parser.add_argument('--local', action='store_true', help='Transcribe locally with faster-whisper instead of RunPod')
    parser.add_argument('--overflow-handler', type=int, default=None, metavar='N', help='Only handle jobs when the queue depth exceeds N')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging (verbose output to console and file)')
    args = parser.parse_args()

    # Bind the health port first so xhost's 120s startup check can't race bot init.
    port = os.getenv('PORT')
    if port:
        start_health_server(int(port))
        logger.info(f"Health server listening on port {port}")

    # Resolve worker count: explicit value wins, else 1 locally / 10 remotely.
    num_workers = args.num_workers
    if num_workers is None:
        num_workers = 1 if args.local else 10

    # Initialize and run the bot
    bot = WhatsAppBot(
        num_workers=num_workers,
        local=args.local,
        overflow_handler=args.overflow_handler,
        debug=args.debug
    )
    bot.run()
