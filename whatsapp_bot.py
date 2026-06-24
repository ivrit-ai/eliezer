import os
import json
import asyncio
import boto3
import requests
import tempfile
import ivrit
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import logging
from datetime import datetime, timezone
import threading
import queue
import socket
import time
import posthog
import uuid
import random
import argparse
import collections
import statistics
import phonenumbers
from phonenumbers import region_code_for_number, country_code_for_region
from status_reporter import StatusReporter

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

# Create a file handler that only writes to file
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

ph = None
if "POSTHOG_API_KEY" in os.environ:
    ph = posthog.Posthog(project_api_key=os.environ["POSTHOG_API_KEY"], host="https://us.i.posthog.com")

def capture_event(distinct_id, event, props=None):
    global ph

    if not ph:
        return

    props = {} if not props else props
    props["source"] = "eliezer.ivrit.ai"

    ph.capture(distinct_id=distinct_id, event=event, properties=props)

# Timeouts for HTTP requests: (connect_timeout, read_timeout) in seconds
REQUEST_TIMEOUT = (10, 30)
DOWNLOAD_TIMEOUT = (10, 120)  # longer for media downloads
SUBPROCESS_TIMEOUT = 30
MONITOR_INTERVAL = 60  # seconds between thread status reports
STALLED_THRESHOLD = 120  # seconds before a thread is flagged as possibly stuck
IDLE_PROBE_INTERVAL = 10  # seconds the dispatcher waits when there is nothing to pull

class LeakyBucket:
    def __init__(self, max_messages_per_hour, max_minutes_per_hour):
        self.max_messages = max_messages_per_hour
        self.max_minutes = max_minutes_per_hour * 60  # Convert to seconds
        self.messages_remaining = max_messages_per_hour
        self.seconds_remaining = max_minutes_per_hour * 60
        self.last_update = time.time()
        
        # Calculate fill rates (per second)
        self.message_fill_rate = max_messages_per_hour / 3600
        self.time_fill_rate = self.max_minutes / 3600
    
    def update(self):
        """Update bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        self.last_update = now
        
        # Add resources based on fill rate
        self.messages_remaining = min(self.max_messages, self.messages_remaining + self.message_fill_rate * elapsed)
        self.seconds_remaining = min(self.max_minutes, self.seconds_remaining + self.time_fill_rate * elapsed)
    
    def can_transcribe(self, duration_seconds):
        """Check if transcription is allowed."""
        self.update()
        return self.messages_remaining >= 1 and self.seconds_remaining >= duration_seconds
    
    def consume(self, duration_seconds):
        """Consume resources for transcription."""
        self.update()
        self.messages_remaining -= 1
        self.seconds_remaining -= duration_seconds
        return self.messages_remaining > 0 and self.seconds_remaining > 0
    
    def is_full(self):
        """Check if the bucket is full (or nearly full)."""
        self.update()
        return (self.messages_remaining >= self.max_messages * 0.95 and 
                self.seconds_remaining >= self.max_minutes * 0.95)

class WhatsAppBot:
    def __init__(self, nudge_interval, user_max_messages_per_hour, user_max_minutes_per_hour, cleanup_frequency, num_workers, local=False, overflow_handler=None, debug=False):
        self.sqs = boto3.client('sqs')
        self.queue_url = os.getenv('APP_SQS_QUEUE')
        self.api_token = os.getenv('WHATSAPP_API_TOKEN')
        self.phone_number_id = os.getenv('WHATSAPP_PHONE_NUMBER_ID')
        self.api_version = 'v22.0'
        self.base_url = f'https://graph.facebook.com/{self.api_version}'

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
        # many worker threads so I/O (SQS, media download, ffmpeg, reply) overlaps
        # with transcription, and gate the transcription step itself with a semaphore.
        self.num_workers = num_workers
        self.num_worker_threads = 2 * num_workers
        self.transcription_semaphore = threading.BoundedSemaphore(num_workers)
        self.overflow_handler = overflow_handler

        # In-process queue: the dispatcher thread fills it from SQS, workers drain it.
        self.job_queue = queue.Queue(maxsize=2 * self.num_worker_threads)
        
        # Logger
        self.logger = logging.getLogger('whatsapp_bot')
        
        # Nudge interval for donation messages
        self.nudge_interval = nudge_interval
        
        # Transcription counter and duration tracker
        self.transcription_counter = 0
        self.total_duration = 0
        self.counter_lock = threading.Lock()

        # Statistics tracking
        self.start_time = time.time()
        self.messages_handled = 0
        self.message_timestamps = collections.deque()  # timestamps of all incoming messages
        self.message_lengths = collections.deque()  # (timestamp, audio_duration) for transcribed messages

        # Fleet-wide stats via the Eliezer Status site (opt-in). When STATUS_SITE_URL is set,
        # each instance POSTs lightweight per-event metadata to the site, which aggregates the
        # whole fleet in SQL and renders a live dashboard. Unset -> the reporter is a no-op and
        # /status reports this instance only (the local windowed view below).
        self.instance_id = os.getenv('INSTANCE_ID') or socket.gethostname()
        self.status_reporter = StatusReporter(
            self.instance_id,
            self.stop_event,
            queue_depth_fn=self._queue_depth,
        )

        # Leaky bucket rate limiter settings
        self.user_max_messages_per_hour = user_max_messages_per_hour
        self.user_max_minutes_per_hour = user_max_minutes_per_hour
        self.cleanup_frequency = cleanup_frequency
        
        # User buckets with lock for thread safety
        self.user_buckets = {}
        self.bucket_lock = threading.Lock()

        # Thread activity tracking for debugging
        self.worker_activity = {}
        self.activity_lock = threading.Lock()

        # Debug mode - verbose logging to file
        if debug:
            self.logger.setLevel(logging.DEBUG)
            logging.getLogger('ivrit').setLevel(logging.DEBUG)

    def is_allowed_region(self, phone_number):
        """Check if the phone number is from an allowed region (Israeli, American/Canadian, or European)."""
        try:
            # Parse the phone number
            parsed_number = phonenumbers.parse("+" + phone_number)
            
            # Get the region code (e.g., 'US', 'IL', 'GB')
            region = region_code_for_number(parsed_number)
            
            # List of allowed regions - North America, Europe, and Israel combined
            allowed_regions = [
                # North America
                'US', 'CA',
                
                # Europe (EU countries and other European countries)
                'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 
                'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 
                'SE', 'GB', 'IS', 'LI', 'NO', 'CH', 'AL', 'AD', 'BA', 'BY', 'FO', 'GI', 'VA',
                'IM', 'JE', 'XK', 'MK', 'MD', 'MC', 'ME', 'RU', 'SM', 'RS', 'SJ', 'TR', 'UA',
                
                # Israel
                'IL'
            ]
            
            # Check if the region is in the allowed list
            return region in allowed_regions
            
        except phonenumbers.phonenumberutil.NumberParseException:
            self.logger.error(f"Failed to parse phone number: {phone_number}")
            return False

    def mark_message_as_read(self, message_id):
        """Mark a WhatsApp message as read."""
        url = f'{self.base_url}/{self.phone_number_id}/messages'
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        data = {
            'messaging_product': 'whatsapp',
            'status': 'read',
            'message_id': message_id
        }
        self.logger.debug(f"mark_message_as_read: {message_id}")
        response = requests.post(url, headers=headers, json=data, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Error marking message {message_id} as read: Status {response.status_code}, Response: {response.text}")
            print(f"Error marking message {message_id} as read: Status {response.status_code}, Response: {response.text}")
            raise
        return response.json()

    def send_reply(self, to_number, message_id, text):
        """Send a reply message with quote. Splits long messages into chunks of 4000 characters."""
        # Define the maximum message length.
        # WhatsApp's maximu message length is 4096, so keep some distance from that limit.
        MAX_MESSAGE_LENGTH = 4000
        
        # If the message is shorter than the limit, send it as is
        if len(text) <= MAX_MESSAGE_LENGTH:
            return self._send_single_message(to_number, message_id, text)
        
        # Split the message into chunks
        message_chunks = []
        for i in range(0, len(text), MAX_MESSAGE_LENGTH):
            message_chunks.append(text[i:i + MAX_MESSAGE_LENGTH])
        
        # Send each chunk as a separate message
        responses = []
        for i, chunk in enumerate(message_chunks):
            # Only the first message should be in reply to the original message
            chunk_message_id = message_id if i == 0 else None
            
            # Add part indicator if there are multiple chunks
            if len(message_chunks) > 1:
                chunk_with_indicator = f"[{i+1}/{len(message_chunks)}]\n{chunk}"
            else:
                chunk_with_indicator = chunk
                
            response = self._send_single_message(to_number, chunk_message_id, chunk_with_indicator)
            responses.append(response)
            
            # Add a small delay between messages to prevent rate limiting
            if i < len(message_chunks) - 1:
                time.sleep(0.1)
        
        # Return the response from the last chunk
        return responses[-1]

    def send_typing_indicator(self, message_id):
        """Send a typing indicator to show the user that you're preparing a response."""
        url = f'{self.base_url}/{self.phone_number_id}/messages'
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        data = {
            'messaging_product': 'whatsapp',
            'status': 'read',
            'message_id': message_id,
            'typing_indicator': {
                'type': 'text'
            }
        }
    
        self.logger.debug(f"send_typing_indicator: {message_id}")
        response = requests.post(url, headers=headers, json=data, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Error sending typing indicator: Status {response.status_code}, Response: {response.text}")
            print(f"Error sending typing indicator: Status {response.status_code}, Response: {response.text}")
            raise

        return response.json()
    
    def _send_single_message(self, to_number, message_id, text):
        """Send a single WhatsApp message."""
        url = f'{self.base_url}/{self.phone_number_id}/messages'
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        data = {
            'messaging_product': 'whatsapp',
            'recipient_type': 'individual',
            'to': to_number,
            'type': 'text',
            'text': {
                'body': text
            },
        }

        if message_id:
            data['context'] = { 'message_id': message_id }

        self.logger.debug(f"_send_single_message: to={to_number}")
        response = requests.post(url, headers=headers, json=data, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Error sending message to {to_number}: Status {response.status_code}, Response: {response.text}")
            print(f"Error sending message to {to_number}: Status {response.status_code}, Response: {response.text}")
            raise
        return response.json()

    def download_audio(self, media_id):
        """Download audio file from WhatsApp."""
        # First, get the media URL
        url = f'{self.base_url}/{media_id}'
        headers = {
            'Authorization': f'Bearer {self.api_token}'
        }
        self.logger.debug(f"download_audio: fetching media URL for {media_id}")
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        media_url = response.json()['url']

        # Download the actual media file
        self.logger.debug(f"download_audio: downloading media for {media_id}")
        response = requests.get(media_url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        
        # Create a temporary file with .ogg extension (WhatsApp voice messages are in OGG format)
        temp_file = tempfile.NamedTemporaryFile(suffix='.ogg', delete=False)
        temp_file.write(response.content)
        temp_file.close()
        
        return temp_file.name

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
        """Process the audio message and generate a response."""
        try:
            # Transcribe the audio
            try:
                transcription = self.transcribe_audio(audio_path)
                if not transcription or transcription.strip() == "":
                    self.logger.warning("Transcription returned empty text")
                    return "התמלול לא החזיר טקסט. ייתכן שההקלטה שקטה מדי."
                return transcription
            except Exception as e:
                self.logger.error(f"Transcription failed: {str(e)}")
                return "אירעה שגיאה בתמלול ההקלטה."
        finally:
            # Clean up the temporary file
            if os.path.exists(audio_path):
                os.unlink(audio_path)

    def convert_document_to_opus(self, document_id):
        """Convert a document to Opus (CBR, mono) via ffmpeg."""
        try:
            # First, get the document URL
            url = f'{self.base_url}/{document_id}'
            headers = {
                'Authorization': f'Bearer {self.api_token}'
            }
            self.logger.debug(f"convert_document_to_opus: fetching media URL for {document_id}")
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            media_url = response.json()['url']

            # Download the document
            self.logger.debug(f"convert_document_to_opus: downloading document for {document_id}")
            response = requests.get(media_url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()

            # Create temporary files
            temp_input = tempfile.NamedTemporaryFile(delete=False)
            temp_output = tempfile.NamedTemporaryFile(suffix='.opus', delete=False)
            temp_output.close()

            try:
                # Save the downloaded document
                temp_input.write(response.content)
                temp_input.close()

                # Convert with ffmpeg: opus codec, CBR (vbr off), mono
                cmd = [
                    'ffmpeg', '-y', '-loglevel', 'error',
                    '-i', temp_input.name,
                    '-c:a', 'libopus',
                    '-b:a', '32k',
                    '-vbr', 'off',
                    '-ac', '1',
                    temp_output.name,
                ]
                self.logger.debug(f"convert_document_to_opus: running ffmpeg on {temp_input.name}")
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(f"ffmpeg failed (rc={result.returncode}): {result.stderr.strip()}")

                return temp_output.name
            finally:
                # Clean up the input file
                if os.path.exists(temp_input.name):
                    os.unlink(temp_input.name)

        except Exception as e:
            self.logger.error(f"Error converting document to Opus: {str(e)}")
            # Clean up output file if it exists
            if 'temp_output' in locals() and os.path.exists(temp_output.name):
                os.unlink(temp_output.name)
            return None

    def send_periodic_donation_nudge(self, to_number):
        """Send a donation nudge message to the user with probability 1/nudge_interval."""
        if random.random() >= (1.0 / self.nudge_interval):
            return
        
        self.logger.info(f"Sending donation nudge to {to_number}")
        donation_message = (
            "אליעזר, וכל פרויקט ivrit.ai, אינם למטרות רווח ומבוססים על תרומות מהציבור.\n"
            "אם נהניתם מהשירות, נודה לתמיכה מכם, כאן: https://patreon.com/ivrit_ai\n\n"
            "אפשר לתרום גם בפייבוקס, כאן: https://links.payboxapp.com/QzVhOJJAzVb\n\n"
            "תודה רבה! 🙏🏻"
        )
        self.send_reply(to_number, None, donation_message)

    def get_user_bucket(self, user_id):
        """Get or create a user's leaky bucket."""
        with self.bucket_lock:
            if user_id not in self.user_buckets:
                self.user_buckets[user_id] = LeakyBucket(
                    self.user_max_messages_per_hour, 
                    self.user_max_minutes_per_hour
                )
            return self.user_buckets[user_id]
    
    def cleanup_full_buckets(self):
        """Remove full buckets to avoid memory congestion."""
        with self.bucket_lock:
            self.logger.info(f"Starting bucket cleanup, {len(self.user_buckets)} buckets in memory")

            full_buckets = []
            for user_id, bucket in self.user_buckets.items():
                if bucket.is_full():
                    full_buckets.append(user_id)
            
            for user_id in full_buckets:
                del self.user_buckets[user_id]
            
            if full_buckets:
                self.logger.info(f"Cleaned up {len(full_buckets)} full buckets")

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

    def _compute_snapshot(self, now):
        """Snapshot this instance's stats (cumulative totals + windowed counts).

        Shared shape between the anon-kv flush payload and the local /status view.
        Acquires counter_lock and prunes the >24h deque tails as a side effect.
        """
        with self.counter_lock:
            # Prune entries older than 24 hours
            one_day_ago = now - 86400
            while self.message_timestamps and self.message_timestamps[0] < one_day_ago:
                self.message_timestamps.popleft()
            while self.message_lengths and self.message_lengths[0][0] < one_day_ago:
                self.message_lengths.popleft()

            timestamps = list(self.message_timestamps)
            one_minute_ago = now - 60
            five_minutes_ago = now - 300
            one_hour_ago = now - 3600

            lengths = [duration for ts, duration in self.message_lengths if ts >= one_hour_ago]
            return {
                "ts": now,
                "uptime": now - self.start_time,
                "c_messages": self.messages_handled,
                "c_transcriptions": self.transcription_counter,
                "c_duration": self.total_duration,
                "msgs_1m": sum(1 for t in timestamps if t >= one_minute_ago),
                "msgs_5m": sum(1 for t in timestamps if t >= five_minutes_ago),
                "msgs_1h": sum(1 for t in timestamps if t >= one_hour_ago),
                "msgs_24h": len(timestamps),
                "len_sum_1h": sum(lengths),
                "len_count_1h": len(lengths),
                "len_median_1h": statistics.median(lengths) if lengths else 0.0,
            }

    def _queue_depth(self):
        """Current estimated SQS backlog (visible, not-in-flight messages) as an int,
        or None if it can't be fetched. Used by /status and the status reporter heartbeat."""
        try:
            attrs = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages'],
            )
            return int(attrs['Attributes']['ApproximateNumberOfMessages'])
        except Exception as e:
            self.logger.error(f"Error fetching SQS queue depth: {str(e)}")
            return None

    def _local_view(self, snap):
        """Single-instance /status view derived from a local snapshot."""
        count = snap['len_count_1h']
        return {
            "messages_handled": snap['c_messages'],
            "total_duration": snap['c_duration'],
            "uptime_seconds": snap['uptime'],
            "mpm_1m": float(snap['msgs_1m']),
            "mpm_5m": snap['msgs_5m'] / 5.0,
            "mpm_1h": snap['msgs_1h'] / 60.0,
            "mpm_24h": snap['msgs_24h'] / 1440.0,
            "avg_length": (snap['len_sum_1h'] / count) if count else 0,
            "median_length": snap['len_median_1h'],
            "live_instances": None,
        }

    @staticmethod
    def _fmt_uptime(seconds):
        """Compact uptime, e.g. '2d 3h 5m 10s'."""
        days, remainder = divmod(int(seconds), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    @staticmethod
    def _fmt_transcribed(seconds):
        """Longform transcribed time, e.g. '1 days, 2 hours, 30 minutes'."""
        td_days, rem = divmod(int(seconds), 86400)
        td_hours, rem = divmod(rem, 3600)
        td_minutes, _ = divmod(rem, 60)
        parts = []
        if td_days > 0:
            parts.append(f"{td_days} days")
        parts.append(f"{td_hours} hours")
        parts.append(f"{td_minutes} minutes")
        return ", ".join(parts)

    def _fetch_site_stats(self):
        """GET the aggregated fleet stats from the status site. Returns the parsed dict,
        or None if the site is unreachable / returns an error (caller falls back to local)."""
        try:
            resp = requests.get(
                f"{self.status_reporter.site_url}/api/stats",
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            self.logger.debug(f"Status site /api/stats fetch failed: {str(e)}")
            return None

    def _site_view(self, stats):
        """Map the site's /api/stats JSON onto the /status view shape used below."""
        totals = stats.get('totals') or {}
        msgs = stats.get('messages') or {}
        tr = stats.get('transcriptions_1h') or {}
        uptimes = [i.get('uptime_seconds', 0) for i in (stats.get('instances') or [])]
        return {
            "messages_handled": totals.get('messages', 0),
            "total_duration": totals.get('duration_seconds', 0) or 0,
            "uptime_seconds": max(uptimes) if uptimes else 0,
            "mpm_1m": float(msgs.get('last_1m', 0)),
            "mpm_5m": (msgs.get('last_5m', 0) or 0) / 5.0,
            "mpm_1h": msgs.get('per_min_1h', 0) or 0,
            "mpm_24h": (msgs.get('last_24h', 0) or 0) / 1440.0,
            "avg_length": tr.get('avg_duration') or 0,
            "median_length": tr.get('median_duration') or 0,
            "live_instances": stats.get('live_instances'),
            "unique_users_24h": stats.get('unique_users_24h'),
        }

    def handle_status_command(self, from_number, message_id):
        """Handle the /status command by returning bot statistics.

        When the status site is configured (STATUS_SITE_URL set), report fleet-wide numbers
        fetched from the site and append the dashboard link. Otherwise (or if the fetch
        fails) report this single instance from the local windowed view.
        """
        now = time.time()

        stats = self._fetch_site_stats() if self.status_reporter.enabled else None
        if stats is not None:
            view = self._site_view(stats)
        else:
            view = self._local_view(self._compute_snapshot(now))

        queue_depth = self._queue_depth()
        if queue_depth is None:
            queue_depth = "unknown"

        uptime_str = self._fmt_uptime(view['uptime_seconds'])
        transcribed_str = self._fmt_transcribed(view['total_duration'])

        instances_line = ""
        if view.get('live_instances') is not None:
            instances_line = f"*Live instances:* {view['live_instances']}\n"
        users_line = ""
        if view.get('unique_users_24h') is not None:
            users_line = f"*Unique users (24h):* {view['unique_users_24h']}\n"

        status_text = (
            f"*Eliezer Status*\n\n"
            f"*Uptime:* {uptime_str}\n"
            f"{instances_line}"
            f"{users_line}"
            f"*Messages handled:* {view['messages_handled']}\n"
            f"*Queue depth (est.):* {queue_depth}\n"
            f"*Total time transcribed:* {transcribed_str}\n\n"
            f"*Messages/min (1m):* {view['mpm_1m']:.1f}\n"
            f"*Messages/min (5m):* {view['mpm_5m']:.1f}\n"
            f"*Messages/min (1h):* {view['mpm_1h']:.2f}\n"
            f"*Messages/min (24h):* {view['mpm_24h']:.2f}\n\n"
            f"*Avg message length (1h):* {view['avg_length']:.1f}s\n"
            f"*Median message length (1h):* {view['median_length']:.1f}s"
        )

        if self.status_reporter.enabled:
            status_text += f"\n\n📊 {self.status_reporter.site_url}"

        self.send_reply(from_number, message_id, status_text)

    def handle_detailed_status_command(self, from_number, message_id):
        """Report each host separately. When the status site is configured, list every live
        instance from /api/stats (no merge); otherwise show this host alone from local stats."""
        now = time.time()

        stats = self._fetch_site_stats() if self.status_reporter.enabled else None
        lines = ["*Eliezer Detailed Status*"]

        if stats is not None:
            totals = stats.get('totals') or {}
            td = int(totals.get('duration_seconds', 0) or 0)
            lines.append(
                f"\n*Fleet totals:* {int(totals.get('messages', 0))} msgs, "
                f"{int(totals.get('transcriptions', 0))} transcriptions, "
                f"{td // 3600}h {(td % 3600) // 60}m transcribed")

            instances = sorted(stats.get('instances') or [], key=lambda i: i.get('instance_id', ''))
            for inst in instances:
                iid = inst.get('instance_id', '?')
                marker = " (me)" if iid == self.instance_id else ""
                up = int(inst.get('uptime_seconds', 0) or 0)
                age = inst.get('age_seconds', 0)
                qd = inst.get('queue_depth')
                qd_str = "n/a" if qd is None else str(qd)
                lines.append(
                    f"\n*{iid}*{marker} — live ({int(age)}s ago)\n"
                    f"  Uptime: {up // 3600}h {(up % 3600) // 60}m\n"
                    f"  Queue depth: {qd_str}")

            if not instances:
                lines.append("\n_No live instances reporting._")

            lines.append(f"\n📊 {self.status_reporter.site_url}")
        else:
            snap = self._compute_snapshot(now)
            view = self._local_view(snap)
            up = int(view['uptime_seconds'])
            lines.append(
                f"\n*{self.instance_id}* (me)\n"
                f"  Uptime: {up // 3600}h {(up % 3600) // 60}m\n"
                f"  Messages handled: {view['messages_handled']}\n"
                f"  Msgs/min: {view['mpm_1m']:.1f} (1m) · {view['mpm_5m']:.1f} (5m) · "
                f"{view['mpm_1h']:.2f} (1h) · {view['mpm_24h']:.2f} (24h)\n"
                f"  Length (1h): avg {view['avg_length']:.1f}s · median {view['median_length']:.1f}s")

        self.send_reply(from_number, message_id, "\n".join(lines))

    def process_message(self, message):
        """Process a single WhatsApp message."""
        try:
            # Extract message details
            entry = message['entry'][0]
            changes = entry['changes'][0]
            value = changes['value']
            
            # Check if this is a message event
            if 'messages' not in value:
                return True  # Return True to delete from queue
                
            message_data = value['messages'][0]
            
            # Get message details
            from_number = message_data['from']
            message_id = message_data['id']
            job_id = str(uuid.uuid4())
            
            # Log incoming message
            self.logger.info(f"Incoming message from {from_number}")
            
            # Check if the number is allowed
            if not self.is_allowed_region(from_number):
                self.logger.info(f"Rejecting non-allowed number: {from_number}")
                self.send_reply(from_number, message_id, "מצטערים, השירות זמין רק כרגע למספרי טלפון מישראל, אירופה וצפון אמריקה.")
                return True  # Return True to delete from queue
                        
            # Initialize event properties
            event_props = {
                "user": from_number,
                "type": message_data.get('type'),
                "job_id": job_id
            }
            
            # Capture message received event
            capture_event(from_number, "message-received", event_props)

            # Track message for statistics
            with self.counter_lock:
                self.messages_handled += 1
                self.message_timestamps.append(time.time())
            self.status_reporter.record_message(
                message_data.get('type'),
                self.status_reporter.user_hash(from_number),
            )

            # Handle different message types
            message_type = message_data.get('type')
            audio_path = None
            
            # Mark message as read
            if message_type in ['audio', 'document', 'text']:
                self.mark_message_as_read(message_id)
            else:
                return True
            
            try:
                if message_type == 'audio':
                    # Process audio message
                    media_id = message_data['audio']['id']
                    self._set_activity(f"downloading audio from {from_number}")
                    audio_path = self.download_audio(media_id)
                elif message_type == 'document':
                    # Try to convert document to Opus
                    media_id = message_data['document']['id']
                    self._set_activity(f"converting document from {from_number}")
                    audio_path = self.convert_document_to_opus(media_id)
                    if audio_path:
                        message_type = 'audio'  # Update type for further processing
                else:
                    # Check for magic words
                    text_body = message_data.get('text', {}).get('body', '').strip()
                    if text_body == '/status':
                        self.handle_status_command(from_number, message_id)
                        return True
                    if text_body == '/detailed-status':
                        self.handle_detailed_status_command(from_number, message_id)
                        return True
                    self.logger.info(f"Ignoring non-voice message of type: {message_type} from {from_number}")
                    self.send_reply(from_number, message_id, "נכון להיום אני יודע לתמלל הקלטות, לא מעבר לזה.")
                    return True
                
                if not audio_path:
                    self.logger.info(f"Could not process message of type: {message_type} from {from_number}")
                    self.send_reply(from_number, message_id, "נכון להיום אני יודע לתמלל הקלטות, לא מעבר לזה.")
                    return True
                
                # Process the audio file
                try:
                    # Check audio duration first
                    duration = self.check_audio_duration(audio_path)
                    if duration is None:
                        self.logger.error(f"Failed to get duration for audio from {from_number}")
                        self.send_reply(from_number, message_id, "אירעה שגיאה בבדיקת אורך הקובץ.")
                        return True
                    
                    # Log duration
                    self.logger.info(f"Audio duration for {from_number}: {duration:.2f} seconds")
                    event_props["audio_duration_seconds"] = duration
                    
                    # Check if audio is longer than 10 minutes (600 seconds)
                    if duration > 600:
                        self.logger.info(f"Audio from {from_number} too long: {duration:.2f} seconds")
                        self.send_reply(from_number, message_id, "אני מתנצל, אך קיבלתי הנחיה שלא לתמלל קבצים שארוכים מ-10 דקות.")
                        return True
                    
                    # Check rate limits using leaky bucket
                    user_bucket = self.get_user_bucket(from_number)
                    if not user_bucket.can_transcribe(duration):
                        self.logger.info(f"Rate limit exceeded for {from_number}")
                        
                        # Calculate remaining time in minutes (approximate)
                        if user_bucket.messages_remaining == 0:
                            remaining_time = 1 / self.user_max_messages_per_hour
                        else:
                            # Must be time limit that's causing the issue
                            remaining_time = (duration - user_bucket.seconds_remaining) / (self.user_max_minutes_per_hour * 60)
                        
                        remaining_minutes = max(1, int(remaining_time * 60))
                        
                        # Capture rate limit hit event
                        limit_hit_props = {
                            "user": from_number,
                            "messages_remaining": user_bucket.messages_remaining,
                            "seconds_remaining": user_bucket.seconds_remaining,
                            "requested_duration": duration,
                            "job_id": job_id
                        }
                        capture_event(from_number, "rate-limit-hit", limit_hit_props)
                        
                        self.send_reply(from_number, message_id, 
                            f"מצטערים, אך הגעת למגבלת השימוש של השירות. "
                            f"ניתן לנסות שוב בעוד כ-{remaining_minutes} דקות.")
                        return True
                    
                    # If audio is valid, proceed with processing
                    self.logger.info(f"Starting transcription for {from_number}")
                    
                    # Record start time for transcription
                    self.send_typing_indicator(message_id)
                    transcription_start = datetime.now(timezone.utc)

                    self._set_activity(f"transcribing audio from {from_number}")
                    response_text = self.process_audio_message(audio_path)
                    
                    # Calculate transcription time
                    transcription_time = (datetime.now(timezone.utc) - transcription_start).total_seconds()
                    
                    # Consume from the user's bucket
                    has_resources_left = user_bucket.consume(duration)
                    
                    # Increment counter and get current count
                    with self.counter_lock:
                        self.transcription_counter += 1
                        self.total_duration += duration
                        self.message_lengths.append((time.time(), duration))
                        current_count = self.transcription_counter
                        total_duration_minutes = self.total_duration / 60
                    self.status_reporter.record_transcription(
                        duration, 'audio', self.status_reporter.user_hash(from_number),
                    )

                    # Capture transcription event
                    transcription_props = {
                        "user": from_number,
                        "audio_duration_seconds": duration,
                        "transcription_seconds": transcription_time,
                        "has_resources_left": has_resources_left,
                        "job_id": job_id
                    }
                    capture_event(from_number, "transcribe-done", transcription_props)
                    
                    self.logger.info(f"Completed transcription #{current_count} for {from_number} (Total duration: {total_duration_minutes:.1f} minutes)")
                    # We used to have an awesome speaking header silhouette.
                    # Removed so people can copy-and-paste the transcription easily.
                    #response_text = "\N{SPEAKING HEAD IN SILHOUETTE}\N{MEMO}: " + response_text
                    self._set_activity(f"sending reply to {from_number}")
                    self.send_reply(from_number, message_id, response_text)

                    # Send donation nudge with probability 1/nudge_interval
                    self.send_periodic_donation_nudge(from_number)
                    
                    # Perform deterministic cleanup after sending all messages
                    if self.transcription_counter % self.cleanup_frequency == 0:
                        self.cleanup_full_buckets()
                        
                finally:
                    # Ensure the audio file is deleted even if processing fails
                    if audio_path and os.path.exists(audio_path):
                        os.unlink(audio_path)
                
                return True
            except Exception as e:
                self.logger.error(f"Error processing message: {str(e)}")
                if audio_path and os.path.exists(audio_path):
                    os.unlink(audio_path)
                return False
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            return False

    def dispatcher(self):
        """Single thread that reads SQS and feeds jobs to the worker pool."""
        threading.current_thread().name = "Dispatcher"
        self.logger.info("Dispatcher thread started")

        while not self.stop_event.is_set():
            try:
                free = self.job_queue.maxsize - self.job_queue.qsize()

                # Overflow mode: only take messages above the threshold.
                if self.overflow_handler is not None:
                    self._set_activity("probing queue depth")
                    attrs = self.sqs.get_queue_attributes(
                        QueueUrl=self.queue_url,
                        AttributeNames=['ApproximateNumberOfMessages'],
                    )
                    pending = int(attrs['Attributes']['ApproximateNumberOfMessages'])
                    free = min(free, pending - self.overflow_handler)
                    self.logger.debug(
                        f"Queue depth {pending}, overflow threshold "
                        f"{self.overflow_handler}, can pull {free}")

                if free < 1:
                    # No room locally, or at/below the overflow threshold.
                    self.stop_event.wait(IDLE_PROBE_INTERVAL)
                    continue

                self._set_activity("polling SQS")
                # Pull up to one job per worker thread this cycle. SQS caps a single
                # ReceiveMessage at 10, so loop to reach the target: long-poll the
                # first request, then short-poll the rest, stopping when drained.
                target = min(free, self.num_worker_threads)
                fetched = 0
                wait = 20
                while fetched < target and not self.stop_event.is_set():
                    response = self.sqs.receive_message(
                        QueueUrl=self.queue_url,
                        MaxNumberOfMessages=min(target - fetched, 10),
                        WaitTimeSeconds=wait
                    )
                    messages = response.get('Messages', [])
                    if not messages:
                        break
                    for message in messages:
                        self.job_queue.put(message)  # guaranteed room -> won't block
                    fetched += len(messages)
                    wait = 0

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
                    message = self.job_queue.get(timeout=1)
                except queue.Empty:
                    continue

                try:
                    # Parse message body
                    message_body = json.loads(message['Body'])

                    # Process the message
                    self._set_activity("processing message")
                    if self.process_message(message_body):
                        # Delete message from queue if processed successfully
                        self.sqs.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                finally:
                    self.job_queue.task_done()

            except Exception as e:
                self.logger.error(f"Error in worker thread: {str(e)}")
                time.sleep(5)  # Wait a bit before retrying
                continue

    def run(self):
        """Start worker threads to poll SQS."""
        # Set main thread name
        threading.current_thread().name = "Main"
        
        self.logger.info(f"Starting WhatsApp bot with {self.num_worker_threads} worker threads "
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
        
        # Start dispatcher thread (sole SQS reader, feeds the worker pool)
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

        # Start status reporter thread (POSTs per-event metadata to the Eliezer Status site).
        # No-op when STATUS_SITE_URL is unset.
        if self.status_reporter.enabled:
            reporter_thread = threading.Thread(
                target=self.status_reporter.run,
                name="StatusReporter",
                daemon=True
            )
            reporter_thread.start()
            self.logger.info(f"Started status reporter thread (instance_id={self.instance_id})")

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
    parser = argparse.ArgumentParser(description='WhatsApp Bot for audio transcription')
    parser.add_argument('--nudge-interval', type=int, default=100, help='Interval for donation nudges (1:N probability)')
    parser.add_argument('--user-max-messages-per-hour', type=float, default=10, help='Maximum messages per hour per user')
    parser.add_argument('--user-max-minutes-per-hour', type=float, default=20, help='Maximum audio minutes per hour per user')
    parser.add_argument('--cleanup-frequency', type=int, default=50, help='Perform bucket cleanup every N transcriptions')
    parser.add_argument('--num-workers', type=int, default=None, help='Number of concurrent transcriptions (default: 10, or 1 in --local mode); the bot runs 2x this many worker threads to overlap I/O')
    parser.add_argument('--local', action='store_true', help='Transcribe locally with faster-whisper instead of RunPod')
    parser.add_argument('--overflow-handler', type=int, default=None, metavar='N', help='Only handle jobs when the SQS queue depth exceeds N')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging (verbose output to console and file)')
    args = parser.parse_args()

    # Resolve worker count: explicit value wins, else 1 locally / 10 remotely.
    num_workers = args.num_workers
    if num_workers is None:
        num_workers = 1 if args.local else 10

    # Initialize and run the bot
    bot = WhatsAppBot(
        nudge_interval=args.nudge_interval,
        user_max_messages_per_hour=args.user_max_messages_per_hour,
        user_max_minutes_per_hour=args.user_max_minutes_per_hour,
        cleanup_frequency=args.cleanup_frequency,
        num_workers=num_workers,
        local=args.local,
        overflow_handler=args.overflow_handler,
        debug=args.debug
    )
    bot.run()
