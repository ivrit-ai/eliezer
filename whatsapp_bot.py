import os
import json
import boto3
import requests
import tempfile
import base64
import runpod
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import logging
from datetime import datetime, timezone
import threading
import time
import posthog
import uuid
import random
import argparse
from pydub import AudioSegment
import collections

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('whatsapp_bot.log'),
        logging.StreamHandler()
    ]
)

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
    def __init__(self, nudge_interval, user_max_messages_per_hour, user_max_minutes_per_hour, cleanup_frequency):
        self.sqs = boto3.client('sqs')
        self.queue_url = os.getenv('APP_SQS_QUEUE')
        self.api_token = os.getenv('WHATSAPP_API_TOKEN')
        self.phone_number_id = os.getenv('WHATSAPP_PHONE_NUMBER_ID')
        self.api_version = 'v22.0'
        self.base_url = f'https://graph.facebook.com/{self.api_version}'
        
        # Initialize RunPod
        runpod.api_key = os.getenv('RUNPOD_API_KEY')
        self.runpod_endpoint = runpod.Endpoint(os.getenv('RUNPOD_ENDPOINT_ID'))
        
        # Thread control
        self.stop_event = threading.Event()
        self.worker_threads = []
        self.num_workers = 3
        
        # Nudge interval for donation messages
        self.nudge_interval = nudge_interval
        
        # Transcription counter and duration tracker
        self.transcription_counter = 0
        self.total_duration = 0
        self.counter_lock = threading.Lock()
        
        # Leaky bucket rate limiter settings
        self.user_max_messages_per_hour = user_max_messages_per_hour
        self.user_max_minutes_per_hour = user_max_minutes_per_hour
        self.cleanup_frequency = cleanup_frequency
        
        # User buckets with lock for thread safety
        self.user_buckets = {}
        self.bucket_lock = threading.Lock()

    def is_israeli_number(self, phone_number):
        """Check if the phone number is an Israeli number (starts with 972)."""
        return phone_number.startswith('972')

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
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    def send_reply(self, to_number, message_id, text):
        """Send a reply message with quote."""
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

        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    def download_audio(self, media_id):
        """Download audio file from WhatsApp."""
        # First, get the media URL
        url = f'{self.base_url}/{media_id}'
        headers = {
            'Authorization': f'Bearer {self.api_token}'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        media_url = response.json()['url']

        # Download the actual media file
        response = requests.get(media_url, headers=headers)
        response.raise_for_status()
        
        # Create a temporary file with .ogg extension (WhatsApp voice messages are in OGG format)
        temp_file = tempfile.NamedTemporaryFile(suffix='.ogg', delete=False)
        temp_file.write(response.content)
        temp_file.close()
        
        return temp_file.name

    def transcribe_audio(self, audio_path):
        """Transcribe audio using RunPod."""
        try:
            # Read the audio file
            with open(audio_path, 'rb') as audio_file:
                audio_data = audio_file.read()
            
            # Encode the audio data as base64
            encoded_data = base64.b64encode(audio_data).decode('utf-8')
            
            # Prepare the payload for RunPod
            payload = {
                'type': 'blob',
                'data': encoded_data,
                'model': 'ivrit-ai/whisper-large-v3-turbo_20250403_rc0-ct2',
                'engine': 'faster-whisper'
            }
            
            # Run the transcription
            result = self.runpod_endpoint.run_sync(payload)
            
            # Extract the transcription from the result
            if len(result) == 1 and 'result' in result[0]:
                text_result = '\n'.join([item['text'].strip() for item in result[0]['result']])
                return text_result
            else:
                print(f"Unexpected RunPod response format: {result}")
                return "לא הצלחתי להבין את ההודעה הקולית."
        except Exception as e:
            print(f"Error transcribing audio: {str(e)}")
            return "אירעה שגיאה בעיבוד ההודעה הקולית."

    def check_audio_duration(self, audio_path):
        """Get the duration of an audio file in seconds using ffprobe."""
        try:
            cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', audio_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            duration = float(result.stdout.strip())
            return duration
        except Exception as e:
            logging.error(f"Error checking audio duration: {str(e)}")
            return None

    def process_audio_message(self, audio_path):
        """Process the audio message and generate a response."""
        try:
            # Transcribe the audio
            transcription = self.transcribe_audio(audio_path)
            return transcription
        finally:
            # Clean up the temporary file
            if os.path.exists(audio_path):
                os.unlink(audio_path)

    def convert_document_to_mp3(self, document_id):
        """Convert a document to MP3 format."""
        try:
            # First, get the document URL
            url = f'{self.base_url}/{document_id}'
            headers = {
                'Authorization': f'Bearer {self.api_token}'
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            media_url = response.json()['url']

            # Download the document
            response = requests.get(media_url, headers=headers)
            response.raise_for_status()
            
            # Create temporary files
            temp_input = tempfile.NamedTemporaryFile(delete=False)
            temp_output = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
            
            try:
                # Save the downloaded document
                temp_input.write(response.content)
                temp_input.close()
                
                # Convert to MP3
                audio = AudioSegment.from_file(temp_input.name)
                audio.export(temp_output.name, format="mp3")
                
                return temp_output.name
            finally:
                # Clean up the input file
                if os.path.exists(temp_input.name):
                    os.unlink(temp_input.name)
                
        except Exception as e:
            logging.error(f"Error converting document to MP3: {str(e)}")
            # Clean up output file if it exists
            if 'temp_output' in locals() and os.path.exists(temp_output.name):
                os.unlink(temp_output.name)
            return None

    def send_periodic_donation_nudge(self, to_number, message_id):
        """Send a donation nudge message to the user with probability 1/nudge_interval."""
        if random.random() >= (1.0 / self.nudge_interval):
            return
        
        logging.info(f"Sending donation nudge to {to_number}")
        donation_message = (
            "אליעזר, וכל פרויקט ivrit.ai, אינם למטרות רווח ומבוססים על תרומות מהציבור.\n"
            "אם נהניתם מהשירות, נודה לתמיכה מכם, כאן: https://patreon.com/ivrit_ai\n\n"          
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
            logging.info(f"Starting bucket cleanup, {len(self.user_buckets)} buckets in memory")

            full_buckets = []
            for user_id, bucket in self.user_buckets.items():
                if bucket.is_full():
                    full_buckets.append(user_id)
            
            for user_id in full_buckets:
                del self.user_buckets[user_id]
            
            if full_buckets:
                logging.info(f"Cleaned up {len(full_buckets)} full buckets")

    def process_message(self, message, queue_timestamp):
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
            logging.info(f"Incoming message from {from_number} - Message ID: {message_id}")
            
            # Check if the number is Israeli
            if not self.is_israeli_number(from_number):
                logging.info(f"Rejecting non-Israeli number: {from_number}")
                self.send_reply(from_number, message_id, "מצטערים, השירות זמין רק למספרי טלפון ישראליים.")
                return True  # Return True to delete from queue
            
            # Mark message as read
            self.mark_message_as_read(message_id)
            
            # Initialize event properties
            event_props = {
                "user": from_number,
                "type": message_data.get('type')
            }
            
            # Capture message received event
            capture_event(job_id, "message-received", event_props)
            
            # Handle different message types
            message_type = message_data.get('type')
            audio_path = None
            
            try:
                if message_type == 'audio':
                    # Process audio message
                    media_id = message_data['audio']['id']
                    audio_path = self.download_audio(media_id)
                elif message_type == 'document':
                    # Try to convert document to MP3
                    media_id = message_data['document']['id']
                    audio_path = self.convert_document_to_mp3(media_id)
                    if audio_path:
                        message_type = 'audio'  # Update type for further processing
                else:
                    logging.info(f"Ignoring non-voice message of type: {message_type} from {from_number}")
                    self.send_reply(from_number, message_id, "נכון להיום אני יודע לתמלל הקלטות, לא מעבר לזה.")
                    return True
                
                if not audio_path:
                    logging.info(f"Could not process message of type: {message_type} from {from_number}")
                    self.send_reply(from_number, message_id, "נכון להיום אני יודע לתמלל הקלטות, לא מעבר לזה.")
                    return True
                
                # Process the audio file
                try:
                    # Check audio duration first
                    duration = self.check_audio_duration(audio_path)
                    if duration is None:
                        logging.error(f"Failed to get duration for audio from {from_number}")
                        self.send_reply(from_number, message_id, "אירעה שגיאה בבדיקת אורך הקובץ.")
                        return True
                    
                    # Log duration
                    logging.info(f"Audio duration for {from_number}: {duration:.2f} seconds")
                    event_props["audio_duration_seconds"] = duration
                    
                    # Check if audio is longer than 5 minutes (300 seconds)
                    if duration > 300:
                        logging.info(f"Audio from {from_number} too long: {duration:.2f} seconds")
                        self.send_reply(from_number, message_id, "אני מתנצל, אך קיבלתי הנחיה שלא לתמלל קבצים שארוכים מ-5 דקות.")
                        return True
                    
                    # Check rate limits using leaky bucket
                    user_bucket = self.get_user_bucket(from_number)
                    if not user_bucket.can_transcribe(duration):
                        logging.info(f"Rate limit exceeded for {from_number}")
                        
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
                            "requested_duration": duration
                        }
                        capture_event(job_id, "rate-limit-hit", limit_hit_props)
                        
                        self.send_reply(from_number, message_id, 
                            f"מצטערים, אך הגעת למגבלת השימוש של השירות. "
                            f"ניתן לנסות שוב בעוד כ-{remaining_minutes} דקות.")
                        return True
                    
                    # If audio is valid, proceed with processing
                    self.send_reply(from_number, message_id, "אני על זה!")
                    logging.info(f"Starting transcription for {from_number}")
                    
                    # Record start time for transcription
                    transcription_start = datetime.now(timezone.utc)
                    
                    response_text = self.process_audio_message(audio_path)
                    
                    # Calculate transcription time
                    transcription_time = (datetime.now(timezone.utc) - transcription_start).total_seconds()
                    
                    # Consume from the user's bucket
                    has_resources_left = user_bucket.consume(duration)
                    
                    # Increment counter and get current count
                    with self.counter_lock:
                        self.transcription_counter += 1
                        self.total_duration += duration
                        current_count = self.transcription_counter
                        total_duration_minutes = self.total_duration / 60

                    # Capture transcription event
                    transcription_props = {
                        "user": from_number,
                        "audio_duration_seconds": duration,
                        "transcription_seconds": transcription_time,
                        "has_resources_left": has_resources_left
                    }
                    capture_event(job_id, "transcribe-done", transcription_props)
                    
                    logging.info(f"Completed transcription #{current_count} for {from_number} (Total duration: {total_duration_minutes:.1f} minutes)")
                    # We used to have an awesome speaking header silhouette.
                    # Removed so people can copy-and-paste the transcription easily.
                    #response_text = "\N{SPEAKING HEAD IN SILHOUETTE}\N{MEMO}: " + response_text
                    self.send_reply(from_number, message_id, response_text)
                    
                    # Send donation nudge with probability 1/nudge_interval
                    self.send_periodic_donation_nudge(from_number, message_id)
                    
                    # Perform deterministic cleanup after sending all messages
                    if self.transcription_counter % self.cleanup_frequency == 0:
                        self.cleanup_full_buckets()
                        
                finally:
                    # Ensure the audio file is deleted even if processing fails
                    if audio_path and os.path.exists(audio_path):
                        os.unlink(audio_path)
                
                return True
            except Exception as e:
                logging.error(f"Error processing message: {str(e)}")
                if audio_path and os.path.exists(audio_path):
                    os.unlink(audio_path)
                return False
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            return False

    def worker(self, worker_id):
        """Worker thread function to poll SQS and process messages."""
        thread_name = f"Worker-{worker_id}"
        logging.info(f"Starting {thread_name}")
        
        while not self.stop_event.is_set():
            try:
                # Receive message from SQS
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                if 'Messages' in response:
                    for message in response['Messages']:
                        try:
                            # Parse message body
                            message_body = json.loads(message['Body'])
                            
                            # Process the message
                            if self.process_message(message_body, None):
                                # Delete message from queue if processed successfully
                                self.sqs.delete_message(
                                    QueueUrl=self.queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                        except Exception as e:
                            logging.error(f"Error processing message in {thread_name}: {str(e)}")
                
            except Exception as e:
                logging.error(f"Error in {thread_name}: {str(e)}")
                time.sleep(5)  # Wait a bit before retrying
                continue

    def run(self):
        """Start worker threads to poll SQS."""
        logging.info(f"Starting WhatsApp bot with {self.num_workers} workers...")
        
        # Start worker threads
        for i in range(self.num_workers):
            thread = threading.Thread(
                target=self.worker,
                args=(i,),
                name=f"Worker-{i}",
                daemon=True
            )
            self.worker_threads.append(thread)
            thread.start()
            logging.info(f"Started thread {thread.name}")
        
        try:
            # Keep the main thread alive
            while not self.stop_event.is_set():
                time.sleep(1)
                
        except KeyboardInterrupt:
            logging.info("Shutting down...")
            self.stop_event.set()
            
            # Wait for all threads to finish
            for thread in self.worker_threads:
                thread.join()
            
            logging.info("Shutdown complete")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='WhatsApp Bot for audio transcription')
    parser.add_argument('--nudge-interval', type=int, default=100, help='Interval for donation nudges (1:N probability)')
    parser.add_argument('--user-max-messages-per-hour', type=float, default=0.5, help='Maximum messages per hour per user')
    parser.add_argument('--user-max-minutes-per-hour', type=float, default=1.5, help='Maximum audio minutes per hour per user')
    parser.add_argument('--cleanup-frequency', type=int, default=50, help='Perform bucket cleanup every N transcriptions')
    args = parser.parse_args()
    
    # Initialize and run the bot
    bot = WhatsAppBot(
        nudge_interval=args.nudge_interval,
        user_max_messages_per_hour=args.user_max_messages_per_hour,
        user_max_minutes_per_hour=args.user_max_minutes_per_hour,
        cleanup_frequency=args.cleanup_frequency
    )
    bot.run() 
