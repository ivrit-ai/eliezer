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

class WhatsAppBot:
    def __init__(self):
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
            'context': {
                'message_id': message_id
            }
        }
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

    def process_message(self, message, queue_timestamp):
        """Process a single WhatsApp message."""
        try:
            # Extract message details
            entry = message['entry'][0]
            changes = entry['changes'][0]
            value = changes['value']
            
            # Check if this is a message event
            if 'messages' not in value:
                logging.debug("Ignoring non-message event")
                return True  # Return True to delete from queue
                
            message_data = value['messages'][0]
            
            # Get message details
            from_number = message_data['from']
            message_id = message_data['id']
            job_id = str(uuid.uuid4())
            
            # Log incoming message
            logging.info(f"Incoming message from {from_number} - Message ID: {message_id}")
            
            # Mark message as read
            self.mark_message_as_read(message_id)
            
            # Initialize event properties
            event_props = {
                "user": from_number,
                "type": message_data.get('type')
            }
            
            # Capture message received event
            capture_event(job_id, "message-received", event_props)
            
            # Only process voice messages
            if message_data.get('type') != 'audio':
                logging.info(f"Ignoring non-voice message of type: {message_data.get('type')} from {from_number}")
                # Reply to non-audio messages
                self.send_reply(from_number, message_id, "נכון להיום אני יודע לתמלל הקלטות, לא מעבר לזה.")
                return True  # Return True to delete from queue
            
            # Process audio message
            media_id = message_data['audio']['id']
            
            # Download and process the audio
            audio_path = self.download_audio(media_id)
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
                
                # If audio is valid, proceed with processing
                self.send_reply(from_number, message_id, "אני על זה!")
                logging.info(f"Starting transcription for {from_number}")
                
                # Record start time for transcription
                transcription_start = datetime.now(timezone.utc)
                
                response_text = self.process_audio_message(audio_path)
                
                # Calculate transcription time
                transcription_time = (datetime.now(timezone.utc) - transcription_start).total_seconds()
                
                # Capture transcription event
                transcription_props = {
                    "user": from_number,
                    "audio_duration_seconds": duration,
                    "transcription_seconds": transcription_time
                }
                capture_event(job_id, "transcribe-done", transcription_props)
                
                logging.info(f"Completed transcription for {from_number}")
                response_text = "\N{SPEAKING HEAD IN SILHOUETTE}\N{MEMO}: " + response_text
                self.send_reply(from_number, message_id, response_text)
            finally:
                # Ensure the audio file is deleted even if processing fails
                if os.path.exists(audio_path):
                    os.unlink(audio_path)
            
            return True
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
    bot = WhatsAppBot()
    bot.run() 