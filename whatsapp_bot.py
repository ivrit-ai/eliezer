import os
import json
import boto3
import requests
import tempfile
import base64
import runpod
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

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
                'model': 'ivrit-ai/whisper-large-v3-turbo-ct2',
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

    def process_audio_message(self, audio_path):
        """Process the audio message and generate a response."""
        try:
            # Transcribe the audio
            transcription = self.transcribe_audio(audio_path)
            
            # For now, just return the transcription
            # You can add more processing here if needed
            return transcription
        finally:
            # Clean up the temporary file
            if os.path.exists(audio_path):
                os.unlink(audio_path)

    def process_message(self, message):
        """Process a single WhatsApp message."""
        try:
            # Extract message details
            entry = message['entry'][0]
            changes = entry['changes'][0]
            value = changes['value']
            
            # Check if this is a message event
            if 'messages' not in value:
                print("Ignoring non-message event")
                return True  # Return True to delete from queue
                
            message_data = value['messages'][0]
            
            # Get message details
            from_number = message_data['from']
            message_id = message_data['id']
            
            # Mark message as read
            self.mark_message_as_read(message_id)
            
            # Only process voice messages
            if message_data.get('type') != 'audio':
                print(f"Ignoring non-voice message of type: {message_data.get('type')}")
                # Reply to non-audio messages
                self.send_reply(from_number, message_id, "מה אני אמור לעשות עם זה?")
                return True  # Return True to delete from queue
            
            # Process audio message
            media_id = message_data['audio']['id']
            
            # Download and process the audio
            audio_path = self.download_audio(media_id)
            try:
                self.send_reply(from_number, message_id, "אני על זה!")

                response_text = "\N{SPEAKING HEAD IN SILHOUETTE}\N{MEMO}: "
                response_text +=self.process_audio_message(audio_path)

                self.send_reply(from_number, message_id, response_text)
            finally:
                # Ensure the audio file is deleted even if processing fails
                if os.path.exists(audio_path):
                    os.unlink(audio_path)
            
            return True
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            return False

    def run(self):
        """Main loop to process messages from SQS."""
        print("Starting WhatsApp bot...")
        while True:
            try:
                print("Waiting for message from SQS...")

                # Receive message from SQS
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                #print(f'Received message from SQS: {response}')

                if 'Messages' in response:
                    #print(f'Messages in response: {response["Messages"]}')

                    for message in response['Messages']:
                        #print(f'Processing message: {message}')
                        # Parse message body
                        message_body = json.loads(message['Body'])
                        
                        # Process the message
                        if self.process_message(message_body):
                            # Delete message from queue if processed successfully
                            self.sqs.delete_message(
                                QueueUrl=self.queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                
            except Exception as e:
                print(f"Error in main loop: {str(e)}")
                continue

if __name__ == "__main__":
    bot = WhatsAppBot()
    bot.run() 