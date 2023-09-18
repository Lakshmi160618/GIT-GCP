import base64
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def send_email(sender, to, subject, message_body):
    try:
        service = build('gmail', 'v1')
        message = create_message(sender, to, subject, message_body)
        send_message(service, 'me', message)
        print("Email sent successfully!")
    except HttpError as error:
        print(f"An error occurred: {error}")

def create_message(sender, to, subject, message_text):
    message = {
        'raw': base64.urlsafe_b64encode(
            f"From: {sender}\nTo: {to}\nSubject: {subject}\n\n{message_text}".encode("utf-8")
        ).decode("utf-8")
    }
    return message

def send_message(service, user_id, message):
    service.users().messages().send(userId=user_id, body=message).execute()

# Call the function to send an email
send_email('your_email@gmail.com', 'recipient_email@gmail.com', 'Hello', 'This is a test email.')

'''
This example demonstrates a function, send_email, that uses the Gmail API to send an email. It includes functions for creating and sending the email message.
'''