import os.path
import pickle
import sqlite3
import base64
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def authenticate_gmail():
    """Authenticate and create a Gmail API service."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('gmail', 'v1', credentials=creds)
    return service


def get_message_payload(message):
    """Extracts the payload from a message."""
    payload = message['payload']
    if 'data' in payload['body']:
        return payload['body']['data']
    elif 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                return part['body']['data']
    return None


def fetch_emails(service):
    """Fetch emails from Gmail and store them in SQLite."""
    results = service.users().messages().list(userId='me', labelIds=['INBOX']).execute()
    messages = results.get('messages', [])

    conn = sqlite3.connect('emails.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS emails
                 (id TEXT PRIMARY KEY, sender TEXT, subject TEXT, message TEXT, received_at TEXT)''')

    for msg in messages:
        msg_id = msg['id']
        msg_data = service.users().messages().get(userId='me', id=msg_id).execute()
        headers = msg_data['payload']['headers']
        sender = subject = ""
        for header in headers:
            if header['name'] == 'From':
                sender = header['value']
            if header['name'] == 'Subject':
                subject = header['value']

        payload_data = get_message_payload(msg_data)
        if payload_data:
            message = base64.urlsafe_b64decode(payload_data.encode('UTF-8')).decode('UTF-8')
        else:
            message = ""

        received_at = msg_data['internalDate']
        c.execute("INSERT OR IGNORE INTO emails (id, sender, subject, message, received_at) VALUES (?, ?, ?, ?, ?)",
                  (msg_id, sender, subject, message, received_at))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    service = authenticate_gmail()
    fetch_emails(service)
