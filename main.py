import json
import sqlite3
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pickle
import os.path


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']


def authenticate_gmail():
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


def load_rules():
    """Load rules from the JSON file."""
    with open('rules.json', 'r') as file:
        return json.load(file)


def construct_query(rules):
    conditions = []
    for condition in rules['Conditions']:
        field = condition['Field_name'].lower()
        predicate = condition['Predicate']
        value = condition['Value']

        if field == "from":
            field = "sender"

        if predicate == 'contains':
            conditions.append(f"{field} LIKE '%{value}%'")
        elif predicate == 'does not contain':
            conditions.append(f"{field} NOT LIKE '%{value}%'")
        elif predicate == 'equals':
            conditions.append(f"{field} = '{value}'")
        elif predicate == 'does not equal':
            conditions.append(f"{field} != '{value}'")
        elif predicate == 'less than':
            conditions.append(f"{field} < '{value}'")
        elif predicate == 'greater than':
            conditions.append(f"{field} > '{value}'")

    query = "SELECT * FROM emails WHERE "
    query += f" {' AND ' if rules['All_or_Any'] == 'all' else ' OR '}".join(conditions)
    return query


def process_emails(rules, service):
    conn = sqlite3.connect('emails.db')
    c = conn.cursor()

    query = construct_query(rules)
    c.execute(query)
    emails = c.fetchall()

    for email in emails:
        email_id = email[0]
        for condition in rules['Conditions']:
            if condition['Action'] == 'STARRED':
                service.users().messages().modify(
                    userId='me',
                    id=email_id,
                    body={'addLabelIds': ['STARRED']}
                ).execute()
                print(f"Email ID: {email_id} has been starred.")

    conn.close()


if __name__ == '__main__':
    service = authenticate_gmail()
    rules = load_rules()
    process_emails(rules, service)
