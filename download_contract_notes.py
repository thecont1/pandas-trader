import os
import pickle
import base64
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os.path
import argparse

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
DEFAULT_DOWNLOAD_DIR = os.path.expanduser('~/Downloads/Contract Notes')

# Broker configurations
BROKERS = [
    {
        'name': 'Zerodha',
        'domain': 'zerodha.com',
        'subject': 'Contract Note',
        'enabled': False
    },
    {
        'name': 'PayTM Money',
        'domain': 'paytmmoney.com',
        'subject': 'Trade Successful - Consolidated Contract Note',
        'enabled': True
    },
    {
        'name': 'Dhan',
        'domain': 'dhan.co',
        'subject': 'Contract Note (Cash F&O and Currency) - Trade',
        'enabled': False  # Disabled for now
    }
]

def get_gmail_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds)

def get_query_for_broker(broker):
    # Constructs the Gmail query string for a broker
    return f'from:{broker["domain"]} subject:"{broker["subject"]}"'

def parse_email_date(date_str):
    """Parse email date string, handling various formats"""
    try:
        # Try different date formats
        formats = [
            '%a, %d %b %Y',  # Mon, 11 Dec 2023
            '%d %b %Y',      # 11 Dec 2023
            '%Y-%m-%d',      # 2023-12-11
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str.split()[0:4], fmt)
            except (ValueError, IndexError):
                continue
        
        # If no format matches, return None
        return None
    except Exception:
        return None

def download_attachments(query="from:zerodha.com subject:Contract Note", 
                        download_dir=DEFAULT_DOWNLOAD_DIR,
                        days_limit=7):
    """
    Downloads attachments from emails matching the query criteria
    
    Args:
        query (str): Gmail search query
        download_dir (str): Directory to save attachments
        days_limit (int): Number of days to look back for emails
    """
    service = get_gmail_service()
    
    # Create download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)
    
    # Add date filter to query
    date_limit = datetime.now() - timedelta(days=days_limit)
    date_str = date_limit.strftime('%Y/%m/%d')
    query = f"{query} after:{date_str}"
    
    # Get messages matching the query
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print('No messages found.')
        return

    print(f'Found {len(messages)} messages matching criteria...')
    
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id']).execute()
        
        # Get email date
        email_date = None
        for header in msg['payload']['headers']:
            if header['name'] == 'Date':
                email_date = parse_email_date(header['value'])
                break
        
        # Check if message has parts
        if 'parts' not in msg['payload']:
            continue

        for part in msg['payload']['parts']:
            if part.get('filename'):
                if 'data' in part['body']:
                    data = part['body']['data']
                else:
                    att_id = part['body']['attachmentId']
                    att = service.users().messages().attachments().get(
                        userId='me', messageId=message['id'], id=att_id).execute()
                    data = att['data']
                
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                
                # Create filename with date prefix
                filename = part.get('filename')
                if email_date:
                    date_prefix = email_date.strftime('%Y-%m-%d_')
                    if not filename.startswith(date_prefix):
                        filename = f"{date_prefix}{filename}"
                
                filepath = os.path.join(download_dir, filename)
                
                # Save the attachment
                with open(filepath, 'wb') as f:
                    f.write(file_data)
                print(f'Downloaded: {filename}')

def main():
    parser = argparse.ArgumentParser(description='Download Gmail attachments matching specific criteria')
    parser.add_argument('--limit', type=int, default=7,
                      help='Number of days to look back for emails (default: 7)')
    args = parser.parse_args()

    # Process each enabled broker
    for broker in BROKERS:
        if broker['enabled']:
            print(f"\nProcessing {broker['name']} contract notes...")
            download_attachments(
                query=get_query_for_broker(broker),
                download_dir=DEFAULT_DOWNLOAD_DIR,
                days_limit=args.limit
            )

if __name__ == '__main__':
    main()
