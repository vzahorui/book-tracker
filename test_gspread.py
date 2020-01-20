import os

from google.oauth2 import service_account
from googleapiclient.discovery import build

from credentials.credentials import SPREADSHEET_ID


def main():
    
    SERVICE_ACCOUNT_FILE = os.path.join('credentials', 'sa_client_secret.json')
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    # get autorization credentials
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    # build connection to Google services
    service = build('sheets', 'v4', credentials=credentials)
    
    # call the Sheets API
    sheet = service.spreadsheets()
    # get pages to crawl
    pages_to_crawl_data = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='pages!A2:A').execute()
    pages_to_crawl = pages_to_crawl_data.get('values', [])
    pages_to_crawl = [i[0] for i in pages_to_crawl]

    return pages_to_crawl



    
if __name__ == "__main__":
    main()
    
