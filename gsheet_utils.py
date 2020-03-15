from pathlib import Path
import logging.config
from typing import List, Any

import yaml
from google.oauth2 import service_account
from googleapiclient.discovery import build

from credentials.credentials import SPREADSHEET_ID


# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


def build_service():
    '''Connect to the Google sheets API.'''
    
    SERVICE_ACCOUNT_FILE = Path(__file__).parent / 'credentials' / 'sa_client_secret.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    # get autorization credentials
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    # build connection to Google services
    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
    
    # call the Sheets API
    sheet = service.spreadsheets()
    return sheet


def check_author() -> str:
    '''Check the name of the selected author.'''
    
    sheet = build_service()
    author = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='display!A2:A').execute()
    author = author.get('values', [])
    if author == []:
        logger.error('No author\'s name provided. Don\'t forget to select the name.')
        return
    else:
        author = author[0][0].strip()
    return author

def paste_books(data: List[List[Any]]):
    '''Fill Google sheet with books data.'''
    
    sheet = build_service()
    # first clear space
    books = sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range='display!A7:C').execute()
    result = sheet.values().update(spreadsheetId=SPREADSHEET_ID, range='display!A7:C',
                                   valueInputOption='USER_ENTERED', body={'values':data}).execute()