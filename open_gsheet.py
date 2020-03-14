import webbrowser

from credentials.credentials import SPREADSHEET_ID

def open_gsheet():
    url = 'https://docs.google.com/spreadsheets/d/' + SPREADSHEET_ID
    webbrowser.open(url)
    
if __name__ == '__main__':
    open_gsheet()