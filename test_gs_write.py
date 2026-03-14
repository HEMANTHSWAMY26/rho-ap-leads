import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def test_write():
    try:
        json_key = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        
        print(f"Testing write access for Sheet ID: {sheet_id}")
        print(f"Using credentials: {json_key}")
        
        if json_key.startswith("{"):
            import json
            creds = Credentials.from_service_account_info(json.loads(json_key), scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file(json_key, scopes=SCOPES)
            
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet("job_leads_master")
        
        # Try to append a dummy row
        test_row = ["DUMMY_TITLE", "DUMMY_COMPANY", "DUMMY_LOCATION", "", "", "", "", "", "", "2026-03-13", ""]
        print("Attempting to append a test row...")
        worksheet.append_row(test_row)
        print("Success! Write permission is active.")
        
        # Delete the test row if possible
        # worksheet.delete_rows(worksheet.row_count)
        
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_write()
