import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import re

# --- CONFIGURATION ---
# Path to your Google Cloud Service Account credentials JSON file
CREDENTIALS_FILE = 'service_account.json'

# Sheet IDs (extracted from your URLs)
DEST_SHEET_ID = '1tqp6sO8FOqZ0xBlIrDKuXYTfHZCV0RRTdRFLdlxCBTQ'
SOURCE_SHEET_ID = '1wWwjvAwXCAnPH3cAXSCaXlyDA7E9h_YrP4jaNFP9qvY'

# Column Indices (0-indexed for Python)
# Destination Sheet
DEST_BRAND_COL = 0   # Column A
# Source Sheet
SRC_DATE_COL = 2     # Column C
SRC_BRAND_COL = 5    # Column F
SRC_STATUS_COL = 37  # Column AL (Corrected from AK to AL)

def normalize_brand_name(name):
    """
    Smart Normalization: Converts to lowercase, strips extra spaces, 
    and removes special characters. It keeps whole words intact to prevent 
    'Times ooh' from matching 'Times Group'.
    """
    if not isinstance(name, str) or not name.strip():
        return ""
    
    name = name.lower()
    # Replace common separators with space
    name = re.sub(r'[&\-_/|]', ' ', name)
    # Remove all other punctuation
    name = re.sub(r'[^\w\s]', '', name)
    # Remove extra whitespace between words
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def main():
    print("Authenticating with Google Sheets...")
    scope =["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)

    print("Fetching data from Google Sheets...")
    # Open Sheets and Tabs
    dest_wb = client.open_by_key(DEST_SHEET_ID)
    dest_sheet = dest_wb.worksheet('Meeting')
    
    source_wb = client.open_by_key(SOURCE_SHEET_ID)
    source_sheet = source_wb.worksheet('Meeting_data')

    # Get all values
    dest_data = dest_sheet.get_all_values()
    source_data = source_sheet.get_all_values()

    print("Processing Source Data...")
    # Create a dictionary for ultra-fast O(1) lookups
    # Format: { 'normalized_brand_name': {'status': 'Conducted', 'date': '2024-04-23'} }
    source_dict = {}
    
    # Skip header row in source (start at 1)
    for row in source_data[1:]:
        # Pad row with empty strings in case of missing trailing columns
        row.extend([''] * (max(SRC_BRAND_COL, SRC_STATUS_COL, SRC_DATE_COL) + 1 - len(row)))
        
        raw_brand = row[SRC_BRAND_COL]
        normalized_brand = normalize_brand_name(raw_brand)
        
        if normalized_brand:
            # If a brand appears multiple times, this keeps the first one found. 
            if normalized_brand not in source_dict:
                source_dict[normalized_brand] = {
                    'date': row[SRC_DATE_COL].strip(),
                    'status': row[SRC_STATUS_COL].strip()
                }

    print("Matching Brands and preparing updates...")
    update_values =[]
    
    # Skip header row in destination (start at 1)
    for row in dest_data[1:]:
        if not row:
            update_values.append(["", ""]) # Handle completely empty rows
            continue
            
        raw_dest_brand = row[DEST_BRAND_COL]
        normalized_dest_brand = normalize_brand_name(raw_dest_brand)
        
        if not normalized_dest_brand:
            update_values.append(["", ""]) # Skip if brand name is empty
            continue

        # Smart Match Lookup
        if normalized_dest_brand in source_dict:
            match_data = source_dict[normalized_dest_brand]
            status = match_data['status']
            date = match_data['date']
            
            # Ensure we only write valid statuses
            if not status: 
                status = "Status Blank in Source"
                
            update_values.append([status, date])
        else:
            update_values.append(["Brand not Found", ""])

    print("Batch updating Destination Sheet...")
    # We update Columns AF (index 31) and AG (index 32)
    # Range is AF2:AG{number of rows}
    start_cell = 'AF2'
    end_cell = f'AG{len(update_values) + 1}'
    cell_range = f'{start_cell}:{end_cell}'
    
    dest_sheet.update(cell_range, update_values)
    
    print(f"Successfully updated {len(update_values)} rows!")

if __name__ == '__main__':
    main()
