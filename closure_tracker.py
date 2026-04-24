import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import re

# --- CONFIGURATION ---
CREDENTIALS_FILE = 'service_account.json'

# Destination Sheet
DEST_SHEET_ID = '1tqp6sO8FOqZ0xBlIrDKuXYTfHZCV0RRTdRFLdlxCBTQ'
DEST_TAB = 'Meeting'
DEST_BRAND_COL = 0        # Column A
DEST_MEETING_COL = 8      # Column I (Meeting fixed Date)

# Source Sheets
SRC1_ID = '1ykMZ7knfOgS_-OWXiO9OZYXmixIXfYMLJsCcJX0HX7c' # Master Helper
SRC2_ID = '1MEcJkJNX1AyLRL8FSltLZdWhF8yf3WAAbsEGT9I3Us4' # ERF and ECF
SRC3_ID = '1rw_7g7GCKlgA2rdzBc_MnJ_JmkGAiCjJxID5WKciFcs' # Consolidated
SRC4_ID = '1AxPIE4kpHb0G6DX7ngAi1DhqacxdCFevZ_3p1pOWlZM' # Relavant sheet

def normalize_brand_name(name):
    """Smart Normalization: Lowercase, removes punctuation, strict word matching."""
    if not isinstance(name, str) or not name.strip():
        return ""
    name = name.lower()
    name = re.sub(r'[&\-_/|]', ' ', name)
    name = re.sub(r'[^\w\s]', '', name)
    return re.sub(r'\s+', ' ', name).strip()

def parse_date(date_str, is_month_year=False):
    """Safely parses different date formats into Pandas Datetime objects."""
    if not date_str or not str(date_str).strip():
        return pd.NaT
    try:
        # Parse date and remove timezone data for safe math comparisons
        dt = pd.to_datetime(str(date_str).strip(), errors='coerce')
        if pd.isna(dt): return pd.NaT
        dt = dt.tz_localize(None) 
        
        if is_month_year:
            # If format is "Sep-2023", move to the end of the month (Sep 30) 
            # to generously ensure it triggers the "After meeting date" logic.
            dt = dt + pd.offsets.MonthEnd(0)
            
        return dt
    except:
        return pd.NaT

def extract_executions(sheet_data, brand_col, date_col, execution_dict, is_month_year=False):
    """Extracts data from a source sheet and appends it to the master dictionary."""
    for row in sheet_data[1:]: # Skip header
        if len(row) > max(brand_col, date_col):
            brand = normalize_brand_name(row[brand_col])
            date_val = parse_date(row[date_col], is_month_year)
            
            if brand and pd.notna(date_val):
                if brand not in execution_dict:
                    execution_dict[brand] = []
                execution_dict[brand].append(date_val)

def main():
    print("Authenticating with Google Sheets...")
    scope =["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)

    print("Downloading Source Sheets (This might take a moment)...")
    src1_wb = client.open_by_key(SRC1_ID)
    src2_wb = client.open_by_key(SRC2_ID)
    src3_wb = client.open_by_key(SRC3_ID)
    src4_wb = client.open_by_key(SRC4_ID)

    # Master dictionary to hold all execution dates: { 'brand name': [date1, date2, ...] }
    all_executions = {}

    print("Aggregating Master Helper...")
    extract_executions(src1_wb.worksheet('Master Helper').get_all_values(), brand_col=2, date_col=0, execution_dict=all_executions, is_month_year=True)
    
    print("Aggregating ERF and ECF...")
    extract_executions(src2_wb.worksheet('ERF').get_all_values(), brand_col=7, date_col=0, execution_dict=all_executions)
    extract_executions(src2_wb.worksheet('ECF').get_all_values(), brand_col=20, date_col=0, execution_dict=all_executions)
    
    print("Aggregating Consolidated...")
    extract_executions(src3_wb.worksheet('Consolidated').get_all_values(), brand_col=3, date_col=0, execution_dict=all_executions)
    
    print("Aggregating Relavant sheet...")
    extract_executions(src4_wb.worksheet('Relavant sheet').get_all_values(), brand_col=2, date_col=0, execution_dict=all_executions)

    print(f"Successfully mapped {len(all_executions)} unique brands across all source sheets.")

    print("Processing Destination Sheet...")
    dest_wb = client.open_by_key(DEST_SHEET_ID)
    dest_sheet = dest_wb.worksheet(DEST_TAB)
    dest_data = dest_sheet.get_all_values()

    update_values =[]

    for row in dest_data[1:]:
        # Pad row to prevent index errors
        row.extend([''] * (DEST_MEETING_COL + 1 - len(row)))
        
        brand = normalize_brand_name(row[DEST_BRAND_COL])
        meeting_date_str = row[DEST_MEETING_COL]
        
        # Parse destination meeting date
        meeting_date = parse_date(meeting_date_str)
        status = "" # Default to blank if no match or conditions aren't met

        if brand in all_executions and pd.notna(meeting_date):
            # Check all execution dates for this brand
            for exec_date in all_executions[brand]:
                # Calculate difference in days
                days_diff = (exec_date - meeting_date).days
                
                # Condition: Execution is AFTER the meeting date AND UP TO 45 days
                if 0 <= days_diff <= 45:
                    status = "Closed"
                    break # Stop checking dates for this brand, we found a valid closure!

        update_values.append([status])

    print("Batch Updating Closure Status to Destination Sheet...")
    # Update Column AH (Index 33, so column 'AH')
    start_cell = 'AH2'
    end_cell = f'AH{len(update_values) + 1}'
    cell_range = f'{start_cell}:{end_cell}'
    
    dest_sheet.update(cell_range, update_values)
    print(f"Successfully evaluated and updated {len(update_values)} rows!")

if __name__ == '__main__':
    main()
