from typing import List, Dict, Any
import os
import sys
import json
import time
import random
import threading
import shutil
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Fix Windows encoding issues
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import boto3
from google.oauth2 import service_account
import google.generativeai as genai
import gspread
from gspread.utils import rowcol_to_a1
from gspread.exceptions import WorksheetNotFound
import pandas as pd
import numpy as np
from PIL import Image

# Try to import gspread_dataframe, handle if missing
try:
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
    HAS_GSPREAD_DF = True
except ImportError:
    HAS_GSPREAD_DF = False
    print("Warning: gspread_dataframe not installed. Some reporting features may be limited.", file=sys.stderr)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

SYSTEM_INSTRUCTION = """
You are an expert data extractor digitizing handwritten automobile garage bills.
Your task is to extract data into a specific JSON structure.
**Read all details carefully.**

**SPECIAL INSTRUCTIONS for Quantity and Price Extraction:**
1.  **Quantity:** Interpret handwritten quantity values correctly. Treat formats like "- 01 -" or "-03/-" as the numerical quantity (e.g., 1 or 3). If no quantity is explicitly written for a part/labour item, default to **1**.
2.  **Price (Rate/Amount):** Extract the numerical value only. **Disregard any trailing formatting marks** such as "/", "-", or ".". For example, treat "1260/-" as the numerical value **1260**.

**STEP 1: Extract Header Information** (Applies to the whole bill)
* **Receipt Number:** Look for red text (e.g., 1, 15, 150). Check and capture the number ensuring it is **always 3 digits** (e.g., 1 -> 001, 15 -> 015, 150 -> 150).
* **Date:** Format 'dd-mm-yyyy'. The year must be **2025,2026**. If the handwritten year is prior to 2025 (e.g., '23, 2023, '22), **do not consider that date**. Convert two-digit years like '25' to '2025' (e.g., 25-05-2025).
* **Customer Name:** Extract the name (e.g., Shri Ganesh...). The name **must be in English** and contain **name only** (exclude salutations like 'Shri', 'Mr.', or 'Sir').
* **Mobile Number:** Extract phone number.
* **Car Number:** Extract vehicle number (e.g., MH 12...). The number **should be standard** and **must not have decimal places**.
* **Odometer:** Reading of km (e.g., 138360).
* **Total Bill Amount:** The final grand total at the bottom.

**STEP 2: Extract Line Items** (The table content)
* Extract EVERY single row written on the paper (Parts and Labour).
* **Description:** Name of the part or labour.
* **Type:** Classify as "Part" or "Labour" based on context.
* **Quantity:** Apply **SPECIAL INSTRUCTIONS** above. **Allow decimals (e.g., 0.5, 1.5).**
* **Rate:** Unit price. Apply **SPECIAL INSTRUCTIONS** above.
* **Amount:** Total for that row (Quantity * Rate). Apply **SPECIAL INSTRUCTIONS** above.
* **Confidence:** Your confidence (0-100%) in reading this specific line.

**OUTPUT FORMAT (JSON ONLY):**
{
    "header": {
        "receipt_number": "...", "date": "...", "customer_name": "...", 
        "mobile_number": "...", "car_number": "...", "odometer": "...", 
        "total_bill_amount": "..."
    },
    "items": [
        { "description": "...", "type": "Part", "quantity": 1, "rate": 100, "amount": 100, "confidence": 95 }
    ]
}
"""

MAX_RETRIES = 5
MAX_WORKERS = 2
TEMP_DOWNLOAD_DIR = "temp_downloads"

# Define IST timezone offset (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

class RateLimiter:
    """Ensures we don't exceed the API's rate limit."""
    def __init__(self, rpm=30):
        self.interval = 60.0 / rpm
        self._last_call = 0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            wait_time = self.interval - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_call = time.time()

limiter = RateLimiter(rpm=30)

def robust_rmtree(path):
    if os.path.exists(path):
        try:
            shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass

# =================================
# DATE FORMATTING HELPERS
# =================================
def normalize_date(date_str: str) -> str:
    """
    Normalizes input to strict DD-MM-YYYY format.
    """
    if not date_str or not date_str.strip():
        return ""
    s = date_str.strip().replace('/', '-')
    parts = s.split('-')
    if len(parts) != 3:
        return ""
    
    try:
        d = parts[0].strip()
        m = parts[1].strip()
        y = parts[2].strip()
        
        if len(d) == 1: d = '0' + d
        if len(m) == 1: m = '0' + m
        
        if len(y) == 2:
            y = '20' + y
        elif len(y) != 4:
            return ""
        
        day = int(d)
        month = int(m)
        year = int(y)
        
        if not (1 <= day <= 31 and 1 <= month <= 12 and 2000 <= year <= 2100):
            return ""
        
        if year not in [2025, 2026]:
            return "" 
        
        return f"{day:02d}-{month:02d}-{year}"
    except:
        return ""

def format_to_mmm(date_str: str) -> str:
    """
    Input: dd-mm-yyyy (12-12-2025)
    Output: dd-mmm-yyyy (12-Dec-2025)
    """
    if not date_str: return ""
    try:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return dt.strftime("%d-%b-%Y")
    except:
        return date_str

def format_to_us(date_str: str) -> str:
    """
    Input: dd-mm-yyyy (12-12-2025)
    Output: mm/dd/yyyy (12/12/2025)
    """
    if not date_str: return ""
    try:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return dt.strftime("%m/%d/%Y")
    except:
        return date_str

# =================================
# CLIENT INITIALIZATION
# =================================
def get_storage_client():
    try:
        config_json = os.environ.get('R2_CONFIG_JSON')
        if not config_json: return None
        cfg = json.loads(config_json)
        from botocore.config import Config
        return boto3.client(
            's3',
            endpoint_url=cfg.get("endpoint_url"),
            aws_access_key_id=cfg.get("access_key_id"),
            aws_secret_access_key=cfg.get("secret_access_key"),
            region_name='auto',
            config=Config(connect_timeout=60, read_timeout=60, retries={'max_attempts': 3})
        )
    except Exception as e:
        print(f"Storage connection failed: {e}", file=sys.stderr)
        return None

def get_google_creds():
    try:
        sa_json = os.environ.get('GCP_SA_JSON')
        if not sa_json: return None
        return service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    except Exception:
        return None

def configure_ai():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key: return False
    genai.configure(api_key=api_key)
    return True

# =================================
# SHEET HELPERS
# =================================
def connect_to_sheets(sheet_id: str, credentials) -> Dict[str, Any]:
    try:
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(sheet_id)
        
        try:
            spreadsheet.update_properties({"locale": "en_US"})
        except: pass
        
        # 1. Invoice All
        HEADERS_ALL = [
            "Receipt Number", "Date", "Customer Name", "Mobile Number", "Car Number", "Odometer Reading", 
            "Description", "Labour/Part", "Quantity", "Rate", "Amount", "Calculated Amount", "Difference", 
            "Review Status", "Total Bill Amount", "Accuracy %", "Receipt Link", "Source File", "Upload Date",
            "_r2_key", "Date_Eng" 
        ]
        
        # 2. Verify Dates
        HEADERS_VERIFY_DATES = [
            "Verification Status", "Receipt Number", "Date", "Receipt Link", "Audit Finding", "Upload Date"
        ]
        
        # 3. Verify Amount
        HEADERS_VERIFY_AMOUNT = [
            "Verification Status", "Receipt Number", "Description", "Amount", "Difference", "Receipt Link", "Upload Date"
        ]

        sheets = {}

        def setup_worksheet(title, headers, rows_min=1000):
            try:
                ws = spreadsheet.worksheet(title)
            except WorksheetNotFound:
                ws = spreadsheet.add_worksheet(title=title, rows=rows_min, cols=len(headers))
                ws.append_row(headers, value_input_option='USER_ENTERED')
                print(f"Created new worksheet: {title}")
            
            current_cols = ws.col_count
            required_cols = len(headers)
            if current_cols < required_cols:
                ws.add_cols(required_cols - current_cols)
            
            existing_headers = ws.row_values(1)
            if existing_headers != headers:
                print(f"Updating headers in {title} to match new schema...")
                ws.update('A1:' + rowcol_to_a1(1, required_cols), [headers], value_input_option='USER_ENTERED')
            
            return ws

        sheets['all'] = setup_worksheet("Invoice All", HEADERS_ALL, 5000)
        sheets['verify_dates'] = setup_worksheet("Verify Dates and Receipt Numbers", HEADERS_VERIFY_DATES, 1000)
        sheets['verify_amount'] = setup_worksheet("Verify Amount", HEADERS_VERIFY_AMOUNT, 1000)
        
        return sheets
    except Exception as e:
        print(f"Error connecting to Sheets: {e}", file=sys.stderr)
        return None

# =================================
# NEW VERIFICATION LOGIC
# =================================
def run_verification_logic(ws_all, ws_verify):
    """
    Reads 'Invoice All', applies strict logic to find date/receipt anomalies,
    and completely refreshes 'Verify Dates and Receipt Numbers' sheet.
    """
    if not HAS_GSPREAD_DF:
        print("Skipping verification logic: gspread_dataframe missing.")
        return

    print("Running Verification Logic on Dates & Receipts...")
    try:
        # Read Data from Invoice All
        df = get_as_dataframe(ws_all, header=0)
        
        if df.empty or 'Receipt Number' not in df.columns or 'Date' not in df.columns:
            print("Invoice All sheet is empty or missing columns. Skipping verification.")
            return

        # ---------------------------
        # 0. Input: original df
        # ---------------------------
        df = df.copy()

        # ---------------------------
        # 1. Normalize fields
        # ---------------------------
        # Ensure Receipt Number is string and stripped
        df['Receipt Number'] = df['Receipt Number'].astype(str).str.strip()
        df['Receipt Link']   = df['Receipt Link'].astype(str).str.strip()
        
        # Parse Date - Handle potential formats from Invoice All
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        # ---------------------------
        # 2. Remove exact duplicates
        # ---------------------------
        df = df.drop_duplicates(subset=['Receipt Number', 'Receipt Link', 'Date'], keep='first')

        # ---------------------------
        # 3. Collapse to ONE row per (Receipt Number, Receipt Link)
        # ---------------------------
        df_sorted = df.sort_values(['Receipt Number', 'Receipt Link', 'Date'], kind='stable')
        df_final = df_sorted.groupby(['Receipt Number', 'Receipt Link'], as_index=False).first()

        # ---------------------------
        # 4. Sort for date diff logic
        # ---------------------------
        df_final = df_final.sort_values(['Receipt Number', 'Date'], kind='stable').reset_index(drop=True)

        # ---------------------------
        # 5. Compute Date Difference
        # ---------------------------
        prev = df_final['Date'].where(df_final['Date'].notna()).ffill().shift()
        df_final['Date_Diff_days'] = (df_final['Date'] - prev).dt.days

        def fmt_diff(x):
            if pd.isna(x) or x <= 1:
                return ""
            return f"Date Diff: {int(x)}"

        df_final['Date_Diff'] = df_final['Date_Diff_days'].apply(fmt_diff)

        df_final['Date_Missing_Comment'] = np.where(df_final['Date'].isna(), 'Missing Date', '')

        # ---------------------------
        # 6. Duplicate checks after grouping
        # ---------------------------
        df_final['Duplicate_Receipt_Final'] = df_final['Receipt Number'].duplicated(keep=False)
        df_final['Duplicate_Link_Final']    = df_final['Receipt Link'].duplicated(keep=False)

        # ---------------------------
        # 7. Build Audit Finding
        # ---------------------------
        def build_audit(row):
            parts = []
            if row['Date_Diff']:
                parts.append(row['Date_Diff'])
            if row['Date_Missing_Comment']:
                parts.append(row['Date_Missing_Comment'])
            if row['Duplicate_Receipt_Final']:
                parts.append("Duplicate Receipt Number")
            if row['Duplicate_Link_Final']:
                parts.append("Duplicate Receipt Link")
            return " | ".join(parts) if parts else ""

        df_final['Audit Finding'] = df_final.apply(build_audit, axis=1)

        # ---------------------------
        # 8. Filter only rows with findings
        # ---------------------------
        output = df_final[df_final['Audit Finding'].astype(bool)].copy()

        # ---------------------------
        # 9. Add required columns
        # ---------------------------
        output['Verification Status'] = "Pending"
        output['Upload Date'] = datetime.now(IST).strftime("%d-%b-%Y")

        # Convert Date to dd-mm-yyyy (Indian Format)
        output['Date'] = output['Date'].dt.strftime("%d-%m-%Y")

        # ---------------------------
        # 10. Final column order
        # ---------------------------
        output = output[
            ["Verification Status", "Receipt Number", "Date",
             "Receipt Link", "Audit Finding", "Upload Date"]
        ].reset_index(drop=True)

        # Write to Google Sheet (Clear and Rewrite)
        if not output.empty:
            ws_verify.clear()
            set_with_dataframe(ws_verify, output, include_index=False, include_column_header=True)
            print(f"Verification Sheet updated with {len(output)} anomalies.")
        else:
            print("No anomalies found. Verification Sheet cleared.")
            ws_verify.clear()
            # Restore header if empty
            headers = ["Verification Status", "Receipt Number", "Date", "Receipt Link", "Audit Finding", "Upload Date"]
            ws_verify.append_row(headers)

    except Exception as e:
        print(f"Error running verification logic: {e}")

# =================================
# PROCESSING LOGIC
# =================================
def process_single_image(storage_client, bucket_name, key, temp_dir):
    """Downloads an image, runs AI extraction, calculates anomalies, and formats results."""
    filename = key.split("/")[-1]
    local_path = os.path.join(temp_dir, filename)
    receipt_link = ""
    
    # Upload Date formatted as dd-MMM-yyyy (e.g., 12-Dec-2025)
    upload_date_iso = datetime.now(IST)
    upload_date_mmm = upload_date_iso.strftime("%d-%b-%Y")

    try:
        r2_config_json = os.environ.get('R2_CONFIG_JSON')
        if r2_config_json:
            cfg = json.loads(r2_config_json)
            public_base = cfg.get("public_base_url")
            if public_base:
                receipt_link = f"{public_base.rstrip('/')}/{bucket_name}/{key.lstrip('/')}"
        if not receipt_link:
            receipt_link = storage_client.generate_presigned_url(
                'get_object', Params={'Bucket': bucket_name, 'Key': key}, ExpiresIn=604800
            )
    except Exception:
        pass

    # Download file
    try:
        storage_client.download_file(bucket_name, key, local_path)
    except Exception:
        return []

    print(f"Analyzing: {filename}...")
    img = None
    rows = []
    
    try:
        img = Image.open(local_path)
        model = genai.GenerativeModel("gemini-2.0-flash", system_instruction=SYSTEM_INSTRUCTION)
        
        response = None
        for attempt in range(MAX_RETRIES):
            try:
                limiter.wait()
                response = model.generate_content(
                    [img, "Extract bill data."], 
                    generation_config={"response_mime_type": "application/json"},
                    request_options={'timeout': 180}
                )
                break
            except Exception:
                time.sleep((2 ** (attempt + 1)) + random.uniform(0, 1))

        if not response: raise Exception("No response from AI")
        
        data = json.loads(response.text)
        header = data.get("header", {})
        items = data.get("items", []) or [{}]

        def safe_num(val, func):
            try: return func(val) if val not in [None, ""] else ""
            except: return ""

        # 1. Base Extraction is dd-mm-yyyy
        rec_date_base = normalize_date(header.get("date", ""))
        
        # 2. Transformations for specific sheets
        rec_date_mmm = format_to_mmm(rec_date_base) # 12-Dec-2025
        rec_date_us = format_to_us(rec_date_base)   # 12/12/2025

        for item in items:
            qty = safe_num(item.get("quantity"), float)
            rate = safe_num(item.get("rate"), int)
            amount = safe_num(item.get("amount"), int)
            
            calc_amount = ""
            difference = ""
            review_reasons = []

            if isinstance(qty, (int, float)) and isinstance(rate, (int, float)):
                try: calc_amount = int(round(qty * rate))
                except: pass
            
            if (amount == "" or amount is None) and isinstance(calc_amount, int):
                amount = calc_amount

            if isinstance(amount, (int, float)) and isinstance(calc_amount, (int, float)):
                difference = abs(calc_amount - amount)

            rec_num = header.get("receipt_number", "")
            try:
                rec_num = f"{int(rec_num):03d}"
            except:
                rec_num = rec_num

            if not rec_num or len(rec_num) != 3 or not rec_num.isdigit(): review_reasons.append("Review Receipt Number")
            if not rec_date_base: review_reasons.append("Review Date")
            if isinstance(difference, (int, float)) and difference > 0: review_reasons.append("Review Price")

            review_status = ", ".join(review_reasons) if review_reasons else "Not Required"

            row = {
                "Receipt Number": rec_num or "",
                "Date": rec_date_mmm or "", # Invoice All: dd-mmm-yyyy
                "Customer Name": header.get("customer_name", "") or "",
                "Mobile Number": header.get("mobile_number", "") or "",
                "Car Number": header.get("car_number", "") or "",
                "Odometer Reading": safe_num(header.get("odometer"), int),
                "Description": item.get("description", "") or "",
                "Labour/Part": item.get("type", "") or "",
                "Quantity": qty,
                "Rate": rate,
                "Amount": amount,
                "Calculated Amount": calc_amount,
                "Difference": difference,
                "Review Status": review_status,
                "Total Bill Amount": safe_num(header.get("total_bill_amount"), int),
                "Accuracy %": safe_num(item.get("confidence"), int),
                "Receipt Link": receipt_link,
                "Source File": filename,
                "Upload Date": upload_date_mmm, # Invoice All: dd-mmm-yyyy
                "_r2_key": key,
                "Date_Eng": rec_date_us or ""   # Invoice All: mm/dd/yyyy
            }
            rows.append(row)
            
        total_extracted = sum([r["Amount"] for r in rows if isinstance(r["Amount"], (int, float))])
        for r in rows:
            if r.get("Total Bill Amount", None) in ["", None]:
                r["Total Bill Amount"] = total_extracted

        print(f"Processed: {filename}")
        return rows

    except Exception as e:
        print(f"Failed {filename}: {e}", file=sys.stderr)
        return []
    finally:
        if img: 
            try: img.close()
            except: pass
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except: pass

def update_google_sheet(worksheet, new_records: List[Dict[str, Any]], headers: List[str]):
    """Appends records to the given worksheet."""
    if not new_records: return
    try:
        rows = [[str(rec.get(h, "")) for h in headers] for rec in new_records]
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')
        print(f"Added {len(rows)} rows to '{worksheet.title}'")
    except Exception as e:
        print(f"Sheet Update Error: {e}", file=sys.stderr)

# =================================
# MAIN
# =================================
def main():
    print("--- Starting AI Processor ---")
    bucket = os.environ.get("R2_BUCKET_NAME")
    sheet_id = os.environ.get("SHEET_ID")
    
    if not bucket or not sheet_id:
        print("Config missing.", file=sys.stderr)
        sys.exit(1)

    client = get_storage_client()
    creds = get_google_creds()
    if not client or not creds or not configure_ai():
        print("Client or AI config failed.", file=sys.stderr)
        sys.exit(1)

    sheets = connect_to_sheets(sheet_id, creds)
    if not sheets: sys.exit(1)

    try:
        all_data_keys = sheets['all'].col_values(sheets['all'].find("_r2_key").col) 
        existing_keys = set(all_data_keys[1:]) 
        print(f"Found {len(existing_keys)} existing keys in 'Invoice All'.")
    except Exception:
        existing_keys = set()

    try:
        storage_files = client.list_objects_v2(Bucket=bucket, Prefix='uploads/').get('Contents', [])
        keys_to_process = [
            obj['Key'] for obj in storage_files 
            if not obj['Key'].endswith('/') and obj['Key'] not in existing_keys
        ]
    except Exception as e:
        print(f"Error listing files: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(keys_to_process)} new files to process.")
    
    if keys_to_process:
        robust_rmtree(TEMP_DOWNLOAD_DIR)
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

        batch = []
        processed_count = 0
        
        def write_progress(current, status="running"):
            try:
                with open("processing_progress.json", 'w') as f:
                    json.dump({
                        "total": len(keys_to_process),
                        "completed": processed_count,
                        "current_file": current,
                        "status": status
                    }, f)
            except: pass

        write_progress("Initializing...", "running")
        
        headers_all = sheets['all'].row_values(1)
        headers_amount = sheets['verify_amount'].row_values(1)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_image, client, bucket, k, TEMP_DOWNLOAD_DIR): k for k in keys_to_process}
            
            for future in as_completed(futures):
                key = futures[future]
                fname = key.split("/")[-1]
                try:
                    res = future.result()
                    if res: batch.extend(res)
                    processed_count += 1
                    write_progress(fname)

                    if len(batch) >= 20:
                        print("Uploading batch...")
                        write_progress(fname, "uploading")
                        
                        update_google_sheet(sheets['all'], batch, headers_all)
                        
                        amount_rows = []
                        for r in batch:
                            diff = r.get("Difference")
                            if isinstance(diff, (int, float)) and diff > 0:
                                amount_rows.append({
                                    "Verification Status": "Pending",
                                    "Receipt Number": r.get("Receipt Number"),
                                    "Description": r.get("Description"),
                                    "Quantity": r.get("Quantity"),
                                    "Rate": r.get("Rate"),
                                    "Amount": r.get("Amount"),
                                    "Difference": diff,
                                    "Receipt Link": r.get("Receipt Link"),
                                    "Upload Date": r.get("Upload Date") 
                                })
                        update_google_sheet(sheets['verify_amount'], amount_rows, headers_amount)
                        
                        batch = []
                        write_progress(fname, "running")

                except Exception as e:
                    print(f"Error on {fname}: {e}", file=sys.stderr)
                    processed_count += 1
                    write_progress(fname)

        if batch:
            print("Uploading final batch...")
            update_google_sheet(sheets['all'], batch, headers_all)
            
            amount_rows = []
            for r in batch:
                diff = r.get("Difference")
                if isinstance(diff, (int, float)) and diff > 0:
                    amount_rows.append({
                        "Verification Status": "Pending",
                        "Receipt Number": r.get("Receipt Number"),
                        "Description": r.get("Description"),
                        "Quantity": r.get("Quantity"),
                        "Rate": r.get("Rate"),
                        "Amount": r.get("Amount"),
                        "Difference": diff,
                        "Receipt Link": r.get("Receipt Link"),
                        "Upload Date": r.get("Upload Date")
                    })
            update_google_sheet(sheets['verify_amount'], amount_rows, headers_amount)

        write_progress("Finalizing Reports", "cleanup")
        robust_rmtree(TEMP_DOWNLOAD_DIR)
    
    # --- RUN NEW VERIFICATION LOGIC ---
    # This reads Invoice All, calculates anomalies based on your logic, and overwrites the Verify sheet
    run_verification_logic(sheets['all'], sheets['verify_dates'])

    print("--- Processing Complete ---")

if __name__ == "__main__":
    main()