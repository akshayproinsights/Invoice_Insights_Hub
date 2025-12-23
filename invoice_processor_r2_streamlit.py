# File: invoice_processor_r2_streamlit.py
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
* **Date:** **CRITICAL - The handwritten date on the invoice is in dd-mm-yy or dd-mm-yyyy format (DAY comes FIRST, then MONTH, then YEAR).** For example, "10-12-25" means 10th December 2025, NOT 12th October. **You MUST output as 'dd-MMM-yyyy' format (e.g., 10-Dec-2025, 05-Jan-2026).** Use 3-letter month abbreviation (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec). The year must be **2025 or 2026** (likely >2025). If the handwritten year is prior to 2025 (e.g., '23, 2023, '22), **do not consider that date**. Convert two-digit years like '25' to '2025', '26' to '2026'. **Examples: Handwritten "10-12-25" = 10-Dec-2025, Handwritten "05/01/2026" = 05-Jan-2026, Handwritten "15-6-25" = 15-Jun-2025**.
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
    """Normalizes input to strict DD-MM-YYYY format.
    Accepts: dd-mm-yyyy, dd/mm/yyyy, dd-MMM-yyyy (e.g., 10-Dec-2025)
    Returns: dd-mm-yyyy format always
    """
    # Handle NaN, None, and float values
    if pd.isna(date_str) or date_str is None:
        return ""
    
    # Convert to string if it's a float/int
    date_str = str(date_str)
    
    if not date_str or not date_str.strip():
        return ""
    
    s = date_str.strip().replace('/', '-')
    
    # Try parsing as dd-MMM-yyyy first (e.g., "10-Dec-2025")
    try:
        dt = datetime.strptime(s, "%d-%b-%Y")
        if dt.year in [2025, 2026]:
            return dt.strftime("%d-%m-%Y")
    except:
        pass
    
    # Try parsing as dd-mm-yyyy (numeric)
    parts = s.split('-')
    if len(parts) != 3:
        return ""
    
    try:
        d = parts[0].strip()
        m = parts[1].strip()
        y = parts[2].strip()
        
        # Pad day and month
        if len(d) == 1: d = '0' + d
        if len(m) == 1: m = '0' + m
        if len(y) == 2: y = '20' + y
        elif len(y) != 4: return ""
        
        # Parse explicitly as day-month-year to avoid ambiguity
        dt = datetime.strptime(f"{d}-{m}-{y}", "%d-%m-%Y")
        
        # Validate year
        if dt.year not in [2025, 2026]:
            return ""
        
        return dt.strftime("%d-%m-%Y")
    except:
        return ""

def format_to_mmm(date_str: str) -> str:
    if not date_str: return ""
    try:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return dt.strftime("%d-%b-%Y")
    except: return date_str

def format_to_us(date_str: str) -> str:
    if not date_str: return ""
    try:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return dt.strftime("%m/%d/%Y")
    except: return date_str

def safe_format_date_series(series, output_format="%d-%b-%Y"):
    """
    Safely format a pandas Series of dates to specified format.
    Handles dd-mm-yyyy and dd-MMM-yyyy input formats.
    Default output: dd-MMM-yyyy (e.g., "10-Dec-2025")
    """
    def parse_and_format(val):
        if pd.isna(val) or not val:
            return ""
        
        s = str(val).strip()
        if not s:
            return ""
        
        # Try different formats
        for fmt in ["%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"]:
            try:
                dt = datetime.strptime(s, fmt)
                if dt.year in [2025, 2026]:  # Validate year
                    return dt.strftime(output_format)
            except:
                continue
        
        # If all fail, use normalize_date and retry
        normalized = normalize_date(s)
        if normalized:
            try:
                dt = datetime.strptime(normalized, "%d-%m-%Y")
                return dt.strftime(output_format)
            except:
                pass
        
        return ""
    
    return series.apply(parse_and_format)

# =================================
# CORE: VERIFIED DATA LOGIC (USER PROVIDED)
# =================================
def _find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if c.lower().replace(" ", "").replace("-", "_") in [x.lower().replace(" ", "").replace("-", "_") for x in candidates]:
            return c
    return None

def build_verified(df_raw: pd.DataFrame, df_date: pd.DataFrame, df_amount: pd.DataFrame) -> pd.DataFrame:
    raw = df_raw.copy()
    date = df_date.copy()
    amount = df_amount.copy()

    rowid_col_raw = _find_col(raw, ["row_id", "Row_Id", "Row ID", "rowid"])
    rowid_col_date = _find_col(date, ["row_id", "Row_Id", "Row ID",  "rowid"])
    rowid_col_amount = _find_col(amount, ["row_id", "Row_Id", "Row ID",  "rowid"])

    def clean_link(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip()
        return s if s else pd.NA

    def as_str_trim(x):
        if pd.isna(x):
            return ""
        s = str(x).strip()
        # Remove .0 suffix from numeric strings (e.g., "891.0" -> "891")
        if s.endswith('.0'):
            s = s[:-2]
        return s

    def fix_date(series):
        """Parse dates using explicit dd-mm-yyyy or dd-MMM-yyyy format to avoid ambiguity"""
        def parse_single_date(val):
            if pd.isna(val) or not val:
                return pd.NaT
            
            s = str(val).strip()
            if not s:
                return pd.NaT
            
            # Try dd-MMM-yyyy format first (e.g., "10-Dec-2025")
            for fmt in ["%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(s, fmt)
                except:
                    continue
            
            # If all fail, use normalize_date and retry
            normalized = normalize_date(s)
            if normalized:
                try:
                    return datetime.strptime(normalized, "%d-%m-%Y")
                except:
                    pass
            
            return pd.NaT
        
        dt = series.apply(parse_single_date)
        # Convert to string, replacing NaT with empty string
        result = dt.apply(lambda x: x.strftime("%d-%b-%Y") if pd.notna(x) else "")
        return result

    raw["Receipt Link_clean"] = raw.get("Receipt Link", pd.Series([pd.NA]*len(raw))).apply(clean_link)
    raw["Receipt Number_str"] = raw.get("Receipt Number", pd.Series([""]*len(raw))).astype(str).fillna("").apply(as_str_trim)
    raw["Date"] = fix_date(raw.get("Date", pd.Series([pd.NA]*len(raw))))

    date["Receipt Link_clean"] = date.get("Receipt Link", pd.Series([pd.NA]*len(date))).apply(clean_link)
    date["Receipt Number_str"] = date.get("Receipt Number", pd.Series([""]*len(date))).astype(str).fillna("").apply(as_str_trim)
    date["Verification Status_clean"] = date.get("Verification Status", pd.Series([""]*len(date))).astype(str).str.strip().str.lower()
    date["Date"] = fix_date(date.get("Date", pd.Series([pd.NA]*len(date))))
    if "Upload Date" in date.columns:
        date["Upload Date_dt"] = pd.to_datetime(date["Upload Date"], errors="coerce")
        date = date.sort_values("Upload Date_dt").drop_duplicates(subset=["Receipt Link_clean", "Receipt Number_str"], keep="last")
    else:
        date = date.drop_duplicates(subset=["Receipt Link_clean", "Receipt Number_str"], keep="last")

    amount["Receipt Link_clean"] = amount.get("Receipt Link", pd.Series([pd.NA]*len(amount))).apply(clean_link)
    amount["Receipt Number_str"] = amount.get("Receipt Number", pd.Series([""]*len(amount))).astype(str).fillna("").apply(as_str_trim)
    amount["Verification Status_clean"] = amount.get("Verification Status", pd.Series([""]*len(amount))).astype(str).str.strip().str.lower()
    if "Upload Date" in amount.columns:
        amount["Upload Date_dt"] = pd.to_datetime(amount["Upload Date"], errors="coerce")
        amount = amount.sort_values("Upload Date_dt").drop_duplicates(subset=["Receipt Number_str", "Description", rowid_col_amount] if rowid_col_amount else ["Receipt Number_str", "Description"], keep="last")
    else:
        amount = amount.drop_duplicates(subset=["Receipt Number_str", "Description", rowid_col_amount] if rowid_col_amount else ["Receipt Number_str", "Description"], keep="last")

    date_pending_row_ids = set()
    if rowid_col_date and rowid_col_date in date.columns:
        date_pending_row_ids = set(date.loc[date["Verification Status_clean"] != "done", rowid_col_date].dropna().astype(str))
    
    amount_pending_row_ids = set()
    if rowid_col_amount and rowid_col_amount in amount.columns:
        amount_pending_row_ids = set(amount.loc[amount["Verification Status_clean"] != "done", rowid_col_amount].dropna().astype(str))

    date_done_links = set(date.loc[date["Verification Status_clean"] == "done", "Receipt Link_clean"].dropna())
    date_done_numbers = set(date.loc[date["Verification Status_clean"] == "done", "Receipt Number_str"].dropna())
    amount_done_numbers = set(amount.loc[amount["Verification Status_clean"] == "done", "Receipt Number_str"].dropna())
    amount_done_rowids = set()
    if rowid_col_amount and rowid_col_amount in amount.columns:
        amount_done_rowids = set(amount.loc[amount["Verification Status_clean"] == "done", rowid_col_amount].dropna().astype(str))

    date_links_all = set(date["Receipt Link_clean"].dropna())
    date_numbers_all = set(date["Receipt Number_str"].dropna())
    amount_numbers_all = set(amount["Receipt Number_str"].dropna())
    amount_links_all = set(amount["Receipt Link_clean"].dropna())
    amount_rowids_all = set()
    if rowid_col_amount and rowid_col_amount in amount.columns:
        amount_rowids_all = set(amount[rowid_col_amount].dropna().astype(str))

    # Combine pending Row_Ids for exclusion (use Row_Id instead of Receipt Number)
    excluded_row_ids = date_pending_row_ids | amount_pending_row_ids
    
    # Also exclude rejected records from Invoice Verified
    if 'Review Status' in raw.columns and rowid_col_raw:
        rejected_mask = raw['Review Status'].astype(str).str.lower() == 'rejected'
        rejected_row_ids = set(raw.loc[rejected_mask, rowid_col_raw].dropna().astype(str))
        excluded_row_ids = excluded_row_ids | rejected_row_ids

    
    # CRITICAL: Distinguish between "Verified" and "Already Verified"
    # "Verified" = Successfully verified records from previous syncs → INCLUDE in Invoice Verified
    # "Already Verified" = Duplicate uploads detected → EXCLUDE from Invoice Verified
    already_verified_mask = pd.Series([False] * len(raw), index=raw.index)
    if 'Review Status' in raw.columns:
        status_lower = raw['Review Status'].astype(str).str.lower()
        already_verified_mask = status_lower == 'verified'  # Include previously verified records

    presence_mask_any_verify = (
        raw["Receipt Link_clean"].isin(date_links_all) |
        raw["Receipt Link_clean"].isin(amount_links_all) |
        raw["Receipt Number_str"].isin(date_numbers_all) |
        raw["Receipt Number_str"].isin(amount_numbers_all) |
        raw.get(rowid_col_raw, pd.Series([pd.NA]*len(raw))).astype(str).isin(amount_rowids_all)
    )

    # Include records that are NOT in verification sheets OR have status='Verified' (from previous syncs)
    base_df = raw[(~presence_mask_any_verify) | already_verified_mask].copy()
    if excluded_row_ids and rowid_col_raw:
        base_df = base_df[~base_df.get(rowid_col_raw, pd.Series([""] * len(base_df))).astype(str).isin(excluded_row_ids)].copy()
    
    # CRITICAL: Exclude 'Rejected' and 'Already Verified' (duplicates) from Invoice Verified
    # These should only exist in Invoice All for tracking purposes
    if 'Review Status' in base_df.columns:
        status_lower = base_df['Review Status'].astype(str).str.lower()
        base_df = base_df[~status_lower.isin(['rejected', 'already verified'])].copy()
    
    # All remaining records are truly verified
    base_df["Review Status"] = "Verified"

    mask_candidate_date_done = raw["Receipt Link_clean"].isin(date_done_links)
    mask_candidate_amount_done_rowid = False
    if rowid_col_raw:
        mask_candidate_amount_done_rowid = raw.get(rowid_col_raw, pd.Series([""]*len(raw))).astype(str).isin(amount_done_rowids)
    mask_candidate_amount_done_number = raw["Receipt Number_str"].isin(amount_done_numbers)

    mask_blocked_by_pending_rowid = False
    if rowid_col_raw and excluded_row_ids:
        mask_blocked_by_pending_rowid = raw.get(rowid_col_raw, pd.Series([""] * len(raw))).astype(str).isin(excluded_row_ids)

    inclusion_mask = (
        (mask_candidate_date_done | mask_candidate_amount_done_rowid | mask_candidate_amount_done_number)
        & (~mask_blocked_by_pending_rowid if isinstance(mask_blocked_by_pending_rowid, pd.Series) else pd.Series([True] * len(raw), index=raw.index))
    )

    included_rows = raw[inclusion_mask].copy()
    
    # Exclude rejected records from being included in Invoice Verified
    if 'Review Status' in included_rows.columns and not included_rows.empty:
        included_rows = included_rows[included_rows['Review Status'].astype(str).str.lower() != 'rejected'].copy()

    if included_rows.empty:
        final_df = base_df.reindex(columns=df_raw.columns)
        return final_df

    date_done_df = date[date["Verification Status_clean"] == "done"].copy()
    date_apply_cols = []
    if "Receipt Number" in date_done_df.columns:
        date_apply_cols.append("Receipt Number")
    if "Receipt Link" in date_done_df.columns:
        date_apply_cols.append("Receipt Link")
    if "Date" in date_done_df.columns:
        date_apply_cols.append("Date")
    date_apply_cols = ["Receipt Link_clean"] + date_apply_cols
    date_done_sel = date_done_df[date_apply_cols].drop_duplicates(subset=["Receipt Link_clean"], keep="last").copy()
    rename_map = {}
    if "Receipt Number" in date_done_sel.columns:
        rename_map["Receipt Number"] = "ReceiptNumber_date_verified"
    if "Receipt Link" in date_done_sel.columns:
        rename_map["Receipt Link"] = "ReceiptLink_date_verified"
    if "Date" in date_done_sel.columns:
        rename_map["Date"] = "Date_date_verified"
    date_done_sel = date_done_sel.rename(columns=rename_map)

    included_rows = included_rows.merge(date_done_sel, on="Receipt Link_clean", how="left", validate="m:1")
    if "ReceiptNumber_date_verified" in included_rows.columns:
        included_rows["Receipt Number"] = included_rows["ReceiptNumber_date_verified"].combine_first(included_rows["Receipt Number"])
    if "ReceiptLink_date_verified" in included_rows.columns:
        included_rows["Receipt Link"] = included_rows["ReceiptLink_date_verified"].combine_first(included_rows["Receipt Link"])
    if "Date_date_verified" in included_rows.columns:
        included_rows["Date"] = included_rows["Date_date_verified"].combine_first(included_rows["Date"])
    for c in ["ReceiptNumber_date_verified", "ReceiptLink_date_verified", "Date_date_verified"]:
        if c in included_rows.columns:
            included_rows.drop(columns=[c], inplace=True)

    amount_done_df = amount[amount["Verification Status_clean"] == "done"].copy()
    amt_cols = [c for c in ["Quantity", "Rate", "Amount", "Amount Mismatch"] if c in amount_done_df.columns]
    
    # Fix for Merge Logic in provided code to handle suffixes
    if not amount_done_df.empty and amt_cols:
        amt_merge = pd.DataFrame()
        if rowid_col_amount and rowid_col_amount in amount_done_df.columns:
            amt_merge = amount_done_df[[rowid_col_amount] + amt_cols].copy()
            amt_merge[rowid_col_amount] = amt_merge[rowid_col_amount].astype(str)
            amt_merge = amt_merge.drop_duplicates(subset=[rowid_col_amount], keep="last")
            included_rows[rowid_col_raw] = included_rows.get(rowid_col_raw, pd.Series([""]*len(included_rows))).astype(str)
            
            # Use suffixes to identify the new verified data
            included_rows = included_rows.merge(
                amt_merge.rename(columns={rowid_col_amount: rowid_col_raw}), 
                on=rowid_col_raw, 
                how="left", 
                suffixes=(None, '_verified'), # Keep original names, add suffix to verified data
                validate="m:1"
            )
            
            for col in amt_cols:
                verified_col = f"{col}_verified"
                if verified_col in included_rows.columns:
                    # Update original column with verified data if present
                    included_rows[col] = included_rows[verified_col].combine_first(included_rows[col])
                    included_rows.drop(columns=[verified_col], inplace=True)
                    
        if amt_merge.empty:
            pass

    included_rows["Review Status"] = "Verified"

    final_df = pd.concat([base_df, included_rows], ignore_index=True, sort=False)
    for c in ["Receipt Link_clean", "Receipt Number_str", "Upload Date_dt"]:
        if c in final_df.columns:
            final_df.drop(columns=[c], inplace=True, errors="ignore")

    final_cols = [c for c in df_raw.columns if c in final_df.columns]
    other_cols = [c for c in final_df.columns if c not in final_cols]
    final_df = final_df[final_cols + other_cols]
    
    # CRITICAL: Deduplicate Invoice Verified - should NEVER have duplicate rows
    # Use Receipt Number + Date + Description as unique key, keep latest upload
    dedup_cols = []
    if "Receipt Number" in final_df.columns:
        dedup_cols.append("Receipt Number")
    if "Date" in final_df.columns:
        dedup_cols.append("Date")
    if "Description" in final_df.columns:
        dedup_cols.append("Description")
    
    if dedup_cols:
        # Sort by Upload Date if available to keep latest, otherwise just deduplicate
        if "Upload Date" in final_df.columns:
            final_df = final_df.sort_values("Upload Date", na_position='first')
        final_df = final_df.drop_duplicates(subset=dedup_cols, keep='last').reset_index(drop=True)
        print(f"Deduplicated Invoice Verified: {len(final_df)} unique records")

    return final_df

def run_sync_verified_logic(sheets: Dict[str, Any]):
    """
    1. Reads All, Verify Dates, Verify Amount.
    2. Identifies 'Done' rows in verification sheets.
    3. UPDATES 'Invoice All' with the corrected values (Dates, Receipt #, Amounts).
    4. REBUILDS 'Invoice Verified' from the corrected 'Invoice All'.
    5. DELETES 'Done' rows from Verification sheets.
    """
    if not HAS_GSPREAD_DF:
        print("Cannot sync verified data: gspread_dataframe missing.")
        return

    print("Saving changes to database...")
    try:
        # 1. Read Dataframes
        df_raw = get_as_dataframe(sheets['all'], header=0, evaluate_formulae=True)
        # Convert Receipt Number to string to ensure matching works
        if "Receipt Number" in df_raw.columns:
            df_raw["Receipt Number"] = df_raw["Receipt Number"].astype(str).str.replace(r'\.0$', '', regex=True)
        
        df_date = get_as_dataframe(sheets['verify_dates'], header=0, evaluate_formulae=True)
        # Convert Receipt Number to string to ensure matching works
        if "Receipt Number" in df_date.columns:
            df_date["Receipt Number"] = df_date["Receipt Number"].astype(str).str.replace(r'\.0$', '', regex=True)
        
        df_amount = get_as_dataframe(sheets['verify_amount'], header=0, evaluate_formulae=True)
        # Convert Receipt Number to string to ensure matching works
        if "Receipt Number" in df_amount.columns:
            df_amount["Receipt Number"] = df_amount["Receipt Number"].astype(str).str.replace(r'\.0$', '', regex=True)

        corrections_made = False
        
        # ---------------------------------------------------------
        # 1.5. HANDLE REJECTED RECORDS - MARK IN INVOICE ALL
        # ---------------------------------------------------------
        # Mark rejected records with "Review Status = Rejected"
        # Files remain in R2 but will be skipped during future processing
        rejected_row_ids = set()
        
        print("=" * 60)
        print("DEBUG: Starting rejection detection")
        print(f"DEBUG: df_date columns: {df_date.columns.tolist() if not df_date.empty else 'EMPTY'}")
        print(f"DEBUG: df_date shape: {df_date.shape}")
        print(f"DEBUG: df_amount columns: {df_amount.columns.tolist() if not df_amount.empty else 'EMPTY'}")
        print(f"DEBUG: df_amount shape: {df_amount.shape}")
        
        # Check for Row_Id column name variations
        date_rowid_col = None
        if 'Row_Id' in df_date.columns:
            date_rowid_col = 'Row_Id'
        elif 'Original Row ID' in df_date.columns:
            date_rowid_col = 'Original Row ID'
        
        print(f"DEBUG: date_rowid_col = {date_rowid_col}")
        
        if 'Verification Status' in df_date.columns and date_rowid_col:
            print(f"DEBUG: Verification Status values in df_date: {df_date['Verification Status'].unique()}")
            
            # Collect manually rejected records
            date_rejected = df_date[df_date['Verification Status'].astype(str).str.lower() == 'rejected'].copy()
            print(f"DEBUG: Found {len(date_rejected)} manually rejected rows in Verify Dates")
            if not date_rejected.empty:
                rejected_ids = date_rejected[date_rowid_col].astype(str).str.replace(r'\.0$', '', regex=True)
                rejected_row_ids.update(rejected_ids.tolist())
                print(f"✓ Found {len(rejected_ids)} manually rejected row IDs in Verify Dates: {rejected_ids.tolist()}")
            
            # ALSO collect "Already Verified" records (auto-reject duplicates)
            if 'Audit Findings' in df_date.columns:
                already_verified = df_date[df_date['Audit Findings'].astype(str).str.contains('Already Verified', case=False, na=False)].copy()
                if not already_verified.empty:
                    already_verified_ids = already_verified[date_rowid_col].astype(str).str.replace(r'\.0$', '', regex=True)
                    rejected_row_ids.update(already_verified_ids.tolist())
                    print(f"✓ Auto-rejecting {len(already_verified_ids)} 'Already Verified' records: {already_verified_ids.tolist()}")
        else:
            print(f"DEBUG: Cannot check Verify Dates - Verification Status exists: {'Verification Status' in df_date.columns}, date_rowid_col: {date_rowid_col}")
        
        amount_rowid_col = None
        if 'Row_Id' in df_amount.columns:
            amount_rowid_col = 'Row_Id'
        elif 'Original Row ID' in df_amount.columns:
            amount_rowid_col = 'Original Row ID'
        
        print(f"DEBUG: amount_rowid_col = {amount_rowid_col}")
        
        if 'Verification Status' in df_amount.columns and amount_rowid_col:
            print(f"DEBUG: Verification Status values in df_amount: {df_amount['Verification Status'].unique()}")
            
            # Collect manually rejected records
            amount_rejected = df_amount[df_amount['Verification Status'].astype(str).str.lower() == 'rejected'].copy()
            print(f"DEBUG: Found {len(amount_rejected)} manually rejected rows in Verify Amount")
            if not amount_rejected.empty:
                rejected_ids = amount_rejected[amount_rowid_col].astype(str).str.replace(r'\.0$', '', regex=True)
                rejected_row_ids.update(rejected_ids.tolist())
                print(f"✓ Found {len(rejected_ids)} manually rejected row IDs in Verify Amount: {rejected_ids.tolist()}")
            
            # ALSO collect "Already Verified" records (auto-reject duplicates)
            if 'Audit Findings' in df_amount.columns:
                already_verified = df_amount[df_amount['Audit Findings'].astype(str).str.contains('Already Verified', case=False, na=False)].copy()
                if not already_verified.empty:
                    already_verified_ids = already_verified[amount_rowid_col].astype(str).str.replace(r'\.0$', '', regex=True)
                    rejected_row_ids.update(already_verified_ids.tolist())
                    print(f"✓ Auto-rejecting {len(already_verified_ids)} 'Already Verified' records: {already_verified_ids.tolist()}")
        else:
            print(f"DEBUG: Cannot check Verify Amount - Verification Status exists: {'Verification Status' in df_amount.columns}, amount_rowid_col: {amount_rowid_col}")
        
        print(f"DEBUG: Total rejected_row_ids: {rejected_row_ids}")
        
        
        # If there are rejected records, mark them in Invoice All
        if rejected_row_ids and 'Row_Id' in df_raw.columns:
            print(f"Processing {len(rejected_row_ids)} rejected Row_Ids...")
            df_raw['Row_Id_str'] = df_raw['Row_Id'].astype(str)
            rejected_mask = df_raw['Row_Id_str'].isin(rejected_row_ids)
            
            print(f"Found {rejected_mask.sum()} matching rows in Invoice All")
            
            if rejected_mask.any():
                # Instead of deleting, mark as "Rejected" in Review Status
                if 'Review Status' not in df_raw.columns:
                    df_raw['Review Status'] = 'Pending'
                
                df_raw.loc[rejected_mask, 'Review Status'] = 'Rejected'
                
                print(f"✓ Marked {rejected_mask.sum()} records as 'Rejected' in Invoice All")
                print(f"ℹ️ Files remain in R2 storage (not deleted)")
                print(f"ℹ️ These records will be skipped in future processing")
                corrections_made = True
            else:
                print(f"⚠️ Warning: Could not find matching records in Invoice All for Row_Ids: {rejected_row_ids}")
                print(f"   This might mean they were already processed in a previous sync")
            
            df_raw = df_raw.drop(columns=['Row_Id_str'], errors='ignore')
        
        print("=" * 60)

        # ---------------------------------------------------------
        # 2. APPLY CORRECTIONS TO DF_RAW (IN MEMORY)
        # ---------------------------------------------------------
        
        # --- A. Apply Date/Receipt Number Corrections ---
        if 'Verification Status' in df_date.columns:
            # Filter for DONE items
            date_done = df_date[df_date['Verification Status'].astype(str).str.lower() == 'done'].copy()
            
            if not date_done.empty:
                print(f"Applying {len(date_done)} date/receipt corrections...")
                # Cleanup keys
                date_done['Receipt Link_clean'] = date_done['Receipt Link'].astype(str).str.strip()
                date_done['Receipt Number'] = date_done['Receipt Number'].astype(str).str.replace(r'\.0$', '', regex=True)
                
                # We need to broadcast strict DD-MM-YYYY format
                date_done['Date'] = date_done['Date'].apply(normalize_date)
                
                # Iterate and update raw
                for _, row in date_done.iterrows():
                    link = row.get('Receipt Link_clean')
                    new_rec_num = row.get('Receipt Number')
                    new_date = row.get('Date')
                    
                    if not link: continue
                    
                    # Find matching rows in df_raw
                    mask = df_raw['Receipt Link'].astype(str).str.strip() == link
                    if mask.any():
                        if new_rec_num: df_raw.loc[mask, 'Receipt Number'] = new_rec_num
                        if new_date: df_raw.loc[mask, 'Date'] = format_to_mmm(new_date) # Convert to MMM format
                        
                        # Mark as Verified
                        df_raw.loc[mask, 'Review Status'] = 'Verified'
                        corrections_made = True

        # --- B. Apply Amount Corrections ---
        if 'Verification Status' in df_amount.columns:
            # Filter for DONE items
            amt_done = df_amount[df_amount['Verification Status'].astype(str).str.lower() == 'done'].copy()
            
            if not amt_done.empty:
                print(f"Applying {len(amt_done)} amount corrections...")
                # Iterate and update
                for _, row in amt_done.iterrows():
                    link = str(row.get('Receipt Link', '')).strip()
                    desc = str(row.get('Description', '')).strip()
                    
                    # New values
                    new_qty = row.get('Quantity')
                    new_rate = row.get('Rate')
                    new_amt = row.get('Amount')
                    
                    if not link: continue

                    # Find matching row in df_raw
                    mask = (
                        (df_raw['Receipt Link'].astype(str).str.strip() == link) & 
                        (df_raw['Description'].astype(str).str.strip() == desc)
                    )
                    
                    if mask.any():
                        if pd.notna(new_qty) and str(new_qty) != '': df_raw.loc[mask, 'Quantity'] = new_qty
                        if pd.notna(new_rate) and str(new_rate) != '': df_raw.loc[mask, 'Rate'] = new_rate
                        if pd.notna(new_amt) and str(new_amt) != '': df_raw.loc[mask, 'Amount'] = new_amt
                        
                        # Recalculate Amount Mismatch/Calculated Amount if possible
                        try:
                            q = float(new_qty) if new_qty else 0
                            r = float(new_rate) if new_rate else 0
                            a = float(new_amt) if new_amt else 0
                            calc = q * r
                            diff = abs(calc - a)
                            
                            df_raw.loc[mask, 'Calculated Amount'] = calc
                            df_raw.loc[mask, 'Amount Mismatch'] = diff
                        except: pass
                        
                        df_raw.loc[mask, 'Review Status'] = 'Verified'
                        corrections_made = True

        # ---------------------------------------------------------
        # 3. SAVE UPDATED RAW DATA (INVOICE ALL)
        # ---------------------------------------------------------
        if corrections_made:
            print("Updating 'Invoice All' sheet with corrections...")
            # Ensure dates are in dd-mmm-yyyy format
            if 'Date' in df_raw.columns:
                df_raw['Date'] = safe_format_date_series(df_raw['Date'])
            ws_all = sheets['all']
            ws_all.clear()
            set_with_dataframe(ws_all, df_raw, include_index=False, include_column_header=True)
            print("Invoice All updated.")

        # ---------------------------------------------------------
        # 4. BUILD AND SAVE VERIFIED DATA (INVOICE VERIFIED)
        # ---------------------------------------------------------
        # Use build_verified function to properly filter out Pending records
        # and apply all verified corrections from date and amount sheets
        
        final_df = build_verified(df_raw, df_date, df_amount)
        
        ws_verified = sheets.get('verified')
        if ws_verified:
            ws_verified.clear()
            set_with_dataframe(ws_verified, final_df, include_index=False, include_column_header=True)
            print(f"Invoice Verified sheet updated with {len(final_df)} rows.")
            
            # IMPORTANT: Also mark these records as 'Verified' in Invoice All
            # This prevents them from going through verification again next time
            if not final_df.empty and 'Receipt Number' in final_df.columns:
                verified_receipt_numbers = set(final_df['Receipt Number'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip())
                if verified_receipt_numbers and 'Receipt Number' in df_raw.columns:
                    df_raw['Receipt Number_str'] = df_raw['Receipt Number'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                    verified_mask = df_raw['Receipt Number_str'].isin(verified_receipt_numbers)
                    
                    if 'Review Status' not in df_raw.columns:
                        df_raw['Review Status'] = ''
                    
                    # Only update records that aren't already marked as Rejected or Already Verified
                    # (preserve these special statuses)
                    status_lower = df_raw['Review Status'].astype(str).str.lower()
                    update_mask = verified_mask & (~status_lower.isin(['rejected', 'already verified']))
                    if update_mask.any():
                        df_raw.loc[update_mask, 'Review Status'] = 'Verified'
                        print(f"Marked {update_mask.sum()} records as 'Verified' in Invoice All")
                        
                        # Save Invoice All with updated Review Status
                        if 'Date' in df_raw.columns:
                            df_raw['Date'] = safe_format_date_series(df_raw['Date'])
                        df_raw = df_raw.drop(columns=['Receipt Number_str'], errors='ignore')
                        ws_all = sheets['all']
                        ws_all.clear()
                        set_with_dataframe(ws_all, df_raw, include_index=False, include_column_header=True)
                        print("Invoice All updated with Verified status.")
        else:
            print("Invoice Verified sheet not found.")
            
        # ---------------------------------------------------------
        # 5. CLEANUP: Delete 'Done' and 'Rejected' rows from Verification Sheets
        # ---------------------------------------------------------
        
        # --- Clean Verify Dates ---
        if 'Verification Status' in df_date.columns:
            # STRICT FILTERING: Keep ONLY "Pending" records
            # After sync, review sheets should only contain records that need attention
            df_date_status_lower = df_date['Verification Status'].astype(str).str.lower().str.strip()
            df_date_clean = df_date[df_date_status_lower == 'pending'].copy()
            
            print(f"Keeping {len(df_date_clean)} Pending records in Verify Dates (removed {len(df_date) - len(df_date_clean)} completed/rejected/done records)")
            
            # Write back to sheet
            ws_dates = sheets['verify_dates']
            
            # ALWAYS ensure headers are in row 1 (reordered)
            headers = ["Verification Status", "Receipt Number", "Date", "Audit Findings", "Receipt Link", "Upload Date", "Row_Id"]
            ensure_headers_in_row1(ws_dates, headers)
            
            # Clear all data below row 1 (keep headers)
            # Use batch_clear instead of delete_rows to avoid API error
            if ws_dates.row_count > 1:
                # Calculate the last column letter (G for 7 columns)
                last_col = chr(65 + len(headers) - 1)
                ws_dates.batch_clear([f'A2:{last_col}{ws_dates.row_count}'])
            
            # Append clean data if exists
            if not df_date_clean.empty:
                rows = [[str(df_date_clean.iloc[i][col]) for col in df_date_clean.columns] for i in range(len(df_date_clean))]
                ws_dates.append_rows(rows, value_input_option='USER_ENTERED')
            print(f"✓ Verify Dates cleaned: {len(df_date_clean)} Pending records remain.")
            
        # --- Clean Verify Amount ---
        if 'Verification Status' in df_amount.columns:
            # STRICT FILTERING: Keep ONLY "Pending" records
            df_amount_status_lower = df_amount['Verification Status'].astype(str).str.lower().str.strip()
            df_amount_clean = df_amount[df_amount_status_lower == 'pending'].copy()
            
            print(f"Keeping {len(df_amount_clean)} Pending records in Verify Amount (removed {len(df_amount) - len(df_amount_clean)} completed/rejected/done records)")
            
            # Write back to sheet
            ws_amount = sheets['verify_amount']
            
            # ALWAYS ensure headers are in row 1
            headers = ["Verification Status", "Receipt Number", "Receipt Link", "Description", 
                      "Quantity", "Rate", "Amount", "Amount Mismatch", "Row_Id"]
            ensure_headers_in_row1(ws_amount, headers)
            
            # Clear all data below row 1 (keep headers)
            # Use batch_clear instead of delete_rows to avoid API error
            if ws_amount.row_count > 1:
                # Calculate the last column letter (I for 9 columns)
                last_col = chr(65 + len(headers) - 1)
                ws_amount.batch_clear([f'A2:{last_col}{ws_amount.row_count}'])
            
            # Append clean data if exists
            if not df_amount_clean.empty:
                rows = [[str(df_amount_clean.iloc[i][col]) for col in df_amount_clean.columns] for i in range(len(df_amount_clean))]
                ws_amount.append_rows(rows, value_input_option='USER_ENTERED')
            print(f"✓ Verify Amount cleaned: {len(df_amount_clean)} Pending records remain.")
        
        # --- Clean Invoice All: Remove "Already Processed" records ---
        if 'Review Status' in df_raw.columns:
            already_processed_count = (df_raw['Review Status'].astype(str).str.strip().str.lower() == 'already processed').sum()
            if already_processed_count > 0:
                df_raw_clean = df_raw[df_raw['Review Status'].astype(str).str.strip().str.lower() != 'already processed'].copy()
                
                ws_all = sheets['all']
                ws_all.clear()
                set_with_dataframe(ws_all, df_raw_clean, include_index=False, include_column_header=True)
                print(f"Cleaned 'Invoice All': Removed {already_processed_count} Already Processed records.")

    except Exception as e:
        print(f"Error in Sync Verified Data: {e}")
        import traceback
        traceback.print_exc()
        raise e # Re-raise so Streamlit knows it failed


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

def delete_from_r2(storage_client, bucket: str, key: str) -> bool:
    """
    Deletes a file from Cloudflare R2.
    Returns True if successful, False otherwise.
    """
    try:
        storage_client.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted from R2: {key}")
        return True
    except Exception as e:
        print(f"Failed to delete {key} from R2: {e}", file=sys.stderr)
        return False

# =================================
# SHEET HELPERS
# =================================
def connect_to_sheets(sheet_id: str, credentials) -> Dict[str, Any]:
    """Connects to the Google Sheet and ensures all required sheets/headers exist."""
    try:
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(sheet_id)
        
        try:
            spreadsheet.update_properties({"locale": "en_US"})
        except: pass
        
        # 1. Invoice All
        HEADERS_ALL = [
            "Receipt Number", "Date", "Customer Name", "Mobile Number", "Car Number", "Odometer Reading", 
            "Description", "Labour/Part", "Quantity", "Rate", "Amount", "Calculated Amount", "Amount Mismatch", 
            "Review Status", "Total Bill Amount", "Accuracy %", "Receipt Link", "Source File", "Upload Date",
            "_r2_key", "Date_Eng", "Row_Id"
        ]
        
        # 2. Verify Dates - Reordered as per requirement
        HEADERS_VERIFY_DATES = [
            "Verification Status", "Receipt Number", "Date", "Audit Findings", "Receipt Link", 
            "Upload Date", "Row_Id"
        ]
        
        # 3. Verify Amount - Reordered with Amount Mismatch
        HEADERS_VERIFY_AMOUNT = [
            "Verification Status", "Receipt Number", "Amount Mismatch", "Description", 
            "Quantity", "Rate", "Amount", "Receipt Link", "Row_Id"
        ]

        # 4. Invoice Verified (Result Sheet)
        # Headers will be determined dynamically by build_verified, but we can set defaults
        HEADERS_VERIFIED = HEADERS_ALL 

        sheets = {}

        def setup_worksheet(title, headers, rows_min=1000):
            try:
                ws = spreadsheet.worksheet(title)
            except WorksheetNotFound:
                ws = spreadsheet.add_worksheet(title=title, rows=rows_min, cols=len(headers))
                ws.append_row(headers, value_input_option='USER_ENTERED')
                print(f"Created new worksheet: {title}")
            
            # Ensure headers match schema
            current_cols = ws.col_count
            required_cols = len(headers)
            if current_cols < required_cols:
                ws.add_cols(required_cols - current_cols)
            
            existing_headers = ws.row_values(1)
            if existing_headers != headers and title != "Invoice Verified": # Don't enforce on dynamic result sheet
                print(f"Updating headers in {title} to match new schema...")
                ws.update(values=[headers], range_name='A1:' + rowcol_to_a1(1, required_cols), value_input_option='USER_ENTERED')
            
            return ws

        sheets['all'] = setup_worksheet("Invoice All", HEADERS_ALL, 5000)
        sheets['verify_dates'] = setup_worksheet("Verify Dates and Receipt Numbers", HEADERS_VERIFY_DATES, 1000)
        sheets['verify_amount'] = setup_worksheet("Verify Amount", HEADERS_VERIFY_AMOUNT, 1000)
        sheets['verified'] = setup_worksheet("Invoice Verified", HEADERS_VERIFIED, 5000)
        
        return sheets
    except Exception as e:
        print(f"Error connecting to Sheets: {e}", file=sys.stderr)
        return None

# =================================
# SHEET HEADER MANAGEMENT
# =================================
def ensure_headers_in_row1(worksheet, expected_headers: List[str]):
    """
    Ensures that the worksheet has the correct headers in Row 1.
    This is called before ANY data operation to guarantee headers are never corrupted.
    """
    try:
        # Read current row 1
        current_row1 = worksheet.row_values(1) if worksheet.row_count > 0 else []
        
        # If row 1 is empty or doesn't match expected headers, write them
        if not current_row1 or current_row1 != expected_headers:
            # Clear row 1 and write headers
            range_notation = f'A1:{chr(65 + len(expected_headers) - 1)}1'
            worksheet.update(range_notation, [expected_headers], value_input_option='USER_ENTERED')
            print(f"Ensured headers in {worksheet.title}: {expected_headers}")
        
        return True
    except Exception as e:
        print(f"Error ensuring headers in {worksheet.title}: {e}")
        return False

# =================================
# VERIFICATION ANOMALY LOGIC (Backend Pre-processing)
# =================================
def run_verification_logic(ws_all, ws_verify, ws_verified=None):
    """
    Reads 'Invoice All', checks for anomalies, updates 'Verify Dates'.
    This is different from build_verified (which merges corrections).
    This function *finds* the problems.
    Excludes already verified receipts from re-verification.
    """
    if not HAS_GSPREAD_DF: return

    print("Analyzing data...")
    
    # Get verified receipts to exclude
    verified_receipt_dates, verified_row_ids = get_verified_receipts(ws_verified)
    
    try:
        df = get_as_dataframe(ws_all, header=0, evaluate_formulae=True, parse_dates=True)
        if df.empty or 'Receipt Number' not in df.columns or 'Date' not in df.columns:
            ws_verify.clear()
            headers = ["Verification Status", "Receipt Number", "Date", "Audit Findings", "Receipt Link", "Upload Date", "Row_Id"]
            ws_verify.append_row(headers)
            return

        df = df.copy().reset_index()
        df['Row_Id'] = df['index'] + 2 
        df = df.drop(columns=['index'])

        # Clean Receipt Numbers - strip .0 suffix for consistent matching
        df['Receipt Number'] = df['Receipt Number'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df['Receipt Link']   = df['Receipt Link'].astype(str).str.strip()
        
        # Parse dates carefully - read as string first, then parse with dayfirst=True to avoid misinterpretation
        # This prevents "11-May-2025" from being read as "05-Nov-2025"
        df['Date'] = pd.to_datetime(df['Date'].astype(str), errors='coerce', dayfirst=True)
        
        # SHOW ALL RECORDS - Don't filter by status
        # Instead, we'll mark them properly and let the user see all records sorted by status
        initial_count = len(df)
        
        # Track existing status for proper display in verification tab
        # These records will show with their current status (Verified/Rejected/Already Verified)
        # and appear at the bottom due to sorting logic
        df['_existing_status'] = ''
        if 'Review Status' in df.columns:
            status_lower = df['Review Status'].astype(str).str.strip().str.lower()
            # Mark records that are already processed, but DON'T filter them out
            df.loc[status_lower.isin(['already processed', 'verified', 'rejected', 'already verified']), '_existing_status'] = df.loc[status_lower.isin(['already processed', 'verified', 'rejected', 'already verified']), 'Review Status']
            print(f"Found {(df['_existing_status'] != '').sum()} already-verified/rejected records to display")
        
        # No filtering - show all records including verified ones
        # They will be sorted properly by status
        
        # Mark records that match verified Receipt ID + Date as "Already Verified" 
        # This ONLY applies to NEW records (not already in verified), flagging duplicates
        df['_already_verified_flag'] = False
        if verified_receipt_dates:
            df['Date_str'] = df['Date'].dt.strftime('%d-%b-%Y')
            df['_temp_key'] = list(zip(df['Receipt Number'], df['Date_str']))
            verified_pair_mask = df['_temp_key'].isin(verified_receipt_dates)
            if verified_pair_mask.any():
                # Mark these instead of filtering out
                df.loc[verified_pair_mask, '_already_verified_flag'] = True
                print(f"Found {verified_pair_mask.sum()} NEW records matching already-verified receipts")
            df = df.drop(columns=['_temp_key', 'Date_str'], errors='ignore')
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            print(f"Total filtered: {filtered_count} already-processed records")

        # Group by Receipt Link ONLY to show ONE ROW PER UPLOADED FILE
        # This ensures 3 uploaded files = 3 rows in review tab
        # Even if a receipt has multiple line items, they all share the same Receipt Link (file upload)
        df_sorted = df.sort_values(['Receipt Link', 'Row_Id'], kind='stable')
        df_final = df_sorted.groupby(['Receipt Link'], as_index=False).first()

        df_final = df_final.sort_values(['Receipt Number', 'Date'], kind='stable').reset_index(drop=True)

        prev = df_final['Date'].where(df_final['Date'].notna()).ffill().shift()
        df_final['Date_Diff_days'] = (df_final['Date'] - prev).dt.days

        def fmt_diff(x):
            if pd.isna(x) or x == 0: return ""
            return f"Date Diff: {int(x)}"

        df_final['Date_Diff'] = df_final['Date_Diff_days'].apply(fmt_diff)
        df_final['Date_Missing_Comment'] = np.where(df_final['Date'].isna(), 'Missing Date', '')
        
        # Check for duplicates at the FILE level (Receipt Link)
        # If two files have the same Receipt Number, that's a duplicate upload
        df_final['Duplicate_Receipt_Final'] = df_final['Receipt Number'].duplicated(keep=False)
        df_final['Duplicate_Link_Final']    = df_final['Receipt Link'].duplicated(keep=False)

        def build_audit(row):
            parts = []
            # PRIORITY: Already Verified comes first
            if row.get('_already_verified_flag', False):
                parts.append("Already Verified")
            if row['Date_Diff']: parts.append(row['Date_Diff'])
            if row['Date_Missing_Comment']: parts.append(row['Date_Missing_Comment'])
            if row['Duplicate_Receipt_Final']: parts.append("Duplicate Receipt Number")
            if row['Duplicate_Link_Final']: parts.append("Duplicate Receipt Link")
            return " | ".join(parts) if parts else ""

        df_final['Audit Findings'] = df_final.apply(build_audit, axis=1)

        # Show ALL records in Review tab (not just those with issues)
        # Records without issues will show as "Done" (no action needed)
        output = df_final.copy()
        
        # Set Verification Status based on Audit Findings and existing status
        def set_status(row):
            # If record has an existing status (Verified/Rejected/etc), preserve it
            existing = row.get('_existing_status', '')
            if existing:
                # Map existing statuses to display values
                existing_lower = str(existing).strip().lower()
                if existing_lower in ['verified', 'done']:
                    return 'Done'
                elif existing_lower == 'rejected':
                    return 'Rejected'
                elif existing_lower == 'already verified':
                    return 'Already Verified'
                # Fall through for other cases
            
            # For new/pending records, check audit findings
            audit = str(row.get('Audit Findings', '')).strip()
            
            # DUPLICATE HANDLING: Mark as special status for user attention
            if 'Duplicate Receipt Number' in audit:
                return 'Duplicate Receipt Number'
            
            # Handle other special cases
            if 'Already Verified' in audit:
                return 'Already Verified'
            elif not audit or audit == '':
                # AUTO-DONE: If no issues found, mark as Done automatically
                return 'Done'
            else:
                # Has issues - needs review
                return 'Pending'
        
        output['Verification Status'] = output.apply(set_status, axis=1)
        output['Upload Date'] = datetime.now(IST).strftime("%d-%b-%Y")
        output['Date'] = output['Date'].dt.strftime("%d-%b-%Y")  # Use dd-mmm-yyyy format

        # Reorder columns to match new specification
        output = output[
            ["Verification Status", "Receipt Number", "Date", "Audit Findings", 
             "Receipt Link", "Upload Date", "Row_Id"]
        ].reset_index(drop=True)
        
        # CRITICAL FIX: Filter out records that don't need review
        # Only keep: Pending and Duplicate Receipt Number (these need user attention)
        # Remove: Done, Rejected, Already Verified (these don't need review)
        status_lower = output['Verification Status'].astype(str).str.lower().str.strip()
        output = output[status_lower.isin(['pending', 'duplicate receipt number'])].copy()
        
        print(f"Filtered to {len(output)} records needing review (excluded Done/Rejected/Already Verified)")
        
        # Sort by priority: Pending first, then Duplicate
        status_priority = {
            'Pending': 1, 
            'Duplicate Receipt Number': 2
        }
        output['_sort_priority'] = output['Verification Status'].map(status_priority).fillna(5)
        output = output.sort_values('_sort_priority').drop(columns=['_sort_priority']).reset_index(drop=True)

        # ALWAYS ensure headers are in row 1 before writing data (reordered)
        headers = ["Verification Status", "Receipt Number", "Date", "Audit Findings", "Receipt Link", "Upload Date", "Row_Id"]
        ensure_headers_in_row1(ws_verify, headers)
        
        if not output.empty:
            # Clear all data below row 1 (keep headers)
            # Use batch_clear instead of delete_rows to avoid API error
            if ws_verify.row_count > 1:
                last_col = chr(65 + len(headers) - 1)
                ws_verify.batch_clear([f'A2:{last_col}{ws_verify.row_count}'])
            
            # Append new data starting from row 2
            rows = [[str(output.iloc[i][col]) for col in output.columns] for i in range(len(output))]
            ws_verify.append_rows(rows, value_input_option='USER_ENTERED')
            print(f"Verification Sheet updated with {len(output)} records needing review.")
        else:
            # No records need review - clear the sheet
            if ws_verify.row_count > 1:
                last_col = chr(65 + len(headers) - 1)
                ws_verify.batch_clear([f'A2:{last_col}{ws_verify.row_count}'])
            print("✓ No records need review - verification sheet cleared.")
            # Just ensure headers exist, clear data rows
            if ws_verify.row_count > 1:
                last_col = chr(65 + len(headers) - 1)
                ws_verify.batch_clear([f'A2:{last_col}{ws_verify.row_count}'])

    except Exception as e:
        print(f"Error running verification logic: {e}", file=sys.stderr)

def get_verified_receipts(ws_verified, ws_all=None):
    """
    Returns sets of verified and rejected receipts to prevent re-processing.
    Returns: (receipt_number_date_set, row_id_set)
    receipt_number_date_set includes BOTH verified AND rejected records
    """
    if not HAS_GSPREAD_DF or not ws_verified:
        return set(), set()
    
    try:
        df = get_as_dataframe(ws_verified, header=0, evaluate_formulae=True)
        if df.empty:
            return set(), set()
        
        if "Receipt Number" in df.columns:
            df["Receipt Number"] = df["Receipt Number"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        receipt_date_set = set()
        if "Receipt Number" in df.columns and "Date" in df.columns:
            # Normalize dates to dd-mmm-yyyy format for consistent matching
            df['Date_normalized'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True).dt.strftime('%d-%b-%Y')
            
            for _, row in df.iterrows():
                rec_num = str(row.get("Receipt Number", "")).strip()
                date_val = row.get("Date_normalized", "")
                if rec_num and date_val and date_val != 'NaT':
                    receipt_date_set.add((rec_num, date_val))
        
        # ALSO get rejected receipts from Invoice All
        if ws_all:
            try:
                df_all = get_as_dataframe(ws_all, header=0, evaluate_formulae=True)
                if not df_all.empty and "Review Status" in df_all.columns:
                    rejected_mask = df_all['Review Status'].astype(str).str.lower() == 'rejected'
                    df_rejected = df_all[rejected_mask].copy()
                    
                    if not df_rejected.empty and "Receipt Number" in df_rejected.columns and "Date" in df_rejected.columns:
                        df_rejected["Receipt Number"] = df_rejected["Receipt Number"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                        df_rejected['Date_normalized'] = pd.to_datetime(df_rejected['Date'], errors='coerce', dayfirst=True).dt.strftime('%d-%b-%Y')
                        
                        for _, row in df_rejected.iterrows():
                            rec_num = str(row.get("Receipt Number", "")).strip()
                            date_val = row.get("Date_normalized", "")
                            if rec_num and date_val and date_val != 'NaT':
                                receipt_date_set.add((rec_num, date_val))
                        
                        print(f"Added {len(df_rejected)} rejected receipts to skip list")
            except Exception as e:
                print(f"Could not load rejected receipts: {e}")
        
        row_id_set = set()
        if "Row_Id" in df.columns:
            row_id_set = set(df["Row_Id"].dropna().astype(str))
        
        print(f"Loaded {len(receipt_date_set)} verified+rejected receipt-dates and {len(row_id_set)} verified Row_Ids")
        return receipt_date_set, row_id_set
    except Exception as e:
        print(f"Error reading verified receipts: {e}", file=sys.stderr)
        return set(), set()

# =================================
# PROCESSING LOGIC
# =================================
def process_single_image(storage_client, bucket_name, key, temp_dir):
    filename = key.split("/")[-1]
    local_path = os.path.join(temp_dir, filename)
    receipt_link = ""
    
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

        rec_date_base = normalize_date(header.get("date", ""))
        rec_date_mmm = format_to_mmm(rec_date_base)
        rec_date_us = format_to_us(rec_date_base)

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
                "Date": rec_date_mmm or "",
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
                "Amount Mismatch": difference,
                "Review Status": review_status,
                "Total Bill Amount": safe_num(header.get("total_bill_amount"), int),
                "Accuracy %": safe_num(item.get("confidence"), int),
                "Receipt Link": receipt_link,
                "Source File": filename,
                "Upload Date": upload_date_mmm,
                "_r2_key": key,
                "Date_Eng": rec_date_us or ""
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
    if not new_records: return
    try:
        # Only ensure headers if sheet appears empty (row_count <= 1)
        # For sheets with existing data, we trust headers are already correct
        # This prevents interference when appending to Invoice All
        if worksheet.row_count <= 1:
            ensure_headers_in_row1(worksheet, headers)
        
        # Append data rows starting from row 2+
        rows = [[str(rec.get(h, "")) for h in headers] for rec in new_records]
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')
        print(f"Added {len(rows)} rows to '{worksheet.title}'")
    except Exception as e:
        print(f"Sheet Update Error: {e}", file=sys.stderr)

# =================================
# MAIN
# =================================
def main():
    print("--- Starting Invoice Processing ---")
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

    # Fetch unique source files from "Invoice Verified" to skip re-processing
    # Extract suffix from "IMG" onwards for matching (handles timestamp prefixes)
    verified_source_suffixes = set()
    try:
        if HAS_GSPREAD_DF and sheets.get('verified'):
            df_verified = get_as_dataframe(sheets['verified'], header=0, evaluate_formulae=True)
            if not df_verified.empty and 'Source File' in df_verified.columns:
                for src_file in df_verified['Source File'].dropna().astype(str).str.strip():
                    # Extract from "IMG" onwards (e.g., "20251218_195040_IMG_file.jpg" -> "IMG_file.jpg")
                    if 'IMG' in src_file:
                        suffix = src_file[src_file.index('IMG'):]
                        verified_source_suffixes.add(suffix)
                    else:
                        # If no IMG, store the whole filename as fallback
                        verified_source_suffixes.add(src_file)
                print(f"Found {len(verified_source_suffixes)} processed files to skip")
        
        # ALSO add source files from Invoice All with Review Status = 'Already Verified' or 'Rejected'
        if HAS_GSPREAD_DF and sheets.get('all'):
            df_all = get_as_dataframe(sheets['all'], header=0, evaluate_formulae=True)
            if not df_all.empty and 'Source File' in df_all.columns and 'Review Status' in df_all.columns:
                skip_statuses = ['already verified', 'rejected']
                skip_mask = df_all['Review Status'].astype(str).str.lower().isin(skip_statuses)
                for src_file in df_all.loc[skip_mask, 'Source File'].dropna().astype(str).str.strip():
                    if 'IMG' in src_file:
                        suffix = src_file[src_file.index('IMG'):]
                        verified_source_suffixes.add(suffix)
                    else:
                        verified_source_suffixes.add(src_file)
                skipped_count = skip_mask.sum()
                if skipped_count > 0:
                    print(f"Found {skipped_count} already-verified/rejected files to skip from Invoice All")
    except Exception as e:
        print(f"Could not fetch verified source files: {e}", file=sys.stderr)

    try:
        all_data_keys = sheets['all'].col_values(sheets['all'].find("_r2_key").col) 
        existing_keys = set(all_data_keys[1:]) 
        invoice_all_current_row_count = sheets['all'].row_count
    except Exception:
        existing_keys = set()
        invoice_all_current_row_count = 1

    try:
        storage_files = client.list_objects_v2(Bucket=bucket, Prefix='uploads/').get('Contents', [])
        keys_to_process = []
        for obj in storage_files:
            key = obj['Key']
            if key.endswith('/'):
                continue
            
            # Extract filename from key (e.g., "uploads/20250112_invoice.jpg" -> "20250112_invoice.jpg")
            filename = key.split('/')[-1]
            
            # Skip if key is in existing_keys (already in Invoice All)
            if key in existing_keys:
                continue
            
            # Extract suffix from "IMG" onwards for comparison
            filename_suffix = filename
            if 'IMG' in filename:
                filename_suffix = filename[filename.index('IMG'):]
            
            # Skip if filename suffix is in verified_source_suffixes (already in Invoice Verified)
            if filename_suffix in verified_source_suffixes:
                print(f"Skipping {filename} - Already in database")
                continue
            
            keys_to_process.append(key)
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
        
        # Fetch verified AND rejected receipts to prevent re-processing
        verified_receipt_dates, verified_row_ids = get_verified_receipts(sheets.get('verified'), sheets.get('all'))
        
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
                        
                        start_row_index = invoice_all_current_row_count + 1
                        amount_rows = []
                        filtered_batch = []  # Only non-duplicate records
                        row_offset = 0  # Track how many duplicates we've skipped
                        seen_in_batch = set()  # Track receipts seen in THIS batch
                        
                        for i, r in enumerate(batch):
                            # Check for duplicates FIRST
                            rec_num = str(r.get("Receipt Number", "")).strip()
                            date_val = str(r.get("Date", "")).strip()
                            receipt_date_pair = (rec_num, date_val)
                            
                            # Check against BOTH existing verified receipts AND current batch
                            is_duplicate = receipt_date_pair in verified_receipt_dates or receipt_date_pair in seen_in_batch
                            
                            if is_duplicate:
                                source = "already in system" if receipt_date_pair in verified_receipt_dates else "duplicate in current batch"
                                print(f"Duplicate detected: Receipt {rec_num} - Adding to Invoice All as 'Already Verified' ({source})")
                                # Add to Invoice All with "Already Verified" status so it won't be re-processed
                                # Assign a proper Row_Id
                                current_row_id = start_row_index + i - row_offset
                                r['Row_Id'] = current_row_id
                                r['Review Status'] = 'Already Verified'
                                filtered_batch.append(r)  # Still add to batch, but with Already Verified status
                                continue
                            
                            # Not a duplicate - add to seen set and process normally
                            seen_in_batch.add(receipt_date_pair)
                            
                            # Assign Row_Id only to non-duplicates
                            current_row_id = start_row_index + i - row_offset
                            r['Row_Id'] = current_row_id
                            filtered_batch.append(r)
                            
                            # Only add to verification if has difference
                            diff = r.get("Amount Mismatch")
                            if isinstance(diff, (int, float)) and diff > 0:
                                amount_rows.append({
                                    "Verification Status": "Pending",
                                    "Receipt Number": rec_num,
                                    "Amount Mismatch": diff,
                                    "Description": r.get("Description"),
                                    "Quantity": r.get("Quantity"),
                                    "Rate": r.get("Rate"),
                                    "Amount": r.get("Amount"),
                                    "Receipt Link": r.get("Receipt Link"),
                                    "Row_Id": current_row_id
                                })
                        
                        update_google_sheet(sheets['all'], filtered_batch, headers_all)
                        update_google_sheet(sheets['verify_amount'], amount_rows, headers_amount)
                        invoice_all_current_row_count += len(filtered_batch)
                        batch = []
                        write_progress(fname, "running")

                except Exception as e:
                    print(f"❌ ERROR processing {fname}: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc()
                    processed_count += 1
                    write_progress(fname)

        if batch:
            print("Uploading final batch...")
            start_row_index = invoice_all_current_row_count + 1
            amount_rows = []
            filtered_batch = []
            row_offset = 0
            seen_in_batch = set()  # Track receipts seen in THIS batch
            
            for i, r in enumerate(batch):
                # Check for duplicates FIRST
                rec_num = str(r.get("Receipt Number", "")).strip()
                date_val = str(r.get("Date", "")).strip()
                receipt_date_pair = (rec_num, date_val)
                
                # Check against BOTH existing verified receipts AND current batch
                is_duplicate = receipt_date_pair in verified_receipt_dates or receipt_date_pair in seen_in_batch
                
                if is_duplicate:
                    source = "already in system" if receipt_date_pair in verified_receipt_dates else "duplicate in current batch"
                    print(f"Duplicate detected: Receipt {rec_num}, Date {date_val} - Adding to Invoice All as 'Already Verified' ({source})")
                    # Add to Invoice All with "Already Verified" status so it won't be re-processed
                    current_row_id = start_row_index + i - row_offset
                    r['Row_Id'] = current_row_id
                    r['Review Status'] = 'Already Verified'
                    filtered_batch.append(r)  # Still add to batch, but with Already Verified status
                    continue
                
                # Not a duplicate - add to seen set and process normally
                seen_in_batch.add(receipt_date_pair)
                
                # Assign Row_Id only to non-duplicates
                current_row_id = start_row_index + i - row_offset
                r['Row_Id'] = current_row_id
                filtered_batch.append(r)
                
                # Only add to verification if has difference
                diff = r.get("Amount Mismatch")
                if isinstance(diff, (int, float)) and diff > 0:
                    amount_rows.append({
                        "Verification Status": "Pending",
                        "Receipt Number": rec_num,
                        "Amount Mismatch": diff,
                        "Description": r.get("Description"),
                        "Quantity": r.get("Quantity"),
                        "Rate": r.get("Rate"),
                        "Amount": r.get("Amount"),
                        "Receipt Link": r.get("Receipt Link"),
                        "Row_Id": current_row_id
                    })
            
            update_google_sheet(sheets['all'], filtered_batch, headers_all)
            update_google_sheet(sheets['verify_amount'], amount_rows, headers_amount)
            print(f"Added {len(filtered_batch)} rows to 'Invoice All'")

        write_progress("Finalizing Reports", "cleanup")
        robust_rmtree(TEMP_DOWNLOAD_DIR)
    
    # 1. Update Verification Sheets (Anomalies) - Exclude already verified receipts
    run_verification_logic(sheets['all'], sheets['verify_dates'], sheets.get('verified'))

    # 2. DO NOT auto-sync after processing - let user review all records first
    # The sync will happen when user clicks "Sync & Finish" button in the UI
    # This allows all records (Pending/Already Verified/Done/Rejected) to be visible in Review tab
    # run_sync_verified_logic(sheets)  # DISABLED - now manual via UI button only

    print("--- Analysis Complete ---")

if __name__ == "__main__":
    main()