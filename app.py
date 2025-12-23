# File: app_streamlit.py (Modified)
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import boto3
import json
import os
import sys
import subprocess
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2 import service_account

# Ensure configs exist
try:
    from configs import get_r2_config, get_users_db, get_gcp_service_account, load_secrets
except ImportError:
    st.error("Missing configuration file.")
    sys.exit(1)

# Import logic from processor to avoid subprocess call
try:
    from invoice_processor_r2_streamlit import run_sync_verified_logic, connect_to_sheets
except ImportError:
    st.error("Could not import processor logic. Ensure 'invoice_processor_r2_streamlit.py' is in the same folder.")
    sys.exit(1)

# ---------------------
# UI CONFIGURATION
# ---------------------
st.set_page_config(page_title="Invoice Insights Hub", page_icon="📄", layout="wide")

def load_css():
    st.markdown("""
    <style>
    .stApp { background-color: #f4f6f9; font-family: 'Segoe UI', sans-serif; }
    
    /* Navigation */
    [data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid #e0e0e0; }
    
    /* Dashboard Container */
    .dashboard-container {
        background: white;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        padding: 20px;
        margin-top: 20px;
        border: 1px solid #eaeaea;
    }
    
    /* Buttons */
    div.stButton > button {
        border-radius: 8px; font-weight: 600; padding: 0.5rem 1rem;
        border: none; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    div.stButton > button:hover { opacity: 0.9; }
    
    /* Header */
    h3 { color: #202124; font-weight: 700; }
    
    /* Custom style for the top-right button container */
    .top-right-button-container {
        display: flex;
        justify-content: flex-end;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True)


# ---------------------
# AUTHENTICATION
# ---------------------
def check_auth():
    if "authenticated" not in st.session_state: st.session_state.authenticated = False
    if st.session_state.authenticated: return True

    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.markdown("<br><br><h2 style='text-align: center;'>🔐 Invoice Hub Login</h2>", unsafe_allow_html=True)
        user = st.text_input("Username")
        pw = st.text_input("Password", type="password")
        if st.button("Sign In", type="primary", use_container_width=True):
            users = get_users_db()
            if users.get(user) and users[user].get("password") == pw:
                st.session_state.authenticated = True
                st.session_state.user = user
                st.session_state.config = users[user]
                st.rerun()
            else:
                st.error("Invalid credentials")
    return False

# ---------------------
# BACKEND HELPERS
# ---------------------
@st.cache_resource
def get_google_creds():
    sa = get_gcp_service_account()
    if not sa: return None
    return service_account.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])

@st.cache_resource
def get_storage():
    cfg = get_r2_config()
    if not cfg: return None
    try:
        from botocore.config import Config
        return boto3.client('s3', 
            endpoint_url=cfg.get("endpoint_url"),
            aws_access_key_id=cfg.get("access_key_id"),
            aws_secret_access_key=cfg.get("secret_access_key"),
            region_name="auto", config=Config(max_pool_connections=10)
        )
    except: return None

# FIX: Renamed creds to _creds to avoid UnhashableParamError
@st.cache_data(ttl=60)
def get_sheet_df(sheet_id, tab_name, _creds):
    try:
        gc = gspread.authorize(_creds)
        sh = gc.open_by_key(sheet_id)
        # Use get_as_dataframe for better data type handling, esp. for dates/numbers
        ws = sh.worksheet(tab_name)
        df = get_as_dataframe(ws, header=0, evaluate_formulae=True, parse_dates=False)
        return df.dropna(axis=0, how='all') # Drop rows that are entirely empty
    except Exception as e: 
        st.error(f"Error reading sheet {tab_name}: {e}")
        return pd.DataFrame()

# ---------------------
# SESSION STATE DATA LOADER (PERFORMANCE OPTIMIZATION)
# ---------------------
def load_data_to_session(sheet_id, creds, force_reload=False):
    """
    Loads data from Google Sheets into session state for instant access.
    Only fetches from API on first call or when force_reload=True.
    
    NOTE: The processor now excludes Done/Rejected/Already Verified records at source,
    so review sheets should only contain Pending and Duplicate Receipt Number records.
    We apply filtering here as an additional safety layer.
    """
    # Check if data is already loaded
    if not force_reload and 'data_loaded' in st.session_state and st.session_state.data_loaded:
        return  # Data already in session, skip API call
    
    with st.spinner("Loading data from database..."):
        # Load review sheets
        df_dates_raw = get_sheet_df(sheet_id, SHEET_VERIFY_DATES, creds)
        df_amount_raw = get_sheet_df(sheet_id, SHEET_VERIFY_AMOUNT, creds)
        
        # SAFETY FILTER: Keep only Pending and Duplicate Receipt Number
        # (Processor should already exclude Done/Rejected/Already Verified, but we double-check)
        
        # Filter dates
        if not df_dates_raw.empty and 'Verification Status' in df_dates_raw.columns:
            status_lower = df_dates_raw['Verification Status'].astype(str).str.lower().str.strip()
            mask = status_lower.isin(['pending', 'duplicate receipt number'])
            st.session_state.df_dates = df_dates_raw[mask].copy()
        else:
            st.session_state.df_dates = df_dates_raw
        
        # Filter amounts
        if not df_amount_raw.empty and 'Verification Status' in df_amount_raw.columns:
            status_lower = df_amount_raw['Verification Status'].astype(str).str.lower().str.strip()
            mask = status_lower.isin(['pending', 'duplicate receipt number'])
            st.session_state.df_amount = df_amount_raw[mask].copy()
        else:
            st.session_state.df_amount = df_amount_raw
        
        # Load verified invoices (no filtering needed - show all verified records)
        st.session_state.df_verified = get_sheet_df(sheet_id, "Invoice Verified", creds)
        
        st.session_state.df_all = get_sheet_df(sheet_id, "Invoice All", creds)
        st.session_state.data_loaded = True
        st.session_state.last_load_time = datetime.now()


def get_session_df(df_key):
    """
    Retrieves DataFrame from session state.
    Returns empty DataFrame if not found.
    """
    return st.session_state.get(df_key, pd.DataFrame())
    
# Function to get the GID for linking
# FIX: Renamed creds to _creds to avoid UnhashableParamError
@st.cache_data(ttl=3600)
def get_gid(name, sheet_id, _creds):
    try:
        gc = gspread.authorize(_creds)
        sh = gc.open_by_key(sheet_id)
        return sh.worksheet(name).id
    except: return "0"
    
# Function to save changes back to Google Sheet
def save_review_data(df: pd.DataFrame, sheet_id: str, tab_name: str, creds, session_key=None):
    if df.empty:
        st.warning("No data to save.")
        return False
        
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(tab_name)
        
        # Clear existing data but keep header (assuming only review data is here)
        ws.clear()
        
        # Write the entire DataFrame back, including header
        set_with_dataframe(ws, df, include_index=False, include_column_header=True)
        
        # OPTIMIZATION: Also update session state so UI refreshes instantly without API call
        if session_key:
            st.session_state[session_key] = df.copy()
        
        return True
    except Exception as e:
        st.error(f"Error saving data to {tab_name}: {e}")
        return False

# Function to run the Sync Verified Data script (extracted for reuse)
def run_sync_verified_data_wrapper(user_conf, creds):
    status_box = st.status("Initializing synchronization...", expanded=True)
    
    try:
        # CRITICAL FIX: Clear cache BEFORE reading data to ensure we get fresh status changes
        # This ensures that "Rejected" statuses saved via "Save Changes" button are detected
        status_box.write("🔄 Clearing cache to ensure fresh data...")
        get_sheet_df.clear()
        
        status_box.write("⚙️ Connecting to Database...")
        sheet_id = user_conf.get("sheet_id", "")
        
        # Use the imported function directly, no subprocess
        sheets = connect_to_sheets(sheet_id, creds)
        
        if not sheets:
             status_box.update(label="❌ Connection Failed", state="error", expanded=True)
             st.error("Could not connect to Google Sheets.")
             return

        status_box.write("🚀 Running Sync Logic...")
        
        # Capture console output to show in UI
        import io
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = io.StringIO()
        
        try:
            run_sync_verified_logic(sheets)
            output = captured_output.getvalue()
        finally:
            sys.stdout = old_stdout
        
        # Parse output for key metrics
        rejected_count = 0
        already_verified_count = 0
        verified_count = 0
        skipped_duplicates = 0
        
        for line in output.split('\n'):
            if 'Auto-rejecting' in line and 'Already Verified' in line:
                try:
                    already_verified_count += int(line.split()[1])
                except:
                    pass
            elif 'Marked' in line and 'Rejected' in line:
                try:
                    rejected_count = int(line.split()[1])
                except:
                    pass
            elif 'Invoice Verified sheet updated with' in line:
                try:
                    verified_count = int(line.split()[-2])
                except:
                    pass
            elif 'Skipping' in line and 'corrected records' in line:
                try:
                    skipped_duplicates = int(line.split()[1])
                except:
                    pass
        
        status_box.update(label="✅ Sync & Cleanup Complete!", state="complete", expanded=False)
        
        # Show detailed summary
        st.success("Sync completed successfully!")
        
        summary_parts = []
        if verified_count > 0:
            summary_parts.append(f"✅ **{verified_count}** records verified")
        if rejected_count > 0:
            summary_parts.append(f"🚫 **{rejected_count}** records rejected")
        if already_verified_count > 0:
            summary_parts.append(f"♻️ **{already_verified_count}** already-verified duplicates auto-rejected")
        if skipped_duplicates > 0:
            summary_parts.append(f"⚠️ **{skipped_duplicates}** corrected records skipped (match verified data)")
        
        if summary_parts:
            st.info("\n\n".join(summary_parts))
        
        # Clear cache again so dashboard updates
        get_sheet_df.clear()
        time.sleep(2)
        st.rerun()

    except Exception as e:
        status_box.update(label="❌ Execution Error", state="error", expanded=True)
        st.error(f"Sync process failed: {e}")


# ---------------------
# PAGE RENDERING
# ---------------------

SHEET_VERIFY_DATES = "Verify Dates and Receipt Numbers"
SHEET_VERIFY_AMOUNT = "Verify Amount"
# User-friendly names
FRIENDLY_DATE_TAB = "Review Dates & Receipts"
FRIENDLY_AMOUNT_TAB = "Review Amounts"

# Helper to find column case-insensitive (used by both tabs)
def _find_col(df, candidates):
    """Find a column in the datafram using case-insensitive matching"""
    for c in candidates:
        if c in df.columns: return c
    # extensive search
    df_cols_norm = {c.lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    for c in candidates:
        norm = c.lower().replace(" ", "").replace("_", "")
        if norm in df_cols_norm:
            return df_cols_norm[norm]
    return None


def render_review_page(user_conf, creds):
    sheet_id = user_conf.get("sheet_id")
    
    # OPTIMIZATION: Load data into session state on first visit (instant access after that)
    if sheet_id and creds:
        load_data_to_session(sheet_id, creds, force_reload=False)
    
    st.markdown("### 🔍 Review & Verify")
    st.markdown("Review the extracted data below and make any necessary corrections. Click **'Save Changes'** when done to preserve your edits.")
    
    # Top button row: Refresh + Sync & Finish
    col_refresh, col_spacer, col_sync = st.columns([1.5, 4.5, 2])
    with col_refresh:
        if st.button("🔄 Refresh Data", key="refresh_data", use_container_width=True):
            load_data_to_session(sheet_id, creds, force_reload=True)
            st.success("Data refreshed!")
            st.rerun()
    
    with col_sync:
        if st.button("✅ Sync & Finish", type="primary", key="sync_top_right", use_container_width=True):
            run_sync_verified_data_wrapper(user_conf, creds)
    
    # Show last refresh time
    if 'last_load_time' in st.session_state:
        st.caption(f"_Last refreshed: {st.session_state.last_load_time.strftime('%I:%M %p')}_")
    
    tab1, tab2, tab3 = st.tabs([FRIENDLY_DATE_TAB, FRIENDLY_AMOUNT_TAB, "Verified Invoices"])


    # --- TAB 1: VERIFY DATES ---
    with tab1:
        st.markdown(f"#### {FRIENDLY_DATE_TAB}")
        if sheet_id and creds:
            # OPTIMIZATION: Read from session state instead of API call (instant performance)
            df_dates = get_session_df('df_dates')
            
            if df_dates.empty:
                st.info("✅ All clear! No date or receipt issues found.")
            else:
                # FIX 1: Reset index to range index to hide it correctly with num_rows='dynamic'
                df_dates = df_dates.reset_index(drop=True)
                
                # CUSTOM SORT: Pending first, then by Receipt Number, then Already Verified, then Done
                def get_sort_key(status):
                    status_lower = str(status).strip().lower()
                    if status_lower == 'pending':
                        return 0          # Highest priority
                    elif status_lower == 'duplicate receipt number':
                        return 1          # Second priority - needs user attention
                    elif status_lower == 'already verified':
                        return 2
                    elif status_lower == 'done':
                        return 3
                    elif status_lower == 'rejected':
                        return 4          # Lowest priority
                    else:
                        return 5          # Unknown statuses last
                
                if 'Verification Status' in df_dates.columns and 'Receipt Number' in df_dates.columns:
                    df_dates['_sort_key'] = df_dates['Verification Status'].apply(get_sort_key)
                    df_dates = df_dates.sort_values(['_sort_key', 'Receipt Number']).drop(columns=['_sort_key']).reset_index(drop=True)

                # FIX 2: Check for Row_Id instead of Row_Index, as that is what the processor outputs
                row_id_col = _find_col(df_dates, ["Row_Id", "Row ID", "row_id", "RowId", "Original Row ID"])
                
                # REPAIR LOGIC: If Row_Id is missing, check for "Column 1" issue
                if not row_id_col:
                    cols_preview = df_dates.columns.tolist()
                    # Check if headers look like default pandas/gspread "Column X" or just numbers
                    is_corrupted = any(str(c).startswith("Column ") for c in cols_preview) or "0" in [str(c) for c in cols_preview]
                    
                    if is_corrupted:
                         st.warning(f"⚠️ Sheet headers appear corrupted (found: {cols_preview[:3]}...).")
                         if st.button("🔧 Repair Headers (Dates Sheet)"):
                             # Restoration logic
                             expected_headers = ['Verification Status', 'Receipt Number', 'Receipt Link', 'Date', 'Audit Findings', 'Upload Date', 'Row_Id', 'Row_Index']
                             try:
                                 STATUS_COL_IDX = 1  # 1-based index for col A
                                 ws = connect_to_sheets(sheet_id, creds)['verify_dates']
                                 
                                 # 1. Read all values
                                 all_vals = ws.get_all_values()
                                 
                                 if not all_vals:
                                     # Empty sheet, just write headers
                                     ws.append_row(expected_headers)
                                 else:
                                     # Update first row
                                     ws.update(range_name='A1:H1', values=[expected_headers])
                                     
                                 st.success("Headers repaired! Please reload.")
                                 get_sheet_df.clear()
                                 time.sleep(1)
                                 st.rerun()
                             except Exception as e:
                                 st.error(f"Repair failed: {e}")
                
                if row_id_col:
                    if row_id_col != "Row_Id":
                        df_dates.rename(columns={row_id_col: "Row_Id"}, inplace=True)
                else:
                    st.warning(f"Missing required column 'Row_Id'. Available columns: {list(df_dates.columns)}")
                
                # 2. Handle Date - KEEP AS TEXT to show exactly what was extracted
                # This allows user to see the raw value and correct if needed
                # Format hint added to column header
                pass  # We will keep Date as text, no conversion needed
                
                # Convert Row_Id to string for display, handling cases where it contains dates/garbage
                if 'Row_Id' in df_dates.columns:
                    df_dates['Row_Id'] = df_dates['Row_Id'].astype(str).str.split().str[0] # Take only the first part (pre-space)

                # 3. Define Status Options - include Duplicate Receipt Number
                status_options = ['Done', 'Pending', 'Duplicate Receipt Number', 'Rejected']
                if 'Verification Status' in df_dates.columns:
                    existing = df_dates['Verification Status'].dropna().unique().tolist()
                    for x in existing:
                        if x not in status_options:
                            status_options.append(x)

                # 4. Prepare DataFrame for editor (using friendly names)
                display_df = df_dates.assign(Verification_Status_Dropdown=df_dates.get('Verification Status', 'Pending')).drop(columns=['Verification Status'], errors='ignore')
                
                # 5. Define Column Order and Names
                FRIENDLY_COLUMN_MAP_DATES = {
                    "Verification_Status_Dropdown": "Status",
                    "Row_Index": "System Row Index",
                    "Receipt Number": "Receipt ID",
                    "Receipt Link": "View Receipt",
                    "Date": "Receipt Date",  # Simplified name
                    "Audit Findings": "Issue Details",
                    "Upload Date": "Date Uploaded",
                    "Row_Id": "Original Row ID"
                }
                
                # Reorder columns to put friendly ones first (as per requirement)
                final_order = [
                    "Verification_Status_Dropdown", 
                    "Receipt Number", 
                    "Date",
                    "Audit Findings",
                    "Receipt Link", 
                    "Upload Date", 
                    "Row_Id"
                ]
                final_order = [col for col in final_order if col in display_df.columns]
                
                edited_df = st.data_editor(
                    display_df[final_order].rename(columns=FRIENDLY_COLUMN_MAP_DATES),
                    column_order=[FRIENDLY_COLUMN_MAP_DATES[c] for c in final_order if c in FRIENDLY_COLUMN_MAP_DATES], # <--- ENFORCES COLUMN ORDER
                    column_config={
                        "Status": st.column_config.SelectboxColumn(
                            "Status",
                            options=status_options,
                            required=True,
                            default='Pending'
                        ),
                        "View Receipt": st.column_config.LinkColumn("View Receipt", display_text="View Receipt"),
                        "Receipt Date": st.column_config.TextColumn(
                            "Receipt Date", 
                            help="Enter date as DD-MM-YYYY (e.g., 10-12-2025) or DD-MMM-YYYY (e.g., 10-Dec-2025)"
                        ),
                        "System Row Index": st.column_config.TextColumn("System Row Index", disabled=True),
                        "Original Row ID": st.column_config.TextColumn("Original Row ID", disabled=True), # FIX: Display as disabled text
                    },
                    hide_index=True,
                    key="editor_dates",
                    num_rows="dynamic"
                )
                
                # Rename back for saving
                edited_df.rename(columns={v: k for k, v in FRIENDLY_COLUMN_MAP_DATES.items()}, inplace=True)
                edited_df.rename(columns={'Verification_Status_Dropdown': 'Verification Status'}, inplace=True)
                
                if st.button("💾 Save Changes (Dates/Receipts)", type="primary"):
                    # Keep date as text - no conversion needed
                    save_df = edited_df.copy()

                    # OPTIMIZATION: Update both Sheets and session state (instant refresh)
                    if save_review_data(save_df, sheet_id, SHEET_VERIFY_DATES, creds, session_key='df_dates'):
                        st.success("Saved successfully!")
                        st.rerun()

    # --- TAB 2: VERIFY AMOUNT ---
    with tab2:
        st.markdown(f"#### {FRIENDLY_AMOUNT_TAB}")
        if sheet_id and creds:
            # OPTIMIZATION: Read from session state instead of API call (instant performance)
            df_amount = get_session_df('df_amount')
            
            if df_amount.empty:
                st.info("✅ All clear! No amount discrepancies found.")
            else:
                # FIX 1: Reset index to range index to hide it correctly with num_rows='dynamic'
                df_amount = df_amount.reset_index(drop=True)
                
                # FIX 2: Check for Row_Id instead of Row_Index
                # Re-use helper logic
                row_id_col = _find_col(df_amount, ["Row_Id", "Row ID", "row_id", "RowId"])
                
                if row_id_col:
                    if row_id_col != "Row_Id":
                        df_amount.rename(columns={row_id_col: "Row_Id"}, inplace=True)

                # Convert Row_Id to string for display
                if 'Row_Id' in df_amount.columns:
                    df_amount['Row_Id'] = df_amount['Row_Id'].astype(str).str.split().str[0]
                
                # CUSTOM SORT: Pending first, then by Receipt Number, then Already Verified, then Done
                def get_sort_key_amount(status):
                    status_lower = str(status).strip().lower()
                    if status_lower == 'pending':
                        return 0
                    elif status_lower == 'duplicate receipt number':
                        return 1
                    elif status_lower == 'already verified':
                        return 2
                    elif status_lower == 'done':
                        return 3
                    elif status_lower == 'rejected':
                        return 4
                    else:
                        return 5
                
                if 'Verification Status' in df_amount.columns and 'Receipt Number' in df_amount.columns:
                    df_amount['_sort_key'] = df_amount['Verification Status'].apply(get_sort_key_amount)
                    df_amount = df_amount.sort_values(['_sort_key', 'Receipt Number']).drop(columns=['_sort_key']).reset_index(drop=True)

                # --- HEADER REPAIR LOGIC (Similar to Date Tab) ---
                if not row_id_col:
                    cols_preview = df_amount.columns.tolist()
                    is_corrupted = any(str(c).startswith("Column ") for c in cols_preview) or "0" in [str(c) for c in cols_preview]
                    
                    if is_corrupted:
                         st.warning(f"⚠️ Sheet headers appear corrupted (found: {cols_preview[:3]}...).")
                         if st.button("🔧 Repair Headers (Amount Sheet)"):
                             expected_headers = [
                                "Verification Status", "Receipt Number", "Receipt Link", "Description", 
                                "Quantity", "Rate", "Amount", "Difference", "Row_Id"
                             ]
                             try:
                                 ws = connect_to_sheets(sheet_id, creds)['verify_amount']
                                 all_vals = ws.get_all_values()
                                 if not all_vals:
                                     ws.append_row(expected_headers)
                                 else:
                                     ws.update(range_name='A1:I1', values=[expected_headers])
                                     
                                 st.success("Headers repaired! Please reload.")
                                 get_sheet_df.clear()
                                 time.sleep(1)
                                 st.rerun()
                             except Exception as e:
                                 st.error(f"Repair failed: {e}")

                # 2. Define Status Options - include Duplicate Receipt Number
                status_options = ['Done', 'Pending', 'Duplicate Receipt Number', 'Rejected']
                if 'Verification Status' in df_amount.columns:
                    existing = df_amount['Verification Status'].dropna().unique().tolist()
                    for x in existing:
                        if x not in status_options:
                            status_options.append(x)

                # 3. Prepare DataFrame
                display_df = df_amount.assign(Verification_Status_Dropdown=df_amount.get('Verification Status', 'Pending')).drop(columns=['Verification Status'], errors='ignore')
                
                # 4. Define Column Order and Names
                FRIENDLY_COLUMN_MAP_AMOUNTS = {
                    "Verification_Status_Dropdown": "Status",
                    "Row_Index": "System Row Index",
                    "Receipt Number": "Receipt ID",
                    "Receipt Link": "View Receipt",
                    "Description": "Item Description",
                    "Quantity": "Quantity",
                    "Rate": "Unit Price",
                    "Amount": "Total Amount",
                    "Difference": "Mismatch (Diff)",
                    "Row_Id": "Original Row ID"
                }

                # ROBUST COLUMN MAPPING
                # Define the target internal names we expect (keys in FRIENDLY_COLUMN_MAP_AMOUNTS)
                # We need to map the ACTUAL columns in df_amount to these target internal names

                # Create a map from Expected Name -> Actual Name in DF
                actual_col_map = {}
                
                # List of columns we care about (updated with Amount Mismatch)
                target_cols = [
                    "Verification Status", "Receipt Number", "Receipt Link", "Description", 
                    "Quantity", "Rate", "Amount", "Amount Mismatch", "Row_Id"
                ]
                
                for target in target_cols:
                    # Try to find it using the module-level helper
                    found = _find_col(df_amount, [target, target.lower(), target.replace(" ", ""), target.replace(" ", "_")])
                    if found:
                        actual_col_map[target] = found
                
                # Update FRIENDLY_COLUMN_MAP_AMOUNTS to point to ACTUAL columns
                # Only if we found the column, otherwise keep the default (which will fail gracefully later)
                
                FRIENDLY_COLUMN_MAP_AMOUNTS = {
                    "Verification_Status_Dropdown": "Status",
                    "Row_Index": "System Row Index",
                    "Receipt Number": "Receipt ID",
                    "Receipt Link": "View Receipt",
                    "Description": "Item Description",
                    "Quantity": "Quantity",
                    "Rate": "Unit Price",
                    "Amount": "Total Amount",
                    "Amount Mismatch": "Amount Mismatch",  # Renamed from Difference
                    "Row_Id": "Original Row ID"
                }

                # Construct final order based on available ACTUAL columns
                final_order = ["Verification_Status_Dropdown"]
                
                # Add other columns if they exist
                if "Receipt Number" in actual_col_map: final_order.append(actual_col_map["Receipt Number"])
                if "Receipt Link" in actual_col_map: final_order.append(actual_col_map["Receipt Link"])
                if "Description" in actual_col_map: final_order.append(actual_col_map["Description"])
                if "Quantity" in actual_col_map: final_order.append(actual_col_map["Quantity"])
                if "Rate" in actual_col_map: final_order.append(actual_col_map["Rate"])
                if "Amount" in actual_col_map: final_order.append(actual_col_map["Amount"])
                if "Amount Mismatch" in actual_col_map: final_order.append(actual_col_map["Amount Mismatch"])
                if "Row_Index" in display_df.columns: final_order.append("Row_Index")
                if "Row_Id" in actual_col_map: final_order.append(actual_col_map["Row_Id"])
                
                # Also update the Friendly Map keys to match actual columns
                # We need to map ACTUAL_COLUMN_NAME -> FRIENDLY_NAME
                # The st.data_editor uses the DataFrame's column names. 
                # So we must rename the DataFrame columns to match what we assume, OR update configuration to use actual names.
                # Easier: Rename DataFrame columns to Standard Names
                
                rename_map = {v: k for k, v in actual_col_map.items()} # Actual -> Standard
                display_df = display_df.rename(columns=rename_map)
                
                # Now display_df has "Standard" names like "Receipt Number"
                # So we can use the original final_order logic
                
                # Reorder columns to match new specification
                final_order_std = [
                    "Verification_Status_Dropdown", 
                    "Receipt Number",
                    "Amount Mismatch",
                    "Description", 
                    "Quantity",
                    "Rate", 
                    "Amount",
                    "Receipt Link",
                    "Row_Id"
                ]
                final_order_std = [col for col in final_order_std if col in display_df.columns]
                
                edited_df_amount = st.data_editor(
                    display_df[final_order_std].rename(columns=FRIENDLY_COLUMN_MAP_AMOUNTS),
                    column_order=[FRIENDLY_COLUMN_MAP_AMOUNTS[c] for c in final_order_std if c in FRIENDLY_COLUMN_MAP_AMOUNTS], # <--- ENFORCES COLUMN ORDER
                    column_config={
                        "Status": st.column_config.SelectboxColumn(
                            "Status",
                            options=status_options,
                            required=True,
                            default='Pending'
                        ),
                        "View Receipt": st.column_config.LinkColumn("View Receipt", display_text="View Receipt"),
                        "Quantity": st.column_config.NumberColumn("Quantity"),
                        "Unit Price": st.column_config.NumberColumn("Unit Price", format="%d"),
                        "Total Amount": st.column_config.NumberColumn("Total Amount", format="%d"),
                        "Amount Mismatch": st.column_config.NumberColumn("Amount Mismatch", disabled=True),
                        "System Row Index": st.column_config.TextColumn("System Row Index", disabled=True),
                        "Original Row ID": st.column_config.TextColumn("Original Row ID", disabled=True), 
                    },
                    hide_index=True,
                    key="editor_amount",
                    num_rows="dynamic"
                )
                
                edited_df_amount.rename(columns={v: k for k, v in FRIENDLY_COLUMN_MAP_AMOUNTS.items()}, inplace=True)
                edited_df_amount.rename(columns={'Verification_Status_Dropdown': 'Verification Status'}, inplace=True)
                
                if st.button("💾 Save Changes (Amount Mismatch)", type="primary"):
                    # OPTIMIZATION: Update both Sheets and session state (instant refresh)
                    if save_review_data(edited_df_amount, sheet_id, SHEET_VERIFY_AMOUNT, creds, session_key='df_amount'):
                        st.success("Saved successfully!")
                        st.rerun()

    # --- TAB 3: VERIFIED INVOICES ---
    with tab3:
        st.markdown("#### Verified Invoices")
        st.markdown("All verified invoices appear here. Use the filters below to search and edit specific records.")
        
        if sheet_id and creds:
            # OPTIMIZATION: Read from session state instead of API call (instant performance)
            df_verified = get_session_df('df_verified')
            
            if df_verified.empty:
                st.info("📋 No verified invoices yet. Upload and process invoices to get started.")
            else:
                # Reset index for clean display
                df_verified = df_verified.reset_index(drop=True)
                
                # --- FILTERING CONTROLS ---
                st.markdown("##### 🔍 Filters")
                
                # Initialize filter session state with defaults if not present
                if 'verified_filter_active' not in st.session_state:
                    st.session_state.verified_filter_active = {
                        'date_enabled': False,
                        'receipt': '',
                        'customer': '',
                        'vehicle': '',
                        'date_from': None,
                        'date_to': None
                    }
                
                # Use a form to prevent Enter key from triggering rerun
                with st.form(key="verified_filters_form"):
                    # First row: Date filter checkbox and other text filters
                    filter_row1_col1, filter_row1_col2, filter_row1_col3, filter_row1_col4 = st.columns([2, 1.5, 1.5, 1.5])
                    
                    with filter_row1_col1:
                        # Date range filter checkbox
                        date_filter_enabled = st.checkbox(
                            "Filter by Date Range", 
                            value=st.session_state.verified_filter_active['date_enabled'],
                            key="form_date_filter_checkbox"
                        )
                    
                    with filter_row1_col2:
                        filter_receipt = st.text_input(
                            "Receipt Number", 
                            value=st.session_state.verified_filter_active['receipt'],
                            placeholder="e.g., 001",
                            key="form_receipt_filter_input"
                        )
                    
                    with filter_row1_col3:
                        filter_customer = st.text_input(
                            "Customer Name", 
                            value=st.session_state.verified_filter_active['customer'],
                            placeholder="Search customer",
                            key="form_customer_filter_input"
                        )
                    
                    with filter_row1_col4:
                        filter_vehicle = st.text_input(
                            "Vehicle Number", 
                            value=st.session_state.verified_filter_active['vehicle'],
                            placeholder="e.g., MH12AB1234",
                            key="form_vehicle_filter_input"
                        )
                    
                    # Second row: Date inputs (only shown if checkbox is enabled)
                    if date_filter_enabled:
                        date_col1, date_col2, date_col_spacer = st.columns([2, 2, 3.5])
                        with date_col1:
                            filter_date_from = st.date_input(
                                "From Date", 
                                value=st.session_state.verified_filter_active['date_from'],
                                key="form_date_from_input"
                            )
                        with date_col2:
                            filter_date_to = st.date_input(
                                "To Date", 
                                value=st.session_state.verified_filter_active['date_to'],
                                key="form_date_to_input"
                            )
                    else:
                        filter_date_from = None
                        filter_date_to = None
                    
                    # Third row: Action buttons
                    btn_col1, btn_col2, btn_col_spacer = st.columns([1, 1, 5.5])
                    with btn_col1:
                        apply_clicked = st.form_submit_button("🔍 Apply Filters", type="primary", use_container_width=True)
                    with btn_col2:
                        clear_clicked = st.form_submit_button("Clear Filters", use_container_width=True)
                
                # Handle button clicks AFTER the form
                if apply_clicked:
                    # Store the filter values and rerun to stay on page with applied filters
                    st.session_state.verified_filter_active = {
                        'date_enabled': date_filter_enabled,
                        'receipt': filter_receipt,
                        'customer': filter_customer,
                        'vehicle': filter_vehicle,
                        'date_from': filter_date_from,
                        'date_to': filter_date_to
                    }
                    st.rerun()  # Rerun to apply filters and stay on same page
                
                if clear_clicked:
                    # Reset all filter values
                    st.session_state.verified_filter_active = {
                        'date_enabled': False,
                        'receipt': '',
                        'customer': '',
                        'vehicle': '',
                        'date_from': None,
                        'date_to': None
                    }
                    st.rerun()  # Rerun to clear the input boxes
                
                # Use the stored filter values for filtering
                active_filters = st.session_state.verified_filter_active

                
                # Apply filters
                filtered_df = df_verified.copy()
                
                # Date filter
                if active_filters['date_enabled'] and 'Date' in filtered_df.columns:
                    # Parse Date column to datetime for filtering
                    filtered_df['_date_parsed'] = pd.to_datetime(filtered_df['Date'], errors='coerce', dayfirst=True)
                    
                    if active_filters['date_from'] is not None:
                        filtered_df = filtered_df[filtered_df['_date_parsed'] >= pd.Timestamp(active_filters['date_from'])]
                    
                    if active_filters['date_to'] is not None:
                        filtered_df = filtered_df[filtered_df['_date_parsed'] <= pd.Timestamp(active_filters['date_to'])]
                    
                    filtered_df = filtered_df.drop(columns=['_date_parsed'], errors='ignore')
                
                # Receipt Number filter (partial match, case-insensitive)
                if active_filters['receipt'] and 'Receipt Number' in filtered_df.columns:
                    filtered_df = filtered_df[
                        filtered_df['Receipt Number'].astype(str).str.contains(active_filters['receipt'], case=False, na=False)
                    ]
                
                # Customer Number filter (partial match, case-insensitive)
                if active_filters['customer'] and 'Customer Name' in filtered_df.columns:
                    filtered_df = filtered_df[
                        filtered_df['Customer Name'].astype(str).str.contains(active_filters['customer'], case=False, na=False)
                    ]
                
                # Vehicle Number filter (partial match, case-insensitive)
                if active_filters['vehicle'] and 'Car Number' in filtered_df.columns:
                    filtered_df = filtered_df[
                        filtered_df['Car Number'].astype(str).str.contains(active_filters['vehicle'], case=False, na=False)
                    ]
                
                # Show filter results count
                if len(filtered_df) < len(df_verified):
                    st.caption(f"_Showing {len(filtered_df)} of {len(df_verified)} records_")
                else:
                    st.caption(f"_Showing all {len(df_verified)} records_")
                
                if filtered_df.empty:
                    st.warning("No records match the selected filters.")
                else:
                    # --- SORTING: Date in descending order ---
                    if 'Date' in filtered_df.columns:
                        # Parse dates for sorting
                        filtered_df['_date_sort'] = pd.to_datetime(filtered_df['Date'], errors='coerce', dayfirst=True)
                        filtered_df = filtered_df.sort_values('_date_sort', ascending=False, na_position='last')
                        filtered_df = filtered_df.drop(columns=['_date_sort'], errors='ignore')
                    
                    filtered_df = filtered_df.reset_index(drop=True)
                    
                    # --- SELECT ONLY SPECIFIED COLUMNS IN EXACT ORDER ---
                    display_columns = [
                        'Receipt Number', 'Date', 'Receipt Link', 'Customer Name', 
                        'Mobile Number', 'Car Number', 'Description', 'Labour/Part', 
                        'Quantity', 'Rate', 'Amount', 'Row_Id'
                    ]
                    
                    # Filter to only columns that exist in the dataframe
                    available_columns = [col for col in display_columns if col in filtered_df.columns]
                    
                    if not available_columns:
                        st.error("Required columns not found in Invoice Verified sheet.")
                    else:
                        display_df = filtered_df[available_columns].copy()
                        
                        # --- DATA TYPE CONVERSIONS ---
                        # Convert columns that should be text but might be stored as numbers
                        text_columns = ['Receipt Number', 'Mobile Number', 'Car Number', 'Description']
                        for col in text_columns:
                            if col in display_df.columns:
                                # Convert to string, removing .0 suffix from floats
                                display_df[col] = display_df[col].astype(str).str.replace(r'\.0$', '', regex=True)
                        
                        # --- DATA EDITOR CONFIGURATION ---
                        edited_df = st.data_editor(
                            display_df,
                            column_config={
                                "Receipt Number": st.column_config.TextColumn(
                                    "Receipt Number",
                                    help="Invoice receipt number",
                                    required=True
                                ),
                                "Date": st.column_config.TextColumn(
                                    "Date",
                                    help="Invoice date (DD-MM-YYYY or DD-MMM-YYYY format)"
                                ),
                                "Receipt Link": st.column_config.LinkColumn(
                                    "Receipt Link",
                                    display_text="View Receipt",
                                    disabled=True
                                ),
                                "Customer Name": st.column_config.TextColumn(
                                    "Customer Name",
                                    help="Name of the customer"
                                ),
                                "Mobile Number": st.column_config.TextColumn(
                                    "Mobile Number",
                                    help="Customer mobile number"
                                ),
                                "Car Number": st.column_config.TextColumn(
                                    "Car Number",
                                    help="Vehicle registration number"
                                ),
                                "Description": st.column_config.TextColumn(
                                    "Description",
                                    help="Item or service description"
                                ),
                                "Labour/Part": st.column_config.TextColumn(
                                    "Labour/Part",
                                    help="Type: Labour or Part"
                                ),
                                "Quantity": st.column_config.NumberColumn(
                                    "Quantity",
                                    help="Quantity of items",
                                    format="%d"
                                ),
                                "Rate": st.column_config.NumberColumn(
                                    "Rate",
                                    help="Unit price",
                                    format="%d"
                                ),
                                "Amount": st.column_config.NumberColumn(
                                    "Amount",
                                    help="Total amount for this line item",
                                    format="%d"
                                ),
                                "Row_Id": st.column_config.TextColumn(
                                    "Row_Id",
                                    help="Unique row identifier (read-only)",
                                    disabled=True
                                ),
                            },
                            hide_index=True,
                            key="editor_verified",
                            num_rows="dynamic"
                        )
                        
                        # --- SAVE BUTTON ---
                        if st.button("💾 Save Changes (Verified Invoices)", type="primary"):
                            # SMART MERGE: Update only the edited filtered rows in the full dataset
                            # Strategy: Use Row_Id as unique identifier to match rows
                            
                            try:
                                # Get the full unfiltered dataframe
                                full_df = get_session_df('df_verified').copy()
                                
                                if full_df.empty:
                                    st.error("No data to save.")
                                elif 'Row_Id' not in full_df.columns:
                                    st.error("Row_Id column not found. Cannot identify rows to update.")
                                else:
                                    # First, ensure filtered_df has Row_Id for matching
                                    if 'Row_Id' not in filtered_df.columns:
                                        st.error("Row_Id not available in filtered data. Cannot save.")
                                    else:
                                        # Fix for FutureWarning: Ensure columns in full_df are object type if we are going to write strings to them
                                        # This handles cases where columns like 'Receipt Number' were inferred as float/int but now have string edits
                                        text_cols_to_fix = ['Receipt Number', 'Mobile Number', 'Car Number', 'Description', 'Customer Name', 'Date', 'Labour/Part']
                                        for col in text_cols_to_fix:
                                            if col in full_df.columns:
                                                full_df[col] = full_df[col].astype('object')

                                        # Track which rows were updated
                                        updated_count = 0
                                        
                                        # We'll iterate through the filtered dataframe and update corresponding rows in full_df
                                        # Match using Row_Id (unique identifier for each row)
                                        
                                        # For each row in edited_df, find and update the corresponding row in full_df
                                        for idx in edited_df.index:
                                            # Get the Row_Id for this edited row (from filtered_df, not edited_df)
                                            row_id = filtered_df.loc[idx, 'Row_Id'] if idx < len(filtered_df) else None
                                            
                                            if pd.isna(row_id) or row_id == '':
                                                continue
                                            
                                            # Convert to string for matching
                                            row_id_str = str(row_id).strip()
                                            
                                            # Use Row_Id for matching
                                            match_mask = (full_df['Row_Id'].astype(str).str.strip() == row_id_str)
                                            
                                            # Update the matched row(s) with edited values
                                            if match_mask.any():
                                                # Get the row index in full_df
                                                full_df_idx = full_df[match_mask].index
                                                
                                                # Update each editable column
                                                for col in available_columns:
                                                    if col in edited_df.columns and col not in ['Receipt Link', 'Row_Id']:  # Don't update Receipt Link or Row_Id
                                                        # Get the edited value
                                                        edited_value = edited_df.loc[idx, col]
                                                        # Update in full_df
                                                        full_df.loc[full_df_idx, col] = edited_value
                                                
                                                updated_count += len(full_df_idx)
                                        
                                        # Show info about filter status
                                        if len(filtered_df) < len(full_df):
                                            st.info(f"ℹ️ Filters are active. Updating {updated_count} filtered record(s) out of {len(full_df)} total records.")
                                        
                                        # Save the full updated dataframe
                                        if save_review_data(full_df, sheet_id, "Invoice Verified", creds, session_key='df_verified'):
                                            st.success(f"✅ Successfully saved changes to {updated_count} record(s)!")
                                            st.rerun()
                                        
                            except Exception as e:
                                st.error(f"❌ Error saving changes: {e}")
                                import traceback
                                st.code(traceback.format_exc())

    # REMOVED the sync section from the bottom of the review page

def main():
    load_css()
    if not check_auth(): return

    user_conf = st.session_state.config
    creds = get_google_creds()
    storage = get_storage()
    
    # Sidebar navigation with workflow-based tabs
    with st.sidebar:
        st.title("🧾 Invoice Hub")
        st.markdown("---")
        
        # Workflow-based navigation
        options = ["Overview", "Step 1: Upload & Process", "Step 2: Verify & Analyze"]
        
        # Initialize navigation state if not present
        if "navigation_selection" not in st.session_state:
            st.session_state.navigation_selection = options[0]
        
        # Handle programmatic navigation (nav_target) by updating the session state key
        if "nav_target" in st.session_state:
            target = st.session_state.nav_target
            # Map old targets if necessary (keeping robust)
            nav_mapping = {
                "Dashboard": "Overview",
                "Upload Invoices": "Step 1: Upload & Process",
                "Run Extractor": "Step 1: Upload & Process",
                "Review & Sync": "Step 2: Verify & Analyze"
            }
            target = nav_mapping.get(target, target)
            
            if target in options:
                st.session_state.navigation_selection = target
            del st.session_state.nav_target

        # Use the key argument to bind directly to session state
        nav = st.radio("Menu", options, key="navigation_selection", label_visibility="collapsed")
        
        st.markdown("---")
        if st.button("Log Out"):
            st.session_state.clear()
            st.rerun()

    # --- PAGE: OVERVIEW ---
    if nav == "Overview":
        # Dynamic greeting based on time of day
        from datetime import datetime
        current_hour = datetime.now().hour
        
        if 5 <= current_hour < 12:
            greeting = "Good Morning"
        elif 12 <= current_hour < 17:
            greeting = "Good Afternoon"
        else:
            greeting = "Good Evening"
        
        st.markdown(f"### 👋 {greeting}, {st.session_state.user}!")
        
        # Dashboard URL from config
        dashboard_url = user_conf.get("dashboard_url")
        
        if dashboard_url:
            # Auto-fix: Convert standard View URL to Embed URL if needed
            # Standard: https://lookerstudio.google.com/reporting/UUID
            # Embed:    https://lookerstudio.google.com/embed/reporting/UUID
            if "lookerstudio.google.com" in dashboard_url and "/embed/" not in dashboard_url:
                dashboard_url = dashboard_url.replace("/reporting/", "/embed/reporting/")
            
            st.markdown(f"""
            <div class="dashboard-container">
                <iframe 
                    src="{dashboard_url}" 
                    width="100%" 
                    height="850" 
                    frameborder="0" 
                    style="border:0; border-radius: 8px;" 
                    allowfullscreen>
                </iframe>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="
                background-color: #f8f9fa; 
                height: 300px; 
                border-radius: 12px; 
                display: flex; 
                flex-direction: column;
                align-items: center; 
                justify-content: center; 
                color: #5f6368; 
                border: 2px dashed #dadce0;
                margin-top: 20px;">
                <div style="font-size: 40px; margin-bottom: 15px;">📊</div>
                <div style="font-size: 18px; font-weight: 500;">No Dashboard Configured</div>
                <div style="font-size: 14px; margin-top: 5px;">Add 'dashboard_url' to your secrets.toml to enable Looker Studio.</div>
            </div>
            """, unsafe_allow_html=True)

    # --- PAGE: UPLOAD & PROCESS (COMBINED) ---
    # --- PAGE: UPLOAD & PROCESS (COMBINED) ---
    elif nav == "Step 1: Upload & Process":
        st.markdown("### 📤 Upload Invoices")
        st.markdown("Select invoice files and click **'Process Invoices'** to extract data automatically.")
        
        # Center the simplified workflow
        c1, c2, c3 = st.columns([1, 2, 1])
        
        with c2:
            # Initialize session state for processing results
            if "processing_complete" not in st.session_state:
                st.session_state.processing_complete = False
            if "processing_stats" not in st.session_state:
                st.session_state.processing_stats = {}
            
            # File Uploader
            if "uploader_key" not in st.session_state: 
                st.session_state.uploader_key = 0
            
            uploaded = st.file_uploader(
                "Select invoice files (PNG, JPG, PDF)", 
                type=['png', 'jpg', 'jpeg', 'pdf'], 
                accept_multiple_files=True, 
                key=f"uploader_{st.session_state.uploader_key}"
            )
            
            # Show file selection info only if files are selected and processing not complete
            if uploaded and not st.session_state.processing_complete:
                st.info(f"📄 {len(uploaded)} file(s) selected")
                
                if st.button("⚡ Process Invoices", type="primary", use_container_width=True):
                    # 1. Upload Phase
                    progress_text = st.empty()
                    progress_bar = st.progress(0)
                    
                    bucket = user_conf.get("r2_bucket")
                    storage = get_storage()
                    
                    progress_text.text("Uploading files to secure storage...")
                    
                    success_count = 0
                    total_files = len(uploaded)
                    
                    def upload_one(f):
                        try:
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            key = f"uploads/{ts}_{f.name.replace(' ', '_')}"
                            f.seek(0)
                            storage.upload_fileobj(f, bucket, key)
                            return (True, None)
                        except Exception as e: 
                            return (False, str(e))

                    errors = []
                    with ThreadPoolExecutor() as exc:
                        futs = {exc.submit(upload_one, f): f for f in uploaded}
                        for i, fut in enumerate(as_completed(futs)):
                            success, error_msg = fut.result()
                            if success: 
                                success_count += 1
                            else:
                                errors.append(f"{futs[fut].name}: {error_msg}")
                            
                            progress_bar.progress((i+1) / len(uploaded) * 0.5)
                    
                    if success_count == 0:
                        st.error("Failed to upload files.")
                        if errors:
                            with st.expander("See error details"):
                                for err in errors:
                                    st.write(f"❌ {err}")
                        st.session_state.processing_complete = False
                    else:
                        # 2. Processing Phase
                        progress_text.text(f"Analyzing {success_count} invoices...")
                        
                        # Prepare Environment
                        env = os.environ.copy()
                        env["R2_BUCKET_NAME"] = user_conf.get("r2_bucket", "")
                        env["SHEET_ID"] = user_conf.get("sheet_id", "")
                        
                        from configs import get_r2_config, get_gcp_service_account, load_secrets
                        env['R2_CONFIG_JSON'] = json.dumps(get_r2_config())
                        env['GCP_SA_JSON'] = json.dumps(get_gcp_service_account())
                        
                        secs = load_secrets()
                        key = secs.get("general", {}).get("gemini_api_key") or secs.get("gemini_api_key")
                        env["GEMINI_API_KEY"] = key or ""

                        # Clear old progress
                        if os.path.exists("processing_progress.json"):
                            try: os.remove("processing_progress.json")
                            except: pass

                        # Run Subprocess
                        proc = subprocess.Popen(
                            [sys.executable, "invoice_processor_r2_streamlit.py"],
                            env=env
                        )
                        
                        # Poll Progress
                        while proc.poll() is None:
                            if os.path.exists("processing_progress.json"):
                                try:
                                    with open("processing_progress.json", 'r', encoding='utf-8') as f:
                                        data = json.load(f)
                                    
                                    total = data.get("total", 0)
                                    completed = data.get("completed", 0)
                                    
                                    if total > 0:
                                        pct = 0.5 + ((completed / total) * 0.5)
                                        progress_bar.progress(pct)
                                        progress_text.text(f"Processing invoice {completed} of {total}...")
                                except: 
                                    pass
                            time.sleep(0.5)
                         
                        proc.wait()
                        
                        # Read final stats
                        processed_count = 0
                        if os.path.exists("processing_progress.json"):
                            try:
                                with open("processing_progress.json", 'r', encoding='utf-8') as f:
                                    final_stats = json.load(f)
                                processed_count = final_stats.get("completed", 0)
                            except:
                                processed_count = success_count
                        
                        # Clean up progress file
                        try:
                            if os.path.exists("processing_progress.json"):
                                os.remove("processing_progress.json")
                        except:
                            pass
                        
                        # Clear progress indicators
                        progress_bar.empty()
                        progress_text.empty()
                        
                        # Store results in session state
                        st.session_state.processing_complete = True
                        st.session_state.processing_stats = {
                            "total_files": total_files,
                            "processed_count": processed_count
                        }
                        
                        # CRITICAL: Store current session ID to track "new" records
                        st.session_state.current_session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
                        
                        # Reset uploader to hide file list
                        st.session_state.uploader_key += 1
                        st.rerun()
            
            # Show success message and navigation button if processing is complete
            if st.session_state.processing_complete:
                stats = st.session_state.processing_stats
                st.success(f"✅ **{stats.get('total_files', 0)}** file(s) uploaded • **{stats.get('processed_count', 0)}** new invoice(s) processed successfully!")
                
                if st.button("🔍 Verify & Analyze Results", type="primary", use_container_width=True, key="nav_to_review"):
                    # Reset processing state
                    st.session_state.processing_complete = False
                    st.session_state.processing_stats = {}
                    # Navigate using nav_target (which gets processed before widget instantiation)
                    st.session_state.nav_target = "Step 2: Verify & Analyze"
                    st.rerun()

    
    # --- PAGE: REVIEW ---
    elif nav == "Step 2: Verify & Analyze":
        render_review_page(user_conf, creds)
                    
if __name__ == "__main__":
    main()