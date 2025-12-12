# File: app_streamlit.py
import streamlit as st
import pandas as pd
import gspread
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
    from configs import get_r2_config, get_users_db, get_gcp_service_account
except ImportError:
    st.error("Missing configuration file.")
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
    
    /* Cards */
    .metric-card {
        background: white; border-radius: 12px; padding: 24px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05); border: 1px solid #eaeaea;
        margin-bottom: 15px; transition: transform 0.2s;
    }
    .metric-card:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(0,0,0,0.1); }
    .metric-title { color: #5f6368; font-size: 0.9rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
    .metric-value { color: #1a73e8; font-size: 2.2rem; font-weight: 700; margin: 10px 0 Liquidity; }
    .metric-desc { color: #9aa0a6; font-size: 0.85rem; }
    
    /* Buttons */
    div.stButton > button {
        border-radius: 8px; font-weight: 600; padding: 0.5rem 1rem;
        border: none; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    div.stButton > button:hover { opacity: 0.9; }
    
    /* Header */
    h3 { color: #202124; font-weight: 700; }
    </style>
    """, unsafe_allow_html=True)

def ui_card(title, value, desc, link_text, link_url):
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-desc">{desc}</div>
        <div style="margin-top: 10px;"><a href="{link_url}" target="_blank" style="color: #1a73e8; text-decoration: none;">{link_text}</a></div>
    </div>
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
def get_google_creds():
    sa = get_gcp_service_account()
    if not sa: return None
    return service_account.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/spreadsheets'])

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

def get_sheet_df(sheet_id, tab_name, creds):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        data = sh.worksheet(tab_name).get_all_records()
        return pd.DataFrame(data)
    except: return pd.DataFrame()

# ---------------------
# MAIN APP
# ---------------------
def main():
    load_css()
    if not check_auth(): return

    user_conf = st.session_state.config
    creds = get_google_creds()
    storage = get_storage()
    
    # Sidebar
    with st.sidebar:
        st.title("📂 Invoice Hub")
        st.caption(f"User: {st.session_state.user}")
        st.markdown("---")
        
        options = ["Overview", "Upload", "Process"]
        
        if "nav_target" in st.session_state:
            idx = options.index(st.session_state.nav_target)
            del st.session_state.nav_target
        else:
            idx = 0
            
        nav = st.radio("Menu", options, index=idx, label_visibility="collapsed")
        
        st.markdown("---")
        if st.button("Log Out"):
            st.session_state.clear()
            st.rerun()

    # --- PAGE: OVERVIEW ---
    if nav == "Overview":
        st.markdown("### 📊 Dashboard Overview")
        st.markdown("View data and summary statistics below. The sheet name in each card is a direct hyperlink to the Google Sheet.")
        
        total_invoices, total_rows_all = 0, 0
        verify_dates_invoices, verify_dates_rows = 0, 0
        verify_amount_invoices, verify_amount_rows = 0, 0
        
        # New Sheet Names
        SHEET_VERIFY_DATES = "Verify Dates and Receipt Numbers"
        SHEET_VERIFY_AMOUNT = "Verify Amount"
        
        if creds and user_conf.get("sheet_id"):
            # 1. Main Data
            df_all = get_sheet_df(user_conf["sheet_id"], "Invoice All", creds)
            if not df_all.empty:
                total_invoices = df_all["Receipt Number"].nunique() if "Receipt Number" in df_all.columns else 0
                total_rows_all = len(df_all)
            
            # 2. Verify Dates
            df_dates = get_sheet_df(user_conf["sheet_id"], SHEET_VERIFY_DATES, creds)
            if not df_dates.empty:
                verify_dates_invoices = df_dates["Receipt Number"].nunique() if "Receipt Number" in df_dates.columns else 0
                verify_dates_rows = len(df_dates)
            
            # 3. Verify Amount
            df_amount = get_sheet_df(user_conf["sheet_id"], SHEET_VERIFY_AMOUNT, creds)
            if not df_amount.empty:
                verify_amount_invoices = df_amount["Receipt Number"].nunique() if "Receipt Number" in df_amount.columns else 0
                verify_amount_rows = len(df_amount)

        c1, c2, c3 = st.columns(3)
        sheet_id = user_conf.get("sheet_id", "")
        def get_gid(name):
            try:
                gc = gspread.authorize(creds)
                sh = gc.open_by_key(sheet_id)
                return sh.worksheet(name).id
            except: return "0"
        
        url_base = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid="
        
        with c1:
            gid_all = get_gid("Invoice All")
            ui_card(
                "TOTAL INVOICES",
                f"{total_invoices}",
                f"{total_invoices} Invoices | {total_rows_all} Total Rows",
                "View Invoice All",
                f"{url_base}{gid_all}"
            )
        
        with c2:
            gid_dates = get_gid(SHEET_VERIFY_DATES)
            ui_card(
                "Review Date and Receipt Numbers",
                f"{verify_dates_rows}",
                f"Pending Verification",
                f"View {SHEET_VERIFY_DATES}",
                f"{url_base}{gid_dates}"
            )
        
        with c3:
            gid_amount = get_gid(SHEET_VERIFY_AMOUNT)
            ui_card(
                "AMOUNT MISMATCH",
                f"{verify_amount_rows}",
                f"Pending Verification",
                f"View {SHEET_VERIFY_AMOUNT}",
                f"{url_base}{gid_amount}"
            )

        st.markdown("### Analytics Dashboard")
        current_date = datetime.now().strftime("%B %d, %Y")
        st.markdown(f"Live insights from your processed invoice data. Last updated: {current_date}")
        
        st.markdown("""
        <div style="background-color: #f0f0f0; height: 200px; border-radius: 8px; display: flex; align-items: center; justify-content: center; color: #9aa0a6;">
            Dashboard visualizations will appear here (e.g., charts from Pandas DataFrames).
        </div>
        """, unsafe_allow_html=True)

    # --- PAGE: UPLOAD ---
    elif nav == "Upload":
        st.markdown("### ☁️ Upload Documents")
        st.markdown("Select invoice images to add to the processing queue.")
        
        uploaded = st.file_uploader("Select Files", type=['png', 'jpg', 'jpeg', 'pdf'], accept_multiple_files=True)
        
        if uploaded and st.button(f"Upload {len(uploaded)} Files", type="primary"):
            progress = st.progress(0)
            status = st.empty()
            bucket = user_conf.get("r2_bucket")
            
            def upload_one(f):
                try:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    key = f"uploads/{ts}_{f.name.replace(' ', '_')}"
                    f.seek(0)
                    storage.upload_fileobj(f, bucket, key)
                    return True
                except: return False

            success = 0
            with ThreadPoolExecutor() as exc:
                futs = [exc.submit(upload_one, f) for f in uploaded]
                for i, f in enumerate(as_completed(futs)):
                    if f.result(): success += 1
                    progress.progress((i+1)/len(uploaded))
            
            status.success(f"✅ Uploaded {success} files successfully.")
            
        if st.checkbox("Show files in storage"):
            try:
                objs = storage.list_objects_v2(Bucket=user_conf.get("r2_bucket"), Prefix="uploads/")
                files = [o['Key'].split('/')[-1] for o in objs.get('Contents', [])]
                st.write(files)
            except: st.error("Could not fetch file list.")
    # --- PAGE: PROCESS ---
    elif nav == "Process":
        st.markdown("### ⚙️ Extraction Engine")
        st.markdown("Run the extraction engine to process uploaded images and populate the database.")
        
        c1, c2 = st.columns([2, 1])
        with c1:
            if st.button("🚀 Start Extraction", type="primary", use_container_width=True):
                # Clear any old progress
                if os.path.exists("processing_progress.json"):
                    try: os.remove("processing_progress.json")
                    except: pass
                
                env = os.environ.copy()
                env["R2_BUCKET_NAME"] = user_conf.get("r2_bucket", "")
                env["SHEET_ID"] = user_conf.get("sheet_id", "")
                
                from configs import get_r2_config, get_gcp_service_account, load_secrets
                env['R2_CONFIG_JSON'] = json.dumps(get_r2_config())
                env['GCP_SA_JSON'] = json.dumps(get_gcp_service_account())
                
                secs = load_secrets()
                key = secs.get("general", {}).get("gemini_api_key") or secs.get("gemini_api_key")
                env["GEMINI_API_KEY"] = key or ""

                # Start the processor (no log capture)
                proc = subprocess.Popen(
                    [sys.executable, "invoice_processor_r2_streamlit.py"],
                    env=env
                )
                
                # Clean UI placeholders
                progress_placeholder = st.empty()
                status_placeholder = st.empty()
                
                # Poll progress file only
                while proc.poll() is None:
                    if os.path.exists("processing_progress.json"):
                        try:
                            with open("processing_progress.json", 'r', encoding='utf-8') as f:
                                progress_data = json.load(f)
                            
                            total = progress_data.get("total", 0)
                            completed = progress_data.get("completed", 0)
                            current = progress_data.get("current_file", "Initializing...")
                            status = progress_data.get("status", "running")
                            
                            if total > 0:
                                prog_val = completed / total
                                remaining = total - completed
                                progress_text = f"Processed: {completed}/{total} | Remaining: {remaining} | Current: {current}"
                                
                                with progress_placeholder.container():
                                    st.progress(prog_val, text=f"{int(prog_val * 100)}% Complete")
                                    st.caption(progress_text)
                            else:
                                with progress_placeholder.container():
                                    st.caption("Initializing extraction engine...")
                        except:
                            with progress_placeholder.container():
                                st.caption("Starting processor...")
                    else:
                        with progress_placeholder.container():
                            st.caption("Launching extraction engine...")
                    
                    time.sleep(0.8)  # Smooth updates
                
                # Cleanup
                if os.path.exists("processing_progress.json"):
                    try: os.remove("processing_progress.json")
                    except: pass
                
                if proc.returncode == 0:
                    st.success("Extraction Complete!")
                    st.session_state.nav_target = "Overview"
                    st.rerun()
                else:
                    st.error("Extraction encountered errors.")
                    
if __name__ == "__main__":
    main()