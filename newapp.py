import json
import requests
from datetime import datetime, timezone, time as dtime
from dateutil import parser
import streamlit as st
from dotenv import load_dotenv
import os
import pandas as pd  # <-- added
from typing import List

# Load .env if present
load_dotenv()

# ---------------------------
# Utility functions
# ---------------------------
def to_epoch_millis(dt: datetime) -> int:
    """Convert aware or naive datetime to epoch milliseconds (UTC).
    If naive, assume local time and convert to UTC.
    """
    if dt.tzinfo is None:
        # assume local time then convert to UTC
        # best-effort: treat naive as local system time
        local_ts = dt.astimezone()
        utc_ts = local_ts.astimezone(timezone.utc)
    else:
        utc_ts = dt.astimezone(timezone.utc)
    return int(utc_ts.timestamp() * 1000)

def combine_date_time(date_obj, time_obj):
    """Combine date (datetime.date) and time (datetime.time) into naive datetime."""
    return datetime(
        year=date_obj.year,
        month=date_obj.month,
        day=date_obj.day,
        hour=time_obj.hour,
        minute=time_obj.minute,
        second=time_obj.second if hasattr(time_obj, "second") else 0,
    )

def safe_request_post(url, params, data, headers, verify_ssl=True):
    try:
        r = requests.post(url, params=params, data=data, headers=headers, verify=verify_ssl, timeout=30)
        return r.status_code, r.text, r.json() if 'application/json' in r.headers.get('Content-Type','') else None
    except requests.exceptions.RequestException as e:
        return None, str(e), None
    except ValueError:
        # JSON decode error
        return r.status_code if 'r' in locals() else None, r.text if 'r' in locals() else str(e), None

def safe_request_get(url, params, headers, verify_ssl=True):
    try:
        r = requests.get(url, params=params, headers=headers, verify=verify_ssl, timeout=30)
        return r.status_code, r.text, r.json() if 'application/json' in r.headers.get('Content-Type','') else None
    except requests.exceptions.RequestException as e:
        return None, str(e), None
    except ValueError:
        return r.status_code if 'r' in locals() else None, r.text if 'r' in locals() else str(e), None

# ---------------------------
# JSON -> DataFrame helpers
# ---------------------------
def find_first_list(obj):
    """
    Recursively find the first list in a JSON-like structure that looks like a list of records.
    Returns the list or None.
    """
    if obj is None:
        return None
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        # prefer common keys that APIs use for records
        preferred_keys = ["requests", "request", "data", "result", "results", "records", "response"]
        for k in preferred_keys:
            if k in obj and isinstance(obj[k], list):
                return obj[k]
        # otherwise search recursively
        for v in obj.values():
            found = find_first_list(v)
            if found is not None:
                return found
    return None

def json_to_dataframe(obj):
    """
    Convert JSON (dict / list) to pandas DataFrame.
    - If a list of dicts is found, normalize it.
    - If single dict, put it into a one-row dataframe.
    """
    if obj is None:
        return pd.DataFrame()
    # If top-level is a list
    if isinstance(obj, list):
        # ensure list items are dict-like
        try:
            return pd.json_normalize(obj, sep='.')
        except Exception:
            return pd.DataFrame(obj)
    # If top-level is dict, try to find inner list of records
    if isinstance(obj, dict):
        lst = find_first_list(obj)
        if lst is not None:
            try:
                return pd.json_normalize(lst, sep='.')
            except Exception:
                return pd.DataFrame(lst)
        else:
            # single object -> one-row DataFrame
            try:
                return pd.json_normalize(obj, sep='.')
            except Exception:
                return pd.DataFrame([obj])
    # fallback
    return pd.DataFrame([obj])

# ---------------------------
# Cleaning / column selection helpers
# ---------------------------
# --- UPDATED create columns (only this list changed per your request) ---
PREFERRED_CREATE_COLUMNS = [
    "id",
    "subject",
    "description",
    "requester.id",
    "requester.name",
    "status.name",
    "site.name",
    "account.name",
    "created_time.display_value"
]
# --- view columns left unchanged ---
PREFERRED_VIEW_COLUMNS = [
    "id",
    "subject",
    "requester.id",
    "requester.name",
    "status.name",
    "priority.name",           # <-- added
    "technician.name",         # <-- added (assign to)
    "assigned_to.name",        # <-- alternative assign to
    "group.name",              # <-- added
    "site.name",
    "account.name",
    "created_time",
    # "created_time.value",
    "created_time.display_value",
    "due_by_time.display_value"  # <-- added (due by date)
]


def pick_preferred_columns(df: pd.DataFrame, preferred: List[str]) -> pd.DataFrame:
    """
    Return df with only the preferred columns that exist.
    If none of the preferred columns are present, return original df (so nothing is lost).
    """
    if df is None or df.empty:
        return df
    present = [c for c in preferred if c in df.columns]
    if present:
        return df[present].copy()
    else:
        return df.copy()

def interactive_table(df: pd.DataFrame, default_columns: List[str], title: str = "Results"):
    """
    Show a clean table with only the preferred columns, no multiselect UI.
    """
    if df is None or df.empty:
        st.info("No tableable data to display.")
        return

    st.subheader(title)

    # auto-select preferred columns that exist
    preferred_present = [c for c in default_columns if c in df.columns]

    if preferred_present:
        df_to_show = df[preferred_present]
    else:
        # fallback: show full df
        df_to_show = df

    # Show clean table (NO TABS)
    st.dataframe(df_to_show)

    # CSV download
    csv = df_to_show.to_csv(index=False)
    st.download_button("Download CSV", csv, file_name=f"{title.replace(' ', '_').lower()}.csv", mime="text/csv")

# ---------------------------
# App config / defaults
# ---------------------------
st.set_page_config(page_title="ManageEngine Chat UI", layout="wide")

st.title("ManageEngine — Simple Streamlit UI Chatbot")
st.write("Use 'create request' or 'view request' (buttons provided). Fill settings in the sidebar.")

# Sidebar for configuration
st.sidebar.header("Configuration")
DEFAULT_URL = os.getenv("MG_URL", "https://srvdesk.wavecorp.in/api/v3/requests")
DEFAULT_TOKEN = os.getenv("AUTHTOKEN", "5B6F41C9-12C0-4AE0-8AB1-8052FFBE5ABF")
DEFAULT_TECH_KEY = os.getenv("TECHNICIAN_KEY", "5B6F41C9-12C0-4AE0-8AB1-8052FFBE5ABF")

api_url = st.sidebar.text_input("API endpoint (requests)", DEFAULT_URL)
authtoken = st.sidebar.text_input("Authtoken (header)", DEFAULT_TOKEN)
technician_key = st.sidebar.text_input("TECHNICIAN_KEY (params)", DEFAULT_TECH_KEY)
verify_ssl = st.sidebar.checkbox("Verify SSL certificates", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown("**Security note:** don't store real secrets in code or in public places. Use a `.env` or secret manager for production.")

# Simple chat-like command input
st.subheader("Command / Chat")
col1, col2 = st.columns([3,1])
with col1:
    user_cmd = st.text_input("Type a command (e.g. 'create request' or 'view request')", "")
with col2:
    create_btn = st.button("Create Request")
    view_btn = st.button("View Request")

# Helper: normalize user's command
cmd = user_cmd.strip().lower()

# ---------------------------
# Create Request flow
# ---------------------------
def create_request_flow():
    st.markdown("### Create Request")
    with st.form("create_form"):
        subject = st.text_input("Subject", value="TEST")
        description = st.text_area("Description", value="This is for a testing purpose")
        requester_id = st.text_input("Requester ID", value="103803")
        requester_name = st.text_input("Requester Name", value="IBM TEST USER")
        resolution_content = st.text_input("Resolution content", value="TEST REQUEST")
        site_name = st.text_input("Site name", value="None7")
        site_id = st.text_input("Site id", value="42")
        account_name = st.text_input("Account name", value="Wave Infratech")
        account_id = st.text_input("Account id", value="3")
        status_name = st.text_input("Status name", value="Open")
        submitted = st.form_submit_button("Submit Create Request")
    if submitted:
        payload = {
            "request": {
                "subject": subject,
                "description": description,
                "requester": {
                    "id": str(requester_id),
                    "name": requester_name
                },
                "resolution": {
                    "content": resolution_content
                },
                "site": {
                    "name": site_name,
                    "id": str(site_id)
                },
                "account": {
                    "name": account_name,
                    "id": str(account_id)
                },
                "status": {
                    "name": status_name
                }
            }
        }
        params = {"TECHNICIAN_KEY": technician_key} if technician_key else {}
        data = {"input_data": json.dumps(payload)}
        headers = {"authtoken": authtoken, "Content-Type": "application/x-www-form-urlencoded"}
        status, text, j = safe_request_post(api_url, params=params, data=data, headers=headers, verify_ssl=verify_ssl)
        if status is None:
            st.error(f"Request error: {text}")
            return
        st.write(f"HTTP {status}")

        # parse response
        try:
            parsed = j if j is not None else json.loads(text)
        except Exception:
            parsed = None

        if not parsed:
            st.text(text)
            return

        # Convert parsed JSON to DataFrame
        df = pd.DataFrame()
        if isinstance(parsed, dict):
            # Prefer top-level 'request' dict if present
            if "request" in parsed and isinstance(parsed["request"], dict):
                df = pd.json_normalize(parsed["request"], sep='.')
            else:
                # check other keys
                for key in ("requests", "data", "result", "results", "records", "response"):
                    if key in parsed:
                        val = parsed[key]
                        if isinstance(val, list):
                            df = pd.json_normalize(val, sep='.')
                            break
                        if isinstance(val, dict):
                            df = pd.json_normalize(val, sep='.')
                            break
                # fallback to whole dict as single-row
                if df.empty:
                    try:
                        df = pd.json_normalize(parsed, sep='.')
                    except Exception:
                        df = pd.DataFrame([parsed])
        elif isinstance(parsed, list):
            try:
                df = pd.json_normalize(parsed, sep='.')
            except Exception:
                df = pd.DataFrame(parsed)

        # Clean / pick preferred columns for create responses
        df_clean = pick_preferred_columns(df, PREFERRED_CREATE_COLUMNS)

        if df_clean is None or df_clean.empty:
            # if cleaning removed everything or nothing table-like, show raw JSON for debugging
            st.json(parsed)
            return

        # Show clean table (no multiselect)
        interactive_table(df_clean, PREFERRED_CREATE_COLUMNS, title="Create Response")

# ---------------------------
# View Request flow
# ---------------------------
def view_request_flow():
    st.markdown("### View Requests (filter by created_time range)")
    with st.form("view_form"):
        col_a, col_b = st.columns(2)
        with col_a:
            start_date = st.date_input("Start date")
            start_time = st.time_input("Start time", value=dtime(hour=0, minute=0))
        with col_b:
            end_date = st.date_input("End date")
            end_time = st.time_input("End time", value=dtime(hour=23, minute=59, second=59))
        # Option for entering free text datetimes
        st.markdown("Or enter start/end as free text (ISO or common formats). If provided, parsed values take precedence.")
        start_text = st.text_input("Start datetime (optional, e.g. '2025-11-04 14:30')", "")
        end_text = st.text_input("End datetime (optional)", "")
        include_tech_key = st.checkbox("Include TECHNICIAN_KEY as param (some setups require it)", value=False)
        submitted = st.form_submit_button("Fetch Requests")
    if submitted:
        # parse start/end
        if start_text.strip():
            try:
                start_dt = parser.parse(start_text)
            except Exception as e:
                st.error(f"Could not parse start datetime: {e}")
                return
        else:
            start_dt = combine_date_time(start_date, start_time)
        if end_text.strip():
            try:
                end_dt = parser.parse(end_text)
            except Exception as e:
                st.error(f"Could not parse end datetime: {e}")
                return
        else:
            end_dt = combine_date_time(end_date, end_time)

        start_epoch_ms = to_epoch_millis(start_dt)
        end_epoch_ms = to_epoch_millis(end_dt)

        input_data = {
            "list_info": {
                "start_index": 1,
                "search_criteria": [
                    {
                        "condition": "greater or equal",
                        "field": "created_time",
                        "logical_operator": "and",
                        "value": str(start_epoch_ms)
                    },
                    {
                        "condition": "lesser than",
                        "field": "created_time",
                        "logical_operator": "and",
                        "value": str(end_epoch_ms)
                    }
                ]
            }
        }

        params = {"input_data": json.dumps(input_data)}
        if include_tech_key and technician_key:
            params["TECHNICIAN_KEY"] = technician_key

        headers = {"authtoken": authtoken}
        status, text, j = safe_request_get(api_url, params=params, headers=headers, verify_ssl=verify_ssl)
        if status is None:
            st.error(f"Request error: {text}")
        else:
            try:
                parsed = j if j is not None else json.loads(text)
            except Exception:
                parsed = None

            if parsed:
                # Convert response JSON to DataFrame
                df = json_to_dataframe(parsed)
                if df is None or df.empty:
                    st.json(parsed)
                    return

                # Clean / pick preferred columns for view responses
                df_clean = pick_preferred_columns(df, PREFERRED_VIEW_COLUMNS)

                # Show clean table (no multiselect)
                interactive_table(df_clean, PREFERRED_VIEW_COLUMNS, title="Requests Export")
            else:
                st.text(text)

# Decide which flow to show
if create_btn or cmd.startswith("create"):
    create_request_flow()
elif view_btn or cmd.startswith("view"):
    view_request_flow()
else:
    st.info("Type a command or press one of the buttons. Examples: 'create request' or 'view request'.")
    st.markdown("**Quick demo:** click *Create Request* or *View Request* above.")

# # Footer with small tips
# st.markdown("---")
# st.write("Tips:")
# st.write("- This app sends `input_data` as a JSON string (url-encoded form field for POST; as a URL param for GET) — matching the examples you provided.")
# st.write("- `Authtoken` is sent in header `authtoken`. `TECHNICIAN_KEY` is sent as param named `TECHNICIAN_KEY` for POST. Adjust if your API requires different names.")
# st.write("- For production: don't keep tokens in the code. Use environment variables or a secrets manager.")
