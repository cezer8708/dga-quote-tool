import os
import io
import uuid
import json
from datetime import datetime
import requests
import re
import sys
from typing import Any
import pytz
import html.parser
import base64  # <-- NEW: For base64 encoding the PDF for the iframe preview

import pandas as pd
import streamlit as st
import gspread

# =============================================================================
# 0. Configuration and Environment
# =============================================================================
# --- CHANGE: Changed 'centered' to 'wide' as requested ---
st.set_page_config(page_title="DGA Quoting Tool", layout="wide")

from dotenv import load_dotenv
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
)

# Load environment variables from .env file (for local development)
# This MUST be called early in the script.
load_dotenv()


def get_env(key, default=None, cast=str):
    """
    Helper to safely retrieve environment variables with casting.
    Checks os.environ (local) first, then st.secrets (Streamlit Cloud).
    """
    # 1. Check Streamlit Secrets (for deployment)
    if key in st.secrets:
        val = st.secrets[key]
    # 2. Check os.environ (for local development via load_dotenv)
    else:
        val = os.getenv(key, default)

    # Casting logic
    try:
        return cast(val) if val is not None else default
    except Exception:
        return default


COMPANY = {
    "name": get_env("COMPANY_NAME", "Disc Golf Association, Inc."),
    "tagline": get_env("COMPANY_TAGLINE", "FIRST IN DISC GOLF"),
    "phone": get_env("COMPANY_PHONE", "(831) 722-6037"),
    "fax": get_env("COMPANY_FAX", "(831) 722-8176"),
    "web": get_env("COMPANY_WEB", "www.discgolf.com"),
    "addr1": get_env("COMPANY_ADDR_1", "73 Hangar Way"),
    "city": get_env("COMPANY_ADDR_CITY", "Watsonville"),
    "state": get_env("COMPANY_ADDR_STATE", "CA"),
    "zip": get_env("COMPANY_ADDR_ZIP", "95076"),
}
DEFAULT_TAX = float(get_env("SALES_TAX_RATE_DEFAULT", 0.0, float))
SANTA_CRUZ_TAX_RATE = 0.0975
# COMPANY_LOGO_PATH = get_env("COMPANY_LOGO_PATH", "assets/dga_logo.png") # Original line

# --- Pipedrive configuration retrieval FIX ---
# We retrieve the domain from the environment (using your .env name)
PIPEDRIVE_DOMAIN = get_env("PIPEDRIVE_API_URL")

# We define the token using the name defined in your .env
PIPEDRIVE_API_TOKEN = get_env("PIPEDRIVE_API_TOKEN")

# The correct API Base URL structure includes the domain and the /v1 endpoint
if PIPEDRIVE_DOMAIN:
    # Ensure the domain doesn't end with a slash before appending /v1
    PIPEDRIVE_BASE_URL = PIPEDRIVE_DOMAIN.rstrip('/') + "/v1"
else:
    PIPEDRIVE_BASE_URL = None  # Will be None if env variable is missing

# --- GOOGLE SHEETS CONFIGURATION ---
GOOGLE_SHEET_ID = "1oR2I5lmxYNhAc4rT1kalzVwop2UJOnGjTkY3eTVzv80"


# -----------------------------------

# --- FIX IMPLEMENTATION START ---
@st.cache_resource(ttl=None)  # Cache the result as this won't change at runtime
def _get_logo_path_robustly(default_path: str = "assets/dga_logo.png") -> str | None:
    """
    Checks for common logo paths/casings, necessary for Streamlit Cloud (Linux)
    which is case-sensitive, unlike many local development environments (Windows/macOS).
    """
    logo_path_base = get_env("COMPANY_LOGO_PATH", default_path)

    # 1. Check the path as provided/defaulted
    if os.path.exists(logo_path_base):
        return logo_path_base

    # 2. Check common casing variations (e.g., if assets/dga_logo.png was committed as Assets/DGA_Logo.png)
    dirname, basename = os.path.split(logo_path_base)

    # Common variations to check
    variations = [
        os.path.join(dirname.capitalize(), basename.capitalize()),  # e.g., Assets/DGA_Logo.png
        os.path.join(dirname.lower(), basename.capitalize()),  # e.g., assets/DGA_Logo.png
        os.path.join(dirname.capitalize(), basename.lower()),  # e.g., Assets/dga_logo.png
    ]

    for path in variations:
        if os.path.exists(path):
            # Print to stderr for deployment debugging
            print(f"Found logo at case-adjusted path: {path}", file=sys.stderr)
            return path

    # 3. Final check: if the provided path is relative, check the root directory as well
    if dirname == 'assets':
        root_path = basename
        if os.path.exists(root_path):
            return root_path

    # If all checks fail, return None
    print(f"Logo not found at expected path: {logo_path_base} or common variations.", file=sys.stderr)
    return None


COMPANY_LOGO_PATH = _get_logo_path_robustly()


# --- FIX IMPLEMENTATION END ---


def fmt_money(value: float) -> str:
    """Formats a float as a currency string, e.g., 1234.56 -> $1,234.56"""
    return f"${value:,.2f}"


# =============================================================================
# 1. Google Sheets Connection and Data Handling
# =============================================================================
@st.cache_resource(ttl=3600)
def get_gsheet_client():
    """
    Authenticates with Google Sheets.
    Priority:
    1. Local 'service_account.json' file (verified via test_sheet.py).
    2. Streamlit Secrets 'gcp_service_account' (for cloud deployment).
    """
    try:
        # 1. LOCAL PRIORITY: Use the file verified with test_sheet.py
        if os.path.exists("service_account.json"):
            return gspread.service_account(filename="service_account.json")

        # 2. CLOUD FALLBACK: Use Streamlit Secrets if local file is missing
        if "gcp_service_account" in st.secrets:
            creds_data = st.secrets["gcp_service_account"]

            # Handle both Dictionary (SecretDict) and JSON String formats
            if isinstance(creds_data, str):
                try:
                    sa_creds = json.loads(creds_data)
                except json.JSONDecodeError:
                    st.error("Secret format error: gcp_service_account is a string but not valid JSON.")
                    return None
            else:
                # Coerce Streamlit SecretDict to a standard dictionary
                sa_creds = dict(creds_data)

            # Normalize private key newlines (The "Invalid private key" fix)
            if "private_key" in sa_creds and isinstance(sa_creds["private_key"], str):
                sa_creds["private_key"] = sa_creds["private_key"].replace("\\n", "\n")

            return gspread.service_account_from_dict(sa_creds)

        st.error("No Google Sheets credentials found. Please add 'service_account.json' or update st.secrets.")
        return None

    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None


@st.cache_data(ttl=300)
def load_all_quotes() -> pd.DataFrame:
    """Loads all quote data from the Google Sheet for lookup."""
    client = get_gsheet_client()
    if not client:
        return pd.DataFrame()

    try:
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.get_worksheet(0)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        if 'Quote #' not in df.columns or 'Quote JSON Payload' not in df.columns:
            st.error("Google Sheet missing required columns: 'Quote #' and 'Quote JSON Payload'.")
            return pd.DataFrame()

        # Convert the JSON string column back to actual dicts
        df['Payload'] = df['Quote JSON Payload'].apply(lambda x: json.loads(x) if x else None)
        return df.dropna(subset=['Payload'])

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found. Check ID and sharing.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading quotes from sheet: {e}")
        return pd.DataFrame()


def save_quote_to_gsheet(payload: dict) -> bool:
    """Saves a new quote or order to the Google Sheet."""
    client = get_gsheet_client()
    if not client:
        return False

    try:
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.get_worksheet(0)

        # Use order doc number if available, otherwise use quote number
        doc_number = payload.get("order_meta", {}).get("order_doc_number") or payload.get("quote_no")

        row_data = [
            doc_number,
            payload.get("date"),
            payload.get("customer", {}).get("company", ""),
            payload.get("customer", {}).get("name", ""),
            payload.get("customer", {}).get("email", ""),
            payload.get("totals", {}).get("grand_total", 0.0),
            json.dumps(payload),  # Full payload saved as JSON string
        ]

        worksheet.append_row(row_data, value_input_option='USER_ENTERED')
        load_all_quotes.clear() # Reset cache so the new row appears immediately
        return True
    except Exception as e:
        st.error(f"Error saving quote to sheet: {e}")
        return False


# =============================================================================
# 2+3. Data: Local Product DB + Session State Initialization
# =============================================================================
import pandas as pd
import pytz
from datetime import datetime
import re
import streamlit as st

DEFAULT_TAX = 0.08  # Example default tax rate

# ----------------------------------------
# Load Products CSV
# ----------------------------------------
@st.cache_data
def load_products(path: str = "products.csv") -> pd.DataFrame:
    """
    Local catalog used for quoting.
    Required columns: SKU, Name, UnitPrice
    Optional column: Notes (Autofilled into line items)
    """
    try:
        df = pd.read_csv(path)
        # Strip whitespace from column headers
        df.columns = [c.strip() for c in df.columns]

        # Verify required columns
        for col in ["SKU", "Name", "UnitPrice"]:
            if col not in df.columns:
                raise ValueError(f"products.csv must have column: {col}")

        # Strip whitespace from string columns
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = df[col].str.strip()

        # Ensure Notes column exists and is cleaned
        if "Notes" not in df.columns:
            df["Notes"] = ""
        else:
            df["Notes"] = df["Notes"].fillna("").astype(str)

        # Ensure numeric conversion
        df["UnitPrice"] = pd.to_numeric(
            df["UnitPrice"].astype(str).str.replace(r"[^0-9.\-]", "", regex=True),
            errors="coerce"
        ).fillna(0.0)

        return df
    except FileNotFoundError:
        st.warning(f"Product file not found at '{path}'. Using minimal placeholder data.")
        return pd.DataFrame({
            "SKU": ["M5-ST", "M7-PT", "M14-CO", "TS-BASIC"],
            "Name": ["Mach 5 Standard Basket", "Mach 7 Portable Basket", "Mach 14 Chain Collar",
                     "Basic Color Tee Sign"],
            "UnitPrice": [499.00, 399.00, 35.00, 55.00],
            "Notes": ["", "", "", ""]
        })

PRODUCTS = load_products()


# ----------------------------------------
# Pacific Time Helper
# ----------------------------------------
def get_pacific_now():
    """Returns current datetime localized to America/Los_Angeles."""
    pacific_tz = pytz.timezone('America/Los_Angeles')
    return datetime.now(pacific_tz)


# ----------------------------------------
# Quote Number Helpers
# ----------------------------------------
def new_quote_number():
    """Generates a new quote number using the current time in Pacific Time."""
    return get_pacific_now().strftime("%m%d-%H%M")


def assign_new_quote_version():
    """Increments the version number of the current quote."""
    current_quote_no = st.session_state["quote_no"]
    match = re.match(r'(.+?)(?:-V(\d+))?$', current_quote_no)
    base, version = match.groups() if match else (current_quote_no, None)
    current_version = int(version) if version is not None else 1
    new_version = current_version + 1
    st.session_state["quote_no"] = f"{base}-V{new_version}"
    st.rerun()


def start_new_quote():
    """Resets session state to start a new quote."""
    # Customer info
    st.session_state["customer"] = {
        "company": "", "name": "", "email": "", "phone": "",
        "ship_addr1": "", "ship_city": "", "ship_state": "", "ship_zip": "",
        "bill_company": "", "bill_name": "", "bill_email": "", "bill_phone": "",
        "bill_addr1": "", "bill_city": "", "bill_state": "", "bill_zip": "",
    }

    st.session_state["line_items"] = []
    st.session_state["drop_fee_input"] = 0.0
    st.session_state["freight_fee_input"] = 0.0
    st.session_state["freight_notes"] = ""
    st.session_state["tax_rate_pct_input"] = 0.0
    st.session_state["sc_county_checkbox"] = False
    st.session_state["footer_notes"] = (
        "Pricing subject to change. Please review all details carefully.\n"
        "International customers will be responsible for all duties and taxes upon delivery."
    )

    # Order/PO info
    st.session_state["order_doc_number_pdf"] = ""
    st.session_state["order_po_number"] = ""
    st.session_state["order_operator"] = "CZ"
    st.session_state["order_terms"] = "NET 30"
    st.session_state["order_comm_to"] = ""
    st.session_state["order_check_number"] = ""
    st.session_state["order_date_received"] = get_pacific_now().strftime('%m/%d/%y')

    # Quote numbering
    st.session_state["quote_no"] = new_quote_number()
    st.session_state["customer_key_suffix"] += 1

    # Pipedrive session fields
    st.session_state["pd_matches"] = []
    st.session_state["pd_term"] = ""
    st.session_state["pd_expander_state"] = False
    st.session_state["show_pdf_preview"] = True
    st.rerun()


# ----------------------------------------
# Initialize Session State Defaults
# ----------------------------------------
if "customer" not in st.session_state:
    st.session_state["customer"] = {}

if "line_items" not in st.session_state:
    st.session_state["line_items"] = []

st.session_state.setdefault("rerun_flag", False)
st.session_state.setdefault("customer_key_suffix", 0)
st.session_state.setdefault("quote_no", new_quote_number())
st.session_state.setdefault("footer_notes", (
    "Pricing subject to change. Please review all details carefully.\n"
    "International customers will be responsible for all duties and taxes upon delivery."
))
st.session_state.setdefault("drop_fee_input", 0.0)
st.session_state.setdefault("freight_fee_input", 0.0)
st.session_state.setdefault("tax_rate_pct_input", 0.0)  # start with 0% tax
st.session_state.setdefault("sc_county_checkbox", False)
st.session_state.setdefault("freight_notes", "")
st.session_state.setdefault("order_doc_number_pdf", "")
st.session_state.setdefault("order_po_number", "")
st.session_state.setdefault("order_operator", "CZ")
st.session_state.setdefault("order_terms", "NET 30")
st.session_state.setdefault("order_comm_to", "")
st.session_state.setdefault("order_check_number", "")
st.session_state.setdefault("order_date_received", get_pacific_now().strftime('%m/%d/%y'))
st.session_state.setdefault("pd_matches", [])
st.session_state.setdefault("pd_expander_state", False)
st.session_state.setdefault("show_pdf_preview", True)



# =============================================================================
# 4. Pipedrive Helpers
# =============================================================================

def _pd_get(endpoint: str, params: dict | None = None) -> dict | None:
    """Helper for Pipedrive API calls."""
    if not PIPEDRIVE_API_TOKEN or not PIPEDRIVE_BASE_URL:
        # Avoid crashing if the API token or URL is missing
        print("Pipedrive API Token or Base URL is missing.", file=sys.stderr)
        return None
    # FIX: Corrected the typo in the variable name
    url = f"{PIPEDRIVE_BASE_URL}/{endpoint}"
    _params = {"api_token": PIPEDRIVE_API_TOKEN, "limit": 5, **(params or {})}
    try:
        response = requests.get(url, params=_params, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data["data"] if data and data.get("success") else []
    except Exception as e:
        # NOTE: Logging to stderr as stdout is captured by Streamlit
        print(f"Pipedrive API Error at {endpoint}: {e}", file=sys.stderr)
        return []


def _pd_scalar(data: Any) -> Any | None:
    """Safely extracts the scalar value from a Pipedrive object/ID or list of values."""
    if isinstance(data, dict):
        return data.get("value")
    # Handle Pipedrive lists like emails/phones: takes the first one
    if isinstance(data, list) and data:
        first_item = data[0]
        return first_item.get("value") if isinstance(first_item, dict) else first_item
    return data


def pd_search_persons(term: str) -> list[dict]:
    """Searches Pipedrive persons by term (name or email)."""
    results = _pd_get("persons/search", {"term": term, "fields": "name,email", "search_by_email": 1})
    if results and isinstance(results, dict) and "items" in results:
        # Simplified extraction from the search API response format
        return [
            {
                "id": item["item"]["id"],
                "name": item["item"]["name"],
                "email": item["item"]["emails"][0] if item["item"]["emails"] else "",
            } for item in results["items"]
        ]
    return []


def pd_get_person(id: str | int) -> dict | None:
    """Fetches a single person record."""
    data = _pd_get(f"persons/{id}")  # Corrected to the general GET endpoint
    return data if isinstance(data, dict) else None


def pd_get_org(id: str | int) -> dict | None:
    """Fetches a single organization record."""
    data = _pd_get(f"organizations/{id}")
    return data if isinstance(data, dict) else None


def _clean(value: Any) -> str:
    """Converts a value to a string and cleans up newlines/extra spaces."""
    if value is None:
        return ""
    if isinstance(value, list):
        value = ", ".join([str(v) for v in value])
    return str(value).strip()


class _ATagTextExtractor(html.parser.HTMLParser):
    """A minimal parser to extract text from the first <a> tag it finds."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_a_tag = False
        self.data = ""
        self.found = False

    def handle_starttag(self, tag, attrs):
        if tag == 'a' and not self.found:
            self.in_a_tag = True

    def handle_endtag(self, tag):
        if tag == 'a' and self.in_a_tag:
            self.in_a_tag = False
            self.found = True  # Stop processing after the first <a> tag closes

    def handle_data(self, data):
        if self.in_a_tag:
            self.data += data.strip()  # Accumulate text within the <a> tag


def _extract_text_from_a_tag(html_string: str) -> str:
    """Helper to parse the Pipedrive HTML links using the custom class."""
    if not html_string or "<a" not in html_string.lower():
        return ""
    parser = _ATagTextExtractor()
    try:
        parser.feed(html_string)
        parser.close()
        return parser.data
    except Exception:
        # Ignore any parsing errors and return nothing if it fails
        return ""


def _extract_address_from_html(raw_input: Any) -> str:
    """
    Robustly extracts the address from Pipedrive, handling:
    1. A raw string that is actually a JSON object containing address details (most common fix).
    2. A raw string containing HTML (with optional <a> tag).
    3. A simple string.
    """

    # Ensure raw_input is a string for cleaning/parsing checks
    if raw_input is None:
        return ""

    html_string = _clean(raw_input)

    # --- 1. JSON Parsing Check (For complex address objects) ---
    if html_string.startswith("{") and html_string.endswith("}"):
        try:
            addr_obj = json.loads(html_string)
            # The most common Pipedrive key for the full address string is 'formatted_address'
            if isinstance(addr_obj, dict) and addr_obj.get("formatted_address"):
                return _clean(addr_obj["formatted_address"])
            # Fallback for address field which sometimes just contains the street in a key like 'label'
            return _clean(addr_obj.get("label", ""))
        except json.JSONDecodeError:
            pass  # Not a valid JSON string, proceed to HTML/string parsing

    # --- 2. HTML Parsing Check (For Pipedrive's internal <a> tag formatting) ---
    # This now correctly calls the defined helper function:
    clean_addr = _extract_text_from_a_tag(html_string)
    if clean_addr:
        return _clean(clean_addr)

    # --- 3. Simple String Fallback ---
    return html_string


def _get_address_from_components(entity: dict, addr_type: str) -> str:
    """
    Constructs a full address string from individual Pipedrive address components
    if the main formatted address field is empty.
    addr_type should be 'address' (for person) or 'org_address' (for organization)
    """
    parts = []

    # 1. Street / Line 1
    # Pipedrive's 'address' object contains sub-keys like 'street_number', 'route', 'sublocality'
    # Fallback to the individual address component fields which are often populated:
    street_parts = []
    # Check common Pipedrive component fields for street information
    for key in ['street_number', 'route', 'sublocality', 'address_line_1']:
        if entity.get(f"{addr_type}_{key}"):
            street_parts.append(_clean(entity[f"{addr_type}_{key}"]))

    # If standard components are missing, check the original top-level address fields
    if not street_parts and entity.get(f"{addr_type}_street"):
        street_parts.append(_clean(entity[f"{addr_type}_street"]))

    if street_parts:
        parts.append(" ".join(street_parts))

    # 2. City
    if entity.get(f"{addr_type}_locality"):
        parts.append(_clean(entity[f"{addr_type}_locality"]))
    elif entity.get(f"{addr_type}_city"):
        parts.append(_clean(entity[f"{addr_type}_city"]))

    # 3. State and Postal Code
    state_zip = []

    # State/Region (admin_area_level_1)
    state = None
    if entity.get(f"{addr_type}_admin_area_level_1"):
        state = _clean(entity[f"{addr_type}_admin_area_level_1"])
    elif entity.get(f"{addr_type}_state"):
        state = _clean(entity[f"{addr_type}_state"])

    # Zip/Postal Code
    zip_code = None
    if entity.get(f"{addr_type}_postal_code"):
        zip_code = _clean(entity[f"{addr_type}_postal_code"])
    elif entity.get(f"{addr_type}_zip"):
        zip_code = _clean(entity[f"{addr_type}_zip"])

    # Combine State and Zip if they exist, then add to parts
    if state and zip_code:
        parts.append(f"{state} {zip_code}")
    elif state:
        parts.append(state)
    elif zip_code:
        parts.append(zip_code)

    # 4. Country (optional, but helpful for parsing)
    if entity.get(f"{addr_type}_country_code"):
        parts.append(_clean(entity[f"{addr_type}_country_code"]))

    return ", ".join(parts)


def _parse_us_address(full_addr: str) -> tuple[str, str, str, str]:
    """
    Robust parser that breaks a full address string (e.g., Line1, City, State, Zip, Country)
    into its components using reliable comma-splitting.
    """
    full_addr = full_addr.strip()
    if not full_addr:
        return "", "", "", ""

    # Example: '102 North Broadway Street, Lewistown, IL 61477, USA'
    parts = [p.strip() for p in full_addr.split(',') if p.strip()]

    street = ""
    city = ""
    state = ""
    zip_code = ""

    if len(parts) >= 1:
        street = parts[0]

    if len(parts) >= 2:
        city = parts[1]

    if len(parts) >= 3:
        # The third part usually contains State and Zip
        state_zip_part = parts[2].upper()

        # Simple split by space to get State and Zip
        sz_parts = [p.strip() for p in state_zip_part.split() if p.strip()]

        for part in sz_parts:
            # 2-letter state code
            if len(part) == 2 and part.isalpha() and not state:
                state = part
            # 5-digit or longer numeric string as the zip
            elif part.isdigit() and len(part) >= 5 and not zip_code:
                zip_code = part

            # Stop if both are found
            if state and zip_code:
                break

        # Final cleanup/fallback for state/zip if they are in the third part but not space-separated
        if not state and len(state_zip_part) == 2 and state_zip_part.isalpha():
            state = state_zip_part
        if not zip_code and len(state_zip_part) >= 5 and state_zip_part.isdigit():
            zip_code = state_zip_part

    return _clean(street), _clean(city), _clean(state), _clean(zip_code)


def pd_person_to_customer(person: dict, org: dict | None = None) -> dict:
    """
    Maps Pipedrive Person and Organization data to the internal customer dict,
    now with robust fallbacks for blank formatted address fields.
    """

    # --- 1. CORE CONTACT FIELDS (from Person object) ---
    name = _clean(person.get("name"))
    email = _clean(_pd_scalar(person.get("email")))
    phone = _clean(_pd_scalar(person.get("phone")))

    # --- 2. ORGANIZATION/COMPANY FIELDS (from Org object) ---
    company = _clean((org or {}).get("name") or "")
    bill_company = company
    bill_name = name
    bill_phone = phone
    bill_email = email

    if org:
        org_email = _clean(_pd_scalar(org.get("email")))
        org_phone = _clean(_pd_scalar(org.get("phone")))
        bill_email = org_email or bill_email
        bill_phone = org_phone or bill_phone

    # --- 3. ADDRESS FIELDS (ROBUST EXTRACTION) ---
    p_addr_formatted = _clean(person.get("address_formatted_address") or person.get("address"))
    o_addr_formatted = _clean((org or {}).get("address_formatted_address") or (org or {}).get("address"))

    # 1. Try formatted address and clean the raw input (NEW: handles JSON and HTML)
    p_addr_full = _extract_address_from_html(p_addr_formatted)
    o_addr_full = _extract_address_from_html(o_addr_formatted)

    # 2. If still empty, construct from components (Fallback)
    if not p_addr_full:
        p_addr_full = _get_address_from_components(person, 'address')
    if not o_addr_full and org:
        o_addr_full = _get_address_from_components(org, 'org_address')

    p_street, p_city, p_state, p_zip = _parse_us_address(p_addr_full)
    o_street, o_city, o_state, o_zip = _parse_us_address(o_addr_full)

    # --- START: SHIPPING ADDRESS LOGIC FIX (Prioritize Person's Address) ---

    # Check if the Person address has any meaningful component
    if p_street or p_city or p_state or p_zip:
        ship_addr1 = p_street
        ship_city = p_city
        ship_state = p_state
        ship_zip = p_zip
    else:
        # Fallback to Organization address if Person's address is completely empty
        ship_addr1 = o_street
        ship_city = o_city
        ship_state = o_state
        ship_zip = o_zip
    # --- END: SHIPPING ADDRESS LOGIC FIX ---

    # BILLING ADDRESS LOGIC (Prioritize Organization's Address)
    if org and (o_addr_full or o_street or o_city or o_state or o_zip):  # Added component check
        bill_addr1 = o_street
        bill_city = o_city
        bill_state = o_state
        bill_zip = o_zip
    else:
        # Fallback to shipping address if no separate organization address exists
        bill_addr1 = ship_addr1
        bill_city = ship_city
        bill_state = ship_state
        bill_zip = ship_zip

    return {
        # SHIPPING/CONTACT INFO
        "company": company,
        "name": name,
        "email": email,
        "phone": phone,
        "ship_addr1": ship_addr1, "ship_city": ship_city, "ship_state": ship_state, "ship_zip": ship_zip,
        # BILLING INFO
        "bill_company": bill_company,
        "bill_name": bill_name,
        "bill_email": bill_email,
        "bill_phone": bill_phone,
        "bill_addr1": bill_addr1, "bill_city": bill_city, "bill_state": bill_state, "bill_zip": bill_zip,
    }


# --- Course Discount helpers ---
ALLOW_COURSE_SKUS = {"M5CO", "M7CO", "MXCO"}


def is_basket_5_7_X(item: dict) -> bool:
    sku = (item.get("sku") or "").upper().strip()
    name = (item.get("name") or "").lower()

    if sku in ALLOW_COURSE_SKUS:
        return True

    name_ok = (("mach 5" in name) or ("mach 7" in name) or ("mach x" in name)) \
              and any(k in name for k in ["standard", "portable", "no frills"])
    if name_ok:
        return True

    if sku.startswith(("M5", "M7", "MX")) and not sku.endswith("CO"):
        bad_keywords = ["COLLAR", "CHAIN", "HOLDER", "WRAP"]
        if any(bad in sku for bad in bad_keywords):
            return False
        return True

    return False


def eligible_qty_for_discount(items: list[dict]) -> int:
    # Only calculate the qty based on non-discount items
    return int(sum((float(it.get("qty", 0)) for it in items if is_basket_5_7_X(it) and it.get("sku") != "CD")))


def find_course_discount_index(items: list[dict]) -> int:
    for idx, it in enumerate(items):
        if (it.get("sku") == "CD") or (it.get("name", "").lower().strip() == "course discount"):
            return idx
    return -1


def ensure_course_discount(items: list[dict]) -> bool:
    """
    Checks for and adds/updates/removes the Course Discount.
    Returns True if the line_items list was modified, False otherwise.
    """
    qty = eligible_qty_for_discount(items)
    idx = find_course_discount_index(items)

    modified = False
    DISCOUNT_NOTE = "Auto-applied for 9+ Mach 5/7/X baskets"

    if qty >= 9:
        # NOTE: Total is calculated in the loop, but stored here for state consistency
        disc_line = {
            "id": items[idx]["id"] if idx != -1 and "id" in items[idx] else str(uuid.uuid4()),
            "sku": "CD",
            "name": "Course Discount (-$100 per qualifying basket)",
            "qty": qty,
            "unit": -100.0,
            "total": round(-100.0 * qty, 2),
            "notes": DISCOUNT_NOTE,
            "prev_sku": "CD",
            "previewChecked": True,  # Ensure discount is always in preview
        }

        if idx == -1:
            # Add the discount item
            items.append(disc_line)
            modified = True
        elif items[idx]["qty"] != disc_line["qty"] or items[idx]["total"] != disc_line["total"]:
            # Update the discount item if qty or total changed
            items[idx] = disc_line
            modified = True

        # FIX: Ensure the discount is at the end immediately after adding/updating
        if modified:
            ensure_course_discount_stays_last(items)
            # Do NOT force a rerun here, let the caller (the on_change callback) do it
            # st.session_state["rerun_flag"] = True

    elif idx != -1:
        # Remove the discount item
        items.pop(idx)
        modified = True
        # Do NOT force a rerun here, let the caller (the on_change callback) do it
        # st.session_state["rerun_flag"] = True

    return modified  # New return value


def ensure_course_discount_stays_last(items: list[dict] = None):
    """Ensures the Course Discount line item is the very last element in the list."""
    if items is None:
        items = st.session_state["line_items"]

    idx = find_course_discount_index(items)
    if idx != -1 and idx != len(items) - 1:
        # Move the discount item to the end
        discount_item = items.pop(idx)
        items.append(discount_item)


# --- PDF Builder Functions ---
def _company_right_block(styles):
    return Paragraph(
        f"<b>Disc Golf Association (DGA)</b><br/>"
        f"73 Hangar Way<br/>"
        f"Watsonville, CA 95076<br/>"
        f"Phone: {COMPANY['phone']}", styles['LeftInfo']
    )


def build_pdf(buffer: io.BytesIO, customer: dict, items: list, fees: dict, totals: dict,
              doc_number: str, footer_notes_text: str, template: str = "quote",
              meta: dict | None = None):
    meta = meta or {}
    CONTENT_WIDTH = 7.5 * inch
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=36, leftMargin=36, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle('CenterTitle', parent=styles['Title'], alignment=TA_CENTER))
    styles.add(ParagraphStyle('LeftInfo', parent=styles['Normal'], fontSize=10, leading=12, alignment=TA_LEFT))
    styles.add(
        ParagraphStyle('QuoteHeaderTitle', parent=styles['Heading2'], alignment=TA_RIGHT, fontSize=14, leading=16))

    story = []

    notes_style = ParagraphStyle(
        "LineNote",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        textColor=colors.grey,
        leftIndent=6
    )
    notes_style_2 = ParagraphStyle(
        "LineNote2",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        textColor=colors.black
    )
    addr_style = ParagraphStyle('AddrStyle', parent=styles['Normal'], fontSize=10, leading=12)

    # ==== TEMPLATE: ORDER ====
    if template == "order":
        # --- FIX: Use the robustly determined COMPANY_LOGO_PATH ---
        # REMOVED: global COMPANY_LOGO_PATH (not needed for read)
        if COMPANY_LOGO_PATH:  # Checks if the path was successfully found
            logo = Image(COMPANY_LOGO_PATH, width=1.8 * inch, height=1.0 * inch)
            logo.hAlign = 'LEFT'
            company_info_block = _company_right_block(styles)
            left_logo_block = [logo, Spacer(1, 4), company_info_block]

            hdr = Table([[left_logo_block, ""]], colWidths=[3.75 * inch, 3.75 * inch])
            hdr.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('ALIGN', (0, 0), (0, 0), 'LEFT')
            ]))
            hdr.hAlign = 'LEFT'
            story += [hdr, Spacer(1, 4)]
        else:
            story += [Paragraph(f"<b>{COMPANY['name']}</b><br/><i>{COMPANY['tagline']}</i>", styles['Title']),
                      Spacer(1, 4)]
        # --- END FIX ---

        # Display only the Order Document # (doc_number)
        story += [
            Paragraph(f"**ORDER: {doc_number}**", styles['Heading2']),
            Spacer(1, 4)
        ]

        # --- PT Date for consistency ---
        grouped_info_text = (
            f"Date: {get_pacific_now().strftime('%m/%d/%y')}<br/>"
            f"Operator: {meta.get('operator', '')}<br/>"
            f"Commission to: {meta.get('commission_to', '')}"
        )
        grouped_para = Paragraph(grouped_info_text, styles['LeftInfo'])

        info_tbl = Table([[grouped_para, ""]], colWidths=[CONTENT_WIDTH / 2, CONTENT_WIDTH / 2])
        info_tbl.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ]))
        info_tbl.hAlign = 'LEFT'
        story += [info_tbl, Spacer(1, 4)]

        ship_block_order = (
            f"<b>Shipping Address</b><br/>"
            f"{customer.get('company', '')}<br/>"
            f"{customer.get('name', '')}<br/>"
            f"{customer.get('ship_addr1', '')}<br/>"
            f"{customer.get('ship_city', '')}, {customer.get('ship_state', '')} {customer.get('ship_zip', '')}<br/>"
            f"{customer.get('phone', '')}<br/>"
            f"{customer.get('email', '')}<br/><br/>"
            f"<b>Purchase Order & Check Info:</b><br/>"
            f"P.O. Number: {meta.get('po_number', '')}<br/>"
            f"Terms: {meta.get('terms', '')}<br/>"
            f"Check Number: {meta.get('check_number', '')}<br/>"
            f"Date Received: {meta.get('date_received', '')}"
        )

        # FIX: Use bill_company, bill_name, bill_phone, bill_email
        bill_block_order = (
            f"<b>Billing Address</b><br/>"
            f"{customer.get('bill_company', customer.get('company', ''))}<br/>"
            f"{customer.get('bill_name', customer.get('name', ''))}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('bill_phone', customer.get('phone', ''))}<br/>"
            f"{customer.get('bill_email', customer.get('email', ''))}"
        )

        addr_data = [
            [
                Paragraph(ship_block_order, addr_style),
                Paragraph(bill_block_order, addr_style)
            ]
        ]

        addr_table = Table(addr_data, colWidths=[CONTENT_WIDTH / 2, CONTENT_WIDTH / 2])
        addr_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('TOPPADDING', (0, 0), (-1, -1), 0),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ]))
        addr_table.hAlign = 'LEFT'
        story += [addr_table, Spacer(1, 6)]

        header = ["Quantity", "Product Description", "Unit Price", "Total"]
        li_cols = [0.7 * inch, 5.15 * inch, 0.825 * inch, 0.825 * inch]
        data = [header]
        # Iterate over the items list passed to the function, not the session state directly
        for r in items:
            # Only include items that have a quantity AND are marked for preview (if the field exists)
            is_checked = r.get("previewChecked", True)  # Default to True for compatibility
            if float(r.get("qty", 0)) == 0 or not is_checked:
                continue

            desc_para = Paragraph(str(r["name"]),
                                  ParagraphStyle('Desc', parent=styles['Normal'], fontSize=9, leading=11))
            data.append([str(r["qty"]), desc_para,
                         fmt_money(float(r['unit'])) if float(r['unit']) >= 0 else fmt_money(float(r['unit'])),
                         fmt_money(float(r['total']))])
            note_txt = (r.get("Notes") or r.get("notes") or "").strip()

            if note_txt:
                data.append(["", Paragraph(note_txt, notes_style), "", ""])

        t_li = Table(data, colWidths=li_cols, repeatRows=1)
        t_li.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.75, colors.black),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (2, 1), (3, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        t_li.hAlign = 'LEFT'
        story += [t_li]

        freight_notes_txt = meta.get("freight_notes", "").strip()  # Use meta/payload if available, else session
        if not freight_notes_txt and st.session_state.get("freight_notes"):
            freight_notes_txt = st.session_state["freight_notes"].strip()

        if freight_notes_txt:
            story += [Spacer(1, 4),
                      Paragraph(f"<b>Freight Notes:</b> {freight_notes_txt}", notes_style_2)]

        story += [Spacer(1, 8)]

        sub_tbl_w = 2.5 * inch
        t_sub = Table([
            ["Subtotal:", fmt_money(totals.get('subtotal', 0.0))],
            ["Drop-Ship Fee:", fmt_money(fees.get('drop_ship_fee', 0.0))],
            [f"Sales Tax ({totals.get('tax_rate_pct', 0.0) * 100:.2f}%):", fmt_money(totals.get('sales_tax', 0.0))],
        ], colWidths=[sub_tbl_w * 0.6, sub_tbl_w * 0.4])
        t_sub.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))

        grand_tbl_w = 2.5 * inch
        t_grand = Table([
            ["Freight:", fmt_money(fees.get('freight', 0.0))],
            ["**GRAND TOTAL:**", f"**{fmt_money(totals.get('grand_total', 0.0))}**"],
        ], colWidths=[grand_tbl_w * 0.6, grand_tbl_w * 0.4])
        t_grand.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))

        v_totals_table = Table([[t_sub], [t_grand]], colWidths=[sub_tbl_w])
        v_totals_table.setStyle(TableStyle([
            ('LEFTPADDING', (0, 0), (-1, -1), 0), ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('TOPPADDING', (0, 0), (-1, -1), 0), ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
        ]))

        final_wrapper = Table([["", v_totals_table]], colWidths=[CONTENT_WIDTH - sub_tbl_w, sub_tbl_w])
        final_wrapper.setStyle(TableStyle([
            ('LEFTPADDING', (0, 0), (-1, -1), 0), ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('TOPPADDING', (0, 0), (-1, -1), 0), ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT')
        ]))
        final_wrapper.hAlign = 'LEFT'
        story += [final_wrapper]
    # ==== TEMPLATE: QUOTE ====
    else:

        company_info_text = (
            f"<b>Disc Golf Association, Inc.</b><br/>"
            f"{COMPANY['addr1']}<br/>"
            f"{COMPANY['city']}, {COMPANY['state']} {COMPANY['zip']}"
        )
        company_info_para = Paragraph(company_info_text, styles['Normal'])

        # --- FIX: Use the robustly determined COMPANY_LOGO_PATH ---
        # REMOVED: global COMPANY_LOGO_PATH (not needed for read)
        if COMPANY_LOGO_PATH:  # Checks if the path was successfully found
            logo = Image(COMPANY_LOGO_PATH, width=1.8 * inch, height=1.0 * inch)
            logo.hAlign = 'LEFT'
            left_logo_block_elements = [logo, Spacer(1, 4), company_info_para]
        else:
            left_logo_block_elements = [company_info_para]
        # --- END FIX ---

        left_logo_block = Table([[elem] for elem in left_logo_block_elements], colWidths=[3.75 * inch])
        left_logo_block.setStyle(TableStyle([
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))

        right_align_style = ParagraphStyle('RightAlignStyle', parent=styles['Normal'], fontSize=10, leading=12,
                                           alignment=TA_RIGHT)
        title_text = "Quotation Form<br/>Pricing Subject to Change"
        title_para = Paragraph(title_text, styles['QuoteHeaderTitle'])
        contact_info_text = (
            f"Phone: {COMPANY['phone']}<br/>"
            f"Fax: {COMPANY['fax']}<br/>"
            f"Web: {COMPANY['web']}"
        )
        contact_info_para = Paragraph(contact_info_text, right_align_style)

        right_title_block_elements = [
            title_para,
            Spacer(1, 40),
            contact_info_para
        ]

        right_title_block = Table([[elem] for elem in right_title_block_elements], colWidths=[3.75 * inch])
        right_title_block.setStyle(TableStyle([
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ]))

        header_data = [
            [
                left_logo_block,
                right_title_block
            ]
        ]
        t = Table(header_data, colWidths=[3.75 * inch, 3.75 * inch])
        t.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ]))
        t.hAlign = 'LEFT'
        story += [t, Spacer(1, 12)]

        # --- PT Date for consistency ---
        date_quote_info = (
            f"Date: {get_pacific_now().strftime('%Y-%m-%d')}<br/>"
            f"Quote #: {doc_number}"
        )
        date_quote_para = Paragraph(date_quote_info, styles['LeftInfo'])

        t = Table([[date_quote_para]], colWidths=[CONTENT_WIDTH])
        t.setStyle(TableStyle([('LEFTPADDING', (0, 0), (-1, -1), 0)]))
        t.hAlign = 'LEFT'
        story += [t, Spacer(1, 8)]

        ship_block = (
            f"<b>Shipping Address</b><br/>"
            f"{customer.get('company', '')}<br/>"
            f"{customer.get('name', '')}<br/>"
            f"{customer.get('ship_addr1', '')}<br/>"
            f"{customer.get('ship_city', '')}, {customer.get('ship_state', '')} {customer.get('ship_zip', '')}<br/>"
            f"{customer.get('phone', '')}<br/>"
            f"{customer.get('email', '')}"
        )

        # FIX: Use bill_company, bill_name, bill_phone, bill_email
        bill_block = (
            f"<b>Billing Address</b><br/>"
            f"{customer.get('bill_company', customer.get('company', ''))}<br/>"
            f"{customer.get('bill_name', customer.get('name', ''))}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('bill_phone', customer.get('phone', ''))}<br/>"
            f"{customer.get('bill_email', customer.get('email', ''))}"
        )

        t = Table([
            [Paragraph(ship_block, addr_style), Paragraph(bill_block, addr_style)]
        ], colWidths=[CONTENT_WIDTH / 2, CONTENT_WIDTH / 2])

        t.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ]))
        t.hAlign = 'LEFT'
        story += [t, Spacer(1, 12)]

        header = ["Qty", "Product Description", "Unit Price", "Total"]
        li_cols = [0.7 * inch, 4.3 * inch, 1.25 * inch, 1.25 * inch]
        data = [header]
        # Iterate over the items list passed to the function, not the session state directly
        for r in items:
            # Only include items that have a quantity AND are marked for preview (if the field exists)
            is_checked = r.get("previewChecked", True)  # Default to True for compatibility
            if float(r.get("qty", 0)) == 0 or not is_checked: continue

            desc_para = Paragraph(str(r["name"]),
                                  ParagraphStyle('Desc', parent=styles['Normal'], fontSize=9, leading=11))
            data.append([str(r["qty"]), desc_para,
                         fmt_money(float(r['unit'])) if float(r['unit']) >= 0 else fmt_money(float(r['unit'])),
                         fmt_money(float(r['total']))])
            note_txt = (r.get("Notes") or r.get("notes") or "").strip()

            if note_txt:
                data.append(["", Paragraph(note_txt, notes_style), "", ""])

        t_li = Table(data, colWidths=li_cols, repeatRows=1)
        t_li.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (2, 1), (3, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))
        t_li.hAlign = 'LEFT'
        story += [t_li, Spacer(1, 12)]

        freight_notes_txt = meta.get("freight_notes", "").strip()  # Use meta/payload if available, else session
        if not freight_notes_txt and st.session_state.get("freight_notes"):
            freight_notes_txt = st.session_state["freight_notes"].strip()

        if freight_notes_txt:
            story += [Spacer(1, 4),
                      Paragraph(f"<b>Freight Notes:</b> {freight_notes_txt}", notes_style_2)]
            story += [Spacer(1, 4)]

        acc_width = 3.5 * inch
        acc_data = [
            [Paragraph("<b>Additional Course Equipment to Consider*</b>",
                       ParagraphStyle('ACCHdr', parent=styles['Normal'], fontSize=9, alignment=1,
                                      textColor=colors.black, leading=11), )],
            ["Number Plate", fmt_money(35.00)],
            ["Powder Coat Fee - Stock Color", fmt_money(90.00)],
            ["Additional Anchor - Pin Positions", fmt_money(30.00)],
            ["Basic Color Tee Sign", fmt_money(55.00)],
            ["12\"x18\" Color Rules Sign", fmt_money(69.00)],
            ["Pole Extension", fmt_money(60.00)],
            ["Basket Flag", fmt_money(30.00)],
            [Paragraph("<b>*Per Unit Pricing</b>",
                       ParagraphStyle('ACCfTR', parent=styles['Normal'], fontSize=8, alignment=1,
                                      textColor=colors.black, leading=10))],
        ]

        acc_tbl = Table(acc_data, colWidths=[acc_width * 0.7, acc_width * 0.3])
        acc_tbl.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('SPAN', (0, 0), (-1, 0)),
            ('SPAN', (0, -1), (-1, -1)),
            ('ALIGN', (1, 1), (1, -2), 'RIGHT'),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('ALIGN', (0, -1), (0, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        acc_tbl.hAlign = 'LEFT'

        totals_width = 3.0 * inch
        totals_data = [
            ["Subtotal:", fmt_money(totals.get('subtotal', 0.0))],
            ["Drop-Ship Fee:", fmt_money(fees.get('drop_ship_fee', 0.0))],
            ["Freight:", fmt_money(fees.get('freight', 0.0))],
            [f"Sales Tax ({totals.get('tax_rate_pct', 0.0) * 100:.2f}%):", fmt_money(totals.get('sales_tax', 0.0))],
            ["**GRAND TOTAL:**", f"**{fmt_money(totals.get('grand_total', 0.0))}**"],
        ]

        t_totals = Table(totals_data, colWidths=[totals_width * 0.65, totals_width * 0.35])
        t_totals.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ]))

        totals_col_width = CONTENT_WIDTH - acc_width  # CONTENT_WIDTH is 7.5 * inch
        combined_row = [[acc_tbl, t_totals]]

        combined_table = Table(combined_row, colWidths=[acc_width, totals_col_width])
        combined_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (0, 0), 0),
            ('RIGHTPADDING', (0, 0), (0, 0), 0),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ]))
        combined_table.hAlign = 'LEFT'
        story += [combined_table, Spacer(1, 18)]

        story += [Paragraph("<b>Notes:</b>", notes_style), Paragraph(footer_notes_text, notes_style)]

    # --- LIVE PREVIEW CONSISTENCY FIX: Ensure buffer is ready for reading ---
    doc.build(story)
    buffer.seek(0)  # Rewind the buffer to the beginning after building
    # The return statement now uses the ready-to-read buffer
    return buffer.getvalue()


# --- Custom Streamlit logic (MODIFIED) ---
def handle_pdf_generation(payload: dict, doc_number: str, template: str, container: st.delta_generator.DeltaGenerator,
                          order_meta: dict | None = None):
    """Generates PDF, attempts save, and renders the download button."""

    # 1. Prepare file names
    is_quote = template == "quote"
    file_prefix = f"{doc_number}_Quote" if is_quote else f"{doc_number}_Order"
    label = f"Download Quote PDF" if is_quote else f"Download Order/PO PDF"

    # 2. Generate PDF data
    pdf_buffer = io.BytesIO()
    pdf_data = build_pdf(
        pdf_buffer,
        payload["customer"],
        payload["line_items"],
        payload["fees"],
        payload["totals"],
        doc_number,
        payload["footer_notes"],
        template=template,
        meta=order_meta,
    )

    # 3. Attempt to save to Google Sheets
    save_successful = save_quote_to_gsheet(payload)

    # 4. Display status message
    if is_quote:
        if save_successful:
            container.success(f"Quote **{doc_number}** successfully saved to **Google Sheets** and PDF generated.")
        else:
            container.warning(
                "Quote PDF generated but **FAILED to save** to Google Sheets. Check Sheet configuration and sharing permissions."
            )
    else:  # is 'order'
        # Check if the Order Document # is the same as the Source Quote #
        source_quote_no = payload.get('source_quote_number', 'N/A')
        doc_msg = (
            f"Order **{doc_number}** PDF generated."
            if doc_number == source_quote_no else
            f"Order **{doc_number}** PDF generated (Source Quote: **{source_quote_no}**)."
        )

        container.success(
            doc_msg
            + (f" Saved to Google Sheets." if save_successful else f" **FAILED to save** to Google Sheets.")
        )

    # 5. Render the download button (ALWAYS renders after successful generation)
    container.download_button(
        label=label,
        data=pdf_data,
        file_name=f"{file_prefix}.pdf",
        mime="application/pdf",
        # Use unique keys to allow multiple download buttons on the page
        key=f"download_{template}_pdf_{doc_number}",
        use_container_width=True
    )


# --- NEW HELPER FUNCTION TO GET PAYLOAD (DRY) ---
def get_current_payload(subtotal: float, drop_ship_fee: float, freight: float, sales_tax: float, grand_total: float,
                        tax_rate: float) -> dict:
    """Assembles and returns the current payload dict from session state."""
    quote_no = st.session_state["quote_no"]

    # Re-assemble order_meta using session state values
    order_meta = {
        "order_doc_number": st.session_state.get("order_doc_number_pdf", quote_no),
        "po_number": st.session_state["order_po_number"],
        "operator": st.session_state["order_operator"],
        "terms": st.session_state["order_terms"],
        "commission_to": st.session_state["order_comm_to"],
        "check_number": st.session_state["order_check_number"],
        "date_received": st.session_state["order_date_received"],
        # Crucial: Save the actual quote number used to create this order/payload
        "source_quote_number": quote_no
    }

    fees = {
        "drop_ship_fee": drop_ship_fee,
        "freight": freight,
    }
    totals = {
        "subtotal": subtotal,
        "sales_tax": sales_tax,
        "grand_total": grand_total,
        "tax_rate_pct": tax_rate,
    }
    tax_meta = {
        "tax_rate_pct_input": st.session_state["tax_rate_pct_input"],
        "sc_county_checkbox": st.session_state["sc_county_checkbox"],
    }

    payload = {
        "quote_no": quote_no,
        "date": get_pacific_now().isoformat(),
        "customer": st.session_state["customer"],
        "line_items": st.session_state["line_items"],
        "fees": fees,
        "totals": totals,
        "tax_meta": tax_meta,
        "freight_notes": st.session_state["freight_notes"],
        "footer_notes": st.session_state["footer_notes"],
        "order_meta": order_meta,
    }
    return payload


# --- END NEW HELPER FUNCTION ---


# =============================================================================
# 5. Main Application Logic
# =============================================================================

# --- Line Item Callback Functions (New/Modified) ---

def move_item(item_id: str, direction: str):
    """
    Moves a line item up or down in the list, ensuring the discount item stays last.
    Sets the rerun flag to trigger a refresh.
    """
    items = st.session_state["line_items"]
    # Find the index of the item to move
    try:
        current_index = next(i for i, item in enumerate(items) if item["id"] == item_id)
    except StopIteration:
        return  # Item not found

    new_index = current_index

    if direction == "up" and current_index > 0:
        new_index = current_index - 1
    elif direction == "down" and current_index < len(items) - 1:
        new_index = current_index + 1

    # Check if the new index is the Course Discount, and prevent moving past it
    discount_idx = find_course_discount_index(items)

    # Prevent moving the discount item up (it should only be moved if it gets out of place)
    if current_index == discount_idx and direction == "up":
        # Do not move the discount item up
        return

    # Prevent moving a regular item into the discount's spot if it's the last item
    if new_index == discount_idx and discount_idx == len(items) - 1:
        # If the discount item is correctly placed at the end, stop the regular item one step before it
        if direction == "down":
            return

    # Swap the items
    if new_index != current_index:
        items[current_index], items[new_index] = items[new_index], items[current_index]

        # After any move, ensure the discount item is still last if it exists
        ensure_course_discount_stays_last(items)

        # Force rerun to reflect the new order
        st.session_state["rerun_flag"] = True


def move_item_up(item_id: str):
    """Callback for moving an item up."""
    move_item(item_id, "up")


def move_item_down(item_id: str):
    """Callback for moving an item down."""
    move_item(item_id, "down")


def remove_item(item_id):
    """Removes a line item based on its ID and sets the rerun flag if the discount is affected."""
    line_items_before = len(st.session_state["line_items"])
    st.session_state["line_items"] = [
        item for item in st.session_state["line_items"] if item["id"] != item_id
    ]
    # Check if removing the item might affect the discount and trigger a rerun if the list size changed.
    if line_items_before != len(st.session_state["line_items"]):
        # Re-run the core discount logic (which adds/removes/updates)
        if ensure_course_discount(st.session_state["line_items"]):
            st.session_state["rerun_flag"] = True  # Rerun if discount changed


def add_item_callback(sku: str = ""):
    """Adds a new line item and sets the rerun flag."""
    new_id = str(uuid.uuid4())
    sku = (sku or "").upper().strip()  # Ensure SKU is uppercase and stripped
    notes = ""

    # Pull Notes from products.csv if SKU exists and it's not CD
    if sku and sku != "CD":
        product_row = PRODUCTS.loc[PRODUCTS["SKU"] == sku]
        if not product_row.empty:
            notes = product_row["Notes"].iloc[0]

    st.session_state["line_items"].append({
        "id": new_id,
        "sku": sku,
        "name": "",
        "qty": 1,
        "unit": 0.0,
        "total": 0.0,
        "Notes": notes,        # <-- dynamically pulled for regular items
        "prev_sku": "",
        "previewChecked": True,
    })
    st.session_state["rerun_flag"] = True


    # Force a rerun so the new line item appears
    st.session_state["rerun_flag"] = True




def handle_quantity_change(item_id: str):
    """
    Callback triggered when a quantity is changed (on change/enter),
    explicitly recalculates total and checks discount.
    """
    items = st.session_state["line_items"]

    # 1. Update the total for the specific item
    for item in items:
        if item["id"] == item_id:
            # The widget updates the session state key directly, we just need to ensure the total is calculated
            # Use the key used in the number_input widget
            item_qty = int(st.session_state[f"qty_input_{item_id}"])
            item_unit = float(item.get("unit", 0.0))
            item["qty"] = item_qty
            item["total"] = round(item_qty * item_unit, 2)
            break

    # 2. Re-run the core discount logic
    if ensure_course_discount(items):
        # If the discount changed (added/removed/updated), force a full rerun
        st.session_state["rerun_flag"] = True


def search_pipedrive_callback():
    """
    Callback triggered when the Pipedrive search term changes (or Enter is hit).
    Performs the search directly.
    """
    term = st.session_state.get("pd_term", "").strip()
    if term:
        try:
            # Performs the search and updates the 'pd_matches' state
            st.session_state["pd_matches"] = pd_search_persons(term)
        except Exception as e:
            # Using st.error here is fine if the user is interacting with the form
            st.error(f"Pipedrive search failed: {e}")
            st.session_state["pd_matches"] = []
    else:
        st.session_state["pd_matches"] = []


def main_app():
    """Contains all the original quoting tool functionality."""

    st.title("DGA Quoting Tool")

    # <<< UI FIX START: CSS Injection and Column Adjustment >>>
    # -------------------------------------------------------------------------
    # UI FIX: Inject CSS for consistent button sizing and drag handle visibility
    # -------------------------------------------------------------------------
    st.markdown("""
        <style>
            /* Prevents the header buttons from wrapping */
            .stButton>button {
                white-space: nowrap !important;
                font-size: 14px;
                line-height: 1.0; 
                height: 38px; 
                margin-top: 0px; 
            }

            /* Targets the container holding the selectbox label/value to align it with the button labels */
            div[data-testid="stVerticalBlock"] div[data-testid="stHorizontalBlock"] > div:nth-child(2) label {
                padding-top: 0;
            }

            /* Ensure the Doc # info box is vertically aligned by moving it up */
            div[data-testid*="stHorizontalBlock"] > div:nth-child(1) .stAlert {
                margin-top: -15px !important; 
            }

            /* --- FIX for Red Key Display (More aggressive selector) --- */
            /* This targets the container holding the red box output from the old sortable component,
               which might still be appearing due to old caching or hidden component rendering. */
            div.stVerticalBlock > div.stVerticalBlock > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) {
                display: none;
            }
            /* A broader approach to hide the output of the line item ID list */
            div[data-testid="stVerticalBlock"] > div > div > div:first-child[data-testid="stVerticalBlock"]:has(div.stAlert) {
                display: none;
            }
            /* End Red Key Fix */

            /* NEW: PDF Preview iframe size fix for mobile/smaller screens */
            .pdf-iframe-container {
                overflow: auto;
                height: 100vh; /* Takes up available height */
            }
            .pdf-iframe-container iframe {
                width: 100%;
                height: 100%;
                border: 1px solid #ddd;
            }

        </style>
    """, unsafe_allow_html=True)
    # -------------------------------------------------------------------------

    # --- RERUN CHECK FOR UNIT PRICE / DISCOUNT FIX ---
    if st.session_state["rerun_flag"]:
        st.session_state["rerun_flag"] = False
        # IMPORTANT: Rerunning immediately fixes the visibility and re-keying issues (Course Discount Phantom).
        st.rerun()

    # (UI for Quote Lookup/New Quote)
    # MODIFIED: Adjusted ratios to [1.0, 1.4, 0.7] to combine the three buttons into one stackable column.
    lookup_col1, lookup_col2, lookup_col_stack = st.columns([1.0, 1.4, 0.7])
    # <<< UI FIX END >>>

    # Set the key suffix for all customer inputs
    cust_key_suffix = st.session_state["customer_key_suffix"]

    with lookup_col1:
        st.markdown("**Current Doc # (PT)**")
        st.info(st.session_state["quote_no"])

    with lookup_col2:
        # --- QUOTE LOOKUP CHANGE: Load from Sheet and display in Selectbox ---
        all_quotes_df = load_all_quotes()
        # Create display options: (New Quote) + all saved Quote #s
        # Handle case where load_all_quotes returns empty DF due to error
        quote_options = ["(New Quote)"]
        if 'Quote #' in all_quotes_df.columns:
            quote_options.extend(all_quotes_df['Quote #'].tolist())

        # Ensure the current quote number is available in the options if it exists
        current_quote_no = st.session_state["quote_no"]
        if current_quote_no not in quote_options:
            quote_options.append(current_quote_no)

        try:
            default_index = quote_options.index(current_quote_no)
        except ValueError:
            default_index = 0  # Default to (New Quote)

        selected_quote_no = st.selectbox("Select or Search for Doc #", quote_options, index=default_index,
                                         key="quote_select_box")

    # --- STACKED BUTTONS COLUMN ---
    # The container ensures the items stack vertically
    with lookup_col_stack:
        # A placeholder div to push the buttons down to align with the selectbox
        # Use st.container() without arguments to create a vertical block
        with st.container():
            st.markdown("<div style='min-height: 25px;'></div>", unsafe_allow_html=True)  # Spacer

            # 1. Retrieve
            if st.button("Retrieve", use_container_width=True, key="btn_retrieve_quote"):
                if selected_quote_no != "(New Quote)":
                    st.session_state["quote_no"] = selected_quote_no

                    # --- RETRIEVAL LOGIC CHANGE: Load from DataFrame (which came from Google Sheets) ---
                    try:
                        # Find the row in the DataFrame corresponding to the selected Quote #
                        # Note: We search by the Quote # column which might contain saved Order document numbers
                        target_row_df = all_quotes_df[all_quotes_df['Quote #'] == selected_quote_no]

                        if target_row_df.empty:
                            st.error(f"Quote/Order # {selected_quote_no} not found in the loaded data.")
                            return

                        payload = target_row_df.iloc[-1]['Payload']  # Get the latest version

                        # Apply payload data to session state
                        # NOTE: The customer dictionary now includes bill_name, bill_company, bill_email, bill_phone keys
                        st.session_state["customer"] = payload.get("customer", {})
                        st.session_state["line_items"] = payload.get("line_items", [])
                        fees = payload.get("fees", {})
                        st.session_state["drop_fee_input"] = float(fees.get("drop_ship_fee", 0.0))
                        st.session_state["freight_fee_input"] = float(fees.get("freight", 0.0))
                        st.session_state["freight_notes"] = payload.get("freight_notes", "")
                        tax_meta = payload.get("tax_meta", {})
                        st.session_state["tax_rate_pct_input"] = float(
                            tax_meta.get("tax_rate_pct_input", DEFAULT_TAX * 100))
                        st.session_state["sc_county_checkbox"] = bool(tax_meta.get("sc_county_checkbox", False))
                        st.session_state["footer_notes"] = payload.get("footer_notes", st.session_state["footer_notes"])

                        # Load Order/PO Details from Payload with robust defaulting
                        order_meta = payload.get("order_meta", {})
                        st.session_state["order_po_number"] = order_meta.get("po_number", "")
                        st.session_state["order_operator"] = order_meta.get("operator", "CZ")
                        st.session_state["order_terms"] = order_meta.get("terms", "NET 30")
                        st.session_state["order_comm_to"] = order_meta.get("commission_to", "")
                        st.session_state["order_check_number"] = order_meta.get("check_number", "")
                        st.session_state["order_date_received"] = order_meta.get("date_received",
                                                                                 get_pacific_now().strftime('%m/%d/%y'))

                        # Use the loaded 'order_doc_number' if available, otherwise default to the quote number
                        loaded_doc_number = order_meta.get("order_doc_number", st.session_state["quote_no"])
                        # Ensure it defaults to the loaded quote number if blank:
                        st.session_state["order_doc_number_pdf"] = loaded_doc_number or st.session_state["quote_no"]

                        # CUSTOMER AUTOFILL FIX: Increment the key suffix to force widget reset
                        st.session_state["customer_key_suffix"] += 1

                        st.success(f"Loaded document **{selected_quote_no}** from Google Sheets.")
                        st.rerun()

                    except IndexError:
                        st.error(f"Quote {selected_quote_no} not found in the loaded data.")
                    except Exception as e:
                        st.error(f"Couldn't load document {selected_quote_no} from Google Sheets: {e}")
                else:
                    st.warning("Please select a document to retrieve or click 'New Quote'.")

            # 2. New Quote
            if st.button("New Quote", use_container_width=True, type="secondary"):
                start_new_quote()

            # 3. New Version
            if st.button("New Version", use_container_width=True, type="primary",
                         help="Create a new version number based on the current quote."):
                assign_new_quote_version()
    # --- END STACKED BUTTONS COLUMN ---

    # -------------------------------------------------------------------------
    # NEW: Sidebar for PDF Preview
    # -------------------------------------------------------------------------
    with st.sidebar:
        st.header("PDF Preview")
        # FIX IMPLEMENTATION: Remove `value=...` and rely on session state default
        st.session_state["show_pdf_preview"] = st.checkbox("Show Live Quote Preview",
                                                           key="live_preview_checkbox",
                                                           value=st.session_state["show_pdf_preview"])

        # Display the live preview if the checkbox is checked
        if st.session_state["show_pdf_preview"]:
            # Apply tax **only if manually set** or SC checkbox checked
            if st.session_state["sc_county_checkbox"]:
                tax_rate = SANTA_CRUZ_TAX_RATE
            else:
                tax_input = float(st.session_state.get("tax_rate_pct_input", 0.0))
                tax_rate = tax_input / 100 if tax_input > 0 else 0.0

            # Subtotal only includes items marked for preview
            subtotal = sum(float(r["total"]) for r in st.session_state["line_items"] if r.get("previewChecked", True))

            drop_ship_fee = st.session_state["drop_fee_input"]
            freight = st.session_state["freight_fee_input"]
            pre_tax = subtotal + float(drop_ship_fee) + float(freight)

            sales_tax = round(pre_tax * tax_rate, 2)
            grand_total = round(pre_tax + sales_tax, 2)

            # Get the current payload (which contains all data needed for the PDF)
            preview_payload = get_current_payload(subtotal, drop_ship_fee, freight, sales_tax, grand_total, tax_rate)

            try:
                # Generate PDF data
                pdf_buffer = io.BytesIO()
                pdf_data = build_pdf(
                    pdf_buffer,
                    preview_payload["customer"],
                    # Pass the full line items list; build_pdf handles filtering by previewChecked
                    preview_payload["line_items"],
                    preview_payload["fees"],
                    preview_payload["totals"],
                    preview_payload["quote_no"],
                    preview_payload["footer_notes"],
                    template="quote",
                    meta=preview_payload["order_meta"],
                )

                # Encode to Base64
                base64_pdf = base64.b64encode(pdf_data).decode('utf-8')

                # Use st.markdown with an iframe to render the PDF
                # The height is set to make it scroll nicely in the sidebar
                # FIX: Removed the #toolbar=0&navpanes=0&scrollbar=0 parameters, which often cause the iframe to fail in Streamlit Cloud/Cross-Origin contexts.
                pdf_display = f"""
                <div class="pdf-iframe-container" style="height: 80vh;">
                    <iframe 
                        src="data:application/pdf;base64,{base64_pdf}" 
                        title="PDF Preview"
                        style="width: 100%; height: 100%; border: none;">
                    </iframe>
                </div>
                """
                st.markdown(pdf_display, unsafe_allow_html=True)

            except Exception as e:
                st.error(f"Error generating PDF preview: {e}")

    # -------------------------------------------------------------------------

    # (UI for Customer Info)
    c = st.session_state["customer"]

    st.subheader("Customer Information")

    # Pipedrive Lookup
    # 🐛 FIX: Removed explicit Search Button, using on_change to trigger search on Enter/Focus Out

    # Set the expander to True if a search term is present OR matches were found.
    has_search_term = st.session_state.get("pd_term", "").strip() != ""
    has_matches = bool(st.session_state.get("pd_matches", []))
    expander_default_state = has_search_term or has_matches

    with st.expander("Pipedrive lookup (by email or name)", expanded=expander_default_state):
        if not PIPEDRIVE_API_TOKEN:
            st.warning("Pipedrive API Token not configured in environment variables. Lookup disabled.")
        else:
            # <<< FIX: ADDED on_change CALLBACK AND REMOVED SEPARATE BUTTON >>>
            term = st.text_input("Search term", placeholder="e.g. jane@city.gov or Jane Smith", key="pd_term",
                                 on_change=search_pipedrive_callback)
            # <<< END FIX >>>

            matches = st.session_state.get("pd_matches", [])

            if matches:
                labels = [f"{m['name']}  <{m['email']}>" if m['email'] else m['name'] for m in matches]
                choice = st.selectbox("Matches", labels, key="pd_choice")
                idx = labels.index(choice) if choice in labels else -1
                if idx >= 0:
                    sel = matches[idx]
                    if st.button("Apply to form", key="pd_apply_btn"):
                        try:
                            # 1. Fetch the full Person record
                            person = pd_get_person(sel["id"])

                            # 2. Get associated Org ID and fetch Organization record (if available)
                            org_id = _pd_scalar(person.get("org_id")) if person and person.get("org_id") else None
                            org = pd_get_org(org_id) if org_id else None

                            # 3. Map Pipedrive data to customer state
                            mapped = pd_person_to_customer(person or {}, org)
                            cust = st.session_state["customer"]
                            for k, v in mapped.items():
                                cust[k] = v or cust.get(k, "")

                            # CUSTOMER AUTOFILL FIX: Increment the key suffix to force widget reset
                            st.session_state["customer_key_suffix"] += 1

                            st.success("Pipedrive contact applied to form (Person details ➜ Org fallback).")
                            # Force rerun to populate all text inputs immediately
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to fetch or apply contact details. Check console: {e}")
            elif term and not matches:  # Use the current 'term' variable for the check
                st.info(f"No Pipedrive contacts found matching '{term}'.")

    # Customer Info Inputs
    with st.container(border=True):
        cols_addr = st.columns(2)

        # --- SHIPPING ADDRESS (LEFT COLUMN) ---
        with cols_addr[0]:
            st.subheader("Shipping Address")
            # NOTE: All customer keys now include the dynamic suffix
            c["company"] = st.text_input("Company", value=c.get("company", ""),
                                         key=f"ship_company_{cust_key_suffix}")
            c["name"] = st.text_input("Name", value=c.get("name", ""),
                                      key=f"ship_contact_name_{cust_key_suffix}")
            c["phone"] = st.text_input("Phone", value=c.get("phone", ""), key=f"ship_phone_{cust_key_suffix}")
            c["email"] = st.text_input("Email", value=c.get("email", ""), key=f"ship_email_{cust_key_suffix}")
            c["ship_addr1"] = st.text_area("Address Line 1", value=c.get("ship_addr1", ""),
                                           key=f"ship_addr1_{cust_key_suffix}")
            sc1, sc2, sc3 = st.columns(3)
            c["ship_city"] = sc1.text_input("City", value=c.get("ship_city", ""),
                                            key=f"ship_city_input_{cust_key_suffix}")
            c["ship_state"] = sc2.text_input("State", value=c.get("ship_state", ""),
                                             key=f"ship_state_input_{cust_key_suffix}")
            c["ship_zip"] = sc3.text_input("Zip", value=c.get("ship_zip", ""),
                                           key=f"ship_zip_input_{cust_key_suffix}")

        # --- BILLING ADDRESS (RIGHT COLUMN) ---
        with cols_addr[1]:
            st.subheader("Billing Address")

            # FIX: Unlock Company/Phone/Email for Bill-To
            c["bill_company"] = st.text_input("Company", value=c.get("bill_company", c.get("company", "")),
                                              key=f"bill_company_{cust_key_suffix}")

            # FIX: UNLOCK NAME FIELD. Use a unique key and the bill_name field.
            c["bill_name"] = st.text_input("Name", value=c.get("bill_name", c.get("name", "")),
                                           # Use bill_name key
                                           key=f"bill_name_input_{cust_key_suffix}",  # New key for editable input
                                           help="This is the contact person for billing.")

            c["bill_phone"] = st.text_input("Phone", value=c.get("bill_phone", c.get("phone", "")),
                                            key=f"bill_phone_{cust_key_suffix}")
            c["bill_email"] = st.text_input("Email", value=c.get("bill_email", c.get("email", "")),
                                            key=f"bill_email_{cust_key_suffix}")

            # Now the main address text area should align
            c["bill_addr1"] = st.text_area("Address Line 1 ", value=c.get("bill_addr1", ""),
                                           key=f"bill_addr1_{cust_key_suffix}")
            bc1, bc2, bc3 = st.columns(3)
            c["bill_city"] = bc1.text_input("City", value=c.get("bill_city", ""),
                                            key=f"bill_city_input_{cust_key_suffix}")
            c["bill_state"] = bc2.text_input("State", value=c.get("bill_state", ""),
                                             key=f"bill_state_input_{cust_key_suffix}")
            c["bill_zip"] = bc3.text_input("Zip", value=c.get("bill_zip", ""),
                                           key=f"bill_zip_input_{cust_key_suffix}")

    st.divider()

    # 2) Line Items
    st.subheader("Line Items")

    # Add Line Item Button (uses on_click to prevent phantom presses on add)
    st.button("Add Line Item", key="btn_add_line_top", on_click=add_item_callback)

    sku_to_name = PRODUCTS.set_index('SKU')['Name'].to_dict()
    sku_options_display = ["(custom)"] + [f"{s} — {sku_to_name.get(s, 'No Name')}" for s in PRODUCTS["SKU"].tolist()]

    # --- COURSE DISCOUNT AUTO-ADD/REMOVE AND RERUN FIX ---
    # Perform this check BEFORE the rendering loop to ensure the list is clean
    ensure_course_discount(st.session_state["line_items"])

    # --- LINE ITEM RENDERING LOOP ---
    # Check the list length again in case the discount check removed an item
    for i in range(len(st.session_state["line_items"])):
        # Retrieve the item by index
        row = st.session_state["line_items"][i]

        # Skip the Course Discount line for the drag handle, but allow removal via button
        is_course_discount = row.get("sku") == "CD"

        # NEW: Checkbox state for preview
        # If the item doesn't have the key (e.g., loaded from old state), default it to True
        is_preview_checked = row.get("previewChecked", True)

        # Determine if the item can be moved
        can_move_up = i > 0
        can_move_down = i < len(st.session_state["line_items"]) - 1

        # The discount item should not be moved if it is the last item
        if is_course_discount and i == len(st.session_state["line_items"]) - 1:
            can_move_up = False
            can_move_down = False

        # Also prevent moving an item down past the discount line if it's the item just before it
        if not is_course_discount and i == len(st.session_state["line_items"]) - 2 and find_course_discount_index(
                st.session_state["line_items"]) == len(st.session_state["line_items"]) - 1:
            can_move_down = False

        # Start of the individual item card for rendering
        item_container = st.container(border=True)
        with item_container:
            # Row for Item Number and Move/Remove buttons
            header_col1, header_col2, header_col3, header_col4, header_col5 = st.columns([0.8, 0.4, 0.4, 0.4, 1.2])

            with header_col1:
                st.markdown(f"**Item {i + 1}**")

            with header_col2:
                if can_move_up:
                    st.button("⬆️", key=f"btn_up_{row['id']}", help="Move item up",
                              on_click=move_item_up, args=(row["id"],), use_container_width=True)
                else:
                    st.empty()

            with header_col3:
                if can_move_down:
                    st.button("⬇️", key=f"btn_down_{row['id']}", help="Move item down",
                              on_click=move_item_down, args=(row["id"],), use_container_width=True)
                else:
                    st.empty()

            with header_col4:
                st.button("🗑️", key=f"btn_rm_{row['id']}", help="Remove item",
                          on_click=remove_item, args=(row["id"],), use_container_width=True)

            with header_col5:
                # Preview Checkbox Widget
                if is_course_discount:
                    st.checkbox("Show in Preview", value=True, disabled=True, key=f"preview_check_{row['id']}",
                                help="Discount is always shown in preview.")
                else:
                    # NOTE: This uses the existing `row` dictionary to store/retrieve state.
                    # Setting the key directly to the item's ID makes it sticky.
                    new_checked_state = st.checkbox("Show in Preview", value=is_preview_checked,
                                                    key=f"preview_check_{row['id']}")

                    # Check if the state changed, and update the list item
                    if new_checked_state != is_preview_checked:
                        row["previewChecked"] = new_checked_state
                        # Force rerun to ensure the total recalculation (in case the item was unchecked)
                        st.session_state["rerun_flag"] = True

            # Adjust columns for input/price/total
            c1, c2, c3, c4 = st.columns([4, 1, 1, 1])

            current_sku = row.get("sku", "")
            prod_name = row.get("name", "")
            prod_price = row.get("unit", 0.0)

            current_display = "(custom)"
            if current_sku:
                match = f"{current_sku} — {sku_to_name.get(current_sku, prod_name)}"
                if match in sku_options_display:
                    current_display = match

            try:
                sel_idx = sku_options_display.index(current_display)
            except ValueError:
                sel_idx = 0

            with c1:
                # Discount item is a locked Text input, not a selectbox
                if is_course_discount:
                    # FIX: Use markdown/metric layout for cleaner non-editable display
                    st.markdown("**Auto-Discount**", help="This line is automatically calculated and non-editable.")
                    st.markdown(f"**{row['name']}**")
                else:
                    sku_selected_display = st.selectbox("Product Description", sku_options_display, index=sel_idx,
                                                        key=f"sku_select_{row['id']}")

                    # --- UNIT PRICE, NAME, AND NOTES AUTOFILL LOGIC ---
                    # Start with existing Notes (use capital N consistently)
                    new_notes = row.get("Notes", "")  # <-- match CSV column

                    if sku_selected_display == "(custom)":
                        # Keep current values for custom items
                        new_sku = ""
                        new_name = prod_name
                        new_unit = prod_price
                        # Do not overwrite Notes
                    else:
                        # Parse SKU from display string
                        parts = sku_selected_display.split('—', 1)
                        new_sku = parts[0].strip()

                        # Lookup product in PRODUCTS DataFrame
                        prod = PRODUCTS[PRODUCTS["SKU"] == new_sku]
                        if not prod.empty:
                            new_name = str(prod.iloc[0]["Name"])
                            new_unit = float(prod.iloc[0]["UnitPrice"]) if pd.notna(prod.iloc[0]["UnitPrice"]) else 0.0

                            # <-- NEW: Only pull Notes from CSV if SKU is NOT CD
                            if new_sku != "CD":
                                new_notes = str(prod.iloc[0]["Notes"]) if "Notes" in prod.columns and pd.notna(
                                    prod.iloc[0]["Notes"]) else ""
                        else:
                            # Fallback if SKU not found
                            new_name = parts[1].strip() if len(parts) > 1 else new_sku
                            new_unit = prod_price

                            if new_sku != "CD":
                                new_notes = ""

                    # --- Update the row in session_state line_items ---
                    if new_sku != row["sku"]:
                        row["sku"] = new_sku
                        row["name"] = new_name
                        row["unit"] = new_unit
                        row["Notes"] = new_notes
                        row["prev_sku"] = new_sku if new_sku else "(custom)"

                        # 🔥 FORCE notes textarea to refresh
                        st.session_state[f"Notes_input_{row['id']}"] = new_notes

                        st.session_state["rerun_flag"] = True

                    # Custom Name input for non-SKU items
                    if not row["sku"] and not is_course_discount:
                        row["name"] = st.text_input("Custom Name (Required)", value=row["name"],
                                                    key=f"name_input_{row['id']}")
                    # ---------------------------------------------

            with c2:
                # Discount item quantity is auto-calculated and cannot be edited
                if is_course_discount:
                    # FIX: Use markdown/metric layout for cleaner non-editable display
                    st.markdown("**Qty**")
                    st.markdown(f"**{int(row['qty'])}**")
                else:
                    # <<< FIX: ADDED on_change CALLBACK >>>
                    row["qty"] = st.number_input("Qty", min_value=0, value=int(row.get("qty", 1)), step=1,
                                                 key=f"qty_input_{row['id']}",
                                                 on_change=handle_quantity_change, args=(row["id"],))
                    # <<< END FIX >>>

            with c3:
                current_unit = float(row.get("unit", 0.0) if pd.notna(row.get("unit", 0.0)) else 0.0)

                # Discount item unit price is locked
                if is_course_discount:
                    # FIX: Use markdown/metric layout for cleaner non-editable display
                    st.markdown("**Unit Price**")
                    st.markdown(f"**{fmt_money(current_unit)}**")
                else:
                    # UNIT PRICE AUTOFILL FIX: Dynamic Key including SKU forces widget reset when SKU changes
                    row["unit"] = st.number_input("Unit Price", min_value=-100000.0, value=current_unit, step=0.01,
                                                  format="%.2f",
                                                  key=f"unit_input_{row['id']}_{row['sku'] or 'custom'}")

            with c4:
                # Calculate total in every run, regardless of inputs
                row["total"] = round(float(row["qty"]) * float(row["unit"]), 2)
                st.markdown("**Total**")
                st.write(f"**{fmt_money(row['total'])}**")

            notes_key = f"Notes_input_{row['id']}"

            # Initialize widget state ONCE from CSV / existing value
            if notes_key not in st.session_state:
                st.session_state[notes_key] = row.get("Notes", "")

            # Render textarea
            new_notes_val = st.text_area(
                "Notes (optional)",
                key=notes_key,
                height=30
            )

            # Only update row if user actually changed something
            if new_notes_val != row.get("Notes", ""):
                row["Notes"] = new_notes_val

        # --- End of item container ---

    # Add Line Item Button (bottom)
    st.button("Add Line Item", key="btn_add_line_bottom", on_click=add_item_callback)

    # 3) Fees, Tax & Totals
    st.subheader("Fees, Tax & Totals")
    cc1, cc2, cc3, cc4 = st.columns(4)
    with cc1:
        drop_ship_fee = st.number_input("Drop-Ship Fee", min_value=0.0, step=1.0, key="drop_fee_input")
    with cc2:
        freight = st.number_input("Freight", min_value=0.0, step=1.0, key="freight_fee_input")
    with cc3:
        _ = st.number_input("Sales Tax Rate (%)", min_value=0.0, step=0.01, key="tax_rate_pct_input")
    with cc4:
        _ = st.checkbox(f"Use Santa Cruz County Sales Tax ({SANTA_CRUZ_TAX_RATE * 100:.2f}%)", key="sc_county_checkbox")

    st.text_area("Freight Notes (optional)", key="freight_notes",
                 placeholder="e.g., XPO, quote #12345, residential w/ liftgate, 2 pallets, ETA 5–7 biz days")

    tax_rate = SANTA_CRUZ_TAX_RATE if st.session_state["sc_county_checkbox"] \
        else float(st.session_state["tax_rate_pct_input"]) / 100.0

    # Subtotal only includes items marked for preview (consistent with PDF sidebar)
    subtotal = sum(float(r["total"]) for r in st.session_state["line_items"] if r.get("previewChecked", True))

    pre_tax = subtotal + float(drop_ship_fee) + float(freight)

    sales_tax = round(pre_tax * tax_rate, 2)
    grand_total = round(pre_tax + sales_tax, 2)

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        st.metric("Subtotal", f"${subtotal:,.2f}")
    with s2:
        st.metric("Drop-Ship Fee", f"${drop_ship_fee:,.2f}")
    with s3:
        st.metric("Freight", f"${freight:,.2f}")
    with s4:
        st.metric("Grand Total", f"${grand_total:,.2f}")

    qual_qty = eligible_qty_for_discount(st.session_state["line_items"])
    if qual_qty >= 9:
        st.success(f"Course Discount active: **-$100** × {qual_qty} qualifying baskets.")
    else:
        st.info(
            f"Qualifying baskets: {qual_qty}. Add {max(0, 9 - qual_qty)} more Mach 5/7/X (Std/Portable/No Frills) to trigger the Course Discount.")

    st.divider()

    # 4) Generate PDF Quote + Order PDF
    st.subheader("Generate PDF Documents")

    # --- FIX: REMOVED QUOTE # INPUT FIELD ---
    quote_no = st.session_state["quote_no"]  # Use the canonical value
    st.markdown(f"**Current Quote #:** `{quote_no}`")
    # ----------------------------------------

    st.session_state["footer_notes"] = st.text_area("Footer Notes (shown on PDF)",
                                                    value=st.session_state["footer_notes"],
                                                    key="footer_notes_input")

    # Order/PO Details Section
    with st.expander("Order/PO Details (for Order PDF)", expanded=False):
        # Seed the order doc number to the current quote if empty/missing
        if not st.session_state.get("order_doc_number_pdf"):
            st.session_state["order_doc_number_pdf"] = st.session_state["quote_no"]

        order_col1, order_col2 = st.columns(2)
        with order_col1:
            st.text_input(
                "Order/PO Document # (Used for Order PDF Header/File Name)",
                key="order_doc_number_pdf",  # Binds directly to the session key
                value=st.session_state.get("order_doc_number_pdf", quote_no)
            )
            st.text_input(
                "P.O. Number",
                key="order_po_number",  # Binds directly to the session key
            )
            st.text_input(
                "Operator",
                key="order_operator",  # Binds directly to the session key
            )
            st.text_input(
                "Terms",
                key="order_terms",  # Binds directly to the session key
            )
        with order_col2:
            st.text_input(
                "Commission To",
                key="order_comm_to",  # Binds directly to the session key
            )
            st.text_input(
                "Check Number",
                key="order_check_number",  # Binds directly to the session key
            )
            st.text_input(
                "Date Received",
                key="order_date_received",  # Binds directly to the session key
            )

    # --- NEW: Use Helper Function to assemble final payload ---
    payload = get_current_payload(subtotal, drop_ship_fee, freight, sales_tax, grand_total, tax_rate)
    order_meta = payload["order_meta"]  # Get the assembled order_meta from the payload

    # --- PDF Buttons ---
    pdf_col1, pdf_col2 = st.columns(2)

    # **MODIFIED QUOTE BUTTON LOGIC**
    if pdf_col1.button("Generate & SAVE Quote PDF", use_container_width=True, type="primary"):
        # The payload is already up-to-date from the helper function
        handle_pdf_generation(payload, quote_no, "quote", pdf_col1)

    # **MODIFIED ORDER BUTTON LOGIC**
    if pdf_col2.button("Process as Order / PO", use_container_width=True, type="secondary"):
        # The 'order_doc_number' is the number the user wants on the file name/header
        order_doc_number = st.session_state["order_doc_number_pdf"]
        # The payload is already up-to-date from the helper function
        handle_pdf_generation(payload, order_doc_number, "order", pdf_col2, order_meta=order_meta)


# =============================================================================
# 6. Main App Entry Point
# =============================================================================
if __name__ == '__main__':
    main_app()