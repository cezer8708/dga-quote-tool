import os
import io
import uuid
import json
from datetime import datetime
import requests
import re
import sys
from typing import Any

import pandas as pd
import streamlit as st
import gspread

# =============================================================================
# UI CHANGE 1: Set default layout to centered (NOT wide mode)
# =============================================================================
st.set_page_config(page_title="DGA Quoting Tool", layout="centered")

from dotenv import load_dotenv
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
)

# =============================================================================
# 0. Configuration and Environment
# =============================================================================
load_dotenv()


def get_env(key, default=None, cast=str):
    val = os.getenv(key, default)
    try:
        return cast(val) if val is not None else val
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
COMPANY_LOGO_PATH = get_env("COMPANY_LOGO_PATH", "assets/dga_logo.png")

# Pipedrive configuration retrieval
PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
PIPEDRIVE_BASE_URL = "https://api.pipedrive.com/v1"

# --- GOOGLE SHEETS CONFIGURATION ---
# **ENSURE THIS MATCHES THE TITLE OF THE SHEET YOU CREATED AND SHARED**
GOOGLE_SHEET_TITLE = "DGA Quoting Database"


# -----------------------------------


def fmt_money(value: float) -> str:
    """Formats a float as a currency string, e.g., 1234.56 -> $1,234.56"""
    return f"${value:,.2f}"


# =============================================================================
# 1. Google Sheets Connection and Data Handling
# =============================================================================
@st.cache_resource(ttl=3600)
def get_gsheet_client():
    """Initializes/caches gspread using a robust secrets loader."""
    try:
        # 0) If there's no section, allow local file fallback
        if "gcp_service_account" not in st.secrets:
            if os.path.exists("service_account.json"):
                return gspread.service_account(filename="service_account.json")
            st.error("Google Sheets Service Account not configured.")
            return None

        creds_data = st.secrets["gcp_service_account"]

        # 1) Accept any Mapping (Streamlit's SecretDict), then coerce to dict
        from collections.abc import Mapping
        sa_creds = None

        if isinstance(creds_data, Mapping):
            d = dict(creds_data)
            # Handle accidental nesting (rare, but seen in cloud UIs)
            if "gcp_service_account" in d and isinstance(d["gcp_service_account"], Mapping):
                d = dict(d["gcp_service_account"])
            if d.get("type") == "service_account":
                sa_creds = d

        # 2) If user stored JSON string for the block, parse it
        if sa_creds is None and isinstance(creds_data, str):
            try:
                decoded = json.loads(creds_data)
                if isinstance(decoded, dict) and decoded.get("type") == "service_account":
                    sa_creds = decoded
            except json.JSONDecodeError:
                st.error("Secret format error: gcp_service_account is a string but not valid JSON.")
                return None

        if not sa_creds or sa_creds.get("type") != "service_account":
            st.error("Google Sheets Service Account secret is invalid or missing 'type'.")
            return None

        # 3) Normalize private key newlines
        if "private_key" in sa_creds and isinstance(sa_creds["private_key"], str):
            sa_creds["private_key"] = sa_creds["private_key"].replace("\\n", "\n")

        # 4) Build the client
        return gspread.service_account_from_dict(sa_creds)

    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None


@st.cache_data(ttl=300)  # Cache for 5 minutes (adjust TTL as needed)
def load_all_quotes() -> pd.DataFrame:
    """Loads all quote data from the Google Sheet for lookup."""
    client = get_gsheet_client()
    if not client:
        return pd.DataFrame()

    try:
        # FIX: client.open_by_title(GOOGLE_SHEET_TITLE) -> client.open(GOOGLE_SHEET_TITLE)
        sh = client.open(GOOGLE_SHEET_TITLE)
        worksheet = sh.get_worksheet(0)

        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        if 'Quote #' not in df.columns or 'Quote JSON Payload' not in df.columns:
            st.error("Google Sheet missing required columns: 'Quote #' and 'Quote JSON Payload'. Check row 1.")
            return pd.DataFrame()

        # Convert the JSON string column back to actual dicts
        df['Payload'] = df['Quote JSON Payload'].apply(lambda x: json.loads(x) if x else None)
        return df.dropna(subset=['Payload'])

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Google Sheet with title '{GOOGLE_SHEET_TITLE}' not found. Check title and sharing.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading quotes from sheet: {e}")
        return pd.DataFrame()


def save_quote_to_gsheet(payload: dict) -> bool:
    """Saves a new quote to the Google Sheet."""
    client = get_gsheet_client()
    if not client:
        return False

    try:
        # FIX: client.open_by_title(GOOGLE_SHEET_TITLE) -> client.open(GOOGLE_SHEET_TITLE)
        sh = client.open(GOOGLE_SHEET_TITLE)
        worksheet = sh.get_worksheet(0)

        # Prepare the row data for the Sheet's main columns (A to G)
        row_data = [
            payload.get("quote_no"),
            payload.get("date"),
            payload["customer"].get("company", ""),
            payload["customer"].get("name", ""),
            payload["customer"].get("email", ""),
            payload["totals"].get("grand_total", 0.0),
            json.dumps(payload),  # Full payload is saved as a JSON string
        ]

        # Append the new row to the sheet
        worksheet.append_row(row_data, value_input_option='USER_ENTERED')

        # Clear the quote cache so the next load reflects the new entry
        load_all_quotes.clear()

        return True
    except Exception as e:
        st.error(f"Error saving quote to sheet: {e}")
        return False


# =============================================================================
# 2. Data: Local Product DB (Placeholder)
# =============================================================================
@st.cache_data
def load_products(path: str = "products.csv") -> pd.DataFrame:
    """
    Local catalog used for quoting.
    Required columns: SKU, Name, UnitPrice
    """
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        if "SKU" not in df.columns or "Name" not in df.columns or "UnitPrice" not in df.columns:
            raise ValueError("products.csv must have columns: SKU, Name, UnitPrice")

        df["SKU"] = df["SKU"].astype(str)
        df["UnitPrice"] = pd.to_numeric(
            df["UnitPrice"].astype(str).str.replace(r"[^0-9.\-]", "", regex=True),
            errors="coerce"
        )
        return df
    except FileNotFoundError:
        # Fallback to a minimal DataFrame if products.csv is missing
        st.warning(f"Product file not found at '{path}'. Using minimal placeholder data.")
        return pd.DataFrame({
            "SKU": ["M5-ST", "M7-PT", "M14-CO", "TS-BASIC"],
            "Name": ["Mach 5 Standard Basket", "Mach 7 Portable Basket", "Mach 14 Chain Collar",
                     "Basic Color Tee Sign"],
            "UnitPrice": [499.00, 399.00, 35.00, 55.00]
        })


PRODUCTS = load_products()

# =============================================================================
# 3. Session State Initialization
# =============================================================================

# --- App State ---
if "customer" not in st.session_state:
    st.session_state["customer"] = {
        "company": "", "name": "", "email": "", "phone": "",
        "ship_addr1": "", "ship_city": "", "ship_state": "", "ship_zip": "",
        "bill_addr1": "", "bill_city": "", "bill_state": "", "bill_zip": "",
    }

if "line_items" not in st.session_state:
    st.session_state["line_items"] = []

# --- RERUN FLAG (USED FOR UNIT PRICE FIX - PREVIOUS LOGIC) ---
if "rerun_flag" not in st.session_state:
    st.session_state["rerun_flag"] = False

# --- CUSTOMER AUTOFILL FIX: Dynamic Key Suffix ---
if "customer_key_suffix" not in st.session_state:
    st.session_state["customer_key_suffix"] = 0


def new_quote_number():
    return datetime.now().strftime("%Y%m%d-%H%M")


if "quote_no" not in st.session_state:
    st.session_state["quote_no"] = new_quote_number()

if "footer_notes" not in st.session_state:
    st.session_state["footer_notes"] = (
        "Pricing subject to change. Please review all details carefully.\n"
        "International customers will be responsible for all duties and taxes upon delivery."
    )

# --- widget-backed fields: initialize once (prevents Streamlit warning) ---
if "drop_fee_input" not in st.session_state:
    st.session_state["drop_fee_input"] = 0.0
if "freight_fee_input" not in st.session_state:
    st.session_state["freight_fee_input"] = 0.0
if "tax_rate_pct_input" not in st.session_state:
    st.session_state["tax_rate_pct_input"] = float(DEFAULT_TAX * 100)
if "sc_county_checkbox" not in st.session_state:
    st.session_state["sc_county_checkbox"] = False
if "freight_notes" not in st.session_state:
    st.session_state["freight_notes"] = ""

# --- NEW: Order/PO Details Session State (Persisted on Load/Save) ---
if "order_doc_number_pdf" not in st.session_state:
    # This will hold the specific document number for the Order PDF
    st.session_state["order_doc_number_pdf"] = ""
if "order_po_number" not in st.session_state:
    st.session_state["order_po_number"] = ""
if "order_operator" not in st.session_state:
    st.session_state["order_operator"] = "CZ"
if "order_terms" not in st.session_state:
    st.session_state["order_terms"] = "NET 30"
if "order_comm_to" not in st.session_state:
    st.session_state["order_comm_to"] = ""
if "order_check_number" not in st.session_state:
    st.session_state["order_check_number"] = ""
if "order_date_received" not in st.session_state:
    st.session_state["order_date_received"] = datetime.now().strftime('%m/%d/%y')


# --- END NEW ORDER STATE ---


# =============================================================================
# 4. Helper Functions (Includes Pipedrive Logic and PDF Builder)
# =============================================================================
def start_new_quote():
    for key in list(st.session_state.keys()):
        # Only clear keys created by this app
        if key in ["customer", "line_items", "quote_no", "footer_notes", "drop_fee_input", "freight_fee_input",
                   "tax_rate_pct_input", "sc_county_checkbox", "freight_notes", "pd_matches", "rerun_flag",
                   "customer_key_suffix",
                   # --- NEW KEYS FOR ORDER META ---
                   "order_po_number", "order_operator", "order_terms", "order_comm_to",
                   "order_check_number", "order_date_received", "order_doc_number_pdf"]:
            del st.session_state[key]

    # Re-initialize the minimum required keys
    st.session_state["quote_no"] = new_quote_number()
    if "customer" not in st.session_state: st.session_state["customer"] = {}
    st.session_state["line_items"] = []
    st.session_state["customer_key_suffix"] = 0
    if "footer_notes" not in st.session_state:
        st.session_state["footer_notes"] = (
            "Pricing subject to change. Please review all details carefully.\n"
            "International customers will be responsible for all duties and taxes upon delivery."
        )

    # Re-initialize Order/PO fields with defaults
    if "order_operator" not in st.session_state:
        st.session_state["order_operator"] = "CZ"
    if "order_terms" not in st.session_state:
        st.session_state["order_terms"] = "NET 30"
    if "order_date_received" not in st.session_state:
        st.session_state["order_date_received"] = datetime.now().strftime('%m/%d/%y')
    if "order_doc_number_pdf" not in st.session_state:
        # Crucial for the fix: ensures order doc # defaults to the new quote # on a fresh start
        st.session_state["order_doc_number_pdf"] = st.session_state["quote_no"]

    st.rerun()


def _clean(val):
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s in {"-", "—"} else s


def _get_nested_field_value(data: dict, key: str) -> str:
    """Extracts the 'value' of the first item from a Pipedrive field list (e.g., phone, email)."""
    val = data.get(key)
    if isinstance(val, list) and val:
        first_item = val[0]
        if isinstance(first_item, dict):
            return _clean(first_item.get("value"))
        elif isinstance(first_item, str):
            return _clean(first_item)
    return ""


# --- Pipedrive Helpers ---
def _pd_request(path: str, params: dict | None = None):
    if not PIPEDRIVE_API_TOKEN:
        print("PIPEDRIVE_API_TOKEN is missing or empty.", file=sys.stderr)
        return None

    headers = {"Content-Type": "application/json"}
    params = params or {}
    params["api_token"] = PIPEDRIVE_API_TOKEN

    url = f"{PIPEDRIVE_BASE_URL}/{path}"

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code != 200:
            print(f"Pipedrive API Error: {response.status_code} for URL: {url}", file=sys.stderr)
            return None

        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Pipedrive network request failed for {url}: {e}", file=sys.stderr)
        return None


def _pd_scalar(v: Any):
    if v is None:
        return None
    if isinstance(v, dict):
        for k in ("value", "id", "name"):
            if k in v and v[k] is not None:
                return v[k]
        return None
    return v


def pd_search_persons(term: str, limit: int = 10):
    if not PIPEDRIVE_API_TOKEN: return []

    data = _pd_request(
        "persons/search",
        {"term": term.strip(), "fields": "name,email", "exact_match": "false", "limit": limit}
    )

    if not data or not data.get("data"):
        return []

    items = data["data"].get("items", [])
    results = []
    for it in items:
        p = it.get("item", {})
        email = _get_nested_field_value(p, "email")

        results.append({
            "id": p.get("id"),
            "name": p.get("name") or "",
            "email": email or "",
        })
    return results


def pd_get_person(person_id: int):
    if not PIPEDRIVE_API_TOKEN: return None
    data = _pd_request(f"persons/{person_id}")
    return data.get("data") if data else None


def pd_get_org(org_id: int | None):
    if not PIPEDRIVE_API_TOKEN or not org_id: return None
    data = _pd_request(f"organizations/{org_id}")
    return data.get("data") if data else None


def _parse_us_address(addr: str):
    """
    Robustly parses US address string (e.g., '123 Main St, Anytown, CA 90210, USA')
    """
    street = city = state = postal = ""
    if not addr:
        return street, city, state, postal

    addr = re.sub(r',\s*(USA|US|United States)$', '', addr, flags=re.IGNORECASE).strip()
    parts = [p.strip() for p in addr.split(",") if p.strip()]

    if not parts:
        return street, city, state, postal

    city_state_zip_pattern = r"(.+?),\s*([A-Za-z]{2})(?:\s*(\d{5}(?:-\d{4})?))?$"
    state_zip_pattern = r"([A-Za-z]{2})\s*(\d{5}(?:-\d{4})?)$"

    if len(parts) >= 1:
        tail = parts[-1]
        m_csz = re.search(city_state_zip_pattern, tail)

        if m_csz:
            city, state, postal_match = m_csz.groups()
            postal = postal_match or ""
            street_remainder = tail[:m_csz.start()].strip().rstrip(',').strip()

            if street_remainder:
                street = ", ".join(parts[:-1] + [street_remainder])
            else:
                street = ", ".join(parts[:-1])

            if not street and len(parts) == 1 and m_csz.groups():
                if m_csz.start() > 0:
                    street_full = parts[0][:m_csz.start()].strip().rstrip(',').strip()
                    if street_full:
                        street = street_full

            return street.strip(), city.strip(), state.strip(), postal.strip()

    if len(parts) >= 3:
        tail = parts[-1]
        m_sz = re.search(state_zip_pattern, tail)

        if m_sz:
            state, postal = m_sz.groups()
            city_part = tail[:m_sz.start()].strip().rstrip(',').strip()

            if not city_part and len(parts) >= 2:
                city = parts[-2]
                street = ", ".join(parts[:-2])
                return street.strip(), city.strip(), state.strip(), postal.strip()

            elif city_part:
                city = city_part
                street = ", ".join(parts[:-1])
                return street.strip(), city.strip(), state.strip(), postal.strip()

    if len(parts) > 0:
        street = parts[0]
        if len(parts) > 1:
            city = ", ".join(parts[1:])

    return street.strip(), city.strip(), state.strip(), postal.strip()


def _compose_street_from_parts(rec: dict | None) -> str:
    rec = rec or {}
    street = _clean(rec.get("address_street"))
    if street:
        base = street
    else:
        num = _clean(rec.get("address_street_number"))
        route = _clean(rec.get("address_route"))
        base = " ".join([p for p in [num, route] if p])
    sub = _clean(rec.get("address_subpremise"))
    if sub:
        base = f"{base}, {sub}" if base else sub
    return base


def pd_person_to_customer(person: dict, org: dict | None) -> dict:
    """
    Prefer PERSON address (Details). Fill any missing pieces from ORG.
    """
    name = _clean(person.get("name"))
    phone = _get_nested_field_value(person, "phone")
    email = _get_nested_field_value(person, "email")

    # Person Address Fields
    p_street = _compose_street_from_parts(person)
    p_city = _clean(person.get("address_locality") or person.get("address_city"))
    p_state = _clean(person.get("address_admin_area_level_1") or person.get("address_state"))
    p_zip = _clean(person.get("address_postal_code") or person.get("address_zip"))
    p_addr_full = _clean(person.get("address_formatted_address") or person.get("address"))
    if p_addr_full and not (p_street and p_city and p_state and p_zip):
        s, c, st, z = _parse_us_address(p_addr_full)
        p_street = p_street or s
        p_city = p_city or c
        p_state = p_state or st
        p_zip = p_zip or z

    # Organization Address Fields
    company = _clean((org or {}).get("name"))
    o_street = _compose_street_from_parts(org)
    o_city = _clean((org or {}).get("address_locality") or (org or {}).get("address_city"))
    o_state = _clean((org or {}).get("address_admin_area_level_1") or (org or {}).get("address_state"))
    o_zip = _clean((org or {}).get("address_postal_code") or (org or {}).get("address_zip"))
    o_addr_full = _clean((org or {}).get("address_formatted_address") or (org or {}).get("address"))
    if o_addr_full and not (o_street and o_city and o_state and o_zip):
        s, c, st, z = _parse_us_address(o_addr_full)
        o_street = o_street or s
        o_city = o_city or c
        o_state = o_state or st
        o_zip = o_zip or z

    # Map to Customer (Person address takes precedence for shipping)
    ship_addr1 = p_street or o_street
    ship_city = p_city or o_city
    ship_state = p_state or o_state
    ship_zip = p_zip or o_zip

    return {
        "company": company,
        "name": name,
        "email": email,
        "phone": phone,
        "ship_addr1": ship_addr1, "ship_city": ship_city, "ship_state": ship_state, "ship_zip": ship_zip,
        "bill_addr1": ship_addr1, "bill_city": ship_city, "bill_state": ship_state, "bill_zip": ship_zip,
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
    return int(sum((float(it.get("qty", 0)) for it in items if is_basket_5_7_X(it))))


def find_course_discount_index(items: list[dict]) -> int:
    for idx, it in enumerate(items):
        if (it.get("sku") == "CD") or (it.get("name", "").lower().strip() == "course discount"):
            return idx
    return -1


def ensure_course_discount(items: list[dict]) -> None:
    qty = eligible_qty_for_discount(items)
    idx = find_course_discount_index(items)
    DISCOUNT_NOTE = "Auto-applied for 9+ Mach 5/7/X baskets"

    if qty >= 9:
        existing_notes = items[idx]["notes"] if idx != -1 else ""
        note_to_use = existing_notes if (
                existing_notes and not existing_notes.startswith(DISCOUNT_NOTE)) else DISCOUNT_NOTE

        disc_line = {
            "id": items[idx]["id"] if idx != -1 else str(uuid.uuid4()),
            "sku": "CD",
            "name": "Course Discount (-$100 per qualifying basket)",
            "qty": qty,
            "unit": -100.0,
            "total": round(-100.0 * qty, 2),
            "notes": note_to_use,
            "prev_sku": "CD",
        }
        if idx == -1:
            items.append(disc_line)
        else:
            items[idx] = disc_line
    elif idx != -1:
        items.pop(idx)


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
        if COMPANY_LOGO_PATH and os.path.exists(COMPANY_LOGO_PATH):
            logo = Image(COMPANY_LOGO_PATH, width=1.8 * inch, height=1.0 * inch)
            logo.hAlign = 'LEFT'
            company_info_block = _company_right_block(styles)
            left_block = [logo, Spacer(1, 4), company_info_block]

            hdr = Table([[left_block, ""]], colWidths=[3.75 * inch, 3.75 * inch])
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

        # Display only the Order Document # (doc_number)
        story += [
            Paragraph(f"**ORDER: {doc_number}**", styles['Heading2']),
            Spacer(1, 4)
        ]

        # --- FIX: Removed "Source Quote Number" line entirely from display ---
        # The previous attempt was to remove it from meta, but it was still being rendered from meta
        # This line is now completely gone:
        # story += [Paragraph(f"**Source Quote Number:** {meta.get('source_quote_number', '')}", styles['LeftInfo'])]
        # ----------------------------------------------------

        grouped_info_text = (
            f"Date: {datetime.now().strftime('%m/%d/%y')}<br/>"
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

        bill_block_order = (
            f"<b>Billing Address</b><br/>"
            f"{customer.get('company', '')}<br/>"
            f"{customer.get('name', '')}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('phone', '')}<br/>"
            f"{customer.get('email', '')}"
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
            if float(r.get("qty", 0)) == 0:
                continue
            desc_para = Paragraph(str(r["name"]),
                                  ParagraphStyle('Desc', parent=styles['Normal'], fontSize=9, leading=11))
            data.append([str(r["qty"]), desc_para, fmt_money(float(r['unit'])) if float(r['unit']) >= 0 else fmt_money(float(r['unit'])), fmt_money(float(r['total']))])
            note_txt = (r.get("notes") or "").strip()
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

        freight_notes_txt = st.session_state.get("freight_notes", "").strip()
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

        if COMPANY_LOGO_PATH and os.path.exists(COMPANY_LOGO_PATH):
            logo = Image(COMPANY_LOGO_PATH, width=1.8 * inch, height=1.0 * inch)
            logo.hAlign = 'LEFT'
            left_logo_block_elements = [logo, Spacer(1, 4), company_info_para]
        else:
            left_logo_block_elements = [company_info_para]

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

        date_quote_info = (
            f"Date: {datetime.now().strftime('%Y-%m-%d')}<br/>"
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

        bill_block = (
            f"<b>Billing Address</b><br/>"
            f"{customer.get('company', '')}<br/>"
            f"{customer.get('name', '')}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('phone', '')}<br/>"
            f"{customer.get('email', '')}"
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
            if float(r.get("qty", 0)) == 0: continue
            desc_para = Paragraph(str(r["name"]),
                                  ParagraphStyle('Desc', parent=styles['Normal'], fontSize=9, leading=11))
            data.append([str(r["qty"]), desc_para, fmt_money(float(r['unit'])) if float(r['unit']) >= 0 else fmt_money(float(r['unit'])), fmt_money(float(r['total']))])
            note_txt = (r.get("notes") or "").strip()
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

        freight_notes_txt = st.session_state.get("freight_notes", "").strip()
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
            ["Pole Extension - New Product", fmt_money(60.00)],
            ["Basket Flag - New Product", fmt_money(30.00)],
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

    doc.build(story)
    return buffer.getvalue()


# =============================================================================
# 5. Main Application Logic
# =============================================================================

def main_app():
    """Contains all the original quoting tool functionality."""

    st.title("DGA Quoting Tool")
    st.caption("Local product DB • Pipedrive Lookup • Auto Course Discount • PDF export")

    # --- RERUN CHECK FOR UNIT PRICE FIX ---
    if st.session_state["rerun_flag"]:
        st.session_state["rerun_flag"] = False
        st.rerun()

    # (UI for Quote Lookup/New Quote)
    lookup_col1, lookup_col2, lookup_col3, lookup_col4 = st.columns([1, 1.2, 0.4, 0.4])

    # Set the key suffix for all customer inputs
    cust_key_suffix = st.session_state["customer_key_suffix"]

    with lookup_col1:
        st.markdown("**Current Quote #**")
        st.info(st.session_state["quote_no"])

    with lookup_col2:
        # --- QUOTE LOOKUP CHANGE: Load from Sheet and display in Selectbox ---
        all_quotes_df = load_all_quotes()
        # Create display options: (New Quote) + all saved Quote #s
        # Handle case where load_all_quotes returns empty DF due to error
        quote_options = ["(New Quote)"] + all_quotes_df['Quote #'].tolist() if 'Quote #' in all_quotes_df.columns else [
            "(New Quote)"]

        selected_quote_no = st.selectbox("Select or Search for Quote #", quote_options)

    with lookup_col3:
        if st.button("Retrieve", use_container_width=True, key="btn_retrieve_quote"):
            if selected_quote_no != "(New Quote)":
                st.session_state["quote_no"] = selected_quote_no

                # --- RETRIEVAL LOGIC CHANGE: Load from DataFrame (which came from Google Sheets) ---
                try:
                    # Find the row in the DataFrame corresponding to the selected Quote #
                    target_row = all_quotes_df[all_quotes_df['Quote #'] == selected_quote_no].iloc[0]
                    payload = target_row['Payload']  # Access the pre-parsed JSON payload

                    # Apply payload data to session state
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
                                                                             datetime.now().strftime('%m/%d/%y'))

                    # Use the loaded 'order_doc_number' if available, otherwise default to the quote number
                    loaded_doc_number = order_meta.get("order_doc_number", st.session_state["quote_no"])
                    # Ensure it defaults to the loaded quote number if blank:
                    st.session_state["order_doc_number_pdf"] = loaded_doc_number or st.session_state["quote_no"]

                    # CUSTOMER AUTOFILL FIX: Increment the key suffix to force widget reset
                    st.session_state["customer_key_suffix"] += 1

                    st.success(f"Loaded quote {st.session_state['quote_no']} from Google Sheets.")
                    st.rerun()

                except IndexError:
                    st.error(f"Quote {selected_quote_no} not found in the loaded data.")
                except Exception as e:
                    st.error(f"Couldn't load quote {selected_quote_no} from Google Sheets: {e}")
            else:
                st.warning("Please select a quote to retrieve or click 'New Quote'.")

    with lookup_col4:
        if st.button("New Quote", use_container_width=True, type="secondary"):
            start_new_quote()

    # (UI for Customer Info)
    c = st.session_state["customer"]

    st.subheader("Customer Information")

    # Pipedrive Lookup
    with st.expander("Pipedrive lookup (by email or name)", expanded=False):
        if not PIPEDRIVE_API_TOKEN:
            st.warning("Pipedrive API Token not configured in environment variables. Lookup disabled.")
        else:
            term = st.text_input("Search term", placeholder="e.g. jane@city.gov or Jane Smith", key="pd_term")
            if st.button("Search Pipedrive", key="pd_search_btn") and term.strip():
                try:
                    st.session_state["pd_matches"] = pd_search_persons(term.strip())
                except Exception as e:
                    st.error(f"Search failed due to unexpected error. Check console: {e}")
                    st.session_state["pd_matches"] = []

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
            elif "pd_matches" in st.session_state and st.session_state["pd_matches"] == []:
                st.info("No Pipedrive contacts found matching the search term.")

    # Customer Info Inputs
    with st.container(border=True):
        cols_addr = st.columns(2)

        # --- SHIPPING ADDRESS (LEFT COLUMN) ---
        with cols_addr[0]:
            st.subheader("Shipping Address")
            # NOTE: All customer keys now include the dynamic suffix
            c["company"] = st.text_input("Company", value=c.get("company", ""), key=f"ship_company_{cust_key_suffix}")
            c["name"] = st.text_input("Name", value=c.get("name", ""), key=f"ship_contact_name_{cust_key_suffix}")
            c["phone"] = st.text_input("Phone", value=c.get("phone", ""), key=f"ship_phone_{cust_key_suffix}")
            c["email"] = st.text_input("Email", value=c.get("email", ""), key=f"ship_email_{cust_key_suffix}")
            c["ship_addr1"] = st.text_area("Address (Ship)", value=c.get("ship_addr1", ""),
                                           key=f"ship_addr1_{cust_key_suffix}")
            sc1, sc2, sc3 = st.columns(3)
            c["ship_city"] = sc1.text_input("City", value=c.get("ship_city", ""),
                                            key=f"ship_city_input_{cust_key_suffix}")
            c["ship_state"] = sc2.text_input("State", value=c.get("ship_state", ""),
                                             key=f"ship_state_input_{cust_key_suffix}")
            c["ship_zip"] = sc3.text_input("Zip", value=c.get("ship_zip", ""), key=f"ship_zip_input_{cust_key_suffix}")

        # --- BILLING ADDRESS (RIGHT COLUMN) ---
        with cols_addr[1]:
            st.subheader("Billing Address")

            # Dummy inputs for alignment
            st.text_input("Company", value="", disabled=True, label_visibility="hidden",
                          key=f"bill_dummy_comp_{cust_key_suffix}")
            st.text_input("Name", value="", disabled=True, label_visibility="hidden",
                          key=f"bill_dummy_name_{cust_key_suffix}")
            st.text_input("Phone", value="", disabled=True, label_visibility="hidden",
                          key=f"bill_dummy_phone_{cust_key_suffix}")
            st.text_input("Email", value="", disabled=True, label_visibility="hidden",
                          key=f"bill_dummy_email_{cust_key_suffix}")

            # Now the main address text area should align
            c["bill_addr1"] = st.text_area("Address (Bill)", value=c.get("bill_addr1", ""),
                                           key=f"bill_addr1_{cust_key_suffix}")
            bc1, bc2, bc3 = st.columns(3)
            c["bill_city"] = bc1.text_input("City", value=c.get("bill_city", ""),
                                            key=f"bill_city_input_{cust_key_suffix}")
            c["bill_state"] = bc2.text_input("State", value=c.get("bill_state", ""),
                                             key=f"bill_state_input_{cust_key_suffix}")
            c["bill_zip"] = bc3.text_input("Zip", value=c.get("bill_zip", ""), key=f"bill_zip_input_{cust_key_suffix}")

    st.divider()

    # 2) Line Items
    st.subheader("Line Items")

    def add_item(default_sku: str = ""):
        st.session_state["line_items"].append({
            "id": str(uuid.uuid4()),
            "sku": default_sku,
            "name": "",
            "qty": 1,
            "unit": 0.0,
            "total": 0.0,
            "notes": "",
            "prev_sku": "",
        })

    if st.button("Add Line Item", key="btn_add_line"):
        add_item()

    remove_ids = []
    sku_to_name = PRODUCTS.set_index('SKU')['Name'].to_dict()
    sku_options_display = ["(custom)"] + [f"{s} — {sku_to_name.get(s, 'No Name')}" for s in PRODUCTS["SKU"].tolist()]

    for i, row in enumerate(st.session_state["line_items"]):
        st.markdown(f"**Item {i + 1}**")
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
            sku_selected_display = st.selectbox("Product Description", sku_options_display, index=sel_idx,
                                                key=f"sku_select_{row['id']}")

            # --- UNIT PRICE AUTOFILL LOGIC ---
            new_sku = ""
            new_name = prod_name
            new_unit = prod_price

            if sku_selected_display == "(custom)":
                new_sku = ""
                new_name = prod_name
                new_unit = prod_price
            else:
                parts = sku_selected_display.split('—', 1)
                new_sku = parts[0].strip()

                prod = PRODUCTS[PRODUCTS["SKU"] == new_sku]
                if not prod.empty:
                    new_name = str(prod.iloc[0]["Name"])
                    new_unit = float(prod.iloc[0]["UnitPrice"]) if pd.notna(prod.iloc[0]["UnitPrice"]) else 0.0
                else:
                    new_name = parts[1].strip() if len(parts) > 1 else new_sku
                    new_unit = prod_price

            if new_sku != row["sku"]:
                row["sku"] = new_sku
                row["name"] = new_name
                row["unit"] = new_unit
                row["prev_sku"] = new_sku if new_sku else "(custom)"
                # Set the flag to trigger a rerun on the next loop
                st.session_state["rerun_flag"] = True

            if not row["sku"]:
                row["name"] = st.text_input("Custom Name (Required)", value=row["name"], key=f"name_input_{row['id']}")
            # ---------------------------------------------

        with c2:
            row["qty"] = st.number_input("Qty", min_value=0, value=int(row.get("qty", 1)), step=1,
                                         key=f"qty_input_{row['id']}")

        with c3:
            current_unit = float(row.get("unit", 0.0) if pd.notna(row.get("unit", 0.0)) else 0.0)

            # UNIT PRICE AUTOFILL FIX: Dynamic Key including SKU forces widget reset when SKU changes
            row["unit"] = st.number_input("Unit Price", min_value=-100000.0, value=current_unit, step=0.01,
                                          format="%.2f",
                                          key=f"unit_input_{row['id']}_{row['sku'] or 'custom'}")

        with c4:
            row["total"] = round(float(row["qty"]) * float(row["unit"]), 2)
            st.write(f"**${row['total']:,.2f}**")

        row["notes"] = st.text_area("Notes (optional)", value=row.get("notes", ""), key=f"notes_input_{row['id']}")
        if st.button("Remove", key=f"rm_btn_{row['id']}"):
            remove_ids.append(row["id"])
        st.divider()

    if remove_ids:
        st.session_state["line_items"] = [r for r in st.session_state["line_items"] if r["id"] not in remove_ids]
        st.rerun()

    ensure_course_discount(st.session_state["line_items"])

    if st.button("Add Line Item", key="btn_add_line_bottom"):
        add_item()

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

    subtotal = sum(float(r["total"]) for r in st.session_state["line_items"])
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
        st.success(f"Course Discount active: -$100 × {qual_qty} qualifying baskets.")
    else:
        st.info(
            f"Qualifying baskets: {qual_qty}. Add {max(0, 9 - qual_qty)} more Mach 5/7/X (Std/Portable/No Frills) to trigger the Course Discount.")

    st.info("Note: International customers will be responsible for all duties and taxes upon delivery.")
    st.divider()

    # 4) Generate PDF Quote + Order PDF
    st.subheader("Generate PDF Documents")

    # --- FIX: REMOVED QUOTE # INPUT FIELD ---
    quote_no = st.session_state["quote_no"] # Use the canonical value
    st.markdown(f"**Quote #:** `{quote_no}`")
    # ----------------------------------------

    footer_notes = st.text_area("Footer Notes (shown on PDF)", value=st.session_state["footer_notes"],
                                key="footer_notes_input")

    # Order/PO Details Section
    with st.expander("Order/PO Details (for Order PDF)", expanded=False):
        # Seed the order doc number to the current quote if empty/missing
        if not st.session_state.get("order_doc_number_pdf"):
            st.session_state["order_doc_number_pdf"] = st.session_state["quote_no"]

        order_col1, order_col2 = st.columns(2)
        with order_col1:
            st.text_input(
                "Order/PO Document #",
                key="order_doc_number_pdf", # Binds directly to the session key
            )
            st.text_input(
                "P.O. Number",
                key="order_po_number",      # Binds directly to the session key
            )
            st.text_input(
                "Operator",
                key="order_operator",       # Binds directly to the session key
            )
            st.text_input(
                "Terms",
                key="order_terms",          # Binds directly to the session key
            )
        with order_col2:
            st.text_input(
                "Commission To",
                key="order_comm_to",        # Binds directly to the session key
            )
            st.text_input(
                "Check Number",
                key="order_check_number",   # Binds directly to the session key
            )
            st.text_input(
                "Date Received",
                key="order_date_received",  # Binds directly to the session key
            )

    # Re-assemble order_meta using session state values
    order_meta = {
        "order_doc_number": st.session_state["order_doc_number_pdf"],
        "po_number": st.session_state["order_po_number"],
        "operator": st.session_state["order_operator"],
        "terms": st.session_state["order_terms"],
        "commission_to": st.session_state["order_comm_to"],
        "check_number": st.session_state["order_check_number"],
        "date_received": st.session_state["order_date_received"],
        # Crucial: Save the actual quote number used to create this order/payload
        "source_quote_number": st.session_state["quote_no"]
    }

    # --- Generate and Save Quote Logic (MODIFIED FOR SHEETS) ---
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
        "date": datetime.now().isoformat(),
        "customer": st.session_state["customer"],
        "line_items": st.session_state["line_items"],
        "fees": fees,
        "totals": totals,
        "tax_meta": tax_meta,
        "freight_notes": st.session_state["freight_notes"],
        "footer_notes": footer_notes,
        "order_meta": order_meta,  # --- Save Order/PO Details to Payload ---
    }

    # --- PDF Buttons ---
    pdf_col1, pdf_col2 = st.columns(2)

    if pdf_col1.button("Generate & SAVE Quote PDF", use_container_width=True, type="primary"):
        pdf_buffer = io.BytesIO()
        pdf_data = build_pdf(
            pdf_buffer, st.session_state["customer"], st.session_state["line_items"], fees, totals,
            quote_no, footer_notes, template="quote"
        )

        # --- NEW PERSISTENCE STEP: SAVE TO GOOGLE SHEET ---
        if save_quote_to_gsheet(payload):
            st.success(f"Quote **{quote_no}** successfully saved to **Google Sheets** and PDF generated.")
            st.download_button(
                label="Download Quote PDF",
                data=pdf_data,
                file_name=f"{quote_no}_Quote.pdf",
                mime="application/pdf",
                key="download_quote_pdf",
            )
        else:
            st.error(
                "Quote PDF generated but **FAILED to save** to Google Sheets. Check Sheet configuration and sharing permissions.")

    if pdf_col2.button("Process as Order / PO", use_container_width=True, type="secondary"):
        # The 'order_doc_number' is the number the user wants on the file name/header
        order_doc_number = st.session_state["order_doc_number_pdf"]
        order_file_name = f"{order_doc_number}_Order.pdf"

        pdf_buffer_order = io.BytesIO()
        pdf_data_order = build_pdf(
            pdf_buffer_order, st.session_state["customer"], st.session_state["line_items"], fees, totals,
            order_doc_number, footer_notes, template="order", meta=order_meta
        )

        # NEW: persist order_meta with the quote so re-loads remember it
        payload["order_meta"] = order_meta
        _saved = save_quote_to_gsheet(payload) # safe even if row already exists; appends a new row

        # UPDATED SUCCESS MESSAGE:
        st.success(
            f"Order **{order_doc_number}** PDF generated, based on Quote **{st.session_state['quote_no']}**."
        )

        st.download_button(
            label="Download Order/PO PDF",
            data=pdf_data_order,
            file_name=order_file_name,
            mime="application/pdf",
            key="download_order_pdf",
        )


# =============================================================================
# 6. Main App Entry Point
# =============================================================================
if __name__ == '__main__':
    main_app()