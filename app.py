import os
import io
import uuid
import json
import copy
import concurrent.futures
import html
from datetime import datetime
import requests
import re
import sys
from typing import Any
import pytz
import html.parser
import base64
from urllib.parse import urlencode

import pandas as pd
import streamlit as st
import gspread
from gspread.utils import rowcol_to_a1

try:
    from PyPDF2 import PdfReader
except ImportError:
    PdfReader = None

try:
    import fitz
except ImportError:
    fitz = None

st.set_page_config(page_title="DGA Quoting Tool", layout="wide", initial_sidebar_state="expanded")


def is_health_check_request() -> bool:
    try:
        health_value = st.query_params.get("health", "")
    except Exception:
        return False

    return str(health_value).strip().lower() in {"1", "true", "yes", "ok"}


def render_health_check() -> None:
    checked_at = datetime.now(pytz.timezone("America/Los_Angeles")).strftime("%Y-%m-%d %I:%M:%S %p %Z")
    st.title("UPTIME_OK")
    st.write("DGA Quote Tool is awake.")
    st.caption(f"Checked at {checked_at}")


from dotenv import load_dotenv
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
)

load_dotenv()


def get_env(key, default=None, cast=str):
    if key in st.secrets:
        val = st.secrets[key]
    else:
        val = os.getenv(key, default)

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
DEFAULT_FOOTER_NOTES = (
    "Pricing subject to change. Please review all details carefully.\n"
    "International customers will be responsible for all duties and taxes upon delivery."
)
DEFAULT_TAX = float(get_env("SALES_TAX_RATE_DEFAULT", 0.0, float))
SANTA_CRUZ_TAX_RATE = 0.0975
US_STATE_ABBREVIATIONS = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}

PIPEDRIVE_DOMAIN = get_env("PIPEDRIVE_API_URL")
PIPEDRIVE_API_TOKEN = get_env("PIPEDRIVE_API_TOKEN")

if PIPEDRIVE_DOMAIN:
    PIPEDRIVE_BASE_URL = PIPEDRIVE_DOMAIN.rstrip("/") + "/v1"
else:
    PIPEDRIVE_BASE_URL = None

GOOGLE_SHEET_ID = "1oR2I5lmxYNhAc4rT1kalzVwop2UJOnGjTkY3eTVzv80"

FREIGHT_NOTE_OPTIONS = [
    "Business Address",
    "Residential Address",
    "Lift Gate Needed",
    "Fork Lift Access",
    "Loading Dock Access",
    "Local Pickup",
    "UPS",
    "Ground Freight",
]

MANAGER_USERNAME = "CZ"
MANAGER_PASSWORD = "272188"


@st.cache_resource(ttl=None)
def _get_logo_path_robustly(default_path: str = "assets/dga_logo.png") -> str | None:
    logo_path_base = get_env("COMPANY_LOGO_PATH", default_path)

    if os.path.exists(logo_path_base):
        return logo_path_base

    dirname, basename = os.path.split(logo_path_base)

    variations = [
        os.path.join(dirname.capitalize(), basename.capitalize()),
        os.path.join(dirname.lower(), basename.capitalize()),
        os.path.join(dirname.capitalize(), basename.lower()),
    ]

    for path in variations:
        if os.path.exists(path):
            print(f"Found logo at case-adjusted path: {path}", file=sys.stderr)
            return path

    if dirname == "assets":
        root_path = basename
        if os.path.exists(root_path):
            return root_path

    print(f"Logo not found at expected path: {logo_path_base} or common variations.", file=sys.stderr)
    return None


COMPANY_LOGO_PATH = _get_logo_path_robustly()


@st.cache_resource(ttl=None)
def _get_app_logo_path(default_path: str = "assets/dga_logo_white.png") -> str | None:
    app_logo_path = get_env("APP_LOGO_PATH", default_path)
    if os.path.exists(app_logo_path):
        return app_logo_path
    return COMPANY_LOGO_PATH


APP_LOGO_PATH = _get_app_logo_path()
HUB_HEADER_LOGO_PATH = "assets/dga_logo_white.png" if os.path.exists("assets/dga_logo_white.png") else APP_LOGO_PATH
QUOTE_PATENT_TILE_PATH = "assets/ahhhh-whit.png"
WAREHOUSE_QUEUE_URL = get_env("WAREHOUSE_QUEUE_URL", "https://dga-warehouse-inventory.netlify.app")
CUSTOM_DISC_ORDERING_URL = get_env("CUSTOM_DISC_ORDERING_URL", "https://dga-custom-disc-ordering.onrender.com")
ARTWORK_GENERATOR_URL = get_env("ARTWORK_GENERATOR_URL", "https://dga-artwork-preview-generator.streamlit.app")
PDGA_CONTACT_SCRAPER_URL = get_env("PDGA_CONTACT_SCRAPER_URL", "https://dga-scraper-app.streamlit.app")
MACH_FAMILY_FORECASTING_URL = get_env("MACH_FAMILY_FORECASTING_URL", "https://mach-family-po-planner.streamlit.app")
IT_TICKETS_URL = get_env("IT_TICKETS_URL", "https://it-tickets-jigv.onrender.com")
QUOTE_TOOL_IT_TICKETS_URL = f"{IT_TICKETS_URL}?hub_area=Quote%20Tool"
QUOTE_TOOL_URL = get_env("QUOTE_TOOL_URL", "https://dga-quote-tool-v5.streamlit.app")
OPERATIONS_HUB_URL = get_env("OPERATIONS_HUB_URL", "https://dga-operations.streamlit.app")
WAREHOUSE_STATE_URL = get_env("WAREHOUSE_STATE_URL", f"{WAREHOUSE_QUEUE_URL.rstrip('/')}/.netlify/functions/warehouse-load")

def fmt_money(value: float) -> str:
    return f"${value:,.2f}"


@st.cache_resource(ttl=None)
def _asset_data_uri(path: str, mime_type: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    with open(path, "rb") as asset_file:
        encoded = base64.b64encode(asset_file.read()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _freight_note_key(label: str) -> str:
    slug = label.lower().replace(" ", "_").replace("-", "_")
    return f"freight_note_{slug}"


def sync_freight_checkboxes_from_text(text: str):
    text_upper = (text or "").upper()

    for label in FREIGHT_NOTE_OPTIONS:
        st.session_state[_freight_note_key(label)] = label.upper() in text_upper

    remaining = text or ""
    for label in FREIGHT_NOTE_OPTIONS:
        remaining = re.sub(re.escape(label), "", remaining, flags=re.IGNORECASE)

    remaining = re.sub(r"\s*,\s*,+", ", ", remaining)
    remaining = re.sub(r"^\s*,\s*|\s*,\s*$", "", remaining).strip()
    st.session_state["freight_notes_other"] = remaining


def get_selected_freight_notes() -> str:
    selected = [
        label for label in FREIGHT_NOTE_OPTIONS
        if st.session_state.get(_freight_note_key(label), False)
    ]

    other = st.session_state.get("freight_notes_other", "").strip()

    if selected and other:
        return ", ".join(selected + [other])
    if selected:
        return ", ".join(selected)
    return other


def get_discount_label(discount_type: str) -> str:
    if discount_type == "team":
        return "Team Discount"
    if discount_type == "commission":
        return "Commission Discount"
    if discount_type == "discount":
        note = st.session_state.get("discount_note", "").strip()
        return f"{note} Discount" if note else "Discount"
    return ""


def sync_discount_checkboxes_from_type(discount_type: str):
    st.session_state["team_discount_checkbox"] = discount_type == "team"
    st.session_state["commission_discount_checkbox"] = discount_type == "commission"
    st.session_state["discount_checkbox"] = discount_type == "discount"
    st.session_state["active_discount_type"] = discount_type


def handle_team_discount_toggle():
    if st.session_state.get("team_discount_checkbox", False):
        st.session_state["commission_discount_checkbox"] = False
        st.session_state["discount_checkbox"] = False
        st.session_state["active_discount_type"] = "team"
    elif st.session_state.get("active_discount_type") == "team":
        st.session_state["active_discount_type"] = ""


def handle_commission_discount_toggle():
    if st.session_state.get("commission_discount_checkbox", False):
        st.session_state["team_discount_checkbox"] = False
        st.session_state["discount_checkbox"] = False
        st.session_state["active_discount_type"] = "commission"
    elif st.session_state.get("active_discount_type") == "commission":
        st.session_state["active_discount_type"] = ""


def handle_discount_toggle():
    if st.session_state.get("discount_checkbox", False):
        st.session_state["team_discount_checkbox"] = False
        st.session_state["commission_discount_checkbox"] = False
        st.session_state["active_discount_type"] = "discount"
    elif st.session_state.get("active_discount_type") == "discount":
        st.session_state["active_discount_type"] = ""
        st.session_state["discount_note"] = ""


def clear_manager_credentials():
    st.session_state["manager_username"] = ""
    st.session_state["manager_password"] = ""


def validate_manager_credentials() -> bool:
    username = st.session_state.get("manager_username", "").strip()
    password = st.session_state.get("manager_password", "").strip()
    return username == MANAGER_USERNAME and password == MANAGER_PASSWORD


def handle_manager_pricing_toggle():
    if not st.session_state.get("manager_pricing_checkbox", False):
        st.session_state["manager_pricing_authorized"] = False
        clear_manager_credentials()


def authorize_manager_pricing():
    if validate_manager_credentials():
        st.session_state["manager_pricing_authorized"] = True
        st.session_state["manager_clear_credentials_on_rerun"] = True
        st.rerun()
    else:
        st.session_state["manager_pricing_authorized"] = False


def calculate_discountable_subtotal(items: list[dict]) -> float:
    total = 0.0
    for item in items:
        if not item.get("previewChecked", True):
            continue

        if item.get("sku") == "CD":
            total += float(item.get("total", 0.0))
            continue

        if item.get("exclude_from_10_discount", False):
            continue

        line_total = float(item.get("total", 0.0))
        if line_total <= 0:
            continue

        total += line_total

    return round(max(total, 0.0), 2)


def calculate_primary_discount(items: list[dict], discount_type: str) -> float:
    if not discount_type:
        return 0.0
    return round(calculate_discountable_subtotal(items) * 0.10, 2)


def calculate_manager_discount(discountable_base: float, enabled: bool) -> float:
    if not enabled:
        return 0.0
    return round(max(discountable_base, 0.0) * 0.05, 2)


@st.cache_resource(ttl=3600)
def get_gsheet_client():
    try:
        if os.path.exists("service_account.json"):
            return gspread.service_account(filename="service_account.json")

        if "gcp_service_account" in st.secrets:
            creds_data = st.secrets["gcp_service_account"]

            if isinstance(creds_data, str):
                try:
                    sa_creds = json.loads(creds_data)
                except json.JSONDecodeError:
                    st.error("Secret format error: gcp_service_account is a string but not valid JSON.")
                    return None
            else:
                sa_creds = dict(creds_data)

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
    client = get_gsheet_client()
    if not client:
        return empty_saved_quotes_df()

    def _fetch_quotes() -> pd.DataFrame:
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.get_worksheet(0)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        return normalize_saved_quotes_df(df)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_fetch_quotes)
    try:
        return future.result(timeout=8)
    except concurrent.futures.TimeoutError:
        future.cancel()
        st.warning("Saved quotes are taking too long to load right now. The page will open without them.")
        return empty_saved_quotes_df()
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found. Check ID and sharing.")
        return empty_saved_quotes_df()
    except Exception as e:
        if is_sheets_rate_limit_error(e):
            st.warning("Google Sheets is rate-limiting saved-quote reads right now. The app will keep working without a fresh saved-quotes sync for the moment.")
        else:
            st.error(f"Error loading quotes from sheet: {e}")
        return empty_saved_quotes_df()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


SAVED_QUOTE_HEADERS = [
    "Quote #",
    "Date",
    "Company",
    "Name",
    "Email",
    "Grand Total",
    "Quote JSON Payload",
    "Record Type",
    "Order #",
    "Source Quote #",
]


def empty_saved_quotes_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=SAVED_QUOTE_HEADERS + ["Payload", "Explicit Record Type", "Doc #"]
    )


def get_saved_quotes_snapshot(force_refresh: bool = False) -> pd.DataFrame:
    snapshot_key = "saved_quotes_snapshot_df"
    cached_snapshot = st.session_state.get(snapshot_key)

    if not force_refresh and isinstance(cached_snapshot, pd.DataFrame):
        return cached_snapshot.copy()

    latest_df = load_all_quotes()
    if not latest_df.empty:
        st.session_state[snapshot_key] = latest_df.copy()
        return latest_df

    if isinstance(cached_snapshot, pd.DataFrame):
        st.caption("Using the most recent saved-quotes snapshot while Google Sheets is rate-limited.")
        return cached_snapshot.copy()

    return latest_df


def is_sheets_rate_limit_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return "quota exceeded" in message or "[429]" in message or "read requests per minute per user" in message


def _worksheet_headers(worksheet) -> list[str]:
    return [str(value).strip() for value in worksheet.row_values(1) if str(value).strip()]


def ensure_saved_quote_headers(worksheet) -> list[str]:
    headers = _worksheet_headers(worksheet)
    if not headers:
        headers = SAVED_QUOTE_HEADERS.copy()
        end_cell = rowcol_to_a1(1, len(headers))
        worksheet.update(range_name=f"A1:{end_cell}", values=[headers])
        return headers

    updated_headers = headers.copy()
    for header in SAVED_QUOTE_HEADERS:
        if header not in updated_headers:
            updated_headers.append(header)

    if updated_headers != headers:
        end_cell = rowcol_to_a1(1, len(updated_headers))
        worksheet.update(range_name=f"A1:{end_cell}", values=[updated_headers])

    return updated_headers


def _parse_saved_payload(value: Any) -> dict | None:
    if not value:
        return None

    try:
        return json.loads(value) if isinstance(value, str) else value
    except Exception:
        return None


def _infer_record_type(payload: dict | None, stored_type: str, order_number: str, source_quote_number: str, doc_number: str) -> str:
    if stored_type in {"quote", "order"}:
        return stored_type

    if order_number:
        return "order"

    if isinstance(payload, dict):
        payload_quote_no = str(payload.get("quote_no", "") or "").strip()
        payload_order_no = str(payload.get("order_meta", {}).get("order_doc_number", "") or "").strip()
        if payload_order_no and payload_quote_no and payload_order_no != payload_quote_no:
            return "order"
        if source_quote_number and doc_number and source_quote_number != doc_number:
            return "order"

    return "quote"


def normalize_saved_quotes_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_saved_quotes_df()

    working_df = df.copy()
    working_df.columns = [str(col).strip() for col in working_df.columns]

    if "Quote JSON Payload" not in working_df.columns:
        st.error("Google Sheet missing required column: 'Quote JSON Payload'.")
        return empty_saved_quotes_df()

    for col in ["Quote #", "Record Type", "Order #", "Source Quote #", "Date"]:
        if col not in working_df.columns:
            working_df[col] = ""

    working_df["Payload"] = working_df["Quote JSON Payload"].apply(_parse_saved_payload)
    working_df = working_df.dropna(subset=["Payload"]).copy()

    working_df["Quote #"] = working_df["Quote #"].fillna("").astype(str).str.strip()
    working_df["Order #"] = working_df["Order #"].fillna("").astype(str).str.strip()
    working_df["Source Quote #"] = working_df["Source Quote #"].fillna("").astype(str).str.strip()
    working_df["Record Type"] = working_df["Record Type"].fillna("").astype(str).str.strip().str.lower()
    working_df["Explicit Record Type"] = working_df["Record Type"]

    working_df["Source Quote #"] = working_df.apply(
        lambda row: row["Source Quote #"]
        or str((row["Payload"] or {}).get("order_meta", {}).get("source_quote_number", "") or "").strip()
        or str((row["Payload"] or {}).get("quote_no", "") or "").strip(),
        axis=1,
    )

    working_df["Order #"] = working_df.apply(
        lambda row: row["Order #"]
        or str((row["Payload"] or {}).get("order_meta", {}).get("order_doc_number", "") or "").strip(),
        axis=1,
    )

    working_df["Doc #"] = working_df.apply(
        lambda row: row["Order #"] or row["Quote #"] or row["Source Quote #"],
        axis=1,
    )

    working_df["Record Type"] = working_df.apply(
        lambda row: _infer_record_type(
            row.get("Payload"),
            row.get("Record Type", ""),
            row.get("Order #", ""),
            row.get("Source Quote #", ""),
            row.get("Doc #", ""),
        ),
        axis=1,
    )

    return working_df


def save_quote_to_gsheet(payload: dict, record_type: str = "quote") -> bool:
    client = get_gsheet_client()
    if not client:
        return False

    try:
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.get_worksheet(0)

        headers = ensure_saved_quote_headers(worksheet)
        quote_number = str(payload.get("quote_no", "") or "").strip()
        source_quote_number = str(payload.get("order_meta", {}).get("source_quote_number", "") or quote_number).strip()
        order_number = str(payload.get("order_meta", {}).get("order_doc_number", "") or "").strip()
        doc_number = order_number if record_type == "order" and order_number else quote_number

        row_map = {
            "Quote #": doc_number,
            "Date": payload.get("date"),
            "Company": payload.get("customer", {}).get("company", ""),
            "Name": payload.get("customer", {}).get("name", ""),
            "Email": payload.get("customer", {}).get("email", ""),
            "Grand Total": payload.get("totals", {}).get("grand_total", 0.0),
            "Quote JSON Payload": json.dumps(payload),
            "Record Type": record_type,
            "Order #": order_number if record_type == "order" else "",
            "Source Quote #": source_quote_number,
        }

        row_data = [row_map.get(header, "") for header in headers]

        worksheet.append_row(row_data, value_input_option="USER_ENTERED")
        load_all_quotes.clear()
        st.session_state.pop("saved_quotes_snapshot_df", None)
        return True
    except Exception as e:
        st.error(f"Error saving quote to sheet: {e}")
        return False


def _quote_customer_search_blob(payload: dict, row: pd.Series | None = None) -> str:
    customer = payload.get("customer", {}) if isinstance(payload, dict) else {}
    row_doc_no = row.get("Doc #", "") if row is not None else ""
    row_quote_no = row.get("Quote #", "") if row is not None else ""
    row_order_no = row.get("Order #", "") if row is not None else ""
    row_source_quote_no = row.get("Source Quote #", "") if row is not None else ""
    parts = [
        row_doc_no,
        row_quote_no,
        row_order_no,
        row_source_quote_no,
        customer.get("company", ""),
        customer.get("name", ""),
        customer.get("email", ""),
        customer.get("phone", ""),
        customer.get("bill_company", ""),
        customer.get("bill_name", ""),
        customer.get("bill_email", ""),
        customer.get("bill_phone", ""),
    ]
    return " ".join(str(part).strip().lower() for part in parts if part)


def search_saved_quotes(df: pd.DataFrame, term: str) -> pd.DataFrame:
    search_term = (term or "").strip().lower()
    if df.empty or not search_term:
        return pd.DataFrame()

    working_df = df.copy()
    working_df["Search Blob"] = working_df.apply(lambda row: _quote_customer_search_blob(row.get("Payload"), row), axis=1)
    matches = working_df[working_df["Search Blob"].str.contains(search_term, na=False)].copy()

    if "Date" in matches.columns:
        matches = matches.sort_values(by="Date", ascending=False, na_position="last")

    return matches


def format_saved_quote_match(row: pd.Series) -> str:
    payload = row.get("Payload", {}) or {}
    customer = payload.get("customer", {}) if isinstance(payload, dict) else {}
    doc_number = row.get("Doc #", row.get("Quote #", ""))
    date_text = str(row.get("Date", "") or "")[:10]
    company = customer.get("company", "") or customer.get("bill_company", "")
    name = customer.get("name", "") or customer.get("bill_name", "")
    email = customer.get("email", "") or customer.get("bill_email", "")

    details = " | ".join(part for part in [company, name, email] if part)
    if date_text and details:
        return f"{doc_number} | {date_text} | {details}"
    if details:
        return f"{doc_number} | {details}"
    return str(doc_number)


def build_processed_order_search_blob(row: pd.Series) -> str:
    payload = row.get("Payload", {}) or {}
    customer = payload.get("customer", {}) if isinstance(payload, dict) else {}
    parts = [
        row.get("Doc #", ""),
        row.get("Order #", ""),
        row.get("Source Quote #", ""),
        row.get("Date", ""),
        row.get("Company", ""),
        row.get("Name", ""),
        row.get("Email", ""),
        customer.get("company", ""),
        customer.get("name", ""),
        customer.get("email", ""),
        customer.get("bill_company", ""),
        customer.get("bill_name", ""),
        customer.get("bill_email", ""),
    ]
    return " ".join(str(part or "").strip().lower() for part in parts)


def format_processed_order_label(row: pd.Series) -> str:
    payload = row.get("Payload", {}) or {}
    customer = payload.get("customer", {}) if isinstance(payload, dict) else {}
    order_no = str(row.get("Order #", "") or row.get("Doc #", "") or "").strip()
    date_text = str(row.get("Date", "") or "")[:10]
    company = (
        str(row.get("Company", "") or "").strip()
        or str(customer.get("company", "") or customer.get("bill_company", "") or "").strip()
    )
    name = (
        str(row.get("Name", "") or "").strip()
        or str(customer.get("name", "") or customer.get("bill_name", "") or "").strip()
    )

    pieces = [order_no]
    if company:
        pieces.append(company)
    elif name:
        pieces.append(name)
    if date_text:
        pieces.append(date_text)
    return " - ".join(pieces)


@st.cache_data(ttl=30)
def load_warehouse_status_snapshot() -> dict:
    if not WAREHOUSE_STATE_URL:
        return {"queue_by_order": {}, "applied_orders": set(), "error": "Warehouse status URL is not configured."}

    try:
        response = requests.get(WAREHOUSE_STATE_URL, timeout=6)
        response.raise_for_status()
        payload = response.json()
        state = payload.get("state", {}) if isinstance(payload, dict) else {}
        queue_items = state.get("queueItems", []) if isinstance(state, dict) else []
        applied_orders = state.get("appliedOrders", []) if isinstance(state, dict) else []

        queue_by_order = {}
        for item in queue_items:
            order_number = str(item.get("orderNumber", "") or "").strip()
            if order_number:
                queue_by_order[order_number] = item

        applied_set = {
            str(record.get("orderNumber", "") or "").strip()
            for record in applied_orders
            if str(record.get("orderNumber", "") or "").strip()
        }

        return {"queue_by_order": queue_by_order, "applied_orders": applied_set, "error": ""}
    except Exception as exc:
        return {"queue_by_order": {}, "applied_orders": set(), "error": str(exc)}


def _products_cache_signature(path: str = "products.csv") -> float | None:
    try:
        return os.path.getmtime(path)
    except OSError:
        return None


@st.cache_data
def load_products(path: str = "products.csv", file_signature: float | None = None) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]

        for col in ["SKU", "Name", "UnitPrice"]:
            if col not in df.columns:
                raise ValueError(f"products.csv must have column: {col}")

        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = df[col].str.strip()

        if "Notes" not in df.columns:
            df["Notes"] = ""
        else:
            df["Notes"] = df["Notes"].fillna("").astype(str)

        df["UnitPrice"] = pd.to_numeric(
            df["UnitPrice"].astype(str).str.replace(r"[^0-9.\-]", "", regex=True),
            errors="coerce"
        ).fillna(0.0)

        return df
    except FileNotFoundError:
        st.warning(f"Product file not found at '{path}'. Using minimal placeholder data.")
        return pd.DataFrame({
            "SKU": ["M5-ST", "M7-PT", "M14-CO", "TS-BASIC"],
            "Name": ["Mach 5 Standard Basket", "Mach 7 Portable Basket", "Mach 14 Chain Collar", "Basic Color Tee Sign"],
            "UnitPrice": [499.00, 399.00, 35.00, 55.00],
            "Notes": ["", "", "", ""]
        })


PRODUCTS = load_products(file_signature=_products_cache_signature())


def get_pacific_now():
    pacific_tz = pytz.timezone("America/Los_Angeles")
    return datetime.now(pacific_tz)


def new_quote_number():
    return get_pacific_now().strftime("%m%d-%H%M")


def assign_new_quote_version():
    current_quote_no = st.session_state["quote_no"]
    match = re.match(r"(.+?)(?:-V(\d+))?$", current_quote_no)
    base, version = match.groups() if match else (current_quote_no, None)
    current_version = int(version) if version is not None else 1
    new_version = current_version + 1
    st.session_state["quote_no"] = f"{base}-V{new_version}"
    st.rerun()


def capture_freight_state() -> dict:
    return {
        "freight_fee_input": float(st.session_state.get("freight_fee_input", 0.0) or 0.0),
        "freight_notes": get_selected_freight_notes(),
        "freight_notes_other": st.session_state.get("freight_notes_other", ""),
        "freight_checks": {
            label: bool(st.session_state.get(_freight_note_key(label), False))
            for label in FREIGHT_NOTE_OPTIONS
        },
    }


def restore_freight_state(freight_state: dict):
    st.session_state["freight_fee_input"] = float(freight_state.get("freight_fee_input", 0.0) or 0.0)
    st.session_state["freight_notes"] = freight_state.get("freight_notes", "")
    st.session_state["freight_notes_other"] = freight_state.get("freight_notes_other", "")
    freight_checks = freight_state.get("freight_checks", {})
    for label in FREIGHT_NOTE_OPTIONS:
        st.session_state[_freight_note_key(label)] = bool(freight_checks.get(label, False))


def has_freight_state(freight_state: dict | None = None) -> bool:
    freight_state = freight_state or capture_freight_state()
    return (
        float(freight_state.get("freight_fee_input", 0.0) or 0.0) > 0
        or bool((freight_state.get("freight_notes", "") or "").strip())
        or bool((freight_state.get("freight_notes_other", "") or "").strip())
        or any(bool(v) for v in freight_state.get("freight_checks", {}).values())
    )


def request_new_quote():
    if has_freight_state():
        st.session_state["new_quote_dialog_open"] = True
        st.rerun()
    else:
        start_new_quote()


def start_new_quote(preserve_freight: bool = False):
    freight_state = capture_freight_state() if preserve_freight else None

    st.session_state["customer"] = {
        "company": "", "name": "", "email": "", "phone": "",
        "ship_addr1": "", "ship_city": "", "ship_state": "", "ship_zip": "",
        "bill_company": "", "bill_name": "", "bill_email": "", "bill_phone": "",
        "bill_addr1": "", "bill_city": "", "bill_state": "", "bill_zip": "",
    }
    st.session_state["billing_same_as_shipping"] = False

    st.session_state["line_items"] = []
    st.session_state["drop_fee_input"] = 0.0
    st.session_state["freight_fee_input"] = 0.0
    st.session_state["freight_notes"] = ""
    st.session_state["freight_notes_other"] = ""
    for label in FREIGHT_NOTE_OPTIONS:
        st.session_state[_freight_note_key(label)] = False

    sync_discount_checkboxes_from_type("")
    st.session_state["discount_note"] = ""
    st.session_state["manager_pricing_checkbox"] = False
    st.session_state["manager_pricing_authorized"] = False
    st.session_state["manager_clear_credentials_on_rerun"] = False
    clear_manager_credentials()

    st.session_state["tax_rate_pct_input"] = 0.0
    st.session_state["sc_county_checkbox"] = False
    st.session_state["footer_notes"] = DEFAULT_FOOTER_NOTES
    st.session_state["footer_notes_touched"] = False

    st.session_state["order_doc_number_pdf"] = ""
    st.session_state["order_po_number"] = ""
    st.session_state["order_operator"] = "CZ"
    st.session_state["order_auth_code"] = "AP - "
    st.session_state["order_comm_to"] = ""
    st.session_state["order_check_number"] = ""
    st.session_state["order_date_received"] = ""

    st.session_state["quote_no"] = new_quote_number()
    st.session_state["customer_key_suffix"] += 1

    st.session_state["pd_matches"] = []
    st.session_state["pd_term"] = ""
    st.session_state["pd_expander_state"] = False
    st.session_state["show_pdf_preview"] = True
    st.session_state["show_pdf_preview_touched"] = False
    st.session_state["new_quote_dialog_open"] = False

    if preserve_freight and freight_state:
        restore_freight_state(freight_state)

    st.rerun()


@st.dialog("Start New Quote")
def render_new_quote_dialog():
    freight_state = capture_freight_state()
    freight_amount = float(freight_state.get("freight_fee_input", 0.0) or 0.0)
    freight_notes = freight_state.get("freight_notes", "").strip()

    st.write("You have freight details on this quote. What should happen to them?")
    if freight_amount > 0:
        st.caption(f"Current freight: {fmt_money(freight_amount)}")
    if freight_notes:
        st.caption(f"Current freight notes: {freight_notes}")

    keep_col, clear_col, cancel_col = st.columns(3)
    if keep_col.button("Keep Freight", type="primary", use_container_width=True):
        start_new_quote(preserve_freight=True)
    if clear_col.button("Clear Freight", type="secondary", use_container_width=True):
        start_new_quote(preserve_freight=False)
    if cancel_col.button("Cancel", use_container_width=True):
        st.session_state["new_quote_dialog_open"] = False
        st.rerun()


if "customer" not in st.session_state:
    st.session_state["customer"] = {}

if "line_items" not in st.session_state:
    st.session_state["line_items"] = []

st.session_state.setdefault("rerun_flag", False)
st.session_state.setdefault("customer_key_suffix", 0)
st.session_state.setdefault("quote_no", new_quote_number())
st.session_state.setdefault("footer_notes", DEFAULT_FOOTER_NOTES)
st.session_state.setdefault("footer_notes_touched", False)
st.session_state.setdefault("drop_fee_input", 0.0)
st.session_state.setdefault("freight_fee_input", 0.0)
st.session_state.setdefault("freight_notes", "")
st.session_state.setdefault("freight_notes_other", "")
for label in FREIGHT_NOTE_OPTIONS:
    st.session_state.setdefault(_freight_note_key(label), False)

st.session_state.setdefault("active_discount_type", "")
st.session_state.setdefault("team_discount_checkbox", False)
st.session_state.setdefault("commission_discount_checkbox", False)
st.session_state.setdefault("discount_checkbox", False)
st.session_state.setdefault("discount_note", "")

st.session_state.setdefault("manager_pricing_checkbox", False)
st.session_state.setdefault("manager_pricing_authorized", False)
st.session_state.setdefault("manager_username", "")
st.session_state.setdefault("manager_password", "")
st.session_state.setdefault("manager_clear_credentials_on_rerun", False)

st.session_state.setdefault("tax_rate_pct_input", 0.0)
st.session_state.setdefault("sc_county_checkbox", False)
st.session_state.setdefault("order_doc_number_pdf", "")
st.session_state.setdefault("order_po_number", "")
st.session_state.setdefault("order_operator", "CZ")
st.session_state.setdefault("order_auth_code", "AP - ")
st.session_state.setdefault("order_comm_to", "")
st.session_state.setdefault("order_check_number", "")
st.session_state.setdefault("order_date_received", "")
st.session_state.setdefault("pd_matches", [])
st.session_state.setdefault("pd_expander_state", False)
st.session_state.setdefault("show_pdf_preview", True)
st.session_state.setdefault("show_pdf_preview_touched", False)
st.session_state.setdefault("person_quote_search", "")
st.session_state.setdefault("person_quote_match_label", "")
st.session_state.setdefault("query_preview_loaded", "")
st.session_state.setdefault("quote_workspace_view", "builder")
st.session_state.setdefault("processed_order_search", "")
st.session_state.setdefault("processed_order_selected", "")
st.session_state.setdefault("billing_same_as_shipping", False)
st.session_state.setdefault("new_quote_dialog_open", False)


def sync_billing_from_shipping(customer: dict, cust_key_suffix: int) -> None:
    shipping_to_billing_fields = {
        "bill_company": "company",
        "bill_name": "name",
        "bill_phone": "phone",
        "bill_email": "email",
        "bill_addr1": "ship_addr1",
        "bill_city": "ship_city",
        "bill_state": "ship_state",
        "bill_zip": "ship_zip",
    }
    billing_widget_keys = {
        "bill_company": f"bill_company_{cust_key_suffix}",
        "bill_name": f"bill_name_input_{cust_key_suffix}",
        "bill_phone": f"bill_phone_{cust_key_suffix}",
        "bill_email": f"bill_email_{cust_key_suffix}",
        "bill_addr1": f"bill_addr1_{cust_key_suffix}",
        "bill_city": f"bill_city_input_{cust_key_suffix}",
        "bill_state": f"bill_state_input_{cust_key_suffix}",
        "bill_zip": f"bill_zip_input_{cust_key_suffix}",
    }

    for billing_field, shipping_field in shipping_to_billing_fields.items():
        copied_value = customer.get(shipping_field, "")
        customer[billing_field] = copied_value
        st.session_state[billing_widget_keys[billing_field]] = copied_value


def _pd_get(endpoint: str, params: dict | None = None) -> dict | None:
    if not PIPEDRIVE_API_TOKEN or not PIPEDRIVE_BASE_URL:
        print("Pipedrive API Token or Base URL is missing.", file=sys.stderr)
        return None

    url = f"{PIPEDRIVE_BASE_URL}/{endpoint}"
    _params = {"api_token": PIPEDRIVE_API_TOKEN, "limit": 5, **(params or {})}
    try:
        response = requests.get(url, params=_params, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data["data"] if data and data.get("success") else []
    except Exception as e:
        print(f"Pipedrive API Error at {endpoint}: {e}", file=sys.stderr)
        return []


def _pd_scalar(data: Any) -> Any | None:
    if isinstance(data, dict):
        return data.get("value")
    if isinstance(data, list) and data:
        first_item = data[0]
        return first_item.get("value") if isinstance(first_item, dict) else first_item
    return data


def _pd_preferred_value(data: Any) -> Any | None:
    if isinstance(data, list):
        preferred_items = [item for item in data if isinstance(item, dict) and (item.get("primary") or item.get("primary_flag"))]
        candidate_items = preferred_items or data

        for item in candidate_items:
            value = _pd_preferred_value(item)
            if _clean(value):
                return value
        return None

    if isinstance(data, dict):
        for key in ("value", "label", "name", "id"):
            value = data.get(key)
            if _clean(value):
                return value
        return None

    return data


@st.cache_data(ttl=3600, show_spinner=False)
def _pd_org_phone_field_keys() -> list[str]:
    fields = _pd_get("organizationFields", {"limit": 500})
    if not isinstance(fields, list):
        return []

    keys: list[str] = []
    for field in fields:
        if not isinstance(field, dict):
            continue

        field_type = _clean(field.get("field_type")).lower()
        field_name = _clean(field.get("name")).lower()
        field_key = _clean(field.get("key"))

        if not field_key:
            continue

        if field_type == "phone" or "phone" in field_name or "tel" in field_name:
            keys.append(field_key)

    return keys


def _format_phone_number(value: Any) -> str:
    phone = _clean(value)
    if not phone:
        return ""

    digits = re.sub(r"\D", "", phone)

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

    return phone


def _pd_phone_from_entity(entity: dict | None, entity_type: str = "person") -> str:
    if not entity:
        return ""

    phone = _format_phone_number(_pd_preferred_value(entity.get("phone")))
    if phone:
        return phone

    if entity_type == "organization":
        for field_key in _pd_org_phone_field_keys():
            phone = _format_phone_number(_pd_preferred_value(entity.get(field_key)))
            if phone:
                return phone

    return ""


def pd_search_persons(term: str) -> list[dict]:
    results = _pd_get("persons/search", {"term": term, "fields": "name,email", "search_by_email": 1})
    if results and isinstance(results, dict) and "items" in results:
        return [
            {
                "id": item["item"]["id"],
                "name": item["item"]["name"],
                "email": item["item"]["emails"][0] if item["item"]["emails"] else "",
            } for item in results["items"]
        ]
    return []


def pd_get_person(id: str | int) -> dict | None:
    data = _pd_get(f"persons/{id}")
    return data if isinstance(data, dict) else None


def pd_get_org(id: str | int) -> dict | None:
    data = _pd_get(f"organizations/{id}")
    return data if isinstance(data, dict) else None


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = ", ".join([str(v) for v in value])
    return str(value).strip()


class _ATagTextExtractor(html.parser.HTMLParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_a_tag = False
        self.data = ""
        self.found = False

    def handle_starttag(self, tag, attrs):
        if tag == "a" and not self.found:
            self.in_a_tag = True

    def handle_endtag(self, tag):
        if tag == "a" and self.in_a_tag:
            self.in_a_tag = False
            self.found = True

    def handle_data(self, data):
        if self.in_a_tag:
            self.data += data.strip()


def _extract_text_from_a_tag(html_string: str) -> str:
    if not html_string or "<a" not in html_string.lower():
        return ""
    parser = _ATagTextExtractor()
    try:
        parser.feed(html_string)
        parser.close()
        return parser.data
    except Exception:
        return ""


def _extract_address_from_html(raw_input: Any) -> str:
    if raw_input is None:
        return ""

    if isinstance(raw_input, dict):
        for key in ["formatted_address", "value", "label", "address"]:
            extracted = _extract_address_from_html(raw_input.get(key))
            if extracted:
                return extracted
        for key, value in raw_input.items():
            if "address" in str(key).lower():
                extracted = _extract_address_from_html(value)
                if extracted:
                    return extracted
        return ""

    if isinstance(raw_input, list):
        for item in raw_input:
            extracted = _extract_address_from_html(item)
            if extracted:
                return extracted
        return ""

    html_string = _clean(raw_input)

    if html_string.startswith("{") and html_string.endswith("}"):
        try:
            addr_obj = json.loads(html_string)
            if isinstance(addr_obj, dict) and addr_obj.get("formatted_address"):
                return _clean(addr_obj["formatted_address"])
            return _clean(addr_obj.get("label", ""))
        except json.JSONDecodeError:
            pass

    clean_addr = _extract_text_from_a_tag(html_string)
    if clean_addr:
        return _clean(clean_addr)

    return html_string


def _extract_address_from_entity(entity: dict | None) -> str:
    if not isinstance(entity, dict):
        return ""

    direct_keys = [
        "address_formatted_address",
        "address",
        "postal_address_formatted_address",
        "postal_address",
    ]
    for key in direct_keys:
        extracted = _extract_address_from_html(entity.get(key))
        if extracted:
            return extracted

    for key, value in entity.items():
        key_lower = str(key).lower()
        if "formatted_address" in key_lower or key_lower.endswith("_address"):
            extracted = _extract_address_from_html(value)
            if extracted:
                return extracted

    return ""


def _get_address_from_components(entity: dict, addr_type: str) -> str:
    parts = []
    street_parts = []

    for key in ["street_number", "route", "sublocality", "address_line_1"]:
        if entity.get(f"{addr_type}_{key}"):
            street_parts.append(_clean(entity[f"{addr_type}_{key}"]))

    if not street_parts and entity.get(f"{addr_type}_street"):
        street_parts.append(_clean(entity[f"{addr_type}_street"]))

    if street_parts:
        parts.append(" ".join(street_parts))

    if entity.get(f"{addr_type}_locality"):
        parts.append(_clean(entity[f"{addr_type}_locality"]))
    elif entity.get(f"{addr_type}_city"):
        parts.append(_clean(entity[f"{addr_type}_city"]))

    state = None
    if entity.get(f"{addr_type}_admin_area_level_1"):
        state = _clean(entity[f"{addr_type}_admin_area_level_1"])
    elif entity.get(f"{addr_type}_state"):
        state = _clean(entity[f"{addr_type}_state"])

    zip_code = None
    if entity.get(f"{addr_type}_postal_code"):
        zip_code = _clean(entity[f"{addr_type}_postal_code"])
    elif entity.get(f"{addr_type}_zip"):
        zip_code = _clean(entity[f"{addr_type}_zip"])

    if state and zip_code:
        parts.append(f"{state} {zip_code}")
    elif state:
        parts.append(state)
    elif zip_code:
        parts.append(zip_code)

    if entity.get(f"{addr_type}_country_code"):
        parts.append(_clean(entity[f"{addr_type}_country_code"]))

    return ", ".join(parts)


def _parse_us_address(full_addr: str) -> tuple[str, str, str, str]:
    full_addr = full_addr.strip()
    if not full_addr:
        return "", "", "", ""

    parts = [p.strip() for p in full_addr.split(",") if p.strip()]

    street = ""
    city = ""
    state = ""
    zip_code = ""

    if len(parts) >= 1:
        street = parts[0]

    if len(parts) >= 2:
        city = parts[1]

    if len(parts) >= 3:
        state_zip_part = " ".join(parts[2:]).upper()
        sz_parts = [p.strip() for p in state_zip_part.split() if p.strip()]

        for part in sz_parts:
            if len(part) == 2 and part.isalpha() and not state:
                state = part
            elif part.isdigit() and len(part) >= 5 and not zip_code:
                zip_code = part

            if state and zip_code:
                break

        if not zip_code:
            zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", state_zip_part)
            if zip_match:
                zip_code = zip_match.group(0)

        if not state:
            state_zip_lower = " ".join(parts[2:]).lower()
            for state_name, state_abbrev in US_STATE_ABBREVIATIONS.items():
                if state_name in state_zip_lower:
                    state = state_abbrev
                    break

        if not state and len(state_zip_part) == 2 and state_zip_part.isalpha():
            state = state_zip_part
        if not zip_code and len(state_zip_part) >= 5 and state_zip_part.isdigit():
            zip_code = state_zip_part

    if full_addr and not any([street, city, state, zip_code]):
        return full_addr, "", "", ""

    return _clean(street), _clean(city), _clean(state), _clean(zip_code)


def pd_person_to_customer(person: dict, org: dict | None = None) -> dict:
    name = _clean(person.get("name"))
    email = _clean(_pd_preferred_value(person.get("email")))
    person_phone = _pd_phone_from_entity(person, "person")

    company = _clean((org or {}).get("name") or "")
    bill_company = company
    bill_name = name
    phone = person_phone
    bill_phone = phone
    bill_email = email

    if org:
        org_email = _clean(_pd_preferred_value(org.get("email")))
        org_phone = _pd_phone_from_entity(org, "organization")
        phone = phone or org_phone
        bill_email = org_email or bill_email
        bill_phone = org_phone or bill_phone

    p_addr_full = _extract_address_from_entity(person)
    o_addr_full = _extract_address_from_entity(org or {})

    if not p_addr_full:
        p_addr_full = _get_address_from_components(person, "address")
    if not o_addr_full and org:
        o_addr_full = _get_address_from_components(org, "org_address")

    p_street, p_city, p_state, p_zip = _parse_us_address(p_addr_full)
    o_street, o_city, o_state, o_zip = _parse_us_address(o_addr_full)

    if p_street or p_city or p_state or p_zip:
        ship_addr1 = p_street
        ship_city = p_city
        ship_state = p_state
        ship_zip = p_zip
    else:
        ship_addr1 = o_street
        ship_city = o_city
        ship_state = o_state
        ship_zip = o_zip

    if org and (o_addr_full or o_street or o_city or o_state or o_zip):
        bill_addr1 = o_street
        bill_city = o_city
        bill_state = o_state
        bill_zip = o_zip
    else:
        bill_addr1 = ship_addr1
        bill_city = ship_city
        bill_state = ship_state
        bill_zip = ship_zip

    return {
        "company": company,
        "name": name,
        "email": email,
        "phone": phone,
        "ship_addr1": ship_addr1, "ship_city": ship_city, "ship_state": ship_state, "ship_zip": ship_zip,
        "bill_company": bill_company,
        "bill_name": bill_name,
        "bill_email": bill_email,
        "bill_phone": bill_phone,
        "bill_addr1": bill_addr1, "bill_city": bill_city, "bill_state": bill_state, "bill_zip": bill_zip,
    }


ALLOW_COURSE_SKUS = {"M5CO", "M7CO", "MXCO"}


def is_basket_5_7_X(item: dict) -> bool:
    sku = (item.get("sku") or "").upper().strip()
    name = (item.get("name") or "").lower()

    if sku in ALLOW_COURSE_SKUS:
        return True

    name_ok = (("mach 5" in name) or ("mach 7" in name) or ("mach x" in name)) and any(
        k in name for k in ["standard", "portable", "no frills"]
    )
    if name_ok:
        return True

    if sku.startswith(("M5", "M7", "MX")) and not sku.endswith("CO"):
        bad_keywords = ["COLLAR", "CHAIN", "HOLDER", "WRAP"]
        if any(bad in sku for bad in bad_keywords):
            return False
        return True

    return False


def eligible_qty_for_discount(items: list[dict]) -> int:
    return int(sum((float(it.get("qty", 0)) for it in items if is_basket_5_7_X(it) and it.get("sku") != "CD")))


def find_course_discount_index(items: list[dict]) -> int:
    for idx, it in enumerate(items):
        if (it.get("sku") == "CD") or (it.get("name", "").lower().strip() == "course discount"):
            return idx
    return -1


def find_last_course_discount_anchor_index(items: list[dict]) -> int:
    anchor_index = -1
    for idx, item in enumerate(items):
        if item.get("sku") == "CD":
            continue
        if is_basket_5_7_X(item):
            anchor_index = idx
    return anchor_index


def ensure_course_discount_position(items: list[dict] = None):
    if items is None:
        items = st.session_state["line_items"]

    discount_idx = find_course_discount_index(items)
    if discount_idx == -1:
        return

    anchor_idx = find_last_course_discount_anchor_index(items)
    if anchor_idx == -1:
        return

    discount_item = items.pop(discount_idx)
    if discount_idx < anchor_idx:
        anchor_idx -= 1

    insert_at = anchor_idx + 1
    items.insert(insert_at, discount_item)


def ensure_course_discount(items: list[dict]) -> bool:
    qty = eligible_qty_for_discount(items)
    idx = find_course_discount_index(items)
    modified = False
    discount_note = "Auto-applied for 9+ Mach 5/7/X baskets"

    if qty >= 9:
        disc_line = {
            "id": items[idx]["id"] if idx != -1 and "id" in items[idx] else str(uuid.uuid4()),
            "sku": "CD",
            "name": "Course Discount (-$100 per qualifying basket)",
            "qty": qty,
            "unit": -100.0,
            "total": round(-100.0 * qty, 2),
            "Notes": discount_note,
            "prev_sku": "CD",
            "previewChecked": True,
            "exclude_from_10_discount": True,
        }

        if idx == -1:
            items.append(disc_line)
            modified = True
        elif items[idx]["qty"] != disc_line["qty"] or items[idx]["total"] != disc_line["total"]:
            items[idx] = disc_line
            modified = True

        ensure_course_discount_position(items)

    elif idx != -1:
        items.pop(idx)
        modified = True

    return modified


def _company_right_block(styles):
    return Paragraph(
        f"<b>Disc Golf Association (DGA)</b><br/>"
        f"73 Hangar Way<br/>"
        f"Watsonville, CA 95076<br/>"
        f"Phone: {COMPANY['phone']}",
        styles["LeftInfo"]
    )


def _build_pdf_brand_header(
    styles,
    content_width: float,
    logo_w: float,
    logo_h: float,
    title: str,
    subtitle: str,
    info_left: str,
    info_right: str,
    compact_level: int,
    info_third: str = "",
    info_col_widths: list[float] | None = None,
):
    section_fill = colors.HexColor("#E7F0FB")
    section_border = colors.HexColor("#B8CAE6")

    if compact_level == 0:
        logo_display_w = 1.7 * inch
        logo_display_h = 1.05 * inch
        kicker_font = 8
        title_font = 23
        title_leading = 24
        detail_font = 8
        detail_leading = 10
        header_gap = 4
        info_font = 10
        info_pad_v = 4
        info_pad_h = 6
        right_top_pad = 14
    elif compact_level == 1:
        logo_display_w = 1.45 * inch
        logo_display_h = 0.9 * inch
        kicker_font = 7
        title_font = 19
        title_leading = 20
        detail_font = 7
        detail_leading = 8
        header_gap = 3
        info_font = 9
        info_pad_v = 3
        info_pad_h = 5
        right_top_pad = 10
    else:
        logo_display_w = 1.2 * inch
        logo_display_h = 0.74 * inch
        kicker_font = 6
        title_font = 16
        title_leading = 17
        detail_font = 6
        detail_leading = 7
        header_gap = 2
        info_font = 8
        info_pad_v = 2
        info_pad_h = 4
        right_top_pad = 7

    detail_line_1 = f"{COMPANY['addr1']}  |  {COMPANY['city']}, {COMPANY['state']} {COMPANY['zip']}"
    detail_line_2 = f"{COMPANY['phone']}  |  {COMPANY['web']}"

    left_col_width = min(1.9 * inch, content_width * 0.22)
    right_col_width = content_width - left_col_width

    left_elements = []
    if COMPANY_LOGO_PATH:
        try:
            img_reader = ImageReader(COMPANY_LOGO_PATH)
            img_w, img_h = img_reader.getSize()
            aspect = (img_h / img_w) if img_w else 1.0
        except Exception:
            aspect = (logo_display_h / logo_display_w) if logo_display_w else 1.0

        logo = Image(COMPANY_LOGO_PATH, width=logo_display_w, height=logo_display_w * aspect)
        logo.hAlign = "LEFT"
        left_elements.append(logo)
    else:
        left_elements.append(Paragraph(f"<b>{COMPANY['name']}</b>", styles["Normal"]))

    left_block = Table([[elem] for elem in left_elements], colWidths=[left_col_width])
    left_block.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
    ]))

    title_para = Paragraph(
        f"<b>{title}</b>",
        ParagraphStyle(
            "PdfHeaderTitle",
            parent=styles["Normal"],
            fontSize=title_font,
            leading=title_leading,
            alignment=TA_RIGHT,
        )
    )
    detail_para = Paragraph(
        f"{detail_line_1}<br/>{detail_line_2}",
        ParagraphStyle(
            "PdfHeaderDetail",
            parent=styles["Normal"],
            fontSize=detail_font,
            leading=detail_leading,
            alignment=TA_RIGHT,
        )
    )
    right_rows = [[title_para]]
    if subtitle.strip():
        subtitle_para = Paragraph(
            f'<font color="#2D6FC2"><b>{subtitle}</b></font>',
            ParagraphStyle(
                "PdfHeaderKicker",
                parent=styles["Normal"],
                fontSize=kicker_font,
                leading=kicker_font + 1,
                alignment=TA_RIGHT,
            )
        )
        right_rows.append([subtitle_para])
    right_rows.append([detail_para])
    right_block = Table(right_rows, colWidths=[right_col_width])
    right_block.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), right_top_pad),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
    ]))

    header_table = Table([[left_block, right_block]], colWidths=[left_col_width, right_col_width])
    header_table.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
    ]))
    header_table.hAlign = "LEFT"

    info_left_para = Paragraph(
        f"<b>{info_left}</b>",
        ParagraphStyle(
            "PdfHeaderInfoLeft",
            parent=styles["Normal"],
            fontSize=info_font,
            leading=info_font + 1,
        )
    )
    info_right_para = Paragraph(
        info_right,
        ParagraphStyle(
            "PdfHeaderInfoRight",
            parent=styles["Normal"],
            fontSize=info_font,
            leading=info_font + 1,
        )
    )

    if info_third:
        info_third_para = Paragraph(
            info_third,
            ParagraphStyle(
                "PdfHeaderInfoThird",
                parent=styles["Normal"],
                fontSize=info_font,
                leading=info_font + 1,
            )
        )
        info_widths = info_col_widths or [content_width / 3, content_width / 3, content_width / 3]
        info_table = Table(
            [[info_left_para, info_right_para, info_third_para]],
            colWidths=info_widths
        )
    else:
        info_widths = info_col_widths or [content_width * 0.53, content_width * 0.47]
        info_table = Table([[info_left_para, info_right_para]], colWidths=info_widths)
    info_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), section_fill),
        ("BOX", (0, 0), (-1, -1), 0.5, section_border),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, section_border),
        ("LEFTPADDING", (0, 0), (-1, -1), info_pad_h),
        ("RIGHTPADDING", (0, 0), (-1, -1), info_pad_h),
        ("TOPPADDING", (0, 0), (-1, -1), info_pad_v),
        ("BOTTOMPADDING", (0, 0), (-1, -1), info_pad_v),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    info_table.hAlign = "LEFT"

    return [header_table, Spacer(1, header_gap), info_table, Spacer(1, header_gap)]


def _build_address_card(
    title: str,
    body: Paragraph,
    width: float,
    header_font: int,
    body_pad: int = 6,
    body_min_height: float | None = None,
):
    row_heights = None if body_min_height is None else [None, body_min_height]
    card = Table(
        [
            [Paragraph(f"<b>{title}</b>", ParagraphStyle(
                f"AddressCardTitle_{title}",
                fontName="Helvetica-Bold",
                fontSize=header_font,
                leading=header_font + 1,
            ))],
            [body],
        ],
        colWidths=[width],
        rowHeights=row_heights,
    )
    card.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, 0), colors.HexColor("#E7F0FB")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#B8CAE6")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#B8CAE6")),
        ("LEFTPADDING", (0, 0), (-1, 0), body_pad),
        ("RIGHTPADDING", (0, 0), (-1, 0), body_pad),
        ("TOPPADDING", (0, 0), (-1, 0), 4),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 4),
        ("LEFTPADDING", (0, 1), (-1, -1), body_pad),
        ("RIGHTPADDING", (0, 1), (-1, -1), body_pad),
        ("TOPPADDING", (0, 1), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    card.hAlign = "LEFT"
    return card


def _truncate_text(text: str, max_len: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def _prepare_items_for_pdf(items: list[dict], compact_level: int) -> list[dict]:
    prepared = copy.deepcopy(items)

    for item in prepared:
        if compact_level >= 2:
            notes_val = item.get("Notes") or item.get("notes") or ""
            item["Notes"] = _truncate_text(notes_val.replace("\n", " "), 80)
        elif compact_level >= 1:
            notes_val = item.get("Notes") or item.get("notes") or ""
            item["Notes"] = _truncate_text(notes_val.replace("\n", " "), 140)

    return prepared


def _prepare_text_for_pdf(text: str, compact_level: int, field_type: str) -> str:
    if compact_level >= 2:
        limits = {"freight": 120, "footer": 180}
    elif compact_level >= 1:
        limits = {"freight": 220, "footer": 350}
    else:
        return text or ""

    return _truncate_text((text or "").replace("\n", " "), limits[field_type])


def _get_pdf_page_count(pdf_bytes: bytes) -> int:
    if PdfReader is None:
        raise RuntimeError("PyPDF2 is required for single-page enforcement. Install it with: pip install PyPDF2")
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return len(reader.pages)


def build_pdf(
    buffer: io.BytesIO,
    customer: dict,
    items: list,
    fees: dict,
    totals: dict,
    doc_number: str,
    footer_notes_text: str,
    template: str = "quote",
    meta: dict | None = None,
    compact_level: int = 0,
):
    meta = meta or {}
    pdf_section_fill = colors.HexColor("#E7F0FB")

    if compact_level == 0:
        left_margin = right_margin = 36
        top_margin = bottom_margin = 30
        content_width = 7.5 * inch
        desc_font = 9
        desc_leading = 11
        notes_font = 8
        notes_leading = 10
        notes_font_2 = 8
        notes_leading_2 = 10
        addr_font = 10
        addr_leading = 12
        logo_w, logo_h = 1.8 * inch, 1.0 * inch
        row_top_pad = 3
        row_bottom_pad = 3
        block_spacer_small = 4
        block_spacer_med = 8
        block_spacer_large = 12
        show_accessory_box = True
    elif compact_level == 1:
        left_margin = right_margin = 24
        top_margin = bottom_margin = 20
        content_width = letter[0] - left_margin - right_margin
        desc_font = 8
        desc_leading = 9
        notes_font = 7
        notes_leading = 8
        notes_font_2 = 7
        notes_leading_2 = 8
        addr_font = 9
        addr_leading = 10
        logo_w, logo_h = 1.5 * inch, 0.85 * inch
        row_top_pad = 2
        row_bottom_pad = 2
        block_spacer_small = 2
        block_spacer_med = 4
        block_spacer_large = 6
        show_accessory_box = True
    else:
        left_margin = right_margin = 18
        top_margin = bottom_margin = 14
        content_width = letter[0] - left_margin - right_margin
        desc_font = 7
        desc_leading = 8
        notes_font = 6
        notes_leading = 7
        notes_font_2 = 6
        notes_leading_2 = 7
        addr_font = 8
        addr_leading = 9
        logo_w, logo_h = 1.25 * inch, 0.72 * inch
        row_top_pad = 1
        row_bottom_pad = 1
        block_spacer_small = 1
        block_spacer_med = 2
        block_spacer_large = 4
        show_accessory_box = False

    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=right_margin,
        leftMargin=left_margin,
        topMargin=top_margin,
        bottomMargin=bottom_margin
    )
    styles = getSampleStyleSheet()

    if "CenterTitle" not in styles:
        styles.add(ParagraphStyle("CenterTitle", parent=styles["Title"], alignment=TA_CENTER))
    if "LeftInfo" not in styles:
        styles.add(ParagraphStyle("LeftInfo", parent=styles["Normal"], fontSize=10, leading=12, alignment=TA_LEFT))
    if "QuoteHeaderTitle" not in styles:
        styles.add(ParagraphStyle("QuoteHeaderTitle", parent=styles["Heading2"], alignment=TA_RIGHT, fontSize=14, leading=16))

    story = []

    notes_style = ParagraphStyle(
        "LineNote",
        parent=styles["Normal"],
        fontSize=notes_font,
        leading=notes_leading,
        textColor=colors.grey,
        leftIndent=6
    )
    product_note_style = ParagraphStyle(
        "ProductNote",
        parent=styles["Normal"],
        fontSize=notes_font + 0.5,
        leading=notes_leading + 1,
        textColor=colors.HexColor("#374151"),
    )
    notes_style_2 = ParagraphStyle(
        "LineNote2",
        parent=styles["Normal"],
        fontSize=notes_font_2,
        leading=notes_leading_2,
        textColor=colors.black
    )
    addr_style = ParagraphStyle("AddrStyle", parent=styles["Normal"], fontSize=addr_font, leading=addr_leading)
    desc_style = ParagraphStyle("Desc", parent=styles["Normal"], fontSize=desc_font, leading=desc_leading)

    footer_notes_text = _prepare_text_for_pdf(footer_notes_text, compact_level, "footer")
    freight_notes_meta = _prepare_text_for_pdf(meta.get("freight_notes", ""), compact_level, "freight")
    items = _prepare_items_for_pdf(items, compact_level)

    primary_discount_label = totals.get("discount_label", "")
    primary_discount_amount = totals.get("ten_percent_discount", 0.0)
    manager_discount_amount = totals.get("manager_discount", 0.0)

    if template == "order":
        story += _build_pdf_brand_header(
            styles,
            content_width,
            logo_w,
            logo_h,
            "DGA Order",
            "",
            f"Order: {doc_number}",
            f"Submitted: {get_pacific_now().strftime('%Y-%m-%d')}",
            compact_level,
            f"Operator: {meta.get('operator', '')}",
        )

        ship_block_order = (
            f"{customer.get('company', '')}<br/>"
            f"{customer.get('name', '')}<br/>"
            f"{customer.get('ship_addr1', '')}<br/>"
            f"{customer.get('ship_city', '')}, {customer.get('ship_state', '')} {customer.get('ship_zip', '')}<br/>"
            f"{customer.get('phone', '')}<br/>"
            f"{customer.get('email', '')}"
        )

        bill_block_order = (
            f"{customer.get('bill_company', customer.get('company', ''))}<br/>"
            f"{customer.get('bill_name', customer.get('name', ''))}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('bill_phone', customer.get('phone', ''))}<br/>"
            f"{customer.get('bill_email', customer.get('email', ''))}"
        )

        po_block_order = (
            f"P.O. Number: {meta.get('po_number', '')}<br/>"
            f"Authorization Code: {meta.get('auth_code', '')}<br/>"
            f"Check Number: {meta.get('check_number', '')}<br/>"
            f"Date Received: {meta.get('date_received', '')}"
        )

        commission_to = meta.get('commission_to', '').strip()
        if commission_to:
            po_block_order += f"<br/><br/><b>Commission to:</b> {commission_to}"

        card_col_width = content_width / 3
        addr_card_width = card_col_width
        card_body_width = addr_card_width - 12

        ship_para = Paragraph(ship_block_order, addr_style)
        bill_para = Paragraph(bill_block_order, addr_style)
        po_para = Paragraph(po_block_order, addr_style)

        order_card_body_height = max(
            ship_para.wrap(card_body_width, 1000)[1],
            bill_para.wrap(card_body_width, 1000)[1],
            po_para.wrap(card_body_width, 1000)[1],
        ) + 10

        addr_table = Table(
            [[
                _build_address_card("Shipping Address", ship_para, addr_card_width, max(8, addr_font), body_min_height=order_card_body_height),
                _build_address_card("Billing Address", bill_para, addr_card_width, max(8, addr_font), body_min_height=order_card_body_height),
                _build_address_card("Purchase Order & Check Info", po_para, addr_card_width, max(8, addr_font), body_min_height=order_card_body_height),
            ]],
            colWidths=[card_col_width, card_col_width, card_col_width]
        )
        addr_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        addr_table.hAlign = "LEFT"
        story += [addr_table, Spacer(1, block_spacer_med)]

        header = ["Quantity", "Product Description", "Unit Price", "Total"]
        li_cols = [0.7 * inch, content_width - 0.7 * inch - 0.825 * inch - 0.825 * inch, 0.825 * inch, 0.825 * inch]
        data = [header]
        note_row_indexes = []

        for r in items:
            is_checked = r.get("previewChecked", True)
            if float(r.get("qty", 0)) == 0 or not is_checked:
                continue

            desc_para = Paragraph(str(r["name"]), desc_style)
            data.append([
                str(r["qty"]),
                desc_para,
                fmt_money(float(r["unit"])),
                fmt_money(float(r["total"]))
            ])

            note_txt = (r.get("Notes") or r.get("notes") or "").strip()
            if note_txt:
                note_html = html.escape(note_txt).replace("\n", "<br/>")
                note_row_indexes.append(len(data))
                data.append(["", Paragraph(note_html, product_note_style), "", ""])

        t_li = Table(data, colWidths=li_cols, repeatRows=1)
        line_item_style = [
            ("BOX", (0, 0), (-1, -1), 0.75, colors.black),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), pdf_section_fill),
            ("ALIGN", (0, 1), (0, -1), "CENTER"),
            ("ALIGN", (2, 1), (3, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), row_top_pad),
            ("BOTTOMPADDING", (0, 0), (-1, -1), row_bottom_pad),
        ]
        for note_row_idx in note_row_indexes:
            line_item_style.extend([
                ("SPAN", (1, note_row_idx), (3, note_row_idx)),
                ("BACKGROUND", (1, note_row_idx), (3, note_row_idx), colors.HexColor("#F7F9FC")),
                ("LEFTPADDING", (1, note_row_idx), (3, note_row_idx), 8),
                ("RIGHTPADDING", (1, note_row_idx), (3, note_row_idx), 8),
                ("TOPPADDING", (1, note_row_idx), (3, note_row_idx), 4),
                ("BOTTOMPADDING", (1, note_row_idx), (3, note_row_idx), 5),
            ])
        t_li.setStyle(TableStyle(line_item_style))
        t_li.hAlign = "LEFT"
        story += [t_li]

        freight_notes_txt = freight_notes_meta.strip()
        if not freight_notes_txt and st.session_state.get("freight_notes"):
            freight_notes_txt = _prepare_text_for_pdf(st.session_state["freight_notes"], compact_level, "freight").strip()

        if freight_notes_txt:
            story += [Spacer(1, block_spacer_small), Paragraph(f"<b>Freight Notes:</b> {freight_notes_txt}", notes_style_2)]

        story += [Spacer(1, block_spacer_med)]

        sub_rows = [["Subtotal:", fmt_money(totals.get("subtotal", 0.0))]]
        if primary_discount_label and primary_discount_amount > 0:
            sub_rows.append([f"{primary_discount_label}:", fmt_money(-primary_discount_amount)])
        if manager_discount_amount > 0:
            sub_rows.append(["Manager Pricing:", fmt_money(-manager_discount_amount)])
        sub_rows.extend([
            ["Drop-Ship Fee:", fmt_money(fees.get("drop_ship_fee", 0.0))],
            [f"Sales Tax ({totals.get('tax_rate_pct', 0.0) * 100:.2f}%):", fmt_money(totals.get("sales_tax", 0.0))],
        ])

        sub_tbl_w = 2.5 * inch
        t_sub = Table(sub_rows, colWidths=[sub_tbl_w * 0.6, sub_tbl_w * 0.4])
        t_sub.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]))

        grand_tbl_w = 2.5 * inch
        t_grand = Table([
            ["Freight:", fmt_money(fees.get("freight", 0.0))],
            ["**GRAND TOTAL:**", f"**{fmt_money(totals.get('grand_total', 0.0))}**"],
        ], colWidths=[grand_tbl_w * 0.6, grand_tbl_w * 0.4])
        t_grand.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("BACKGROUND", (0, -1), (-1, -1), pdf_section_fill),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]))

        v_totals_table = Table([[t_sub], [t_grand]], colWidths=[sub_tbl_w])
        v_totals_table.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))

        final_wrapper = Table([["", v_totals_table]], colWidths=[content_width - sub_tbl_w, sub_tbl_w])
        final_wrapper.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (1, 0), (1, 0), "RIGHT")
        ]))
        final_wrapper.hAlign = "LEFT"
        story += [final_wrapper]

    else:
        story += _build_pdf_brand_header(
            styles,
            content_width,
            logo_w,
            logo_h,
            "DGA Quote",
            "Pricing Subject to Change",
            f"Quote: {doc_number}",
            f"Submitted: {get_pacific_now().strftime('%Y-%m-%d')}",
            compact_level,
            info_col_widths=[content_width / 2, content_width / 2],
        )

        ship_block = (
            f"{customer.get('company', '')}<br/>"
            f"{customer.get('name', '')}<br/>"
            f"{customer.get('ship_addr1', '')}<br/>"
            f"{customer.get('ship_city', '')}, {customer.get('ship_state', '')} {customer.get('ship_zip', '')}<br/>"
            f"{customer.get('phone', '')}<br/>"
            f"{customer.get('email', '')}"
        )

        bill_block = (
            f"{customer.get('bill_company', customer.get('company', ''))}<br/>"
            f"{customer.get('bill_name', customer.get('name', ''))}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('bill_phone', customer.get('phone', ''))}<br/>"
            f"{customer.get('bill_email', customer.get('email', ''))}"
        )

        addr_card_width = content_width / 2
        quote_card_body_width = addr_card_width - 12
        ship_para = Paragraph(ship_block, addr_style)
        bill_para = Paragraph(bill_block, addr_style)
        quote_card_body_height = max(
            ship_para.wrap(quote_card_body_width, 1000)[1],
            bill_para.wrap(quote_card_body_width, 1000)[1],
        ) + 10
        t = Table([[
            _build_address_card("Shipping Address", ship_para, addr_card_width, max(8, addr_font), body_min_height=quote_card_body_height),
            _build_address_card("Billing Address", bill_para, addr_card_width, max(8, addr_font), body_min_height=quote_card_body_height),
        ]],
                  colWidths=[content_width / 2, content_width / 2])
        t.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]))
        t.hAlign = "LEFT"
        story += [t, Spacer(1, block_spacer_large)]

        header = ["Qty", "Product Description", "Unit Price", "Total"]
        li_cols = [0.65 * inch, content_width - 0.65 * inch - 1.1 * inch - 1.1 * inch, 1.1 * inch, 1.1 * inch]
        data = [header]
        note_row_indexes = []

        for r in items:
            is_checked = r.get("previewChecked", True)
            if float(r.get("qty", 0)) == 0 or not is_checked:
                continue

            desc_para = Paragraph(str(r["name"]), desc_style)
            data.append([
                str(r["qty"]),
                desc_para,
                fmt_money(float(r["unit"])),
                fmt_money(float(r["total"]))
            ])

            note_txt = (r.get("Notes") or r.get("notes") or "").strip()
            if note_txt:
                note_html = html.escape(note_txt).replace("\n", "<br/>")
                note_row_indexes.append(len(data))
                data.append(["", Paragraph(note_html, product_note_style), "", ""])

        t_li = Table(data, colWidths=li_cols, repeatRows=1)
        line_item_style = [
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("BACKGROUND", (0, 0), (-1, 0), pdf_section_fill),
            ("ALIGN", (0, 1), (0, -1), "CENTER"),
            ("ALIGN", (2, 1), (3, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), row_top_pad),
            ("BOTTOMPADDING", (0, 0), (-1, -1), row_bottom_pad),
        ]
        for note_row_idx in note_row_indexes:
            line_item_style.extend([
                ("SPAN", (1, note_row_idx), (3, note_row_idx)),
                ("BACKGROUND", (1, note_row_idx), (3, note_row_idx), colors.HexColor("#F7F9FC")),
                ("LEFTPADDING", (1, note_row_idx), (3, note_row_idx), 8),
                ("RIGHTPADDING", (1, note_row_idx), (3, note_row_idx), 8),
                ("TOPPADDING", (1, note_row_idx), (3, note_row_idx), 4),
                ("BOTTOMPADDING", (1, note_row_idx), (3, note_row_idx), 5),
            ])
        t_li.setStyle(TableStyle(line_item_style))
        t_li.hAlign = "LEFT"
        story += [t_li, Spacer(1, block_spacer_large)]

        freight_notes_txt = freight_notes_meta.strip()
        if not freight_notes_txt and st.session_state.get("freight_notes"):
            freight_notes_txt = _prepare_text_for_pdf(st.session_state["freight_notes"], compact_level, "freight").strip()

        if freight_notes_txt:
            story += [Spacer(1, block_spacer_small), Paragraph(f"<b>Freight Notes:</b> {freight_notes_txt}", notes_style_2)]
            story += [Spacer(1, block_spacer_small)]

        acc_width = 3.5 * inch if compact_level < 2 else 0
        totals_width = 3.0 * inch if compact_level < 2 else min(3.2 * inch, content_width)

        totals_rows = [["Subtotal:", fmt_money(totals.get("subtotal", 0.0))]]
        if primary_discount_label and primary_discount_amount > 0:
            totals_rows.append([f"{primary_discount_label}:", fmt_money(-primary_discount_amount)])
        if manager_discount_amount > 0:
            totals_rows.append(["Manager Pricing:", fmt_money(-manager_discount_amount)])
        totals_rows.extend([
            ["Drop-Ship Fee:", fmt_money(fees.get("drop_ship_fee", 0.0))],
            ["Freight:", fmt_money(fees.get("freight", 0.0))],
            [f"Sales Tax ({totals.get('tax_rate_pct', 0.0) * 100:.2f}%):", fmt_money(totals.get("sales_tax", 0.0))],
            ["**GRAND TOTAL:**", f"**{fmt_money(totals.get('grand_total', 0.0))}**"],
        ])

        t_totals = Table(totals_rows, colWidths=[totals_width * 0.65, totals_width * 0.35])
        t_totals.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("BACKGROUND", (0, -1), (-1, -1), pdf_section_fill),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("TOPPADDING", (0, 0), (-1, -1), row_top_pad),
            ("BOTTOMPADDING", (0, 0), (-1, -1), row_bottom_pad),
        ]))

        if show_accessory_box:
            acc_data = [
                [Paragraph("<b>Additional Course Equipment to Consider*</b>",
                           ParagraphStyle("ACCHdr", parent=styles["Normal"], fontSize=notes_font_2 + 1, alignment=1,
                                          textColor=colors.black, leading=notes_leading_2 + 1))],
                ["Number Plate", fmt_money(35.00)],
                ["Powder Coat Fee - Stock Color", fmt_money(90.00)],
                ["Additional Anchor - Pin Positions", fmt_money(30.00)],
                ["Basic Color Tee Sign", fmt_money(55.00)],
                ['12"x18" Color Rules Sign', fmt_money(69.00)],
                ["Pole Extension", fmt_money(60.00)],
                ["Basket Flag", fmt_money(30.00)],
                [Paragraph("<b>*Per Unit Pricing</b>",
                           ParagraphStyle("ACCfTR", parent=styles["Normal"], fontSize=notes_font, alignment=1,
                                          textColor=colors.black, leading=notes_leading))]
            ]

            acc_tbl = Table(acc_data, colWidths=[acc_width * 0.7, acc_width * 0.3])
            acc_tbl.setStyle(TableStyle([
                ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                ("BACKGROUND", (0, 0), (-1, 0), pdf_section_fill),
                ("SPAN", (0, 0), (-1, 0)),
                ("SPAN", (0, -1), (-1, -1)),
                ("ALIGN", (1, 1), (1, -2), "RIGHT"),
                ("ALIGN", (0, 0), (0, 0), "CENTER"),
                ("ALIGN", (0, -1), (0, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTNAME", (0, 0), (1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("TOPPADDING", (0, 0), (-1, -1), row_top_pad),
                ("BOTTOMPADDING", (0, 0), (-1, -1), row_bottom_pad),
            ]))
            acc_tbl.hAlign = "LEFT"

            totals_col_width = content_width - acc_width
            t_totals.hAlign = "RIGHT"
            combined_table = Table([[acc_tbl, t_totals]], colWidths=[acc_width, totals_col_width])
            combined_table.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ]))
            combined_table.hAlign = "LEFT"
            story += [combined_table, Spacer(1, block_spacer_large)]
        else:
            t_totals.hAlign = "RIGHT"
            totals_wrapper = Table([["", t_totals]], colWidths=[content_width - totals_width, totals_width])
            totals_wrapper.setStyle(TableStyle([
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ]))
            totals_wrapper.hAlign = "LEFT"
            story += [totals_wrapper, Spacer(1, block_spacer_med)]

        story += [Paragraph("<b>Notes:</b>", notes_style), Paragraph(footer_notes_text, notes_style)]

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


def generate_single_page_pdf(
    customer: dict,
    items: list,
    fees: dict,
    totals: dict,
    doc_number: str,
    footer_notes_text: str,
    template: str = "quote",
    meta: dict | None = None,
):
    last_page_count = None

    for compact_level in (0, 1, 2):
        pdf_buffer = io.BytesIO()
        pdf_data = build_pdf(
            pdf_buffer,
            customer,
            items,
            fees,
            totals,
            doc_number,
            footer_notes_text,
            template=template,
            meta=meta,
            compact_level=compact_level,
        )
        page_count = _get_pdf_page_count(pdf_data)
        last_page_count = page_count
        if page_count == 1:
            return pdf_data, compact_level

    raise ValueError(
        f"This document still renders as {last_page_count} pages after compacting. "
        f"Reduce line items or shorten notes/freight text before generating the PDF."
    )


def handle_pdf_generation(payload: dict, doc_number: str, template: str, container: st.delta_generator.DeltaGenerator,
                          order_meta: dict | None = None):
    is_quote = template == "quote"
    file_prefix = f"{doc_number}_Quote" if is_quote else f"{doc_number}_Order"
    label = "Download Quote PDF" if is_quote else "Download Order/PO PDF"

    try:
        pdf_data, compact_level_used = generate_single_page_pdf(
            payload["customer"],
            payload["line_items"],
            payload["fees"],
            payload["totals"],
            doc_number,
            payload["footer_notes"],
            template=template,
            meta=order_meta,
        )
    except Exception as e:
        container.error(f"PDF not generated: {e}")
        return

    save_successful = save_quote_to_gsheet(payload, record_type=template)

    if compact_level_used > 0:
        container.info("Single-page compact mode was applied to keep the PDF on one page.")

    if is_quote:
        if save_successful:
            container.success(f"Quote **{doc_number}** successfully saved to **Google Sheets** and PDF generated.")
        else:
            container.warning(
                "Quote PDF generated but **FAILED to save** to Google Sheets. Check Sheet configuration and sharing permissions."
            )
    else:
        source_quote_no = payload.get("order_meta", {}).get("source_quote_number", payload.get("quote_no", "N/A"))
        doc_msg = (
            f"Order **{doc_number}** PDF generated."
            if doc_number == source_quote_no else
            f"Order **{doc_number}** PDF generated (Source Quote: **{source_quote_no}**)."
        )

        container.success(
            doc_msg + (" Saved to Google Sheets." if save_successful else " **FAILED to save** to Google Sheets.")
        )

    container.download_button(
        label=label,
        data=pdf_data,
        file_name=f"{file_prefix}.pdf",
        mime="application/pdf",
        key=f"download_{template}_pdf_{doc_number}",
        use_container_width=True
    )


def generate_pdf_preview_data(payload: dict, template: str = "quote") -> tuple[bytes, int, str]:
    doc_number = payload["quote_no"] if template == "quote" else (
        payload.get("order_meta", {}).get("order_doc_number") or payload["quote_no"]
    )

    pdf_data, compact_level_used = generate_single_page_pdf(
        payload["customer"],
        payload["line_items"],
        payload["fees"],
        payload["totals"],
        doc_number,
        payload["footer_notes"],
        template=template,
        meta=payload.get("order_meta"),
    )
    return pdf_data, compact_level_used, doc_number


def _preview_height_to_pixels(height: str) -> int:
    height_text = str(height or "").strip().lower()
    if height_text.endswith("vh"):
        try:
            return max(320, int(float(height_text[:-2]) * 8.5))
        except ValueError:
            return 680
    if height_text.endswith("px"):
        try:
            return max(320, int(float(height_text[:-2])))
        except ValueError:
            return 680
    return 680


@st.cache_data(show_spinner=False)
def _pdf_preview_png_data_uri(pdf_data: bytes, render_scale: float = 2.5) -> str:
    if fitz is None:
        raise RuntimeError("PyMuPDF is required for fitted live previews. Install it with: pip install PyMuPDF")

    document = fitz.open(stream=pdf_data, filetype="pdf")
    try:
        page = document.load_page(0)
        pixmap = page.get_pixmap(matrix=fitz.Matrix(render_scale, render_scale), alpha=False)
        image_data = pixmap.tobytes("png")
    finally:
        document.close()

    return f"data:image/png;base64,{base64.b64encode(image_data).decode('ascii')}"


def _render_pdf_image_preview(pdf_data: bytes, height: str, zoom_percent: int):
    image_uri = _pdf_preview_png_data_uri(pdf_data)
    zoom = max(100, int(zoom_percent))
    st.markdown(
        f"""
        <div class="pdf-image-preview-shell" style="height: {height};">
            <img src="{image_uri}" alt="PDF preview" style="width: {zoom}%; min-width: 100%;">
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_pdf_native_browser_preview(pdf_data: bytes, height: str):
    base64_pdf = base64.b64encode(pdf_data).decode("utf-8")
    preview_nonce = uuid.uuid4().hex
    pdf_display = f"""
    <div class="pdf-iframe-container" style="height: {height};">
        <iframe
            src="data:application/pdf;base64,{base64_pdf}#preview={preview_nonce}"
            title="PDF Preview {preview_nonce}"
            style="width: 100%; height: 100%; border: none;">
        </iframe>
    </div>
    """
    st.markdown(pdf_display, unsafe_allow_html=True)


def render_pdf_preview_from_payload(
    payload: dict,
    template: str = "quote",
    height: str = "80vh",
    mode: str = "pdf",
    zoom_percent: int = 100,
):
    pdf_data, compact_level_used, doc_number = generate_pdf_preview_data(payload, template=template)

    if compact_level_used > 0:
        st.caption("Preview is using compact single-page mode.")

    preview_nonce = uuid.uuid4().hex
    component_height = _preview_height_to_pixels(height)

    if mode == "image":
        try:
            _render_pdf_image_preview(pdf_data, height=height, zoom_percent=zoom_percent)
        except RuntimeError:
            st.pdf(pdf_data, height=component_height, key=f"pdf_preview_{preview_nonce}")
    else:
        _render_pdf_native_browser_preview(pdf_data, height=height)
    return pdf_data, doc_number


def get_current_payload(
    subtotal: float,
    drop_ship_fee: float,
    freight: float,
    sales_tax: float,
    grand_total: float,
    tax_rate: float,
    primary_discount_amount: float,
    primary_discount_label: str,
    manager_discount_amount: float,
) -> dict:
    quote_no = st.session_state["quote_no"]

    st.session_state["freight_notes"] = get_selected_freight_notes()

    order_meta = {
        "order_doc_number": st.session_state.get("order_doc_number_pdf", quote_no),
        "po_number": st.session_state["order_po_number"],
        "operator": st.session_state["order_operator"],
        "auth_code": st.session_state["order_auth_code"],
        "commission_to": st.session_state["order_comm_to"],
        "check_number": st.session_state["order_check_number"],
        "date_received": st.session_state["order_date_received"],
        "source_quote_number": quote_no,
        "freight_notes": st.session_state["freight_notes"],
    }

    fees = {
        "drop_ship_fee": drop_ship_fee,
        "freight": freight,
    }
    totals = {
        "subtotal": subtotal,
        "ten_percent_discount": primary_discount_amount,
        "discount_label": primary_discount_label,
        "manager_discount": manager_discount_amount,
        "sales_tax": sales_tax,
        "grand_total": grand_total,
        "tax_rate_pct": tax_rate,
    }
    tax_meta = {
        "tax_rate_pct_input": st.session_state["tax_rate_pct_input"],
        "sc_county_checkbox": st.session_state["sc_county_checkbox"],
    }
    discount_meta = {
        "active_discount_type": st.session_state["active_discount_type"],
        "discount_note": st.session_state["discount_note"],
        "manager_pricing_authorized": st.session_state["manager_pricing_authorized"],
    }

    return {
        "quote_no": quote_no,
        "date": get_pacific_now().isoformat(),
        "customer": st.session_state["customer"],
        "line_items": st.session_state["line_items"],
        "fees": fees,
        "totals": totals,
        "tax_meta": tax_meta,
        "discount_meta": discount_meta,
        "freight_notes": st.session_state["freight_notes"],
        "footer_notes": st.session_state["footer_notes"],
        "order_meta": order_meta,
    }


def move_item(item_id: str, direction: str):
    items = st.session_state["line_items"]
    try:
        current_index = next(i for i, item in enumerate(items) if item["id"] == item_id)
    except StopIteration:
        return

    new_index = current_index

    if direction == "up" and current_index > 0:
        new_index = current_index - 1
    elif direction == "down" and current_index < len(items) - 1:
        new_index = current_index + 1

    if new_index != current_index:
        items[current_index], items[new_index] = items[new_index], items[current_index]
        ensure_course_discount_position(items)
        st.session_state["rerun_flag"] = True


def move_item_up(item_id: str):
    move_item(item_id, "up")


def move_item_down(item_id: str):
    move_item(item_id, "down")


def remove_item(item_id):
    line_items_before = len(st.session_state["line_items"])
    st.session_state["line_items"] = [
        item for item in st.session_state["line_items"] if item["id"] != item_id
    ]
    if line_items_before != len(st.session_state["line_items"]):
        if ensure_course_discount(st.session_state["line_items"]):
            st.session_state["rerun_flag"] = True
        else:
            st.session_state["rerun_flag"] = True


def add_item_callback(sku: str = ""):
    new_id = str(uuid.uuid4())
    sku = (sku or "").upper().strip()
    notes = ""

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
        "Notes": notes,
        "prev_sku": "",
        "previewChecked": True,
        "exclude_from_10_discount": False,
    })

    st.session_state[f"Notes_input_{new_id}"] = notes
    ensure_course_discount_position(st.session_state["line_items"])
    st.session_state["rerun_flag"] = True


STOCK_NUMBER_PLATE_SKU = "NP"
STOCK_NUMBER_PLATE_QTY_NOTES = {
    9: "#1-9",
    10: "#1-9, P",
    18: "#1-18",
    19: "#1-18, P",
}


def apply_stock_number_plate_qty_note(item: dict):
    if item.get("sku") != STOCK_NUMBER_PLATE_SKU:
        return

    note_key = f"Notes_input_{item['id']}"
    qty = int(item.get("qty", 0))
    current_note = st.session_state.get(note_key, item.get("Notes", ""))
    generated_notes = set(STOCK_NUMBER_PLATE_QTY_NOTES.values())
    new_note = STOCK_NUMBER_PLATE_QTY_NOTES.get(qty)

    if new_note:
        item["Notes"] = new_note
        st.session_state[note_key] = new_note
    elif current_note in generated_notes:
        item["Notes"] = ""
        st.session_state[note_key] = ""


def handle_quantity_change(item_id: str):
    items = st.session_state["line_items"]

    for item in items:
        if item["id"] == item_id:
            item_qty = int(st.session_state[f"qty_input_{item_id}"])
            item_unit = float(item.get("unit", 0.0))
            item["qty"] = item_qty
            item["total"] = round(item_qty * item_unit, 2)
            apply_stock_number_plate_qty_note(item)
            break

    if ensure_course_discount(items):
        st.session_state["rerun_flag"] = True


def handle_line_item_notes_change(item_id: str):
    note_key = f"Notes_input_{item_id}"
    for item in st.session_state["line_items"]:
        if item["id"] == item_id:
            item["Notes"] = st.session_state.get(note_key, "")
            break


def handle_footer_notes_change():
    st.session_state["footer_notes_touched"] = True


def handle_show_pdf_preview_toggle():
    st.session_state["show_pdf_preview_touched"] = True


def search_pipedrive_callback():
    term = st.session_state.get("pd_term", "").strip()
    if term:
        try:
            st.session_state["pd_matches"] = pd_search_persons(term)
        except Exception as e:
            st.error(f"Pipedrive search failed: {e}")
            st.session_state["pd_matches"] = []
    else:
        st.session_state["pd_matches"] = []


def load_quote_payload_into_session(payload: dict, selected_quote_no: str):
    st.session_state["quote_no"] = selected_quote_no
    st.session_state["customer"] = payload.get("customer", {})
    st.session_state["line_items"] = payload.get("line_items", [])
    for item in st.session_state["line_items"]:
        item.setdefault("exclude_from_10_discount", False)

    fees = payload.get("fees", {})
    st.session_state["drop_fee_input"] = float(fees.get("drop_ship_fee", 0.0))
    st.session_state["freight_fee_input"] = float(fees.get("freight", 0.0))
    st.session_state["freight_notes"] = payload.get("freight_notes", "")
    sync_freight_checkboxes_from_text(st.session_state["freight_notes"])

    tax_meta = payload.get("tax_meta", {})
    st.session_state["tax_rate_pct_input"] = float(tax_meta.get("tax_rate_pct_input", DEFAULT_TAX * 100))
    st.session_state["sc_county_checkbox"] = bool(tax_meta.get("sc_county_checkbox", False))

    discount_meta = payload.get("discount_meta", {})
    active_discount_type = discount_meta.get("active_discount_type", "")
    if not active_discount_type and discount_meta.get("apply_10_discount", False):
        active_discount_type = "team"
    sync_discount_checkboxes_from_type(active_discount_type)
    st.session_state["discount_note"] = discount_meta.get("discount_note", "")

    manager_authorized = bool(discount_meta.get("manager_pricing_authorized", False))
    st.session_state["manager_pricing_authorized"] = manager_authorized
    st.session_state["manager_pricing_checkbox"] = manager_authorized
    st.session_state["manager_clear_credentials_on_rerun"] = False
    clear_manager_credentials()

    st.session_state["footer_notes"] = payload.get("footer_notes", st.session_state["footer_notes"])
    st.session_state["footer_notes_touched"] = True
    st.session_state["billing_same_as_shipping"] = False

    order_meta = payload.get("order_meta", {})
    st.session_state["order_po_number"] = order_meta.get("po_number", "")
    st.session_state["order_operator"] = order_meta.get("operator", "CZ")
    st.session_state["order_auth_code"] = order_meta.get("auth_code", order_meta.get("terms", "AP - "))
    st.session_state["order_comm_to"] = order_meta.get("commission_to", "")
    st.session_state["order_check_number"] = order_meta.get("check_number", "")
    st.session_state["order_date_received"] = order_meta.get("date_received", "")

    loaded_doc_number = order_meta.get("order_doc_number", st.session_state["quote_no"])
    st.session_state["order_doc_number_pdf"] = loaded_doc_number or st.session_state["quote_no"]

    for item in st.session_state["line_items"]:
        item_id = item.get("id")
        if item_id:
            st.session_state[f"Notes_input_{item_id}"] = item.get("Notes", item.get("notes", ""))

    st.session_state["customer_key_suffix"] += 1


def render_saved_quote_search_ui():
    st.text_input(
        "Search saved quotes",
        key="person_quote_search",
        placeholder="e.g. 0107, Cesar Zermeno, discgolf.com, cesar@discgolf.com",
    )

    search_term = st.session_state.get("person_quote_search", "")
    if search_term.strip():
        all_quotes_df = get_saved_quotes_snapshot()
        if all_quotes_df.empty:
            st.info("Saved quotes are temporarily unavailable. Try again in a minute.")
            return

        person_matches_df = search_saved_quotes(all_quotes_df, search_term)
        if person_matches_df.empty:
            st.info("No saved quotes matched that Doc # / person / company / email search.")
        else:
            match_labels = [format_saved_quote_match(row) for _, row in person_matches_df.iterrows()]
            default_match_index = 0
            current_match_label = st.session_state.get("person_quote_match_label", "")
            if current_match_label in match_labels:
                default_match_index = match_labels.index(current_match_label)

            selected_match_label = st.selectbox(
                "Matching saved quotes",
                match_labels,
                index=default_match_index,
                key="person_quote_match_select",
            )
            st.session_state["person_quote_match_label"] = selected_match_label

            if st.button("Load Matching Quote", key="btn_load_person_quote_match"):
                selected_index = match_labels.index(selected_match_label)
                selected_row = person_matches_df.iloc[selected_index]
                selected_doc_no = str(selected_row.get("Doc #", "") or selected_row.get("Quote #", ""))
                load_saved_document(all_quotes_df, selected_doc_no)
                st.success(f"Loaded document **{selected_doc_no}** from saved quote search.")
                st.rerun()


def render_pipedrive_lookup_ui():
    if not PIPEDRIVE_API_TOKEN:
        st.warning("Pipedrive API Token not configured in environment variables. Lookup disabled.")
        return

    term = st.text_input(
        "Search term",
        placeholder="e.g. jane@city.gov or Jane Smith",
        key="pd_term",
        on_change=search_pipedrive_callback
    )

    matches = st.session_state.get("pd_matches", [])

    if matches:
        labels = [f"{m['name']}  <{m['email']}>" if m["email"] else m["name"] for m in matches]
        choice = st.selectbox("Matches", labels, key="pd_choice")
        idx = labels.index(choice) if choice in labels else -1
        if idx >= 0:
            sel = matches[idx]
            if st.button("Apply to form", key="pd_apply_btn"):
                try:
                    person = pd_get_person(sel["id"])
                    org_id = _pd_scalar(person.get("org_id")) if person and person.get("org_id") else None
                    org = pd_get_org(org_id) if org_id else None

                    mapped = pd_person_to_customer(person or {}, org)
                    cust = st.session_state["customer"]
                    for k, v in mapped.items():
                        cust[k] = v or cust.get(k, "")

                    st.session_state["customer_key_suffix"] += 1
                    st.success("Pipedrive contact applied to form (Person details -> Org fallback).")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to fetch or apply contact details. Check console: {e}")
    elif term and not matches:
        st.info(f"No Pipedrive contacts found matching '{term}'.")


def load_saved_document(all_quotes_df: pd.DataFrame, selected_doc_no: str):
    if all_quotes_df.empty or "Doc #" not in all_quotes_df.columns:
        raise ValueError("Saved quotes are unavailable right now. Please try again in a moment.")

    target_row_df = all_quotes_df[all_quotes_df["Doc #"] == selected_doc_no]
    if target_row_df.empty and "Quote #" in all_quotes_df.columns:
        target_row_df = all_quotes_df[all_quotes_df["Quote #"] == selected_doc_no]

    if target_row_df.empty:
        raise ValueError(f"Quote/Order # {selected_doc_no} not found in the loaded data.")

    payload = target_row_df.iloc[-1]["Payload"]
    load_quote_payload_into_session(payload, selected_doc_no)
    return payload


def render_exact_pdf_preview(
    template: str = "quote",
    height: str = "80vh",
    mode: str = "pdf",
    zoom_percent: int = 100,
):
    if st.session_state["sc_county_checkbox"]:
        tax_rate = SANTA_CRUZ_TAX_RATE
    else:
        tax_input = float(st.session_state.get("tax_rate_pct_input", 0.0))
        tax_rate = tax_input / 100 if tax_input > 0 else 0.0

    subtotal = sum(float(r["total"]) for r in st.session_state["line_items"] if r.get("previewChecked", True))
    discount_type = st.session_state["active_discount_type"]
    primary_discount_label = get_discount_label(discount_type)
    discountable_base = calculate_discountable_subtotal(st.session_state["line_items"])
    primary_discount_amount = calculate_primary_discount(st.session_state["line_items"], discount_type)
    manager_discount_amount = calculate_manager_discount(
        discountable_base,
        st.session_state["manager_pricing_authorized"]
    )

    drop_ship_fee = st.session_state["drop_fee_input"]
    freight = st.session_state["freight_fee_input"]
    pre_tax = subtotal - primary_discount_amount - manager_discount_amount + float(drop_ship_fee) + float(freight)
    sales_tax = round(pre_tax * tax_rate, 2)
    grand_total = round(pre_tax + sales_tax, 2)

    preview_payload = get_current_payload(
        subtotal,
        drop_ship_fee,
        freight,
        sales_tax,
        grand_total,
        tax_rate,
        primary_discount_amount,
        primary_discount_label,
        manager_discount_amount,
    )

    return render_pdf_preview_from_payload(
        preview_payload,
        template=template,
        height=height,
        mode=mode,
        zoom_percent=zoom_percent,
    )


def render_builder_sidebar_preview():
    with st.sidebar:
        with st.container(key="sidebar_preview_controls"):
            preview_is_live = st.session_state.get("show_pdf_preview", True)
            preview_status = "Live PDF" if preview_is_live else "Hidden"
            preview_status_class = "preview-toolbar-status" if preview_is_live else "preview-toolbar-status is-off"
            st.markdown(
                f"""
                <div class="preview-toolbar-heading">
                    <div>
                        <div class="preview-toolbar-kicker">Quote Preview</div>
                        <div class="preview-toolbar-doc">{st.session_state['quote_no']}</div>
                    </div>
                    <div class="{preview_status_class}">{preview_status}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            doc_col1, doc_col2 = st.columns(2)
            if doc_col1.button("New Quote", key="sidebar_new_quote", use_container_width=True):
                request_new_quote()
            if doc_col2.button("New Version", key="sidebar_new_version", type="primary", use_container_width=True):
                assign_new_quote_version()

            if hasattr(st, "toggle"):
                st.toggle(
                    "Live preview",
                    key="show_pdf_preview",
                    on_change=handle_show_pdf_preview_toggle,
                    help="Keep the PDF preview synced while editing.",
                )
            else:
                st.checkbox(
                    "Live preview",
                    key="show_pdf_preview",
                    on_change=handle_show_pdf_preview_toggle,
                    help="Keep the PDF preview synced while editing.",
                )

        if st.session_state["show_pdf_preview"]:
            try:
                render_exact_pdf_preview(
                    template="quote",
                    height="calc(100vh - 310px)",
                    mode="image",
                    zoom_percent=100,
                )
            except Exception as e:
                st.error(f"Preview unavailable: {e}")


def maybe_render_query_preview(all_quotes_df: pd.DataFrame) -> bool:
    try:
        query_params = st.query_params
    except Exception:
        return False

    selected_doc_no = str(query_params.get("doc", "") or "").strip()
    preview_template = str(query_params.get("preview", "") or "").strip().lower()
    pdf_only = str(query_params.get("pdf_only", "") or "").strip().lower() in {"1", "true", "yes"}

    if not selected_doc_no or preview_template not in {"quote", "order"}:
        return False

    loaded_signature = f"{selected_doc_no}:{preview_template}"
    if st.session_state.get("query_preview_loaded") != loaded_signature:
        try:
            load_saved_document(all_quotes_df, selected_doc_no)
        except ValueError as exc:
            st.warning(str(exc))
            return False
        st.session_state["query_preview_loaded"] = loaded_signature

    if pdf_only:
        st.markdown(
            """
            <style>
                [data-testid="stImage"] img {
                    max-width: 110px !important;
                    height: auto !important;
                }
                .preview-header-tight h1 {
                    margin: 0;
                    font-size: 1.15rem;
                    line-height: 1.05;
                }
                .preview-header-tight p {
                    margin: 3px 0 0;
                    color: rgba(250, 250, 250, 0.72);
                    font-size: 0.78rem;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            <div class="preview-header-tight">
                <h1>DGA {preview_template.title()} Preview</h1>
                <p>Document {selected_doc_no}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        render_exact_pdf_preview(template=preview_template, height="92vh")
        return True

    st.session_state["show_pdf_preview"] = True
    return False


def has_query_preview_request() -> bool:
    try:
        query_params = st.query_params
    except Exception:
        return False

    selected_doc_no = str(query_params.get("doc", "") or "").strip()
    preview_template = str(query_params.get("preview", "") or "").strip().lower()
    return bool(selected_doc_no and preview_template in {"quote", "order"})


def render_processed_orders_history(all_quotes_df: pd.DataFrame) -> None:
    explicit_record_type = all_quotes_df.get("Explicit Record Type", all_quotes_df.get("Record Type", "")).astype(str).str.lower()
    orders_df = all_quotes_df[explicit_record_type == "order"].copy()

    if orders_df.empty:
        st.info("No processed orders are available yet.")
        return

    orders_df["Sort Date"] = pd.to_datetime(orders_df["Date"], errors="coerce")
    orders_df = orders_df.sort_values(by=["Sort Date", "Doc #"], ascending=[False, False], na_position="last")
    orders_df = orders_df.drop_duplicates(subset=["Doc #"], keep="first")

    st.caption("Browse processed orders from the dropdown, or use quick search to narrow the list.")

    search_term = st.text_input(
        "Quick Search Processed Orders",
        key="processed_order_search",
        placeholder="Order #, company, customer, source quote #, or email",
    ).strip()

    filtered_orders_df = orders_df
    if search_term:
        needle = search_term.lower()
        filtered_orders_df = orders_df[
            orders_df.apply(lambda row: needle in build_processed_order_search_blob(row), axis=1)
        ].copy()

        if filtered_orders_df.empty:
            st.warning("No processed orders matched that quick search. Showing the full processed order list instead.")
            filtered_orders_df = orders_df

    options = filtered_orders_df["Doc #"].astype(str).tolist()
    label_map = {
        str(row["Doc #"]): format_processed_order_label(row)
        for _, row in filtered_orders_df.iterrows()
    }
    current_selected = st.session_state.get("processed_order_selected", "")

    selected_doc = st.selectbox(
        "Select a processed order",
        [""] + options,
        index=([""] + options).index(current_selected) if current_selected in options else 0,
        format_func=lambda doc: "Choose a matching order" if doc == "" else label_map.get(doc, doc),
        key="processed_order_selected",
    )

    if not selected_doc:
        st.info(f"{len(options)} order{'s' if len(options) != 1 else ''} available in the current list. Pick one to view details.")
        return

    selected_row = filtered_orders_df[filtered_orders_df["Doc #"].astype(str) == str(selected_doc)].iloc[0]
    selected_doc = str(selected_row.get("Doc #", "") or "")
    payload = selected_row.get("Payload", {}) or {}
    customer = payload.get("customer", {}) if isinstance(payload, dict) else {}
    line_items = payload.get("line_items", []) if isinstance(payload, dict) else []
    totals = payload.get("totals", {}) if isinstance(payload, dict) else {}
    warehouse_snapshot = load_warehouse_status_snapshot()
    queue_item = warehouse_snapshot.get("queue_by_order", {}).get(selected_doc, {})
    inventory_applied = selected_doc in warehouse_snapshot.get("applied_orders", set())
    queue_status = str(queue_item.get("status", "") or "").strip() or ("Inventory Applied" if inventory_applied else "Not in warehouse queue")
    tracking_number = str(queue_item.get("trackingNumber", "") or "").strip()
    freight_pro_number = str(queue_item.get("freightProNumber", "") or "").strip()

    with st.sidebar:
        st.markdown("### Order PDF Preview")
        pdf_data, pdf_doc_number = render_pdf_preview_from_payload(payload, template="order", height="78vh")

    detail_col, actions_col = st.columns([2.2, 1.1], gap="large")
    with detail_col:
        st.markdown("### Processed Order Details")
        st.caption(
            f"Showing {format_processed_order_label(selected_row)}"
            + (f" • {len(filtered_orders_df)} order{'s' if len(filtered_orders_df) != 1 else ''} in current list" if len(filtered_orders_df) > 1 else "")
        )
        meta_left, meta_right = st.columns(2)
        with meta_left:
            st.write(f"**Order #:** {selected_row.get('Order #') or selected_row.get('Doc #') or 'N/A'}")
            st.write(f"**Source Quote #:** {selected_row.get('Source Quote #') or 'N/A'}")
            st.write(f"**Customer:** {customer.get('company') or customer.get('bill_company') or selected_row.get('Company') or 'N/A'}")
            st.write(f"**Contact:** {customer.get('name') or customer.get('bill_name') or selected_row.get('Name') or 'N/A'}")
        with meta_right:
            st.write(f"**Email:** {customer.get('email') or customer.get('bill_email') or selected_row.get('Email') or 'N/A'}")
            st.write(f"**Date:** {str(selected_row.get('Date', '') or '')[:10] or 'N/A'}")
            st.write(f"**Grand Total:** {fmt_money(float(totals.get('grand_total', 0.0) or 0.0))}")
            st.write(f"**Line Items:** {len(line_items)}")

        st.markdown("#### Warehouse Update")
        warehouse_left, warehouse_right = st.columns(2)
        with warehouse_left:
            st.write(f"**Queue status:** {queue_status}")
            st.write(f"**Inventory:** {'Applied' if inventory_applied else 'Not applied'}")
        with warehouse_right:
            st.write(f"**UPS Tracking #:** {tracking_number or 'N/A'}")
            st.write(f"**Freight PRO #:** {freight_pro_number or 'N/A'}")

        warehouse_error = warehouse_snapshot.get("error", "")
        if warehouse_error:
            st.caption(f"Warehouse update unavailable right now: {warehouse_error}")

    with actions_col:
        st.markdown("### Actions")
        preview_url = f"{QUOTE_TOOL_URL}?{urlencode({'doc': str(selected_doc), 'preview': 'order', 'pdf_only': '1'})}"
        if hasattr(st, "link_button"):
            st.link_button("Open exact order preview", preview_url, use_container_width=True)
        else:
            st.markdown(f"[Open exact order preview]({preview_url})")

        st.download_button(
            "Download order",
            data=pdf_data,
            file_name=f"{pdf_doc_number}_Order.pdf",
            mime="application/pdf",
            use_container_width=True,
            type="primary",
            key=f"download_processed_order_{pdf_doc_number}",
        )

        if hasattr(st, "link_button"):
            st.link_button("Open Warehouse Queue / Inventory", WAREHOUSE_QUEUE_URL, use_container_width=True)
        else:
            st.markdown(f"[Open Warehouse Queue / Inventory]({WAREHOUSE_QUEUE_URL})")


def main_app():
    all_quotes_df = get_saved_quotes_snapshot() if has_query_preview_request() else empty_saved_quotes_df()
    if maybe_render_query_preview(all_quotes_df):
        return
    header_col1, header_col2, header_col3 = st.columns([1.1, 2.8, 0.9])
    with header_col1:
        if APP_LOGO_PATH:
            st.image(APP_LOGO_PATH, use_container_width=True)
    with header_col2:
        st.title("DGA Quoting Tool")
    with header_col3:
        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
        if hasattr(st, "link_button"):
            st.link_button("Open Operations Hub", OPERATIONS_HUB_URL, use_container_width=True)
        else:
            st.markdown(f"[Open Operations Hub]({OPERATIONS_HUB_URL})")
        if hasattr(st, "link_button"):
            st.link_button("Submit IT Ticket", QUOTE_TOOL_IT_TICKETS_URL, use_container_width=True)
        else:
            st.markdown(f"[Submit IT Ticket]({QUOTE_TOOL_IT_TICKETS_URL})")

    nav_col1, nav_col2, nav_col3 = st.columns([1.1, 1.1, 3.2])
    with nav_col1:
        if st.button(
            "Quote Builder",
            use_container_width=True,
            type="primary" if st.session_state.get("quote_workspace_view") == "builder" else "secondary",
        ):
            st.session_state["quote_workspace_view"] = "builder"
            st.rerun()
    with nav_col2:
        if st.button(
            "Processed Orders",
            use_container_width=True,
            type="primary" if st.session_state.get("quote_workspace_view") == "history" else "secondary",
        ):
            st.session_state["quote_workspace_view"] = "history"
            st.rerun()
    with nav_col3:
        st.caption("Use the quote builder for new docs and the processed-orders page for order history lookup.")

    if st.session_state.get("quote_workspace_view") == "history":
        all_quotes_df = get_saved_quotes_snapshot()
        render_processed_orders_history(all_quotes_df)
        return

    combined_patent_uri = _asset_data_uri(QUOTE_PATENT_TILE_PATH, "image/png")
    patent_markup = ""

    quote_view_css = """
        <style>
            .stApp > header,
            .stApp [data-testid="stAppViewContainer"] {
                position: relative;
                z-index: 1;
                background-color: #0d0f14;
                background-image:
                    linear-gradient(rgba(13, 15, 20, 0.86), rgba(13, 15, 20, 0.86)),
                    url("__PATENT_URI__");
                background-repeat: no-repeat;
                background-position: center top;
                background-size: cover;
                background-attachment: fixed;
            }

            .main .block-container,
            .stApp [data-testid="stSidebar"] {
                position: relative;
                z-index: 2;
            }

            [data-testid="stSidebar"],
            [data-testid="stSidebar"][aria-expanded="true"],
            [data-testid="stSidebar"] > div,
            [data-testid="stSidebarContent"],
            [data-testid="stSidebarUserContent"],
            section[data-testid="stSidebar"],
            section[data-testid="stSidebar"] > div,
            .stApp [data-testid="stSidebar"],
            .stApp [data-testid="stSidebar"] > div:first-child {
                flex-basis: 600px !important;
                flex-shrink: 0 !important;
                max-width: 600px !important;
                min-width: 600px !important;
                width: 600px !important;
            }

            [data-testid="stSidebarUserContent"] {
                padding-left: 0.75rem !important;
                padding-right: 0.75rem !important;
            }

            [data-testid="stSidebar"] {
                background: #20242f !important;
            }

            [data-testid="stSidebar"] [data-testid="stBaseButton-headerNoPadding"],
            [data-testid="stExpandSidebarButton"] {
                display: none !important;
                visibility: hidden !important;
                pointer-events: none !important;
            }

            .st-key-sidebar_preview_controls {
                margin: 0.35rem 0 0.95rem !important;
                padding: 0.8rem !important;
                border: 1px solid rgba(210, 228, 255, 0.14) !important;
                border-radius: 10px !important;
                background: rgba(13, 18, 29, 0.42) !important;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04) !important;
            }

            .st-key-sidebar_preview_controls,
            .st-key-sidebar_preview_controls * {
                background-image: none !important;
            }

            .preview-toolbar-heading {
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 0.75rem;
                margin-bottom: 0.7rem;
            }

            .preview-toolbar-kicker {
                color: rgba(246, 248, 251, 0.62);
                font-size: 0.74rem;
                font-weight: 800;
                letter-spacing: 0;
                line-height: 1.1;
                text-transform: uppercase;
            }

            .preview-toolbar-doc {
                color: #f6f8fb;
                font-size: 1.05rem;
                font-weight: 800;
                line-height: 1.25;
                margin-top: 0.18rem;
            }

            .preview-toolbar-status {
                flex: 0 0 auto;
                border: 1px solid rgba(134, 232, 171, 0.36);
                border-radius: 999px;
                color: #c8f8d8;
                background: rgba(31, 138, 76, 0.18) !important;
                font-size: 0.72rem;
                font-weight: 800;
                line-height: 1;
                padding: 0.36rem 0.5rem;
            }

            .preview-toolbar-status.is-off {
                border-color: rgba(210, 228, 255, 0.18);
                color: rgba(246, 248, 251, 0.62);
                background: rgba(210, 228, 255, 0.08) !important;
            }

            .st-key-sidebar_preview_controls div[data-testid="stHorizontalBlock"] {
                gap: 0.55rem !important;
                margin-bottom: 0.45rem !important;
            }

            .st-key-sidebar_preview_controls .stButton > button {
                height: 34px !important;
                border-radius: 7px !important;
                font-size: 13px !important;
                font-weight: 800 !important;
            }

            .st-key-sidebar_preview_controls div[data-testid="stCheckbox"],
            .st-key-sidebar_preview_controls div[data-testid="stToggle"] {
                border-top: 1px solid rgba(210, 228, 255, 0.12) !important;
                margin-top: 0.55rem !important;
                padding-top: 0.65rem !important;
            }

            .st-key-sidebar_preview_controls div[data-testid="stCheckbox"] label,
            .st-key-sidebar_preview_controls div[data-testid="stToggle"] label {
                align-items: center !important;
                color: rgba(246, 248, 251, 0.9) !important;
                font-weight: 800 !important;
            }

            .st-key-sidebar_preview_controls div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input:checked) > div:first-child {
                background: #1f8a4c !important;
                border-color: rgba(165, 244, 190, 0.75) !important;
                box-shadow: 0 0 0 1px rgba(74, 222, 128, 0.26) !important;
            }

            .st-key-sidebar_preview_controls div[data-testid="stCheckbox"] label[data-baseweb="checkbox"] input + div,
            .st-key-sidebar_preview_controls div[data-testid="stCheckbox"] [data-testid="stWidgetLabel"],
            .st-key-sidebar_preview_controls div[data-testid="stCheckbox"] [data-testid="stWidgetLabel"] * {
                background: transparent !important;
                background-color: transparent !important;
            }

            [data-testid="stSidebar"] iframe,
            [data-testid="stSidebar"] [data-testid="stIFrame"],
            [data-testid="stSidebar"] [data-testid="stPdf"] {
                width: 100% !important;
                max-width: 100% !important;
            }

            @media (max-width: 900px) {
                [data-testid="stSidebar"],
                [data-testid="stSidebar"][aria-expanded="true"],
                [data-testid="stSidebar"] > div,
                [data-testid="stSidebarContent"],
                [data-testid="stSidebarUserContent"],
                section[data-testid="stSidebar"],
                section[data-testid="stSidebar"] > div,
                .stApp [data-testid="stSidebar"],
                .stApp [data-testid="stSidebar"] > div:first-child {
                    flex-basis: min(92vw, 600px) !important;
                    max-width: min(92vw, 600px) !important;
                    min-width: min(92vw, 600px) !important;
                    width: min(92vw, 600px) !important;
                }
            }

            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-customer_information_panel),
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-lookup_tools_panel),
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-line_items_panel),
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-fees_tax_totals_panel),
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-generate_pdf_panel),
            div[data-testid="stVerticalBlockBorderWrapper"]:has([class*="st-key-line_item_panel_"]),
            .st-key-lookup_tools_panel,
            .st-key-customer_information_panel,
            .st-key-line_items_panel,
            .st-key-fees_tax_totals_panel,
            .st-key-generate_pdf_panel,
            [class*="st-key-line_item_panel_"] {
                position: relative !important;
                z-index: 3 !important;
                isolation: isolate !important;
                background: rgba(15, 24, 38, 0.46) !important;
                border: 1px solid rgba(255, 255, 255, 0.12) !important;
                border-radius: 16px !important;
                overflow: hidden !important;
                background-clip: padding-box !important;
                backdrop-filter: blur(6px) !important;
                -webkit-backdrop-filter: blur(6px) !important;
            }

            .st-key-lookup_tools_panel,
            .st-key-customer_information_panel,
            .st-key-line_items_panel,
            .st-key-fees_tax_totals_panel,
            .st-key-generate_pdf_panel,
            [class*="st-key-line_item_panel_"] {
                background: rgba(15, 24, 38, 0.46) !important;
                background-image: none !important;
                box-shadow: inset 0 0 0 9999px rgba(15, 24, 38, 0.46) !important;
                backdrop-filter: blur(6px) !important;
                -webkit-backdrop-filter: blur(6px) !important;
            }

            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-lookup_tools_panel) > div,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-lookup_tools_panel) *,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-customer_information_panel) > div,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-customer_information_panel) * ,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-line_items_panel) > div,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-line_items_panel) *,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-fees_tax_totals_panel) > div,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-fees_tax_totals_panel) *,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-generate_pdf_panel) > div,
            div[data-testid="stVerticalBlockBorderWrapper"]:has(.st-key-generate_pdf_panel) *,
            div[data-testid="stVerticalBlockBorderWrapper"]:has([class*="st-key-line_item_panel_"]) > div,
            div[data-testid="stVerticalBlockBorderWrapper"]:has([class*="st-key-line_item_panel_"]) *,
            .st-key-lookup_tools_panel,
            .st-key-lookup_tools_panel *,
            .st-key-customer_information_panel,
            .st-key-customer_information_panel *,
            .st-key-line_items_panel,
            .st-key-line_items_panel *,
            .st-key-fees_tax_totals_panel,
            .st-key-fees_tax_totals_panel *,
            .st-key-generate_pdf_panel,
            .st-key-generate_pdf_panel *,
            [class*="st-key-line_item_panel_"],
            [class*="st-key-line_item_panel_"] * {
                background-color: transparent !important;
                background-image: none !important;
            }

            .stButton>button {
                white-space: nowrap !important;
                font-size: 14px;
                line-height: 1.0;
                height: 38px;
                margin-top: 0px;
                background: #18263c !important;
                border: 1px solid rgba(210, 228, 255, 0.24) !important;
                color: #f6f8fb !important;
                box-shadow: none !important;
            }

            .stButton>button:hover {
                background: #20314c !important;
                border-color: rgba(210, 228, 255, 0.34) !important;
            }

            .stButton>button:focus {
                border-color: rgba(147, 190, 255, 0.9) !important;
                box-shadow: 0 0 0 1px rgba(147, 190, 255, 0.45) !important;
            }

            .stTextInput input,
            .stTextArea textarea,
            .stNumberInput input,
            .stSelectbox [data-baseweb="select"] > div,
            .stMultiSelect [data-baseweb="select"] > div {
                background: #1c2b43 !important;
                border: 1px solid rgba(210, 228, 255, 0.28) !important;
                color: #f6f8fb !important;
                box-shadow: none !important;
            }

            .stNumberInput [data-testid="stNumberInputStepUp"],
            .stNumberInput [data-testid="stNumberInputStepDown"] {
                background: #1c2b43 !important;
                border-color: rgba(210, 228, 255, 0.28) !important;
                color: #f6f8fb !important;
            }

            .stTextInput input::placeholder,
            .stTextArea textarea::placeholder,
            .stNumberInput input::placeholder {
                color: rgba(246, 248, 251, 0.58) !important;
            }

            .stTextInput input:focus,
            .stTextArea textarea:focus,
            .stNumberInput input:focus,
            .stSelectbox [data-baseweb="select"] > div:focus-within,
            .stMultiSelect [data-baseweb="select"] > div:focus-within {
                border-color: rgba(147, 190, 255, 0.9) !important;
                box-shadow: 0 0 0 1px rgba(147, 190, 255, 0.45) !important;
            }

            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"] > :first-child {
                position: relative !important;
                width: 1.18rem !important;
                height: 1.18rem !important;
                border-radius: 0.28rem !important;
                border: 1.5px solid rgba(210, 228, 255, 0.42) !important;
                background: rgba(28, 43, 67, 0.7) !important;
                box-shadow: none !important;
                transition: background-color 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease !important;
            }

            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:hover > :first-child {
                border-color: rgba(143, 211, 255, 0.9) !important;
                background: rgba(41, 57, 86, 0.82) !important;
            }

            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input:focus) > :first-child,
            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input:focus-visible) > :first-child {
                border-color: rgba(143, 211, 255, 0.95) !important;
                box-shadow: 0 0 0 2px rgba(72, 179, 255, 0.3) !important;
            }

            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input:checked) > :first-child,
            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input[aria-checked="true"]) > :first-child {
                background: #1f8a4c !important;
                background-color: #1f8a4c !important;
                background-image: linear-gradient(#1f8a4c, #1f8a4c) !important;
                border-color: #a5f4be !important;
                box-shadow: 0 0 0 2px rgba(74, 222, 128, 0.28) !important;
            }

            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input:checked) > :first-child::after,
            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input[aria-checked="true"]) > :first-child::after {
                content: "" !important;
                position: absolute !important;
                left: 0.39rem !important;
                top: 0.15rem !important;
                width: 0.34rem !important;
                height: 0.66rem !important;
                border: solid #ffffff !important;
                border-width: 0 0.16rem 0.16rem 0 !important;
                transform: rotate(45deg) !important;
                filter: drop-shadow(0 0 1px rgba(0, 0, 0, 0.45));
            }

            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input:checked) > :first-child svg,
            div[data-testid="stCheckbox"] label[data-baseweb="checkbox"]:has(input[aria-checked="true"]) > :first-child svg {
                display: block !important;
                width: 0.82rem !important;
                height: 0.82rem !important;
                fill: #ffffff !important;
                stroke: #ffffff !important;
                stroke-width: 2.4px !important;
                opacity: 1 !important;
                filter: drop-shadow(0 0 1px rgba(0, 0, 0, 0.4));
            }

            div[data-testid="stExpander"] {
                border: 1px solid rgba(160, 196, 255, 0.24) !important;
                border-radius: 12px !important;
                background: #162235 !important;
                overflow: hidden !important;
            }

            div[data-testid="stExpander"] summary {
                background: #b3262d !important;
                color: #fff7f7 !important;
                font-weight: 700 !important;
            }

            div[data-testid="stExpander"] summary > div,
            div[data-testid="stExpander"] summary * {
                background: transparent !important;
                background-image: none !important;
            }

            div[data-testid="stExpander"] summary:hover {
                background: #c7333a !important;
            }

            div[data-testid="stExpander"] details > div {
                background: #142033 !important;
                border-top: 1px solid rgba(160, 196, 255, 0.16) !important;
            }

            .st-key-generate_pdf_panel .stButton > button {
                background: #b3262d !important;
                border-color: rgba(255, 120, 120, 0.5) !important;
                color: #fff7f7 !important;
            }

            .st-key-generate_pdf_panel .stButton > button * {
                background: transparent !important;
                background-image: none !important;
                color: inherit !important;
                text-shadow: none !important;
            }

            .st-key-generate_pdf_panel .stButton > button:hover {
                background: #c7333a !important;
                border-color: rgba(255, 150, 150, 0.62) !important;
            }

            .st-key-generate_pdf_panel .stButton > button:focus {
                border-color: rgba(255, 190, 190, 0.95) !important;
                box-shadow: 0 0 0 1px rgba(255, 120, 120, 0.4) !important;
            }

            .st-key-top_new_version button,
            .st-key-sidebar_new_version button,
            .st-key-generate_pdf_panel .st-key-bottom_new_version button,
            .st-key-bottom_new_version button {
                background: #b3262d !important;
                border: 1px solid rgba(255, 120, 120, 0.5) !important;
                color: #fff7f7 !important;
                box-shadow: none !important;
            }

            .st-key-top_new_version button *,
            .st-key-sidebar_new_version button *,
            .st-key-generate_pdf_panel .st-key-bottom_new_version button *,
            .st-key-bottom_new_version button * {
                background: transparent !important;
                background-image: none !important;
                color: inherit !important;
                text-shadow: none !important;
            }

            .st-key-top_new_version button:hover,
            .st-key-sidebar_new_version button:hover,
            .st-key-generate_pdf_panel .st-key-bottom_new_version button:hover,
            .st-key-bottom_new_version button:hover {
                background: #c7333a !important;
                border-color: rgba(255, 150, 150, 0.62) !important;
            }

            .st-key-top_new_version button:focus,
            .st-key-sidebar_new_version button:focus,
            .st-key-generate_pdf_panel .st-key-bottom_new_version button:focus,
            .st-key-bottom_new_version button:focus {
                border-color: rgba(255, 190, 190, 0.95) !important;
                box-shadow: 0 0 0 1px rgba(255, 120, 120, 0.4) !important;
            }

            .st-key-top_new_quote button,
            .st-key-sidebar_new_quote button,
            .st-key-generate_pdf_panel .st-key-bottom_new_quote button,
            .st-key-bottom_new_quote button {
                background: #1f8a4c !important;
                border: 1px solid rgba(134, 232, 171, 0.42) !important;
                color: #f4fff7 !important;
                box-shadow: none !important;
            }

            .st-key-top_new_quote button *,
            .st-key-sidebar_new_quote button *,
            .st-key-generate_pdf_panel .st-key-bottom_new_quote button *,
            .st-key-bottom_new_quote button * {
                background: transparent !important;
                background-image: none !important;
                color: inherit !important;
                text-shadow: none !important;
            }

            .st-key-top_new_quote button:hover,
            .st-key-sidebar_new_quote button:hover,
            .st-key-generate_pdf_panel .st-key-bottom_new_quote button:hover,
            .st-key-bottom_new_quote button:hover {
                background: #269d57 !important;
                border-color: rgba(165, 244, 190, 0.55) !important;
            }

            .st-key-top_new_quote button:focus,
            .st-key-sidebar_new_quote button:focus,
            .st-key-generate_pdf_panel .st-key-bottom_new_quote button:focus,
            .st-key-bottom_new_quote button:focus {
                border-color: rgba(187, 247, 208, 0.95) !important;
                box-shadow: 0 0 0 1px rgba(74, 222, 128, 0.38) !important;
            }

            .st-key-generate_pdf_panel .st-key-generate_quote_pdf button,
            .st-key-generate_quote_pdf button {
                background: #24598f !important;
                border: 1px solid rgba(147, 197, 253, 0.5) !important;
                color: #f4f9ff !important;
                box-shadow: none !important;
            }

            .st-key-generate_pdf_panel .st-key-generate_quote_pdf button *,
            .st-key-generate_quote_pdf button * {
                background: transparent !important;
                background-image: none !important;
                color: inherit !important;
                text-shadow: none !important;
            }

            .st-key-generate_pdf_panel .st-key-generate_quote_pdf button:hover,
            .st-key-generate_quote_pdf button:hover {
                background: #2d6faf !important;
                border-color: rgba(191, 219, 254, 0.68) !important;
            }

            .st-key-generate_pdf_panel .st-key-generate_quote_pdf button:focus,
            .st-key-generate_quote_pdf button:focus {
                border-color: rgba(219, 234, 254, 0.95) !important;
                box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.42) !important;
            }

            .st-key-generate_pdf_panel .st-key-process_order_po button,
            .st-key-process_order_po button {
                background: #d8871d !important;
                border: 1px solid rgba(255, 214, 153, 0.72) !important;
                color: #fff8ec !important;
                box-shadow: 0 0 0 1px rgba(255, 185, 84, 0.16), 0 10px 24px rgba(216, 135, 29, 0.22) !important;
                font-weight: 800 !important;
            }

            .st-key-generate_pdf_panel .st-key-process_order_po button *,
            .st-key-process_order_po button * {
                background: transparent !important;
                background-image: none !important;
                color: inherit !important;
                text-shadow: none !important;
            }

            .st-key-generate_pdf_panel .st-key-process_order_po button:hover,
            .st-key-process_order_po button:hover {
                background: #f09a24 !important;
                border-color: rgba(255, 230, 180, 0.88) !important;
                box-shadow: 0 0 0 1px rgba(255, 206, 128, 0.24), 0 12px 28px rgba(240, 154, 36, 0.3) !important;
            }

            .st-key-generate_pdf_panel .st-key-process_order_po button:focus,
            .st-key-process_order_po button:focus {
                border-color: rgba(255, 244, 214, 0.98) !important;
                box-shadow: 0 0 0 1px rgba(251, 191, 36, 0.52) !important;
            }

            .stDownloadButton > button {
                background: #1f8a4c !important;
                border: 1px solid rgba(134, 232, 171, 0.42) !important;
                color: #f4fff7 !important;
                box-shadow: none !important;
            }

            .stDownloadButton > button:hover {
                background: #269d57 !important;
                border-color: rgba(165, 244, 190, 0.55) !important;
            }

            .stDownloadButton > button:focus {
                border-color: rgba(187, 247, 208, 0.95) !important;
                box-shadow: 0 0 0 1px rgba(74, 222, 128, 0.38) !important;
            }

            .stDownloadButton > button * {
                background: transparent !important;
                background-image: none !important;
                color: inherit !important;
                text-shadow: none !important;
            }

            div[data-testid="stVerticalBlock"] div[data-testid="stHorizontalBlock"] > div:nth-child(2) label {
                padding-top: 0;
            }

            div[data-testid*="stHorizontalBlock"] > div:nth-child(1) .stAlert {
                margin-top: -15px !important;
            }

            div.stVerticalBlock > div.stVerticalBlock > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) {
                display: none;
            }

            div[data-testid="stVerticalBlock"] > div > div > div:first-child[data-testid="stVerticalBlock"]:has(div.stAlert) {
                display: none;
            }

            .pdf-iframe-container {
                overflow: auto;
                height: 100vh;
            }
            .pdf-iframe-container iframe {
                width: 100%;
                height: 100%;
                border: 1px solid #ddd;
            }

            .pdf-image-preview-shell {
                background: #ffffff;
                border: 1px solid rgba(255, 255, 255, 0.16);
                border-radius: 4px;
                box-sizing: border-box;
                overflow: auto;
                padding: 10px;
            }

            .pdf-image-preview-shell img {
                display: block;
                height: auto;
                margin: 0 auto;
            }
        </style>
        __PATENT_MARKUP__
    """.replace("__PATENT_MARKUP__", patent_markup).replace("__PATENT_URI__", combined_patent_uri)
    st.markdown(quote_view_css, unsafe_allow_html=True)

    if st.session_state["rerun_flag"]:
        st.session_state["rerun_flag"] = False
        st.rerun()

    if st.session_state.get("manager_clear_credentials_on_rerun", False):
        clear_manager_credentials()
        st.session_state["manager_clear_credentials_on_rerun"] = False

    if not st.session_state.get("show_pdf_preview_touched", False):
        st.session_state["show_pdf_preview"] = True

    if not st.session_state.get("footer_notes_touched", False) and not st.session_state.get("footer_notes", "").strip():
        st.session_state["footer_notes"] = DEFAULT_FOOTER_NOTES

    if st.session_state.get("new_quote_dialog_open", False):
        render_new_quote_dialog()

    lookup_col1, lookup_col2, lookup_col3 = st.columns([1.2, 0.9, 0.9])
    cust_key_suffix = st.session_state["customer_key_suffix"]

    with lookup_col1:
        st.markdown("**Current Doc # (PT)**")
        st.info(st.session_state["quote_no"])

    with lookup_col2:
        st.markdown("<div style='min-height: 27px;'></div>", unsafe_allow_html=True)
        if st.button("New Quote", key="top_new_quote", use_container_width=True, type="secondary"):
            request_new_quote()

    with lookup_col3:
        st.markdown("<div style='min-height: 27px;'></div>", unsafe_allow_html=True)
        if st.button("New Version", key="top_new_version", use_container_width=True, type="primary",
                     help="Create a new version number based on the current quote."):
            assign_new_quote_version()

    with st.container(border=True, key="lookup_tools_panel"):
        st.subheader("Lookup Tools")
        lookup_tabs = st.tabs(["Saved Quotes", "Pipedrive"])
        with lookup_tabs[0]:
            render_saved_quote_search_ui()
        with lookup_tabs[1]:
            render_pipedrive_lookup_ui()

    c = st.session_state["customer"]

    with st.container(border=True, key="customer_information_panel"):
        st.subheader("Customer Information")
        cols_addr = st.columns(2)

        with cols_addr[0]:
            st.subheader("Shipping Address")
            st.markdown("<div style='min-height: 2.49rem;'></div>", unsafe_allow_html=True)
            c["company"] = st.text_input("Company", value=c.get("company", ""), key=f"ship_company_{cust_key_suffix}")
            c["name"] = st.text_input("Name", value=c.get("name", ""), key=f"ship_contact_name_{cust_key_suffix}")
            c["phone"] = st.text_input("Phone", value=c.get("phone", ""), key=f"ship_phone_{cust_key_suffix}")
            c["email"] = st.text_input("Email", value=c.get("email", ""), key=f"ship_email_{cust_key_suffix}")
            c["ship_addr1"] = st.text_area("Address Line 1", value=c.get("ship_addr1", ""), key=f"ship_addr1_{cust_key_suffix}")
            sc1, sc2, sc3 = st.columns(3)
            c["ship_city"] = sc1.text_input("City", value=c.get("ship_city", ""), key=f"ship_city_input_{cust_key_suffix}")
            c["ship_state"] = sc2.text_input("State", value=c.get("ship_state", ""), key=f"ship_state_input_{cust_key_suffix}")
            c["ship_zip"] = sc3.text_input("Zip", value=c.get("ship_zip", ""), key=f"ship_zip_input_{cust_key_suffix}")

        with cols_addr[1]:
            st.subheader("Billing Address")
            billing_same_as_shipping = st.checkbox(
                "Same as shipping",
                key="billing_same_as_shipping",
                help="Copy the shipping company, contact, phone, email, and address into billing."
            )
            if billing_same_as_shipping:
                sync_billing_from_shipping(c, cust_key_suffix)

            c["bill_company"] = st.text_input(
                "Company",
                value=c.get("bill_company", c.get("company", "")),
                key=f"bill_company_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )
            c["bill_name"] = st.text_input(
                "Name",
                value=c.get("bill_name", c.get("name", "")),
                key=f"bill_name_input_{cust_key_suffix}",
                help="This is the contact person for billing.",
                disabled=billing_same_as_shipping,
            )
            c["bill_phone"] = st.text_input(
                "Phone",
                value=c.get("bill_phone", c.get("phone", "")),
                key=f"bill_phone_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )
            c["bill_email"] = st.text_input(
                "Email",
                value=c.get("bill_email", c.get("email", "")),
                key=f"bill_email_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )
            c["bill_addr1"] = st.text_area(
                "Address Line 1 ",
                value=c.get("bill_addr1", ""),
                key=f"bill_addr1_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )
            bc1, bc2, bc3 = st.columns(3)
            c["bill_city"] = bc1.text_input(
                "City",
                value=c.get("bill_city", ""),
                key=f"bill_city_input_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )
            c["bill_state"] = bc2.text_input(
                "State",
                value=c.get("bill_state", ""),
                key=f"bill_state_input_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )
            c["bill_zip"] = bc3.text_input(
                "Zip",
                value=c.get("bill_zip", ""),
                key=f"bill_zip_input_{cust_key_suffix}",
                disabled=billing_same_as_shipping,
            )

    st.divider()

    with st.container(border=True, key="line_items_panel"):
        st.subheader("Line Items")
        st.button("Add Line Item", key="btn_add_line_top", on_click=add_item_callback)

        sku_to_name = PRODUCTS.set_index("SKU")["Name"].to_dict()
        sku_options_display = ["(custom)"] + [f"{s} — {sku_to_name.get(s, 'No Name')}" for s in PRODUCTS["SKU"].tolist()]

        ensure_course_discount(st.session_state["line_items"])
        ensure_course_discount_position(st.session_state["line_items"])

        for i in range(len(st.session_state["line_items"])):
            row = st.session_state["line_items"][i]
            row.setdefault("exclude_from_10_discount", False)
            is_course_discount = row.get("sku") == "CD"
            is_preview_checked = row.get("previewChecked", True)
            is_excluded_from_10 = row.get("exclude_from_10_discount", False)

            can_move_up = i > 0
            can_move_down = i < len(st.session_state["line_items"]) - 1

            item_container = st.container(border=True, key=f"line_item_panel_{row['id']}")
            with item_container:
                header_col1, header_col2, header_col3, header_col4, header_col5, header_col6 = st.columns([0.8, 0.4, 0.4, 0.4, 1.1, 1.4])

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
                if is_course_discount:
                    st.checkbox("Show in Preview", value=True, disabled=True, key=f"preview_check_{row['id']}",
                                help="Discount is always shown in preview.")
                else:
                    new_checked_state = st.checkbox("Show in Preview", value=is_preview_checked, key=f"preview_check_{row['id']}")
                    if new_checked_state != is_preview_checked:
                        row["previewChecked"] = new_checked_state
                        st.session_state["rerun_flag"] = True

            with header_col6:
                if is_course_discount:
                    st.checkbox("Exclude From 10% Discount", value=True, disabled=True, key=f"exclude_10_{row['id']}")
                    row["exclude_from_10_discount"] = True
                else:
                    new_exclude_state = st.checkbox(
                        "Exclude From 10% Discount",
                        value=is_excluded_from_10,
                        key=f"exclude_10_{row['id']}"
                    )
                    if new_exclude_state != is_excluded_from_10:
                        row["exclude_from_10_discount"] = new_exclude_state

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
                if is_course_discount:
                    st.markdown("**Auto-Discount**", help="This line is automatically calculated and non-editable.")
                    st.markdown(f"**{row['name']}**")
                else:
                    sku_selected_display = st.selectbox("Product Description", sku_options_display, index=sel_idx,
                                                        key=f"sku_select_{row['id']}")

                    new_notes = row.get("Notes", "")

                    if sku_selected_display == "(custom)":
                        new_sku = ""
                        new_name = prod_name
                        new_unit = prod_price
                    else:
                        parts = sku_selected_display.split("—", 1)
                        new_sku = parts[0].strip()

                        prod = PRODUCTS[PRODUCTS["SKU"] == new_sku]
                        if not prod.empty:
                            new_name = str(prod.iloc[0]["Name"])
                            new_unit = float(prod.iloc[0]["UnitPrice"]) if pd.notna(prod.iloc[0]["UnitPrice"]) else 0.0
                            if new_sku != "CD":
                                new_notes = str(prod.iloc[0]["Notes"]) if "Notes" in prod.columns and pd.notna(prod.iloc[0]["Notes"]) else ""
                        else:
                            new_name = parts[1].strip() if len(parts) > 1 else new_sku
                            new_unit = prod_price
                            if new_sku != "CD":
                                new_notes = ""

                    if new_sku != row["sku"]:
                        row["sku"] = new_sku
                        row["name"] = new_name
                        row["unit"] = new_unit
                        row["Notes"] = new_notes
                        row["prev_sku"] = new_sku if new_sku else "(custom)"
                        st.session_state[f"Notes_input_{row['id']}"] = new_notes
                        apply_stock_number_plate_qty_note(row)
                        st.session_state["rerun_flag"] = True

                    if not row["sku"] and not is_course_discount:
                        row["name"] = st.text_input("Custom Name (Required)", value=row["name"], key=f"name_input_{row['id']}")

            with c2:
                if is_course_discount:
                    st.markdown("**Qty**")
                    st.markdown(f"**{int(row['qty'])}**")
                else:
                    row["qty"] = st.number_input(
                        "Qty",
                        min_value=0,
                        value=int(row.get("qty", 1)),
                        step=1,
                        key=f"qty_input_{row['id']}",
                        on_change=handle_quantity_change,
                        args=(row["id"],)
                    )

            with c3:
                current_unit = float(row.get("unit", 0.0) if pd.notna(row.get("unit", 0.0)) else 0.0)

                if is_course_discount:
                    st.markdown("**Unit Price**")
                    st.markdown(f"**{fmt_money(current_unit)}**")
                else:
                    row["unit"] = st.number_input(
                        "Unit Price",
                        min_value=-100000.0,
                        value=current_unit,
                        step=0.01,
                        format="%.2f",
                        key=f"unit_input_{row['id']}_{row['sku'] or 'custom'}"
                    )

            with c4:
                row["total"] = round(float(row["qty"]) * float(row["unit"]), 2)
                st.markdown("**Total**")
                st.write(f"**{fmt_money(row['total'])}**")

            notes_key = f"Notes_input_{row['id']}"
            if notes_key not in st.session_state:
                st.session_state[notes_key] = row.get("Notes", "")

            st.text_area(
                "Notes (optional)",
                key=notes_key,
                height=68,
                on_change=handle_line_item_notes_change,
                args=(row["id"],),
            )
            row["Notes"] = st.session_state[notes_key]

        st.button("Add Line Item", key="btn_add_line_bottom", on_click=add_item_callback)

    with st.container(border=True, key="fees_tax_totals_panel"):
        st.subheader("Fees, Tax, and Totals")
        cc1, cc2, cc3, cc4, cc5, cc6, cc7, cc8 = st.columns(8)
        with cc1:
            drop_ship_fee = st.number_input("Drop-Ship Fee", min_value=0.0, step=1.0, key="drop_fee_input")
        with cc2:
            freight = st.number_input("Freight", min_value=0.0, step=1.0, key="freight_fee_input")
        with cc3:
            st.number_input("Sales Tax Rate (%)", min_value=0.0, step=0.01, key="tax_rate_pct_input")
        with cc4:
            st.checkbox(f"Use Santa Cruz County Sales Tax ({SANTA_CRUZ_TAX_RATE * 100:.2f}%)", key="sc_county_checkbox")
        with cc5:
            st.checkbox("Team Discount", key="team_discount_checkbox", on_change=handle_team_discount_toggle)
        with cc6:
            st.checkbox("Commission Discount", key="commission_discount_checkbox", on_change=handle_commission_discount_toggle)
        with cc7:
            st.checkbox("Discount", key="discount_checkbox", on_change=handle_discount_toggle)
        with cc8:
            st.checkbox("Manager Pricing", key="manager_pricing_checkbox", on_change=handle_manager_pricing_toggle)

        if st.session_state["active_discount_type"] == "discount":
            st.text_input("Discount Note (required)", key="discount_note", placeholder="Required reason for discount")

        if st.session_state["manager_pricing_checkbox"]:
            if not st.session_state["manager_pricing_authorized"]:
                mp1, mp2, mp3 = st.columns([1, 1, 0.8])
                with mp1:
                    st.text_input("Manager Username", key="manager_username")
                with mp2:
                    st.text_input("Manager Password", key="manager_password", type="password")
                with mp3:
                    st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
                    if st.button("Authorize Manager Pricing", key="btn_authorize_manager"):
                        authorize_manager_pricing()
            else:
                st.success("Manager pricing authorized.")

        st.markdown("**Freight Notes**")
        fn1, fn2, fn3, fn4 = st.columns(4)
        with fn1:
            st.checkbox("Business Address", key=_freight_note_key("Business Address"))
            st.checkbox("Residential Address", key=_freight_note_key("Residential Address"))
        with fn2:
            st.checkbox("Lift Gate Needed", key=_freight_note_key("Lift Gate Needed"))
            st.checkbox("Fork Lift Access", key=_freight_note_key("Fork Lift Access"))
        with fn3:
            st.checkbox("Loading Dock Access", key=_freight_note_key("Loading Dock Access"))
            st.checkbox("Local Pickup", key=_freight_note_key("Local Pickup"))
        with fn4:
            st.checkbox("UPS", key=_freight_note_key("UPS"))
            st.checkbox("Ground Freight", key=_freight_note_key("Ground Freight"))

        st.text_input(
            "Other Freight Notes",
            key="freight_notes_other",
            placeholder="Optional extra freight details"
        )
        fees_summary_slot = st.container()

    st.session_state["freight_notes"] = get_selected_freight_notes()

    tax_rate = SANTA_CRUZ_TAX_RATE if st.session_state["sc_county_checkbox"] else float(st.session_state["tax_rate_pct_input"]) / 100.0

    subtotal = sum(float(r["total"]) for r in st.session_state["line_items"] if r.get("previewChecked", True))
    discount_type = st.session_state["active_discount_type"]
    primary_discount_label = get_discount_label(discount_type)
    discountable_base = calculate_discountable_subtotal(st.session_state["line_items"])
    primary_discount_amount = calculate_primary_discount(st.session_state["line_items"], discount_type)
    manager_discount_amount = calculate_manager_discount(
        discountable_base,
        st.session_state["manager_pricing_authorized"]
    )

    pre_tax = subtotal - primary_discount_amount - manager_discount_amount + float(drop_ship_fee) + float(freight)
    sales_tax = round(pre_tax * tax_rate, 2)
    grand_total = round(pre_tax + sales_tax, 2)

    with fees_summary_slot:
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        with s1:
            st.metric("Subtotal", f"${subtotal:,.2f}")
        with s2:
            if primary_discount_label and primary_discount_amount > 0:
                st.metric(primary_discount_label, f"-${primary_discount_amount:,.2f}")
            else:
                st.metric("Primary Discount", "$0.00")
        with s3:
            if manager_discount_amount > 0:
                st.metric("Manager Pricing", f"-${manager_discount_amount:,.2f}")
            else:
                st.metric("Manager Pricing", "$0.00")
        with s4:
            st.metric("Drop-Ship Fee", f"${drop_ship_fee:,.2f}")
        with s5:
            st.metric("Freight", f"${freight:,.2f}")
        with s6:
            st.metric("Grand Total", f"${grand_total:,.2f}")

        qual_qty = eligible_qty_for_discount(st.session_state["line_items"])
        if qual_qty >= 9:
            st.success(f"Course Discount active: **-$100** × {qual_qty} qualifying baskets.")
        else:
            st.info(
                f"Qualifying baskets: {qual_qty}. Add {max(0, 9 - qual_qty)} more Mach 5/7/X (Std/Portable/No Frills) to trigger the Course Discount."
            )

    payload = get_current_payload(
        subtotal,
        drop_ship_fee,
        freight,
        sales_tax,
        grand_total,
        tax_rate,
        primary_discount_amount,
        primary_discount_label,
        manager_discount_amount,
    )
    order_meta = payload["order_meta"]

    render_builder_sidebar_preview()

    def discount_note_valid() -> bool:
        if st.session_state["active_discount_type"] != "discount":
            return True
        return bool(st.session_state.get("discount_note", "").strip())

    with st.container(border=True, key="generate_pdf_panel"):
        st.subheader("Generate PDF Documents")

        quote_no = st.session_state["quote_no"]
        action_col1, action_col2, action_col3 = st.columns([1.4, 0.9, 0.9])
        with action_col1:
            st.markdown(f"**Current Quote #:** `{quote_no}`")
        with action_col2:
            if st.button("New Quote", key="bottom_new_quote", type="secondary", use_container_width=True):
                request_new_quote()
        with action_col3:
            if st.button("New Version", key="bottom_new_version", type="primary", use_container_width=True):
                assign_new_quote_version()

        st.text_area("Footer Notes (shown on PDF)", key="footer_notes", on_change=handle_footer_notes_change)

        with st.expander("Order/PO Details (for Order PDF)", expanded=False):
            if not st.session_state.get("order_doc_number_pdf"):
                st.session_state["order_doc_number_pdf"] = st.session_state["quote_no"]

            order_col1, order_col2 = st.columns(2)
            with order_col1:
                st.text_input("Order/PO Document # (Used for Order PDF Header/File Name)", key="order_doc_number_pdf")
                st.text_input("P.O. Number", key="order_po_number")
                operator_options = ["CZ", "MP", "KG"]
                current_operator = st.session_state.get("order_operator", "CZ")
                if current_operator not in operator_options:
                    st.session_state["order_operator"] = "CZ"
                st.selectbox("Operator", operator_options, key="order_operator")
                st.text_input("Authorization Code", key="order_auth_code")
            with order_col2:
                st.text_input("Commission To", key="order_comm_to")
                st.text_input("Check Number", key="order_check_number")
                st.text_input("Date Received", key="order_date_received")

        pdf_col1, pdf_col2 = st.columns(2)

        if pdf_col1.button("Generate & SAVE Quote PDF", key="generate_quote_pdf", use_container_width=True, type="primary"):
            if not discount_note_valid():
                pdf_col1.error("Discount Reason is required when Discount is selected.")
            else:
                handle_pdf_generation(payload, quote_no, "quote", pdf_col1)

        if pdf_col2.button("Process as Order / PO", key="process_order_po", use_container_width=True, type="secondary"):
            if not discount_note_valid():
                pdf_col2.error("Discount Reason is required when Discount is selected.")
            else:
                order_doc_number = st.session_state["order_doc_number_pdf"]
                handle_pdf_generation(payload, order_doc_number, "order", pdf_col2, order_meta=order_meta)


if __name__ == "__main__":
    if is_health_check_request():
        render_health_check()
    else:
        main_app()
