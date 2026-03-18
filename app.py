import os
import io
import uuid
import json
import copy
from datetime import datetime
import requests
import re
import sys
from typing import Any
import pytz
import html.parser
import base64

import pandas as pd
import streamlit as st
import gspread

try:
    from PyPDF2 import PdfReader
except ImportError:
    PdfReader = None

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
DEFAULT_TAX = float(get_env("SALES_TAX_RATE_DEFAULT", 0.0, float))
SANTA_CRUZ_TAX_RATE = 0.0975

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
    "Lift Gate Need",
    "Fork Lift Access",
    "Loading Dock Access",
    "Local Pickup",
]


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


def fmt_money(value: float) -> str:
    return f"${value:,.2f}"


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
    return ""


def sync_discount_checkboxes_from_type(discount_type: str):
    st.session_state["team_discount_checkbox"] = discount_type == "team"
    st.session_state["commission_discount_checkbox"] = discount_type == "commission"
    st.session_state["active_discount_type"] = discount_type


def handle_team_discount_toggle():
    if st.session_state.get("team_discount_checkbox", False):
        st.session_state["commission_discount_checkbox"] = False
        st.session_state["active_discount_type"] = "team"
    elif st.session_state.get("active_discount_type") == "team":
        st.session_state["active_discount_type"] = ""


def handle_commission_discount_toggle():
    if st.session_state.get("commission_discount_checkbox", False):
        st.session_state["team_discount_checkbox"] = False
        st.session_state["active_discount_type"] = "commission"
    elif st.session_state.get("active_discount_type") == "commission":
        st.session_state["active_discount_type"] = ""


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


def calculate_ten_percent_discount(items: list[dict], discount_type: str) -> float:
    if not discount_type:
        return 0.0
    return round(calculate_discountable_subtotal(items) * 0.10, 2)


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
        return pd.DataFrame()

    try:
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.get_worksheet(0)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        if "Quote #" not in df.columns or "Quote JSON Payload" not in df.columns:
            st.error("Google Sheet missing required columns: 'Quote #' and 'Quote JSON Payload'.")
            return pd.DataFrame()

        df["Payload"] = df["Quote JSON Payload"].apply(lambda x: json.loads(x) if x else None)
        return df.dropna(subset=["Payload"])

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found. Check ID and sharing.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading quotes from sheet: {e}")
        return pd.DataFrame()


def save_quote_to_gsheet(payload: dict) -> bool:
    client = get_gsheet_client()
    if not client:
        return False

    try:
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.get_worksheet(0)

        doc_number = payload.get("order_meta", {}).get("order_doc_number") or payload.get("quote_no")

        row_data = [
            doc_number,
            payload.get("date"),
            payload.get("customer", {}).get("company", ""),
            payload.get("customer", {}).get("name", ""),
            payload.get("customer", {}).get("email", ""),
            payload.get("totals", {}).get("grand_total", 0.0),
            json.dumps(payload),
        ]

        worksheet.append_row(row_data, value_input_option="USER_ENTERED")
        load_all_quotes.clear()
        return True
    except Exception as e:
        st.error(f"Error saving quote to sheet: {e}")
        return False


@st.cache_data
def load_products(path: str = "products.csv") -> pd.DataFrame:
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


PRODUCTS = load_products()


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


def start_new_quote():
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
    st.session_state["freight_notes_other"] = ""
    for label in FREIGHT_NOTE_OPTIONS:
        st.session_state[_freight_note_key(label)] = False

    sync_discount_checkboxes_from_type("")
    st.session_state["tax_rate_pct_input"] = 0.0
    st.session_state["sc_county_checkbox"] = False
    st.session_state["footer_notes"] = (
        "Pricing subject to change. Please review all details carefully.\n"
        "International customers will be responsible for all duties and taxes upon delivery."
    )

    st.session_state["order_doc_number_pdf"] = ""
    st.session_state["order_po_number"] = ""
    st.session_state["order_operator"] = "CZ"
    st.session_state["order_auth_code"] = "AP - "
    st.session_state["order_comm_to"] = ""
    st.session_state["order_check_number"] = ""
    st.session_state["order_date_received"] = get_pacific_now().strftime("%m/%d/%y")

    st.session_state["quote_no"] = new_quote_number()
    st.session_state["customer_key_suffix"] += 1

    st.session_state["pd_matches"] = []
    st.session_state["pd_term"] = ""
    st.session_state["pd_expander_state"] = False
    st.session_state["show_pdf_preview"] = True
    st.rerun()


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
st.session_state.setdefault("freight_notes", "")
st.session_state.setdefault("freight_notes_other", "")
for label in FREIGHT_NOTE_OPTIONS:
    st.session_state.setdefault(_freight_note_key(label), False)

st.session_state.setdefault("active_discount_type", "")
st.session_state.setdefault("team_discount_checkbox", False)
st.session_state.setdefault("commission_discount_checkbox", False)

st.session_state.setdefault("tax_rate_pct_input", 0.0)
st.session_state.setdefault("sc_county_checkbox", False)
st.session_state.setdefault("order_doc_number_pdf", "")
st.session_state.setdefault("order_po_number", "")
st.session_state.setdefault("order_operator", "CZ")
st.session_state.setdefault("order_auth_code", "AP - ")
st.session_state.setdefault("order_comm_to", "")
st.session_state.setdefault("order_check_number", "")
st.session_state.setdefault("order_date_received", get_pacific_now().strftime("%m/%d/%y"))
st.session_state.setdefault("pd_matches", [])
st.session_state.setdefault("pd_expander_state", False)
st.session_state.setdefault("show_pdf_preview", True)


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
        state_zip_part = parts[2].upper()
        sz_parts = [p.strip() for p in state_zip_part.split() if p.strip()]

        for part in sz_parts:
            if len(part) == 2 and part.isalpha() and not state:
                state = part
            elif part.isdigit() and len(part) >= 5 and not zip_code:
                zip_code = part

            if state and zip_code:
                break

        if not state and len(state_zip_part) == 2 and state_zip_part.isalpha():
            state = state_zip_part
        if not zip_code and len(state_zip_part) >= 5 and state_zip_part.isdigit():
            zip_code = state_zip_part

    return _clean(street), _clean(city), _clean(state), _clean(zip_code)


def pd_person_to_customer(person: dict, org: dict | None = None) -> dict:
    name = _clean(person.get("name"))
    email = _clean(_pd_scalar(person.get("email")))
    phone = _clean(_pd_scalar(person.get("phone")))

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

    p_addr_formatted = _clean(person.get("address_formatted_address") or person.get("address"))
    o_addr_formatted = _clean((org or {}).get("address_formatted_address") or (org or {}).get("address"))

    p_addr_full = _extract_address_from_html(p_addr_formatted)
    o_addr_full = _extract_address_from_html(o_addr_formatted)

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

        if modified:
            ensure_course_discount_stays_last(items)

    elif idx != -1:
        items.pop(idx)
        modified = True

    return modified


def ensure_course_discount_stays_last(items: list[dict] = None):
    if items is None:
        items = st.session_state["line_items"]

    idx = find_course_discount_index(items)
    if idx != -1 and idx != len(items) - 1:
        discount_item = items.pop(idx)
        items.append(discount_item)


def _company_right_block(styles):
    return Paragraph(
        f"<b>Disc Golf Association (DGA)</b><br/>"
        f"73 Hangar Way<br/>"
        f"Watsonville, CA 95076<br/>"
        f"Phone: {COMPANY['phone']}",
        styles["LeftInfo"]
    )


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

    discount_label = totals.get("discount_label", "")
    discount_amount = totals.get("ten_percent_discount", 0.0)

    if template == "order":
        if COMPANY_LOGO_PATH:
            logo = Image(COMPANY_LOGO_PATH, width=logo_w, height=logo_h)
            logo.hAlign = "LEFT"
            company_info_block = _company_right_block(styles)
            left_logo_block = [logo, Spacer(1, block_spacer_small), company_info_block]

            hdr = Table([[left_logo_block, ""]], colWidths=[content_width / 2, content_width / 2])
            hdr.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("ALIGN", (0, 0), (0, 0), "LEFT")
            ]))
            hdr.hAlign = "LEFT"
            story += [hdr, Spacer(1, block_spacer_small)]
        else:
            story += [Paragraph(f"<b>{COMPANY['name']}</b><br/><i>{COMPANY['tagline']}</i>", styles["Title"]), Spacer(1, block_spacer_small)]

        story += [Paragraph(f"**ORDER: {doc_number}**", styles["Heading2"]), Spacer(1, block_spacer_small)]

        grouped_info_text = (
            f"Date: {get_pacific_now().strftime('%m/%d/%y')}<br/>"
            f"Operator: {meta.get('operator', '')}<br/>"
            f"Commission to: {meta.get('commission_to', '')}"
        )
        grouped_para = Paragraph(grouped_info_text, styles["LeftInfo"])

        info_tbl = Table([[grouped_para, ""]], colWidths=[content_width / 2, content_width / 2])
        info_tbl.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("ALIGN", (0, 0), (0, 0), "LEFT"),
        ]))
        info_tbl.hAlign = "LEFT"
        story += [info_tbl, Spacer(1, block_spacer_small)]

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
            f"Authorization Code: {meta.get('auth_code', '')}<br/>"
            f"Check Number: {meta.get('check_number', '')}<br/>"
            f"Date Received: {meta.get('date_received', '')}"
        )

        bill_block_order = (
            f"<b>Billing Address</b><br/>"
            f"{customer.get('bill_company', customer.get('company', ''))}<br/>"
            f"{customer.get('bill_name', customer.get('name', ''))}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('bill_phone', customer.get('phone', ''))}<br/>"
            f"{customer.get('bill_email', customer.get('email', ''))}"
        )

        addr_table = Table(
            [[Paragraph(ship_block_order, addr_style), Paragraph(bill_block_order, addr_style)]],
            colWidths=[content_width / 2, content_width / 2]
        )
        addr_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ]))
        addr_table.hAlign = "LEFT"
        story += [addr_table, Spacer(1, block_spacer_med)]

        header = ["Quantity", "Product Description", "Unit Price", "Total"]
        li_cols = [0.7 * inch, content_width - 0.7 * inch - 0.825 * inch - 0.825 * inch, 0.825 * inch, 0.825 * inch]
        data = [header]

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
                data.append(["", Paragraph(note_txt, notes_style), "", ""])

        t_li = Table(data, colWidths=li_cols, repeatRows=1)
        t_li.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.75, colors.black),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("ALIGN", (0, 1), (0, -1), "CENTER"),
            ("ALIGN", (2, 1), (3, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), row_top_pad),
            ("BOTTOMPADDING", (0, 0), (-1, -1), row_bottom_pad),
        ]))
        t_li.hAlign = "LEFT"
        story += [t_li]

        freight_notes_txt = freight_notes_meta.strip()
        if not freight_notes_txt and st.session_state.get("freight_notes"):
            freight_notes_txt = _prepare_text_for_pdf(st.session_state["freight_notes"], compact_level, "freight").strip()

        if freight_notes_txt:
            story += [Spacer(1, block_spacer_small), Paragraph(f"<b>Freight Notes:</b> {freight_notes_txt}", notes_style_2)]

        story += [Spacer(1, block_spacer_med)]

        sub_rows = [["Subtotal:", fmt_money(totals.get("subtotal", 0.0))]]
        if discount_label and discount_amount > 0:
            sub_rows.append([f"{discount_label}:", fmt_money(-discount_amount)])
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
            ("BACKGROUND", (0, -1), (-1, -1), colors.lightgrey),
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
        company_info_text = (
            f"<b>Disc Golf Association, Inc.</b><br/>"
            f"{COMPANY['addr1']}<br/>"
            f"{COMPANY['city']}, {COMPANY['state']} {COMPANY['zip']}"
        )
        company_info_para = Paragraph(company_info_text, styles["Normal"])

        if COMPANY_LOGO_PATH:
            logo = Image(COMPANY_LOGO_PATH, width=logo_w, height=logo_h)
            logo.hAlign = "LEFT"
            left_logo_block_elements = [logo, Spacer(1, block_spacer_small), company_info_para]
        else:
            left_logo_block_elements = [company_info_para]

        left_logo_block = Table([[elem] for elem in left_logo_block_elements], colWidths=[content_width / 2])
        left_logo_block.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))

        right_align_style = ParagraphStyle(
            "RightAlignStyle",
            parent=styles["Normal"],
            fontSize=max(8, addr_font),
            leading=max(9, addr_leading),
            alignment=TA_RIGHT
        )
        header_title_font = 14 if compact_level == 0 else (12 if compact_level == 1 else 10)
        header_title_leading = 16 if compact_level == 0 else (13 if compact_level == 1 else 11)
        title_para = Paragraph(
            "Quotation Form<br/>Pricing Subject to Change",
            ParagraphStyle("CompactQuoteHeaderTitle", parent=styles["Heading2"], alignment=TA_RIGHT,
                           fontSize=header_title_font, leading=header_title_leading)
        )
        contact_info_para = Paragraph(
            f"Phone: {COMPANY['phone']}<br/>Fax: {COMPANY['fax']}<br/>Web: {COMPANY['web']}",
            right_align_style
        )

        right_spacer_height = 40 if compact_level == 0 else (18 if compact_level == 1 else 8)
        right_title_block = Table(
            [[title_para], [Spacer(1, right_spacer_height)], [contact_info_para]],
            colWidths=[content_width / 2]
        )
        right_title_block.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (0, -1), "RIGHT"),
        ]))

        t = Table([[left_logo_block, right_title_block]], colWidths=[content_width / 2, content_width / 2])
        t.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (0, 0), "LEFT"),
            ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ]))
        t.hAlign = "LEFT"
        story += [t, Spacer(1, block_spacer_large)]

        date_quote_para = Paragraph(
            f"Date: {get_pacific_now().strftime('%Y-%m-%d')}<br/>Quote #: {doc_number}",
            styles["LeftInfo"]
        )
        t = Table([[date_quote_para]], colWidths=[content_width])
        t.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0)]))
        t.hAlign = "LEFT"
        story += [t, Spacer(1, block_spacer_med)]

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
            f"{customer.get('bill_company', customer.get('company', ''))}<br/>"
            f"{customer.get('bill_name', customer.get('name', ''))}<br/>"
            f"{customer.get('bill_addr1', '')}<br/>"
            f"{customer.get('bill_city', '')}, {customer.get('bill_state', '')} {customer.get('bill_zip', '')}<br/>"
            f"{customer.get('bill_phone', customer.get('phone', ''))}<br/>"
            f"{customer.get('bill_email', customer.get('email', ''))}"
        )

        t = Table([[Paragraph(ship_block, addr_style), Paragraph(bill_block, addr_style)]],
                  colWidths=[content_width / 2, content_width / 2])
        t.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ]))
        t.hAlign = "LEFT"
        story += [t, Spacer(1, block_spacer_large)]

        header = ["Qty", "Product Description", "Unit Price", "Total"]
        li_cols = [0.65 * inch, content_width - 0.65 * inch - 1.1 * inch - 1.1 * inch, 1.1 * inch, 1.1 * inch]
        data = [header]

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
                data.append(["", Paragraph(note_txt, notes_style), "", ""])

        t_li = Table(data, colWidths=li_cols, repeatRows=1)
        t_li.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("ALIGN", (0, 1), (0, -1), "CENTER"),
            ("ALIGN", (2, 1), (3, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), row_top_pad),
            ("BOTTOMPADDING", (0, 0), (-1, -1), row_bottom_pad),
        ]))
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
        if discount_label and discount_amount > 0:
            totals_rows.append([f"{discount_label}:", fmt_money(-discount_amount)])
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
            ("BACKGROUND", (0, -1), (-1, -1), colors.lightgrey),
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
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
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
            combined_table = Table([[acc_tbl, t_totals]], colWidths=[acc_width, totals_col_width])
            combined_table.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (0, 0), 0),
                ("RIGHTPADDING", (0, 0), (0, 0), 0),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ]))
            combined_table.hAlign = "LEFT"
            story += [combined_table, Spacer(1, block_spacer_large)]
        else:
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

    save_successful = save_quote_to_gsheet(payload)

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


def get_current_payload(
    subtotal: float,
    drop_ship_fee: float,
    freight: float,
    sales_tax: float,
    grand_total: float,
    tax_rate: float,
    ten_percent_discount: float,
    discount_label: str,
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
        "ten_percent_discount": ten_percent_discount,
        "discount_label": discount_label,
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

    discount_idx = find_course_discount_index(items)

    if current_index == discount_idx and direction == "up":
        return

    if new_index == discount_idx and discount_idx == len(items) - 1:
        if direction == "down":
            return

    if new_index != current_index:
        items[current_index], items[new_index] = items[new_index], items[current_index]
        ensure_course_discount_stays_last(items)
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
    st.session_state["rerun_flag"] = True


def handle_quantity_change(item_id: str):
    items = st.session_state["line_items"]

    for item in items:
        if item["id"] == item_id:
            item_qty = int(st.session_state[f"qty_input_{item_id}"])
            item_unit = float(item.get("unit", 0.0))
            item["qty"] = item_qty
            item["total"] = round(item_qty * item_unit, 2)
            break

    if ensure_course_discount(items):
        st.session_state["rerun_flag"] = True


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


def main_app():
    st.title("DGA Quoting Tool")

    st.markdown("""
        <style>
            .stButton>button {
                white-space: nowrap !important;
                font-size: 14px;
                line-height: 1.0;
                height: 38px;
                margin-top: 0px;
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
        </style>
    """, unsafe_allow_html=True)

    if st.session_state["rerun_flag"]:
        st.session_state["rerun_flag"] = False
        st.rerun()

    lookup_col1, lookup_col2, lookup_col_stack = st.columns([1.0, 1.4, 0.7])
    cust_key_suffix = st.session_state["customer_key_suffix"]

    with lookup_col1:
        st.markdown("**Current Doc # (PT)**")
        st.info(st.session_state["quote_no"])

    with lookup_col2:
        all_quotes_df = load_all_quotes()
        quote_options = ["(New Quote)"]
        if "Quote #" in all_quotes_df.columns:
            quote_options.extend(all_quotes_df["Quote #"].tolist())

        current_quote_no = st.session_state["quote_no"]
        if current_quote_no not in quote_options:
            quote_options.append(current_quote_no)

        try:
            default_index = quote_options.index(current_quote_no)
        except ValueError:
            default_index = 0

        selected_quote_no = st.selectbox(
            "Select or Search for Doc #",
            quote_options,
            index=default_index,
            key="quote_select_box"
        )

    with lookup_col_stack:
        with st.container():
            st.markdown("<div style='min-height: 25px;'></div>", unsafe_allow_html=True)

            if st.button("Retrieve", use_container_width=True, key="btn_retrieve_quote"):
                if selected_quote_no != "(New Quote)":
                    st.session_state["quote_no"] = selected_quote_no

                    try:
                        target_row_df = all_quotes_df[all_quotes_df["Quote #"] == selected_quote_no]

                        if target_row_df.empty:
                            st.error(f"Quote/Order # {selected_quote_no} not found in the loaded data.")
                            return

                        payload = target_row_df.iloc[-1]["Payload"]

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

                        st.session_state["footer_notes"] = payload.get("footer_notes", st.session_state["footer_notes"])

                        order_meta = payload.get("order_meta", {})
                        st.session_state["order_po_number"] = order_meta.get("po_number", "")
                        st.session_state["order_operator"] = order_meta.get("operator", "CZ")
                        st.session_state["order_auth_code"] = order_meta.get("auth_code", order_meta.get("terms", "AP - "))
                        st.session_state["order_comm_to"] = order_meta.get("commission_to", "")
                        st.session_state["order_check_number"] = order_meta.get("check_number", "")
                        st.session_state["order_date_received"] = order_meta.get(
                            "date_received",
                            get_pacific_now().strftime("%m/%d/%y")
                        )

                        loaded_doc_number = order_meta.get("order_doc_number", st.session_state["quote_no"])
                        st.session_state["order_doc_number_pdf"] = loaded_doc_number or st.session_state["quote_no"]

                        for item in st.session_state["line_items"]:
                            item_id = item.get("id")
                            if item_id:
                                st.session_state[f"Notes_input_{item_id}"] = item.get("Notes", item.get("notes", ""))

                        st.session_state["customer_key_suffix"] += 1

                        st.success(f"Loaded document **{selected_quote_no}** from Google Sheets.")
                        st.rerun()

                    except IndexError:
                        st.error(f"Quote {selected_quote_no} not found in the loaded data.")
                    except Exception as e:
                        st.error(f"Couldn't load document {selected_quote_no} from Google Sheets: {e}")
                else:
                    st.warning("Please select a document to retrieve or click 'New Quote'.")

            if st.button("New Quote", use_container_width=True, type="secondary"):
                start_new_quote()

            if st.button("New Version", use_container_width=True, type="primary",
                         help="Create a new version number based on the current quote."):
                assign_new_quote_version()

    with st.sidebar:
        st.header("PDF Preview")
        st.checkbox("Show Live Quote Preview", key="show_pdf_preview")

        if st.session_state["show_pdf_preview"]:
            if st.session_state["sc_county_checkbox"]:
                tax_rate = SANTA_CRUZ_TAX_RATE
            else:
                tax_input = float(st.session_state.get("tax_rate_pct_input", 0.0))
                tax_rate = tax_input / 100 if tax_input > 0 else 0.0

            subtotal = sum(float(r["total"]) for r in st.session_state["line_items"] if r.get("previewChecked", True))
            discount_type = st.session_state["active_discount_type"]
            discount_label = get_discount_label(discount_type)
            ten_percent_discount = calculate_ten_percent_discount(
                st.session_state["line_items"],
                discount_type
            )
            drop_ship_fee = st.session_state["drop_fee_input"]
            freight = st.session_state["freight_fee_input"]
            pre_tax = subtotal - ten_percent_discount + float(drop_ship_fee) + float(freight)
            sales_tax = round(pre_tax * tax_rate, 2)
            grand_total = round(pre_tax + sales_tax, 2)

            preview_payload = get_current_payload(
                subtotal,
                drop_ship_fee,
                freight,
                sales_tax,
                grand_total,
                tax_rate,
                ten_percent_discount,
                discount_label,
            )

            try:
                pdf_data, compact_level_used = generate_single_page_pdf(
                    preview_payload["customer"],
                    preview_payload["line_items"],
                    preview_payload["fees"],
                    preview_payload["totals"],
                    preview_payload["quote_no"],
                    preview_payload["footer_notes"],
                    template="quote",
                    meta=preview_payload["order_meta"],
                )

                if compact_level_used > 0:
                    st.caption("Preview is using compact single-page mode.")

                base64_pdf = base64.b64encode(pdf_data).decode("utf-8")
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
                st.error(f"Preview unavailable: {e}")

    c = st.session_state["customer"]

    st.subheader("Customer Information")

    has_search_term = st.session_state.get("pd_term", "").strip() != ""
    has_matches = bool(st.session_state.get("pd_matches", []))
    expander_default_state = has_search_term or has_matches

    with st.expander("Pipedrive lookup (by email or name)", expanded=expander_default_state):
        if not PIPEDRIVE_API_TOKEN:
            st.warning("Pipedrive API Token not configured in environment variables. Lookup disabled.")
        else:
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

    with st.container(border=True):
        cols_addr = st.columns(2)

        with cols_addr[0]:
            st.subheader("Shipping Address")
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
            c["bill_company"] = st.text_input("Company", value=c.get("bill_company", c.get("company", "")), key=f"bill_company_{cust_key_suffix}")
            c["bill_name"] = st.text_input(
                "Name",
                value=c.get("bill_name", c.get("name", "")),
                key=f"bill_name_input_{cust_key_suffix}",
                help="This is the contact person for billing."
            )
            c["bill_phone"] = st.text_input("Phone", value=c.get("bill_phone", c.get("phone", "")), key=f"bill_phone_{cust_key_suffix}")
            c["bill_email"] = st.text_input("Email", value=c.get("bill_email", c.get("email", "")), key=f"bill_email_{cust_key_suffix}")
            c["bill_addr1"] = st.text_area("Address Line 1 ", value=c.get("bill_addr1", ""), key=f"bill_addr1_{cust_key_suffix}")
            bc1, bc2, bc3 = st.columns(3)
            c["bill_city"] = bc1.text_input("City", value=c.get("bill_city", ""), key=f"bill_city_input_{cust_key_suffix}")
            c["bill_state"] = bc2.text_input("State", value=c.get("bill_state", ""), key=f"bill_state_input_{cust_key_suffix}")
            c["bill_zip"] = bc3.text_input("Zip", value=c.get("bill_zip", ""), key=f"bill_zip_input_{cust_key_suffix}")

    st.divider()

    st.subheader("Line Items")
    st.button("Add Line Item", key="btn_add_line_top", on_click=add_item_callback)

    sku_to_name = PRODUCTS.set_index("SKU")["Name"].to_dict()
    sku_options_display = ["(custom)"] + [f"{s} — {sku_to_name.get(s, 'No Name')}" for s in PRODUCTS["SKU"].tolist()]

    ensure_course_discount(st.session_state["line_items"])

    for i in range(len(st.session_state["line_items"])):
        row = st.session_state["line_items"][i]
        row.setdefault("exclude_from_10_discount", False)
        is_course_discount = row.get("sku") == "CD"
        is_preview_checked = row.get("previewChecked", True)
        is_excluded_from_10 = row.get("exclude_from_10_discount", False)

        can_move_up = i > 0
        can_move_down = i < len(st.session_state["line_items"]) - 1

        if is_course_discount and i == len(st.session_state["line_items"]) - 1:
            can_move_up = False
            can_move_down = False

        if not is_course_discount and i == len(st.session_state["line_items"]) - 2 and find_course_discount_index(
                st.session_state["line_items"]) == len(st.session_state["line_items"]) - 1:
            can_move_down = False

        item_container = st.container(border=True)
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

            st.text_area("Notes (optional)", key=notes_key, height=30)
            row["Notes"] = st.session_state[notes_key]

    st.button("Add Line Item", key="btn_add_line_bottom", on_click=add_item_callback)

    st.subheader("Fees, Tax & Totals")
    cc1, cc2, cc3, cc4, cc5, cc6 = st.columns(6)
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

    st.markdown("**Freight Notes**")
    fn1, fn2, fn3 = st.columns(3)
    with fn1:
        st.checkbox("Business Address", key=_freight_note_key("Business Address"))
        st.checkbox("Residential Address", key=_freight_note_key("Residential Address"))
    with fn2:
        st.checkbox("Lift Gate Need", key=_freight_note_key("Lift Gate Need"))
        st.checkbox("Fork Lift Access", key=_freight_note_key("Fork Lift Access"))
    with fn3:
        st.checkbox("Loading Dock Access", key=_freight_note_key("Loading Dock Access"))
        st.checkbox("Local Pickup", key=_freight_note_key("Local Pickup"))

    st.text_input(
        "Other Freight Notes",
        key="freight_notes_other",
        placeholder="Optional extra freight details"
    )

    st.session_state["freight_notes"] = get_selected_freight_notes()

    tax_rate = SANTA_CRUZ_TAX_RATE if st.session_state["sc_county_checkbox"] else float(st.session_state["tax_rate_pct_input"]) / 100.0

    subtotal = sum(float(r["total"]) for r in st.session_state["line_items"] if r.get("previewChecked", True))
    discount_type = st.session_state["active_discount_type"]
    discount_label = get_discount_label(discount_type)
    ten_percent_discount = calculate_ten_percent_discount(
        st.session_state["line_items"],
        discount_type
    )
    pre_tax = subtotal - ten_percent_discount + float(drop_ship_fee) + float(freight)
    sales_tax = round(pre_tax * tax_rate, 2)
    grand_total = round(pre_tax + sales_tax, 2)

    s1, s2, s3, s4, s5 = st.columns(5)
    with s1:
        st.metric("Subtotal", f"${subtotal:,.2f}")
    with s2:
        if discount_label and ten_percent_discount > 0:
            st.metric(discount_label, f"-${ten_percent_discount:,.2f}")
        else:
            st.metric("Discount", "$0.00")
    with s3:
        st.metric("Drop-Ship Fee", f"${drop_ship_fee:,.2f}")
    with s4:
        st.metric("Freight", f"${freight:,.2f}")
    with s5:
        st.metric("Grand Total", f"${grand_total:,.2f}")

    qual_qty = eligible_qty_for_discount(st.session_state["line_items"])
    if qual_qty >= 9:
        st.success(f"Course Discount active: **-$100** × {qual_qty} qualifying baskets.")
    else:
        st.info(
            f"Qualifying baskets: {qual_qty}. Add {max(0, 9 - qual_qty)} more Mach 5/7/X (Std/Portable/No Frills) to trigger the Course Discount."
        )

    st.divider()

    st.subheader("Generate PDF Documents")

    quote_no = st.session_state["quote_no"]
    st.markdown(f"**Current Quote #:** `{quote_no}`")

    st.text_area("Footer Notes (shown on PDF)", key="footer_notes")

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

    payload = get_current_payload(
        subtotal,
        drop_ship_fee,
        freight,
        sales_tax,
        grand_total,
        tax_rate,
        ten_percent_discount,
        discount_label,
    )
    order_meta = payload["order_meta"]

    pdf_col1, pdf_col2 = st.columns(2)

    if pdf_col1.button("Generate & SAVE Quote PDF", use_container_width=True, type="primary"):
        handle_pdf_generation(payload, quote_no, "quote", pdf_col1)

    if pdf_col2.button("Process as Order / PO", use_container_width=True, type="secondary"):
        order_doc_number = st.session_state["order_doc_number_pdf"]
        handle_pdf_generation(payload, order_doc_number, "order", pdf_col2, order_meta=order_meta)


if __name__ == "__main__":
    main_app()
