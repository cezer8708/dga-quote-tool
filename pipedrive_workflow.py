"""Local intake import and resumable Pipedrive quote synchronization."""
import hashlib
import json
import re
import sqlite3
import uuid
from decimal import Decimal, ROUND_HALF_UP
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


class WorkflowError(Exception):
    pass


def quote_number():
    return datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%m%d-%H%M')


def draft_key(payload):
    return payload['quote_no']


def quote_file_stem(payload):
    return payload['quote_no'] + '_Quote'


QUOTE_FORM_IDS = {'f73eafc0-d9c5-11ef-9e24-09a7a7827b85'}


def is_quote_intake(lead):
    if lead.get('is_archived') or lead.get('is_deleted'):
        return False
    source = str(lead.get('source_name', '')).lower().replace(' ', '')
    origin = str(lead.get('origin', '')).lower()
    channel = str(lead.get('channel_id') or '').lower()
    title = str(lead.get('title') or '').lower()
    return lead.get('origin_id') in QUOTE_FORM_IDS or (
        (source == 'webforms' or origin == 'webforms') and
        ('quote' in channel or 'quote' in title or title.startswith('course lead ')))


def quote_intakes(client):
    return [lead for lead in client.all('/v1/leads', sort='add_time DESC', limit=500) if is_quote_intake(lead)]


def publish_saved_quote(api, store, lead_id, payload):
    """Save once per exact payload through the normal Saved Quotes backend."""
    signature = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
    state = store.assign(lead_id)
    if state.get('published_quotes', {}).get(payload['quote_no']) == signature:
        return True
    if not api['save_quote_to_gsheet_with_timeout'](payload, record_type='quote'):
        return False
    state = store.get(lead_id)
    state.setdefault('published_quotes', {})[payload['quote_no']] = signature
    state.setdefault('quotes', {})[draft_key(payload)] = payload
    store.save(lead_id, state)
    return True


def money(value):
    amount = Decimal(str(value))
    if not amount.is_finite():
        raise WorkflowError('Amounts must be finite numbers.')
    return amount.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


class Client:
    def __init__(self, domain, token):
        u = urlsplit(domain)
        self.base = f'{u.scheme}://{u.netloc}/api'
        self.session = requests.Session()
        self.session.headers['x-api-token'] = token

    def request(self, method, path, **kwargs):
        try:
            r = self.session.request(method, self.base + path, timeout=25, **kwargs)
            data = r.json()
        except (requests.RequestException, ValueError):
            raise WorkflowError('Pipedrive connection failed. Refresh before retrying; a write may have completed.') from None
        if not r.ok or not data.get('success'):
            raise WorkflowError(f'Pipedrive {method} {path}: HTTP {r.status_code}. {data.get("error", "Request failed")}')
        return data

    def get(self, path, **params):
        return self.request('GET', path, params=params).get('data')

    def all(self, path, **params):
        rows, paging = [], {'limit': params.pop('limit', 100)}
        while True:
            response = self.request('GET', path, params={**params, **paging})
            rows.extend(response.get('data') or [])
            extra = response.get('additional_data') or {}
            if extra.get('next_cursor'):
                paging['cursor'] = extra['next_cursor']
            elif (extra.get('pagination') or {}).get('more_items_in_collection'):
                paging['start'] = extra['pagination']['next_start']
            else:
                return rows


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__(); self.parts = []
    def handle_starttag(self, tag, attrs):
        if tag in ('br', 'p', 'div'): self.parts.append('\n')
    def handle_endtag(self, tag):
        if tag in ('p', 'div'): self.parts.append('\n')
    def handle_data(self, data): self.parts.append(data)


def plain(value):
    parser = TextParser(); parser.feed(value)
    return unescape(''.join(parser.parts)).strip()


def form_fields(content):
    # Pipedrive's generated summaries use bold question labels followed by answers.
    pieces = re.split(r'<b\b[^>]*>(.*?)</b>', content, flags=re.I | re.S)
    fields = {}
    for i in range(1, len(pieces), 2):
        label = plain(pieces[i])
        if 'submission summary' not in label.lower():
            fields[label] = plain(pieces[i + 1]).strip()
    return fields


def suggest_items(fields, catalog):
    """Only exact SKU/name selections auto-populate; free text remains reviewable."""
    proposed, issues = [], []
    for label, answer in fields.items():
        if not any(w in label.lower() for w in ('type', 'model', 'product', 'basket', 'sign')):
            continue
        if any(w in label.lower() for w in ('how many', 'quantity', 'number of')):
            continue
        matches = [p for p in catalog if answer.strip().casefold() in
                   (str(p['SKU']).casefold(), str(p['Name']).casefold())]
        if not matches:
            if answer and answer.strip().lower() not in ('(blank)', 'no'): issues.append(f'{label}: {answer}')
            continue
        kind = 'sign' if 'sign' in label.lower() else 'basket'
        quantities = [v for k, v in fields.items() if
                      any(w in k.lower() for w in ('how many', 'quantity', 'number of')) and kind in k.lower()]
        if len(quantities) != 1 or not re.fullmatch(r'\d+', quantities[0].strip()) or int(quantities[0]) < 1:
            issues.append(f'Confirm quantity for {answer}.'); continue
        p = matches[0]; qty = int(quantities[0]); unit = float(p['UnitPrice'])
        proposed.append({'id': str(uuid.uuid4()), 'sku': p['SKU'], 'prev_sku': p['SKU'],
                         'name': p['Name'], 'qty': qty, 'unit': unit, 'total': round(qty * unit, 2),
                         'Notes': str(p.get('Notes') or ''), 'previewChecked': True,
                         'exclude_from_10_discount': False})
    return proposed, issues


def basket_products(model, catalog):
    families = {
        'mach 2 pro': {'M2IG','M2P'},
        'mach 5': {'M5STD','M5P','M5NF','M5CO'},
        'mach 7': {'M7STD','M7P','M7NF','M7CO'},
        'mach x': {'MXSTD','MXP','MXNF','MXCO'},
        'mach x pro': {'MXPRSTD','MXPRP'},
    }
    codes = families.get(model.strip().lower(), set())
    return [p for p in catalog if str(p['SKU']).upper() in codes]


class Store:
    def __init__(self, path='.local/pipedrive.sqlite3'):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS intake (lead TEXT PRIMARY KEY, data TEXT NOT NULL)')
    def connect(self):
        return sqlite3.connect(self.path, timeout=1)
    def get(self, lead):
        with self.connect() as db:
            row = db.execute('SELECT data FROM intake WHERE lead=?', (lead,)).fetchone()
        return json.loads(row[0]) if row else {}
    def save(self, lead, data):
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO intake VALUES (?,?)', (lead, json.dumps(data)))
    def list_drafts(self):
        with self.connect() as db:
            return [(lead, json.loads(data)) for lead, data in db.execute('SELECT lead,data FROM intake')]

    def assign(self, lead):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT data FROM intake WHERE lead=?', (lead,)).fetchone()
            if row: return json.loads(row[0])
            state = {'quote_no': quote_number()}
            db.execute('INSERT INTO intake VALUES (?,?)', (lead, json.dumps(state)))
            return state


def product_plan(payload, products):
    codes = {}
    for p in products:
        code = str(p.get('code') or '').casefold()
        if code: codes.setdefault(code, []).append(p)
    special_names = {'DROP': 'Drop Ship Fee', 'CD-M2P': 'Mach 2 Course Discount'}
    rows = []
    def add(key, sku, name, qty, unit):
        qty = Decimal(str(qty)); price = money(unit)
        if not qty.is_finite() or qty <= 0: raise WorkflowError(f'Invalid quantity for {sku}.')
        matches = codes.get(sku.casefold(), [])
        if not matches:
            target = special_names.get(sku, name)
            matches = [p for p in products if str(p.get('name', '')).casefold() == target.casefold()]
        if len(matches) != 1:
            raise WorkflowError(f'{sku} / {name}: needs a unique Pipedrive product mapping before syncing.')
        rows.append({'key': key, 'sku': sku, 'name': name, 'product_id': int(matches[0]['id']),
                     'quantity': float(qty), 'item_price': float(price), 'tax': 0,
                     'tax_method': 'none', 'discount': 0, 'is_enabled': True})
    for item in payload['line_items']:
        if item.get('previewChecked', True):
            add(item['id'], item['sku'], item['name'], item['qty'], item['unit'])
    for key, sku, name, amount in [
        ('freight','FT','Freight',payload['fees']['freight']),
        ('drop','DROP','Drop Ship Fee',payload['fees']['drop_ship_fee']),
        ('tax','TX','Tax',payload['totals']['sales_tax']),
        ('discount','COMD' if payload.get('discount_meta',{}).get('active_discount_type')=='commission' else 'ADD',
         'Additional Discount',-payload['totals']['ten_percent_discount']),
        ('manager','ADD','Additional Discount',-payload['totals']['manager_discount'])]:
        if money(amount): add(key, sku, name, 1, amount)
    total = sum((money(Decimal(str(r['quantity'])) * Decimal(str(r['item_price']))) for r in rows), Decimal(0))
    if total != money(payload['totals']['grand_total']):
        raise WorkflowError(f'Product rows total {total} differs from quote total {payload["totals"]["grand_total"]}.')
    if not rows: raise WorkflowError('Add quote items before syncing.')
    return rows


def attach_pdf(client, deal_id, pdf_bytes, title, identity=None):
    """Content-addressed upload with a read-back check and safe retry."""
    digest = hashlib.sha256(identity if identity is not None else pdf_bytes).hexdigest()
    filename = f'{title}.pdf'
    marker = 'DGA quote fingerprint: ' + digest
    staging_name = f'{title}-{digest[:16]}.pdf'
    files = client.all(f'/v1/deals/{deal_id}/files')
    existing = next((f for f in files if f.get('name') == filename and f.get('description') == marker), None)
    if existing:
        return existing
    staged = next((f for f in files if f.get('name') == staging_name), None)
    if staged is None:
        staged = client.request('POST', '/v1/files', data={'deal_id': deal_id},
                       files={'file': (staging_name, pdf_bytes, 'application/pdf')})['data']
    client.request('PUT', f'/v1/files/{staged["id"]}', json={'name': filename, 'description': marker})
    saved = client.all(f'/v1/deals/{deal_id}/files')
    matches = [f for f in saved if f.get('name') == filename and f.get('description') == marker]
    if len(matches) != 1:
        raise WorkflowError('The PDF upload could not be verified in the deal files. Refresh before retrying.')
    return matches[0]


def sync(client, store, lead_id, payload, rows, stage_id, pdf_bytes):
    # Serialize sync calls across local sessions. Persist each completed operation separately.
    import fcntl
    with open(store.path + '.lock', 'a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise WorkflowError('Another sync is running. Try again after it finishes.')
        state = store.get(lead_id)
        if not state: raise WorkflowError('Load an intake before syncing.')
        state['payload'] = payload
        state.setdefault('quotes', {})[draft_key(payload)] = payload
        store.save(lead_id, state)
        if state.get('uncertain_conversion'):
            raise WorkflowError('The previous conversion response was lost. Check Pipedrive before attempting another conversion.')
        if not state.get('deal_id'):
            if not state.get('conversion_id'):
                lead = client.get('/v1/leads/' + lead_id)
                person = client.get('/v1/persons/' + str(lead['person_id']))
                if person.get('name','').strip().casefold() != 'cesar quote test':
                    raise WorkflowError('Local testing only: deal writes are limited to Cesar Quote Test.')
                state['uncertain_conversion'] = True; store.save(lead_id, state)
                result = client.request('POST', f'/v2/leads/{lead_id}/convert/deal', json={'stage_id': stage_id})['data']
                state['conversion_id'] = result.get('conversion_id') or result.get('id')
                if not state['conversion_id']: raise WorkflowError('Conversion returned no job ID. Check Pipedrive.')
                state['uncertain_conversion'] = False; store.save(lead_id, state)
            status = client.get(f'/v2/leads/{lead_id}/convert/status/{state["conversion_id"]}')
            if status.get('status') != 'completed':
                raise WorkflowError(f'Conversion status: {status.get("status")}. Click sync again to check progress.')
            state['deal_id'] = status['deal_id']; store.save(lead_id, state)
        deal_id = state['deal_id']
        # Recheck person even on revisions; local test must not modify unrelated deals.
        deal = client.get(f'/v2/deals/{deal_id}')
        person = client.get('/v1/persons/' + str(deal['person_id']))
        if person.get('name','').strip().casefold() != 'cesar quote test':
            raise WorkflowError('This deal is not linked to Cesar Quote Test.')
        existing = client.all(f'/v2/deals/{deal_id}/products')
        prefix = f'DGA intake {lead_id} row '
        if any(not str(r.get('comments','')).startswith(prefix) for r in existing):
            raise WorkflowError('The deal has manually added product rows. Review those before syncing to avoid double counting.')
        keyed = {}
        for row in existing:
            marker = row['comments']
            if marker in keyed: raise WorkflowError('Duplicate imported rows detected; review the deal.')
            keyed[marker] = row
        desired = set()
        for row in rows:
            marker = prefix + row['key']; desired.add(marker)
            body = {k:v for k,v in row.items() if k not in ('key','sku','name')}; body['comments'] = marker
            if marker in keyed:
                client.request('PATCH', f'/v2/deals/{deal_id}/products/{keyed[marker]["id"]}', json=body)
            else:
                client.request('POST', f'/v2/deals/{deal_id}/products', json=body)
        for marker, row in keyed.items():
            if marker not in desired:
                client.request('DELETE', f'/v2/deals/{deal_id}/products/{row["id"]}')
        # Verify returned product sums independently of the headline deal value.
        saved = client.all(f'/v2/deals/{deal_id}/products')
        actual = sum((money(r['sum']) for r in saved if r.get('is_enabled', True)), Decimal(0))
        expected = money(payload['totals']['grand_total'])
        if actual != expected: raise WorkflowError(f'Pipedrive product total ${actual} does not match quote ${expected}.')
        client.request('PATCH', f'/v2/deals/{deal_id}', json={
            'title': f'{payload["customer"].get("company") or "Cesar Quote Test"} — Quote {payload["quote_no"]}'})
        confirmed = client.get(f'/v2/deals/{deal_id}')
        if money(confirmed['value']) != expected: raise WorkflowError('Saved deal value differs from the quote.')
        attach_pdf(client, deal_id, pdf_bytes, quote_file_stem(payload),
                   identity=json.dumps(payload, sort_keys=True, default=str).encode())
        state.update(payload=payload, verified_total=str(expected)); store.save(lead_id,state)
        return deal_id, expected
