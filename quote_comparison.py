"""Build independent basket options with the existing quote pricing engine."""
import copy
import io
import re

from PyPDF2 import PdfReader
from pipedrive_workflow import WorkflowError, basket_products, quote_file_stem


def comparison_payloads(api, source, selections, reference):
    catalog = api['PRODUCTS'].to_dict('records')
    basket_skus = {p['SKU'] for model in ('Mach 2 Pro', 'Mach 5', 'Mach 7', 'Mach X', 'Mach X Pro')
                   for p in basket_products(model, catalog)}
    baskets = [r for r in source['line_items'] if r['sku'] in basket_skus and r.get('previewChecked', True)]
    if len(baskets) != 1:
        raise WorkflowError('Start with a reviewed quote containing one basket model to create comparison pages.')
    base = baskets[0]
    results = []
    match = re.fullmatch(r'(\d{4}-\d{4})(?:-V(\d+))?', reference)
    if not match:
        raise WorkflowError('Quote numbers must use MMDD-HHMM, with an optional -V2 revision.')
    root, version = match.groups()
    first_version = int(version or 1)
    for index, selection in enumerate(selections):
        product = next((p for p in catalog if p['SKU'] == selection['sku']), None)
        if not product or product['SKU'] not in basket_skus:
            raise WorkflowError('Choose a valid basket configuration for every option.')
        payload = copy.deepcopy(source)
        number = first_version + index
        payload['quote_no'] = root + (f'-V{number}' if number > 1 else '')
        if payload.get('pipedrive_link'):
            payload['pipedrive_link']['option_sku'] = product['SKU']
        payload['order_meta']['source_quote_number'] = payload['quote_no']
        payload['order_meta']['order_doc_number'] = payload['quote_no']
        items = [r for r in payload['line_items'] if r['sku'] not in api['COURSE_DISCOUNT_SKUS']]
        row = next(r for r in items if r['id'] == base['id'])
        if row['sku'] != product['SKU']:
            row.update(sku=product['SKU'], prev_sku=product['SKU'], name=product['Name'],
                       unit=float(product['UnitPrice']), Notes=str(product.get('Notes') or ''))
        row['total'] = round(row['qty'] * row['unit'], 2)
        api['ensure_course_discount'](items)
        subtotal = sum(float(r['total']) for r in items if r.get('previewChecked', True))
        meta = payload['discount_meta']
        primary = api['calculate_primary_discount'](items, meta.get('active_discount_type', ''))
        manager = api['calculate_manager_discount'](subtotal, bool(meta.get('manager_pricing_applied')))
        payload['fees']['freight'] = float(selection['freight'])
        pre_tax = subtotal - primary - manager + payload['fees']['freight'] + payload['fees']['drop_ship_fee']
        tax = round(pre_tax * float(payload['totals']['tax_rate_pct']), 2)
        payload['line_items'] = items
        payload['totals'].update(subtotal=subtotal, ten_percent_discount=primary, manager_discount=manager,
                                 sales_tax=tax, grand_total=round(pre_tax + tax, 2))
        results.append(payload)
    if not results:
        raise WorkflowError('Choose at least one comparison option.')
    return results


def comparison_files(api, payloads):
    files = []
    for payload in payloads:
        pdf, _, _ = api['generate_pdf_preview_data'](payload)
        reader = PdfReader(io.BytesIO(pdf))
        if len(reader.pages) != 1:
            raise WorkflowError(f"Quote {payload['quote_no']} does not fit on one page.")
        files.append({'payload':payload, 'filename':quote_file_stem(payload) + '.pdf', 'pdf':pdf})
    return files
