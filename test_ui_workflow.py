"""Exercise compact controls with real Streamlit state and PDFs, without external writes."""
import copy
import io
import unittest
from pathlib import Path
from unittest.mock import patch

from PyPDF2 import PdfReader
from streamlit.testing.v1 import AppTest
import pipedrive_workflow_ui


class CompactWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.intake = patch.object(pipedrive_workflow_ui, 'render_intake').start()
        self.sync = patch.object(pipedrive_workflow_ui, 'render_sync').start()
        self.addCleanup(patch.stopall)
        source = Path('app.py').read_text()
        # Replace only network/save boundaries; actual lookup, load, widgets,
        # calculations, preview and PDF generation run unchanged.
        fixtures = '''
PIPEDRIVE_API_TOKEN = "test-token"
def pd_search_persons(term):
    return [{"id": 1, "name": "Test Customer", "email": "test@example.invalid"}]
def pd_get_person(person_id):
    return {"name": "Test Customer", "email": [{"value": "test@example.invalid"}], "phone": []}
def get_saved_quotes_snapshot():
    return pd.DataFrame([{"Doc #": "0901-1000-V2", "Quote #": "0901-1000-V2", "Name": "Legacy", "Company": "Legacy", "Email": "", "Date": "2026-09-01", "Payload": {"customer": {"name": "Legacy", "ship_addr2": "Legacy Suite", "ship_addr1": "Line one\\nLine two"}, "line_items": [{"id": "legacy-custom", "sku": "", "name": "Saved custom product", "qty": 2, "unit": 12.5, "total": 25, "Notes": "Saved configuration"}], "fees": {"freight": 42}, "freight_notes": "UPS", "footer_notes": "Legacy footer"}}])
def handle_pdf_generation(payload, doc_number, template, container, order_meta=None):
    pdf, _, _ = generate_pdf_preview_data(payload, template)
    st.session_state["_test_pdf"] = pdf
    st.session_state["_test_saved_payload"] = copy.deepcopy(payload)
def validate_manager_credentials():
    return st.session_state.get("manager_username") == "test" and st.session_state.get("manager_password") == "test"
'''
        source = source.replace('if __name__ == "__main__":', fixtures + '\nif __name__ == "__main__":')
        self.at = AppTest.from_string(source, default_timeout=30).run()
        self.healthy()

    def healthy(self):
        self.assertEqual([e.message for e in self.at.exception], [])
        self.assertFalse([e.value for e in self.at.error if 'Preview unavailable' in e.value])

    def rerun(self, widget, value=None):
        if value is None:
            widget.click().run()
        else:
            widget.set_value(value).run()
        if self.at.session_state["rerun_flag"]:
            self.at.run()
        self.healthy()

    def payload(self):
        return copy.deepcopy(self.sync.call_args.args[-1])

    def test_billing_restores_and_new_quote_clears_customer(self):
        a = self.at
        self.rerun(a.text_input(key='ship_company_0'), 'Shipping Co')
        self.rerun(a.text_input(key='bill_company_0'), 'Billing Co')
        self.rerun(a.checkbox(key='billing_same_as_shipping'), True)
        self.assertNotIn('bill_company_0', [w.key for w in a.text_input])
        self.assertEqual(self.payload()['customer']['bill_company'], 'Shipping Co')
        self.rerun(a.text_input(key='ship_company_0'), 'Updated Shipping')
        self.assertEqual(self.payload()['customer']['bill_company'], 'Updated Shipping')
        self.rerun(a.checkbox(key='billing_same_as_shipping'), False)
        self.assertEqual(a.text_input(key='bill_company_0').value, 'Billing Co')
        self.rerun(a.button(key='top_new_quote'))
        self.assertEqual(self.payload()['customer']['bill_company'], '')

    def test_second_address_lines_sync_restore_and_render(self):
        a = self.at
        self.assertEqual(a.text_area(key='ship_addr1_0').label, 'Address #1')
        self.assertEqual(a.text_input(key='ship_addr2_0').label, 'Address #2')
        self.rerun(a.text_input(key='ship_addr2_0'), 'Suite 200 & Receiving')
        self.rerun(a.text_input(key='bill_addr2_0'), 'Accounts Suite 300')
        self.rerun(a.checkbox(key='billing_same_as_shipping'), True)
        self.assertEqual(self.payload()['customer']['bill_addr2'], 'Suite 200 & Receiving')
        self.rerun(a.checkbox(key='billing_same_as_shipping'), False)
        self.assertEqual(a.text_input(key='bill_addr2_0').value, 'Accounts Suite 300')
        for key in ('generate_quote_pdf', 'process_order_po'):
            self.rerun(a.button(key=key))
            saved = a.session_state['_test_saved_payload']
            self.assertEqual(saved['customer']['ship_addr2'], 'Suite 200 & Receiving')
            self.assertEqual(saved['customer']['bill_addr2'], 'Accounts Suite 300')
            text = '\n'.join(page.extract_text() for page in PdfReader(io.BytesIO(a.session_state['_test_pdf'])).pages)
            self.assertIn('Suite 200 & Receiving', text)
            self.assertIn('Accounts Suite 300', text)
        self.rerun(a.button(key='top_new_quote'))
        self.assertEqual(self.payload()['customer']['ship_addr2'], '')
        self.assertEqual(self.payload()['customer']['bill_addr2'], '')

    def test_items_discounts_fees_notes_preview_and_pdf(self):
        a = self.at
        self.assertFalse(any('more' in c.value and 'unlock' in c.value for c in a.caption))
        self.rerun(a.button(key='btn_add_line_top'))
        item = a.session_state['line_items'][0]['id']
        select = a.selectbox(key=f'sku_select_{item}')
        self.rerun(select, next(v for v in select.options if v.startswith('M5STD —')))
        self.rerun(a.number_input(key=f'qty_input_{item}'), 9)
        codes = {r['sku'] for r in self.payload()['line_items']}
        self.assertTrue({'CD', '50th'} <= codes)
        self.rerun(a.checkbox(key='apply_course_discount'), False)
        self.assertNotIn('CD', {r['sku'] for r in self.payload()['line_items']})
        self.rerun(a.checkbox(key='apply_course_discount'), True)
        self.rerun(a.checkbox(key='apply_anniversary_discount'), False)
        self.assertNotIn('50th', {r['sku'] for r in self.payload()['line_items']})
        self.rerun(a.checkbox(key='apply_anniversary_discount'), True)
        select = a.selectbox(key=f'sku_select_{item}')
        self.rerun(select, next(v for v in select.options if v.startswith('M2IG —')))
        self.assertTrue({'CD-M2P', '50thM2'} <= {r['sku'] for r in self.payload()['line_items']})
        self.rerun(a.number_input(key='drop_fee_input'), 25.0)
        self.rerun(a.number_input(key='freight_fee_input'), 100.0)
        self.rerun(a.number_input(key='tax_rate_pct_input'), 5.0)
        self.rerun(a.text_input(key='freight_notes_other'), 'Call ahead')
        self.rerun(next(c for c in a.checkbox if c.label == 'Lift Gate Needed'), True)
        self.rerun(a.text_area(key=f'Notes_input_{item}'), 'Blue configuration')
        self.rerun(a.text_area(key='footer_notes'), 'Custom footer')
        self.rerun(a.checkbox(key='discount_checkbox'), True)
        self.rerun(a.text_input(key='discount_note'), 'Test discount')
        self.assertGreater(self.payload()['totals']['ten_percent_discount'], 0)
        self.assertEqual(self.payload()['totals']['tax_rate_pct'], .05)
        self.rerun(a.checkbox(key='sc_county_checkbox'), True)
        self.assertEqual(self.payload()['totals']['tax_rate_pct'], .0975)
        self.rerun(a.checkbox(key='manager_pricing_checkbox'), True)
        self.rerun(a.text_input(key='manager_pricing_note'), 'Test pricing')
        self.rerun(a.text_input(key='manager_username'), 'test')
        self.rerun(a.text_input(key='manager_password'), 'test')
        self.rerun(a.button(key='btn_authorize_manager'))
        self.assertGreater(self.payload()['totals']['manager_discount'], 0)
        self.rerun(a.button(key='generate_quote_pdf'))
        document = PdfReader(io.BytesIO(a.session_state['_test_pdf']))
        text = ''.join(p.extract_text() for p in document.pages)
        for expected in ['Custom footer', 'Call ahead', 'Blue configuration', 'Lift Gate Needed']:
            self.assertIn(expected, text)
        self.assertTrue(any("data:application/pdf;base64," in m.value for m in a.sidebar.markdown))
        self.assertNotIn('show_pdf_preview', [w.key for w in a.toggle])
        self.rerun(a.button(key='top_new_version'))
        self.assertTrue(a.session_state['quote_no'].endswith('-V2'))
        self.assertEqual(a.number_input(key='freight_fee_input').value, 100)
        self.assertEqual(a.text_area(key='footer_notes').value, 'Custom footer')
        self.rerun(a.button(key=f'btn_rm_{item}'))
        self.assertEqual(a.session_state['line_items'], [])

    def test_sidebar_preview_updates_with_current_quote(self):
        import base64
        import re
        a = self.at
        self.rerun(a.button(key='btn_add_line_top'))
        item = a.session_state['line_items'][0]['id']
        select = a.selectbox(key=f'sku_select_{item}')
        self.rerun(select, next(v for v in select.options if v.startswith('M5STD —')))
        self.rerun(a.number_input(key=f'qty_input_{item}'), 3)
        self.rerun(a.text_input(key='ship_company_0'), 'Preview Customer')
        self.rerun(a.number_input(key='freight_fee_input'), 85)
        self.rerun(a.text_area(key='footer_notes'), 'Preview review notes')
        markup = next(m.value for m in a.sidebar.markdown if 'data:application/pdf;base64,' in m.value)
        encoded = re.search(r'data:application/pdf;base64,([^#"]+)', markup).group(1)
        text = ''.join(p.extract_text() for p in PdfReader(io.BytesIO(base64.b64decode(encoded))).pages)
        for expected in ['Preview Customer', 'Preview review notes', '85.00', a.session_state['quote_no']]:
            self.assertIn(expected, text)
        self.assertNotIn('top_preview_quote', [w.key for w in a.button])
        self.assertEqual(a.number_input(key=f'qty_input_{item}').value, 3)

    def test_saved_lookup_and_pipedrive_apply(self):
        a = self.at
        self.rerun(a.text_input(key='pd_term'), 'Test Customer')
        self.rerun(a.button(key='pd_apply_btn'))
        self.assertEqual(self.payload()['customer']['name'], 'Test Customer')
        self.rerun(a.text_input(key='person_quote_search'), '0901-1000')
        self.rerun(a.button(key='btn_load_person_quote_match'))
        self.assertEqual(a.session_state['quote_no'], '0901-1000-V2')
        self.assertEqual(self.payload()['customer']['name'], 'Legacy')
        self.assertEqual(a.number_input(key='freight_fee_input').value, 42)
        self.assertIn('UPS', self.payload()['freight_notes'])
        self.assertEqual(a.text_area(key='footer_notes').value, 'Legacy footer')
        self.assertEqual(a.text_input(key='name_input_legacy-custom').value, 'Saved custom product')
        self.assertEqual(a.text_area(key='Notes_input_legacy-custom').value, 'Saved configuration')
        self.assertEqual(self.payload()['line_items'][0]['total'], 25)
        self.assertEqual(self.payload()['customer']['ship_addr1'], 'Line one\nLine two')
        self.assertEqual(self.payload()['customer']['ship_addr2'], 'Legacy Suite')
        self.assertEqual(self.payload()['customer']['bill_addr2'], '')

    def test_custom_name_only_when_applicable(self):
        a = self.at
        self.rerun(a.button(key='btn_add_line_top'))
        item = a.session_state['line_items'][0]['id']
        self.assertNotIn(f'name_input_{item}', [w.key for w in a.text_input])
        self.rerun(a.selectbox(key=f'sku_select_{item}'), '(custom)')
        self.rerun(a.text_input(key=f'name_input_{item}'), 'Custom product')
        self.assertEqual(self.payload()['line_items'][0]['name'], 'Custom product')
        select = a.selectbox(key=f'sku_select_{item}')
        self.rerun(select, next(v for v in select.options if v.startswith('M5STD —')))
        self.assertNotIn(f'name_input_{item}', [w.key for w in a.text_input])


if __name__ == '__main__':
    unittest.main()
