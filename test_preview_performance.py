import copy
import unittest
from unittest.mock import patch

import app


class PreviewAndSearchPerformanceTests(unittest.TestCase):
    def test_preview_reuses_identical_payload_and_invalidates_on_changes(self):
        payload = dict(quote_no='test-V1', date='2026-10-30', customer={},
                       line_items=[], fees={}, totals={}, footer_notes='', order_meta={})
        with patch.object(app.st, 'session_state', {}), patch.object(
            app, 'generate_single_page_pdf', return_value=(b'pdf', 0)
        ) as build:
            first = app.generate_pdf_preview_data(payload)
            self.assertEqual(first, app.generate_pdf_preview_data(copy.deepcopy(payload)))
            self.assertEqual(build.call_count, 1)
            payload['customer']['name'] = 'Updated'
            app.generate_pdf_preview_data(payload)
            self.assertEqual(build.call_count, 2)
            app.generate_pdf_preview_data(payload, template='order')
            self.assertEqual(build.call_count, 3)

    def test_search_uses_index_and_treats_punctuation_literally(self):
        frame = app.pd.DataFrame([
            {'Doc #': 'one', 'Search Blob': 'customer [a]* example.com'},
            {'Doc #': 'two', 'Search Blob': 'customer b exampleXcom'},
        ])
        with patch.object(app, '_quote_customer_search_blob', side_effect=AssertionError('index rebuilt')):
            for term in ('[a]*', 'example.com'):
                self.assertEqual(app.search_saved_quotes(frame, term)['Doc #'].tolist(), ['one'])

    def test_unchanged_pdf_keeps_same_browser_preview(self):
        with patch.object(app.st, 'markdown') as markdown:
            app._render_pdf_native_browser_preview(b'pdf', '80vh')
            first = markdown.call_args
            app._render_pdf_native_browser_preview(b'pdf', '80vh')
            self.assertEqual(first, markdown.call_args)
